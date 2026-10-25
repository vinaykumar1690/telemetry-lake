# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

```bash
# Configure and build (from repo root)
cd build && cmake .. -GNinja -DCMAKE_POLICY_VERSION_MINIMUM=3.5 && ninja

# Build specific targets
ninja otel_receiver      # HTTP ingester
ninja otel_appender      # Kafka consumer/Iceberg writer

# Run tests
ctest --output-on-failure
./http_server_test       # Run single test
./appender_core_test     # Appender logic (DuckDB-free, fakes)
./appender_duckdb_test   # Staging and commit SQL against in-memory DuckDB
./observability_test     # Logger, metrics rendering, librdkafka stats parsing

# Query service (Rust, separate Cargo workspace)
cd query && cargo build && cargo test --workspace

# UI (React + TS + Vite; npm run dev proxies /api to localhost:8090, :4318 and :8095)
cd ui && npm ci && npm test && npm run build

# Load generator (Rust, its own crate; the ignored test prints MB/s per core)
cd loadgen && cargo test && cargo test --release -- --ignored --nocapture throughput
```

Tests against a live catalog/Kafka are opt-in (`ICEBERG_IT=1`, `E2E=1`, `QUERY_IT=1`, `COMPACTION_IT=1`); the local stack (`tests/it/stack.sh up`) and prerequisites are in docs/integration-testing.md.

The `-DCMAKE_POLICY_VERSION_MINIMUM=3.5` flag is required for cppkafka compatibility.

## Architecture

Telemetry Lake is a C++23 pipeline for ingesting OpenTelemetry logs and writing them to Apache Iceberg tables with exactly-once semantics. Full design: `docs/design.md`.

```
OTel Agents → Ingester (HTTP :4318) → Kafka → Appender → Iceberg (S3/MinIO, REST catalog) ← Query (HTTP :8090)
```

**Key design decisions:**
- Ingester wraps raw payloads in `RawTelemetryMessage` (proto/telemetry_wrapper.proto) without parsing; parsing happens in the appender
- Appender exactly-once: each Iceberg commit inserts the log rows *and* their Kafka offsets into `_appender_offsets` in one DuckDB transaction (sent as a single REST `transactions/commit`). Recovery reads that table; Kafka consumer-group offsets are only hints for lag monitoring
- Revoked partitions are fenced: the revoke callback waits for any in-flight commit covering them (or exits) before returning
- Nothing is skipped: undecodable messages go to a Kafka DLQ topic before their offset advances; full pipelines pause partitions. librdkafka keeps a partition's pause flag across revoke and reassignment, so `onRevoked` resumes the partitions it had paused while it still owns them. Never resume a partition that is not paused: the image's librdkafka (2.3, Ubuntu 24.04) then restarts its fetch from an earlier position and duplicates messages (fixed in 2.4)
- Appender memory: DuckDB sizes itself from what it detects (often the node's memory and cores) and spills to `temp_directory`, which must be writable; the chart sets `DUCKDB_MEMORY_LIMIT_MB`, `DUCKDB_THREADS`, `DUCKDB_TEMP_DIRECTORY` (emptyDir), `KAFKA_QUEUED_MAX_KBYTES` and `MALLOC_ARENA_MAX` to fit the pod
- Appender DuckDB sessions run in UTC: Iceberg partition transforms of timestamptz are UTC, DuckDB otherwise uses the machine's zone
- Compaction/maintenance is a separate Spark job (`compaction/compact.py`, docker/compaction, Helm CronJob); on Nessie (gc.enabled=false) files are reclaimed by the nessie-gc CronJob. Never set `write.target-file-size-bytes` on the logs table: DuckDB refuses to append to partitioned tables that set it
- `PROMOTED_ATTRIBUTES` adds VARCHAR columns after the base ones (`schema::TableSchema`); the query service resolves `k8s.pod.name` to `k8s_pod_name` by its `.`→`_` rule, so names must stay in sync
- `_record_index` (position within the Kafka message) + topic/partition/offset is the unique row key; the query service's cursors depend on it
- DuckDB (≥ 1.5.4) stages rows per partition (`duckdb::Appender`) and writes Iceberg; include it only via `src/appender/duckdb_include.hpp` (works around a C++23 incompatibility in DuckDB's headers)

**Components:**
- `otel_receiver` (src/main.cpp + src/ingester/): Crow HTTP server that accepts OTLP logs and produces to Kafka
- `otel_appender` (src/appender/): `KafkaSource` (poll thread) → `PipelinePool` (decode/transform/stage) → `StagingRegistry` → `IcebergCommitter` → `DuckdbIcebergSink`; wired in `app.cpp`
- `otel-query` (query/, Rust): `logquery` (IR, JSON frontend, cursors; no engine dependency) → `planner` (`FieldResolver` field namespace, IR → DataFusion exprs, paginated search) → `logscan` (`LogsTableProvider`/`LogsScanExec`: partitioned Iceberg scan, `LogsScanCodec` for workers) → `query-server` (axum API, `IcebergSource` table cache, read-only SQL, `distributed.rs`: datafusion-distributed handlers, `--worker` mode). DataFusion 55 with iceberg-rust and apache/datafusion-iceberg pinned by git revision (no release supports DF 55 yet); plain EXPLAIN hides distribution, use EXPLAIN ANALYZE. Every frontend must lower to the IR and resolve fields through `FieldResolver`; this keeps adding LogQL a two-way door (docs/query-design.md §3)
- `otel-loadgen` (loadgen/, Rust): sustained OTLP load for the UI's Load test tab (docs/load-testing.md).
  - Protobuf and JSON are written straight to bytes (`generate.rs`), with the Generate tab's message templates; keep the two in sync.
  - Rate loops are open-model (`node.rs`): a tick with no free in-flight slot counts as "behind" and is never queued.
  - The controller (`controller.rs`) splits the rate across `--worker` pods found through a headless Service, or sends the load itself without workers.
  - `INGEST_URL` comes only from the environment, never from a request.
- `ui/` (React, Vite): Search tab (`/v1/search`; facets and histogram via `/v1/sql`), Generate tab (OTLP/JSON built in the browser, POSTed to the ingester) and Load test tab (drives otel-loadgen through `/api/loadgen/`). nginx (`ui/nginx.conf.template`) serves it and proxies `/api/query/` and `/api/ingest/`, resolving upstreams per request, so they must be fully qualified names. Pure logic (`buildRequest.ts`, `facets.ts`, `generate.ts`, `send.ts`) is unit-tested with Vitest
- Libraries: `observability` (src/observability/: structured logger, prometheus-cpp registry, librdkafka statistics → `rdkafka_*`), `appender_core` (DuckDB-free, tested with fakes) and `appender_duckdb`

## Observability

Full reference: `docs/observability.md`.
- Metrics: ingester `:9464/metrics` (`METRICS_PORT`, kept off the OTLP port the UI proxies); appender `/metrics` on `HEALTH_PORT`; counters only, rates via PromQL. Existing `appender_*` names are a dashboard contract; keep them
- Appender commit lag = librdkafka high watermark − (durable Iceberg offset + 1); librdkafka's own `consumer_lag` counts from committed (hint) offsets, so `rdkafka_consumer_lag_messages` is computed from the position instead
- Data files per commit come from the logs table's current snapshot summary (`iceberg_load_table_response`, cast `::JSON`, not `::VARCHAR`)
- Logs: `obs::info/warn/error(component, msg, {fields})`, JSON lines when stdout is not a TTY; never log per message on hot paths (count it in a metric)
- Helm (`observability.enabled`): Prometheus + rules, Loki, Grafana (dashboards in `helm/telemetry-lake/dashboards/`), kafka-exporter, Kafka JMX agent, OTel Collector DaemonSet shipping pod logs to Loki

## Helm profiles

`values-local.yaml` (MinIO, in-memory Nessie, small budgets) and `values-eks.yaml` (S3 with keys in `s3.existingSecret`, gp3, Nessie on RocksDB, larger budgets); `values.yaml` holds the local defaults.
- Every S3 consumer takes its env from the `telemetry-lake.s3Env` helper; don't copy S3 settings into a template.
- `telemetry-lake.checkBudgets` fails the render when a memory budget doesn't fit its container.
- `duckdbShell` is the ad hoc CLI pod, not the appender's DuckDB.
- kind's volumes are world-writable host directories, while EBS volumes are root-owned ext4 with `lost+found`. Any pod writing a PVC needs an `fsGroup`, and a data dir must not be the volume root: Kafka refuses to start on `lost+found`. Check volume changes with the Docker rehearsal described in docs/eks.md, not only on kind.

## Configuration

`IngesterConfig::fromEnv()` (src/config.hpp) and `appender::AppenderConfig::fromEnv()` (src/appender/appender_config.cpp); integers go through `config_detail::integerFromEnv` and invalid values fail startup.

**Ingester required:** `KAFKA_BROKERS`
**Appender required:** `KAFKA_BROKERS`, `ICEBERG_CATALOG_URI`, `S3_ENDPOINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY` (full list in README.md)
**Query required:** `ICEBERG_CATALOG_URI`, `S3_ENDPOINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY` (`Config::from_env`, query/crates/query-server/src/config.rs; full list in query/README.md)

## Key Files

- `src/appender/schema.hpp` - Column lists for the logs, staging and offsets tables
- `src/appender/iceberg_committer.cpp` - Commit protocol (retries, indeterminate commits, fencing)
- `src/appender/duckdb_iceberg.cpp` - The atomic logs+offsets commit and offset recovery query
- `src/appender/kafka_source.cpp` - Rebalance handling and recovery seek
- `proto/telemetry_wrapper.proto` - Kafka message wrapper format

## Signal Handling

Appender: signals are blocked in all threads and handled by a `sigwait` thread. `SIGINT/SIGTERM` (drain, final commit, exit 0), `SIGUSR1` (force a commit). Exit code 2 on fatal errors.
