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

# Query service (Rust, separate Cargo workspace)
cd query && cargo build && cargo test --workspace
```

Tests against a live catalog/Kafka are opt-in (`ICEBERG_IT=1`, `E2E=1`, `QUERY_IT=1`); the local stack (`tests/it/stack.sh up`) and prerequisites are in docs/integration-testing.md.

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
- Nothing is skipped: undecodable messages go to a Kafka DLQ topic before their offset advances; full pipelines pause partitions
- Appender DuckDB sessions run in UTC: Iceberg partition transforms of timestamptz are UTC, DuckDB otherwise uses the machine's zone
- `_record_index` (position within the Kafka message) + topic/partition/offset is the unique row key; the query service's cursors depend on it
- DuckDB (≥ 1.5.4) stages rows per partition (`duckdb::Appender`) and writes Iceberg; include it only via `src/appender/duckdb_include.hpp` (works around a C++23 incompatibility in DuckDB's headers)

**Components:**
- `otel_receiver` (src/main.cpp + src/ingester/): Crow HTTP server that accepts OTLP logs and produces to Kafka
- `otel_appender` (src/appender/): `KafkaSource` (poll thread) → `PipelinePool` (decode/transform/stage) → `StagingRegistry` → `IcebergCommitter` → `DuckdbIcebergSink`; wired in `app.cpp`
- `otel-query` (query/, Rust): `logquery` (IR, JSON frontend, cursors; no engine dependency) → `planner` (`FieldResolver` field namespace, IR → DataFusion exprs, paginated search) → `query-server` (axum API, `IcebergSource` table cache, read-only SQL). DataFusion 53 + iceberg-rust 0.10 (pinned together). Every frontend must lower to the IR and resolve fields through `FieldResolver`; this keeps adding LogQL a two-way door (docs/query-design.md §3)
- Libraries: `appender_core` (DuckDB-free, tested with fakes) and `appender_duckdb`

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
