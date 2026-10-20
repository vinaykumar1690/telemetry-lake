# Integration testing

The unit tests (`ctest`) run against fakes and in-memory DuckDB, so they need
no services. This document covers the tests that run against a live Kafka,
Iceberg REST catalog (Nessie) and S3 store (MinIO). The design they verify is in
[design.md](design.md).

| Test | What it proves | Needs |
|------|----------------|-------|
| `tests/it/iceberg_spike.sh` | DuckDB + Nessie support everything the appender relies on: partitioned `day()` writes of `TIMESTAMPTZ`/`MAP` columns, a two-table transaction sent as one REST `transactions/commit`, a failed transaction leaving both tables unchanged, backslash round-trips. It also lists the available Iceberg maintenance functions | MinIO, Nessie, `duckdb` CLI |
| `appender_duckdb_test --gtest_filter='IcebergIntegration*'` | The appender's own code (`DuckdbSession::openIceberg`, staging, `DuckdbIcebergSink::commit`, `DuckdbOffsetStore::lastCommitted`) creates both tables, commits rows plus offsets, and recovers the offset | MinIO, Nessie |
| `tests/e2e/exactly_once.sh` | Exactly-once from Kafka to Iceberg under `kill -9`, a graceful restart and a rebalance between two appenders, plus poison messages going to the DLQ | MinIO, Nessie, Kafka, built binaries, `kcat`, `duckdb` CLI |
| `tests/it/compaction_spike.sh` | Spark (Iceberg Java) maintains DuckDB-written tables through Nessie: properties, sorted rewrites while DuckDB appends, bloom filters, DuckDB `ADD COLUMN`, Nessie GC | MinIO, Nessie, compaction image, `duckdb` CLI |
| `tests/it/compaction_it.py` | Compaction against real appender output with promoted attributes: file count, unchanged rows (checksum), offsets pruning, bloom filters, sort order, pagination across a compaction, and every `query_it` search afterwards | as `query_it.py`, plus the compaction image |
| `tests/it/query_it.py` | The query service against real appender output: 11 search shapes (time, column and map tags, numeric and absent attributes, grep, regex, combined) and cursor pagination (no duplicate or out-of-order rows), with each total checked against DuckDB over the same table; SQL count; the time filter reaching the Iceberg scan | MinIO, Nessie, Kafka, C++ binaries, `otel-query`, `duckdb` CLI, Python 3 |

All are opt-in (`ICEBERG_IT=1` / `E2E=1` / `QUERY_IT=1` / `COMPACTION_IT=1`) and print `skipped` otherwise. `COMPACT_DURING=1 E2E=1 tests/e2e/exactly_once.sh` additionally runs the compaction job back to back for the whole exactly-once test.

## Prerequisites

Tested on macOS 15 (Intel) with Docker Desktop 28.5.

| Tool | Version | Install | Used for |
|------|---------|---------|----------|
| Docker Desktop | with `docker compose` v2 | <https://docs.docker.com/desktop/> | Kafka and Nessie containers |
| MinIO server | 2025-10-15 or newer | `brew install minio && brew services start minio` | S3 store on `localhost:9000` |
| DuckDB CLI | **1.5.4 or newer** (1.5.6 tested) | `brew install duckdb` | spike, e2e assertions |
| kcat | 1.7 | `brew install kcat` | e2e: poison messages, end offsets, DLQ count |
| curl | 7.75 or newer (for `--aws-sigv4`) | ships with macOS | bucket creation, health checks |
| Build toolchain | see [README.md](../README.md#how-to-build) | | `otel_receiver`, `otel_appender`, `appender_duckdb_test` |
| Rust | 1.95 or newer (1.99 tested) | `curl https://sh.rustup.rs -sSf \| sh` (or `brew install rustup`) | `otel-query` (`cd query && cargo build`) |
| Python | 3.9 or newer, standard library only | ships with macOS / Homebrew | `tests/it/query_it.py` |

**MinIO runs on the host, not in Docker.** MinIO no longer publishes public
container images (`minio/minio` and `quay.io/minio/minio` pulls are denied),
so the stack uses the Homebrew server. Nessie reaches it through
`host.docker.internal`.

**Check which `duckdb` is on your PATH.** The DuckDB install script puts a CLI
in `~/.duckdb/cli/<version>/`, and that directory can come before Homebrew's on
`PATH`. A 1.4.x CLI cannot write partitioned tables and fails the spike.
`tests/it/stack.sh env` exports `DUCKDB` pointing at the Homebrew binary, and
the scripts use `$DUCKDB` when it is set:

```bash
duckdb --version                 # may be an old CLI
/usr/local/bin/duckdb --version  # Homebrew (Intel); /opt/homebrew/bin on Apple silicon
```

The appender itself links against the Homebrew `libduckdb`, which CMake checks
for version 1.5.4 or newer.

## The test stack

`tests/it/compose.yaml` together with `tests/it/stack.sh`:

| Service | Where | Port | Notes |
|---------|-------|------|-------|
| MinIO | host (Homebrew) | 9000 | credentials `minioadmin`/`minioadmin`; bucket `telemetrylakeit` |
| Kafka 4.3.1 | Docker, single KRaft node | 9092 | auto topic creation is off; the scripts create topics explicitly |
| Nessie (latest) | Docker | 19120 | Iceberg REST at `http://localhost:19120/iceberg/`; in-memory version store; warehouse `telemetrylakeit` → `s3://telemetrylakeit` |

```bash
brew services start minio          # once; it stays running
tests/it/stack.sh up               # bucket, Kafka, Nessie, topics e2e-logs (4 partitions) + e2e-logs-dlq
eval "$(tests/it/stack.sh env)"    # KAFKA_BROKERS, ICEBERG_*, S3_*, DUCKDB
...
tests/it/stack.sh down             # remove the containers
```

`stack.sh up` is idempotent, so you can run it again after changing
`compose.yaml`. `stack.sh topic NAME [PARTITIONS]` creates another topic.

### Nessie settings that matter

You only need these if you build your own stack, for example with the Nessie
`docker run` in [mac-setup.md](mac-setup.md):

- **Warehouse map keys.** `nessie.catalog.warehouses.<name>.location` is only
  read from its upper-case environment form,
  `NESSIE_CATALOG_WAREHOUSES_<NAME>_LOCATION`. A dotted variable name is ignored,
  and `/iceberg/v1/config` then returns `500 Default warehouse '...' is not
  defined`. Because of that upper-case mapping, keep the warehouse name to
  letters and digits; a hyphenated name cannot be expressed.
- **Request signing.** By default Nessie tells clients to use S3 remote signing
  (`s3.remote-signing-enabled: true`), which DuckDB does not implement. The
  stack sets `request-signing-enabled: false`. Because the appender also
  disables access delegation (below), only other clients depend on this.
- **External endpoint.** Nessie talks to MinIO at `http://host.docker.internal:9000`,
  and `external-endpoint` hands clients the same address. Iceberg's Java client
  (the Spark compaction job, in a container) uses the catalog's endpoint over its
  own, so it must be reachable from containers. DuckDB (access delegation off)
  and `otel-query` (its own `S3_ENDPOINT` wins) ignore it.
- **In-memory store.** `stack.sh down` (or any Nessie restart) forgets every
  table. The Parquet and metadata files stay in the bucket. Tests always create
  fresh namespaces, so leftovers are harmless; see [Cleaning up](#cleaning-up).

### Access delegation (DuckDB ↔ catalog)

By default DuckDB's Iceberg extension builds an S3 secret from the storage
config the catalog returns for each table (vended credentials). Nessie returns
an endpoint and region but no keys, so DuckDB writes anonymously and MinIO
answers `403 Forbidden`, even though a correct `CREATE SECRET` exists. The
appender therefore attaches with `ACCESS_DELEGATION_MODE 'none'`, set by
`ICEBERG_ACCESS_DELEGATION_MODE` (default `none`), and the scripts do the same.
Set it to `vended_credentials` only for a catalog that actually vends
credentials.

## Running the tests

From the repository root, with the stack up and its environment loaded:

```bash
eval "$(tests/it/stack.sh env)"

# 1. Capability spike (about 10 s)
ICEBERG_IT=1 tests/it/iceberg_spike.sh

# 2. Appender commit + recovery against the live catalog (about 5 s)
(cd build && ICEBERG_IT=1 DUCKDB_HOME_DIRECTORY=$HOME/.duckdb \
    ./appender_duckdb_test --gtest_filter='IcebergIntegration*')

# 3. End-to-end exactly-once (a few minutes)
E2E=1 tests/e2e/exactly_once.sh

# 4. Query service against real appender output (about 1 minute)
(cd query && cargo build)
QUERY_IT=1 tests/it/query_it.py

# 5. Compaction (build the image once; about 2-3 minutes each)
docker build -f docker/compaction/Dockerfile -t telemetry-lake/compaction:dev .
ICEBERG_IT=1 tests/it/compaction_spike.sh
COMPACTION_IT=1 tests/it/compaction_it.py
COMPACT_DURING=1 E2E=1 tests/e2e/exactly_once.sh
```

The compaction image is about 1.4 GB (Spark 4.1.3 plus Iceberg 1.11.0). It
reaches Nessie and MinIO through `host.docker.internal`; the tests rewrite
`localhost` in the environment for it.

`DUCKDB_HOME_DIRECTORY` is where DuckDB caches the `iceberg`/`httpfs`
extensions. Reusing `~/.duckdb` avoids a download per run.

### What a pass looks like

**Spike.** Each check prints a labelled row:

| Row | Expected |
|-----|----------|
| `two-table commit` | `logs` 2, `offsets` 1 |
| `partitions` | `day_ts_3` / `day` |
| `after failed txn (expect 2, 1)` | 2, 1 (the injected `Conversion Error` above it is expected) |
| `backslash round-trip` | `v\w` |
| `maintenance` | the Iceberg rewrite/expire functions in this DuckDB build (none in 1.5.6) |

To confirm the commit was a single catalog transaction, look in Nessie's access log:

```bash
docker compose -f tests/it/compose.yaml logs nessie | grep transactions/commit
# "POST /iceberg/v1/main%7Ctelemetrylakeit/transactions/commit HTTP/1.1" 204
```

**gtest.** `[ PASSED ] 1 test.`

**E2E.** Each run uses its own topic (`e2e-<epoch>`, 4 partitions), DLQ topic,
consumer group and Iceberg namespace (`e2e_<epoch>`), and starts its own
ingester on port 14318. The two appenders, `a` and `b`, use health ports
18080 and 18081. The script:

1. Sends 200 OTLP/JSON requests of 50 records through the ingester. Each
   record carries a unique `seq` attribute.
2. Along the way it `kill -9`s appender `a` and restarts it, starts `b` (a
   rebalance), `kill -9`s `b` and restarts it, and sends `a` a SIGTERM
   (graceful drain), restarting it afterwards.
3. Produces 5 undecodable messages straight to Kafka.
4. Waits until the table holds every row and the offsets table has reached
   every partition's end offset, then checks:
   - row count == distinct `seq` == 10 000: no duplicates and no loss
   - no duplicate `(partition, offset, seq)` rows
   - `max(last_offset) + 1` per partition equals the topic's end offset, so
     nothing was skipped and nothing is left uncommitted (poison messages
     included)
   - the DLQ holds at least 5 messages

It ends with `PASS: 10000 rows, no duplicates, no loss; N DLQ messages`.
A run takes about 3–6 minutes. Expect `rows=` to stop moving for up to a
minute after the crashes: a `kill -9`ed consumer keeps its partitions until
Kafka's session timeout (45 s) expires and the group rebalances. That pause
is expected, not a hang.
On failure, the appender and ingester logs are kept and their directory is
printed. `BATCHES`, `PER_BATCH`, `POISON` and `TOPIC_PARTITIONS` scale the run.

**Query test.** It creates a fresh topic (`qit-<epoch>`, 4 partitions) and
namespace, and starts an ingester (`:14319`), an appender (`:18082`) and the
query service (`:18090`). It then:
1. sends 60 requests of 50 records spread over the last 100 minutes; all
   records of a request share one timestamp, so pagination must break ties;
2. flushes the appender until DuckDB sees all 3000 rows;
3. pages through every search with 250-row pages, failing on any duplicate
   or out-of-order row, and compares each total with a DuckDB `count(*)`
   using the same condition.

Each line prints `ok`/`FAIL`, the row count, the oracle count and the time
taken; it ends with `PASS`. On failure the service logs are kept and their
directory is printed. `BATCHES` and `PER_BATCH` scale the data. It uses
`target/release/otel-query` when present, else the debug build, or
`QUERY_BIN`.

The e2e test covers Kafka → Iceberg. It does not crash the ingester: the
ingester acknowledges a request before Kafka has it (see design.md §6.2), so
client → Kafka is not exactly-once.

## Results (2026-10-18, DuckDB 1.5.6, Nessie latest, Kafka 4.3.1)

- **Spike: pass.** Partitioned `day()` writes with `TIMESTAMPTZ` and `MAP`
  work. The two-table insert went out as one `POST .../transactions/commit`
  (204). A failed transaction left both tables unchanged. Backslashes
  round-trip. **No Iceberg maintenance functions exist in DuckDB 1.5.6**, so
  the appender's offsets-table pruning stays disabled (it checks for
  `iceberg_rewrite_data_files` first). Snapshot expiry and compaction of both
  tables need an external job; see design.md §9.
- **gtest: pass.**
- **E2E: pass**, twice:
  - 200 × 50 records: `PASS: 10000 rows, no duplicates, no loss; 5 DLQ messages`
  - 100 × 50 records: `PASS: 5000 rows, ...`

  In both runs, every partition's durable next offset equalled its end offset,
  for example `0 44 1 55 2 54 3 52`.

### Query service (2026-10-19)

- **`query_it.py`: pass.** All 11 search shapes matched DuckDB exactly (for
  example 3000/3000 rows, 1111/1111 for the regex), every page sequence was
  duplicate-free and newest-first, the SQL count matched, and `EXPLAIN`
  showed `predicate:[timestamp >= ...]` inside `IcebergTableScan`.
- Its first run **found an appender bug**: DuckDB computed `day(timestamp)`
  partition values in the laptop's time zone (PDT). Rows written late in the
  local evening landed in the previous day's partition, which every reader
  then pruned for recent time ranges; DuckDB and the query service agreed on
  the wrong count, 600 of 3000. Appender sessions now run in UTC, and the
  `ICEBERG_IT` gtest writes rows at 03:00Z and 22:00Z to catch a regression
  (it fails without the fix).

### Compaction spike (2026-10-19, Spark 4.1.3, Iceberg 1.11.0)

`ICEBERG_IT=1 tests/it/compaction_spike.sh`, after building the image with
`docker build -f docker/compaction/Dockerfile -t telemetry-lake/compaction:dev .`:

- **Spark reads DuckDB-written tables.** Iceberg Java through the Nessie REST
  catalog, S3FileIO to MinIO.
- **Bloom-filter table properties do not block DuckDB appends, but
  `write.target-file-size-bytes` does.** DuckDB refuses to append to a
  partitioned table that sets it. The job therefore passes the target size as
  a `rewrite_data_files` option and sets no file-size property.
- **DuckDB supports `ALTER TABLE ... ADD COLUMN`** on Iceberg tables, so the
  appender adds promoted-attribute columns itself.
- **Compaction while DuckDB appends.** `rewrite_data_files` (sort) turned 11
  files into 1 and the manifests 12 into 1. All rows survived, and offsets
  pruning kept the maximum offset (6 rows down to 1).
- **The rewritten files have bloom filters** (`trace_id` has a
  `bloom_filter_offset`), and their rows are sorted by `service_name`.
- **Nessie keeps only the current snapshot in table metadata** (history lives
  in Nessie's commit log) and sets `gc.enabled=false`. Iceberg's
  `expire_snapshots` fails with "GC is disabled", so the job skips the expire
  and orphan steps on such tables.
  - **Nessie GC** (`ghcr.io/projectnessie/nessie-gc`) reclaims the files
    compaction replaced: the namespace went from 12 Parquet files to 1, and
    the table stayed readable and appendable.
  - Nessie GC must run with `--max-file-modification` well in the past. The
    appender writes data files before it commits them, and GC would otherwise
    delete files an in-flight commit is about to reference.
- **Consequence for the query service.** A cursor's pinned snapshot is gone
  from Nessie's metadata after the next appender commit. Cursors therefore
  fall back to the current snapshot (see query/README.md, Pagination).

### Compaction (2026-10-19)

- **`compaction_it.py`: pass.**
  - 60 one-request commits made 60 data files, which compaction turned into 1.
  - The original 3000 rows were unchanged (same count and checksum), and all
    3250 row keys stayed distinct.
  - The offsets table went from 60 rows to 4, with each partition's maximum
    kept.
  - The largest file has a bloom filter on `trace_id` and is sorted by
    service. The two-value promoted `k8s_pod_name` is fully
    dictionary-encoded, and Parquet skips bloom filters for such columns.
  - A search paginated across the compaction returned all 3000 rows once.
    `k8s_pod_name = …` reached the Iceberg scan, and all 11 `query_it`
    searches matched DuckDB afterwards.
- **`COMPACT_DURING=1` exactly-once: pass.** Five compaction runs (rewrites
  and offsets pruning) raced two appenders through `kill -9`, a rebalance and
  a graceful restart, and every run succeeded. The result was
  `PASS: 10000 rows, no duplicates, no loss; 5 DLQ messages`.

### Distributed query execution (2026-10-20)

`DISTRIBUTED=1` makes `query_it.py` and `compaction_it.py` start two local
workers (`otel-query --worker`) and distribute *every* scan
(`QUERY_DISTRIBUTE_MIN_BYTES=0`, `QUERY_BYTES_PER_TASK=1`):

```bash
(cd query && cargo build)
DISTRIBUTED=1 QUERY_IT=1 tests/it/query_it.py
DISTRIBUTED=1 COMPACTION_IT=1 tests/it/compaction_it.py
```

- **Both pass.** All 11 searches matched DuckDB with every scan on the
  workers, including after compaction and with pagination across a
  compaction. `query_it` also checks `EXPLAIN ANALYZE` for
  `NetworkCoalesceExec` and `DistributedLeafExec`. Plain `EXPLAIN` shows the
  plan before distribution.
- **The first run found a balancing bug.** With fewer files than groups,
  one task got every file (`t0: files=2`, `t1: files=0`). Fixed and covered
  by a test.

**Benchmark** (`tests/bench/bench.py`). Spark loads 20 M synthetic rows over
7 days (8 files, 551 MiB). Each figure is the p50 of 5 runs, in ms, on one
16-core laptop, so the workers share the coordinator's CPUs and disk:

```bash
python3 tests/bench/bench.py load --rows 20000000 --days 7
python3 tests/bench/bench.py run --workers 0,1,2,4
```

| Query | local | 1 worker | 2 workers | 4 workers |
|-------|------:|---------:|----------:|----------:|
| newest 100 in the last hour | 117 | 111 | 110 | 121 |
| grep over a day (newest 100) | 1880 | 1871 | 1788 | 1795 |
| map tag over a week (newest 100) | 5092 | 5147 | 4914 | 4281 |
| count by service over a week (SQL) | 187 | 184 | 164 | 160 |

`EXPLAIN ANALYZE` confirmed that a week-long query ran as 2 tasks of 4 files
each (323 MB and 255 MB).

What this does and does not show:
- Distribution adds no measurable latency at these sizes (the network hop is
  a few ms).
- Speedups are small because one machine's cores are shared. On separate
  nodes the scan work, which dominates the map-tag and grep queries, would
  divide across machines.
- The 5-second map-tag query is decode cost: every row's map is read. That
  is what phase 4's per-file `key=value` index and promoted columns address.

## Troubleshooting

| Symptom | Cause / fix |
|---------|-------------|
| `HTTP Error: ... Forbidden (HTTP code 403)` writing `s3://...parquet` | Vended-credential mode; attach with `ACCESS_DELEGATION_MODE 'none'` (see above) |
| `500 Default warehouse '...' is not defined` from `/iceberg/v1/config` | Warehouse location set through a dotted env var; use the upper-case form |
| Spike fails at `PARTITIONED BY` or `MAP` | An old `duckdb` CLI is first on `PATH`; use `$DUCKDB` |
| `MinIO is not reachable` | `brew services start minio`; check `curl localhost:9000/minio/health/live` |
| `bind: address already in use` on 9092/19120 | Another Kafka or Nessie is running; `docker ps`, or stop the other service |
| DuckDB: `Table property target-file-size-bytes is currently not supported for partitioned tables` | Something set `write.target-file-size-bytes` on the logs table; unset it (`ALTER TABLE ... UNSET TBLPROPERTIES`). The compaction job passes the size as a rewrite option instead |
| Compaction: `Cannot expire snapshots: GC is disabled` | Expected on Nessie; current `compact.py` skips the step (use Nessie GC) |
| Time-range queries miss recent rows that `count(*)` finds | Data written by an appender built before the UTC fix on a non-UTC machine; rewrite it, or use a fresh namespace |
| Ingester log: `Coordinator load in progress: retrying` | Harmless right after Kafka starts (idempotent producer initialization) |
| Appender exits 2 on startup with a schema mismatch | The namespace holds tables from an older schema; use a fresh `ICEBERG_NAMESPACE` |

## Cleaning up

```bash
tests/it/stack.sh down     # containers; Nessie forgets all tables
```

Data files stay in the `telemetrylakeit` bucket. Delete the bucket in the MinIO
console (the `WebUI` address in `/usr/local/var/log/minio.log`) or with the
MinIO client: `brew install minio-mc`, then
`mc alias set local http://localhost:9000 minioadmin minioadmin` and
`mc rb --force local/telemetrylakeit`. `stack.sh up` recreates the bucket.
