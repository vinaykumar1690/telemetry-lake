# Log query service: design

## Context
The appender writes OTel logs to an Iceberg table, `lake.<ns>.logs`, through Nessie REST on S3/MinIO:
- The table is partitioned by `day(timestamp)`.
- The OTel resource fields are top-level columns (`service_name`, `host_name`, `severity_*`, `trace_id`, …).
- Everything else lives in two `MAP(VARCHAR,VARCHAR)` columns: `resource_attributes` and `log_attributes`.
- Schema: [`src/appender/schema.hpp`](../src/appender/schema.hpp).

We need a separate **query microservice** that supports:
- time filters
- tag filters, on top-level columns *and* on map keys
- grep (substring or regex on `body`)

It should scale elastically, be extensible in code (custom indexes in Puffin or the catalog), and be forkable.

Today the table is written as **one small file per Kafka-partition-day per commit**, with no sort order, no Parquet bloom filters and no compaction ([design.md §9](design.md#9-known-limitations-and-future-work)). Any query engine will be slow on that layout. Fixing the layout is part of this plan.

---

## Status (2026-10-20)

**Phase 5 (distributed execution) is implemented**, together with the parts of phase 3 it depends on. §11 describes the result: the libraries, why DataFusion 55, what our scan and datafusion-distributed each do, and a query's full path. Operating details are in `query/README.md` (Distributed execution); results are in [integration-testing.md](integration-testing.md#distributed-query-execution-2026-10-20).
- **DataFusion 55**, with iceberg-rust and `apache/datafusion-iceberg` pinned by git revision ([§11.2](#112-dependency-versions-why-datafusion-55)).
- **`logscan`**, our own partitioned, serializable Iceberg scan, with a `FileFilter` hook for phase 4's index ([§11.3](#113-logscan-and-datafusion-distributed)).
- **datafusion-distributed 4** with the placement policy of §6: workers are `otel-query --worker`, discovered through a headless Service's DNS (Helm `query-workers`, CPU HPA).
- **Verified by:**
  - an in-process cluster test (results equal local execution for every search shape, pagination, GROUP BY, small scans and no-worker fallback stay local)
  - `DISTRIBUTED=1` runs of `query_it.py` and `compaction_it.py` (all searches match DuckDB with every scan distributed)
  - a benchmark (`tests/bench/bench.py`)

Still open in phases 3 and 4: regex literal extraction, newest-first file ordering with early termination, and the index itself.

**Phase 1 (table layout) is implemented:**
- The compaction job (`compaction/compact.py`, Spark 4.1.3 + Iceberg 1.11.0, Helm `compaction` CronJob) rewrites files older than an hour, sorted by `(service_name, severity_number, timestamp)`, with bloom filters.
- It also prunes and compacts the offsets table and rewrites manifests.
- A Nessie GC CronJob reclaims replaced files, because Nessie disables Iceberg's own GC.
- `PROMOTED_ATTRIBUTES` columns in the appender, which the field resolver's `.`→`_` rule already maps.
- The sort order is passed to each rewrite rather than set on the table, and the target file size is a rewrite option. DuckDB refuses to append to partitioned tables that set `write.target-file-size-bytes`.

Results are in [integration-testing.md](integration-testing.md#compaction-2026-10-19). Building phase 1 also changed query-service cursors: Nessie keeps only the current snapshot, so a cursor whose snapshot is gone continues on the current one. That is exact for an append-only table whose row keys survive compaction.

**Phase 2 (the single-node query MVP) is implemented** in `query/` ([query/README.md](../query/README.md) is the API reference). It is verified by:
- 43 unit and HTTP tests
- a live test, `tests/it/query_it.py`, which loads logs through the real ingester and appender and checks 11 search shapes, pagination and SQL against DuckDB

Where it differs from the plan below:
- **Three crates, not eight.** `logquery` holds the IR, the JSON frontend and cursors; `planner` holds the field resolver, lowering and the search plan; `query-server` holds HTTP, the table source, SQL and metrics. The index and indexer crates come with phase 4.
- **SQL** is HTTP (`POST /v1/sql`) with NDJSON or Arrow IPC output. Flight SQL is deferred. The search API returns one JSON document per page; pages are bounded by `limit`, so it is not streamed.
- **Initially DataFusion 53 with a single-stream scan.** Both have since been replaced (see the phase 5 status above).
- **Elasticity** uses CPU-based HPAs with two warm replicas, for the query pods and the worker pool. KEDA (scaling on in-flight bytes) is still a follow-up. There is no scanned-bytes limit yet; the time-range cap, timeout, memory pool and concurrency limit bound a query.

The integration test also found two appender bugs, both fixed:
- **Pagination tiebreak.** Rows had no stable unique key. The appender now writes `_record_index`, the record's position within its Kafka message.
- **Partition time zone.** DuckDB computed `day(timestamp)` partition values in the machine's time zone, so readers pruned rows whose local date differed from their UTC date. Sessions now run in UTC.

## 1. Engine decision: Apache DataFusion + apache/iceberg-rust (Rust)

| Engine | Iceberg | Interactive latency | Extensible from code | Elastic | Verdict |
|---|---|---|---|---|---|
| **DataFusion + iceberg-rust** | REST catalog, scan planning, partition and stats pruning, delete files (0.8–0.10, 2026). v3 DV reading is in progress | ms-level startup; it is a library inside our process | **Best.** `TableProvider`, `ExecutionPlan`, optimizer rules, UDFs, `Expr` rewriting, all in our own binary | stateless pods; `datafusion-distributed` (Datadog, in production) for scale-out | **Chosen** |
| Trino | most mature | good, but a JVM cluster with a coordinator | plugin SPI; deep scan changes mean forking a large Java codebase | autoscaling workers is possible, scale-to-zero is not | runner-up |
| Spark + Gluten/Velox | Velox reads COW/MOR; full Iceberg is still on the 2026 roadmap | poor. Per-query scheduling and JVM startup make sub-second hard | high, but across three layers (Spark, Gluten, Velox C++) | good for batch | use for **compaction jobs only** |
| StarRocks / Doris | strong | good | fork of a large C++ MPP engine | manual | too heavy to extend |
| DuckDB | already in the appender | very good on one node | extensions are possible, but there is no distributed mode | one node per query | handy for ad-hoc use, not the service |

Why DataFusion:
- The engine is a library, so the query service *is* the engine. Indexes become code paths (a custom scan that asks an index before reading files) rather than plugins negotiating through an SPI.
- Forking means pinning `iceberg-datafusion` (a small crate) instead of maintaining a fork of a whole engine. In the end we replaced its scan with our own and kept only its filter translation ([§11.3](#113-logscan-and-datafusion-distributed)).
- DataFusion 50+ already has **TopK dynamic filters** and, in 55 (Aug 2026), **runtime row-group pruning**. "Newest N matches" is the dominant log query, and those features are exactly what it needs.

---

## 2. Data layout prerequisites (the biggest latency lever, independent of engine)
1. **Compaction job.** A Spark `rewrite_data_files` CronJob (or the iceberg-rust rewrite once it is mature) runs on partitions older than about 1 hour:
   - target files of 256–512 MB
   - **sort order** `(service_name, severity_number, timestamp)`. Per-file min/max on `service_name` then prunes well, and `timestamp` stays tight inside each file.
   - writes **Parquet bloom filters** on `trace_id`, `span_id`, `host_name`, `service_instance_id`
   - runs snapshot expiry and orphan cleanup, which also covers the deferred `_appender_offsets` compaction.
2. **Promoted attributes.** A configurable list of hot map keys (for example `k8s.namespace.name`, `k8s.pod.name`, `http.status_code`) becomes real top-level columns, filled by the appender's transformer ([log_transformer.cpp](../src/appender/log_transformer.cpp), [schema.hpp](../src/appender/schema.hpp)). Columns get statistics and bloom filters; map entries effectively don't.
3. Keep `day(timestamp)` partitioning. Optionally add `hour(timestamp)` once volume justifies it. The Iceberg partition spec can evolve without a rewrite.

---

## 3. Query model: one IR, many frontends (this keeps the query-language choice reversible)

```
 JSON search API ─┐
 LogQL (later) ───┴─► LogQuery IR ─► FieldResolver ─► DataFusion LogicalPlan ─┐
 SQL ────────────────────────────────────► DataFusion SQL planner ─────────────┴─► physical plan (+ index-aware scan)
```

SQL is more general than the IR, so it goes straight to DataFusion's planner.
It still meets the same scan, and the index hooks (which match expression
shapes, not syntax) apply to it as well.

- **`LogQuery` IR** (a small Rust enum algebra):
  - `TimeRange`
  - `Predicate` tree (And/Or/Not over `Field op Value`, with `In`, `Exists`, `Prefix`)
  - `TextMatch` (`Contains`, `Regex`, `Token`) on body or a field
  - pipeline stages: `Parse(json|logfmt)`, `Aggregate(window, group_by, fn)`, `Sort`, `Limit`, `Cursor`
- **`FieldResolver`.** One namespace for all frontends. `service.name` resolves to the column `service_name`. A promoted key resolves to its column. Anything else resolves to `log_attributes['k']`, falling back to `resource_attributes['k']` (with explicit `resource.k` and `attr.k` prefixes available).
- **Index hooks match DataFusion `Expr` shapes** (`col = lit`, `map_extract(col,'k') = lit`, `body LIKE '%x%'`, `regexp_like`), not frontend syntax. SQL users get index acceleration for free.

**Is starting with structured API + SQL a two-way door for LogQL?** Yes, as long as the boundary sits at the IR and the FieldResolver:
- **Stream selectors** `{service="x"}` lower to `Predicate`.
- **Line filters** `|= |~ != !~` lower to `TextMatch`.
- **Parsers** `| json` / `| logfmt` become UDFs on `body`.
- **Metric queries** (`rate`, `count_over_time`, `sum by`) become `date_bin` plus aggregation.
- The only conceptual mismatch is LogQL's *stream* identity: results grouped by the full label set. That is emulated by grouping on the resolved label columns.

What would make it a one-way door, and is therefore avoided:
- frontends generating SQL strings, or
- each frontend having its own field naming.

LogQL is then a parser crate plus a lowering function, and nothing below the IR changes.

---

## 4. Execution strategy for the three filter types
**Pruning stack, applied in order:**
1. **Snapshot and partition pruning** from the time range, using `day(timestamp)`.
2. **Manifest file stats**: per-file min/max of `timestamp`, `service_name` and the promoted columns.
3. **Index lookup** (§5): the files whose blobs could match the tag and grep predicates.
4. **Parquet row-group and page pruning**: stats, bloom filters, page index.
5. **Scan**: late materialization, decoding `body` and the maps only for rows that survive the cheap columns.

**Ordering and limits.** "Newest N" runs over the surviving files ordered by max `timestamp` descending. A TopK operator with a dynamic filter (`timestamp > current Nth`) prunes the remaining files and row groups as soon as N rows are found. Pagination uses a `(timestamp, file, row)` cursor rather than OFFSET.

**Filter types:**
- **Time:** layers 1, 2 and 4, plus `_ingested_at` for "what arrived recently".
- **Tag on a column:** layers 2 and 4; bloom filters for equality on high-cardinality columns.
- **Tag on a map key:** layer 3 (the key=value bloom index) and then a `map_extract` filter. Without the index this is a scan of the map columns inside time-pruned files.
- **Grep:**
  - Extract required literals from the substring or regex, Russ Cox trigram style, and check them against the per-file n-gram index (layer 3).
  - Then use a vectorized `LIKE` or the `regex` crate on `body`.
  - A regex with no extractable literal falls back to scanning the time-pruned files.

---

## 5. Indexing framework (built in from the first version, filled in phases)

**Index blobs.** An `indexer` worker watches new snapshots: compaction output, and optionally appender commits older than X minutes. For each new data file it writes **Puffin** blobs:
- `bloom-kv-v1`: key=value tokens from both maps and the promoted columns
- `ngram-bloom-v1`: 3-grams of `body`, or tokens
- later, optionally, `tantivy-v1`: an inverted index for ranked or phrase search

**Index registry.** An Iceberg table, `_log_index (data_file_path, index_type, puffin_path, blob_offset, blob_length, built_at)`:
- Data file paths are immutable, so entries remain valid in every snapshot that contains the file.
- It needs no catalog extension and no Nessie changes.
- It is queried by the service itself, through DataFusion, at planning time.

Iceberg table-level `statistics` files are not used here, because Iceberg allows only one statistics file per snapshot, so the index would have to be rewritten on every commit. Once the Iceberg secondary-index proposal (Phase 1: Bloom skipping indexes in Puffin, discussed on dev@ in Jan 2026) lands, the registry moves to that metadata.

**Query side.** The index plugs into our scan (`logscan::LogsTableProvider`, §11.3) as a `FileFilter`, applied after iceberg-rust has planned the files and before they are split into partitions:
1. It fetches the index blobs for the candidate files (with an LRU cache on local disk).
2. It drops files that cannot match.
3. The rest are read as usual, locally or on workers.

Unindexed files, the recent ones not yet compacted, are always scanned. **Indexes are advisory**, so correctness never depends on them. Because the scan is ours, no fork of `iceberg-datafusion` is needed for this.

---

## 6. Elasticity and small-query latency (answer to "does distributed hurt small queries?")
No, provided two conditions hold:

1. **Adaptive placement per query** (implemented, defaults below; see §11.4 for where it happens in a query). After pruning (§4 layers 1–3), the planner knows the candidate bytes.
   - Below `QUERY_DISTRIBUTE_MIN_BYTES` (2 GiB), the query runs **locally in the pod that received it**, with no network stages.
   - Above it, one worker task per `QUERY_BYTES_PER_TASK` (1 GiB), capped at the worker count, each over a disjoint, size-balanced set of files.
   - Filter plus TopK plans need only a scatter and a gather (a `NetworkCoalesce`) with no shuffle, so they add one network hop (milliseconds). Datadog designed the project for this: "small queries shouldn't be penalized".
2. **No cold starts on the hot path.**
   - The frontend/coordinator pods have `minReplicas ≥ 2`.
   - The worker pool scales with **KEDA** on queue depth and in-flight scan bytes, keeps a warm minimum, and may scale to zero only off-hours.
   - Spark-style "launch executors per query" is what actually trades latency for scale, and this design avoids it.

**Budget for a query under one day, after compaction:**
- catalog `loadTable`: about 20 ms, cached per snapshot
- manifest list and manifests: cached, keyed by immutable manifest path
- index blobs: cached
- Parquet footers and column chunks: parallel S3 GETs at roughly 30–80 ms per round trip, 2–4 rounds

Total: **under 1 s typical, low seconds worst case.** Without compaction, thousands of tiny files and manifests per day dominate the time, which is why §2 is a prerequisite.

Caches are local disk/RAM per pod:
- table metadata keyed by snapshot
- manifests
- Parquet footers and page indexes
- index blobs
- optionally a hot-file byte cache

A shared Foyer-style cache comes later.

---

## 7. Service shape
- New Rust workspace `query/` in this repo, with these crates:
  - `logquery-ir`
  - `frontend-json`
  - `frontend-sql` (Arrow Flight SQL)
  - `field-resolver`
  - `planner`
  - `indexed-iceberg` (the TableProvider plus index cache)
  - `indexer` (binary)
  - `query-server` (axum HTTP + Flight)
- **API:**
  - `POST /v1/search` takes `{start, end, filters, grep, limit, cursor, fields}` and returns NDJSON or Arrow, streamed.
  - `POST /v1/sql`
  - Flight SQL on `:50051`
  - `/health`, `/ready`, `/metrics`, matching the appender's conventions (`src/appender/health.cpp`)
- **Guardrails:**
  - a mandatory time range, with a configurable maximum span
  - limits on scanned bytes and time per query
  - query cancellation on client disconnect
  - per-tenant concurrency limits, later
- **Deployment:**
  - Helm templates `query-deployment.yaml`, `query-workers.yaml`, a KEDA `ScaledObject` and `indexer-deployment.yaml` in `helm/telemetry-lake`
  - a compaction CronJob (Spark)
  - catalog configuration mirroring the appender's (`ICEBERG_CATALOG_URI`, warehouse, S3 keys, access delegation `none`)

---

## 8. Implementation phases
1. **Layout.**
   - The compaction CronJob (sort order, bloom filters, snapshot expiry).
   - The promoted-attributes config in the appender (`schema.hpp`, `log_transformer.cpp`, `staging_store.cpp` append order, `ensureTable`).
   - Extend `tests/it` with a compaction smoke test.
2. **Query MVP, single node.**
   - The workspace and the IR.
   - FieldResolver.
   - The JSON search API.
   - The SQL endpoint.
   - IcebergTableProvider over Nessie.
   - TopK newest-first and cursor pagination.
   - Metadata caches.
3. **Grep and tag performance without indexes.** Literal extraction for regex, a `map_extract` pushdown into the Parquet predicate where possible, and late-materialization tuning. Benchmark on generated data.
4. **Index framework.** `_log_index`, the indexer with `bloom-kv-v1` and `ngram-bloom-v1`, and `IndexedIcebergTableProvider`. Measure the fraction of files skipped.
5. **Distributed.** `datafusion-distributed` workers, adaptive placement and KEDA autoscaling.
6. **LogQL frontend** (optional): a parser crate lowering to the IR, plus metric queries.

## 9. Risks
- **iceberg-rust gaps** (v3 deletion vectors, some delete handling). Our table is append-only with no deletes, so the exposure is low. Pin versions.
- **`datafusion-distributed` maturity** outside Datadog. Phase 5 is isolated; the fallback is more replicas plus per-query parallelism on bigger nodes.
- **Compaction runtime.** Spark adds a JVM dependency for a batch job only. Revisit when iceberg-rust's rewrite matures.
- **Index lag.** Recent data is unindexed until it is compacted. That data is small and recent, so scanning it is cheap.

## 10. Verification
- **Unit tests:** IR lowering per frontend (the same IR from JSON, SQL and, later, LogQL), FieldResolver, regex literal extraction, and index pruning (with an injected index that must never drop a matching file).
- **Integration:** extend `tests/it/stack.sh`.
  1. Load N million generated logs through the existing ingester and appender.
  2. Run compaction and the indexer.
  3. Run golden queries (time, column tag, map tag, grep, regex, newest-N with cursor). Compare results against DuckDB over the same table as an oracle.
- **Performance:** EXPLAIN ANALYZE shows files and row groups pruned at each layer; p50/p99 latency for one-hour and one-day ranges; a check that small queries never take the distributed path.

## 11. Architecture as built: libraries and the query path

### 11.1 Layers

```
┌──────────────────────────────────────────────────────────────────────────────┐
│ otel-query  (query/)                                                           │
│   logquery    JSON request → query IR, cursors          (no engine dependency) │
│   planner     field names → DataFusion expressions, search plan, pagination    │
│   logscan     our Iceberg scan: plan files, split them, ship them to workers   │
│   query-server  HTTP API, table cache, SQL endpoint, worker mode               │
├──────────────────────────────────────────────────────────────────────────────┤
│ datafusion-distributed   runs one DataFusion plan across many processes        │
├──────────────────────────────────────────────────────────────────────────────┤
│ iceberg-datafusion (apache/datafusion-iceberg)   glue between the two below    │
├───────────────────────────────────────┬──────────────────────────────────────┤
│ DataFusion   SQL parser, planner,     │ iceberg-rust   Iceberg spec in Rust:  │
│ optimizer, execution engine           │ catalogs, metadata, file planning,    │
│                                       │ Parquet reading, storage access       │
├───────────────────────────────────────┴──────────────────────────────────────┤
│ Apache Arrow (arrow-rs)   columnar in-memory format; RecordBatch everywhere   │
│ parquet (arrow-rs)        Parquet reader and writer                           │
│ OpenDAL                   S3 / GCS / Azure / local-file access                │
└──────────────────────────────────────────────────────────────────────────────┘
```

**DataFusion** is a query engine embedded as a library, not a server. It provides every stage of a query engine:

| Stage | What it does |
|---|---|
| SQL parser (`sqlparser-rs`) | text to a syntax tree |
| SQL to logical plan (`SqlToRel`) | resolves names and types into relational operators (scan, filter, sort, limit, projection, aggregate, join) |
| Logical optimizer | rewrite rules, e.g. push filters and projections towards the scan |
| Physical planner | executable operators (`FilterExec`, `SortExec`, `AggregateExec`, …), the number of parallel partitions, and asks each table for its scan operator |
| Physical optimizer | e.g. Sort + Limit becomes a TopK; adds repartitioning for parallelism |
| Execution | each operator produces async streams of Arrow `RecordBatch`es, one per partition, on Tokio threads |

Data enters DataFusion through the `TableProvider` trait: "give me an operator that scans you, with these columns and filters". Everything above the scan is generic.

**Apache Arrow** is the in-memory columnar format. A `RecordBatch` holds a few thousand rows stored column by column. DataFusion, iceberg-rust, the Parquet reader and our JSON output all exchange Arrow batches, which is why their versions must line up (§11.2).

**iceberg-rust** (crate `iceberg`) is the Apache project's implementation of the Iceberg spec. It is not a query engine; it knows what an Iceberg table is and how to read or change one:
- **Metadata model:** table metadata, schemas with field IDs, partition specs, snapshots, manifest lists, manifests, data and delete files.
- **Catalog clients:** `iceberg-catalog-rest` speaks the REST protocol to Nessie (`loadTable`, `transactions/commit`, …). There are also memory, Glue and SQL catalogs.
- **Storage (`FileIO`):** reads and writes `s3://` and `file://` paths through OpenDAL, configured with our endpoint and keys.
- **Scan planning:** given a snapshot and a filter, it reads the manifest list and manifests. It prunes by partition value (`day(timestamp)`) and by per-file column min/max statistics, and returns `FileScanTask`s. A task is one file, or part of one, plus the columns to read, the bound predicate and any delete files.
- **Arrow reader:** reads Parquet for those tasks. It matches columns by Iceberg field ID rather than by name, so added or renamed columns stay correct. It skips row groups using statistics and bloom filters, applies delete files, and outputs Arrow batches.
- **Writers and transactions:** appends, rewrites and commits.

It serves projects that need Iceberg without a JVM: Rust databases and streaming engines (for example RisingWave and Databend), the Lakekeeper catalog, PyIceberg's Rust-accelerated core, and the DataFusion integration. In this system only the query service uses it, for three jobs:
1. talking to Nessie;
2. planning which files to read;
3. reading them.

The appender writes through DuckDB's C++ Iceberg extension, and compaction uses Java Iceberg inside Spark.

**iceberg-datafusion** is the glue that makes an Iceberg table look like a DataFusion table. It provides:
- Iceberg `TableProvider`s and a scan operator;
- `convert_filters_to_predicate`, which translates DataFusion expressions into iceberg-rust predicates that can prune files;
- `INSERT INTO` support and metadata tables (`logs$snapshots`, …).

It used to live in the iceberg-rust repository. In September 2026 both projects voted to move it into the DataFusion project as `apache/datafusion-iceberg`. The reason was release coupling: the glue must follow DataFusion's breaking major release every month or two, while iceberg-rust releases on its own schedule.

We now use only part of it:
- its filter translation;
- an error-conversion helper;
- its write path, but only to build test tables.

Its scan was replaced by `logscan` (§11.3).

### 11.2 Dependency versions: why DataFusion 55
A Rust binary can link two major versions of a crate, but their types are unrelated. `ExecutionPlan` from DataFusion 53 is not the trait of the same name in 55, and a `RecordBatch` from Arrow 58 is not one from Arrow 59.

datafusion-distributed wraps our scan operator, serializes it and runs it on workers. That only compiles if every library touching the plan and its batches was built against the same DataFusion and Arrow:
- datafusion-distributed;
- iceberg-datafusion;
- iceberg-rust's Arrow reader;
- our code.

| Crate | DataFusion / Arrow |
|---|---|
| datafusion-distributed 4.0 (Aug 2026, current) | 55 / 59 |
| datafusion-distributed 2.x–3.x | 54 / 58 |
| datafusion-distributed 1.0 (Apr 2026) | 53 / 58 |
| iceberg-datafusion 0.10.1 (newest on crates.io) | 53 / 58 |
| iceberg-rust 0.10.1 (newest on crates.io) | Arrow 58 |
| iceberg-rust main, datafusion-iceberg main | 55 / 59 |

The current datafusion-distributed line, whose leaf-splitting handlers we use, needs DataFusion 55. DataFusion 55 means Arrow 59, and only the unreleased main branches of iceberg-rust and datafusion-iceberg are on Arrow 59. Staying on datafusion-distributed 1.0 with DataFusion 53 would have kept us on an outdated line and given up DataFusion 55's runtime row-group pruning.

`query/Cargo.toml` therefore pins git revisions:
- `apache/datafusion-iceberg` at `b641e5eb`;
- iceberg-rust at `5b82ab99`, the commit datafusion-iceberg itself uses, so the two agree.

`Cargo.lock` records them, which keeps builds (including the Docker image) reproducible. Our code compiled against DataFusion 55 unchanged.

The cost is unreleased code. One upstream bug was hit: DataFusion `INSERT` into a *partitioned* Iceberg table with MAP columns fails. Reads are unaffected and production data is written by DuckDB, so only the test fixture works around it. Switch the pins to releases when both projects publish versions on DataFusion 55.

### 11.3 logscan and datafusion-distributed
They solve different halves of the problem.

**`logscan` decides what to split and how to describe a piece.** This is specific to Iceberg and to our table. The upstream scan read every file in one stream, with nothing to split or serialize.
- `LogsTableProvider` plans the files through iceberg-rust (partition and statistics pruning), applies the optional `FileFilter` (§5), and bin-packs the files by size into up to `target_partitions` partitions.
- `LogsScanExec` reads each partition with iceberg-rust's Arrow reader. `split(n)` turns it into n disjoint scans with equal partition counts, one per worker task, dealing files round-robin so tasks stay balanced.
- `LogsScanCodec` serializes a scan as its `FileScanTask`s. Each task carries the file path, byte range, columns, predicate and delete files, so a worker needs S3 access and never calls the catalog.

**datafusion-distributed runs the pieces elsewhere and combines the results.** Our part is about 300 lines of handlers and codec; it supplies the distributed runtime:

| What it does | Without it, we would write… |
|---|---|
| **Stage planning:** cuts the physical plan where data must move between machines and inserts network operators: `NetworkCoalesceExec` (many tasks to one), `NetworkShuffleExec` (repartition by key, for GROUP BY and joins), `NetworkBroadcastExec` | a distributed query planner |
| **Task counts:** asks each leaf how many tasks it wants (our handler applies §6's policy), reconciles leaves in a stage, caps at the worker count | the scheduling policy |
| **Routing:** assigns tasks to worker URLs from our resolver (the headless Service's DNS) | placement and discovery plumbing |
| **Plan shipping:** serializes each stage with datafusion-proto plus our codec and sends it over gRPC | a protocol and plan serializer |
| **Worker runtime:** a gRPC service that builds a session per task (ours adds the codec and the memory limit), executes it and streams Arrow batches back with Arrow Flight encoding; task-state cache and cancellation | the worker server |
| **Pushing work down:** e.g. TopK runs on each worker before the network, so only the best N rows per task cross it; partial aggregation runs below shuffles | optimizer rules |
| **Observability:** `EXPLAIN ANALYZE` across stages and tasks, with timings, bytes sent and network latency | distributed metrics |

The same DataFusion plan runs locally or distributed, which is why queries below the threshold simply skip the distributed stage.

### 11.4 A query, end to end

**Processes:**

```
                         ┌───────────────────────────── query pod (coordinator) ──────────────────────────────┐
 client ── HTTP :8090 ──▶│ axum ─▶ logquery ─▶ planner ─▶ DataFusion planning ─▶ execution ─▶ JSON response │
                         │                         │            │                     ▲                          │
                         │                         │      LogsTableProvider           │ Arrow batches            │
                         │                         │            │                     │ (gRPC, Arrow Flight)      │
                         └─────────────────────────┼────────────┼─────────────────────┼──────────────────────────┘
                                                   │            │ iceberg-rust        │
                              cached Table ◀───────┘            │ REST: loadTable     │  only for large scans
                              (IcebergSource, ≤5 s old)          ▼                     │
                                                          ┌──────────┐        ┌───────┴──────────────────┐
                                                          │  Nessie  │        │ worker pods (--worker)    │
                                                          └──────────┘        │ LogsScanExec + TopK       │
                                                                              └───────┬──────────────────┘
                                         manifests, Parquet (S3 range reads)          │
                                  MinIO/S3 ◀──────────────────────────────────────────┘ (and from the coordinator
                                                                                          when it runs locally)
```

**Steps of a search**, for example `{"start":"now-7d","filters":{"k8s.pod.name":"pod-7"},"grep":"Timeout","limit":100}`. The IR and field resolution (steps ③ and ⑤) are as described in §3, and the pruning layers in step ⑨ are those of §4.

```
 ① HTTP                axum handler (query-server/api.rs): concurrency limit (429), timeout
 ② Parse & validate    logquery::json::SearchRequest: times resolved, limits, cursor decoded
 ③ Query IR            LogQuery { time, filter: And[pod = "pod-7", body contains "Timeout"],
                                  order: Desc, limit: 100 }
 ④ Table               IcebergSource: cached iceberg-rust Table (reloaded from Nessie
                       at most every 5 s), wrapped as logscan::LogsTableProvider
 ⑤ Field resolution    planner::FieldResolver: column if promoted, else map lookup
 ⑥ Expressions         planner::filters::lower → DataFusion Expr
                       (+ the cursor's key condition on later pages)
 ⑦ Logical plan        planner::SearchPlanner::frame:
                         Projection ← Limit(101) ← Sort(row key DESC) ← Filter ← TableScan
                       (101 = one extra row to know whether a next page exists)
 ⑧ Logical optimizer   DataFusion: filter and projection pushed into the TableScan
 ⑨ Physical planning   DataFusion calls LogsTableProvider::scan(columns, filters, limit):
                         a. convert_filters_to_predicate        (iceberg-datafusion)
                         b. table.scan()…plan_files()           (iceberg-rust: §4 layers 1–2)
                         c. FileFilter, then bin_pack           (logscan)
                         → LogsScanExec { groups of FileScanTasks }
                       physical optimizer: Sort + Limit → TopK, FilterExec, repartitioning
 ⑩ Distribute?         only with QUERY_WORKERS (datafusion-distributed): our handler applies
                       §6's policy. Small: unchanged, runs here. Large: split the scan into
                       disjoint tasks, cut the plan (workers: scan → filter → TopK;
                       coordinator: NetworkCoalesceExec → merge → projection), encode the
                       stages (datafusion-proto + LogsScanCodec), send them over gRPC
 ⑪ Execution           LogsScanExec → iceberg-rust ArrowReader → FileIO (OpenDAL) → S3 range
                       reads; row groups skipped by stats and bloom filters; columns by field
                       id → FilterExec (exact, incl. LIKE) → TopK per partition →
                       (NetworkCoalesceExec) → SortPreservingMergeExec → 101 rows
 ⑫ Page & cursor       SearchPlanner::run: 100 rows; the 101st gives next_cursor
 ⑬ Response            arrow-json → {"rows", "next_cursor", "snapshot_id", "stats"}
```

**A distributed plan** (`EXPLAIN ANALYZE` from the benchmark: a week of data on two workers; plain `EXPLAIN` shows the plan before distribution):

```
DistributedExec                                   ← coordinator: sends stages, gathers results
  ProjectionExec
    SortPreservingMergeExec [timestamp DESC], fetch=100
      [Stage 1] => NetworkCoalesceExec: input_tasks=2     ← network boundary
  ┌── Stage 1  tasks=2 ─────────────────────────────────── runs on the workers
  │ SortExec: TopK(fetch=100)                   ← only 100 rows per task cross the network
  │   FilterExec
  │     DistributedLeafExec
  │       t0: LogsScanExec files=4 bytes=323 MB
  │       t1: LogsScanExec files=4 bytes=255 MB
  └──────────────────────────────────────────────────────
```

Below the threshold, the same query has no `DistributedExec` and no stage, and runs entirely in the query pod.

**SQL** joins the same path at step ⑦. sqlparser-rs and `SqlToRel` build the logical plan, with `logs` resolved to `LogsTableProvider`. Steps ⑧–⑪ follow; aggregations get `NetworkShuffleExec` when distributed. Results stream as NDJSON or Arrow IPC, with no paging. The IR and the field resolver are skipped, but pruning, partitioning and distribution all apply.

**Which library does what:**

| Step | Component | Library |
|---|---|---|
| HTTP, admission | `api.rs` | axum, tokio |
| JSON → IR, cursors | `logquery` | serde |
| Table metadata from Nessie | `IcebergSource` | iceberg-rust, iceberg-catalog-rest |
| Field names, expressions, search plan | `planner` | DataFusion (Expr / DataFrame) |
| SQL parsing | `sql.rs` | DataFusion (sqlparser-rs) |
| Optimizers, physical planning, TopK | — | DataFusion |
| Filter translation to Iceberg | `LogsTableProvider` | iceberg-datafusion |
| File planning and pruning | `LogsTableProvider` | iceberg-rust |
| Partitioning files, splitting for workers | `logscan` | ours |
| Parquet reading, field-ID mapping | `LogsScanExec` | iceberg-rust → parquet → OpenDAL |
| Stages, routing, gRPC, worker runtime | `distributed.rs` handlers | datafusion-distributed (tonic, Arrow Flight) |
| Serializing the scan | `LogsScanCodec` | ours, on datafusion-proto |
| Filter, TopK, merge, aggregation | — | DataFusion operators |
| JSON output | `output.rs` | arrow-json |

## References
- [Apache Iceberg Rust 0.8.0](https://iceberg.apache.org/blog/apache-iceberg-rust-0.8.0-release/), [0.9.0](https://iceberg.apache.org/blog/apache-iceberg-rust-0.9.0-release/), [0.10.0](https://iceberg.apache.org/blog/apache-iceberg-rust-0.10.0-release/) releases
- [Apache DataFusion 55.0.0](https://datafusion.apache.org/blog/2026/08/25/datafusion-55.0.0): runtime row-group pruning
- [Why the Iceberg DataFusion integration is moving to Apache DataFusion](https://datalakehousehub.com/blog/datafusion-iceberg-moves-to-apache-datafusion) / [apache/datafusion-iceberg](https://github.com/apache/datafusion-iceberg)
- [Distributed DataFusion (Datadog)](https://www.datadoghq.com/blog/engineering/distributed-datafusion/) / [datafusion-contrib/datafusion-distributed](https://github.com/datafusion-contrib/datafusion-distributed)
- [DataFusion Ballista 54.0.0](https://datafusion.apache.org/blog/2026/07/12/datafusion-ballista-54.0.0): the alternative distributed runtime
- [Gluten 2026 roadmap](https://github.com/apache/gluten/issues/11827): Iceberg support for the Velox backend
- [Iceberg dev@: Secondary Indexes Phase 1, Bloom filter skipping index in Puffin](https://lists.apache.org/thread/fzqk3jjf0xpj5m4cfqb3v4c65p0t04ty)
- [Puffin-backed vector indexes on Iceberg snapshots (arXiv 2606.04196)](https://arxiv.org/html/2606.04196)
- [OpenObserve architecture](https://openobserve.ai/docs/architecture/): DataFusion + Parquet + Tantivy for logs
- Russ Cox, [Regular Expression Matching with a Trigram Index](https://swtch.com/~rsc/regexp/regexp4.html)
