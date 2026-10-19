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
- Forking means pinning `iceberg-datafusion` (a small crate) instead of maintaining a fork of a whole engine.
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
 SQL (Flight SQL) ─┼─► LogQuery IR ─► FieldResolver ─► DataFusion LogicalPlan ─► physical plan (+ index-aware scan)
 LogQL (later) ───┘
```

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

**Query side.** `IndexedIcebergTableProvider` wraps `iceberg-datafusion`'s `IcebergTableProvider`:
1. It plans files through iceberg-rust.
2. It fetches the index blobs for the candidate files (with an LRU cache on local disk).
3. It drops files that cannot match.
4. It hands the rest to the Parquet scan.

Unindexed files, the recent ones not yet compacted, are always scanned. **Indexes are advisory**, so correctness never depends on them.

**Fork policy.** Depend on upstream crates. Fork `iceberg-datafusion` (vendored as a git dependency) only if its scan-task API can't take an external file filter. Today it plans `FileScanTask`s that we can filter before execution.

---

## 6. Elasticity and small-query latency (answer to "does distributed hurt small queries?")
No, provided two conditions hold:

1. **Adaptive placement per query.** After pruning (§4 layers 1–3), the planner knows the candidate bytes.
   - Below a threshold (for example 2 GB, or ≤ 64 files), the query runs **locally in the pod that received it**, with no network stages.
   - Above it, `datafusion-distributed` splits the leaf scan across workers. Filter plus TopK plans need only a scatter and a gather (a `NetworkCoalesce`) with no shuffle, so they add one network hop (milliseconds). Datadog designed the project for this: "small queries shouldn't be penalized".
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

## References
- [Apache Iceberg Rust 0.8.0](https://iceberg.apache.org/blog/apache-iceberg-rust-0.8.0-release/), [0.9.0](https://iceberg.apache.org/blog/apache-iceberg-rust-0.9.0-release/), [0.10.0](https://iceberg.apache.org/blog/apache-iceberg-rust-0.10.0-release/) releases
- [Apache DataFusion 55.0.0](https://datafusion.apache.org/blog/2026/08/25/datafusion-55.0.0): runtime row-group pruning
- [Distributed DataFusion (Datadog)](https://www.datadoghq.com/blog/engineering/distributed-datafusion/) / [datafusion-contrib/datafusion-distributed](https://github.com/datafusion-contrib/datafusion-distributed)
- [DataFusion Ballista 54.0.0](https://datafusion.apache.org/blog/2026/07/12/datafusion-ballista-54.0.0): the alternative distributed runtime
- [Gluten 2026 roadmap](https://github.com/apache/gluten/issues/11827): Iceberg support for the Velox backend
- [Iceberg dev@: Secondary Indexes Phase 1, Bloom filter skipping index in Puffin](https://lists.apache.org/thread/fzqk3jjf0xpj5m4cfqb3v4c65p0t04ty)
- [Puffin-backed vector indexes on Iceberg snapshots (arXiv 2606.04196)](https://arxiv.org/html/2606.04196)
- [OpenObserve architecture](https://openobserve.ai/docs/architecture/): DataFusion + Parquet + Tantivy for logs
- Russ Cox, [Regular Expression Matching with a Trigram Index](https://swtch.com/~rsc/regexp/regexp4.html)
