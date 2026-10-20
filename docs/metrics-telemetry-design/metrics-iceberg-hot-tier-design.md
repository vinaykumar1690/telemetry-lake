# Design: Metrics Hot Tier on Iceberg

Status: **Draft for review** · Scope: **design only, no code in this phase**

## 0. Context

Telemetry Lake today ingests **logs** only:

```
OTel Agent → Ingester (HTTP :4318) → Kafka → Appender → DuckDB → Iceberg (S3/MinIO)
```

with two properties we intend to preserve and reuse:

- **Exactly-once**: Kafka offsets are committed only after a successful Iceberg flush (`src/appender/iceberg_appender.cpp`); on startup the appender seeks to `max(committed offset)` per partition.
- **Multi-partition threading**: one worker per Kafka partition, each owning its own DuckDB buffer and offset state (`src/appender/partition_worker.*`, `buffer_manager.*`, `partition_coordinator.*`).

This document extends the same substrate to **metrics**, with a more ambitious goal than logs: serve **PromQL-style queries — `rate()`, `histogram_quantile()`, percentile aggregations — with Iceberg as the source of truth**, i.e. Iceberg backs a *hot* tier, not just cold analytics.

The core bet is to reproduce the classic TSDB architecture — an inverted index (`label=value → series_ids`), then fetch samples by series — on top of Iceberg, using **Kafka-partition-aligned bucketing** so every series maps deterministically to one Iceberg bucket and one appender/writer.

### Locked decisions

1. **Representation**: native/exponential histograms + counters/gauges are the primary path; classic `le`-label histograms supported as a compatibility mapping onto the samples table.
2. **Secondary index substrate**: embedded **RocksDB** (optionally Redis) serves hot postings, **rebuildable from Iceberg** — Iceberg is the source of truth; the KV is a derived cache.
3. This deliverable is the design doc; implementation follows after review.

### Non-goals (v1)

- **Bit-exact PromQL `rate()`/`increase()` boundary extrapolation.** v1 ships a documented approximation (§6); Prometheus's edge extrapolation is a later refinement.
- **Unbounded out-of-order ingestion.** v1 assumes largely in-order samples per series with a bounded lateness window; wide out-of-order support is deferred (§12).
- Recording/alerting rules, full PromQL surface area, and multi-tenant isolation are out of scope here.

---

## 1. Overview & goals

**Goal**: a metrics pipeline that shares the ingester/Kafka/appender/Iceberg spine with logs and answers, at interactive latency:

- Counter queries: `rate(http_requests_total{job="api"}[5m])`
- Histogram quantiles: `histogram_quantile(0.99, rate(http_request_duration_seconds_bucket[5m]))` and native-histogram equivalents.
- Gauge and summary reads.

**Signal scope**: counters, gauges, native (exponential) histograms, classic histograms (compat), summaries.

**Data flow (target)**:

```
OTel Agent
  │  OTLP/HTTP metrics
  ▼
Ingester (:4318)  ── computes series_id, partitions by bucket(N, series_id) ──►  Kafka (N partitions)
                                                                                   │
                                                                                   ▼
                                                                            Appender worker[p]
                                                                            (owns bucket p)
                                                                              │   │
                                                              head block ◄────┘   └───► DuckDB → Iceberg
                                                              (last few min)              metrics_samples
                                                                                          metrics_histograms
                                                                                          series (catalog)
                                                                              │
                                                            postings upsert ──┘──► RocksDB (label=value → Roaring)

Query executor:  PromQL → postings intersect (RocksDB) → series_ids+buckets
                 → DuckDB scan over Iceberg (bucket-pruned, semi-join on series_id)
                 → merge with head block → window-function rate/quantile → result
```

The metrics writer is a **sibling of the logs writer** inside the appender: same per-partition worker, same exactly-once offset discipline, keyed by **bucket = Kafka partition**.

---

## 2. Prior art

The `label=value → series_ids → fetch samples` pattern is the standard TSDB design. Every component below is proven in production; the novelty here is the *composition on Iceberg*.

| System | Inverted index | Sample locality | Takeaway for us |
|---|---|---|---|
| **Prometheus TSDB** | postings lists per label pair, per block | chunks grouped per series | series fingerprint + postings + per-block time scoping |
| **Cortex / Mimir** | label-hash → series/chunk index (now TSDB blocks in object store) | blocks in object storage | index-over-object-store is viable at scale |
| **VictoriaMetrics** | `indexdb` (mergeset/LSM) label=value → TSIDs | sorted by TSID | dense TSID; LSM postings |
| **InfluxDB IOx** ⭐ | tag index over **Parquet + Arrow + DataFusion + object store** | Parquet sorted by series | **closest analog**: series/tag index over columnar files + catalog |
| **Druid** | **Roaring bitmap** inverted indexes on dimensions | segments | Roaring bitmaps for fast intersect/union |
| **Uber M3 / Lucene** | **FST** term dictionary → postings | — | FST for regex/prefix term enumeration |

**InfluxDB IOx** is the closest precedent: it is a rebuild of InfluxDB on Parquet/object storage with a catalog and a tag index — exactly the "series index over columnar files" problem. Iceberg gives us the table format, snapshots, and hidden-partition pruning IOx had to hand-roll.

---

## 3. Series identity

A series is `(__name__, sorted label set)`. All downstream structures key off a stable identity.

**Canonical encoding** (must be identical in ingester and appender, and in any index rebuild):

- Sort labels by key (bytewise). Include `__name__` as a normal label.
- Encode each pair length-prefixed to avoid separator ambiguity: `varint(len(key)) key varint(len(value)) value`, concatenated in sorted order.

**Fingerprint**:

- `series_id = xxhash64(canonical_bytes)` → `int64`.
- **Collision handling**: 64-bit collides at scale (~billions of active series). The `series` catalog (§5) stores the full label set; on any lookup that resolves a `series_id` to labels, verify the labels match. On a genuine fingerprint collision, salt and rehash (store the salt in the catalog). This mirrors Prometheus's approach of never trusting the fingerprint alone.

**Dense ordinal**:

- Roaring bitmaps require dense `uint32`. The catalog assigns a monotonically increasing `series_ordinal` on first sight of a new series. `series_ordinal` is what postings bitmaps contain; `series_id` is what Iceberg scans filter on.

Encoding/fingerprint/ordinal live in a small shared library (proposed `src/common/series_id.*`) used by both the ingester partitioner and the appender.

---

## 4. Kafka ⇄ Iceberg bucket alignment

This is the load-bearing idea that makes Iceberg a viable hot store: **make a series deterministically map to exactly one Iceberg bucket and one writer.**

Iceberg's `bucket[N]` transform is:

```
bucket_N(x) = (murmur3_x86_32(bytes(x)) & 0x7FFFFFFF) % N
```

If the **ingester computes the Kafka partition with the same function on the same `series_id` byte encoding**:

```
kafka_partition(series) = bucket_N(series_id)
```

then for a table declared:

```sql
PARTITIONED BY (days(ts), bucket(N, series_id))
```

we get the alignment chain:

```
Kafka partition p   ⟺   Iceberg bucket p   ⟺   one series shard   ⟺   appender worker[p]
```

**Consequences (all desirable):**

- Each appender worker is the **sole writer** of its buckets → no cross-writer commit contention; slots directly into the existing per-partition exactly-once/offset model.
- A series always lands in one bucket → reading a series touches exactly one bucket → free partition pruning.
- The postings index can be sharded by bucket (each worker owns its buckets' postings) or centralized; either way, query planning can prune buckets before touching Iceberg.

**Constraints to document loudly:**

- `N` is effectively fixed once chosen — changing an Iceberg bucket count requires a table rewrite, and Kafka repartitioning reshuffles keys. **Keep `num_kafka_partitions == N`** (or a clean integer multiple, with a documented fold). **Choose `N` generously up front (256 or 1024).**
- The murmur3 implementation in the ingester (C++) must byte-for-byte match Iceberg's `murmur3_x86_32` over the identical `series_id` serialization. This gets a dedicated property test (§15).

---

## 5. Sample storage & schemas

Three Iceberg tables plus one catalog. All sample/histogram tables share the partition spec and sort order so per-series scans are contiguous.

Shared:
```
PARTITIONED BY (days(ts), bucket(N, series_id))
WRITE ORDERED BY (series_id, ts)     -- critical: makes per-series window scans sequential
```

### 5.1 `metrics_samples` — counters & gauges

```sql
CREATE TABLE metrics_samples (
  series_id   BIGINT,
  ts          TIMESTAMP,      -- UTC, micros
  value       DOUBLE,
  -- exactly-once lineage (mirrors TransformedLogRecord)
  kafka_partition INT,
  kafka_offset    BIGINT
)
PARTITIONED BY (days(ts), bucket(N, series_id))
WRITE ORDERED BY (series_id, ts);
```

Store **raw cumulative** counter values (not deltas): rate windows must be alignable at query time, and boundary extrapolation needs the raw edge samples. Gauges use the same table (no reset handling on read).

Enable a **Parquet bloom filter on `series_id`** for row-group skipping when the query provides a `series_id` set.

### 5.2 `metrics_histograms` — native / exponential

One row per `(series_id, ts)`; sparse buckets stored as Parquet nested columns.

```sql
CREATE TABLE metrics_histograms (
  series_id       BIGINT,
  ts              TIMESTAMP,
  schema          TINYINT,     -- OTel/Prom exponential scale factor
  zero_threshold  DOUBLE,
  zero_count      BIGINT,
  count           BIGINT,      -- total observations
  sum             DOUBLE,
  positive_buckets LIST<STRUCT<index INT, count BIGINT>>,  -- sparse, offset-indexed
  negative_buckets LIST<STRUCT<index INT, count BIGINT>>,
  sketch          BINARY,      -- optional DDSketch for mergeable arbitrary quantiles
  kafka_partition INT,
  kafka_offset    BIGINT
)
PARTITIONED BY (days(ts), bucket(N, series_id))
WRITE ORDERED BY (series_id, ts);
```

`schema` + bucket `index` reconstruct bucket boundaries as `base = 2^(2^-schema)`, `lower(index) = base^index`. Native histograms are counters over `count`/`sum`/per-bucket counts, so `rate()` applies per component before quantile interpolation (§7).

### 5.3 `series` — catalog / rebuild source

```sql
CREATE TABLE series (
  series_ordinal BIGINT,       -- dense, used by Roaring
  series_id      BIGINT,       -- xxhash64 fingerprint
  salt           INT,          -- 0 unless a fingerprint collision forced a rehash
  metric_name    STRING,
  labels         MAP<STRING,STRING>,
  metric_type    STRING,       -- counter|gauge|histogram|summary
  bucket         INT,          -- bucket_N(series_id), denormalized for pruning
  created_at     TIMESTAMP,
  last_seen      TIMESTAMP
)
PARTITIONED BY (bucket);
```

This is the **authoritative source** for both (a) resolving `series_id`/`series_ordinal` ↔ labels and (b) rebuilding the RocksDB postings index from scratch (§9). Because the catalog lives in Iceberg, the entire index is reproducible.

---

## 6. Counters & `rate()`

`rate()`/`increase()` need consecutive samples of the **same series in timestamp order** with counter-reset handling. The `WRITE ORDERED BY (series_id, ts)` layout makes this a sequential scan rather than a shuffle.

Reference DuckDB query (candidate `series_id`s come from the index, §9–10):

```sql
WITH ordered AS (
  SELECT series_id, ts, value,
         LAG(value) OVER (PARTITION BY series_id ORDER BY ts) AS prev
  FROM metrics_samples
  WHERE bucket IN (:buckets)               -- partition pruning
    AND series_id IN (:series)             -- or semi-join; see §10
    AND ts BETWEEN :t0 AND :t1
),
deltas AS (
  SELECT series_id, ts,
         CASE WHEN value >= prev THEN value - prev   -- normal increment
              ELSE value END AS delta                 -- counter reset: assume dropped to 0
  FROM ordered
  WHERE prev IS NOT NULL
)
SELECT series_id,
       SUM(delta) / EXTRACT(EPOCH FROM (MAX(ts) - MIN(ts))) AS rate
FROM deltas
GROUP BY series_id;
```

**Fidelity note (documented v1 approximation):** Prometheus additionally extrapolates the summed increase to the *exact* range boundaries `[t-range, t]` (and clamps at series start). v1 uses the observed first/last samples in-window. This differs slightly at range edges and for sparse series; porting the extrapolation is a scoped follow-up, not a blocker. Staleness handling (§12) also affects edge correctness.

---

## 7. Histograms & percentiles

**Native / exponential (primary).** For `histogram_quantile(q, rate(...))`:

1. Per candidate series, `rate()` the per-bucket counts and `count` over the range (each bucket is a counter — reuse §6, including reset handling).
2. Rebuild the cumulative distribution from the rated sparse buckets (positive/negative + zero bucket) using `schema` to derive boundaries.
3. Find the bucket where the cumulative fraction crosses `q`; interpolate within it.

This is decode + arithmetic over the nested columns — no cross-series data movement beyond the series set already selected.

**Classic `le`-label histograms (compat).** `le` is just another label, so `_bucket{le="..."}`, `_sum`, `_count` are ordinary counter series that live in `metrics_samples`. `histogram_quantile` becomes: `rate()` each `le` bucket series, sort by `le`, linear-interpolate in the crossing bucket. This reuses all counter machinery at the cost of `le`-multiplied cardinality — acceptable as a compatibility path.

**DDSketch (aggregatable / arbitrary quantiles).** The optional `sketch BINARY` column stores a relative-error-bounded, **mergeable** sketch per `(series, ts)`. Unlike classic histograms and summaries, sketches merge across series and time, which makes them the right tool for arbitrary-quantile queries, cross-instance aggregation, and downsampled rollups.

---

## 8. Summaries

Prometheus summaries carry **client-computed** quantiles (`{quantile="0.99"}`) plus `_sum`/`_count`. Storage:

- Quantile streams → gauges in `metrics_samples`.
- `_sum`/`_count` → counters in `metrics_samples`.

The defining limitation — **precomputed quantiles cannot be aggregated across instances** — is inherent to the type, not a storage problem. The doc's guidance: represent summaries faithfully for compatibility, but steer users toward native histograms / DDSketch for server-side, aggregatable percentiles.

---

## 9. Secondary index (postings)

**Logical model**: `(label_name, label_value) → Roaring bitmap of series_ordinals`, **time-scoped per day** (stale series don't bloat query-time intersections — same rationale as Prometheus per-block indexing).

**Physical substrate**: embedded **RocksDB** (optionally Redis for a shared hot cache), **rebuildable from the `series` Iceberg table**.

Key layout:

```
key:   b"p" | day(u16) | label_name | 0x00 | label_value
value: serialized Roaring bitmap of series_ordinals active that day

key:   b"o" | series_ordinal(u32)          value: series_id(i64) | bucket(i32)   (ordinal→id/bucket)
key:   b"t" | label_name | 0x00 | label_value | day   (term existence, for FST/enumeration)
```

- **Term dictionary / regex & prefix matchers**: build an in-memory **FST** (Lucene/VictoriaMetrics/M3 style) over the `t`-keys per `(label_name, day)` so `=~` / `!~` / prefix matchers enumerate matching values, then union their bitmaps.
- **Freshness**: a small **in-memory head** of postings covers series seen since the last RocksDB flush; queries merge head + persisted postings (mirrors the sample head block, §11).
- **Durability & recovery**: RocksDB is a *cache*. On loss/corruption it is rebuilt by scanning `series` (parallelized by `bucket`). This keeps Iceberg the single source of truth.
- **Writer**: the appender worker that owns a bucket upserts postings for new/updated series in that bucket as part of the same flush cycle, so index and samples advance together.

---

## 10. Query flow

For `rate(http_requests_total{job="api", status=~"5.."}[5m])`:

```
1. Parse matchers.
2. Postings lookup (RocksDB, day-scoped to the query range):
     __name__=http_requests_total → bitmap A
     job=api                      → bitmap B
     status=~"5.."  → FST enumerate {500,502,503,...} → union → bitmap C
   candidate_ordinals = A ∩ B ∩ C            (Roaring intersect/union, in memory)
3. ordinals → (series_id, bucket) via the "o" keyspace.
4. bucket set → Iceberg partition pruning.
5. DuckDB scan over metrics_samples:
     WHERE bucket IN (…)
       AND ts BETWEEN t0 AND t1
       AND series_id  ← semi-join against a temp table of series_ids
                         (NOT a literal IN(...) of millions)
   + Parquet bloom filter on series_id skips row groups.
6. Merge with head block (§11).
7. Window-function rate() / histogram_quantile() → PromQL vector.
```

**Large series sets**: never emit an `IN (...)` list of millions. Register the candidate `series_id`s as a temp relation and let DuckDB do a hash **semi-join**; the Parquet `series_id` bloom filter prunes row groups; the sort order bounds min/max per file. These three together keep the scan proportional to matched data, not table size.

---

## 11. Freshness — head + historical

Iceberg commits are batch-oriented, so raw Iceberg alone can't be sub-second fresh. Use the Druid/Pinot/Prometheus split:

- **Head block**: last ~N minutes of samples buffered in the appender worker's DuckDB (reuses the existing `buffer_manager`), plus in-memory head postings for newly seen series.
- **Historical**: flushed to Iceberg every ~1–2 min, then compacted.
- **Query = merge(head, Iceberg)**; **index = merge(head postings, RocksDB)**.

The head is owned by the same worker that owns the bucket, so head reads for a series are local to one worker — the query executor scatters by bucket and gathers.

---

## 12. Hard problems / risks

1. **murmur3 alignment** — the ingester's C++ murmur3 must byte-match Iceberg's `murmur3_x86_32` over the identical `series_id` encoding. Property-tested (§15). If alignment is ever broken, series still land in *a* bucket deterministically; only write-locality with Kafka partitions degrades.
2. **Out-of-order / late data** — breaks per-series sort locality and `rate()` correctness. v1 policy: accept a bounded lateness window into the head block; reject/dead-letter older samples (reuse `dead_letter_queue.*`). Wide OOO (Mimir/VM style) is deferred.
3. **Staleness markers** — PromQL correctness for series that stop reporting needs staleness handling at read time; v1 documents the gap and applies a lookback-delta cutoff.
4. **Cardinality churn & ordinal GC** — `series_ordinal` space grows monotonically; day-scoped postings bound query cost, but long-term ordinal compaction/GC is a maintenance job to design.
5. **Small-file / compaction** — frequent commits on `metrics_samples` create small files; schedule Iceberg compaction and size targets (reuse the logs appender's commit-cadence learnings).
6. **Index rebuild cost** — full rebuild scans `series`; parallelize by `bucket` and cap memory per shard.

---

## 13. Component-by-component implementation sketch

Following the numbered style of `docs/logs-telemetry-initial-design/`:

**13.1 Proto** — `proto/telemetry_wrapper.proto`: add `OTEL_METRICS` to `TelemetryType`. Wrapper stays payload-agnostic (deferred parsing, as with logs).

**13.2 Series-id library** — new `src/common/series_id.{hpp,cpp}`: canonical label encoding, xxhash64 fingerprint, `bucket_N` (murmur3_x86_32), collision/salt handling. Shared by ingester and appender.

**13.3 Ingester partitioner** — `src/ingester/`: parse just enough of OTLP metrics to compute `series_id`, set Kafka partition = `bucket_N(series_id)`. (Full transform still deferred to the appender.)

**13.4 Config** — `src/config.hpp`: extend `AppenderConfig`/`IngesterConfig` (or add `MetricsConfig`) with `METRICS_BUCKETS` (N), `METRICS_HEAD_WINDOW_SECONDS`, `POSTINGS_DB_PATH` (RocksDB) / optional `POSTINGS_REDIS_URI`, following the `fromEnv()` pattern.

**13.5 Transform structs** — parallel to `TransformedLogRecord`: `TransformedSample` and `TransformedHistogram` in new `src/appender/metric_transformer.{hpp,cpp}`, each carrying `kafka_partition`/`kafka_offset` for exactly-once.

**13.6 Metrics writer** — new `src/appender/metrics_appender.{hpp,cpp}` as a sibling of `iceberg_appender`, reusing `partition_worker`/`buffer_manager`; writes `metrics_samples`, `metrics_histograms`, `series`, and upserts postings on flush.

**13.7 Postings index** — new `src/appender/postings_index.{hpp,cpp}` (RocksDB) + `src/query/term_fst.*` (FST) + rebuild-from-`series` routine.

**13.8 Query executor** — new `src/query/promql_executor.*`: matcher parse → postings intersect → DuckDB scan (bucket-pruned, semi-join) → head merge → rate/quantile. (PromQL parser scope TBD; start with a matcher + `rate`/`histogram_quantile` subset.)

---

## 14. Phased implementation order

1. **(a) Foundations** — series-id/bucketing library (§13.2) + Iceberg DDL for `metrics_samples`, `metrics_histograms`, `series` (§5). Proto + config.
2. **(b) Counter path end-to-end** — ingester partitioner → appender writes `metrics_samples` + `series` → DuckDB `rate()` query by explicit `series_id` (no index yet).
3. **(c) Native histograms** — `metrics_histograms` write + `histogram_quantile` decode.
4. **(d) Postings index + query executor** — RocksDB postings, FST term dictionary, matcher→series→scan flow; rebuild-from-Iceberg.
5. **(e) Head-block freshness** — head buffer + head postings + merge on read.
6. **(f) Summaries / DDSketch / rollups** — summary compat, sketch column, downsampled rollup tables.

---

## 15. Verification strategy

**Design-review checkpoints**: schemas are valid against the Iceberg type system; `bucket_N` math is reproducible; the query-flow SQL is valid DuckDB.

**Tests the implementation will carry** (mirroring `tests/test_log_transformer.cpp`, `test_buffer_manager.cpp`, `test_partition_worker.cpp`):

- **series-id**: canonicalization + fingerprint stability across runs; collision/salt path; ordinal assignment.
- **alignment (property test)**: for random label sets, `kafka_partition(series) == iceberg bucket_N(series_id)` — the ingester murmur3 matches Iceberg's.
- **rate()**: correctness including counter resets, against known fixtures (document the extrapolation delta vs Prometheus).
- **native histograms**: encode/decode round-trip; `histogram_quantile` against reference values; classic-`le` compat cross-check.
- **postings**: intersect/union correctness; regex enumeration via FST; **rebuild-from-Iceberg reproduces KV state exactly**.
- **end-to-end**: synthetic OTLP metrics → appender → Iceberg → PromQL query returns expected `rate`/percentile, extending the harness described in `docs/dev-prompts/2026-01-25-end-to-end-integration-test.md`.

---

## 16. Notes

- No source changes in this phase — only this document under `docs/metrics-telemetry-design/`.
- Consistent with `CLAUDE.md`: deferred parsing (wrapper carries raw payload), exactly-once (offsets committed post-flush), and the per-partition threading model are all reused rather than replaced.
