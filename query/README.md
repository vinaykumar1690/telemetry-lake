# Query service (`otel-query`)

Searches the Iceberg logs table written by the appender. Apache DataFusion and
iceberg-rust run in the same process; there is no separate engine cluster.
Design: [docs/query-design.md](../docs/query-design.md).

```
crates/logquery      query IR, JSON search frontend, time parsing, cursors (no engine dependency)
crates/planner       field namespace, IR -> DataFusion expressions, search plan + pagination
crates/query-server  HTTP API, Iceberg table source, SQL endpoint, metrics (binary: otel-query)
```

## Build and test

```bash
cd query
cargo build --release          # target/release/otel-query
cargo test --workspace         # unit + HTTP tests over an in-memory table
```

Needs Rust ≥ 1.95. iceberg-datafusion 0.10 pins DataFusion 53 and Arrow 58,
so upgrade those three together. The live test is in
[docs/integration-testing.md](../docs/integration-testing.md) (`tests/it/query_it.py`).

## Run

```bash
export ICEBERG_CATALOG_URI=http://localhost:19120/iceberg/
export ICEBERG_WAREHOUSE=telemetrylake
export S3_ENDPOINT=http://localhost:9000
export S3_ACCESS_KEY=minioadmin S3_SECRET_KEY=minioadmin
./target/release/otel-query     # :8090
```

| Variable | Default | Description |
|----------|---------|-------------|
| `ICEBERG_CATALOG_URI` | (required) | Iceberg REST catalog (same as the appender) |
| `ICEBERG_WAREHOUSE` | `S3_BUCKET` | Warehouse name |
| `ICEBERG_NAMESPACE` / `ICEBERG_TABLE_NAME` | `default` / `logs` | Table to query |
| `S3_ENDPOINT` | (required) | `http(s)://host:port`; a bare `host:port` means http |
| `S3_ACCESS_KEY` / `S3_SECRET_KEY` | (required) | S3 credentials (always used; the catalog's vended config is overridden) |
| `S3_REGION` / `S3_URL_STYLE` | `us-east-1` / `path` | S3 addressing (`path` or `vhost`) |
| `HTTP_PORT` | `8090` | API, health and metrics |
| `QUERY_DEFAULT_LIMIT` / `QUERY_MAX_LIMIT` | `100` / `1000` | Rows per search page |
| `QUERY_MAX_RANGE_HOURS` | `168` | Longest time range of one search |
| `QUERY_TIMEOUT_SECONDS` | `30` | Per query; the scan is cancelled on timeout or client disconnect |
| `QUERY_MAX_CONCURRENT` | `16` | Concurrent queries; more wait up to 2 s, then get 429 |
| `QUERY_SQL_MAX_ROWS` | `100000` | Row cap of `/v1/sql` |
| `QUERY_MEMORY_LIMIT_MB` | `2048` | Shared DataFusion memory pool |
| `QUERY_TARGET_PARTITIONS` | CPU count | DataFusion parallelism per query |
| `TABLE_REFRESH_MS` | `5000` | Maximum age of the cached table metadata; new commits show up within this |
| `RUST_LOG` | `info` | Log filter |

Exit codes: 0 clean shutdown (SIGTERM/SIGINT finishes in-flight queries), 1
configuration error, 2 server error. `/ready` is 503 until the table has
loaded; loading retries with backoff, so the service can start before the
catalog does.

## `POST /v1/search`

```bash
curl -s localhost:8090/v1/search -H 'Content-Type: application/json' -d '{
  "start": "now-1h",
  "filters": {"service.name": "checkout", "attr.http.status_code": ["500", "503"]},
  "grep": "timeout", "grep_ignore_case": true,
  "fields": ["k8s.pod.name"],
  "limit": 50
}'
```

| Key | Meaning |
|-----|---------|
| `start` (required), `end` (default `now`) | RFC 3339, `now`, `now-<n>{s,m,h,d,w}`, or epoch milliseconds. Half-open: `[start, end)` on the record's event time |
| `filters` | `{field: value}` for equality, `{field: [values]}` for IN |
| `where` | Boolean tree, see below |
| `grep`, `grep_ignore_case` | Substring of the body; `%` and `_` are literal |
| `regex` | Regular expression on the body ([Rust regex syntax](https://docs.rs/regex/latest/regex/#syntax), at most 1024 characters) |
| `fields` | Extra fields to return, by field name (each becomes a key in the row) |
| `order` | `desc` (newest first, default) or `asc` |
| `limit` | Rows per page (`QUERY_DEFAULT_LIMIT`, at most `QUERY_MAX_LIMIT`) |
| `cursor` | `next_cursor` of the previous page; send it with the same query |

All conditions (`filters`, `where`, `grep`, `regex`) are ANDed.

`where` nodes: `{"and": [...]}`, `{"or": [...]}`, `{"not": {...}}`, or
`{"field": F, "op": OP, "value": V}` with `OP` one of
`=` `!=` `<` `<=` `>` `>=` `in` `not_in` (`"values": [...]`) `exists`
`not_exists` `contains` `icontains` `prefix` `regex` `iregex`.

Response:

```json
{"rows": [{"timestamp": "2026-10-19T07:18:39.894660Z", "severity_text": "WARN", "body": "...",
           "service_name": "checkout", "resource_attributes": {"k8s.pod.name": "pod-1"},
           "log_attributes": {"http.status_code": "500"}, "_kafka_topic": "otel-logs",
           "_kafka_partition": 2, "_kafka_offset": 81234, "_record_index": 7, "k8s.pod.name": "pod-1"}],
 "next_cursor": "eyJ2Ijox...", "snapshot_id": 4182312937112, "stats": {"rows": 50, "elapsed_ms": 41}}
```

NULL fields are left out. `next_cursor` is absent on the last page.

### Field names

One namespace for every API (and later LogQL). A name resolves, first match wins, to:

1. a column of that exact name (`service_name`, `severity_number`, `_kafka_offset`)
2. an alias: `time` → `timestamp`, `severity`/`level` → `severity_text`, `message`/`msg` → `body`, `deployment.environment.name` → `deployment_environment`
3. the column named like the dotted key with `.` → `_` (`service.name` → `service_name`, `host.name` → `host_name`, and every appender `PROMOTED_ATTRIBUTES` key such as `k8s.pod.name` → `k8s_pod_name`, which then filters on the column and its statistics)
4. an attribute: `log_attributes[name]`, else `resource_attributes[name]`

`attr.<key>` forces the record's attributes; `resource.<key>` forces the
resource (using the extracted column for `service.name`, `host.name`, …).

Attribute values are strings. `=`/`in` compare strings (`500` matches `"500"`);
`<`, `>` etc. with a number compare numerically, and values that are not
numbers never match. A field that is absent never matches a positive
condition, while `!=`, `not_in`, `not_exists` and `not` match it, so a
condition and its negation always split the rows exactly.

### Pagination

Rows are ordered by `(timestamp, _kafka_topic, _kafka_partition, _kafka_offset, _record_index)`,
a unique and stable key that compaction does not change, so pages never skip
or repeat rows, even across timestamp ties. The cursor keeps the first page's
resolved time range (`now-1h` stays the same window). It also names the first
page's Iceberg snapshot, which later pages read while the catalog still has
it. Nessie keeps only the current snapshot, and other catalogs expire old
ones, so later pages usually read the current snapshot. The table is
append-only, so that cannot skip or repeat a row; rows committed since the
first page can appear on later pages when their timestamps fall after the
cursor. A cursor is bound to its query's filters, fields and order: other
queries get 400.

## `POST /v1/sql`

```bash
curl -s localhost:8090/v1/sql -d '{"sql": "SELECT service_name, count(*) AS n FROM logs
  WHERE \"timestamp\" >= now() - INTERVAL '\''1 hour'\'' GROUP BY 1 ORDER BY 2 DESC"}'
```

Read-only DataFusion SQL over the table `logs` (raw column names; map values
as `log_attributes['key']`). DDL, DML and `SET` are rejected, and nothing but
the logs table is reachable. Results stream as NDJSON, or as an Arrow IPC
stream with `Accept: application/vnd.apache.arrow.stream`. At most
`max_rows` (default and cap `QUERY_SQL_MAX_ROWS`) rows are returned. SQL has
no mandatory time range; add one to the `WHERE` clause so partitions and
files are pruned. `EXPLAIN` shows what reaches the Iceberg scan.

## Errors and operations

Errors are `{"error": {"code": ..., "message": ...}}`:

| Status | Code | When |
|--------|------|------|
| 400 | `invalid_query`, `invalid_sql`, `invalid_request` | the request must change |
| 429 | `overloaded` | no query slot within 2 s |
| 503 | `catalog_unavailable`, `table_schema`, `resources_exhausted` | catalog down (with no cached table), the table lacks the row-key columns, or the memory pool is full |
| 504 | `timeout` | over `QUERY_TIMEOUT_SECONDS` |

`/health` (liveness), `/ready` (table loaded), `/metrics` (Prometheus):
`query_requests_total{endpoint,outcome}`, `query_rows_returned_total`,
`query_duration_seconds_sum`, `query_in_flight`, `query_rejected_total`,
`query_table_ready`, `query_table_metadata_age_seconds`,
`query_table_refresh_failures_total`.

If the catalog becomes unreachable, the service keeps answering from the
last loaded metadata and counts the failed refreshes.
