# Observability

How to watch the ingester, the appender and Kafka: the metrics each service exposes, what the lag numbers mean, the PromQL behind the dashboards, the log format, and the monitoring stack the Helm chart deploys.

## Overview

```
ingester :9464/metrics ─┐                            ┌─ Grafana (dashboards, alerts view)
appender :8080/metrics ─┤                            │
query    :8090/metrics ─┼── Prometheus (rules) ──────┤
kafka    :7071 (JMX)   ─┤                            │
kafka-exporter :9308   ─┘                            │
                                                     │
every pod's stdout ── OTel Collector (DaemonSet) ── Loki
```

- **Metrics.** Both C++ services use [prometheus-cpp](https://github.com/jupp0r/prometheus-cpp) (core library only), served by Crow. They expose counters, not per-second gauges: rates come from `rate()` in Prometheus.
- **Logs.** Both services write one JSON object per line to stdout (`LOG_FORMAT=json`). The collector ships every pod's output in the namespace to Loki, including Kafka, Nessie and MinIO.
- **Kafka.** Kafka is observed from three sides:
  - client statistics: librdkafka's `statistics.interval.ms`, exported as `rdkafka_*`;
  - broker-side group lag and offsets: kafka-exporter;
  - broker internals: the JMX exporter agent.

The shared code is in `src/observability/`:

| File | Contents |
|------|----------|
| `log.hpp` | The logger |
| `metrics.hpp` | The registry, `build_info` and the process metrics |
| `kafka_stats.hpp` | librdkafka statistics → `rdkafka_*`, plus the log and error callback bridge |
| `crow_log.hpp` | Routes Crow's own logs to the logger |

## Configuration

| Variable | Service | Default | Description |
|----------|---------|---------|-------------|
| `METRICS_PORT` | ingester | `9464` | Port of `/metrics`. It is separate from the OTLP port, which the UI's nginx proxies publicly. |
| `HEALTH_PORT` | appender | `8080` | Port of `/metrics`, alongside `/health`, `/ready` and `/flush`. |
| `KAFKA_STATS_INTERVAL_MS` | both | `15000` | librdkafka statistics interval. `0` disables the `rdkafka_*` metrics and the appender's commit lag. |
| `LOG_FORMAT` | both | `text` on a terminal, `json` otherwise | Log line format. |
| `LOG_LEVEL` | both | `info` | One of `debug`, `info`, `warn`, `error`. |

## Lag: three different numbers

| Metric | Measures | Source |
|--------|----------|--------|
| `appender_kafka_commit_lag_messages{partition}` | Messages in Kafka that are **not yet in Iceberg**: the high watermark minus the next offset after the last durable offset (or minus the low watermark, if retention has already deleted older messages). **This is the lag that matters.** | Watermarks from librdkafka statistics; durable offsets from the commit protocol |
| `rdkafka_consumer_lag_messages{partition}` | Messages **not yet read**: the high watermark minus the consumer position. It is about 0 while the appender keeps up, even though the data only becomes durable at the next commit. | librdkafka statistics. librdkafka's own `consumer_lag` counts from the committed offset, so it is not used. |
| `kafka_consumergroup_lag{consumergroup="otel-appender"}` | The group's committed offsets, as the broker sees them. These are the appender's *hints*, written after each Iceberg commit, so this lag is at least the true lag. | kafka-exporter. **It is the only lag still reported when the appender is down.** |

The commit lag saw-tooths between 0 and roughly one commit's worth of messages (`BUFFER_TIME_SECONDS` / `BUFFER_SIZE_MB`). A lag that keeps rising means the appender is falling behind.

**Freshness** is `time() - appender_last_committed_message_timestamp_seconds`: the age of the newest committed message, using its Kafka timestamp, which is set when the ingester produced it. It keeps growing while no data arrives, so alert on commit lag instead.

`appender_end_to_end_latency_seconds` is a histogram of the time from each committed buffer's oldest Kafka timestamp to its commit, i.e. the time from ingestion until the data can be queried.

## Ingester metrics (`:9464/metrics`)

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `ingester_http_requests_total` | counter | `route`, `code` | Requests by route and status code (`/v1/logs`, `/health`, `/ready`) |
| `ingester_http_request_duration_seconds` | histogram | `route` | Time to handle `POST /v1/logs` after Crow has read the body |
| `ingester_http_requests_in_flight` | gauge | | `POST /v1/logs` requests being handled |
| `ingester_http_received_bytes_total` | counter | `encoding` | Request body bytes as received (`identity`, `gzip`) |
| `ingester_payload_bytes_total` | counter | `wire` | Accepted payload bytes after decompression, i.e. the volume written to Kafka (`protobuf`, `json`) |
| `ingester_payload_size_bytes` | histogram | | Payload size after decompression |
| `ingester_requests_rejected_total` | counter | `reason` | Rejects by reason. The reasons are listed below. |
| `ingester_kafka_produced_messages_total` / `_bytes_total` | counter | | Messages accepted by the producer |
| `ingester_kafka_delivery_failures_total` | counter | `error` | Messages librdkafka gave up on: acknowledged over HTTP but lost. Alerted on. |
| `ingester_kafka_delivery_latency_seconds` | histogram | | Time from `produce()` to the broker acknowledgement (`rd_kafka_message_latency`) |
| `ingester_kafka_in_flight_messages` | gauge | | Messages produced but not yet acknowledged (backpressure starts at `MAX_IN_FLIGHT`) |

The reject reasons are:
- `too_large` and `too_large_decompressed`
- `unsupported_media_type` and `unsupported_encoding`
- `bad_gzip`
- `queue_full`, `queue_retryable` and `queue_too_large`
- `produce_error`

The ingester does not parse payloads, so log events per second are counted by the appender (`appender_rows_staged_total`).

## Appender metrics (`:8080/metrics`)

The metric names that existed before (`appender_messages_total`, `appender_rows_staged_total`, `appender_commits_total`, ...) are unchanged.

**Throughput and volume**

| Metric | Meaning |
|--------|---------|
| `appender_rows_staged_total` | Log records decoded and staged: **log events per second** |
| `appender_rows_committed_total` | Log records committed to Iceberg |
| `appender_messages_total` / `appender_poison_messages_total` | Kafka messages processed, and those sent to the DLQ |
| `appender_kafka_consumed_bytes_total` / `appender_committed_payload_bytes_total` | Payload bytes processed and committed |
| `appender_pipeline_process_duration_seconds` | Decode, transform and stage time per message (histogram) |
| `appender_dlq_sent_bytes_total`, `appender_dlq_failures_total` | DLQ volume and failed DLQ deliveries |

**Sealing, commits and data files.** A *sealed buffer* is one partition's staging generation, handed to a commit. The Parquet data files are written by DuckDB at `COMMIT`. Their count is read from the logs table's snapshot summary (`added-data-files`, one catalog `loadTable` per commit), so these are real file counts, not estimates.

| Metric | Meaning |
|--------|---------|
| `appender_buffers_sealed_total`, `appender_sealed_buffer_rows`, `appender_sealed_buffer_bytes` | Sealed buffers, and their size (histograms) |
| `appender_commits_total`, `appender_commit_failures_total`, `appender_commit_consecutive_failures` | Commit cycles |
| `appender_commit_attempts_total{outcome}` | Attempts by outcome: `ok`, `retryable`, `indeterminate`, `fatal`, `interrupted` (by a revoke), `verify_failed` |
| `appender_commit_landed_after_error_total` | Commits that reported an error but had landed (found by the offsets check) |
| `appender_commit_duration_seconds`, `appender_commit_buffers`, `appender_commit_partitions` | Per-commit histograms |
| `appender_data_files_committed_total`, `appender_data_files_per_commit` | Parquet data files added to the logs table |
| `appender_table_data_files` | Data files in the logs table after the last commit. It grows until compaction, and shows small-file pressure. |
| `appender_commit_files_unknown_total` | Commits whose snapshot was not ours, because another writer (for example compaction) committed in between |
| `appender_offsets_maintenance_total{outcome}` | Offsets table pruning |

DuckDB's writer does not record `added-files-size` in the summary, and listing the data files would read every manifest. So the appender does not report the bytes of the Parquet it writes; `appender_committed_payload_bytes_total` is the input side.

**Lag, freshness, rebalances and resources**

| Metric | Meaning |
|--------|---------|
| `appender_kafka_commit_lag_messages{partition}` | See [Lag](#lag-three-different-numbers) |
| `appender_last_committed_message_timestamp_seconds{partition}` | Kafka timestamp of the newest committed message |
| `appender_end_to_end_latency_seconds` | Time from ingestion to commit (histogram) |
| `appender_assigned_partitions`, `appender_paused_partitions`, `appender_parked_messages` | Assignment and backpressure |
| `appender_rebalance_partitions_total{type}`, `appender_revoke_fence_duration_seconds` | Rebalances, and how long revokes waited for in-flight commits |
| `appender_staged_bytes`, `appender_staged_bytes_cap`, `appender_backpressure_wait_seconds_total`, `appender_pipeline_queue_bytes{worker}` | Staging and backpressure |
| `appender_duckdb_memory_bytes{tag}`, `appender_duckdb_temp_bytes` | DuckDB buffer-manager memory, and bytes spilled to disk |
| `appender_seconds_since_last_commit`, `appender_last_commit_duration_seconds`, `appender_fatal` | Health |

## Shared metrics

Both services export these:
- `telemetry_lake_build_info{service,version}`
- `process_cpu_seconds_total`, `process_resident_memory_bytes`, `process_open_fds`, `process_threads`, `process_start_time_seconds`

**librdkafka statistics (`rdkafka_*`)** are labeled by `client`:
- `ingester`: the ingester's producer
- `appender-consumer`: the appender's consumer
- `appender-dlq`: the DLQ producer

They cover:
- **Queues:** `rdkafka_producer_queue_messages` / `_bytes`, `rdkafka_reply_queue_ops`
- **Traffic:** `rdkafka_tx_*` / `rdkafka_rx_*` requests, bytes and messages
- **Per broker:** `rdkafka_broker_up`, RTT avg and p99, producer queue latency p99, outbuf/waitresp depth, timeouts, errors, retries, connects and disconnects. The consumer's RTT includes the fetch long-poll.
- **Per topic:** batch size and messages per batch
- **Per partition:** the consumer's lag, watermarks, position, committed offset and fetch queue; the producer's queued and in-flight messages
- **Consumer group:** `rdkafka_cgrp_up`, rebalances, assignment size

The internal partition `-1` and logical brokers are skipped, and so are offsets that are not known yet (`-1001`).

## PromQL for the throughput numbers

| Number | Query |
|--------|-------|
| HTTP requests/s | `sum(rate(ingester_http_requests_total{route="/v1/logs"}[5m]))` |
| Rejected requests/s by reason | `sum by (reason) (rate(ingester_requests_rejected_total[5m]))` |
| Ingest volume (bytes/s) | `sum(rate(ingester_payload_bytes_total[5m]))` |
| gzip ratio | `sum(rate(ingester_payload_bytes_total[5m])) / sum(rate(ingester_http_received_bytes_total[5m]))` |
| Log events/s | `sum(rate(appender_rows_staged_total[5m]))` |
| Committed log events/s | `sum(rate(appender_rows_committed_total[5m]))` |
| Appender volume (bytes/s) | `sum(rate(appender_kafka_consumed_bytes_total[5m]))` |
| Buffers sealed/min | `sum(rate(appender_buffers_sealed_total[5m])) * 60` |
| Data files committed/min | `sum(rate(appender_data_files_committed_total[5m])) * 60` |
| Commit lag | `sum(appender_kafka_commit_lag_messages)` |
| Freshness | `time() - max(appender_last_committed_message_timestamp_seconds)` |
| End-to-end p99 | `histogram_quantile(0.99, sum by (le) (rate(appender_end_to_end_latency_seconds_bucket[5m])))` |

## Logs

One line per event, with the same fields in both formats:

```json
{"ts":"2026-10-23T22:35:26.533Z","level":"info","service":"otel-appender","component":"committer","msg":"Iceberg commit","rows":5000,"bytes":585000,"buffers":3,"partitions":3,"data_files":"4","duration_ms":193,"oldest_message_age_ms":3422}
```

`component` is one of:
- ingester: `http`, `producer`
- appender: `committer`, `pipeline`, `staging`, `sink`, `session`, `health`
- both: `kafka`, `main`, `crow`

librdkafka's log and error callbacks are logged as `component="kafka"` with the `client` field.

Hot paths never log once per message: the counts are in the metrics. The only per-message failure logs are rate-limited: Kafka delivery failures at most once a second, consumer errors at most once every 5 seconds. Retry logs are bounded by their backoff. The appender writes one info line per commit, which is a useful heartbeat.

The collector (`otel-collector-config.yaml`) does the following with each line:
- parses JSON lines;
- moves `msg` into the log body;
- maps `level` to severity and `ts` to the timestamp;
- sets `service.name` from `service`, or from the container name for other pods (`kafka`, `nessie`, ...).

Loki turns `service.name` into the `service_name` stream label, and the other fields become structured metadata, so they can be filtered without `| json`:

```logql
{service_name="otel-appender"} | component="committer"
{service_name="otel-appender"} | detected_level="warn"
{service_name=~"otel-ingester|otel-appender"} | component="kafka"
{service_name="kafka"} |= "ERROR"
sum by (service_name) (count_over_time({service_name=~"otel-.*"} | detected_level="error" [5m]))
```

## The Helm stack

Everything is gated by `observability.enabled` (default `true`).

| Component | Template | Notes |
|-----------|----------|-------|
| Prometheus | `prometheus.yaml`, `prometheus-config.yaml` | Scrapes pods in the namespace that have `prometheus.io/scrape`, `port` and `path` annotations (namespaced Role). 7-day retention on a PVC. |
| Alert rules | `prometheus-config.yaml` | Appender down or fatal; consecutive commit failures; commit lag high; no commit for 3× `BUFFER_TIME_SECONDS` while data waits; backpressure; poison messages; DLQ failing; ingester 5xx ratio; delivery failures; group lag (kafka-exporter); broker unreachable; under-replicated partitions. Thresholds are under `observability.prometheus.alerts`. |
| kafka-exporter | `kafka-exporter.yaml` | Group lag, topic and partition offsets, ISR |
| Kafka JMX | `kafka-statefulset.yaml`, `kafka-jmx-configmap.yaml` | `kafka.metrics.jmx.enabled`. An init container fetches the JMX exporter agent from Maven Central; it exports broker bytes in/out, request rates and p99 latency, handler and network idle ratios, replication health, and `jvm_*`. |
| Loki | `loki.yaml` | Single binary, filesystem storage on a PVC, 7-day retention (compactor), OTLP ingestion |
| OTel Collector | `otel-collector.yaml`, `otel-collector-config.yaml` | DaemonSet. `filelog` on `/var/log/pods/<namespace>_*` (it excludes its own pods) ships to Loki over OTLP/HTTP. Its own metrics are on `:8888`. |
| Grafana | `grafana.yaml`, `dashboards/*.json` | Provisioned Prometheus and Loki datasources, and three dashboards in a *Telemetry Lake* folder: **Appender** (the home dashboard), **Ingester** and **Kafka**. Each has a logs panel. Anonymous viewers are allowed. |

```bash
kubectl port-forward -n telemetry-lake svc/grafana 3000:3000      # admin/admin by default
kubectl port-forward -n telemetry-lake svc/prometheus 9090:9090   # targets: /targets, alerts: /alerts
```

The dashboards are generated JSON. Edit the files in `helm/telemetry-lake/dashboards/`: Grafana does not keep changes made in the UI.

## Locally

Without Kubernetes, the endpoints are plain HTTP:

```bash
curl -s localhost:9464/metrics | grep ^ingester_
curl -s localhost:8080/metrics | grep -E '^appender_(kafka_commit_lag|rows_|data_files)'
LOG_FORMAT=json ./otel_appender | jq 'select(.component == "committer")'
```
