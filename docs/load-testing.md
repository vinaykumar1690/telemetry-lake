# Load testing

The UI's **Load test** tab drives `otel-loadgen` (`loadgen/`, Rust). That service sends a sustained rate of synthetic OTLP logs straight to the ingester. The browser only configures the run and watches it, and nginx only proxies the control API (`/api/loadgen/`); neither is in the data path.

The **Generate logs** tab is unchanged: it still builds a fixed number of records over a past time range in the browser.

```
UI ──/api/loadgen/──▶ loadgen (controller) ──/internal/run──▶ loadgen-workers ×N
                            │ (no workers: sends itself)            │
                            └──────────── OTLP/HTTP ────────────────┴──▶ ingester :4318
```

## Running a test

1. Open the UI (`kubectl port-forward -n telemetry-lake svc/ui 8080:80`) and go to **Load test**.
2. Set the rate (records/s), the duration (0 runs until stopped) and the request shape: records per request, the in-flight limit, protobuf or JSON, gzip.
3. Set the cardinality:
   - services and pods per service;
   - nodes (`host.name`);
   - distinct `user.id` values;
   - extra record attributes, each with a number of distinct values;
   - pod churn: every pod is replaced every N minutes, so cardinality grows over time as it does during rolling deploys.
4. The summary line shows the service's own estimate (it builds one sample request): requests/s, the request size, and MB/s on the wire.
5. **Start load**. The page polls once a second and shows:
   - achieved vs target records/s, accepted records/s, requests/s, bytes/s;
   - p50/p99 latency, failed requests/s, and "behind"/s;
   - a per-worker table.
6. The rate and the in-flight limit can be changed while the run is going (**Apply**).
7. **Search this run** opens the Search tab on the run's time window, filtered by `generator.run_id`.

Every record has the current time (up to 500 ms earlier), a `generator.run_id` attribute, and the message templates the Generate tab uses.

## What the numbers mean

- **Achieved records/s** counts records sent. **Accepted** counts records the ingester answered with 2xx; anything else is **rejected**. A request with no response at all (refused, reset or timed out) counts as **failed**.
- **Behind** is a request the schedule called for that wasn't sent. Load is open-model: requests go out on schedule regardless of responses, and are never queued. A tick is counted as behind in two cases:
  - **Every allowed request was still in flight.** Either the ingester is the bottleneck, or the in-flight limit is too low for the latency. Check p99: by Little's law, sustaining a rate R needs at least R × latency requests in flight.
  - **The generator ran out of CPU.** It fell more than a second behind schedule. Add workers.
- Requests aren't retried; a load test measures what the ingester accepts. Failures are counted by status code (the HTTP line under the stats).
- Latency is from the start of the request until the response body has been read.

## Scaling the generator

With `loadgen.workers.replicas: 0` (the default, so kind stays small), the controller pod sends the load itself. One core generates roughly the throughput printed by:

```bash
cd loadgen && cargo test --release -- --ignored --nocapture throughput
```

The controller runs one rate loop per core (`DRIVER_TASKS`, which defaults to the container's CPUs).

For more, set workers and upgrade:

```bash
helm upgrade telemetry-lake helm/telemetry-lake --reuse-values --set loadgen.workers.replicas=4
```

- The controller resolves the headless `loadgen-workers` Service once a second and gives each worker an even share of the rate and of the in-flight limit.
- `kubectl scale deploy/loadgen-workers` during a run rebalances within a couple of seconds.
- A worker that hasn't heard from the controller for 15 s stops its run, so a lost controller can't leave load running.

Each worker keeps one HTTP/1.1 connection per request in flight. Through the ingester's ClusterIP Service, kube-proxy balances connections, not requests. Ingester replicas added during a run only get load once new connections open, so start the run after scaling the ingester.

## What to scale next

At tens of thousands of requests and hundreds of MB/s, the generator is rarely the limit. In the order they usually saturate:

| Component | Values | Watch |
|---|---|---|
| Ingester | `ingester.replicas`, `ingester.httpThreads`, CPU limit | Ingester dashboard: request latency, 503 (`queue_full`), delivery latency |
| Kafka | `kafka.config.numPartitions` (one appender pipeline per partition), broker CPU/disk; the chart runs one broker | Kafka dashboard: request handler idle, produce latency |
| Appender | `appender.pipeline.threads`, `appender.buffer.*`, memory limit | Appender dashboard: commit lag, records/s, backpressure |

The ingester answers 503 when its Kafka producer queue is full. A run that shows mostly 503s with low latency is limited by Kafka or the producer, not by HTTP.

The **Load generator** row of the Grafana Ingester dashboard plots the service's own view (`loadgen_*`) next to the ingester's:
- sent vs accepted records/s;
- responses by code;
- client-side latency.

## API

The UI's calls, for scripting a test (`kubectl port-forward svc/loadgen 8095`):

| Call | |
|---|---|
| `POST /v1/run` | Start a run with a `RunSpec` (`loadgen/src/spec.rs`); 409 if one is going |
| `PATCH /v1/run` | `{"records_per_second": N, "max_in_flight": N}` while running |
| `DELETE /v1/run` | Stop |
| `GET /v1/run` | The run, totals, current rates, and the per-worker breakdown |
| `POST /v1/estimate` | Validate a spec and size one request |
| `GET /v1/limits` | The caps below |

```bash
curl -s localhost:8095/v1/run -H 'content-type: application/json' -d '{
  "records_per_second": 50000, "records_per_request": 500, "duration_seconds": 600,
  "max_in_flight": 128, "wire": "protobuf", "compression": "none",
  "services": [{"name": "checkout", "pods": 20}], "nodes": 10,
  "service_namespace": "shop", "k8s_namespace": "shop", "environment": "load",
  "users": 100000, "extra_attributes": [{"key": "tenant.id", "distinct_values": 500}]
}'
```

## Configuration

| Variable | Default | |
|---|---|---|
| `INGEST_URL` | required | The ingester; `/v1/logs` is appended. Set by Helm and never taken from a request, so the UI can't point the generator anywhere else |
| `HTTP_PORT` | 8095 | |
| `WORKERS_DNS` | none | Headless Service of the workers. Without it the controller sends the load itself |
| `WORKERS_PORT` | `HTTP_PORT` | |
| `DRIVER_TASKS` | CPUs | Rate loops per pod |
| `MAX_RECORDS_PER_SECOND` | 2,000,000 | Caps on what a run may ask for |
| `MAX_IN_FLIGHT` | 4096 | |
| `MAX_REQUEST_BYTES` | 16 MiB | A run whose sample request (plus 20 %) exceeds this is refused; Helm sets the ingester's limit |
| `REQUEST_TIMEOUT_SECONDS` | 30 | |
