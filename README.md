# Telemetry Lake

A C++23 pipeline that receives OpenTelemetry (OTel) logs over OTLP/HTTP, buffers them in Apache Kafka, and writes them to Apache Iceberg tables that SQL engines (DuckDB, Trino, Spark) can query. Delivery from Kafka into Iceberg is **exactly-once**, across crashes, restarts and consumer-group rebalances. A Rust query service searches the tables by time, tags and text, and serves read-only SQL.

The full design, including the guarantees each hop gives and the reasons for them, is in [docs/design.md](docs/design.md).

## Architecture

```
┌──────────────┐  OTLP/HTTP  ┌────────────────┐  raw payload   ┌──────────┐     ┌────────────────┐
│ OTel SDKs /  │────────────▶│    Ingester    │───────────────▶│  Kafka   │────▶│    Appender    │
│ Collectors   │  :4318      │ (otel_receiver)│  (no parsing)  │ otel-logs│     │ (otel_appender)│
└──────────────┘             └────────────────┘                └──────────┘     └───┬────────┬───┘
                                                                                    │        │ undecodable
                                         one atomic commit: rows + Kafka offsets    │        ▼
                                  ┌─────────────────────────────────────────────────┘   ┌──────────────┐
                                  ▼                                                     │ otel-logs-dlq│
                     ┌─────────────────────────┐      ┌──────────────────┐              └──────────────┘
                     │ Iceberg REST catalog    │      │ S3 / MinIO       │
                     │ (Nessie)                │      │ (Parquet files)  │
                     └─────────────────────────┘      └──────────────────┘
                                  ▲
                     ┌────────────┴────────────┐      ┌──────────────────┐
                     │ Query (otel-query)      │◀─────│ Clients          │
                     │ :8090  search / SQL     │      │ (search API, SQL)│
                     └─────────────────────────┘      └──────────────────┘
```

- **Ingester** (`otel_receiver`, `src/ingester/`): a Crow HTTP server on port 4318. It validates `POST /v1/logs` requests (size, media type, gzip), sheds load with 503 when too many produces are in flight, and wraps the decompressed body, unparsed, in a `RawTelemetryMessage` ([proto/telemetry_wrapper.proto](proto/telemetry_wrapper.proto)) that it produces to Kafka with an idempotent, `acks=all` producer.
- **Appender** (`otel_appender`, `src/appender/`): consumes the topic, decodes OTLP (protobuf or JSON), transforms each log record into a row, and stages rows per Kafka partition in in-memory DuckDB. On size, age or flush triggers it commits the staged rows to Iceberg.
- **Query** (`otel-query`, Rust, `query/`): log search (time ranges, tags on columns and attribute maps, grep and regex, cursor pagination) and read-only SQL over the logs table, executed in-process by Apache DataFusion with iceberg-rust. API reference: [query/README.md](query/README.md); design: [docs/query-design.md](docs/query-design.md).

### Guarantees in brief

| Hop | Guarantee |
|-----|-----------|
| Client → Kafka | At-least-once for retried requests. A 200 means the message reached the producer's local queue, not that Kafka has it ([design §6.2](docs/design.md#62-client--kafka-ingester)) |
| Kafka → Iceberg | **Exactly-once** per Kafka message |
| Kafka → DLQ | At-least-once |
| Within a Kafka partition | Order preserved |

How exactly-once works:
- Each commit inserts the log rows **and** the Kafka offsets they cover into the `_appender_offsets` table in one DuckDB transaction, which DuckDB (≥ 1.5.4) sends to the catalog as a single REST `transactions/commit`.
- On partition assignment the appender resumes from that table. Kafka consumer-group offsets are written afterwards, only as hints for lag monitoring.
- When partitions are revoked, the appender waits for any in-flight commit covering them (or exits) before the new owner can start.
- Nothing is skipped: undecodable messages go to a DLQ topic before their offset advances, and full pipelines pause partitions instead of dropping data.

Memory stays bounded: when Iceberg is unavailable the appender stops consuming at `MAX_STAGED_MB`, and Kafka holds the backlog. See [design §7](docs/design.md#7-backpressure-and-memory-bounds) and the [known limitations](docs/design.md#9-known-limitations-and-future-work).

### Tables

| Table | Contents |
|-------|----------|
| `<namespace>.logs` | One row per log record, partitioned by `day(timestamp)`: Kafka coordinates, timestamps, severity, body, trace/span IDs, promoted resource fields (`service_name`, `host_name`, …), `resource_attributes`/`log_attributes` maps, and a column per configured `PROMOTED_ATTRIBUTES` key |
| `<namespace>._appender_offsets` | Append-only record of the Kafka offsets each commit made durable; the source of truth for recovery |

The column definitions are in [src/appender/schema.hpp](src/appender/schema.hpp) and [design §8](docs/design.md#8-data-model). The appender creates both tables on startup, and refuses to start if an existing table has a different shape.

The appender writes many small files. A **compaction job** ([compaction/compact.py](compaction/compact.py), Spark, run hourly by a CronJob) rewrites them into large files sorted by service, severity and time, with bloom filters, and prunes the offsets table. On Nessie, a daily **Nessie GC** job deletes the files it replaced ([design §8](docs/design.md#8-data-model)).

## Prerequisites

### To build

| Dependency | Version | macOS (Homebrew) | Linux (Ubuntu 24.04+) |
|------------|---------|------------------|------------------------|
| C++ compiler | C++23 (GCC 13+, Clang 17+) | Xcode Command Line Tools | `build-essential` |
| CMake | 3.20+ | `cmake` | `cmake` |
| Ninja | any | `ninja` | `ninja-build` |
| librdkafka | any recent (2.12 tested) | `librdkafka` | `librdkafka-dev` |
| Crow | 1.3.0 | `crow` (pulls in `asio`) | [release tarball](https://github.com/CrowCpp/Crow/releases), plus `libasio-dev` |
| GoogleTest | 1.14+ (1.17 tested) | `googletest` | `libgtest-dev libgmock-dev` |
| DuckDB (library) | **1.5.4 or newer** (1.5.6 tested) | `duckdb` | `libduckdb-linux-amd64.zip` from [DuckDB releases](https://github.com/duckdb/duckdb/releases) |
| zlib | any | usually present | `zlib1g-dev` |
| OpenSSL | any | usually present | `libssl-dev` |

CMake downloads the rest during configuration: Protocol Buffers v25.3, OpenTelemetry Proto v1.11.1, cppkafka v0.3.1 and nlohmann/json v3.12.0 (and zlib v1.3.1 if no system zlib is found).

DuckDB is only needed for the appender. If CMake cannot find it, it prints a warning and skips `otel_appender` and `appender_duckdb_test`; if it finds a version older than 1.5.4, configuration fails. Point CMake at a non-standard install with `-DDUCKDB_ROOT=<dir>`.

### To run the local stack

| Tool | Version | macOS install | Used for |
|------|---------|---------------|----------|
| Docker Desktop | with `docker compose` v2 | <https://docs.docker.com/desktop/> | Kafka 4.3.1 and Nessie containers |
| MinIO server | 2025-10-15 or newer | `brew install minio` | S3 store on `localhost:9000`, runs on the host |
| DuckDB CLI | 1.5.4 or newer | `brew install duckdb` | Querying the tables, integration tests |
| kcat | 1.7 | `brew install kcat` | End-to-end test only |
| curl | 7.75 or newer (`--aws-sigv4`) | ships with macOS | Bucket creation, health checks |

MinIO runs on the host rather than in Docker because MinIO no longer publishes public container images. Check that `duckdb --version` is 1.5.4 or newer: the DuckDB install script may have put an older CLI ahead of Homebrew's on your `PATH` (see [integration-testing.md](docs/integration-testing.md#prerequisites)).

## How to Build

### 1. Install the dependencies

**macOS (Homebrew):**
```bash
brew install cmake ninja librdkafka crow googletest duckdb
```

**Linux (Ubuntu 24.04+):**
```bash
sudo apt update
sudo apt install build-essential cmake ninja-build git pkg-config curl unzip \
  librdkafka-dev libasio-dev libgtest-dev libgmock-dev zlib1g-dev libssl-dev

# Crow (header-only)
curl -L https://github.com/CrowCpp/Crow/releases/download/v1.3.0/Crow-1.3.0-Linux.tar.gz | tar -xz -C /tmp
sudo cp -r /tmp/Crow-1.3.0-Linux/include/* /usr/local/include/
sudo mkdir -p /usr/local/lib/cmake && sudo cp -r /tmp/Crow-1.3.0-Linux/lib/cmake/* /usr/local/lib/cmake/

# DuckDB library
curl -L -o /tmp/libduckdb.zip \
  https://github.com/duckdb/duckdb/releases/download/v1.5.6/libduckdb-linux-amd64.zip
unzip /tmp/libduckdb.zip -d /tmp/duckdb
sudo cp /tmp/duckdb/duckdb.hpp /tmp/duckdb/duckdb.h /usr/local/include/
sudo cp /tmp/duckdb/libduckdb.so /usr/local/lib/ && sudo ldconfig
```

The same steps are in [docker/appender/Dockerfile](docker/appender/Dockerfile).

### 2. Configure and build

```bash
git clone <repository-url>
cd telemetry-lake
mkdir -p build && cd build

cmake .. -GNinja -DCMAKE_POLICY_VERSION_MINIMUM=3.5
ninja                     # everything

ninja otel_receiver       # ingester only
ninja otel_appender       # appender only
```

| Option | Description |
|--------|-------------|
| `-DCMAKE_POLICY_VERSION_MINIMUM=3.5` | Required for cppkafka compatibility |
| `-DCMAKE_BUILD_TYPE=Release` / `Debug` | Optimized build / build with symbols |
| `-DDUCKDB_ROOT=<dir>` | Where to look for the DuckDB library and headers |

## Running the Stack Locally

The integration-test stack doubles as a local development environment: Kafka and Nessie (the Iceberg REST catalog) run in Docker, MinIO runs on the host. [docs/integration-testing.md](docs/integration-testing.md) describes it in detail, including the Nessie settings that matter and troubleshooting. To set the services up by hand instead, see [docs/mac-setup.md](docs/mac-setup.md).

| Service | Where | Address | Notes |
|---------|-------|---------|-------|
| MinIO | host (Homebrew) | `localhost:9000` | credentials `minioadmin`/`minioadmin`; bucket `telemetrylakeit` |
| Kafka 4.3.1 | Docker, single KRaft node | `localhost:9092` | auto topic creation is off; `stack.sh` creates the topics |
| Nessie | Docker | `localhost:19120` | Iceberg REST at `http://localhost:19120/iceberg/`; warehouse `telemetrylakeit`; **in-memory**, so tables are forgotten when it restarts |

### 1. Start the services

From the repository root:

```bash
brew services start minio          # once; it stays running
tests/it/stack.sh up               # bucket, Kafka, Nessie, topics e2e-logs (4 partitions) + e2e-logs-dlq
eval "$(tests/it/stack.sh env)"    # exports KAFKA_BROKERS, KAFKA_TOPIC, ICEBERG_*, S3_*, DUCKDB
```

`stack.sh up` is idempotent. `tests/it/stack.sh topic NAME [PARTITIONS]` creates another topic; set `KAFKA_TOPIC` before `up` and `env` to use a different main topic (its `-dlq` topic is created too).

### 2. Start the ingester and the appender

In two terminals, each with the environment from step 1 loaded:

```bash
cd build
./otel_receiver
# QueueProducer initialized with brokers: localhost:9092, topic: e2e-logs
# OTel Log Receiver is running at http://0.0.0.0:4318
```

```bash
cd build
./otel_appender                    # writes to lake."default"."logs" by default

curl localhost:8080/ready          # 200 once it is polling and committing
```

The appender commits every `BUFFER_TIME_SECONDS` (60 s by default) or when `BUFFER_SIZE_MB` is staged. To see data sooner, force a commit:

```bash
curl -X POST localhost:8080/flush  # or: kill -USR1 <appender pid>
curl localhost:8080/metrics        # Prometheus metrics
```

`SIGINT`/`SIGTERM` (Ctrl-C) drains, makes a final commit and exits.

### 3. Send logs and query them

Send a request as shown in [How to Send Logs](#how-to-send-logs), flush, then query the table with the DuckDB CLI:

```bash
$DUCKDB -c "
INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;
CREATE SECRET s3 (TYPE S3, KEY_ID 'minioadmin', SECRET 'minioadmin',
                  ENDPOINT 'localhost:9000', REGION 'us-east-1', USE_SSL false, URL_STYLE 'path');
ATTACH 'telemetrylakeit' AS lake (TYPE ICEBERG, ENDPOINT 'http://localhost:19120/iceberg/',
                                  AUTHORIZATION_TYPE 'none', ACCESS_DELEGATION_MODE 'none');
SELECT timestamp, service_name, severity_text, body FROM lake.\"default\".logs LIMIT 10;"
```

`ACCESS_DELEGATION_MODE 'none'` matters: Nessie does not vend S3 keys, so without it DuckDB reads and writes anonymously and MinIO answers 403 ([details](docs/integration-testing.md#access-delegation-duckdb--catalog)).

Or search through the query service (needs Rust 1.95+; the first build takes several minutes):

```bash
(cd query && cargo build --release)
query/target/release/otel-query &  # :8090, same environment as the appender

curl -s localhost:8090/v1/search -H 'Content-Type: application/json' \
  -d '{"start": "now-1h", "filters": {"service.name": "my-service"}, "grep": "Hello", "limit": 10}'
curl -s localhost:8090/v1/sql -d '{"sql": "SELECT service_name, count(*) AS n FROM logs GROUP BY 1"}'
```

The search API, field names, pagination and SQL rules are in [query/README.md](query/README.md).

### 4. Stop

```bash
tests/it/stack.sh down             # removes the containers; Nessie forgets all tables
```

Data files stay in the MinIO bucket; see [Cleaning up](docs/integration-testing.md#cleaning-up) to delete them. After a restart of Nessie, either clean the bucket or use a fresh `ICEBERG_NAMESPACE`.

### Kubernetes

The Helm chart in [helm/telemetry-lake](helm/telemetry-lake) deploys the ingester, the appender, the query service and its distributed query workers (each with a CPU autoscaler), the compaction and Nessie GC CronJobs, Kafka, MinIO, Nessie and a DuckDB pod, for example on Docker Desktop's Kubernetes; see [docs/telemetry-lake-helm-README.md](docs/telemetry-lake-helm-README.md). Build the service images from the repository root:

```bash
docker build -f docker/ingester/Dockerfile -t telemetry-lake/ingester:latest .
docker build -f docker/appender/Dockerfile -t telemetry-lake/appender:latest .
docker build -f docker/query/Dockerfile -t telemetry-lake/query:latest .
docker build -f docker/compaction/Dockerfile -t telemetry-lake/compaction:latest .
```

## Configuration

All components are configured via environment variables. The query service's variables (it shares the appender's catalog and S3 names) are listed in [query/README.md](query/README.md#run).

### Ingester (otel_receiver)

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BROKERS` | (required) | Kafka broker addresses (e.g., `kafka:9092`) |
| `KAFKA_TOPIC` | `otel-logs` | Topic to produce messages to |
| `MAX_IN_FLIGHT` | `1000` | Max pending messages before backpressure |
| `PRODUCER_ACKS` | `-1` | Acks required (-1=all, 1=leader, 0=none) |
| `PRODUCER_COMPRESSION` | `snappy` | Compression type (snappy/gzip/lz4/zstd) |
| `HTTP_HOST` | `0.0.0.0` | Address to bind the OTLP/HTTP server to |
| `HTTP_PORT` | `4318` | OTLP/HTTP port |
| `HTTP_THREADS` | `0` | HTTP worker threads (0 = one per hardware thread; set to the CPU limit in containers) |
| `MAX_BODY_BYTES` | `16777216` | Max request body as received; larger requests get 413 |
| `MAX_DECOMPRESSED_BYTES` | `16777216` | Max payload after gzip decompression; larger payloads get 413 |

Invalid numeric values fail startup instead of falling back to 0. The ingester also exits if the Kafka producer cannot be initialized, so the orchestrator restarts it instead of routing traffic to a pod that would drop data.

#### Message size limits

Each accepted request becomes one Kafka message containing the **decompressed** payload. OpenTelemetry Collector batches (8192 records by default) are commonly 2–8 MB uncompressed, well above Kafka's ~1 MB defaults, so the limits are raised end to end:

| Layer | Setting | Default here |
|-------|---------|--------------|
| Ingester HTTP | `MAX_BODY_BYTES`, `MAX_DECOMPRESSED_BYTES` | 16 MiB each |
| Ingester producer | librdkafka `message.max.bytes` | derived: max of the two limits + 64 KiB |
| Broker / topic | `message.max.bytes` / topic `max.message.bytes` | 17 MiB (Helm `kafka.config.messageMaxBytes`) |
| Appender consumer | librdkafka `fetch.max.bytes` / `receive.message.max.bytes` | librdkafka defaults (50 MB / 100 MB) |

When using an **external** Kafka, raise the topic limit yourself, otherwise large payloads are acknowledged over HTTP and then fail delivery (visible only in the ingester's delivery-failure log):

```bash
kafka-configs.sh --bootstrap-server <broker> --alter --entity-type topics \
  --entity-name <topic> --add-config max.message.bytes=17825792
```

Also cap the request body at your ingress (for example nginx `client_max_body_size 16m`): the HTTP server reads the whole body into memory before it can check `MAX_BODY_BYTES`.

### Appender (otel_appender)

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BROKERS` | (required) | Kafka broker addresses (e.g., `kafka:9092`) |
| `KAFKA_TOPIC` | `otel-logs` | Topic to consume from |
| `KAFKA_CONSUMER_GROUP` | `otel-appender` | Kafka consumer group ID |
| `KAFKA_DLQ_TOPIC` | `<topic>-dlq` | Topic that receives undecodable messages (original bytes + error headers) |
| `ICEBERG_CATALOG_URI` | (required) | Iceberg REST catalog URL (Nessie: `http://nessie:19120/iceberg/`) |
| `ICEBERG_WAREHOUSE` | `S3_BUCKET` | Warehouse name passed to `ATTACH` |
| `ICEBERG_NAMESPACE` | `default` | Namespace of both tables |
| `ICEBERG_TABLE_NAME` | `logs` | Logs table |
| `ICEBERG_OFFSETS_TABLE` | `_appender_offsets` | Offsets table (source of truth for recovery) |
| `PROMOTED_ATTRIBUTES` | (none) | Comma-separated attribute keys (at most 32) stored as their own VARCHAR columns, `.` → `_` (`k8s.pod.name` → `k8s_pod_name`); the record's value, else the resource's |
| `ICEBERG_ACCESS_DELEGATION_MODE` | `none` | `none` writes with `S3_ACCESS_KEY`/`S3_SECRET_KEY`; `vended_credentials` uses the storage config the catalog returns |
| `ICEBERG_PARTITION_SPEC` | `day` | `day`, `hour` or `none` partitioning of the logs table on `timestamp` |
| `S3_ENDPOINT` | (required) | S3/MinIO endpoint; an `https://` prefix enables TLS |
| `S3_ACCESS_KEY` / `S3_SECRET_KEY` | (required) | S3 credentials |
| `S3_BUCKET` | (optional) | Default warehouse name |
| `S3_REGION` / `S3_URL_STYLE` | `us-east-1` / `path` | S3 addressing |
| `BUFFER_SIZE_MB` | `100` | Commit when this much data is staged across all partitions |
| `BUFFER_TIME_SECONDS` | `60` | ... or when the oldest staged row is this old |
| `MAX_STAGED_MB` | `512` | Staged bytes at which consumption is throttled |
| `PIPELINE_THREADS` | hardware threads (max 16) | Decode/transform/stage threads |
| `PIPELINE_QUEUE_MB` | `64` | Queue size per pipeline thread |
| `ICEBERG_COMMIT_RETRIES` | `5` | Attempts per commit cycle |
| `ICEBERG_RETRY_BASE_DELAY_MS` / `ICEBERG_RETRY_MAX_DELAY_MS` | `200` / `10000` | Retry backoff |
| `REVOKE_TIMEOUT_SECONDS` | `60` | Max wait for an in-flight commit when partitions are revoked (must be below `MAX_POLL_INTERVAL_MS`) |
| `MAX_POLL_INTERVAL_MS` | `300000` | Kafka `max.poll.interval.ms` |
| `OFFSETS_MAINTENANCE_INTERVAL_SECONDS` | `3600` | Offsets table pruning (only where DuckDB can compact; 0 disables) |
| `HEALTH_PORT` | `8080` | `/health`, `/ready`, `POST /flush`, `/metrics` |
| `APPENDER_ID` | hostname | Recorded with each offsets row |
| `DUCKDB_HOME_DIRECTORY` | `/tmp` | Where DuckDB installs its extensions (must be writable) |

The appender needs DuckDB 1.5.4 or newer. Delivery into Iceberg is exactly-once: each commit writes the log rows and their Kafka offsets to the offsets table in one atomic Iceberg transaction, and recovery resumes from that table. See [docs/design.md](docs/design.md).

Signals: `SIGINT`/`SIGTERM` drain, commit and exit; `SIGUSR1` forces a commit. Exit codes: 0 clean, 1 configuration/startup error, 2 fatal runtime error.

## How to Send Logs

The receiver accepts OTLP logs on port 4318 at `/v1/logs`.

### JSON Format

```bash
curl -X POST http://localhost:4318/v1/logs \
  -H "Content-Type: application/json" \
  -d '{
    "resourceLogs": [{
      "resource": {
        "attributes": [{
          "key": "service.name",
          "value": {"stringValue": "my-service"}
        }]
      },
      "scopeLogs": [{
        "scope": {},
        "logRecords": [{
          "timeUnixNano": "1672531200000000000",
          "severityText": "INFO",
          "body": {"stringValue": "Hello from my service"}
        }]
      }]
    }]
  }'
```

### Protobuf Format

```bash
# Using protobuf (more efficient)
curl -X POST http://localhost:4318/v1/logs \
  -H "Content-Type: application/x-protobuf" \
  --data-binary @logs.pb
```

### With Gzip Compression

```bash
# Compress and send
echo '{"resourceLogs":[...]}' | gzip | \
  curl -X POST http://localhost:4318/v1/logs \
    -H "Content-Type: application/json" \
    -H "Content-Encoding: gzip" \
    --data-binary @-
```

## Running Tests

The unit tests need no services: they use fakes and in-memory DuckDB.

```bash
cd build
ctest --output-on-failure

./http_server_test        # ingester
./appender_core_test      # appender logic, DuckDB-free
./appender_duckdb_test    # staging and the Iceberg commit SQL, against in-memory DuckDB
```

| Test binary | Covers |
|-------------|--------|
| `http_server_test` | HTTP endpoint handling, status codes, limits, content types, gzip, OTLP error bodies |
| `appender_core_test` | Config validation, OTLP protobuf/JSON decoding, transformation, bounded queues, backoff, pipeline ordering/poison/backpressure, commit protocol (retries, indeterminate commits, fencing), DLQ headers |
| `appender_duckdb_test` | Staging round-trips (quotes, backslashes, NUL, invalid UTF-8), generation sealing, atomic two-table commits, offset recovery |

The query service has its own Rust tests: `cd query && cargo test --workspace`.

Tests against live Kafka, Nessie and MinIO are opt-in. With the [local stack](#running-the-stack-locally) up and its environment loaded:

```bash
ICEBERG_IT=1 tests/it/iceberg_spike.sh                     # DuckDB + Nessie capabilities (~10 s)
(cd build && ICEBERG_IT=1 DUCKDB_HOME_DIRECTORY=$HOME/.duckdb \
    ./appender_duckdb_test --gtest_filter='IcebergIntegration*')   # commit + recovery (~5 s)
E2E=1 tests/e2e/exactly_once.sh                            # exactly-once under kill -9, restarts, rebalance (3–6 min)
QUERY_IT=1 tests/it/query_it.py                           # query service vs DuckDB (~1 min; build query/ first)
ICEBERG_IT=1 tests/it/compaction_spike.sh                  # Spark/Nessie maintenance capabilities (~2 min)
COMPACTION_IT=1 tests/it/compaction_it.py                  # compaction vs appender and query service (~3 min)
COMPACT_DURING=1 E2E=1 tests/e2e/exactly_once.sh           # exactly-once with compaction running throughout
```

The end-to-end test ends with `PASS: 10000 rows, no duplicates, no loss; N DLQ messages`. Expected output, the latest results and troubleshooting are in [docs/integration-testing.md](docs/integration-testing.md).

## Development

### Project Structure

```
telemetry-lake/
├── CMakeLists.txt
├── proto/telemetry_wrapper.proto   # Kafka message wrapper
├── src/
│   ├── main.cpp                    # Ingester entry point
│   ├── config.hpp/cpp              # Ingester configuration
│   ├── ingester/                   # HTTP server, Kafka producer
│   └── appender/                   # Kafka -> Iceberg appender (see docs/design.md)
│       ├── main.cpp, app.cpp           # wiring, signals, shutdown order
│       ├── kafka_source.*              # consumer, rebalance, recovery, backpressure
│       ├── pipeline_pool.*             # decode -> transform -> stage threads
│       ├── otlp_decoder.*, log_transformer.*
│       ├── staging_store.*             # per-partition DuckDB staging generations
│       ├── iceberg_committer.*         # commit cycles, retries, fencing
│       ├── duckdb_iceberg.*            # atomic logs+offsets commit, offset store
│       ├── duckdb_session.*, schema.hpp
│       └── dlq_producer.*, health*.*
├── query/                          # Rust query service (see query/README.md)
│   └── crates/{logquery,planner,query-server}
├── tests/
│   ├── test_http_server.cpp, appender/  # unit tests
│   ├── it/                         # local stack, spikes, query_it.py, compaction_it.py
│   └── e2e/exactly_once.sh
├── compaction/compact.py           # Iceberg maintenance job (Spark)
├── docker/                         # ingester, appender, query and compaction Dockerfiles
├── helm/telemetry-lake/            # Helm chart
└── docs/
    ├── design.md                   # architecture and guarantees
    ├── query-design.md             # query service design and roadmap
    ├── integration-testing.md      # local stack and live tests
    └── mac-setup.md                # setting the services up by hand
```

### Troubleshooting

| Symptom | Fix |
|---------|-----|
| CMake error from cppkafka about the policy version | Pass `-DCMAKE_POLICY_VERSION_MINIMUM=3.5` |
| `DuckDB library not found. otel_appender target will not be built.` | Install DuckDB (`brew install duckdb`) or pass `-DDUCKDB_ROOT=<dir>` |
| `DuckDB x.y.z found ...; the appender needs >= 1.5.4` | `brew upgrade duckdb`, or point `DUCKDB_ROOT` at a newer library |
| `librdkafka is required but not found` | `brew install librdkafka` / `apt install librdkafka-dev` |
| `Could not find a package configuration file provided by "Crow"` | Install Crow (see [step 1](#1-install-the-dependencies)) |
| Appender exits 2 at startup with a schema mismatch | The namespace holds tables from an older schema; use a fresh `ICEBERG_NAMESPACE` |
| `403 Forbidden` writing `s3://...parquet` | Keep `ICEBERG_ACCESS_DELEGATION_MODE=none` unless the catalog vends credentials |

More stack-related problems are covered in [integration-testing.md](docs/integration-testing.md#troubleshooting).

## License

Apache License 2.0; see [LICENSE](LICENSE).
