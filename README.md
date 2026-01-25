# Telemetry Lake

A high-performance C++ telemetry ingestion pipeline that receives OpenTelemetry (OTel) logs via HTTP and writes them to Apache Iceberg tables. The system uses Apache Kafka as a message queue for reliable, scalable log processing.

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│ OTel Agent  │────▶│  Ingester   │────▶│    Kafka    │────▶│   Appender  │
│ (HTTP POST) │     │ (port 4318) │     │             │     │             │
└─────────────┘     └─────────────┘     └─────────────┘     └──────┬──────┘
                                                                   │
                                                                   ▼
                                                            ┌─────────────┐
                                                            │   Iceberg   │
                                                            │   (S3/MinIO)│
                                                            └─────────────┘
```

## Components

- **Ingester** (`otel_receiver`): HTTP server that accepts OTLP logs and produces to Kafka
- **Appender** (`otel_appender`): Kafka consumer that transforms logs and writes to Iceberg

## Dependencies

### System Dependencies (must be installed)

| Dependency | Version | Installation |
|------------|---------|--------------|
| C++ Compiler | C++17 compatible | Xcode Command Line Tools (macOS) or build-essential (Linux) |
| CMake | 3.11+ | `brew install cmake` (macOS) or `apt install cmake` (Linux) |
| Ninja | any | `brew install ninja` (macOS) or `apt install ninja-build` (Linux) |
| librdkafka | any | `brew install librdkafka` (macOS) or `apt install librdkafka-dev` (Linux) |
| Boost | 1.70+ | `brew install boost` (macOS) or `apt install libboost-all-dev` (Linux) |
| zlib | any | Usually pre-installed; `brew install zlib` (macOS) or `apt install zlib1g-dev` (Linux) |

### Fetched Dependencies (automatically downloaded by CMake)

These dependencies are fetched automatically during the CMake configuration step:

| Dependency | Version | Purpose |
|------------|---------|---------|
| Crow | v1.3.0.0 | HTTP web framework |
| Protocol Buffers | v25.3 | Serialization |
| OpenTelemetry Proto | v1.3.1 | OTel message definitions |
| Google Test | v1.14.0 | Unit testing |
| DuckDB | v1.0.0 | Iceberg table writes |
| cppkafka | v0.3.1 | C++ Kafka wrapper |

## How to Build

### 1. Install System Dependencies

**macOS (Homebrew):**
```bash
brew install cmake ninja librdkafka boost zlib
```

**Linux (Ubuntu/Debian):**
```bash
sudo apt update
sudo apt install build-essential cmake ninja-build librdkafka-dev libboost-all-dev zlib1g-dev
```

### 2. Clone and Build

```bash
# Clone the repository
git clone <repository-url>
cd telemetry-lake

# Create build directory
mkdir -p build
cd build

# Configure with CMake (using Ninja generator)
cmake .. -GNinja -DCMAKE_POLICY_VERSION_MINIMUM=3.5

# Build all targets
ninja

# Or build specific targets
ninja otel_receiver      # HTTP receiver only
ninja otel_appender      # Kafka consumer/Iceberg writer only
ninja http_server_test   # Tests only
```

### Build Options

| Option | Description |
|--------|-------------|
| `-GNinja` | Use Ninja build system (faster than Make) |
| `-DCMAKE_BUILD_TYPE=Release` | Release build with optimizations |
| `-DCMAKE_BUILD_TYPE=Debug` | Debug build with symbols |
| `-DCMAKE_POLICY_VERSION_MINIMUM=3.5` | Required for cppkafka compatibility |

### Alternative: Using Make

If you prefer Make over Ninja:

```bash
cd build
cmake ..
make -j$(nproc)
```

## Configuration

Both components are configured via environment variables.

### Ingester (otel_receiver)

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BROKERS` | (required) | Kafka broker addresses (e.g., `kafka:9092`) |
| `KAFKA_TOPIC` | `otel-logs` | Topic to produce messages to |
| `MAX_IN_FLIGHT` | `1000` | Max pending messages before backpressure |
| `PRODUCER_ACKS` | `-1` | Acks required (-1=all, 1=leader, 0=none) |
| `PRODUCER_COMPRESSION` | `snappy` | Compression type (snappy/gzip/lz4/zstd) |

### Appender (otel_appender)

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BROKERS` | (required) | Kafka broker addresses (e.g., `kafka:9092`) |
| `KAFKA_TOPIC` | `otel-logs` | Topic to consume from |
| `KAFKA_CONSUMER_GROUP` | `otel-appender` | Kafka consumer group ID |
| `ICEBERG_CATALOG_URI` | (required) | Nessie/Iceberg REST catalog URL |
| `S3_ENDPOINT` | (required) | S3/MinIO endpoint URL |
| `S3_ACCESS_KEY` | (required) | S3 access key |
| `S3_SECRET_KEY` | (required) | S3 secret key |
| `S3_BUCKET` | (required) | S3 bucket for Iceberg data |
| `ICEBERG_TABLE_NAME` | `logs` | Iceberg table name |
| `BUFFER_SIZE_MB` | `100` | Buffer size before flush (MB) |
| `BUFFER_TIME_SECONDS` | `300` | Max time before flush (seconds) |
| `DLQ_PATH` | (optional) | Dead letter queue file path |

## How to Run

### Start the Ingester

```bash
# With required environment variables
KAFKA_BROKERS=localhost:9092 ./otel_receiver

# With custom topic
KAFKA_BROKERS=kafka:9092 KAFKA_TOPIC=telemetry-logs ./otel_receiver
```

Output:
```
QueueProducer initialized with brokers: localhost:9092, topic: otel-logs
OTel Log Receiver is running at http://0.0.0.0:4318
```

### Start the Appender

```bash
# Configure and run
export KAFKA_BROKERS=localhost:9092
export KAFKA_TOPIC=otel-logs
export ICEBERG_CATALOG_URI=http://localhost:8181
export S3_ENDPOINT=http://localhost:9000
export S3_ACCESS_KEY=minioadmin
export S3_SECRET_KEY=minioadmin
export S3_BUCKET=iceberg

./otel_appender
```

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

### Build and Run All Tests

```bash
cd build

# Build tests
ninja http_server_test log_transformer_test buffer_manager_test

# Run all tests via CTest
ctest --output-on-failure

# Or run individual test executables
./http_server_test
./log_transformer_test
./buffer_manager_test
```

### Test Coverage

| Test Suite | Description |
|------------|-------------|
| `http_server_test` | HTTP endpoint handling, content types, gzip, error cases |
| `log_transformer_test` | OTel log record transformation |
| `buffer_manager_test` | Buffer size/time threshold management |

## Development

### Project Structure

```
telemetry-lake/
├── CMakeLists.txt          # Build configuration
├── proto/                  # Custom protobuf definitions
│   └── telemetry_wrapper.proto
├── src/
│   ├── main.cpp            # Receiver entry point
│   ├── config.hpp/cpp      # Configuration management
│   ├── ingester/           # HTTP receiver components
│   │   ├── http_server.hpp/cpp
│   │   └── queue_producer.hpp/cpp
│   └── appender/           # Kafka consumer components
│       ├── main.cpp
│       ├── queue_consumer.hpp/cpp
│       ├── log_transformer.hpp/cpp
│       ├── iceberg_appender.hpp/cpp
│       ├── buffer_manager.hpp/cpp
│       └── dead_letter_queue.hpp/cpp
├── tests/                  # Unit tests
└── docs/                   # Documentation
    └── dev-prompts/        # Development session logs
```

### Rebuilding After Changes

```bash
cd build
ninja                    # Incremental build
ninja clean && ninja     # Clean rebuild
```

### Troubleshooting Build Issues

**CMake configuration fails with cppkafka error:**
```bash
cmake .. -GNinja -DCMAKE_POLICY_VERSION_MINIMUM=3.5
```

**Missing Boost:**
```bash
brew install boost  # macOS
sudo apt install libboost-all-dev  # Linux
```

**Missing librdkafka:**
```bash
brew install librdkafka  # macOS
sudo apt install librdkafka-dev  # Linux
```

## License

[Add license information here]
