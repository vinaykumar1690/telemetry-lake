# Implementation Plan: OTel Logs Iceberg Ingester Service

## Architecture Overview

The system consists of two main components:
1. **Ingester Service**: Enhanced HTTP server that receives OTLP logs and writes to Redpanda queue
2. **Appender Service**: Consumer service that buffers logs, converts to Parquet via DuckDB, and commits to Iceberg tables

```
OTel Agent → Ingester (HTTP) → Redpanda Queue → Appender → DuckDB → Iceberg (S3)
```

## Component 1: Enhanced Ingester Service

### 1.1 Queue Integration
**File**: `src/ingester/queue_producer.hpp` / `src/ingester/queue_producer.cpp`

- Add Redpanda/Kafka producer using `librdkafka` directly (C API)
- Implement async producer with `DeliveryReportCb` callback class for delivery tracking
- Serialize `ExportLogsServiceRequest` protobuf messages to queue
- Configuration: broker addresses, topic name, producer settings (acks, compression)

**Key Features**:
- Write to queue before returning 200 OK (ensures durability)
- Handle producer errors gracefully (return 503 on queue full, 500 on persistent errors)
- Implement backpressure: track in-flight messages, reject requests when queue is full

### 1.2 Modify HTTP Server
**File**: `src/ingester/http_server.cpp`

- Replace `handle_logs()` stub with queue producer call
- Add queue producer as member/dependency
- Error handling: return appropriate HTTP status codes based on queue write result
- Maintain existing protobuf/JSON parsing and gzip decompression

### 1.3 Configuration
**File**: `src/config.hpp` / `src/config.cpp`

- Environment variables or config file for:
  - Queue broker addresses
  - Queue topic name
  - Producer settings (acks=all for durability)
  - Max in-flight messages (backpressure threshold)

## Component 2: Appender Service

### 2.1 Queue Consumer
**File**: `src/appender/queue_consumer.hpp` / `src/appender/queue_consumer.cpp`

- Redpanda consumer using `librdkafka` with consumer groups
- Deserialize protobuf messages from queue
- Batch messages into buffers based on size/time thresholds
- Handle consumer group coordination for horizontal scaling

### 2.2 Log Record Transformer
**File**: `src/appender/log_transformer.hpp` / `src/appender/log_transformer.cpp`

- Extract fields from OTel protobuf structure:
  - `timestamp` (from `timeUnixNano`)
  - `severity` (from `severityText` or `severityNumber`)
  - `body` (string value from log record body)
  - `trace_id`, `span_id` (if present)
  - Well-known attributes: `service.name`, `deployment.environment`, `host.name`
  - Remaining attributes → map column
- Convert to DuckDB-compatible data structures

### 2.3 DuckDB Integration
**File**: `src/appender/iceberg_appender.hpp` / `src/appender/iceberg_appender.cpp`

- Initialize DuckDB connection with Iceberg extension
- Create Iceberg table with hybrid schema:
  ```sql
  CREATE TABLE logs (
    timestamp TIMESTAMP,
    severity VARCHAR,
    body VARCHAR,
    trace_id VARCHAR,
    span_id VARCHAR,
    service_name VARCHAR,
    deployment_environment VARCHAR,
    host_name VARCHAR,
    attributes MAP(VARCHAR, VARCHAR)
  ) USING ICEBERG
  ```
- Buffer log records in DuckDB DataChunk
- On threshold (100MB or 5 minutes):
  - Insert buffered data into Iceberg table
  - DuckDB handles Parquet file creation and Iceberg commit
- Configure Iceberg REST catalog connection
- Configure S3-compatible storage backend

### 2.4 Buffering Logic
**File**: `src/appender/buffer_manager.hpp` / `src/appender/buffer_manager.cpp`

- Track buffer size (approximate byte count)
- Track time since last flush
- Trigger flush when either threshold is met
- Thread-safe buffer management

### 2.5 Appender Main
**File**: `src/appender/main.cpp`

- Initialize queue consumer
- Initialize DuckDB/Iceberg connection
- Main loop: consume → transform → buffer → flush
- Graceful shutdown handling

## Component 3: Schema Design

### 3.1 Hybrid Schema Implementation

**Top-level columns** (for query performance):
- `timestamp` (TIMESTAMP) - from `timeUnixNano`
- `severity` (VARCHAR) - from `severityText`
- `body` (VARCHAR) - log message body
- `trace_id` (VARCHAR) - optional
- `span_id` (VARCHAR) - optional
- `service_name` (VARCHAR) - from resource attributes
- `deployment_environment` (VARCHAR) - from resource attributes
- `host_name` (VARCHAR) - from resource attributes

**Map column**:
- `attributes` (MAP(VARCHAR, VARCHAR)) - all other attributes

**Rationale**: Iceberg file-level statistics enable partition pruning on top-level columns, dramatically improving query performance for common filters like `WHERE service_name = 'auth-service'`.

## Component 4: Build System Updates

### 4.1 CMake Dependencies
**File**: `CMakeLists.txt`

Add dependencies:
- `librdkafka` (required, for Redpanda/Kafka client - direct C API usage)
- `DuckDB` (with Iceberg extension support)
- `aws-sdk-cpp` (for S3-compatible storage, if DuckDB doesn't handle it)

Use CPM or FetchContent to manage these dependencies.

### 4.2 Executables
- Keep existing `otel_receiver` (ingester)
- Add new `otel_appender` executable

## Component 5: Configuration Management

### 5.1 Configuration Structure
**File**: `src/config.hpp`

```cpp
struct IngesterConfig {
    std::string queue_brokers;
    std::string queue_topic;
    int max_in_flight;
    // ... other settings
};

struct AppenderConfig {
    std::string queue_brokers;
    std::string queue_topic;
    std::string consumer_group;
    std::string iceberg_catalog_uri;
    std::string s3_endpoint;
    std::string s3_access_key;
    std::string s3_secret_key;
    std::string s3_bucket;
    std::string iceberg_table_name;
    size_t buffer_size_mb;
    int buffer_time_seconds;
};
```

### 5.2 Environment Variables
Support configuration via environment variables (12-factor app style):
- `REDPANDA_BROKERS`
- `REDPANDA_TOPIC`
- `ICEBERG_CATALOG_URI`
- `S3_ENDPOINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_BUCKET`
- `BUFFER_SIZE_MB`, `BUFFER_TIME_SECONDS`

## Component 6: Error Handling & Resilience

### 6.1 Ingester Error Handling
- Queue write failures: retry with exponential backoff (up to N retries)
- Persistent failures: return 503 Service Unavailable
- Backpressure: return 429 Too Many Requests when in-flight limit reached

### 6.2 Appender Error Handling
- Consumer errors: log and continue (consumer group handles retries)
- DuckDB/Iceberg errors: log error, keep message in buffer for retry
- S3 errors: retry with exponential backoff
- Dead letter queue: after N retries, write to separate topic/file

## Component 7: Testing Strategy

### 7.1 Unit Tests
**File**: `tests/test_queue_producer.cpp`
- Test queue producer initialization
- Test message serialization
- Test error handling

**File**: `tests/test_log_transformer.cpp`
- Test OTel protobuf to schema transformation
- Test attribute extraction (well-known vs map)

**File**: `tests/test_buffer_manager.cpp`
- Test size-based flushing
- Test time-based flushing

### 7.2 Integration Tests
**File**: `tests/integration_test.cpp`
- End-to-end test: HTTP → Queue → Appender → Iceberg
- Use embedded Redpanda or testcontainers
- Use local filesystem for Iceberg (development mode)

## Implementation Order

1. **Phase 1: Queue Integration** (Ingester) ✅
   - Add `librdkafka` dependency (direct C API, required)
   - Implement queue producer with `DeliveryReportCb` for delivery tracking
   - Integrate with HTTP server
   - Add configuration management
   - Moved to `src/ingester/` subfolder for better organization

2. **Phase 2: Basic Appender**
   - Implement queue consumer
   - Implement log transformer
   - Basic DuckDB integration (local filesystem first)

3. **Phase 3: Iceberg Integration**
   - Configure DuckDB Iceberg extension
   - Set up REST catalog connection
   - Implement table creation and schema management
   - S3-compatible storage configuration

4. **Phase 4: Buffering & Optimization**
   - Implement buffer manager
   - Add size/time threshold logic
   - Performance tuning

5. **Phase 5: Error Handling & Resilience**
   - Add retry logic
   - Implement backpressure
   - Dead letter queue handling

6. **Phase 6: Testing & Documentation**
   - Unit tests
   - Integration tests
   - Update README with deployment instructions

## Key Dependencies to Add

- **librdkafka** (required): Redpanda/Kafka client - direct C API usage (no wrapper)
- **DuckDB**: Embedded database with Iceberg extension
- **aws-sdk-cpp**: S3-compatible storage (if needed)

## Configuration Files

- `.env.example`: Example environment variables
- `config/config.yaml`: Optional YAML config (future enhancement)

## Notes

- The ingester remains stateless and horizontally scalable behind a load balancer
- The appender scales via Kafka consumer groups (one partition per appender instance)
- DuckDB's Iceberg extension handles Parquet file creation and atomic commits automatically
- Consider adding metrics/observability (Prometheus) in future iterations

## Implementation Todos

1. **Queue Integration**: ✅ Add Redpanda/Kafka producer to ingester: implement queue_producer class, integrate with HTTP server, add backpressure handling
   - Completed: Direct librdkafka integration, DeliveryReportCb implementation, moved to `src/ingester/`, removed conditional compilation

2. **Appender Consumer**: Implement appender service queue consumer: create queue_consumer class, deserialize protobuf messages, implement consumer group coordination

3. **Log Transformer**: Create log transformer: extract fields from OTel protobuf, implement hybrid schema mapping (top-level columns + map column)

4. **DuckDB Integration**: Integrate DuckDB: initialize connection, create Iceberg table with hybrid schema, implement data insertion (depends on: Log Transformer)

5. **Iceberg Catalog**: Configure Iceberg REST catalog: set up catalog connection, configure S3-compatible storage backend (depends on: DuckDB Integration)

6. **Buffer Manager**: Implement buffering logic: track buffer size and time, trigger flushes based on thresholds (100MB or 5 minutes) (depends on: DuckDB Integration)

7. **Config Management**: Add configuration system: environment variable support, config structs for ingester and appender, validation

8. **CMake Dependencies**: ✅ Update CMakeLists.txt: add librdkafka (required, direct API), DuckDB, aws-sdk-cpp dependencies using CPM/FetchContent
   - Completed: Removed cppkafka dependency, made librdkafka required with proper error handling

9. **Error Handling**: Implement error handling: retry logic, backpressure (429/503 responses), dead letter queue for failed messages (depends on: Queue Integration, Appender Consumer)

10. **Testing**: Add tests: unit tests for queue producer, log transformer, buffer manager; integration test for end-to-end flow (depends on: Queue Integration, Appender Consumer, Log Transformer)

## Implementation Changes Log

### Refactoring: Direct librdkafka Integration and Code Organization

**Date**: Implementation session

**Changes Made**:

1. **Replaced cppkafka wrapper with direct librdkafka API**
   - Removed dependency on `mfontanini/cppkafka` wrapper library
   - Updated `queue_producer` to use `librdkafka` C API directly (`rd_kafka_t`, `rd_kafka_conf_t`, etc.)
   - Benefits: Reduced dependency chain, more control over configuration, direct access to librdkafka features

2. **Implemented DeliveryReportCb class for async delivery tracking**
   - Created `DeliveryReportCb` class to handle delivery report callbacks from librdkafka
   - Callback updates `in_flight_count_` atomically on both success and failure
   - Properly configured via `rd_kafka_conf_set_dr_cb()` and `rd_kafka_conf_set_opaque()`
   - Ensures accurate tracking of in-flight messages for backpressure management

3. **Reorganized code structure: moved to `src/ingester/` subfolder**
   - Moved `http_server.hpp` and `http_server.cpp` to `src/ingester/`
   - Moved `queue_producer.hpp` and `queue_producer.cpp` to `src/ingester/`
   - Updated all includes and CMakeLists.txt paths accordingly
   - Better code organization separating ingester components from appender components

4. **Removed conditional compilation for librdkafka**
   - Removed all `#ifdef HAVE_RDKAFKA` compile-time conditionals
   - Made librdkafka a required dependency (build fails if not found)
   - Updated CMakeLists.txt to:
     - Use `REQUIRED` flag in `find_library()` and `find_path()`
     - Fail with `FATAL_ERROR` if librdkafka is not found
     - Remove all `HAVE_RDKAFKA` compile definitions
     - Always link librdkafka (no conditional linking)

**Files Modified**:
- `src/ingester/queue_producer.hpp` - Direct librdkafka includes, DeliveryReportCb class
- `src/ingester/queue_producer.cpp` - Direct librdkafka API usage, removed conditionals
- `src/ingester/http_server.hpp` - Moved from `src/`
- `src/ingester/http_server.cpp` - Moved from `src/`
- `CMakeLists.txt` - Removed cppkafka dependency, made librdkafka required, updated paths
- `src/main.cpp` - Updated includes to `ingester/` subfolder
- `tests/test_http_server.cpp` - Updated includes to `ingester/` subfolder

**Impact**:
- Cleaner codebase without conditional compilation branches
- More explicit dependency management (build fails early if librdkafka missing)
- Better code organization with ingester components grouped together
- Direct librdkafka usage provides more flexibility and control

