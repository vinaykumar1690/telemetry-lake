# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

```bash
# Configure and build (from repo root)
cd build && cmake .. -GNinja -DCMAKE_POLICY_VERSION_MINIMUM=3.5 && ninja

# Build specific targets
ninja otel_receiver      # HTTP ingester
ninja otel_appender      # Kafka consumer/Iceberg writer

# Run tests
ctest --output-on-failure
./http_server_test       # Run single test
```

The `-DCMAKE_POLICY_VERSION_MINIMUM=3.5` flag is required for cppkafka compatibility.

## Architecture

Telemetry Lake is a C++ pipeline for ingesting OpenTelemetry logs and writing them to Apache Iceberg tables with exactly-once semantics.

```
OTel Agents → Ingester (HTTP :4318) → Kafka → Appender → Iceberg (S3/MinIO)
```

**Key design decisions:**
- Ingester wraps raw payloads in `RawTelemetryMessage` (proto/telemetry_wrapper.proto) without parsing - parsing is deferred to the appender
- Appender implements exactly-once semantics: Kafka offsets are committed only AFTER successful Iceberg flush
- On startup, appender queries Iceberg for max committed offset per partition and seeks to that position for recovery
- DuckDB is used as the transformation and buffering layer before Iceberg writes

**Components:**
- `otel_receiver` (src/main.cpp + src/ingester/): Crow HTTP server that accepts OTLP logs and produces to Kafka
- `otel_appender` (src/appender/main.cpp + src/appender/): Kafka consumer that transforms logs via DuckDB and flushes to Iceberg

## Configuration

Both services load config from environment via `IngesterConfig::fromEnv()` and `AppenderConfig::fromEnv()` in src/config.hpp.

**Ingester required:** `KAFKA_BROKERS`
**Appender required:** `KAFKA_BROKERS`, `ICEBERG_CATALOG_URI`, `S3_ENDPOINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_BUCKET`

## Key Files

- `src/config.hpp` - Configuration structs for both services
- `src/appender/log_transformer.hpp` - `TransformedLogRecord` struct (the normalized log format)
- `src/appender/iceberg_appender.cpp` - DuckDB + Iceberg integration with exactly-once logic
- `proto/telemetry_wrapper.proto` - Kafka message wrapper format

## Signal Handling

Appender supports: `SIGINT/SIGTERM` (graceful shutdown), `SIGUSR1` (force flush)
