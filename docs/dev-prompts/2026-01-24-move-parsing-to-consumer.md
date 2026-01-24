# Move Payload Parsing from HTTP Server to Queue Consumer

**Date:** 2026-01-24

## User Prompt 1

> Right now, the heavylifting of decompressing the gzip payload. Parsing Json/Protobuf into the ExportLogsServiceRequest is all done in the http server on the ingester side in the file src/ingester/http_server.cpp. I want to move the heavy lifting to the queue consumer side. In the http server, simply read the request headers to determine the content encoding and the content type and produce the message immediately. The produced message should be a new protobuf that has fields contentType, contentEncoding and array of bytes. The consumer should read this wrapper protobuf and do the required processing to extract the ExportLogsServiceRequest from the wrapper protobuf. First, analyze if this is the most efficient approach. Give me an overview of the changes being planned and then execute the changes.

### Analysis Provided

**Pros:**
- Lower HTTP latency (server responds faster)
- Better scalability (HTTP servers become thin proxies)
- Efficient Kafka storage (gzipped payloads stay compressed)
- Separation of concerns

**Cons:**
- Delayed error detection (clients won't know if payload is malformed)
- Consumer complexity increases
- Double compression (gzip inside Kafka's snappy) is inefficient

---

## User Prompt 2 (Refinement)

> If Gzip inside snappy is not efficient, can we move the gzip decompression alone to the http server and store the uncompressed raw bytes in the wrapper payload? Get rid of the content encoding field from the wrapper. Introduce a new enum field called telemetrytype. At present the enum should only have OTEL_LOGS as the type.

### Revised Approach

- Keep gzip decompression in HTTP server (avoids double compression)
- Remove `content_encoding` field from wrapper
- Add `telemetry_type` enum with `OTEL_LOGS`
- Store uncompressed raw bytes in wrapper payload

---

## Completed Changes

### New Files

| File | Description |
|------|-------------|
| `proto/telemetry_wrapper.proto` | New wrapper protobuf with `content_type`, `telemetry_type` enum, and `payload` bytes |

### Modified Files

| File | Changes |
|------|---------|
| `CMakeLists.txt` | Added custom proto compilation, fixed appender cppkafka linking |
| `src/ingester/http_server.cpp` | Removed JSON/Protobuf parsing, keeps gzip decompression, creates wrapper |
| `src/ingester/queue_producer.hpp/cpp` | Changed to accept `RawTelemetryMessage` instead of `ExportLogsServiceRequest` |
| `src/appender/queue_consumer.hpp/cpp` | Added wrapper deserialization and payload parsing based on content_type |
| `tests/test_http_server.cpp` | Updated tests for new behavior (invalid payloads now return 200) |
| `src/appender/iceberg_appender.hpp/cpp` | Fixed pre-existing DuckDB include issues |

### Architecture Change

```
Before:
HTTP Server: Receive → Decompress → Parse JSON/Proto → Serialize → Kafka
Consumer: Deserialize → Process

After:
HTTP Server: Receive → Decompress → Wrap raw bytes → Serialize → Kafka
Consumer: Deserialize wrapper → Parse JSON/Proto → Process
```

### New Protobuf Schema

```protobuf
enum TelemetryType {
    TELEMETRY_TYPE_UNSPECIFIED = 0;
    OTEL_LOGS = 1;
}

message RawTelemetryMessage {
    string content_type = 1;      // "application/json" or "application/x-protobuf"
    TelemetryType telemetry_type = 2;
    bytes payload = 3;            // decompressed raw bytes
}
```

### Test Results

All 14 HTTP server tests pass. Both `otel_receiver` and `otel_appender` binaries build successfully.

### Trade-offs Accepted

1. **Delayed validation** - Invalid JSON/Protobuf payloads are now accepted by HTTP server (return 200) and will fail at consumer level. Errors go to Dead Letter Queue instead of HTTP 400.
2. **Consumer complexity** - Consumer now handles content-type detection and JSON/Protobuf parsing.
