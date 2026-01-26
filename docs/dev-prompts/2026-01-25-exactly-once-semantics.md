# Exactly-Once Semantics for Kafka-to-Iceberg Appender

**Date:** 2026-01-25
**Task:** Implement exactly-once delivery semantics for the appender service

## Problem Statement

The appender was consuming from Kafka and flushing to Iceberg, but offsets were committed **per-message immediately after processing** (`queue_consumer.cpp:82`), not after the Iceberg flush. This caused:

1. **Data Loss**: If appender crashes after offset commit but before Iceberg flush, buffered data is lost
2. **Duplicates**: If Iceberg flush succeeds but appender crashes before offset commit, data is re-processed on restart

**Previous flow (buggy):**
```
Poll message → Transform → Add to buffer → COMMIT OFFSET → ... → Eventually flush to Iceberg
```

**New flow (exactly-once):**
```
Poll message → Transform → Add to buffer → Track offset → ... → Flush to Iceberg → COMMIT OFFSET
```

## Solution: Idempotent Writes with Per-Record Kafka Offset Columns

The chosen approach embeds Kafka coordinates (`_kafka_topic`, `_kafka_partition`, `_kafka_offset`) directly into each Iceberg record. On startup, the appender queries Iceberg for the maximum offset per partition and seeks the Kafka consumer past already-written data.

## Files Modified

| File | Changes |
|------|---------|
| `src/appender/queue_consumer.hpp` | Added `KafkaMessageMeta` struct, updated `MessageCallback` signature, added offset tracking and seek methods |
| `src/appender/queue_consumer.cpp` | Removed per-message commit, implemented `trackOffset()`, `commitPendingOffsets()`, `clearPendingOffsets()`, `seekToOffsets()` |
| `src/appender/log_transformer.hpp` | Added `kafka_topic`, `kafka_partition`, `kafka_offset` fields to `TransformedLogRecord` |
| `src/appender/log_transformer.cpp` | Updated `transform()` to accept and propagate Kafka metadata |
| `src/appender/iceberg_appender.hpp` | Added `getMaxCommittedOffsets()` declaration |
| `src/appender/iceberg_appender.cpp` | Updated staging/Iceberg table schemas with Kafka columns, added recovery query |
| `src/appender/main.cpp` | Added startup recovery sequence, deferred offset commit flow |

## Key Implementation Details

### 1. QueueConsumer Changes

New callback signature passes Kafka metadata:
```cpp
using MessageCallback = std::function<void(
    const ExportLogsServiceRequest&,
    const KafkaMessageMeta&)>;
```

New offset tracking methods:
```cpp
void trackOffset(int32_t partition, int64_t offset);
std::map<int32_t, int64_t> getPendingOffsets() const;
bool commitPendingOffsets();
void clearPendingOffsets();
bool seekToOffsets(const std::map<int32_t, int64_t>& offsets);
```

### 2. Schema Changes

Added three columns to both staging and Iceberg tables:
```sql
_kafka_topic VARCHAR,
_kafka_partition INTEGER,
_kafka_offset BIGINT
```

### 3. Startup Recovery Query

```cpp
std::map<int32_t, int64_t> IcebergAppender::getMaxCommittedOffsets(const std::string& topic) {
    // SELECT _kafka_partition, MAX(_kafka_offset) as max_offset
    // FROM iceberg_table WHERE _kafka_topic = ? GROUP BY _kafka_partition
}
```

### 4. Main Orchestration

**Startup:**
```cpp
auto max_offsets = appender.getMaxCommittedOffsets(consumer.getTopic());
consumer.seekToOffsets(max_offsets);
```

**Flush flow:**
```cpp
if (appender.flush()) {
    if (consumer.commitPendingOffsets()) {
        consumer.clearPendingOffsets();
    }
    buffer_manager.reset();
}
```

## Failure Scenarios & Recovery

| Crash Point | Iceberg State | Kafka Offset | Recovery |
|-------------|---------------|--------------|----------|
| During buffer accumulation | No data | Not committed | Re-consume batch |
| During Iceberg flush (partial) | Partial data | Not committed | Query shows partial max; re-consume rest |
| After Iceberg flush, before Kafka commit | Full data | Not committed | Query shows all data; seek past it |
| After Kafka commit | Full data | Committed | Normal operation |

## Build Verification

```bash
cmake .. -GNinja -DCMAKE_POLICY_VERSION_MINIMUM=3.5
ninja otel_appender
# Build completed successfully (432 steps)
```

## Prompt Used

The implementation was guided by a detailed plan document that specified:
- The exactly-once semantics approach (per-record Kafka offset columns)
- Step-by-step implementation order
- File-by-file changes required
- Recovery query design
- Failure scenario analysis

## Notes

- The Iceberg table becomes the source of truth for what data has been persisted
- Even if Kafka offsets are stale, the recovery query finds what was actually written
- New partitions with no Iceberg data use `auto.offset.reset: earliest`
- The `/flush` HTTP endpoint was also updated to commit offsets after flush
