# Multi-Partition Threading Model for Appender

**Date:** 2026-01-28
**Task:** Implement per-partition worker threads for parallel Kafka message processing in the appender

## Problem Statement

The appender used a single-threaded main processing loop where one Kafka consumer polled all partitions sequentially, one shared DuckDB buffer table held records from all partitions, and a mutex serialized all DuckDB operations. This created a bottleneck where all message processing, buffering, and Iceberg writes were sequential.

The goal was to implement a multi-partition threading model where each partition gets its own worker thread with independent DuckDB connection and buffer table, enabling true parallelism.

## User Prompts

1. Implement the multi-partition threading plan (provided detailed architecture plan covering worker threads, coordinator, rebalance handling, retry logic)
2. Finish the unit testing task and fix the existing unit test issue

## Actions Taken

### Files Modified/Created

| File | Changes |
|------|---------|
| `src/config.hpp` | Added 6 new config options for partition threading (buffer size/time, retry settings, rebalance timeout) |
| `src/appender/iceberg_utils.hpp` | **NEW** - Extracted SQL utilities class with escapeSqlString, formatTimestamp, formatAttributesMap, etc. |
| `src/appender/iceberg_utils.cpp` | **NEW** - Implementation of shared utilities for SQL building and storage configuration |
| `src/appender/partition_worker.hpp` | **NEW** - Worker thread class owning DuckDB connection, buffer table, message queue |
| `src/appender/partition_worker.cpp` | **NEW** - Worker implementation with enqueue, flush, retry logic with exponential backoff |
| `src/appender/partition_coordinator.hpp` | **NEW** - Coordinator managing workers, routing messages, handling rebalance |
| `src/appender/partition_coordinator.cpp` | **NEW** - Coordinator implementation with dynamic worker creation/destruction |
| `src/appender/queue_consumer.hpp` | Added rebalance callback types, seekPartition, commitPartitionOffset methods |
| `src/appender/queue_consumer.cpp` | Implemented rebalance callbacks via cppkafka's set_assignment_callback/set_revocation_callback |
| `src/appender/main.cpp` | Replaced single-threaded logic with PartitionCoordinator, updated health endpoints |
| `CMakeLists.txt` | Added new source files and test targets |
| `tests/test_buffer_manager.cpp` | Fixed SizeThreshold test (was adding 1000 bytes expecting to exceed 1024 threshold) |
| `tests/test_iceberg_utils.cpp` | **NEW** - 19 unit tests for SQL utilities |
| `tests/test_partition_worker.cpp` | **NEW** - 11 unit tests for worker lifecycle, queue, concurrency |

### Key Implementation Details

**Architecture:**
```
Main Thread
    |
    +-- consumer.poll() --> KafkaMessage
    |
    +-- Transform message --> TransformedLogRecord
    |
    +-- Dispatch to workers_[partition].enqueue(records, offset)

PartitionWorker N (separate thread)
    |
    +-- Dequeue from queue (blocking wait with condition variable)
    |
    +-- Insert to local_buffer_N (per-worker DuckDB connection)
    |
    +-- Check flush thresholds (size OR time OR forced)
    |
    +-- Flush to Iceberg with retry (exponential backoff + jitter)
    |
    +-- Update committed offset
    |
    +-- Signal coordinator to commit Kafka offset
```

**Retry Logic:**
- Exponential backoff with jitter for Iceberg commit conflicts
- Configurable retries (default 5), base delay (100ms), max delay (5000ms)
- `calculateBackoff()` uses `base * 2^attempt` capped at max, plus 0-50% jitter

**Rebalance Handling:**
- Assignment callback creates workers, queries Iceberg for recovery offset, seeks consumer
- Revocation callback commits pending offsets, stops workers gracefully with timeout

**New Configuration Options:**
```cpp
size_t partition_buffer_size_mb = 50;      // Per-partition buffer threshold
int partition_buffer_time_seconds = 60;     // Per-partition time threshold
int iceberg_commit_retries = 5;             // Max retry attempts
int iceberg_retry_base_delay_ms = 100;      // Base backoff delay
int iceberg_retry_max_delay_ms = 5000;      // Max backoff cap
int rebalance_timeout_seconds = 30;         // Worker shutdown timeout
```

### Commands Run

```bash
# Configure and build
cd build && cmake .. -GNinja -DCMAKE_POLICY_VERSION_MINIMUM=3.5
ninja otel_appender

# Build and run tests
ninja iceberg_utils_test && ./iceberg_utils_test
ninja partition_worker_test && ./partition_worker_test
ctest --output-on-failure

# Commit
git add <files>
git commit -m "Implement multi-partition threading model for appender"
```

## Summary

Successfully implemented a multi-partition threading model that enables parallel processing of Kafka partitions. Each partition now has its own worker thread with independent DuckDB connection and buffer table, eliminating the single-threaded bottleneck.

Key accomplishments:
- Created PartitionWorker class with thread-safe message queue and flush logic
- Created PartitionCoordinator for dynamic worker management during rebalance
- Extracted IcebergUtils for shared SQL operations
- Added exponential backoff retry for Iceberg commit conflicts
- Fixed existing test bug and added 30 new unit tests (19 for IcebergUtils, 11 for PartitionWorker)
- All 5 test suites pass (100% success rate)

Commit: `e20777e` - 14 files changed, 2022 insertions(+), 144 deletions(-)
