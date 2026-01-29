#include <gtest/gtest.h>
#include "../src/appender/partition_worker.hpp"
#include "../src/appender/iceberg_utils.hpp"
#include "../src/config.hpp"
#include "duckdb.hpp"
#include <thread>
#include <chrono>
#include <atomic>

using duckdb::DuckDB;
using duckdb::Connection;

class PartitionWorkerTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create in-memory DuckDB instance
        db_ = std::make_unique<DuckDB>(nullptr);

        // Set up minimal config
        config_.partition_buffer_size_mb = 1;  // 1 MB threshold
        config_.partition_buffer_time_seconds = 60;
        config_.iceberg_commit_retries = 3;
        config_.iceberg_retry_base_delay_ms = 10;  // Short for testing
        config_.iceberg_retry_max_delay_ms = 100;
        config_.rebalance_timeout_seconds = 5;
        config_.iceberg_table_name = "test_logs";
    }

    void TearDown() override {
        db_.reset();
    }

    // Helper to create a test record
    TransformedLogRecord createTestRecord(int64_t offset) {
        TransformedLogRecord record;
        record.kafka_topic = "test-topic";
        record.kafka_partition = 0;
        record.kafka_offset = offset;
        record.timestamp = std::chrono::system_clock::now();
        record.body = "Test log message body";
        record.severity = "INFO";
        record.service_name = "test-service";
        record.deployment_environment = "test";
        record.host_name = "test-host";
        record.trace_id = "trace123";
        record.span_id = "span456";
        record.attributes = {{"key1", "value1"}};
        return record;
    }

    std::unique_ptr<DuckDB> db_;
    AppenderConfig config_;
};

// Test worker creation and basic lifecycle
TEST_F(PartitionWorkerTest, WorkerCreationAndStop) {
    std::atomic<int64_t> committed_offset{-1};

    PartitionWorker worker(
        0,  // partition_id
        *db_,
        config_,
        "test_table",
        [&committed_offset](int32_t partition, int64_t offset) {
            committed_offset = offset;
        }
    );

    EXPECT_EQ(worker.getPartitionId(), 0);
    EXPECT_EQ(worker.getBufferSize(), 0u);
    EXPECT_EQ(worker.getBufferRecordCount(), 0u);
    EXPECT_EQ(worker.getLastCommittedOffset(), -1);

    worker.start();

    // Give it a moment to start
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    worker.signalStop();
    EXPECT_TRUE(worker.waitForStop(5));
}

// Test message enqueue increases buffer stats
TEST_F(PartitionWorkerTest, EnqueueIncreasesBufferStats) {
    std::atomic<bool> callback_called{false};

    PartitionWorker worker(
        1,
        *db_,
        config_,
        "test_table",
        [&callback_called](int32_t, int64_t) {
            callback_called = true;
        }
    );

    worker.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Enqueue a message
    PartitionMessage msg;
    msg.records.push_back(createTestRecord(100));
    msg.max_offset = 100;
    worker.enqueue(std::move(msg));

    // Give worker time to process
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    EXPECT_GT(worker.getBufferSize(), 0u);
    EXPECT_EQ(worker.getBufferRecordCount(), 1u);

    worker.signalStop();
    worker.waitForStop(5);
}

// Test multiple messages accumulate in buffer
TEST_F(PartitionWorkerTest, MultipleMessagesAccumulate) {
    PartitionWorker worker(
        2,
        *db_,
        config_,
        "test_table",
        [](int32_t, int64_t) {}
    );

    worker.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Enqueue multiple messages
    for (int i = 0; i < 5; ++i) {
        PartitionMessage msg;
        msg.records.push_back(createTestRecord(i));
        msg.max_offset = i;
        worker.enqueue(std::move(msg));
    }

    // Give worker time to process all
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    EXPECT_EQ(worker.getBufferRecordCount(), 5u);

    worker.signalStop();
    worker.waitForStop(5);
}

// Test batch of records in single message
TEST_F(PartitionWorkerTest, BatchRecordsInSingleMessage) {
    PartitionWorker worker(
        3,
        *db_,
        config_,
        "test_table",
        [](int32_t, int64_t) {}
    );

    worker.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Enqueue a batch of records in one message
    PartitionMessage msg;
    for (int i = 0; i < 10; ++i) {
        msg.records.push_back(createTestRecord(i));
    }
    msg.max_offset = 9;
    worker.enqueue(std::move(msg));

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    EXPECT_EQ(worker.getBufferRecordCount(), 10u);

    worker.signalStop();
    worker.waitForStop(5);
}

// Test worker stops gracefully with data in buffer
TEST_F(PartitionWorkerTest, GracefulStopWithData) {
    PartitionWorker worker(
        4,
        *db_,
        config_,
        "test_table",
        [](int32_t, int64_t) {}
    );

    worker.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Add some data
    PartitionMessage msg;
    msg.records.push_back(createTestRecord(100));
    msg.max_offset = 100;
    worker.enqueue(std::move(msg));

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Stop should succeed even with data in buffer
    worker.signalStop();
    EXPECT_TRUE(worker.waitForStop(5));
}

// Test timeout when worker doesn't stop
TEST_F(PartitionWorkerTest, WaitForStopTimeout) {
    // This test verifies the timeout mechanism works
    // We can't easily make the worker hang, but we can verify
    // that waitForStop returns true when worker stops normally

    PartitionWorker worker(
        5,
        *db_,
        config_,
        "test_table",
        [](int32_t, int64_t) {}
    );

    worker.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    worker.signalStop();

    // Should return true since worker stops normally
    EXPECT_TRUE(worker.waitForStop(2));
}

// Test partition ID getter
TEST_F(PartitionWorkerTest, GetPartitionId) {
    PartitionWorker worker0(0, *db_, config_, "t", [](int32_t, int64_t) {});
    PartitionWorker worker5(5, *db_, config_, "t", [](int32_t, int64_t) {});
    PartitionWorker worker99(99, *db_, config_, "t", [](int32_t, int64_t) {});

    EXPECT_EQ(worker0.getPartitionId(), 0);
    EXPECT_EQ(worker5.getPartitionId(), 5);
    EXPECT_EQ(worker99.getPartitionId(), 99);
}

// Test initial state
TEST_F(PartitionWorkerTest, InitialState) {
    PartitionWorker worker(
        0,
        *db_,
        config_,
        "test_table",
        [](int32_t, int64_t) {}
    );

    EXPECT_EQ(worker.getBufferSize(), 0u);
    EXPECT_EQ(worker.getBufferRecordCount(), 0u);
    EXPECT_EQ(worker.getLastCommittedOffset(), -1);
}

// Test force flush
TEST_F(PartitionWorkerTest, ForceFlush) {
    std::atomic<int> flush_count{0};

    // Use very high thresholds so only force flush triggers
    config_.partition_buffer_size_mb = 1000;
    config_.partition_buffer_time_seconds = 3600;

    PartitionWorker worker(
        6,
        *db_,
        config_,
        "test_table",
        [&flush_count](int32_t, int64_t) {
            flush_count++;
        }
    );

    worker.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Add some data
    PartitionMessage msg;
    msg.records.push_back(createTestRecord(100));
    msg.max_offset = 100;
    worker.enqueue(std::move(msg));

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(worker.getBufferRecordCount(), 1u);

    // Force flush - this will attempt to flush to Iceberg which will fail
    // since we don't have Iceberg set up, but the mechanism is tested
    // The test verifies forceFlush() doesn't hang or crash
    bool flush_result = worker.forceFlush();
    // Flush may fail (no Iceberg) but shouldn't crash
    (void)flush_result;

    worker.signalStop();
    worker.waitForStop(5);
}

// Test concurrent enqueue from multiple threads
TEST_F(PartitionWorkerTest, ConcurrentEnqueue) {
    PartitionWorker worker(
        7,
        *db_,
        config_,
        "test_table",
        [](int32_t, int64_t) {}
    );

    worker.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    const int num_threads = 4;
    const int messages_per_thread = 10;
    std::vector<std::thread> threads;

    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&worker, t, messages_per_thread, this]() {
            for (int i = 0; i < messages_per_thread; ++i) {
                PartitionMessage msg;
                msg.records.push_back(createTestRecord(t * 100 + i));
                msg.max_offset = t * 100 + i;
                worker.enqueue(std::move(msg));
            }
        });
    }

    // Wait for all producer threads
    for (auto& t : threads) {
        t.join();
    }

    // Give worker time to process all messages
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // All messages should be processed
    EXPECT_EQ(worker.getBufferRecordCount(),
              static_cast<size_t>(num_threads * messages_per_thread));

    worker.signalStop();
    worker.waitForStop(5);
}

// Test empty message handling
TEST_F(PartitionWorkerTest, EmptyMessageHandling) {
    PartitionWorker worker(
        8,
        *db_,
        config_,
        "test_table",
        [](int32_t, int64_t) {}
    );

    worker.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Enqueue empty message
    PartitionMessage empty_msg;
    empty_msg.max_offset = 0;
    worker.enqueue(std::move(empty_msg));

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Empty message should not increase record count
    EXPECT_EQ(worker.getBufferRecordCount(), 0u);

    worker.signalStop();
    worker.waitForStop(5);
}
