#ifndef PARTITION_WORKER_HPP
#define PARTITION_WORKER_HPP

#include "../config.hpp"
#include "log_transformer.hpp"
#include "iceberg_utils.hpp"
#include "duckdb.hpp"
#include <queue>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <atomic>
#include <chrono>
#include <functional>

using duckdb::DuckDB;
using duckdb::Connection;

// Message envelope for partition worker queue
struct PartitionMessage {
    std::vector<TransformedLogRecord> records;
    int64_t max_offset;  // Max offset in this batch
};

// Callback for notifying coordinator of committed offsets
using OffsetCommitCallback = std::function<void(int32_t partition, int64_t offset)>;

// Worker thread for a single Kafka partition
// Each worker has its own DuckDB connection and buffer table
class PartitionWorker {
public:
    PartitionWorker(int32_t partition_id,
                    DuckDB& db,
                    const AppenderConfig& config,
                    const std::string& full_table_name,
                    OffsetCommitCallback commit_callback);
    ~PartitionWorker();

    // Start the worker thread
    void start();

    // Thread-safe message enqueue
    void enqueue(PartitionMessage msg);

    // Signal worker to stop (graceful)
    void signalStop();

    // Wait for worker to stop (with timeout)
    // Returns true if stopped cleanly, false if timeout
    bool waitForStop(int timeout_seconds);

    // Force flush and wait for completion
    bool forceFlush();

    // Get current buffer stats
    size_t getBufferSize() const { return buffer_size_bytes_.load(); }
    size_t getBufferRecordCount() const { return buffer_records_.load(); }
    int64_t getLastCommittedOffset() const { return committed_offset_.load(); }
    int32_t getPartitionId() const { return partition_id_; }

    // Query max committed offset for this partition from Iceberg
    int64_t recoverMaxOffset(const std::string& topic);

private:
    int32_t partition_id_;
    const AppenderConfig& config_;
    std::string full_table_name_;
    std::string buffer_table_name_;
    OffsetCommitCallback commit_callback_;

    // DuckDB connection (per-worker for parallelism)
    std::unique_ptr<Connection> conn_;

    // Message queue
    std::queue<PartitionMessage> queue_;
    std::mutex queue_mutex_;
    std::condition_variable queue_cv_;

    // Worker thread
    std::thread worker_thread_;
    std::atomic<bool> running_;
    std::atomic<bool> stop_requested_;
    std::atomic<bool> flush_requested_;

    // Buffer tracking
    std::atomic<size_t> buffer_size_bytes_;
    std::atomic<size_t> buffer_records_;
    std::chrono::system_clock::time_point last_flush_time_;
    std::mutex flush_time_mutex_;

    // Offset tracking
    std::atomic<int64_t> pending_offset_;     // Max offset in buffer
    std::atomic<int64_t> committed_offset_;   // Max offset successfully flushed

    // Main worker loop
    void run();

    // Process a single message batch
    void processMessage(const PartitionMessage& msg);

    // Insert records into buffer table
    bool insertToBuffer(const std::vector<TransformedLogRecord>& records);

    // Check if flush thresholds are met
    bool shouldFlush() const;

    // Flush buffer to Iceberg with retry logic
    bool flushWithRetry();

    // Single flush attempt
    bool attemptFlush();

    // Clear buffer table
    bool clearBuffer();

    // Calculate backoff delay with jitter
    std::chrono::milliseconds calculateBackoff(int attempt) const;
};

#endif // PARTITION_WORKER_HPP
