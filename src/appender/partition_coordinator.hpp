#ifndef PARTITION_COORDINATOR_HPP
#define PARTITION_COORDINATOR_HPP

#include "../config.hpp"
#include "partition_worker.hpp"
#include "queue_consumer.hpp"
#include "log_transformer.hpp"
#include "iceberg_utils.hpp"
#include "duckdb.hpp"
#include <map>
#include <memory>
#include <mutex>
#include <atomic>
#include <set>

using duckdb::DuckDB;
using duckdb::Connection;

// Coordinates partition workers and consumer
// Handles rebalance events by creating/destroying workers dynamically
class PartitionCoordinator {
public:
    PartitionCoordinator(const AppenderConfig& config);
    ~PartitionCoordinator();

    // Initialize DuckDB, storage configuration, and consumer
    bool initialize();

    // Start processing messages
    // This runs the main polling loop in the current thread
    void start();

    // Stop all workers gracefully
    void stop();

    // Force flush all partitions
    bool forceFlushAll();

    // Check if coordinator is running
    bool isRunning() const { return running_; }

    // Get aggregate stats
    size_t getTotalBufferSize() const;
    size_t getTotalBufferRecordCount() const;

    // Get consumer for external access (e.g., health endpoints)
    QueueConsumer* getConsumer() { return consumer_.get(); }

    // Get the topic name
    const std::string& getTopic() const { return config_.queue_topic; }

private:
    AppenderConfig config_;
    std::string full_table_name_;

    // Shared DuckDB instance (connections are per-worker)
    std::unique_ptr<DuckDB> db_;
    std::unique_ptr<Connection> main_conn_;  // For catalog operations

    // Consumer
    std::unique_ptr<QueueConsumer> consumer_;

    // Active workers by partition
    std::map<int32_t, std::unique_ptr<PartitionWorker>> workers_;
    std::mutex workers_mutex_;

    // Pending offset commits (partition -> offset)
    std::map<int32_t, int64_t> pending_commits_;
    std::mutex commits_mutex_;

    // Running state
    std::atomic<bool> running_;
    std::atomic<bool> stop_requested_;

    // Create worker for a partition
    void createWorker(int32_t partition);

    // Destroy worker for a partition
    void destroyWorker(int32_t partition);

    // Handle partition assignment (rebalance)
    void onPartitionsAssigned(const std::vector<int32_t>& partitions);

    // Handle partition revocation (rebalance)
    void onPartitionsRevoked(const std::vector<int32_t>& partitions);

    // Handle committed offset notification from worker
    void onOffsetCommitted(int32_t partition, int64_t offset);

    // Commit pending offsets to Kafka
    void commitPendingOffsets();

    // Process a single message from consumer
    void processMessage(const opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest& request,
                        const KafkaMessageMeta& meta);
};

#endif // PARTITION_COORDINATOR_HPP
