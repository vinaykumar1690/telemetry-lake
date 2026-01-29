#include "partition_coordinator.hpp"
#include <iostream>
#include <algorithm>

PartitionCoordinator::PartitionCoordinator(const AppenderConfig& config)
    : config_(config)
    , running_(false)
    , stop_requested_(false) {
    full_table_name_ = IcebergUtils::getFullTableName(config.iceberg_table_name);
}

PartitionCoordinator::~PartitionCoordinator() {
    stop();
}

bool PartitionCoordinator::initialize() {
    try {
        // Initialize shared DuckDB instance (in-memory for speed)
        db_ = std::make_unique<DuckDB>(nullptr);
        main_conn_ = std::make_unique<Connection>(*db_);

        // Load extensions on main connection
        if (!IcebergUtils::loadExtensions(*main_conn_)) {
            std::cerr << "Failed to load DuckDB extensions" << std::endl;
            return false;
        }

        // Configure storage on main connection
        if (!IcebergUtils::configureStorage(*main_conn_, config_)) {
            std::cerr << "Failed to configure storage" << std::endl;
            return false;
        }

        // Create Iceberg table if it doesn't exist
        if (!IcebergUtils::createIcebergTableIfNotExists(*main_conn_, full_table_name_)) {
            std::cerr << "Failed to create Iceberg table" << std::endl;
            return false;
        }

        // Initialize consumer
        consumer_ = std::make_unique<QueueConsumer>(config_);
        if (!consumer_->initialize()) {
            std::cerr << "Failed to initialize queue consumer" << std::endl;
            return false;
        }

        // Set up rebalance callbacks
        consumer_->setAssignmentCallback([this](const std::vector<int32_t>& partitions) {
            onPartitionsAssigned(partitions);
        });

        consumer_->setRevocationCallback([this](const std::vector<int32_t>& partitions) {
            onPartitionsRevoked(partitions);
        });

        std::cout << "PartitionCoordinator initialized successfully" << std::endl;
        std::cout << "Iceberg table: " << full_table_name_ << std::endl;
        return true;
    } catch (const std::exception& e) {
        std::cerr << "Failed to initialize PartitionCoordinator: " << e.what() << std::endl;
        return false;
    }
}

void PartitionCoordinator::start() {
    if (running_) {
        std::cerr << "Coordinator is already running" << std::endl;
        return;
    }

    running_ = true;
    stop_requested_ = false;

    std::cout << "Starting partition coordinator..." << std::endl;
    std::cout << "Per-partition settings: " << config_.partition_buffer_size_mb << " MB or "
              << config_.partition_buffer_time_seconds << " seconds" << std::endl;
    std::cout << "Iceberg commit retries: " << config_.iceberg_commit_retries
              << " (base delay: " << config_.iceberg_retry_base_delay_ms << "ms)" << std::endl;

    // Start consuming messages - the callback dispatches to workers
    consumer_->start([this](const opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest& request,
                            const KafkaMessageMeta& meta) {
        if (!running_ || stop_requested_) {
            return;
        }
        processMessage(request, meta);
    });

    running_ = false;
    std::cout << "Partition coordinator stopped" << std::endl;
}

void PartitionCoordinator::stop() {
    if (!running_ && workers_.empty()) {
        return;
    }

    std::cout << "Stopping partition coordinator..." << std::endl;
    stop_requested_ = true;

    // Stop consumer first
    if (consumer_) {
        consumer_->stop();
    }

    // Stop all workers
    {
        std::lock_guard<std::mutex> lock(workers_mutex_);
        for (auto& kv : workers_) {
            kv.second->signalStop();
        }

        for (auto& kv : workers_) {
            if (!kv.second->waitForStop(config_.rebalance_timeout_seconds)) {
                std::cerr << "Partition " << kv.first << ": Worker did not stop cleanly" << std::endl;
            }
        }

        workers_.clear();
    }

    // Final offset commit
    commitPendingOffsets();

    running_ = false;
    std::cout << "Partition coordinator stopped" << std::endl;
}

bool PartitionCoordinator::forceFlushAll() {
    std::lock_guard<std::mutex> lock(workers_mutex_);
    bool all_success = true;

    for (auto& kv : workers_) {
        if (!kv.second->forceFlush()) {
            std::cerr << "Partition " << kv.first << ": Force flush failed" << std::endl;
            all_success = false;
        }
    }

    if (all_success) {
        commitPendingOffsets();
    }

    return all_success;
}

size_t PartitionCoordinator::getTotalBufferSize() const {
    std::lock_guard<std::mutex> lock(const_cast<std::mutex&>(workers_mutex_));
    size_t total = 0;
    for (const auto& kv : workers_) {
        total += kv.second->getBufferSize();
    }
    return total;
}

size_t PartitionCoordinator::getTotalBufferRecordCount() const {
    std::lock_guard<std::mutex> lock(const_cast<std::mutex&>(workers_mutex_));
    size_t total = 0;
    for (const auto& kv : workers_) {
        total += kv.second->getBufferRecordCount();
    }
    return total;
}

void PartitionCoordinator::createWorker(int32_t partition) {
    std::lock_guard<std::mutex> lock(workers_mutex_);

    if (workers_.find(partition) != workers_.end()) {
        std::cout << "Partition " << partition << ": Worker already exists" << std::endl;
        return;
    }

    auto worker = std::make_unique<PartitionWorker>(
        partition,
        *db_,
        config_,
        full_table_name_,
        [this](int32_t p, int64_t offset) { onOffsetCommitted(p, offset); }
    );

    // Recover max offset from Iceberg and seek consumer
    int64_t max_offset = worker->recoverMaxOffset(config_.queue_topic);
    if (max_offset >= 0) {
        consumer_->seekPartition(partition, max_offset + 1);
    }

    worker->start();
    workers_[partition] = std::move(worker);

    std::cout << "Partition " << partition << ": Created worker" << std::endl;
}

void PartitionCoordinator::destroyWorker(int32_t partition) {
    std::unique_lock<std::mutex> lock(workers_mutex_);

    auto it = workers_.find(partition);
    if (it == workers_.end()) {
        return;
    }

    auto worker = std::move(it->second);
    workers_.erase(it);
    lock.unlock();

    // Stop worker gracefully
    worker->signalStop();
    if (!worker->waitForStop(config_.rebalance_timeout_seconds)) {
        std::cerr << "Partition " << partition << ": Worker did not stop cleanly during rebalance" << std::endl;
    }

    std::cout << "Partition " << partition << ": Destroyed worker" << std::endl;
}

void PartitionCoordinator::onPartitionsAssigned(const std::vector<int32_t>& partitions) {
    std::cout << "Partitions assigned: ";
    for (int32_t p : partitions) {
        std::cout << p << " ";
    }
    std::cout << std::endl;

    for (int32_t partition : partitions) {
        createWorker(partition);
    }
}

void PartitionCoordinator::onPartitionsRevoked(const std::vector<int32_t>& partitions) {
    std::cout << "Partitions revoked: ";
    for (int32_t p : partitions) {
        std::cout << p << " ";
    }
    std::cout << std::endl;

    // Commit pending offsets before losing partitions
    commitPendingOffsets();

    for (int32_t partition : partitions) {
        destroyWorker(partition);
    }
}

void PartitionCoordinator::onOffsetCommitted(int32_t partition, int64_t offset) {
    std::lock_guard<std::mutex> lock(commits_mutex_);

    auto it = pending_commits_.find(partition);
    if (it == pending_commits_.end() || offset > it->second) {
        pending_commits_[partition] = offset;
    }
}

void PartitionCoordinator::commitPendingOffsets() {
    std::map<int32_t, int64_t> to_commit;
    {
        std::lock_guard<std::mutex> lock(commits_mutex_);
        to_commit = std::move(pending_commits_);
        pending_commits_.clear();
    }

    if (to_commit.empty()) {
        return;
    }

    // Use consumer's existing commit mechanism
    for (const auto& kv : to_commit) {
        consumer_->trackOffset(kv.first, kv.second);
    }

    if (consumer_->commitPendingOffsets()) {
        consumer_->clearPendingOffsets();
        std::cout << "Committed offsets for " << to_commit.size() << " partition(s)" << std::endl;
    } else {
        std::cerr << "Failed to commit offsets to Kafka" << std::endl;
    }
}

void PartitionCoordinator::processMessage(
    const opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest& request,
    const KafkaMessageMeta& meta) {

    // Transform the message
    auto transformed = LogTransformer::transform(
        request, meta.topic, meta.partition, meta.offset);

    if (transformed.empty()) {
        return;
    }

    // Find the worker for this partition
    std::lock_guard<std::mutex> lock(workers_mutex_);
    auto it = workers_.find(meta.partition);
    if (it == workers_.end()) {
        std::cerr << "No worker for partition " << meta.partition
                  << ", creating one now" << std::endl;
        // This shouldn't normally happen if rebalance callbacks work correctly
        // But handle it gracefully
        auto worker = std::make_unique<PartitionWorker>(
            meta.partition,
            *db_,
            config_,
            full_table_name_,
            [this](int32_t p, int64_t offset) { onOffsetCommitted(p, offset); }
        );
        worker->start();
        it = workers_.emplace(meta.partition, std::move(worker)).first;
    }

    // Create message envelope and enqueue
    PartitionMessage msg;
    msg.records = std::move(transformed);
    msg.max_offset = meta.offset;

    it->second->enqueue(std::move(msg));
}
