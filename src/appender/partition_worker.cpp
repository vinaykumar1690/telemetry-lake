#include "partition_worker.hpp"
#include <iostream>
#include <random>
#include <algorithm>

PartitionWorker::PartitionWorker(int32_t partition_id,
                                 DuckDB& db,
                                 const AppenderConfig& config,
                                 const std::string& full_table_name,
                                 OffsetCommitCallback commit_callback)
    : partition_id_(partition_id)
    , config_(config)
    , full_table_name_(full_table_name)
    , commit_callback_(std::move(commit_callback))
    , running_(false)
    , stop_requested_(false)
    , flush_requested_(false)
    , buffer_size_bytes_(0)
    , buffer_records_(0)
    , pending_offset_(-1)
    , committed_offset_(-1) {

    // Create per-worker buffer table name
    buffer_table_name_ = "local_buffer_" + std::to_string(partition_id);

    // Create connection for this worker
    conn_ = std::make_unique<Connection>(db);

    // Initialize last flush time
    last_flush_time_ = std::chrono::system_clock::now();
}

PartitionWorker::~PartitionWorker() {
    signalStop();
    if (worker_thread_.joinable()) {
        worker_thread_.join();
    }
}

void PartitionWorker::start() {
    if (running_) {
        return;
    }

    // Create buffer table for this partition
    if (!IcebergUtils::createBufferTable(*conn_, std::to_string(partition_id_))) {
        std::cerr << "Partition " << partition_id_ << ": Failed to create buffer table" << std::endl;
        return;
    }

    running_ = true;
    stop_requested_ = false;
    worker_thread_ = std::thread(&PartitionWorker::run, this);

    std::cout << "Partition " << partition_id_ << ": Worker started" << std::endl;
}

void PartitionWorker::enqueue(PartitionMessage msg) {
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        queue_.push(std::move(msg));
    }
    queue_cv_.notify_one();
}

void PartitionWorker::signalStop() {
    stop_requested_ = true;
    queue_cv_.notify_all();
}

bool PartitionWorker::waitForStop(int timeout_seconds) {
    if (!worker_thread_.joinable()) {
        return true;
    }

    // Use a timed join approach
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeout_seconds);

    // Signal stop if not already done
    signalStop();

    // Busy wait with periodic checks (std::thread doesn't have timed join)
    while (running_ && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    if (running_) {
        std::cerr << "Partition " << partition_id_ << ": Timeout waiting for worker to stop" << std::endl;
        return false;
    }

    if (worker_thread_.joinable()) {
        worker_thread_.join();
    }

    return true;
}

bool PartitionWorker::forceFlush() {
    flush_requested_ = true;
    queue_cv_.notify_one();

    // Wait for flush to complete (simple busy wait)
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (flush_requested_ && running_ && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    return !flush_requested_;
}

int64_t PartitionWorker::recoverMaxOffset(const std::string& topic) {
    try {
        std::ostringstream sql;
        sql << "SELECT MAX(_kafka_offset) as max_offset "
            << "FROM " << full_table_name_ << " "
            << "WHERE _kafka_topic = '" << IcebergUtils::escapeSqlString(topic) << "' "
            << "AND _kafka_partition = " << partition_id_;

        auto result = conn_->Query(sql.str());
        if (result->HasError()) {
            std::cerr << "Partition " << partition_id_
                      << ": Error querying max offset: " << result->GetError() << std::endl;
            return -1;
        }

        if (result->RowCount() > 0 && !result->GetValue(0, 0).IsNull()) {
            int64_t max_offset = result->GetValue(0, 0).GetValue<int64_t>();
            committed_offset_ = max_offset;
            std::cout << "Partition " << partition_id_
                      << ": Recovered max offset " << max_offset << std::endl;
            return max_offset;
        }

        std::cout << "Partition " << partition_id_
                  << ": No previous data found, starting fresh" << std::endl;
        return -1;
    } catch (const std::exception& e) {
        std::cerr << "Partition " << partition_id_
                  << ": Error during recovery: " << e.what() << std::endl;
        return -1;
    }
}

void PartitionWorker::run() {
    std::cout << "Partition " << partition_id_ << ": Worker thread running" << std::endl;

    while (running_ && !stop_requested_) {
        PartitionMessage msg;
        bool has_message = false;

        {
            std::unique_lock<std::mutex> lock(queue_mutex_);

            // Wait for message, flush request, or stop signal
            // Use timeout to check time-based flush trigger
            queue_cv_.wait_for(lock, std::chrono::seconds(1), [this] {
                return !queue_.empty() || stop_requested_ || flush_requested_;
            });

            if (!queue_.empty()) {
                msg = std::move(queue_.front());
                queue_.pop();
                has_message = true;
            }
        }

        // Process message if available
        if (has_message) {
            processMessage(msg);
        }

        // Check flush triggers
        if (shouldFlush() || flush_requested_.exchange(false)) {
            if (buffer_records_ > 0) {
                std::cout << "Partition " << partition_id_ << ": Triggering flush ("
                          << buffer_records_ << " records, "
                          << (buffer_size_bytes_ / (1024 * 1024)) << " MB)" << std::endl;
                if (flushWithRetry()) {
                    // Notify coordinator of committed offset
                    int64_t offset = committed_offset_.load();
                    if (commit_callback_ && offset >= 0) {
                        commit_callback_(partition_id_, offset);
                    }
                }
            }
        }
    }

    // Final flush before shutdown
    if (buffer_records_ > 0) {
        std::cout << "Partition " << partition_id_ << ": Final flush on shutdown" << std::endl;
        if (flushWithRetry()) {
            int64_t offset = committed_offset_.load();
            if (commit_callback_ && offset >= 0) {
                commit_callback_(partition_id_, offset);
            }
        }
    }

    // Cleanup buffer table
    try {
        conn_->Query("DROP TABLE IF EXISTS " + buffer_table_name_ + ";");
    } catch (...) {
        // Ignore cleanup errors
    }

    running_ = false;
    std::cout << "Partition " << partition_id_ << ": Worker thread stopped" << std::endl;
}

void PartitionWorker::processMessage(const PartitionMessage& msg) {
    if (msg.records.empty()) {
        return;
    }

    // Insert records into buffer
    if (!insertToBuffer(msg.records)) {
        std::cerr << "Partition " << partition_id_
                  << ": Failed to insert records to buffer" << std::endl;
        return;
    }

    // Update pending offset
    if (msg.max_offset > pending_offset_.load()) {
        pending_offset_ = msg.max_offset;
    }

    // Update buffer stats
    buffer_size_bytes_ += IcebergUtils::estimateRecordsSize(msg.records);
    buffer_records_ += msg.records.size();
}

bool PartitionWorker::insertToBuffer(const std::vector<TransformedLogRecord>& records) {
    try {
        std::string sql = IcebergUtils::buildInsertSQL(records, buffer_table_name_);
        auto result = conn_->Query(sql);
        if (result->HasError()) {
            std::cerr << "Partition " << partition_id_
                      << ": Error inserting to buffer: " << result->GetError() << std::endl;
            return false;
        }
        return true;
    } catch (const std::exception& e) {
        std::cerr << "Partition " << partition_id_
                  << ": Exception inserting to buffer: " << e.what() << std::endl;
        return false;
    }
}

bool PartitionWorker::shouldFlush() const {
    // Check size threshold
    size_t buffer_mb = buffer_size_bytes_ / (1024 * 1024);
    if (buffer_mb >= config_.partition_buffer_size_mb) {
        return true;
    }

    // Check time threshold
    std::lock_guard<std::mutex> lock(const_cast<std::mutex&>(flush_time_mutex_));
    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now() - last_flush_time_);
    if (elapsed.count() >= config_.partition_buffer_time_seconds && buffer_records_ > 0) {
        return true;
    }

    return false;
}

bool PartitionWorker::flushWithRetry() {
    for (int attempt = 0; attempt < config_.iceberg_commit_retries; ++attempt) {
        if (attempt > 0) {
            auto delay = calculateBackoff(attempt);
            std::cout << "Partition " << partition_id_
                      << ": Retry attempt " << (attempt + 1)
                      << " after " << delay.count() << "ms" << std::endl;
            std::this_thread::sleep_for(delay);
        }

        if (attemptFlush()) {
            return true;
        }

        std::cerr << "Partition " << partition_id_
                  << ": Flush attempt " << (attempt + 1) << " failed" << std::endl;
    }

    std::cerr << "Partition " << partition_id_
              << ": All flush attempts failed" << std::endl;
    return false;
}

bool PartitionWorker::attemptFlush() {
    try {
        std::cout << "Partition " << partition_id_ << ": Flushing "
                  << buffer_records_ << " records to Iceberg..." << std::endl;

        // Insert from buffer to Iceberg
        std::ostringstream insert_sql;
        insert_sql << "INSERT INTO " << full_table_name_
                   << " SELECT * FROM " << buffer_table_name_ << ";";

        auto result = conn_->Query(insert_sql.str());
        if (result->HasError()) {
            std::cerr << "Partition " << partition_id_
                      << ": Error flushing to Iceberg: " << result->GetError() << std::endl;
            return false;
        }

        // Clear buffer
        if (!clearBuffer()) {
            std::cerr << "Partition " << partition_id_
                      << ": Warning: Failed to clear buffer after successful flush" << std::endl;
            // Data was written successfully, don't fail
        }

        // Update committed offset
        committed_offset_ = pending_offset_.load();

        // Reset buffer stats
        buffer_size_bytes_ = 0;
        buffer_records_ = 0;

        // Reset flush timer
        {
            std::lock_guard<std::mutex> lock(flush_time_mutex_);
            last_flush_time_ = std::chrono::system_clock::now();
        }

        std::cout << "Partition " << partition_id_
                  << ": Flush completed, committed offset: " << committed_offset_ << std::endl;
        return true;
    } catch (const std::exception& e) {
        std::cerr << "Partition " << partition_id_
                  << ": Exception during flush: " << e.what() << std::endl;
        return false;
    }
}

bool PartitionWorker::clearBuffer() {
    try {
        auto result = conn_->Query("DELETE FROM " + buffer_table_name_ + ";");
        return !result->HasError();
    } catch (...) {
        return false;
    }
}

std::chrono::milliseconds PartitionWorker::calculateBackoff(int attempt) const {
    // Exponential backoff with jitter
    int base_delay = config_.iceberg_retry_base_delay_ms;
    int max_delay = config_.iceberg_retry_max_delay_ms;

    // Calculate exponential delay: base * 2^attempt
    int delay = base_delay * (1 << attempt);
    delay = std::min(delay, max_delay);

    // Add jitter (0-50% of delay)
    static thread_local std::mt19937 gen(std::random_device{}());
    std::uniform_int_distribution<> dist(0, delay / 2);
    delay += dist(gen);

    return std::chrono::milliseconds(delay);
}
