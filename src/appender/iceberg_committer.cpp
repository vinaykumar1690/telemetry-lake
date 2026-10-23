#include "iceberg_committer.hpp"

#include "backoff.hpp"

#include "observability/log.hpp"

#include <algorithm>
#include <string>

namespace appender {

IcebergCommitter::IcebergCommitter(StagingSource& source, IcebergSink& sink, OffsetStore& store,
                                   CommitterOptions options, CommittedCallback on_committed,
                                   FatalCallback on_fatal)
    : source_(source),
      sink_(sink),
      store_(store),
      options_(options),
      metrics_(options_.metrics ? options_.metrics : AppenderMetrics::standalone()),
      on_committed_(std::move(on_committed)),
      on_fatal_(std::move(on_fatal)),
      last_maintenance_(std::chrono::steady_clock::now()) {}

IcebergCommitter::~IcebergCommitter() {
    stop();
}

void IcebergCommitter::start() {
    thread_ = std::thread(&IcebergCommitter::run, this);
}

void IcebergCommitter::stop() {
    {
        std::lock_guard lock(mutex_);
        stopping_ = true;
    }
    wake_.notify_all();
    if (thread_.joinable()) thread_.join();
}

std::future<Result<void>> IcebergCommitter::requestFlush() {
    std::promise<Result<void>> promise;
    auto future = promise.get_future();
    {
        std::lock_guard lock(mutex_);
        if (stats_.fatal || !thread_.joinable()) {
            promise.set_value(fatal("committer is not running"));
            return future;
        }
        flush_requests_.push_back(std::move(promise));
    }
    wake_.notify_all();
    return future;
}

bool IcebergCommitter::fence(const std::set<std::int32_t>& partitions,
                             std::chrono::steady_clock::time_point deadline) {
    std::unique_lock lock(mutex_);
    excluded_.insert(partitions.begin(), partitions.end());
    fence_requests_.insert(partitions.begin(), partitions.end());
    wake_.notify_all();  // interrupts a retry backoff that holds these partitions
    const bool clear = progress_.wait_until(lock, deadline, [&] {
        return std::ranges::none_of(partitions, [&](auto p) { return in_flight_.contains(p); });
    });
    for (auto p : partitions) fence_requests_.erase(p);
    return clear;
}

void IcebergCommitter::discard(const std::set<std::int32_t>& partitions) {
    std::vector<SealedBuffer> dropped;
    {
        std::lock_guard lock(mutex_);
        for (auto p : partitions) last_committed_ts_ms_.erase(p);
        auto [first, last] = std::ranges::remove_if(
            pending_, [&](const SealedBuffer& b) { return partitions.contains(b.partition); });
        dropped.assign(std::make_move_iterator(first), std::make_move_iterator(last));
        pending_.erase(first, last);
    }
    for (const auto& buffer : dropped) source_.dropSealed(buffer);
    source_.discard(partitions);
}

void IcebergCommitter::include(const std::set<std::int32_t>& partitions) {
    std::lock_guard lock(mutex_);
    for (auto p : partitions) excluded_.erase(p);
}

CommitterStats IcebergCommitter::stats() const {
    std::lock_guard lock(mutex_);
    return stats_;
}

std::map<std::int32_t, std::int64_t> IcebergCommitter::lastCommittedTimestamps() const {
    std::lock_guard lock(mutex_);
    return last_committed_ts_ms_;
}

bool IcebergCommitter::dueLocked(std::chrono::steady_clock::time_point now) const {
    if (stopping_ || !flush_requests_.empty()) return true;
    if (!pending_.empty() && now >= next_retry_) return true;
    if (source_.unsealedBytes() >= options_.flush_bytes) return true;
    const auto oldest = source_.oldestUnsealedRow();
    return oldest && now - *oldest >= options_.flush_interval;
}

bool IcebergCommitter::interruptedLocked() const {
    return std::ranges::any_of(fence_requests_, [&](auto p) { return in_flight_.contains(p); });
}

std::vector<SealedBuffer> IcebergCommitter::batchLocked() const {
    std::vector<SealedBuffer> out;
    for (const auto& buffer : pending_) {
        // Buffers of fenced partitions wait for discard(); committing them
        // after the fence returned could race the partition's new owner
        if (!excluded_.contains(buffer.partition)) out.push_back(buffer);
    }
    return out;
}

void IcebergCommitter::run() {
    while (true) {
        {
            std::unique_lock lock(mutex_);
            wake_.wait_for(lock, options_.poll_interval,
                           [&] { return stopping_ || !flush_requests_.empty(); });
            if (stats_.fatal) {
                // Nothing more can be committed; fail requests until stopped
                for (auto& request : flush_requests_) request.set_value(fatal("committer failed"));
                flush_requests_.clear();
                if (stopping_) return;
                continue;
            }
            if (!dueLocked(std::chrono::steady_clock::now())) continue;
        }
        cycle();

        std::lock_guard lock(mutex_);
        if (stopping_) {
            for (auto& request : flush_requests_) request.set_value(fatal("committer stopped"));
            flush_requests_.clear();
            return;
        }
    }
}

bool IcebergCommitter::dropLanded(std::map<std::int32_t, std::int64_t>& committed_offsets,
                                  AppenderError& error) {
    std::set<std::int32_t> unique;
    {
        std::lock_guard lock(mutex_);
        for (const auto& buffer : batchLocked()) unique.insert(buffer.partition);
    }
    const std::vector<std::int32_t> partitions(unique.begin(), unique.end());
    auto durable = store_.lastCommitted(partitions);
    if (!durable) {
        error = durable.error();
        return false;
    }

    // A commit is atomic, so a batch either landed completely or not at all;
    // buffers sealed after it carry higher offsets and are never "landed"
    std::vector<SealedBuffer> landed;
    {
        std::lock_guard lock(mutex_);
        auto [first, last] = std::ranges::remove_if(pending_, [&](const SealedBuffer& b) {
            auto it = durable->find(b.partition);
            return !excluded_.contains(b.partition) && it != durable->end() &&
                   it->second >= b.max_offset;
        });
        landed.assign(std::make_move_iterator(first), std::make_move_iterator(last));
        pending_.erase(first, last);
        verify_before_commit_ = false;
    }
    for (const auto& buffer : landed) {
        auto& offset = committed_offsets[buffer.partition];
        offset = std::max(offset, buffer.max_offset);
        source_.dropSealed(buffer);
    }
    if (!landed.empty()) {
        metrics_->commit_landed_after_error.Increment();
        obs::info("committer", "Iceberg commit had landed despite the error",
                  {{"buffers", landed.size()}});
    }
    return true;
}

void IcebergCommitter::cycle() {
    std::vector<std::promise<Result<void>>> requests;
    std::vector<SealedBuffer> batch;
    {
        // Sealing and marking partitions in flight happen under one lock, so
        // a fence either sees them in flight or keeps them out of this cycle
        std::lock_guard lock(mutex_);
        requests = std::move(flush_requests_);
        flush_requests_.clear();
        auto sealed = source_.sealAll(excluded_);
        if (!sealed) {
            obs::error("committer", "sealing staged data failed", {{"error", sealed.error().message}});
        } else {
            for (const auto& buffer : *sealed) {
                metrics_->buffers_sealed.Increment();
                metrics_->sealed_buffer_rows.Observe(static_cast<double>(buffer.rows));
                metrics_->sealed_buffer_bytes.Observe(static_cast<double>(buffer.bytes));
            }
            std::ranges::move(*sealed, std::back_inserter(pending_));
        }
        in_flight_.clear();
        for (const auto& buffer : batchLocked()) in_flight_.insert(buffer.partition);
    }

    std::map<std::int32_t, std::int64_t> committed;
    std::optional<AppenderError> failure;
    std::size_t rows = 0;
    std::vector<SealedBuffer> landed_batch;  // what the successful attempt committed
    bool attempted = false;
    const auto started = std::chrono::steady_clock::now();

    for (int attempt = 0; attempt < options_.commit_retries; ++attempt) {
        if (attempt > 0) {
            const auto delay = backoffWithJitter(attempt - 1, options_.retry_base_delay,
                                                 options_.retry_max_delay, rng_);
            std::unique_lock lock(mutex_);
            // A revoke must not wait out the whole retry chain
            if (wake_.wait_for(lock, delay, [&] { return interruptedLocked(); })) {
                failure = AppenderError{ErrorKind::Retryable, "commit interrupted by a revoke"};
                metrics_->countAttempt("interrupted");
                break;
            }
        }

        bool verify;
        {
            std::lock_guard lock(mutex_);
            verify = verify_before_commit_;
        }
        if (verify) {
            AppenderError error;
            if (!dropLanded(committed, error)) {
                failure = std::move(error);
                metrics_->countAttempt("verify_failed");
                continue;
            }
        }

        {
            std::lock_guard lock(mutex_);
            batch = batchLocked();
        }
        if (batch.empty()) {
            failure.reset();
            break;
        }

        attempted = true;
        auto result = sink_.commit(batch, {std::chrono::system_clock::now()});
        if (result) {
            failure.reset();
            metrics_->countAttempt("ok");
            landed_batch = batch;
            for (const auto& buffer : batch) {
                auto& offset = committed[buffer.partition];
                offset = std::max(offset, buffer.max_offset);
                rows += buffer.rows;
                source_.dropSealed(buffer);
            }
            std::lock_guard lock(mutex_);
            std::erase_if(pending_, [&](const SealedBuffer& b) {
                return std::ranges::any_of(batch, [&](const auto& c) { return c.table == b.table; });
            });
            break;
        }

        failure = result.error();
        metrics_->countAttempt(std::string(toString(failure->kind)));
        obs::warn("committer", "Iceberg commit attempt failed",
                  {{"attempt", attempt + 1}, {"max_attempts", options_.commit_retries},
                   {"kind", toString(failure->kind)}, {"error", failure->message}});
        if (failure->kind == ErrorKind::Fatal) break;
        if (failure->kind == ErrorKind::Indeterminate) {
            std::lock_guard lock(mutex_);
            verify_before_commit_ = true;
        }
    }

    if (!committed.empty()) reportCommitted(committed);

    const auto now = std::chrono::steady_clock::now();
    if (attempted || failure) {
        metrics_->commit_duration.Observe(std::chrono::duration<double>(now - started).count());
    }
    if (!landed_batch.empty()) recordCommit(landed_batch, rows, now - started);
    bool became_fatal = false;
    {
        std::lock_guard lock(mutex_);
        in_flight_.clear();
        stats_.last_commit_duration =
            std::chrono::duration_cast<std::chrono::milliseconds>(now - started);
        if (!failure) {
            if (!committed.empty()) {
                ++stats_.commits;
                stats_.rows_committed += rows;
            }
            stats_.consecutive_failures = 0;
            stats_.last_success = now;
        } else {
            ++stats_.commit_failures;
            ++stats_.consecutive_failures;
            next_retry_ = now + backoffWithJitter(static_cast<int>(stats_.consecutive_failures),
                                                  options_.retry_base_delay,
                                                  options_.retry_max_delay, rng_);
            if (failure->kind == ErrorKind::Fatal) {
                stats_.fatal = true;
                became_fatal = true;
            }
        }
    }
    progress_.notify_all();

    for (auto& request : requests) {
        if (failure) request.set_value(std::unexpected(*failure));
        else request.set_value({});
    }
    if (became_fatal && on_fatal_) on_fatal_(*failure);

    if (!failure && options_.maintenance_interval.count() > 0 &&
        now - last_maintenance_ >= options_.maintenance_interval) {
        last_maintenance_ = now;
        if (auto pruned = store_.prune(); !pruned) {
            metrics_->countMaintenance("failed");
            obs::warn("committer", "offsets table maintenance failed",
                      {{"error", pruned.error().message}});
        } else {
            metrics_->countMaintenance("ok");
        }
    }
}

void IcebergCommitter::recordCommit(const std::vector<SealedBuffer>& batch, std::size_t rows,
                                    std::chrono::steady_clock::duration duration) {
    const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now().time_since_epoch())
                            .count();
    std::set<std::int32_t> partitions;
    std::size_t bytes = 0;
    std::int64_t oldest_ts_ms = -1;
    {
        std::lock_guard lock(mutex_);
        for (const auto& buffer : batch) {
            partitions.insert(buffer.partition);
            bytes += buffer.bytes;
            if (buffer.min_kafka_ts_ms >= 0) {
                metrics_->end_to_end_latency.Observe(
                    static_cast<double>(std::max<std::int64_t>(now_ms - buffer.min_kafka_ts_ms, 0)) / 1e3);
                oldest_ts_ms = oldest_ts_ms < 0 ? buffer.min_kafka_ts_ms
                                                : std::min(oldest_ts_ms, buffer.min_kafka_ts_ms);
            }
            if (buffer.max_kafka_ts_ms >= 0 && !excluded_.contains(buffer.partition)) {
                auto& last = last_committed_ts_ms_[buffer.partition];
                last = std::max(last, buffer.max_kafka_ts_ms);
            }
        }
    }
    metrics_->commit_buffers.Observe(static_cast<double>(batch.size()));
    metrics_->commit_partitions.Observe(static_cast<double>(partitions.size()));
    metrics_->committed_payload_bytes.Increment(static_cast<double>(bytes));

    // Offsets-only commits (poison or empty messages) add no data files
    std::optional<CommitFiles> files;
    if (rows > 0) {
        files = sink_.lastCommitFiles(rows);
        if (files) {
            metrics_->data_files_committed.Increment(static_cast<double>(files->added_data_files));
            metrics_->data_files_per_commit.Observe(static_cast<double>(files->added_data_files));
            if (files->total_data_files) {
                metrics_->table_data_files.Set(static_cast<double>(*files->total_data_files));
            }
        } else {
            metrics_->commit_files_unknown.Increment();
        }
    }

    obs::info("committer", "Iceberg commit",
              {{"rows", rows},
               {"bytes", bytes},
               {"buffers", batch.size()},
               {"partitions", partitions.size()},
               {"data_files", files ? std::to_string(files->added_data_files) : std::string("unknown")},
               {"duration_ms", std::chrono::duration_cast<std::chrono::milliseconds>(duration).count()},
               {"oldest_message_age_ms", oldest_ts_ms < 0 ? std::int64_t{-1} : now_ms - oldest_ts_ms}});
}

void IcebergCommitter::reportCommitted(const std::map<std::int32_t, std::int64_t>& offsets) {
    if (on_committed_) on_committed_(offsets);
}

}  // namespace appender
