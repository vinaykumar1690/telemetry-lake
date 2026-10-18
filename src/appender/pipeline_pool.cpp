#include "pipeline_pool.hpp"

#include "backoff.hpp"
#include "log_transformer.hpp"
#include "otlp_decoder.hpp"

#include <format>
#include <iostream>

namespace appender {

PipelinePool::PipelinePool(PipelineOptions options, StagingLookup& staging, DlqSink& dlq,
                           FatalCallback on_fatal)
    : options_(std::move(options)), staging_(staging), dlq_(dlq), on_fatal_(std::move(on_fatal)) {
    for (std::size_t i = 0; i < std::max<std::size_t>(options_.threads, 1); ++i) {
        workers_.push_back(std::make_unique<Worker>(options_.queue_bytes));
    }
}

PipelinePool::~PipelinePool() {
    stop(/*abort=*/true);
}

void PipelinePool::start() {
    for (auto& worker : workers_) {
        worker->thread = std::thread(&PipelinePool::run, this, std::ref(*worker));
    }
}

void PipelinePool::stop(bool abort) {
    if (abort) {
        std::lock_guard lock(mutex_);
        aborting_ = true;
    }
    changed_.notify_all();
    for (auto& worker : workers_) worker->queue.close();
    for (auto& worker : workers_) {
        if (worker->thread.joinable()) worker->thread.join();
    }
}

PipelinePool::Worker& PipelinePool::workerFor(std::int32_t partition) const {
    return *workers_[static_cast<std::size_t>(partition) % workers_.size()];
}

bool PipelinePool::tryRoute(Work& work) {
    return workerFor(work.partition).queue.try_push(work);
}

bool PipelinePool::hasRoom(std::int32_t partition) const {
    const auto& queue = workerFor(partition).queue;
    return queue.weight() < queue.capacity() / 2;
}

void PipelinePool::dropPartitions(const std::set<std::int32_t>& partitions) {
    {
        std::lock_guard lock(mutex_);
        dropping_.insert(partitions.begin(), partitions.end());
    }
    changed_.notify_all();  // wakes retry loops of these partitions
    for (auto& worker : workers_) {
        worker->queue.remove_if([&](const Work& w) { return partitions.contains(w.partition); });
    }
    std::unique_lock lock(mutex_);
    changed_.wait(lock, [&] {
        return std::ranges::none_of(workers_, [&](const auto& w) {
            return w->busy_partition && partitions.contains(*w->busy_partition);
        });
    });
    for (auto& worker : workers_) {
        for (auto p : partitions) worker->blocked.erase(p);  // a reassigned partition starts clean
    }
    for (auto p : partitions) dropping_.erase(p);
}

PipelineStats PipelinePool::stats() const {
    return {messages_.load(), rows_.load(), poison_.load(), dlq_failures_.load()};
}

bool PipelinePool::cancelled(std::int32_t partition) const {
    return aborting_ || dropping_.contains(partition);
}

bool PipelinePool::sleepUnlessCancelled(std::int32_t partition, std::chrono::milliseconds delay) {
    std::unique_lock lock(mutex_);
    return !changed_.wait_for(lock, delay, [&] { return cancelled(partition); });
}

std::chrono::milliseconds PipelinePool::backoff(int attempt) {
    std::lock_guard lock(rng_mutex_);
    return backoffWithJitter(attempt, options_.retry_base_delay, options_.retry_max_delay, rng_);
}

void PipelinePool::run(Worker& worker) {
    while (true) {
        auto work = worker.queue.pop_wait(std::chrono::milliseconds(200));
        if (!work) {
            // nullopt means a timeout, or closed and drained
            if (worker.queue.closed() && worker.queue.size() == 0) return;
            continue;
        }
        {
            std::lock_guard lock(mutex_);
            if (cancelled(work->partition) || worker.blocked.contains(work->partition)) continue;
            worker.busy_partition = work->partition;
        }
        process(worker, *work);
        {
            std::lock_guard lock(mutex_);
            worker.busy_partition.reset();
        }
        changed_.notify_all();
    }
}

void PipelinePool::process(Worker& worker, const Work& work) {
    auto block = [&](std::string_view why) {
        std::cerr << std::format("Partition {}: stopped at offset {}: {}\n", work.partition,
                                 work.offset, why);
        std::lock_guard lock(mutex_);
        worker.blocked.insert(work.partition);
    };

    auto writer = staging_.writer(work.partition);
    if (!writer) return;  // revoked while queued
    ++messages_;

    auto decoded = decodeKafkaPayload(work.payload);
    if (!decoded) {
        ++poison_;
        const DlqRecord record{work.key, work.payload, options_.topic, work.partition, work.offset,
                               &decoded.error()};
        for (int attempt = 0;; ++attempt) {
            auto sent = dlq_.send(record);
            if (sent) break;
            ++dlq_failures_;
            std::cerr << std::format("Partition {} offset {}: DLQ send failed: {}\n",
                                     work.partition, work.offset, sent.error().message);
            if (!sleepUnlessCancelled(work.partition, backoff(attempt))) {
                return block("DLQ unavailable");
            }
        }
        writer->advanceOffset(work.offset);
        return;
    }

    std::vector<LogRow> rows;
    transformLogs(*decoded, {work.partition, work.offset}, rows);

    // Backpressure: wait while staging is over its byte cap
    while (!staging_.waitForCapacity(std::chrono::milliseconds(200))) {
        std::lock_guard lock(mutex_);
        if (cancelled(work.partition)) return block("cancelled while waiting for capacity");
    }

    for (int attempt = 0;; ++attempt) {
        auto appended = writer->append(rows, work.offset, work.payload.size());
        if (appended) break;
        std::cerr << std::format("Partition {} offset {}: staging failed: {}\n", work.partition,
                                 work.offset, appended.error().message);
        if (appended.error().kind == ErrorKind::Fatal || attempt + 1 >= options_.max_append_attempts) {
            block("staging failed");
            if (on_fatal_) on_fatal_(appended.error());
            return;
        }
        if (!sleepUnlessCancelled(work.partition, backoff(attempt))) {
            return block("cancelled while retrying staging");
        }
    }
    rows_ += rows.size();
}

}  // namespace appender
