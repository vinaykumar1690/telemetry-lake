#ifndef APPENDER_PIPELINE_POOL_HPP
#define APPENDER_PIPELINE_POOL_HPP

#include "appender_metrics.hpp"
#include "bounded_queue.hpp"
#include "dlq_producer.hpp"
#include "error.hpp"
#include "sink.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace appender {

// One Kafka message on its way to staging. key/payload point into owner,
// which keeps the underlying buffer (e.g. a cppkafka::Message) alive.
struct Work {
    std::int32_t partition = 0;
    std::int64_t offset = 0;
    std::string_view key;
    std::string_view payload;
    std::shared_ptr<const void> owner;
    std::int64_t kafka_ts_ms = -1;  // Kafka message timestamp, -1 if none
};

struct WorkWeight {
    std::size_t operator()(const Work& work) const { return work.payload.size() + 64; }
};

struct PipelineOptions {
    std::size_t threads = 4;
    std::size_t queue_bytes = std::size_t{64} * 1024 * 1024;
    std::string topic;
    std::chrono::milliseconds retry_base_delay{200};
    std::chrono::milliseconds retry_max_delay{10'000};
    int max_append_attempts = 10;  // staging failures beyond this are fatal
    std::vector<std::string> promoted_keys;  // TableSchema::promotedKeys()
    std::shared_ptr<AppenderMetrics> metrics;  // standalone when null
};

struct PipelineStats {
    std::uint64_t messages = 0;
    std::uint64_t rows = 0;
    std::uint64_t poison = 0;
    std::uint64_t dlq_failures = 0;
};

// Decodes, transforms and stages Kafka messages on a fixed set of threads.
// A partition always maps to the same thread, so its messages are staged in
// offset order. Nothing is ever skipped: a message that cannot be staged or
// sent to the DLQ blocks its partition (and fills its queue, which pauses
// the partition upstream) until it succeeds or the partition is dropped.
class PipelinePool {
public:
    using FatalCallback = std::function<void(const AppenderError&)>;

    PipelinePool(PipelineOptions options, StagingLookup& staging, DlqSink& dlq,
                 FatalCallback on_fatal);
    ~PipelinePool();

    PipelinePool(const PipelinePool&) = delete;
    PipelinePool& operator=(const PipelinePool&) = delete;

    void start();

    // Processes everything already queued, then joins the threads. With
    // abort, retry loops give up; their partitions' later messages are
    // skipped so no offset ever moves past an unprocessed message.
    void stop(bool abort = false);

    // Queues work on its partition's thread; false (work untouched) if full
    bool tryRoute(Work& work);

    // True while the partition's thread queue is below half its capacity
    bool hasRoom(std::int32_t partition) const;

    // Removes queued work of these partitions and waits until no thread is
    // still working on them
    void dropPartitions(const std::set<std::int32_t>& partitions);

    PipelineStats stats() const;

    // Bytes queued on each thread
    std::vector<std::size_t> queueBytes() const;

private:
    using Queue = BoundedQueue<Work, WorkWeight>;

    struct Worker {
        explicit Worker(std::size_t queue_bytes) : queue(queue_bytes) {}
        Queue queue;
        std::thread thread;
        std::optional<std::int32_t> busy_partition;  // guarded by mutex_
        std::set<std::int32_t> blocked;              // partitions this thread gave up on
    };

    void run(Worker& worker);
    void process(Worker& worker, const Work& work);
    // Waits for delay unless the partition is dropped or the pool aborts; false if interrupted
    bool sleepUnlessCancelled(std::int32_t partition, std::chrono::milliseconds delay);
    bool cancelled(std::int32_t partition) const;
    std::chrono::milliseconds backoff(int attempt);
    Worker& workerFor(std::int32_t partition) const;

    const PipelineOptions options_;
    const std::shared_ptr<AppenderMetrics> metrics_;
    StagingLookup& staging_;
    DlqSink& dlq_;
    FatalCallback on_fatal_;
    std::vector<std::unique_ptr<Worker>> workers_;

    mutable std::mutex mutex_;
    std::condition_variable changed_;
    std::set<std::int32_t> dropping_;
    bool aborting_ = false;

    std::atomic<std::uint64_t> messages_ = 0;
    std::atomic<std::uint64_t> rows_ = 0;
    std::atomic<std::uint64_t> poison_ = 0;
    std::atomic<std::uint64_t> dlq_failures_ = 0;
    std::mutex rng_mutex_;
    std::mt19937 rng_{std::random_device{}()};
};

}  // namespace appender

#endif  // APPENDER_PIPELINE_POOL_HPP
