#ifndef APPENDER_ICEBERG_COMMITTER_HPP
#define APPENDER_ICEBERG_COMMITTER_HPP

#include "appender_metrics.hpp"
#include "error.hpp"
#include "sink.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <future>
#include <map>
#include <mutex>
#include <optional>
#include <random>
#include <set>
#include <thread>
#include <vector>

namespace appender {

struct CommitterOptions {
    std::size_t flush_bytes = std::size_t{100} * 1024 * 1024;
    std::chrono::milliseconds flush_interval{60'000};
    int commit_retries = 5;
    std::chrono::milliseconds retry_base_delay{200};
    std::chrono::milliseconds retry_max_delay{10'000};
    std::chrono::milliseconds maintenance_interval{3'600'000};  // 0 disables
    std::chrono::milliseconds poll_interval{200};               // how often triggers are checked
    std::shared_ptr<AppenderMetrics> metrics;                   // standalone when null
};

struct CommitterStats {
    std::uint64_t commits = 0;
    std::uint64_t commit_failures = 0;
    std::uint64_t consecutive_failures = 0;
    std::uint64_t rows_committed = 0;
    std::chrono::milliseconds last_commit_duration{0};
    std::optional<std::chrono::steady_clock::time_point> last_success;
    bool fatal = false;
};

// Single thread that turns staged data into Iceberg commits.
//
// Each cycle seals every assigned partition's staging generation, merges the
// result with buffers left over from failed cycles, and commits them all in
// one atomic Iceberg transaction (rows + offsets). After an Indeterminate
// failure the next attempt first reads the offsets table and drops buffers
// that did land, so a retry never writes the same rows twice.
class IcebergCommitter {
public:
    using CommittedCallback = std::function<void(const std::map<std::int32_t, std::int64_t>&)>;
    using FatalCallback = std::function<void(const AppenderError&)>;

    IcebergCommitter(StagingSource& source, IcebergSink& sink, OffsetStore& store,
                     CommitterOptions options, CommittedCallback on_committed,
                     FatalCallback on_fatal);
    ~IcebergCommitter();

    IcebergCommitter(const IcebergCommitter&) = delete;
    IcebergCommitter& operator=(const IcebergCommitter&) = delete;

    void start();

    // Commits whatever is staged (with retries), then stops the thread
    void stop();

    // Seals and commits now; the future reports the outcome of that cycle
    std::future<Result<void>> requestFlush();

    // Stops sealing these partitions and waits until no commit in progress
    // includes them. Returns false on timeout: a commit for them may still
    // land, so the caller must not hand the partitions to another consumer.
    bool fence(const std::set<std::int32_t>& partitions,
               std::chrono::steady_clock::time_point deadline);

    // Drops pending and staged data of fenced partitions
    void discard(const std::set<std::int32_t>& partitions);

    // Allows sealing these partitions again (after they are reassigned)
    void include(const std::set<std::int32_t>& partitions);

    CommitterStats stats() const;

    // Newest Kafka timestamp (ms since the epoch) committed per partition
    std::map<std::int32_t, std::int64_t> lastCommittedTimestamps() const;

private:
    void run();
    bool dueLocked(std::chrono::steady_clock::time_point now) const;
    void cycle();
    // Removes buffers whose offsets are already durable; false if the store failed
    bool dropLanded(std::map<std::int32_t, std::int64_t>& committed_offsets, AppenderError& error);
    bool interruptedLocked() const;
    std::vector<SealedBuffer> batchLocked() const;
    void reportCommitted(const std::map<std::int32_t, std::int64_t>& offsets);
    // Metrics and the log line of a successful commit
    void recordCommit(const std::vector<SealedBuffer>& batch, std::size_t rows,
                      std::chrono::steady_clock::duration duration);

    StagingSource& source_;
    IcebergSink& sink_;
    OffsetStore& store_;
    const CommitterOptions options_;
    const std::shared_ptr<AppenderMetrics> metrics_;
    CommittedCallback on_committed_;
    FatalCallback on_fatal_;

    mutable std::mutex mutex_;
    std::condition_variable wake_;      // triggers, flush requests, stop
    std::condition_variable progress_;  // in-flight set changed (fence waiters)
    std::vector<SealedBuffer> pending_;          // sealed, not yet durable
    bool verify_before_commit_ = false;          // last attempt was Indeterminate
    std::set<std::int32_t> in_flight_;           // partitions of the running cycle
    std::set<std::int32_t> excluded_;            // fenced: never sealed
    std::set<std::int32_t> fence_requests_;      // fences currently waiting
    std::vector<std::promise<Result<void>>> flush_requests_;
    std::chrono::steady_clock::time_point next_retry_{};
    std::chrono::steady_clock::time_point last_maintenance_;
    bool stopping_ = false;
    CommitterStats stats_;
    std::map<std::int32_t, std::int64_t> last_committed_ts_ms_;

    std::mt19937 rng_{std::random_device{}()};
    std::thread thread_;
};

}  // namespace appender

#endif  // APPENDER_ICEBERG_COMMITTER_HPP
