#ifndef APPENDER_HEALTH_HPP
#define APPENDER_HEALTH_HPP

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>

namespace appender {

// Point-in-time view of the appender for /ready and /metrics
struct HealthSnapshot {
    std::chrono::steady_clock::time_point now{};
    bool fatal = false;

    // Kafka source
    bool polling = false;
    std::chrono::steady_clock::time_point last_poll{};
    std::size_t assigned_partitions = 0;
    std::size_t paused_partitions = 0;
    std::size_t parked_messages = 0;

    // Pipelines and staging
    std::uint64_t messages = 0;
    std::uint64_t rows_staged = 0;
    std::uint64_t poison_messages = 0;
    std::uint64_t dlq_failures = 0;
    std::size_t staged_bytes = 0;
    std::size_t staged_bytes_cap = 0;

    // Committer
    std::uint64_t commits = 0;
    std::uint64_t commit_failures = 0;
    std::uint64_t consecutive_commit_failures = 0;
    std::uint64_t rows_committed = 0;
    std::chrono::milliseconds last_commit_duration{0};
    std::optional<std::chrono::steady_clock::time_point> last_commit_success;
};

struct Readiness {
    bool ready = false;
    std::string reason;
};

// Ready while the poll loop is alive and commits are not failing repeatedly
Readiness evaluateReadiness(const HealthSnapshot& snapshot,
                            std::chrono::seconds poll_stall = std::chrono::seconds(30),
                            std::uint64_t max_consecutive_failures = 3);

// Prometheus text exposition format
std::string formatMetrics(const HealthSnapshot& snapshot);

}  // namespace appender

#endif  // APPENDER_HEALTH_HPP
