#include "health.hpp"

#include <format>
#include <iterator>

namespace appender {

Readiness evaluateReadiness(const HealthSnapshot& s, std::chrono::seconds poll_stall,
                            std::uint64_t max_consecutive_failures) {
    if (s.fatal) return {false, "fatal error; shutting down"};
    if (!s.polling) return {false, "Kafka poll loop is not running"};
    if (s.now - s.last_poll > poll_stall) return {false, "Kafka poll loop is stalled"};
    if (s.consecutive_commit_failures >= max_consecutive_failures) {
        return {false, std::format("{} consecutive Iceberg commit failures",
                                   s.consecutive_commit_failures)};
    }
    return {true, "ok"};
}

std::string formatMetrics(const HealthSnapshot& s) {
    std::string out;
    auto metric = [&](std::string_view name, std::string_view type, std::string_view help,
                      auto value) {
        std::format_to(std::back_inserter(out), "# HELP {0} {1}\n# TYPE {0} {2}\n{0} {3}\n", name,
                       help, type, value);
    };
    metric("appender_fatal", "gauge", "1 once the appender hit a fatal error", s.fatal ? 1 : 0);
    metric("appender_assigned_partitions", "gauge", "Kafka partitions assigned",
           s.assigned_partitions);
    metric("appender_paused_partitions", "gauge", "Partitions paused by backpressure",
           s.paused_partitions);
    metric("appender_parked_messages", "gauge", "Messages waiting for pipeline capacity",
           s.parked_messages);
    metric("appender_messages_total", "counter", "Kafka messages processed", s.messages);
    metric("appender_rows_staged_total", "counter", "Log rows staged", s.rows_staged);
    metric("appender_poison_messages_total", "counter", "Undecodable messages sent to the DLQ",
           s.poison_messages);
    metric("appender_dlq_failures_total", "counter", "Failed DLQ deliveries (retried)",
           s.dlq_failures);
    metric("appender_staged_bytes", "gauge", "Bytes staged or awaiting commit", s.staged_bytes);
    metric("appender_staged_bytes_cap", "gauge", "Staged bytes at which pipelines block",
           s.staged_bytes_cap);
    metric("appender_commits_total", "counter", "Successful Iceberg commits", s.commits);
    metric("appender_commit_failures_total", "counter", "Failed Iceberg commit cycles",
           s.commit_failures);
    metric("appender_commit_consecutive_failures", "gauge",
           "Iceberg commit cycles failed in a row", s.consecutive_commit_failures);
    metric("appender_rows_committed_total", "counter", "Log rows committed to Iceberg",
           s.rows_committed);
    metric("appender_last_commit_duration_seconds", "gauge", "Duration of the last commit cycle",
           std::chrono::duration<double>(s.last_commit_duration).count());
    if (s.last_commit_success) {
        metric("appender_seconds_since_last_commit", "gauge",
               "Seconds since the last successful commit cycle",
               std::chrono::duration<double>(s.now - *s.last_commit_success).count());
    }
    return out;
}

}  // namespace appender
