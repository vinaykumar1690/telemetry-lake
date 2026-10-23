#include "health.hpp"

#include "observability/metrics.hpp"

#include <prometheus/text_serializer.h>

#include <format>

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

std::vector<prometheus::MetricFamily> snapshotMetrics(const HealthSnapshot& s) {
    std::vector<prometheus::MetricFamily> out;
    auto metric = [&](const char* name, prometheus::MetricType type, const char* help, double value) {
        out.push_back(obs::FamilyBuilder(name, help, type).add(value).take());
    };
    constexpr auto kGauge = prometheus::MetricType::Gauge;
    constexpr auto kCounter = prometheus::MetricType::Counter;
    auto count = [](auto v) { return static_cast<double>(v); };

    metric("appender_fatal", kGauge, "1 once the appender hit a fatal error", s.fatal ? 1 : 0);
    metric("appender_assigned_partitions", kGauge, "Kafka partitions assigned",
           count(s.assigned_partitions));
    metric("appender_paused_partitions", kGauge, "Partitions paused by backpressure",
           count(s.paused_partitions));
    metric("appender_parked_messages", kGauge, "Messages waiting for pipeline capacity",
           count(s.parked_messages));
    metric("appender_messages_total", kCounter, "Kafka messages processed", count(s.messages));
    metric("appender_rows_staged_total", kCounter, "Log rows staged (log events processed)",
           count(s.rows_staged));
    metric("appender_poison_messages_total", kCounter, "Undecodable messages sent to the DLQ",
           count(s.poison_messages));
    metric("appender_dlq_failures_total", kCounter, "Failed DLQ deliveries (retried)",
           count(s.dlq_failures));
    metric("appender_staged_bytes", kGauge, "Bytes staged or awaiting commit", count(s.staged_bytes));
    metric("appender_staged_bytes_cap", kGauge, "Staged bytes at which pipelines block",
           count(s.staged_bytes_cap));
    metric("appender_commits_total", kCounter, "Successful Iceberg commits", count(s.commits));
    metric("appender_commit_failures_total", kCounter, "Failed Iceberg commit cycles",
           count(s.commit_failures));
    metric("appender_commit_consecutive_failures", kGauge, "Iceberg commit cycles failed in a row",
           count(s.consecutive_commit_failures));
    metric("appender_rows_committed_total", kCounter, "Log rows committed to Iceberg",
           count(s.rows_committed));
    metric("appender_last_commit_duration_seconds", kGauge, "Duration of the last commit cycle",
           std::chrono::duration<double>(s.last_commit_duration).count());
    if (s.last_commit_success) {
        metric("appender_seconds_since_last_commit", kGauge,
               "Seconds since the last successful commit cycle",
               std::chrono::duration<double>(s.now - *s.last_commit_success).count());
    }
    return out;
}

std::string formatMetrics(const HealthSnapshot& s) {
    return prometheus::TextSerializer().Serialize(snapshotMetrics(s));
}

}  // namespace appender
