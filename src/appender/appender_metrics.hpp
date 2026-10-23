#ifndef APPENDER_METRICS_HPP
#define APPENDER_METRICS_HPP

#include <prometheus/counter.h>
#include <prometheus/family.h>
#include <prometheus/gauge.h>
#include <prometheus/histogram.h>
#include <prometheus/registry.h>

#include <memory>
#include <string>

namespace appender {

// Prometheus metrics updated as the appender works. Values that live in the
// components (staged bytes, assigned partitions, commit lag) are read at
// scrape time instead (app.cpp).
struct AppenderMetrics {
    explicit AppenderMetrics(prometheus::Registry& registry);

    // With a registry of its own that nothing exports (tests, defaults)
    static std::shared_ptr<AppenderMetrics> standalone();

private:
    explicit AppenderMetrics(std::shared_ptr<prometheus::Registry> registry);
    // Declared first: the references below are initialized from it
    std::shared_ptr<prometheus::Registry> owned_registry_;

public:
    // Pipelines
    prometheus::Counter& consumed_bytes;        // Kafka payload bytes processed
    prometheus::Histogram& process_duration;    // decode + transform + stage, per message
    prometheus::Counter& backpressure_seconds;  // waiting for staging capacity
    prometheus::Counter& dlq_sent_bytes;

    // Sealing and commits
    prometheus::Counter& buffers_sealed;
    prometheus::Histogram& sealed_buffer_rows;
    prometheus::Histogram& sealed_buffer_bytes;
    prometheus::Histogram& commit_duration;  // a whole cycle, retries included
    prometheus::Histogram& commit_buffers;
    prometheus::Histogram& commit_partitions;
    prometheus::Family<prometheus::Counter>& commit_attempts;  // {outcome}
    prometheus::Counter& commit_landed_after_error;
    prometheus::Counter& committed_payload_bytes;
    prometheus::Counter& data_files_committed;
    prometheus::Histogram& data_files_per_commit;
    prometheus::Counter& commit_files_unknown;
    prometheus::Gauge& table_data_files;
    prometheus::Histogram& end_to_end_latency;  // Kafka timestamp to committed
    prometheus::Family<prometheus::Counter>& offsets_maintenance;  // {outcome}

    // Kafka source
    prometheus::Counter& partitions_assigned;
    prometheus::Counter& partitions_revoked;
    prometheus::Histogram& revoke_fence_duration;

    void countAttempt(const std::string& outcome) {
        commit_attempts.Add({{"outcome", outcome}}).Increment();
    }
    void countMaintenance(const std::string& outcome) {
        offsets_maintenance.Add({{"outcome", outcome}}).Increment();
    }
};

}  // namespace appender

#endif  // APPENDER_METRICS_HPP
