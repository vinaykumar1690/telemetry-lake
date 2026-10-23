#include "appender_metrics.hpp"

#include "observability/metrics.hpp"

namespace appender {
namespace {

using obs::exponentialBuckets;

prometheus::Counter& counter(prometheus::Registry& registry, const char* name, const char* help) {
    return prometheus::BuildCounter().Name(name).Help(help).Register(registry).Add({});
}

prometheus::Histogram& histogram(prometheus::Registry& registry, const char* name,
                                 const char* help,
                                 prometheus::Histogram::BucketBoundaries buckets) {
    return prometheus::BuildHistogram().Name(name).Help(help).Register(registry).Add(
        {}, std::move(buckets));
}

}  // namespace

std::shared_ptr<AppenderMetrics> AppenderMetrics::standalone() {
    return std::shared_ptr<AppenderMetrics>(
        new AppenderMetrics(std::make_shared<prometheus::Registry>()));
}

AppenderMetrics::AppenderMetrics(std::shared_ptr<prometheus::Registry> registry)
    : AppenderMetrics(*registry) {
    owned_registry_ = std::move(registry);
}

AppenderMetrics::AppenderMetrics(prometheus::Registry& r)
    : consumed_bytes(counter(r, "appender_kafka_consumed_bytes_total",
                             "Kafka payload bytes processed by the pipelines")),
      process_duration(histogram(r, "appender_pipeline_process_duration_seconds",
                                 "Decode, transform and stage time per Kafka message "
                                 "(waits for staging capacity excluded)",
                                 exponentialBuckets(0.0001, 2, 16))),  // 0.1 ms .. 3.3 s
      backpressure_seconds(counter(r, "appender_backpressure_wait_seconds_total",
                                   "Time pipelines waited for staging capacity")),
      dlq_sent_bytes(counter(r, "appender_dlq_sent_bytes_total",
                             "Bytes of poison messages written to the DLQ")),
      buffers_sealed(counter(r, "appender_buffers_sealed_total",
                             "Staging buffers sealed for a commit (one per partition per cycle)")),
      sealed_buffer_rows(histogram(r, "appender_sealed_buffer_rows", "Rows per sealed buffer",
                                   exponentialBuckets(1, 4, 12))),  // 1 .. 4M
      sealed_buffer_bytes(histogram(r, "appender_sealed_buffer_bytes",
                                    "Kafka payload bytes per sealed buffer",
                                    exponentialBuckets(1024, 4, 11))),  // 1 KiB .. 1 GiB
      commit_duration(histogram(r, "appender_commit_duration_seconds",
                                "Duration of a commit cycle, retries included",
                                exponentialBuckets(0.1, 2, 11))),  // 0.1 .. 102 s
      commit_buffers(histogram(r, "appender_commit_buffers", "Sealed buffers per Iceberg commit",
                               exponentialBuckets(1, 2, 9))),
      commit_partitions(histogram(r, "appender_commit_partitions",
                                  "Kafka partitions per Iceberg commit",
                                  exponentialBuckets(1, 2, 9))),
      commit_attempts(prometheus::BuildCounter()
                          .Name("appender_commit_attempts_total")
                          .Help("Iceberg commit attempts by outcome")
                          .Register(r)),
      commit_landed_after_error(counter(r, "appender_commit_landed_after_error_total",
                                        "Commits that reported an error but had landed")),
      committed_payload_bytes(counter(r, "appender_committed_payload_bytes_total",
                                      "Kafka payload bytes whose rows were committed")),
      data_files_committed(counter(r, "appender_data_files_committed_total",
                                   "Parquet data files added to the logs table")),
      data_files_per_commit(histogram(r, "appender_data_files_per_commit",
                                      "Parquet data files added per commit",
                                      exponentialBuckets(1, 2, 10))),
      commit_files_unknown(counter(r, "appender_commit_files_unknown_total",
                                   "Commits whose snapshot summary could not be read")),
      table_data_files(prometheus::BuildGauge()
                           .Name("appender_table_data_files")
                           .Help("Data files in the logs table after the last commit "
                                 "(grows until compaction)")
                           .Register(r)
                           .Add({})),
      end_to_end_latency(histogram(r, "appender_end_to_end_latency_seconds",
                                   "Oldest Kafka message timestamp of a buffer to its commit",
                                   exponentialBuckets(0.25, 2, 12))),  // 0.25 s .. 8.5 min
      offsets_maintenance(prometheus::BuildCounter()
                              .Name("appender_offsets_maintenance_total")
                              .Help("Offsets table maintenance runs by outcome")
                              .Register(r)),
      partitions_assigned(prometheus::BuildCounter()
                              .Name("appender_rebalance_partitions_total")
                              .Help("Partitions assigned or revoked by rebalances")
                              .Register(r)
                              .Add({{"type", "assign"}})),
      partitions_revoked(prometheus::BuildCounter()
                             .Name("appender_rebalance_partitions_total")
                             .Help("")
                             .Register(r)
                             .Add({{"type", "revoke"}})),
      revoke_fence_duration(histogram(r, "appender_revoke_fence_duration_seconds",
                                      "Time a revoke waited for in-flight commits",
                                      exponentialBuckets(0.001, 4, 10))) {}

}  // namespace appender
