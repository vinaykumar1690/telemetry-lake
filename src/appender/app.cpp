#include "app.hpp"

#include "appender_metrics.hpp"
#include "dlq_producer.hpp"
#include "duckdb_iceberg.hpp"
#include "duckdb_session.hpp"
#include "health_server.hpp"
#include "iceberg_committer.hpp"
#include "kafka_source.hpp"
#include "pipeline_pool.hpp"
#include "staging_store.hpp"

#include "observability/kafka_stats.hpp"
#include "observability/log.hpp"
#include "observability/metrics.hpp"

#include <algorithm>
#include <atomic>
#include <csignal>
#include <mutex>
#include <unistd.h>
#include <thread>

namespace appender {
namespace {

using Labels = std::vector<prometheus::ClientMetric::Label>;

// Gauges read from the components when /metrics is scraped
std::vector<prometheus::MetricFamily> liveMetrics(const AppenderConfig& config,
                                                  const obs::KafkaStatsCollector& kafka_stats,
                                                  const KafkaSource& source,
                                                  const IcebergCommitter& committer,
                                                  const PipelinePool& pipelines) {
    std::vector<prometheus::MetricFamily> out;
    constexpr auto kGauge = prometheus::MetricType::Gauge;

    // Messages in Kafka that are not in Iceberg yet: high watermark minus
    // the next offset after the durable one (or the low watermark, when
    // retention already deleted older messages)
    obs::FamilyBuilder lag("appender_kafka_commit_lag_messages",
                           "Messages in Kafka not yet committed to Iceberg", kGauge);
    const auto marks = kafka_stats.watermarks(KafkaSource::kClient, config.topic);
    for (const auto& [partition, durable] : source.durableOffsets()) {
        auto it = marks.find(partition);
        if (it == marks.end()) continue;
        const auto next = std::max(durable + 1, it->second.low);
        lag.add(static_cast<double>(std::max<std::int64_t>(it->second.high - next, 0)),
                {{"partition", std::to_string(partition)}});
    }
    out.push_back(lag.take());

    obs::FamilyBuilder committed_ts("appender_last_committed_message_timestamp_seconds",
                                    "Kafka timestamp of the newest committed message; "
                                    "time() minus this is the data freshness", kGauge);
    for (const auto& [partition, ts_ms] : committer.lastCommittedTimestamps()) {
        committed_ts.add(static_cast<double>(ts_ms) / 1e3, {{"partition", std::to_string(partition)}});
    }
    out.push_back(committed_ts.take());

    obs::FamilyBuilder queues("appender_pipeline_queue_bytes", "Bytes queued per pipeline thread",
                              kGauge);
    const auto queue_bytes = pipelines.queueBytes();
    for (std::size_t i = 0; i < queue_bytes.size(); ++i) {
        queues.add(static_cast<double>(queue_bytes[i]), {{"worker", std::to_string(i)}});
    }
    out.push_back(queues.take());
    return out;
}

// DuckDB buffer-manager memory by tag and spilled temporary storage
std::vector<prometheus::MetricFamily> duckdbMetrics(duckdb::Connection& conn) {
    std::vector<prometheus::MetricFamily> out;
    auto result = query(conn, "SELECT tag, memory_usage_bytes, temporary_storage_bytes "
                              "FROM duckdb_memory()");
    if (!result) return out;
    obs::FamilyBuilder memory("appender_duckdb_memory_bytes", "DuckDB memory in use by tag",
                              prometheus::MetricType::Gauge);
    double temp = 0;
    auto& rows = **result;
    for (duckdb::idx_t i = 0; i < rows.RowCount(); ++i) {
        const auto bytes = rows.GetValue(1, i).GetValue<std::int64_t>();
        if (bytes > 0) memory.add(static_cast<double>(bytes), {{"tag", rows.GetValue(0, i).ToString()}});
        temp += static_cast<double>(rows.GetValue(2, i).GetValue<std::int64_t>());
    }
    out.push_back(memory.take());
    out.push_back(obs::FamilyBuilder("appender_duckdb_temp_bytes",
                                     "DuckDB temporary storage spilled to disk",
                                     prometheus::MetricType::Gauge)
                      .add(temp)
                      .take());
    return out;
}

}  // namespace

int runAppender(const AppenderConfig& config) {
    // Declared first so it outlives every component its collectables read
    obs::Metrics metrics("otel-appender");
    auto appender_metrics = std::make_shared<AppenderMetrics>(metrics.registry());
    auto kafka_stats = std::make_shared<obs::KafkaStatsCollector>();
    metrics.addCollectable(kafka_stats);

    auto session = DuckdbSession::openIceberg(config);
    if (!session) {
        obs::error("main", "startup failed", {{"error", session.error().message}});
        return kExitStartup;
    }
    const TableNames& tables = (*session)->tables();

    auto dlq = KafkaDlqProducer::create({.brokers = config.brokers,
                                         .topic = config.dlq_topic,
                                         .appender_id = config.appender_id,
                                         .stats_interval_ms = config.kafka_stats_interval_ms,
                                         .stats = kafka_stats});
    if (!dlq) {
        obs::error("main", "startup failed", {{"error", dlq.error().message}});
        return kExitStartup;
    }

    StagingRegistry staging((*session)->database(), config.max_staged_bytes, (*session)->schema());
    DuckdbIcebergSink sink(**session, tables,
                           {config.topic, config.consumer_group, config.appender_id});
    // Separate connections: the committer and recovery run on different threads
    DuckdbOffsetStore commit_offsets(**session, tables.offsets, config.topic);
    DuckdbOffsetStore recovery_offsets(**session, tables.offsets, config.topic);

    std::atomic<bool> fatal = false;
    KafkaSource* source_ptr = nullptr;
    auto on_fatal = [&](const AppenderError& error) {
        obs::error("main", "fatal error", {{"error", error.message}});
        fatal = true;
        if (source_ptr) source_ptr->stop();
    };

    IcebergCommitter committer(
        staging, sink, commit_offsets,
        {.flush_bytes = config.flush_bytes,
         .flush_interval = config.flush_interval,
         .commit_retries = config.commit_retries,
         .retry_base_delay = config.retry_base_delay,
         .retry_max_delay = config.retry_max_delay,
         .maintenance_interval = config.offsets_maintenance_interval,
         .metrics = appender_metrics},
        [&](const auto& offsets) {
            if (source_ptr) source_ptr->onCommitted(offsets);
        },
        on_fatal);

    PipelinePool pipelines({.threads = config.pipeline_threads,
                            .queue_bytes = config.pipeline_queue_bytes,
                            .topic = config.topic,
                            .retry_base_delay = config.retry_base_delay,
                            .retry_max_delay = config.retry_max_delay,
                            .promoted_keys = (*session)->schema()->promotedKeys(),
                            .metrics = appender_metrics},
                           staging, **dlq, on_fatal);

    KafkaSource source({.brokers = config.brokers,
                        .topic = config.topic,
                        .consumer_group = config.consumer_group,
                        .max_poll_interval_ms = config.max_poll_interval_ms,
                        .revoke_timeout = config.revoke_timeout,
                        .stats_interval_ms = config.kafka_stats_interval_ms,
                        .queued_max_kbytes = config.kafka_queued_max_kbytes,
                        .stats = kafka_stats,
                        .metrics = appender_metrics},
                       staging, pipelines, committer, recovery_offsets, on_fatal);
    source_ptr = &source;
    if (auto initialized = source.init(); !initialized) {
        obs::error("main", "startup failed", {{"error", initialized.error().message}});
        return kExitStartup;
    }

    pipelines.start();
    committer.start();

    auto snapshot = [&] {
        HealthSnapshot s;
        s.now = std::chrono::steady_clock::now();
        s.fatal = fatal;
        const auto kafka = source.stats();
        s.polling = kafka.running;
        s.last_poll = kafka.last_poll;
        s.assigned_partitions = kafka.assigned;
        s.paused_partitions = kafka.paused;
        s.parked_messages = kafka.parked;
        const auto pipeline = pipelines.stats();
        s.messages = pipeline.messages;
        s.rows_staged = pipeline.rows;
        s.poison_messages = pipeline.poison;
        s.dlq_failures = pipeline.dlq_failures;
        s.staged_bytes = staging.budget().used();
        s.staged_bytes_cap = staging.budget().cap();
        const auto commits = committer.stats();
        s.commits = commits.commits;
        s.commit_failures = commits.commit_failures;
        s.consecutive_commit_failures = commits.consecutive_failures;
        s.rows_committed = commits.rows_committed;
        s.last_commit_duration = commits.last_commit_duration;
        s.last_commit_success = commits.last_success;
        s.fatal = s.fatal || commits.fatal;
        return s;
    };
    metrics.addCollectable(std::make_shared<obs::CallbackCollectable>([&] {
        auto families = snapshotMetrics(snapshot());
        std::ranges::move(liveMetrics(config, *kafka_stats, source, committer, pipelines),
                          std::back_inserter(families));
        return families;
    }));
    std::mutex duckdb_metrics_mutex;
    auto duckdb_metrics_conn = (*session)->connect();
    metrics.addCollectable(std::make_shared<obs::CallbackCollectable>([&] {
        std::lock_guard lock(duckdb_metrics_mutex);  // scrapes may run concurrently
        return duckdbMetrics(*duckdb_metrics_conn);
    }));
    metrics.onScrape([&] { (*dlq)->poll(); });  // keeps the idle DLQ producer's stats fresh

    HealthServer health(config.health_port, {
        .snapshot = snapshot,
        .flush = [&]() -> Result<void> {
            auto future = committer.requestFlush();
            if (future.wait_for(std::chrono::minutes(2)) != std::future_status::ready) {
                return retryable("flush did not finish within 2 minutes");
            }
            return future.get();
        },
        .metrics = [&] { return metrics.render(); },
    });
    health.start();

    // The only thread that receives SIGINT/SIGTERM/SIGUSR1 (blocked elsewhere)
    std::atomic<bool> signals_done = false;
    std::thread signals([&] {
        sigset_t set;
        sigemptyset(&set);
        sigaddset(&set, SIGINT);
        sigaddset(&set, SIGTERM);
        sigaddset(&set, SIGUSR1);
        while (true) {
            int signal = 0;
            if (sigwait(&set, &signal) != 0) continue;
            if (signal == SIGUSR1) {
                obs::info("main", "SIGUSR1: flushing staged data");
                (void)committer.requestFlush();  // the outcome is logged by the committer
                continue;
            }
            obs::info("main", "shutting down", {{"signal", signal}});
            source.stop();
            signals_done = true;
            return;
        }
    });

    obs::info("main", "appender running",
              {{"appender_id", config.appender_id}, {"topic", config.topic},
               {"logs_table", tables.logs}, {"offsets_table", tables.offsets}});
    source.run();  // this thread owns the consumer until stop()

    // Shutdown: drain what was polled, commit it, then record the hints and
    // leave the group. After a fatal error nothing more is staged or committed;
    // whatever was not committed is re-read from Kafka on restart.
    pipelines.stop(/*abort=*/fatal);
    committer.stop();
    source.shutdown();
    health.stop();
    // Wake the sigwait thread if no signal ended the run (e.g. a fatal error).
    // Every thread blocks SIGTERM, so it stays pending for sigwait; if that
    // thread already returned it simply stays pending until exit.
    if (!signals_done) ::kill(::getpid(), SIGTERM);
    signals.join();

    const bool failed = fatal || committer.stats().fatal;
    obs::info("main", "appender stopped", {{"outcome", failed ? "fatal error" : "clean"}});
    return failed ? kExitFatal : kExitOk;
}

}  // namespace appender
