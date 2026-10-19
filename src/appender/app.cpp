#include "app.hpp"

#include "dlq_producer.hpp"
#include "duckdb_iceberg.hpp"
#include "duckdb_session.hpp"
#include "health_server.hpp"
#include "iceberg_committer.hpp"
#include "kafka_source.hpp"
#include "pipeline_pool.hpp"
#include "staging_store.hpp"

#include <atomic>
#include <csignal>
#include <format>
#include <iostream>
#include <unistd.h>
#include <thread>

namespace appender {

int runAppender(const AppenderConfig& config) {
    auto session = DuckdbSession::openIceberg(config);
    if (!session) {
        std::cerr << std::format("Startup failed: {}\n", session.error().message);
        return kExitStartup;
    }
    const TableNames& tables = (*session)->tables();

    auto dlq = KafkaDlqProducer::create({.brokers = config.brokers,
                                         .topic = config.dlq_topic,
                                         .appender_id = config.appender_id});
    if (!dlq) {
        std::cerr << std::format("Startup failed: {}\n", dlq.error().message);
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
        std::cerr << std::format("Fatal: {}\n", error.message);
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
         .maintenance_interval = config.offsets_maintenance_interval},
        [&](const auto& offsets) {
            if (source_ptr) source_ptr->onCommitted(offsets);
        },
        on_fatal);

    PipelinePool pipelines({.threads = config.pipeline_threads,
                            .queue_bytes = config.pipeline_queue_bytes,
                            .topic = config.topic,
                            .retry_base_delay = config.retry_base_delay,
                            .retry_max_delay = config.retry_max_delay,
                            .promoted_keys = (*session)->schema()->promotedKeys()},
                           staging, **dlq, on_fatal);

    KafkaSource source({.brokers = config.brokers,
                        .topic = config.topic,
                        .consumer_group = config.consumer_group,
                        .max_poll_interval_ms = config.max_poll_interval_ms,
                        .revoke_timeout = config.revoke_timeout},
                       staging, pipelines, committer, recovery_offsets, on_fatal);
    source_ptr = &source;
    if (auto initialized = source.init(); !initialized) {
        std::cerr << std::format("Startup failed: {}\n", initialized.error().message);
        return kExitStartup;
    }

    pipelines.start();
    committer.start();

    HealthServer health(config.health_port, {
        .snapshot = [&] {
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
        },
        .flush = [&]() -> Result<void> {
            auto future = committer.requestFlush();
            if (future.wait_for(std::chrono::minutes(2)) != std::future_status::ready) {
                return retryable("flush did not finish within 2 minutes");
            }
            return future.get();
        },
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
                std::cout << "SIGUSR1: flushing staged data\n";
                (void)committer.requestFlush();  // the outcome is logged by the committer
                continue;
            }
            std::cout << std::format("Signal {}: shutting down\n", signal);
            source.stop();
            signals_done = true;
            return;
        }
    });

    std::cout << std::format("Appender {} consuming {} into {} (exactly-once via {})\n",
                             config.appender_id, config.topic, tables.logs, tables.offsets);
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
    std::cout << std::format("Appender stopped ({})\n", failed ? "fatal error" : "clean");
    return failed ? kExitFatal : kExitOk;
}

}  // namespace appender
