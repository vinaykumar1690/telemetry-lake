#ifndef APPENDER_KAFKA_SOURCE_HPP
#define APPENDER_KAFKA_SOURCE_HPP

#include "error.hpp"
#include "flow_control.hpp"
#include "iceberg_committer.hpp"
#include "pipeline_pool.hpp"
#include "staging_store.hpp"

#include <cppkafka/cppkafka.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>

namespace appender {

struct KafkaSourceOptions {
    std::string brokers;
    std::string topic;
    std::string consumer_group;
    int max_poll_interval_ms = 300'000;
    std::chrono::seconds revoke_timeout{60};
    std::size_t batch_size = 500;
    std::chrono::milliseconds poll_timeout{100};
};

struct KafkaSourceStats {
    std::size_t assigned = 0;
    std::size_t paused = 0;
    std::size_t parked = 0;
    std::chrono::steady_clock::time_point last_poll{};
    bool running = false;
};

// The only code that touches the Kafka consumer; everything here runs on the
// thread that calls run(). It routes messages to the pipelines, pauses
// partitions whose pipeline is full, positions newly assigned partitions from
// the offsets table, fences revoked partitions, and commits offsets back to
// Kafka. Those Kafka commits are hints for lag monitoring: recovery reads the
// offsets table, never the consumer group's committed offsets.
class KafkaSource {
public:
    using FatalCallback = std::function<void(const AppenderError&)>;

    KafkaSource(KafkaSourceOptions options, StagingRegistry& staging, PipelinePool& pipelines,
                IcebergCommitter& committer, OffsetStore& offsets, FatalCallback on_fatal);
    ~KafkaSource();

    KafkaSource(const KafkaSource&) = delete;
    KafkaSource& operator=(const KafkaSource&) = delete;

    Result<void> init();

    // Polls until stop(); the calling thread becomes the consumer's owner
    void run();
    void stop() { stop_requested_ = true; }

    // Called by the committer (any thread) after offsets became durable
    void onCommitted(const std::map<std::int32_t, std::int64_t>& offsets);

    // After run() returned and the committer stopped: commits the latest
    // hints synchronously, drops parked messages and closes the consumer
    void shutdown();

    KafkaSourceStats stats() const;

private:
    void onAssigned(cppkafka::TopicPartitionList& partitions);
    void onRevoked(const cppkafka::TopicPartitionList& partitions);
    void handle(cppkafka::Message message);
    void updatePauses();
    void commitHints(bool synchronous);
    cppkafka::TopicPartitionList partitionList(const std::set<std::int32_t>& partitions) const;

    const KafkaSourceOptions options_;
    StagingRegistry& staging_;
    PipelinePool& pipelines_;
    IcebergCommitter& committer_;
    OffsetStore& offsets_;
    FatalCallback on_fatal_;

    std::unique_ptr<cppkafka::Consumer> consumer_;
    FlowControl flow_;
    std::set<std::int32_t> assigned_;
    std::set<std::int32_t> paused_;
    std::atomic<bool> stop_requested_ = false;

    std::mutex hints_mutex_;
    std::map<std::int32_t, std::int64_t> hints_;  // durable offsets not yet committed to Kafka

    mutable std::mutex stats_mutex_;
    KafkaSourceStats stats_;
    std::chrono::steady_clock::time_point last_error_log_{};
};

}  // namespace appender

#endif  // APPENDER_KAFKA_SOURCE_HPP
