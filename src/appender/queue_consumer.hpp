#ifndef QUEUE_CONSUMER_HPP
#define QUEUE_CONSUMER_HPP

#include "../config.hpp"
#include <string>
#include <functional>
#include <memory>
#include <map>
#include <vector>
#include <cstdint>

// Forward declarations
namespace opentelemetry {
namespace proto {
namespace collector {
namespace logs {
namespace v1 {
class ExportLogsServiceRequest;
}
}
}
}
}

namespace telemetry {
namespace v1 {
class RawTelemetryMessage;
}
}

using opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest;

#include <cppkafka/cppkafka.h>

// Kafka message metadata passed to callback
struct KafkaMessageMeta {
    std::string topic;
    int32_t partition;
    int64_t offset;
};

// Callback types for rebalance events
using PartitionAssignmentCallback = std::function<void(const std::vector<int32_t>&)>;
using PartitionRevocationCallback = std::function<void(const std::vector<int32_t>&)>;

class QueueConsumer {
public:
    using MessageCallback = std::function<void(
        const opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest&,
        const KafkaMessageMeta&)>;

    QueueConsumer(const AppenderConfig& config);
    ~QueueConsumer();

    // Initialize the consumer (must be called before start)
    bool initialize();

    // Start consuming messages
    // Calls callback for each message received with Kafka metadata
    void start(MessageCallback callback);

    // Stop consuming (graceful shutdown)
    void stop();

    // Check if consumer is running
    bool isRunning() const { return running_; }

    // Track offset for a partition (called after successful processing)
    void trackOffset(int32_t partition, int64_t offset);

    // Get pending offsets per partition
    std::map<int32_t, int64_t> getPendingOffsets() const;

    // Commit all pending offsets to Kafka
    bool commitPendingOffsets();

    // Clear pending offsets (call after successful commit)
    void clearPendingOffsets();

    // Seek consumer to specific offsets per partition (for recovery)
    bool seekToOffsets(const std::map<int32_t, int64_t>& offsets);

    // Seek a single partition to a specific offset
    bool seekPartition(int32_t partition, int64_t offset);

    // Commit offset for a specific partition
    bool commitPartitionOffset(int32_t partition, int64_t offset);

    // Set callback for partition assignment (rebalance)
    void setAssignmentCallback(PartitionAssignmentCallback callback);

    // Set callback for partition revocation (rebalance)
    void setRevocationCallback(PartitionRevocationCallback callback);

    // Get the configured topic name
    const std::string& getTopic() const { return config_.queue_topic; }

private:
    AppenderConfig config_;
    bool running_;

    std::unique_ptr<cppkafka::Consumer> consumer_;
    std::unique_ptr<cppkafka::Configuration> kafka_config_;

    // Pending offsets per partition (max offset seen for each partition)
    std::map<int32_t, int64_t> pending_offsets_;

    // Rebalance callbacks
    PartitionAssignmentCallback assignment_callback_;
    PartitionRevocationCallback revocation_callback_;

    // Deserialize wrapper message from queue
    telemetry::v1::RawTelemetryMessage deserializeWrapper(const std::string& data);

    // Parse payload into ExportLogsServiceRequest based on content_type
    opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest parsePayload(
        const telemetry::v1::RawTelemetryMessage& wrapper);
};

#endif // QUEUE_CONSUMER_HPP

