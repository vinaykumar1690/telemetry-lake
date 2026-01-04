#ifndef QUEUE_CONSUMER_HPP
#define QUEUE_CONSUMER_HPP

#include "../config.hpp"
#include <string>
#include <functional>
#include <memory>

// Forward declaration
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

using opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest;

#include <cppkafka/cppkafka.h>

class QueueConsumer {
public:
    using MessageCallback = std::function<void(const opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest&)>;

    QueueConsumer(const AppenderConfig& config);
    ~QueueConsumer();

    // Initialize the consumer (must be called before start)
    bool initialize();

    // Start consuming messages
    // Calls callback for each message received
    void start(MessageCallback callback);

    // Stop consuming (graceful shutdown)
    void stop();

    // Check if consumer is running
    bool isRunning() const { return running_; }

private:
    AppenderConfig config_;
    bool running_;

    std::unique_ptr<cppkafka::Consumer> consumer_;
    std::unique_ptr<cppkafka::Configuration> kafka_config_;

    opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest deserializeMessage(const std::string& data);
};

#endif // QUEUE_CONSUMER_HPP

