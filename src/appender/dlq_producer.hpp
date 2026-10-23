#ifndef APPENDER_DLQ_PRODUCER_HPP
#define APPENDER_DLQ_PRODUCER_HPP

#include "error.hpp"

#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

typedef struct rd_kafka_s rd_kafka_t;

namespace obs {
class KafkaStatsCollector;
}

namespace appender {

// A Kafka message that can never be decoded, with where it came from
struct DlqRecord {
    std::string_view key;
    std::string_view payload;  // the original bytes, untouched
    std::string_view source_topic;
    std::int32_t source_partition = 0;
    std::int64_t source_offset = 0;
    const AppenderError* error = nullptr;
};

class DlqSink {
public:
    virtual ~DlqSink() = default;
    // Succeeds only once the record is durably in the DLQ
    virtual Result<void> send(const DlqRecord& record) = 0;
};

// Header names and values attached to each DLQ message
std::vector<std::pair<std::string, std::string>> dlqHeaders(const DlqRecord& record,
                                                            std::string_view appender_id);

// Produces DLQ records to a Kafka topic and waits for each acknowledgement
// (acks=all, idempotent). Sends are serialized; the DLQ is a rare path.
class KafkaDlqProducer : public DlqSink {
public:
    struct Options {
        std::string brokers;
        std::string topic;
        std::string appender_id;
        std::chrono::milliseconds delivery_timeout{30'000};
        std::size_t max_message_bytes = std::size_t{32} * 1024 * 1024;
        int stats_interval_ms = 0;  // librdkafka statistics; 0 disables them
        std::shared_ptr<obs::KafkaStatsCollector> stats;
    };

    // Name of this client in logs and in the rdkafka_* metrics
    static constexpr const char* kClient = "appender-dlq";

    static Result<std::unique_ptr<KafkaDlqProducer>> create(Options options);
    ~KafkaDlqProducer() override;

    Result<void> send(const DlqRecord& record) override;

    // Serves pending callbacks without blocking. send() polls only while it
    // waits, so this keeps statistics fresh when the DLQ is idle.
    void poll();

private:
    KafkaDlqProducer(Options options, rd_kafka_t* producer);

    Options options_;
    rd_kafka_t* producer_;
    std::mutex mutex_;
};

}  // namespace appender

#endif  // APPENDER_DLQ_PRODUCER_HPP
