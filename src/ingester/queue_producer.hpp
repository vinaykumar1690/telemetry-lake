#ifndef QUEUE_PRODUCER_HPP
#define QUEUE_PRODUCER_HPP

#include "config.hpp"
#include "telemetry_wrapper.pb.h"
#include <librdkafka/rdkafka.h>
#include <atomic>
#include <cstdint>
#include <memory>
#include <thread>

struct IngesterMetrics;
namespace obs {
class KafkaStatsCollector;
}

enum class ProduceResult {
    SUCCESS,
    QUEUE_FULL,        // Local queue saturated - retryable (503)
    PAYLOAD_TOO_LARGE, // Message exceeds message.max.bytes (413)
    PERSISTENT_ERROR,  // Non-retryable (500)
    RETRYABLE_ERROR    // Transient, e.g. topic metadata not yet known (503)
};

// State of the librdkafka callbacks (the producer's opaque). librdkafka invokes
// dr_cb, stats_cb and error_cb from rd_kafka_poll()/rd_kafka_flush(), i.e. on
// QueueProducer's poll thread; log_cb may run on its internal threads.
class ProducerCallbacks {
public:
    ProducerCallbacks(std::atomic<int>* in_flight_count, std::atomic<uint64_t>* delivery_failures,
                      IngesterMetrics* metrics, obs::KafkaStatsCollector* stats)
        : in_flight_count_(in_flight_count),
          delivery_failures_(delivery_failures),
          metrics_(metrics),
          stats_(stats) {}

    static void dr_cb(rd_kafka_t* rk, const rd_kafka_message_t* rkmessage, void* opaque);
    static int stats_cb(rd_kafka_t* rk, char* json, size_t json_len, void* opaque);
    static void error_cb(rd_kafka_t* rk, int err, const char* reason, void* opaque);
    static void log_cb(const rd_kafka_t* rk, int level, const char* fac, const char* buf);

    // Name of this client in logs and in the rdkafka_* metrics
    static constexpr const char* kClient = "ingester";

private:
    // Logs at most once per second so a broker outage cannot flood the log
    void logFailureRateLimited(rd_kafka_resp_err_t err, uint64_t total_failures);

    std::atomic<int>* in_flight_count_;
    std::atomic<uint64_t>* delivery_failures_;
    IngesterMetrics* metrics_;
    obs::KafkaStatsCollector* stats_;
    std::atomic<int64_t> last_log_ns_{0};
};

class QueueProducer {
public:
    // metrics and stats are optional (tests); both must outlive the producer
    explicit QueueProducer(const IngesterConfig& config,
                           std::shared_ptr<IngesterMetrics> metrics = nullptr,
                           std::shared_ptr<obs::KafkaStatsCollector> stats = nullptr);
    ~QueueProducer();

    QueueProducer(const QueueProducer&) = delete;
    QueueProducer& operator=(const QueueProducer&) = delete;

    // Initialize the producer and start the delivery-report poll thread
    // (must be called before produce)
    bool initialize();

    // Enqueue a message for asynchronous delivery. SUCCESS means librdkafka
    // accepted it; broker delivery outcome is reported via the delivery callback.
    ProduceResult produce(const telemetry::v1::RawTelemetryMessage& message);

    // Get current number of in-flight messages
    int getInFlightCount() const { return in_flight_count_.load(); }

    // Number of messages librdkafka failed to deliver after its internal retries
    uint64_t getDeliveryFailureCount() const { return delivery_failures_.load(); }

    // Check if we're at capacity (for backpressure)
    bool isAtCapacity() const {
        return in_flight_count_.load() >= config_.max_in_flight;
    }

    // Check if the producer is initialized and ready to accept messages
    bool isReady() const {
        return producer_ != nullptr && topic_ != nullptr;
    }

    // Stop polling, flush pending messages and release librdkafka handles
    void shutdown();

private:
    IngesterConfig config_;
    std::shared_ptr<IngesterMetrics> metrics_;
    std::shared_ptr<obs::KafkaStatsCollector> stats_;
    std::atomic<int> in_flight_count_{0};
    std::atomic<uint64_t> delivery_failures_{0};

    rd_kafka_t* producer_ = nullptr;
    rd_kafka_topic_t* topic_ = nullptr;
    ProducerCallbacks callbacks_;

    // Serves delivery callbacks continuously. Without it callbacks only run when
    // something else polls, so in_flight_count_ could stop draining.
    // (std::jthread would fit, but Apple libc++ still gates it behind
    // -fexperimental-library.)
    std::thread poll_thread_;
    std::atomic<bool> stop_polling_{false};
};

#endif // QUEUE_PRODUCER_HPP
