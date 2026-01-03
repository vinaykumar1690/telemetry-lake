#ifndef QUEUE_PRODUCER_HPP
#define QUEUE_PRODUCER_HPP

#include "../config.hpp"
#include "opentelemetry/proto/collector/logs/v1/logs_service.pb.h"
#include <librdkafka/rdkafka.h>
#include <string>
#include <atomic>
#include <memory>
#include <mutex>

enum class ProduceResult {
    SUCCESS,
    QUEUE_FULL,      // Return 503
    PERSISTENT_ERROR, // Return 500
    RETRYABLE_ERROR  // Can retry
};

// Delivery report callback class
class DeliveryReportCb {
public:
    DeliveryReportCb(std::atomic<int>* in_flight_count) 
        : in_flight_count_(in_flight_count) {}

    // Static callback function for librdkafka
    static void dr_cb(rd_kafka_t* rk, const rd_kafka_message_t* rkmessage, void* opaque) {
        DeliveryReportCb* cb = static_cast<DeliveryReportCb*>(opaque);
        if (cb && cb->in_flight_count_) {
            if (rkmessage->err == RD_KAFKA_RESP_ERR_NO_ERROR) {
                // Success - decrement in-flight count
                cb->in_flight_count_->fetch_sub(1);
            } else {
                // Failure - decrement in-flight count
                cb->in_flight_count_->fetch_sub(1);
            }
        }
    }

private:
    std::atomic<int>* in_flight_count_;
};

class QueueProducer {
public:
    QueueProducer(const IngesterConfig& config);
    ~QueueProducer();

    // Initialize the producer (must be called before produce)
    bool initialize();

    // Produce a message to the queue
    // Returns SUCCESS if message was successfully queued
    ProduceResult produce(const opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest& request);

    // Get current number of in-flight messages
    int getInFlightCount() const { return in_flight_count_.load(); }

    // Check if we're at capacity (for backpressure)
    bool isAtCapacity() const {
        return in_flight_count_.load() >= config_.max_in_flight;
    }

    // Shutdown gracefully
    void shutdown();

private:
    IngesterConfig config_;
    std::atomic<int> in_flight_count_;
    std::mutex produce_mutex_;

    rd_kafka_t* producer_;
    rd_kafka_topic_t* topic_;
    DeliveryReportCb delivery_cb_;

    ProduceResult produceWithRetry(const std::string& serialized_data, int retry_count = 0);
    std::string serializeRequest(const opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest& request);
};

#endif // QUEUE_PRODUCER_HPP

