#ifndef INGESTER_METRICS_HPP
#define INGESTER_METRICS_HPP

#include <prometheus/counter.h>
#include <prometheus/family.h>
#include <prometheus/gauge.h>
#include <prometheus/histogram.h>
#include <prometheus/registry.h>

#include <string>

// Prometheus metrics of the ingester. Rates (requests/s, bytes/s) come from
// rate() over the counters.
struct IngesterMetrics {
    explicit IngesterMetrics(prometheus::Registry& registry);

    // HTTP
    prometheus::Family<prometheus::Counter>& requests;  // {route, code}
    prometheus::Histogram& logs_duration;               // POST /v1/logs handling time
    prometheus::Gauge& logs_in_flight;
    prometheus::Counter& received_identity_bytes;  // request bodies as received
    prometheus::Counter& received_gzip_bytes;
    prometheus::Counter& payload_protobuf_bytes;  // after decompression, i.e. sent to Kafka
    prometheus::Counter& payload_json_bytes;
    prometheus::Histogram& payload_size;
    prometheus::Family<prometheus::Counter>& rejected;  // {reason}

    // Kafka producer
    prometheus::Counter& produced_messages;  // accepted by librdkafka
    prometheus::Counter& produced_bytes;
    prometheus::Family<prometheus::Counter>& delivery_failures;  // {error}
    prometheus::Histogram& delivery_latency;                     // produce() to broker ack

    void countRequest(const std::string& route, int code) {
        requests.Add({{"route", route}, {"code", std::to_string(code)}}).Increment();
    }
    void countRejected(const std::string& reason) { rejected.Add({{"reason", reason}}).Increment(); }
};

#endif  // INGESTER_METRICS_HPP
