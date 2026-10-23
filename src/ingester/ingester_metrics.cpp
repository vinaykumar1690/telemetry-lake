#include "ingester_metrics.hpp"

#include "observability/metrics.hpp"

using prometheus::BuildCounter;
using prometheus::BuildGauge;
using prometheus::BuildHistogram;

namespace {

prometheus::Family<prometheus::Counter>& counterFamily(prometheus::Registry& registry,
                                                       const char* name, const char* help) {
    return BuildCounter().Name(name).Help(help).Register(registry);
}

}  // namespace

IngesterMetrics::IngesterMetrics(prometheus::Registry& registry)
    : requests(counterFamily(registry, "ingester_http_requests_total",
                             "HTTP requests by route and status code")),
      logs_duration(BuildHistogram()
                        .Name("ingester_http_request_duration_seconds")
                        .Help("Time to handle POST /v1/logs after the body was read")
                        .Register(registry)
                        .Add({{"route", "/v1/logs"}},
                             obs::exponentialBuckets(0.0005, 2, 15))),  // 0.5 ms .. 8 s
      logs_in_flight(BuildGauge()
                         .Name("ingester_http_requests_in_flight")
                         .Help("POST /v1/logs requests being handled")
                         .Register(registry)
                         .Add({})),
      received_identity_bytes(counterFamily(registry, "ingester_http_received_bytes_total",
                                            "Request body bytes as received")
                                  .Add({{"encoding", "identity"}})),
      received_gzip_bytes(counterFamily(registry, "ingester_http_received_bytes_total", "")
                              .Add({{"encoding", "gzip"}})),
      payload_protobuf_bytes(counterFamily(registry, "ingester_payload_bytes_total",
                                           "Payload bytes after decompression, written to Kafka")
                                 .Add({{"wire", "protobuf"}})),
      payload_json_bytes(counterFamily(registry, "ingester_payload_bytes_total", "")
                             .Add({{"wire", "json"}})),
      payload_size(BuildHistogram()
                       .Name("ingester_payload_size_bytes")
                       .Help("Payload size after decompression")
                       .Register(registry)
                       .Add({}, obs::exponentialBuckets(1024, 4, 9))),  // 1 KiB .. 64 MiB
      rejected(counterFamily(registry, "ingester_requests_rejected_total",
                             "Rejected POST /v1/logs requests by reason")),
      produced_messages(counterFamily(registry, "ingester_kafka_produced_messages_total",
                                      "Messages accepted by the Kafka producer")
                            .Add({})),
      produced_bytes(counterFamily(registry, "ingester_kafka_produced_bytes_total",
                                   "Bytes of messages accepted by the Kafka producer")
                         .Add({})),
      delivery_failures(counterFamily(registry, "ingester_kafka_delivery_failures_total",
                                      "Messages the producer failed to deliver after its retries")),
      delivery_latency(BuildHistogram()
                           .Name("ingester_kafka_delivery_latency_seconds")
                           .Help("Time from produce() to the broker acknowledgement")
                           .Register(registry)
                           .Add({}, obs::exponentialBuckets(0.001, 2, 14))) {}  // 1 ms .. 8 s
