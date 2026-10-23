#include "queue_producer.hpp"
#include "ingester_metrics.hpp"
#include "observability/kafka_stats.hpp"
#include "observability/log.hpp"
#include "telemetry_wrapper.pb.h"
#include <chrono>
#include <cstdlib>
#include <string>
#include <utility>
#include <librdkafka/rdkafka.h>

void ProducerCallbacks::dr_cb(rd_kafka_t* /*rk*/, const rd_kafka_message_t* rkmessage, void* opaque) {
    auto* cb = static_cast<ProducerCallbacks*>(opaque);
    if (!cb) return;

    if (rkmessage->err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        if (cb->metrics_) {
            cb->metrics_->delivery_failures.Add({{"error", rd_kafka_err2name(rkmessage->err)}}).Increment();
        }
        if (cb->delivery_failures_) {
            uint64_t total = cb->delivery_failures_->fetch_add(1) + 1;
            cb->logFailureRateLimited(rkmessage->err, total);
        }
    } else if (cb->metrics_) {
        // Microseconds since produce(), measured by librdkafka
        cb->metrics_->delivery_latency.Observe(
            static_cast<double>(rd_kafka_message_latency(rkmessage)) / 1e6);
    }
    if (cb->in_flight_count_) {
        cb->in_flight_count_->fetch_sub(1);
    }
}

int ProducerCallbacks::stats_cb(rd_kafka_t* /*rk*/, char* json, size_t json_len, void* opaque) {
    auto* cb = static_cast<ProducerCallbacks*>(opaque);
    if (cb && cb->stats_) cb->stats_->update(kClient, std::string_view(json, json_len));
    return 0;  // librdkafka frees json
}

void ProducerCallbacks::error_cb(rd_kafka_t* /*rk*/, int err, const char* reason, void* /*opaque*/) {
    const auto code = static_cast<rd_kafka_resp_err_t>(err);
    obs::logKafkaError(kClient, err, rd_kafka_err2name(code), reason,
                       code == RD_KAFKA_RESP_ERR__FATAL);
}

void ProducerCallbacks::log_cb(const rd_kafka_t* /*rk*/, int level, const char* fac, const char* buf) {
    obs::logKafka(kClient, level, fac, buf);
}

void ProducerCallbacks::logFailureRateLimited(rd_kafka_resp_err_t err, uint64_t total_failures) {
    using namespace std::chrono;
    const int64_t now = duration_cast<nanoseconds>(steady_clock::now().time_since_epoch()).count();
    int64_t last = last_log_ns_.load();
    if (now - last < duration_cast<nanoseconds>(seconds(1)).count()) return;
    if (!last_log_ns_.compare_exchange_strong(last, now)) return;

    obs::error("producer", "Kafka delivery failed",
               {{"error", rd_kafka_err2str(err)}, {"total_failures", total_failures}});
}

QueueProducer::QueueProducer(const IngesterConfig& config, std::shared_ptr<IngesterMetrics> metrics,
                             std::shared_ptr<obs::KafkaStatsCollector> stats)
    : config_(config),
      metrics_(std::move(metrics)),
      stats_(std::move(stats)),
      callbacks_(&in_flight_count_, &delivery_failures_, metrics_.get(), stats_.get())
{
}

QueueProducer::~QueueProducer() {
    shutdown();
}

bool QueueProducer::initialize() {
    char errstr[512];

    rd_kafka_conf_t* conf = rd_kafka_conf_new();

    auto set = [&](const char* key, const std::string& value) {
        if (rd_kafka_conf_set(conf, key, value.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
            obs::error("producer", "invalid Kafka producer setting", {{"key", key}, {"error", errstr}});
            return false;
        }
        return true;
    };

    // Delivery retries for broker/network errors are handled inside librdkafka
    // (bounded by delivery.timeout.ms) and reported through the delivery callback.
    const bool configured =
        set("bootstrap.servers", config_.queue_brokers) &&
        set("acks", std::to_string(config_.acks)) &&
        set("compression.type", config_.compression_type) &&
        set("retry.backoff.ms", std::to_string(config_.retry_backoff_ms)) &&
        set("queue.buffering.max.messages", std::to_string(config_.max_in_flight)) &&
        set("queue.buffering.max.kbytes", "1048576") &&  // 1GB
        set("batch.num.messages", "1000") &&
        set("linger.ms", "10") &&
        set("enable.idempotence", "true") &&
        // Must admit the largest payload the HTTP layer accepts, so oversized
        // requests get 413 there instead of failing in librdkafka
        set("message.max.bytes", std::to_string(config_.kafkaMessageMaxBytes())) &&
        set("statistics.interval.ms", std::to_string(config_.kafka_stats_interval_ms));
    if (!configured) {
        rd_kafka_conf_destroy(conf);
        return false;
    }

    rd_kafka_conf_set_dr_msg_cb(conf, ProducerCallbacks::dr_cb);
    rd_kafka_conf_set_stats_cb(conf, ProducerCallbacks::stats_cb);
    rd_kafka_conf_set_error_cb(conf, ProducerCallbacks::error_cb);
    rd_kafka_conf_set_log_cb(conf, ProducerCallbacks::log_cb);
    rd_kafka_conf_set_opaque(conf, &callbacks_);

    // On success rd_kafka_new takes ownership of conf
    producer_ = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!producer_) {
        obs::error("producer", "failed to create the Kafka producer", {{"error", errstr}});
        rd_kafka_conf_destroy(conf);
        return false;
    }

    topic_ = rd_kafka_topic_new(producer_, config_.queue_topic.c_str(), rd_kafka_topic_conf_new());
    if (!topic_) {
        obs::error("producer", "failed to create the topic handle",
                   {{"topic", config_.queue_topic}, {"error", rd_kafka_err2str(rd_kafka_last_error())}});
        rd_kafka_destroy(producer_);
        producer_ = nullptr;
        return false;
    }

    stop_polling_.store(false);
    poll_thread_ = std::thread([this] {
        while (!stop_polling_.load()) {
            rd_kafka_poll(producer_, 100);
        }
    });

    obs::info("producer", "Kafka producer initialized",
              {{"brokers", config_.queue_brokers}, {"topic", config_.queue_topic}});
    return true;
}

ProduceResult QueueProducer::produce(const telemetry::v1::RawTelemetryMessage& message) {
    if (!isReady()) {
        return ProduceResult::PERSISTENT_ERROR;
    }
    if (isAtCapacity()) {
        return ProduceResult::QUEUE_FULL;
    }

    // Serialize once into a malloc'd buffer that librdkafka takes ownership of
    // (RD_KAFKA_MSG_F_FREE), avoiding a second copy of the payload.
    const size_t size = message.ByteSizeLong();
    void* payload = std::malloc(size > 0 ? size : 1);
    if (!payload) {
        return ProduceResult::PERSISTENT_ERROR;
    }
    if (!message.SerializeToArray(payload, static_cast<int>(size))) {
        std::free(payload);
        obs::error("producer", "failed to serialize RawTelemetryMessage");
        return ProduceResult::PERSISTENT_ERROR;
    }

    in_flight_count_.fetch_add(1);

    const int ret = rd_kafka_produce(
        topic_,
        RD_KAFKA_PARTITION_UA,  // Let the partitioner choose
        RD_KAFKA_MSG_F_FREE,    // librdkafka frees payload after delivery
        payload, size,
        nullptr, 0,             // No key
        nullptr);               // Per-message opaque unused

    if (ret == 0) {
        // Delivery callback (on the poll thread) decrements in_flight_count_
        if (metrics_) {
            metrics_->produced_messages.Increment();
            metrics_->produced_bytes.Increment(static_cast<double>(size));
        }
        return ProduceResult::SUCCESS;
    }

    // Synchronous failure: librdkafka did not take ownership of the payload
    const rd_kafka_resp_err_t err = rd_kafka_last_error();
    std::free(payload);
    in_flight_count_.fetch_sub(1);

    switch (err) {
        case RD_KAFKA_RESP_ERR__QUEUE_FULL:
            return ProduceResult::QUEUE_FULL;
        case RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE:
            return ProduceResult::PAYLOAD_TOO_LARGE;
        case RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC:
        case RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION:
            return ProduceResult::RETRYABLE_ERROR;
        default:
            obs::error("producer", "Kafka produce failed", {{"error", rd_kafka_err2str(err)}});
            return ProduceResult::PERSISTENT_ERROR;
    }
}

void QueueProducer::shutdown() {
    // Stop the poll thread first; rd_kafka_flush serves remaining callbacks itself
    if (poll_thread_.joinable()) {
        stop_polling_.store(true);
        poll_thread_.join();
    }

    if (!producer_) return;

    const rd_kafka_resp_err_t err = rd_kafka_flush(producer_, 5000);
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        obs::warn("producer", "messages were not delivered during shutdown",
                  {{"messages", rd_kafka_outq_len(producer_)}});
    }

    if (topic_) {
        rd_kafka_topic_destroy(topic_);
        topic_ = nullptr;
    }

    rd_kafka_destroy(producer_);
    producer_ = nullptr;
}
