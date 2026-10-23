#include "dlq_producer.hpp"

#include "observability/kafka_stats.hpp"

#include <format>
#include <librdkafka/rdkafka.h>

namespace appender {
namespace {

struct Delivery {
    bool done = false;
    rd_kafka_resp_err_t error = RD_KAFKA_RESP_ERR_NO_ERROR;
};

void onDelivery(rd_kafka_t*, const rd_kafka_message_t* message, void*) {
    if (auto* delivery = static_cast<Delivery*>(message->_private)) {
        delivery->error = message->err;
        delivery->done = true;
    }
}

int onStats(rd_kafka_t*, char* json, size_t json_len, void* opaque) {
    if (auto* stats = static_cast<obs::KafkaStatsCollector*>(opaque)) {
        stats->update(KafkaDlqProducer::kClient, std::string_view(json, json_len));
    }
    return 0;  // librdkafka frees json
}

void onError(rd_kafka_t*, int err, const char* reason, void*) {
    const auto code = static_cast<rd_kafka_resp_err_t>(err);
    obs::logKafkaError(KafkaDlqProducer::kClient, err, rd_kafka_err2name(code), reason,
                       code == RD_KAFKA_RESP_ERR__FATAL);
}

void onLog(const rd_kafka_t*, int level, const char* fac, const char* buf) {
    obs::logKafka(KafkaDlqProducer::kClient, level, fac, buf);
}

}  // namespace

std::vector<std::pair<std::string, std::string>> dlqHeaders(const DlqRecord& record,
                                                            std::string_view appender_id) {
    std::vector<std::pair<std::string, std::string>> headers{
        {"x-source-topic", std::string(record.source_topic)},
        {"x-source-partition", std::to_string(record.source_partition)},
        {"x-source-offset", std::to_string(record.source_offset)},
        {"x-appender-id", std::string(appender_id)},
    };
    if (record.error) {
        headers.emplace_back("x-error-kind", std::string(toString(record.error->kind)));
        headers.emplace_back("x-error-message", record.error->message);
    }
    return headers;
}

Result<std::unique_ptr<KafkaDlqProducer>> KafkaDlqProducer::create(Options options) {
    char errstr[512];
    rd_kafka_conf_t* conf = rd_kafka_conf_new();
    auto set = [&](const char* key, const std::string& value) -> Result<void> {
        if (rd_kafka_conf_set(conf, key, value.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
            return fatal(std::format("DLQ producer: cannot set {}: {}", key, errstr));
        }
        return {};
    };
    for (auto result : {
             set("bootstrap.servers", options.brokers),
             set("acks", "all"),
             set("enable.idempotence", "true"),
             set("linger.ms", "0"),
             set("message.timeout.ms", std::to_string(options.delivery_timeout.count())),
             // Poison payloads can be as large as anything the ingester accepts
             set("message.max.bytes", std::to_string(options.max_message_bytes)),
             set("statistics.interval.ms", std::to_string(options.stats_interval_ms)),
         }) {
        if (!result) {
            rd_kafka_conf_destroy(conf);
            return std::unexpected(result.error());
        }
    }
    rd_kafka_conf_set_dr_msg_cb(conf, onDelivery);
    rd_kafka_conf_set_error_cb(conf, onError);
    rd_kafka_conf_set_log_cb(conf, onLog);
    if (options.stats) {
        rd_kafka_conf_set_stats_cb(conf, onStats);
        rd_kafka_conf_set_opaque(conf, options.stats.get());  // kept alive by options_
    }

    rd_kafka_t* producer = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!producer) {
        rd_kafka_conf_destroy(conf);
        return fatal(std::format("DLQ producer: {}", errstr));
    }
    return std::unique_ptr<KafkaDlqProducer>(new KafkaDlqProducer(std::move(options), producer));
}

KafkaDlqProducer::KafkaDlqProducer(Options options, rd_kafka_t* producer)
    : options_(std::move(options)), producer_(producer) {}

KafkaDlqProducer::~KafkaDlqProducer() {
    rd_kafka_flush(producer_, 5'000);
    rd_kafka_destroy(producer_);
}

void KafkaDlqProducer::poll() {
    // A send in progress polls anyway; its delivery report writes into its frame
    std::unique_lock lock(mutex_, std::try_to_lock);
    if (lock.owns_lock()) rd_kafka_poll(producer_, 0);
}

Result<void> KafkaDlqProducer::send(const DlqRecord& record) {
    std::lock_guard lock(mutex_);

    rd_kafka_headers_t* headers = rd_kafka_headers_new(6);
    for (const auto& [name, value] : dlqHeaders(record, options_.appender_id)) {
        rd_kafka_header_add(headers, name.data(), static_cast<ssize_t>(name.size()), value.data(),
                            static_cast<ssize_t>(value.size()));
    }

    Delivery delivery;
    const auto err = rd_kafka_producev(
        producer_, RD_KAFKA_V_TOPIC(options_.topic.c_str()),
        RD_KAFKA_V_VALUE(const_cast<char*>(record.payload.data()), record.payload.size()),
        RD_KAFKA_V_KEY(record.key.data(), record.key.size()),
        RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY), RD_KAFKA_V_HEADERS(headers),
        RD_KAFKA_V_OPAQUE(&delivery), RD_KAFKA_V_END);
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        rd_kafka_headers_destroy(headers);  // ownership passes only on success
        return retryable(std::format("DLQ produce failed: {}", rd_kafka_err2str(err)));
    }

    // message.timeout.ms bounds delivery, so the report always arrives; it
    // must, because it writes into this stack frame
    while (!delivery.done) rd_kafka_poll(producer_, 100);

    if (delivery.error != RD_KAFKA_RESP_ERR_NO_ERROR) {
        return retryable(std::format("DLQ delivery failed: {}", rd_kafka_err2str(delivery.error)));
    }
    return {};
}

}  // namespace appender
