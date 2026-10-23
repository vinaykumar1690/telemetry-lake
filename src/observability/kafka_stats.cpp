#include "observability/kafka_stats.hpp"

#include "observability/metrics.hpp"

#include <algorithm>
#include <charconv>
#include <optional>

namespace obs {
namespace {

using prometheus::MetricType;
using Labels = std::vector<prometheus::ClientMetric::Label>;
using nlohmann::json;

// Families in first-seen order; each name appears once across all clients
class Families {
public:
    void add(const std::string& name, const char* help, MetricType type, double value,
             Labels labels) {
        auto [it, inserted] = index_.try_emplace(name, builders_.size());
        if (inserted) builders_.emplace_back(name, help, type);
        builders_[it->second].add(value, std::move(labels));
    }

    std::vector<prometheus::MetricFamily> take() {
        std::vector<prometheus::MetricFamily> out;
        for (auto& builder : builders_) out.push_back(builder.take());
        return out;
    }

private:
    std::map<std::string, std::size_t> index_;
    std::vector<FamilyBuilder> builders_;
};

std::optional<double> number(const json& object, const char* key) {
    auto it = object.find(key);
    if (it == object.end() || !it->is_number()) return std::nullopt;
    return it->get<double>();
}

Labels with(Labels labels, std::string name, std::string value) {
    labels.push_back({std::move(name), std::move(value)});
    return labels;
}

struct Field {
    const char* key;
    const char* name;
    const char* help;
    MetricType type;
    double scale = 1;  // e.g. microseconds to seconds
};

void addFields(Families& families, const json& object, const Labels& labels,
               std::initializer_list<Field> fields, bool skip_negative = false) {
    for (const auto& field : fields) {
        auto value = number(object, field.key);
        if (!value || (skip_negative && *value < 0)) continue;
        families.add(field.name, field.help, field.type, *value * field.scale, labels);
    }
}

constexpr auto kCounter = MetricType::Counter;
constexpr auto kGauge = MetricType::Gauge;
constexpr double kMicros = 1e-6;
constexpr double kMillis = 1e-3;

void collectClient(Families& families, const std::string& client, const json& stats) {
    const Labels base{{"client", client}};
    const bool consumer = stats.value("type", "") == "consumer";

    if (!consumer) {
        addFields(families, stats, base, {
            {"msg_cnt", "rdkafka_producer_queue_messages", "Messages in the producer queues", kGauge},
            {"msg_size", "rdkafka_producer_queue_bytes", "Bytes in the producer queues", kGauge},
        });
    }
    addFields(families, stats, base, {
        {"replyq", "rdkafka_reply_queue_ops", "Ops waiting in the reply queue to be served by poll", kGauge},
        {"tx", "rdkafka_tx_requests_total", "Requests sent to brokers", kCounter},
        {"tx_bytes", "rdkafka_tx_bytes_total", "Bytes sent to brokers", kCounter},
        {"rx", "rdkafka_rx_responses_total", "Responses received from brokers", kCounter},
        {"rx_bytes", "rdkafka_rx_bytes_total", "Bytes received from brokers", kCounter},
        {"txmsgs", "rdkafka_tx_messages_total", "Messages produced to brokers", kCounter},
        {"txmsg_bytes", "rdkafka_tx_message_bytes_total", "Message bytes produced to brokers", kCounter},
        {"rxmsgs", "rdkafka_rx_messages_total", "Messages consumed from brokers", kCounter},
        {"rxmsg_bytes", "rdkafka_rx_message_bytes_total", "Message bytes consumed from brokers", kCounter},
    });

    if (auto brokers = stats.find("brokers"); brokers != stats.end() && brokers->is_object()) {
        for (const auto& [name, broker] : brokers->items()) {
            // Internal and logical (GroupCoordinator) entries are not connections
            // of their own
            const auto source = broker.value("source", "");
            if (source == "internal" || source == "logical") continue;
            const auto labels = with(base, "broker", name);
            families.add("rdkafka_broker_up", "1 while the broker connection is up", kGauge,
                         broker.value("state", "") == "UP" ? 1 : 0, labels);
            addFields(families, broker, labels, {
                {"outbuf_cnt", "rdkafka_broker_outbuf_requests", "Requests waiting to be sent", kGauge},
                {"waitresp_cnt", "rdkafka_broker_waitresp_requests", "Requests waiting for a response", kGauge},
                {"req_timeouts", "rdkafka_broker_request_timeouts_total", "Requests that timed out", kCounter},
                {"txerrs", "rdkafka_broker_tx_errors_total", "Transmission errors", kCounter},
                {"txretries", "rdkafka_broker_tx_retries_total", "Request retries", kCounter},
                {"rxerrs", "rdkafka_broker_rx_errors_total", "Response errors", kCounter},
                {"connects", "rdkafka_broker_connects_total", "Connection attempts", kCounter},
                {"disconnects", "rdkafka_broker_disconnects_total", "Disconnects", kCounter},
            });
            if (auto rtt = broker.find("rtt"); rtt != broker.end() && rtt->is_object()) {
                addFields(families, *rtt, labels, {
                    {"avg", "rdkafka_broker_rtt_avg_seconds", "Average broker round-trip time (last window)", kGauge, kMicros},
                    {"p99", "rdkafka_broker_rtt_p99_seconds", "p99 broker round-trip time (last window)", kGauge, kMicros},
                });
            }
            if (auto latency = broker.find("int_latency"); !consumer && latency != broker.end() &&
                                                            latency->is_object()) {
                addFields(families, *latency, labels, {
                    {"p99", "rdkafka_broker_queue_latency_p99_seconds",
                     "p99 time from produce() to the broker send queue (last window)", kGauge, kMicros},
                });
            }
        }
    }

    if (auto topics = stats.find("topics"); topics != stats.end() && topics->is_object()) {
        for (const auto& [topic_name, topic] : topics->items()) {
            const auto topic_labels = with(base, "topic", topic_name);
            if (!consumer) {
                for (auto [key, name, help] : {
                         std::tuple{"batchsize", "rdkafka_topic_batch_size_avg_bytes", "Average batch size (last window)"},
                         std::tuple{"batchcnt", "rdkafka_topic_batch_messages_avg", "Average messages per batch (last window)"}}) {
                    if (auto window = topic.find(key); window != topic.end() && window->is_object()) {
                        if (auto avg = number(*window, "avg")) families.add(name, help, kGauge, *avg, topic_labels);
                    }
                }
            }
            auto partitions = topic.find("partitions");
            if (partitions == topic.end() || !partitions->is_object()) continue;
            for (const auto& [id, partition] : partitions->items()) {
                if (id == "-1") continue;  // librdkafka's internal unassigned partition
                // A consumer reports every partition it knows; keep the assigned ones
                if (consumer && !partition.value("desired", false)) continue;
                const auto labels = with(topic_labels, "partition", id);
                if (consumer) {
                    // librdkafka's own consumer_lag counts from the committed
                    // offset, which for the appender is only a hint
                    const auto hi = number(partition, "hi_offset");
                    const auto position = number(partition, "app_offset");
                    if (hi && position && *hi >= 0 && *position >= 0) {
                        families.add("rdkafka_consumer_lag_messages",
                                     "High watermark minus the consumer position", kGauge,
                                     std::max(0.0, *hi - *position), labels);
                    }
                    addFields(families, partition, labels, {
                        {"hi_offset", "rdkafka_partition_high_watermark", "Partition high watermark", kGauge},
                        {"ls_offset", "rdkafka_partition_last_stable_offset", "Partition last stable offset", kGauge},
                        {"app_offset", "rdkafka_consumer_position", "Next offset handed to the application", kGauge},
                        {"committed_offset", "rdkafka_consumer_committed_offset",
                         "Offset committed to the consumer group (a hint for the appender)", kGauge},
                        {"fetchq_cnt", "rdkafka_consumer_fetch_queue_messages", "Pre-fetched messages", kGauge},
                        {"fetchq_size", "rdkafka_consumer_fetch_queue_bytes", "Pre-fetched bytes", kGauge},
                    }, /*skip_negative=*/true);
                } else {
                    addFields(families, partition, labels, {
                        {"msgq_cnt", "rdkafka_partition_queue_messages", "Messages waiting in the partition queue", kGauge},
                        {"xmit_msgq_cnt", "rdkafka_partition_xmit_queue_messages",
                         "Messages ready to be sent to the broker", kGauge},
                        {"msgs_inflight", "rdkafka_partition_inflight_messages", "Messages in flight to the broker", kGauge},
                    }, /*skip_negative=*/true);
                }
            }
        }
    }

    if (auto cgrp = stats.find("cgrp"); cgrp != stats.end() && cgrp->is_object()) {
        families.add("rdkafka_cgrp_up", "1 while the consumer group state is up", kGauge,
                     cgrp->value("state", "") == "up" ? 1 : 0, base);
        addFields(families, *cgrp, base, {
            {"rebalance_cnt", "rdkafka_cgrp_rebalances_total", "Rebalances (assign or revoke)", kCounter},
            {"rebalance_age", "rdkafka_cgrp_rebalance_age_seconds", "Time since the last rebalance", kGauge, kMillis},
            {"assignment_size", "rdkafka_cgrp_assigned_partitions", "Partitions currently assigned", kGauge},
        });
    }
}

}  // namespace

bool KafkaStatsCollector::update(std::string_view client, std::string_view text) {
    auto parsed = json::parse(text, nullptr, /*allow_exceptions=*/false);
    if (parsed.is_discarded() || !parsed.is_object()) return false;
    std::lock_guard lock(mutex_);
    stats_.insert_or_assign(std::string(client), std::move(parsed));
    return true;
}

std::map<std::int32_t, Watermarks> KafkaStatsCollector::watermarks(std::string_view client,
                                                                   std::string_view topic) const {
    std::map<std::int32_t, Watermarks> out;
    std::lock_guard lock(mutex_);
    auto stats = stats_.find(client);
    if (stats == stats_.end()) return out;
    const auto pointer = json::json_pointer("/topics/" + std::string(topic) + "/partitions");
    if (!stats->second.contains(pointer)) return out;
    for (const auto& [id, partition] : stats->second.at(pointer).items()) {
        std::int32_t number_id = 0;
        auto [ptr, ec] = std::from_chars(id.data(), id.data() + id.size(), number_id);
        if (ec != std::errc{} || number_id < 0) continue;
        const auto hi = partition.value("hi_offset", std::int64_t{-1});
        if (hi < 0) continue;
        out[number_id] = {std::max<std::int64_t>(partition.value("lo_offset", std::int64_t{0}), 0), hi};
    }
    return out;
}

std::vector<prometheus::MetricFamily> KafkaStatsCollector::Collect() const {
    Families families;
    std::lock_guard lock(mutex_);
    for (const auto& [client, stats] : stats_) collectClient(families, client, stats);
    return families.take();
}

Level kafkaLogLevel(int syslog_level) {
    if (syslog_level <= 3) return Level::Error;
    if (syslog_level == 4) return Level::Warn;
    if (syslog_level <= 6) return Level::Info;
    return Level::Debug;
}

void logKafka(std::string_view client, int syslog_level, std::string_view facility,
              std::string_view message) {
    const auto level = kafkaLogLevel(syslog_level);
    if (!enabled(level)) return;
    log(level, "kafka", message, {{"client", client}, {"facility", facility}});
}

void logKafkaError(std::string_view client, int error_code, std::string_view error_name,
                   std::string_view reason, bool fatal) {
    log(fatal ? Level::Error : Level::Warn, "kafka", reason,
        {{"client", client}, {"error", error_name}, {"error_code", error_code}, {"fatal", fatal}});
}

}  // namespace obs
