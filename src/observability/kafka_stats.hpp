#ifndef OBSERVABILITY_KAFKA_STATS_HPP
#define OBSERVABILITY_KAFKA_STATS_HPP

#include "observability/log.hpp"

#include <nlohmann/json.hpp>
#include <prometheus/collectable.h>

#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

// librdkafka telemetry: its statistics JSON (statistics.interval.ms) becomes
// rdkafka_* metrics labeled by client, and its log and error callbacks go to
// the logger

namespace obs {

class KafkaStatsCollector : public prometheus::Collectable {
public:
    // Stores the latest statistics document of a client (called from the
    // librdkafka stats callback). False, keeping the previous one, if the
    // document is not valid JSON.
    bool update(std::string_view client, std::string_view json);

    // High watermark per partition of a topic, from the client's latest
    // statistics; partitions without a known watermark are absent
    std::map<std::int32_t, std::int64_t> highWatermarks(std::string_view client,
                                                       std::string_view topic) const;

    std::vector<prometheus::MetricFamily> Collect() const override;

private:
    mutable std::mutex mutex_;
    std::map<std::string, nlohmann::json, std::less<>> stats_;
};

// librdkafka log levels are syslog levels (0 emergency ... 7 debug)
Level kafkaLogLevel(int syslog_level);

// For librdkafka's log callback
void logKafka(std::string_view client, int syslog_level, std::string_view facility,
              std::string_view message);

// For librdkafka's error callback; fatal errors are logged as errors, the
// rest (broker down, timeouts) as warnings because librdkafka recovers
void logKafkaError(std::string_view client, int error_code, std::string_view error_name,
                   std::string_view reason, bool fatal);

}  // namespace obs

#endif  // OBSERVABILITY_KAFKA_STATS_HPP
