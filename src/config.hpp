#ifndef CONFIG_HPP
#define CONFIG_HPP

#include <string>
#include <algorithm>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <expected>
#include <format>
#include <stdexcept>
#include <string_view>

namespace config_detail {

// Strict integer parse: the whole string must be a number of type T
template <typename T>
std::expected<T, std::errc> parseInteger(std::string_view text) {
    T value{};
    const char* end = text.data() + text.size();
    auto [ptr, ec] = std::from_chars(text.data(), end, value);
    if (ec != std::errc{}) return std::unexpected(ec);
    if (ptr != end || text.empty()) return std::unexpected(std::errc::invalid_argument);
    return value;
}

// Read an integer env var in [min, max]; unset keeps the default, anything
// unparsable or out of range throws instead of silently becoming 0
template <typename T>
T integerFromEnv(const char* name, T default_value, T min, T max) {
    const char* raw = std::getenv(name);
    if (!raw) return default_value;
    auto parsed = parseInteger<T>(raw);
    if (!parsed || *parsed < min || *parsed > max) {
        throw std::runtime_error(std::format("{} must be an integer in [{}, {}], got '{}'",
                                             name, min, max, raw));
    }
    return *parsed;
}

}  // namespace config_detail

struct IngesterConfig {
    std::string queue_brokers;
    std::string queue_topic;
    int max_in_flight = 1000;
    int acks = -1;  // -1 means all replicas must acknowledge
    std::string compression_type = "snappy";
    int retry_backoff_ms = 100;
    // librdkafka statistics (rdkafka_* metrics); 0 disables them
    int kafka_stats_interval_ms = 15'000;

    // HTTP server
    std::string http_host = "0.0.0.0";
    std::uint16_t http_port = 4318;
    unsigned http_threads = 0;  // 0 = one per hardware thread
    std::size_t max_body_bytes = 16 * 1024 * 1024;
    // Each accepted payload becomes one Kafka message; the producer's
    // message.max.bytes is derived from these limits (see kafkaMessageMaxBytes)
    std::size_t max_decompressed_bytes = 16 * 1024 * 1024;

    // Prometheus /metrics, on its own port: the HTTP port is reachable
    // through the UI's /api/ingest/ proxy
    std::uint16_t metrics_port = 9464;

    // Room for the RawTelemetryMessage wrapper fields and Kafka record overhead
    static constexpr std::size_t kKafkaMessageOverheadBytes = 64 * 1024;
    // Upper bound for both body limits (librdkafka allows at most 1,000,000,000)
    static constexpr std::size_t kMaxPayloadLimitBytes = std::size_t{512} * 1024 * 1024;

    // Largest Kafka message the ingester can produce
    std::size_t kafkaMessageMaxBytes() const {
        return std::max(max_body_bytes, max_decompressed_bytes) + kKafkaMessageOverheadBytes;
    }

    static IngesterConfig fromEnv() {
        using config_detail::integerFromEnv;
        IngesterConfig config;

        const char* brokers = std::getenv("KAFKA_BROKERS");
        if (!brokers || strlen(brokers) == 0) {
            throw std::runtime_error("KAFKA_BROKERS environment variable is required");
        }
        config.queue_brokers = brokers;

        const char* topic = std::getenv("KAFKA_TOPIC");
        if (!topic || strlen(topic) == 0) {
            config.queue_topic = "otel-logs";  // default
        } else {
            config.queue_topic = topic;
        }

        config.max_in_flight = integerFromEnv("MAX_IN_FLIGHT", config.max_in_flight, 1, 10'000'000);
        config.acks = integerFromEnv("PRODUCER_ACKS", config.acks, -1, 1000);
        config.kafka_stats_interval_ms = integerFromEnv(
            "KAFKA_STATS_INTERVAL_MS", config.kafka_stats_interval_ms, 0, 86'400'000);

        const char* compression = std::getenv("PRODUCER_COMPRESSION");
        if (compression) {
            config.compression_type = compression;
        }

        if (const char* host = std::getenv("HTTP_HOST"); host && *host) {
            config.http_host = host;
        }
        config.http_port = integerFromEnv<std::uint16_t>("HTTP_PORT", config.http_port, 1, 65535);
        config.metrics_port = integerFromEnv<std::uint16_t>("METRICS_PORT", config.metrics_port, 1, 65535);
        config.http_threads = integerFromEnv("HTTP_THREADS", config.http_threads, 0u, 1024u);
        config.max_body_bytes = integerFromEnv<std::size_t>(
            "MAX_BODY_BYTES", config.max_body_bytes, 1, kMaxPayloadLimitBytes);
        config.max_decompressed_bytes = integerFromEnv<std::size_t>(
            "MAX_DECOMPRESSED_BYTES", config.max_decompressed_bytes, 1, kMaxPayloadLimitBytes);

        return config;
    }
};

#endif // CONFIG_HPP

