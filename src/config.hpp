#ifndef CONFIG_HPP
#define CONFIG_HPP

#include <string>
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

    // HTTP server
    std::string http_host = "0.0.0.0";
    std::uint16_t http_port = 4318;
    unsigned http_threads = 0;  // 0 = one per hardware thread
    std::size_t max_body_bytes = 16 * 1024 * 1024;
    std::size_t max_decompressed_bytes = 64 * 1024 * 1024;

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

        const char* compression = std::getenv("PRODUCER_COMPRESSION");
        if (compression) {
            config.compression_type = compression;
        }

        if (const char* host = std::getenv("HTTP_HOST"); host && *host) {
            config.http_host = host;
        }
        config.http_port = integerFromEnv<std::uint16_t>("HTTP_PORT", config.http_port, 1, 65535);
        config.http_threads = integerFromEnv("HTTP_THREADS", config.http_threads, 0u, 1024u);
        config.max_body_bytes = integerFromEnv<std::size_t>(
            "MAX_BODY_BYTES", config.max_body_bytes, 1, std::size_t{1} << 31);
        config.max_decompressed_bytes = integerFromEnv<std::size_t>(
            "MAX_DECOMPRESSED_BYTES", config.max_decompressed_bytes, 1, std::size_t{1} << 31);

        return config;
    }
};

struct AppenderConfig {
    std::string queue_brokers;
    std::string queue_topic;
    std::string consumer_group = "otel-appender";
    std::string iceberg_catalog_uri;
    std::string s3_endpoint;
    std::string s3_access_key;
    std::string s3_secret_key;
    std::string s3_bucket;
    std::string iceberg_table_name = "logs";
    size_t buffer_size_mb = 100;
    int buffer_time_seconds = 300;  // 5 minutes

    // Multi-partition threading configuration
    size_t partition_buffer_size_mb = 50;      // Per-partition buffer threshold
    int partition_buffer_time_seconds = 60;     // Per-partition time threshold
    int iceberg_commit_retries = 5;             // Max retry attempts for Iceberg commits
    int iceberg_retry_base_delay_ms = 100;      // Base backoff delay
    int iceberg_retry_max_delay_ms = 5000;      // Max backoff cap
    int rebalance_timeout_seconds = 30;         // Timeout for worker shutdown during rebalance

    static AppenderConfig fromEnv() {
        AppenderConfig config;

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

        const char* consumer_group = std::getenv("KAFKA_CONSUMER_GROUP");
        if (consumer_group) {
            config.consumer_group = consumer_group;
        }

        const char* catalog_uri = std::getenv("ICEBERG_CATALOG_URI");
        if (!catalog_uri || strlen(catalog_uri) == 0) {
            throw std::runtime_error("ICEBERG_CATALOG_URI environment variable is required");
        }
        config.iceberg_catalog_uri = catalog_uri;

        const char* s3_endpoint = std::getenv("S3_ENDPOINT");
        if (!s3_endpoint || strlen(s3_endpoint) == 0) {
            throw std::runtime_error("S3_ENDPOINT environment variable is required");
        }
        config.s3_endpoint = s3_endpoint;

        const char* s3_access_key = std::getenv("S3_ACCESS_KEY");
        if (!s3_access_key || strlen(s3_access_key) == 0) {
            throw std::runtime_error("S3_ACCESS_KEY environment variable is required");
        }
        config.s3_access_key = s3_access_key;

        const char* s3_secret_key = std::getenv("S3_SECRET_KEY");
        if (!s3_secret_key || strlen(s3_secret_key) == 0) {
            throw std::runtime_error("S3_SECRET_KEY environment variable is required");
        }
        config.s3_secret_key = s3_secret_key;

        const char* s3_bucket = std::getenv("S3_BUCKET");
        if (!s3_bucket || strlen(s3_bucket) == 0) {
            throw std::runtime_error("S3_BUCKET environment variable is required");
        }
        config.s3_bucket = s3_bucket;

        const char* table_name = std::getenv("ICEBERG_TABLE_NAME");
        if (table_name) {
            config.iceberg_table_name = table_name;
        }

        const char* buffer_size = std::getenv("BUFFER_SIZE_MB");
        if (buffer_size) {
            config.buffer_size_mb = std::atoi(buffer_size);
        }

        const char* buffer_time = std::getenv("BUFFER_TIME_SECONDS");
        if (buffer_time) {
            config.buffer_time_seconds = std::atoi(buffer_time);
        }

        // Multi-partition threading configuration
        const char* partition_buffer_size = std::getenv("PARTITION_BUFFER_SIZE_MB");
        if (partition_buffer_size) {
            config.partition_buffer_size_mb = std::atoi(partition_buffer_size);
        }

        const char* partition_buffer_time = std::getenv("PARTITION_BUFFER_TIME_SECONDS");
        if (partition_buffer_time) {
            config.partition_buffer_time_seconds = std::atoi(partition_buffer_time);
        }

        const char* iceberg_retries = std::getenv("ICEBERG_COMMIT_RETRIES");
        if (iceberg_retries) {
            config.iceberg_commit_retries = std::atoi(iceberg_retries);
        }

        const char* retry_base_delay = std::getenv("ICEBERG_RETRY_BASE_DELAY_MS");
        if (retry_base_delay) {
            config.iceberg_retry_base_delay_ms = std::atoi(retry_base_delay);
        }

        const char* retry_max_delay = std::getenv("ICEBERG_RETRY_MAX_DELAY_MS");
        if (retry_max_delay) {
            config.iceberg_retry_max_delay_ms = std::atoi(retry_max_delay);
        }

        const char* rebalance_timeout = std::getenv("REBALANCE_TIMEOUT_SECONDS");
        if (rebalance_timeout) {
            config.rebalance_timeout_seconds = std::atoi(rebalance_timeout);
        }

        return config;
    }
};

#endif // CONFIG_HPP

