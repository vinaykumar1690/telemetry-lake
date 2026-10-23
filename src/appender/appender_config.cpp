#include "appender_config.hpp"

#include "../config.hpp"
#include "schema.hpp"
#include "sql.hpp"

#include <algorithm>
#include <array>
#include <cctype>
#include <cstdlib>
#include <format>
#include <ranges>
#include <set>
#include <stdexcept>
#include <thread>
#include <unistd.h>

namespace appender {
namespace {

using config_detail::integerFromEnv;

std::string requiredEnv(const char* name) {
    const char* value = std::getenv(name);
    if (!value || !*value) {
        throw std::runtime_error(std::format("{} environment variable is required", name));
    }
    return value;
}

void optionalEnv(const char* name, std::string& target) {
    if (const char* value = std::getenv(name); value && *value) {
        target = value;
    }
}

PartitionSpec partitionSpecFromEnv(PartitionSpec default_value) {
    const char* raw = std::getenv("ICEBERG_PARTITION_SPEC");
    if (!raw || !*raw) return default_value;
    const std::string_view value = raw;
    if (value == "day") return PartitionSpec::Day;
    if (value == "hour") return PartitionSpec::Hour;
    if (value == "none") return PartitionSpec::None;
    throw std::runtime_error(
        std::format("ICEBERG_PARTITION_SPEC must be one of day, hour, none; got '{}'", value));
}

// PROMOTED_ATTRIBUTES: comma-separated attribute keys
std::vector<std::string> promotedAttributesFromEnv() {
    constexpr std::size_t kMaxPromoted = 32;
    std::vector<std::string> keys;
    const char* raw = std::getenv("PROMOTED_ATTRIBUTES");
    if (!raw || !*raw) return keys;
    std::set<std::string> columns;
    for (auto part : std::string_view(raw) | std::views::split(',')) {
        std::string_view text(part.begin(), part.end());
        const auto first = text.find_first_not_of(" \t");
        const auto last = text.find_last_not_of(" \t");
        std::string key(first == std::string_view::npos ? "" : text.substr(first, last - first + 1));
        auto fail = [&](std::string_view why) {
            return std::runtime_error(std::format("PROMOTED_ATTRIBUTES: '{}' {}", key, why));
        };
        if (key.empty()) throw fail("is empty");
        if (!std::ranges::all_of(key, [](unsigned char c) {
                return std::isalnum(c) || c == '.' || c == '_' || c == '-';
            })) {
            throw fail("may only contain letters, digits, '.', '_' and '-'");
        }
        const auto column = schema::promotedColumnName(key);
        if (std::ranges::any_of(schema::kLogsColumns, [&](const auto& c) { return c.name == column; })) {
            throw fail(std::format("maps to the existing column {}", column));
        }
        if (!columns.insert(column).second) throw fail(std::format("maps to column {} twice", column));
        keys.push_back(std::move(key));
    }
    if (keys.size() > kMaxPromoted) {
        throw std::runtime_error(std::format("PROMOTED_ATTRIBUTES: at most {} keys", kMaxPromoted));
    }
    return keys;
}

std::string hostname() {
    std::array<char, 256> buf{};
    if (gethostname(buf.data(), buf.size() - 1) == 0 && buf[0] != '\0') {
        return buf.data();
    }
    return "otel-appender";
}

std::string qualified(const AppenderConfig& config, const std::string& table) {
    return std::format("{}.{}.{}", AppenderConfig::kCatalogAlias,
                       sql::identifier(config.iceberg_namespace), sql::identifier(table));
}

}  // namespace

std::string AppenderConfig::qualifiedLogsTable() const {
    return qualified(*this, table_name);
}

std::string AppenderConfig::qualifiedOffsetsTable() const {
    return qualified(*this, offsets_table_name);
}

AppenderConfig AppenderConfig::fromEnv() {
    constexpr std::size_t kMiB = 1024 * 1024;
    AppenderConfig config;

    config.brokers = requiredEnv("KAFKA_BROKERS");
    optionalEnv("KAFKA_TOPIC", config.topic);
    optionalEnv("KAFKA_CONSUMER_GROUP", config.consumer_group);
    config.dlq_topic = config.topic + "-dlq";
    optionalEnv("KAFKA_DLQ_TOPIC", config.dlq_topic);
    config.max_poll_interval_ms =
        integerFromEnv("MAX_POLL_INTERVAL_MS", config.max_poll_interval_ms, 10'000, 86'400'000);
    config.kafka_stats_interval_ms = integerFromEnv(
        "KAFKA_STATS_INTERVAL_MS", config.kafka_stats_interval_ms, 0, 86'400'000);

    config.catalog_uri = requiredEnv("ICEBERG_CATALOG_URI");
    optionalEnv("ICEBERG_WAREHOUSE", config.warehouse);
    optionalEnv("ICEBERG_NAMESPACE", config.iceberg_namespace);
    optionalEnv("ICEBERG_TABLE_NAME", config.table_name);
    optionalEnv("ICEBERG_OFFSETS_TABLE", config.offsets_table_name);
    config.partition_spec = partitionSpecFromEnv(config.partition_spec);
    optionalEnv("ICEBERG_ACCESS_DELEGATION_MODE", config.access_delegation_mode);
    if (config.access_delegation_mode != "none" &&
        config.access_delegation_mode != "vended_credentials") {
        throw std::runtime_error(std::format(
            "ICEBERG_ACCESS_DELEGATION_MODE must be none or vended_credentials; got '{}'",
            config.access_delegation_mode));
    }
    config.promoted_attributes = promotedAttributesFromEnv();
    if (config.table_name == config.offsets_table_name) {
        throw std::runtime_error("ICEBERG_TABLE_NAME and ICEBERG_OFFSETS_TABLE must differ");
    }

    config.s3_endpoint = requiredEnv("S3_ENDPOINT");
    config.s3_access_key = requiredEnv("S3_ACCESS_KEY");
    config.s3_secret_key = requiredEnv("S3_SECRET_KEY");
    optionalEnv("S3_BUCKET", config.s3_bucket);
    if (config.warehouse.empty()) config.warehouse = config.s3_bucket;
    optionalEnv("S3_REGION", config.s3_region);
    optionalEnv("S3_URL_STYLE", config.s3_url_style);
    if (config.s3_endpoint.starts_with("https://")) {
        config.s3_use_ssl = true;
        config.s3_endpoint.erase(0, 8);
    } else if (config.s3_endpoint.starts_with("http://")) {
        config.s3_endpoint.erase(0, 7);
    }
    if (config.s3_url_style != "path" && config.s3_url_style != "vhost") {
        throw std::runtime_error(std::format("S3_URL_STYLE must be path or vhost; got '{}'",
                                             config.s3_url_style));
    }

    config.flush_bytes =
        integerFromEnv<std::size_t>("BUFFER_SIZE_MB", config.flush_bytes / kMiB, 1, 4096) * kMiB;
    config.flush_interval = std::chrono::seconds{integerFromEnv(
        "BUFFER_TIME_SECONDS", static_cast<int>(config.flush_interval.count()), 1, 3600)};
    config.max_staged_bytes =
        integerFromEnv<std::size_t>("MAX_STAGED_MB", config.max_staged_bytes / kMiB, 1, 65536) *
        kMiB;
    if (config.max_staged_bytes < config.flush_bytes) {
        throw std::runtime_error("MAX_STAGED_MB must be at least BUFFER_SIZE_MB");
    }

    config.pipeline_threads = integerFromEnv("PIPELINE_THREADS", config.pipeline_threads, 0u, 64u);
    if (config.pipeline_threads == 0) {
        config.pipeline_threads = std::clamp(std::thread::hardware_concurrency(), 1u, 16u);
    }
    config.pipeline_queue_bytes =
        integerFromEnv<std::size_t>("PIPELINE_QUEUE_MB", config.pipeline_queue_bytes / kMiB, 1,
                                    4096) *
        kMiB;

    config.commit_retries = integerFromEnv("ICEBERG_COMMIT_RETRIES", config.commit_retries, 1, 100);
    config.retry_base_delay = std::chrono::milliseconds{integerFromEnv(
        "ICEBERG_RETRY_BASE_DELAY_MS", static_cast<int>(config.retry_base_delay.count()), 1,
        60'000)};
    config.retry_max_delay = std::chrono::milliseconds{integerFromEnv(
        "ICEBERG_RETRY_MAX_DELAY_MS", static_cast<int>(config.retry_max_delay.count()), 1,
        600'000)};
    if (config.retry_max_delay < config.retry_base_delay) {
        throw std::runtime_error(
            "ICEBERG_RETRY_MAX_DELAY_MS must be at least ICEBERG_RETRY_BASE_DELAY_MS");
    }
    config.revoke_timeout = std::chrono::seconds{integerFromEnv(
        "REVOKE_TIMEOUT_SECONDS", static_cast<int>(config.revoke_timeout.count()), 1, 3600)};
    // The revoke callback runs inside poll(); exceeding the poll interval gets
    // the consumer evicted while it still believes it owns the partitions
    if (std::chrono::milliseconds{config.revoke_timeout} >=
        std::chrono::milliseconds{config.max_poll_interval_ms}) {
        throw std::runtime_error("REVOKE_TIMEOUT_SECONDS must be below MAX_POLL_INTERVAL_MS");
    }
    config.offsets_maintenance_interval = std::chrono::seconds{integerFromEnv(
        "OFFSETS_MAINTENANCE_INTERVAL_SECONDS",
        static_cast<int>(config.offsets_maintenance_interval.count()), 0, 86'400 * 7)};

    optionalEnv("DUCKDB_HOME_DIRECTORY", config.duckdb_home_directory);
    config.health_port = integerFromEnv<std::uint16_t>("HEALTH_PORT", config.health_port, 1, 65535);
    config.appender_id = hostname();
    optionalEnv("APPENDER_ID", config.appender_id);

    return config;
}

}  // namespace appender
