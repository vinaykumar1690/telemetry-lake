#ifndef CONFIG_HPP
#define CONFIG_HPP

#include <string>
#include <cstdlib>
#include <stdexcept>

struct IngesterConfig {
    std::string queue_brokers;
    std::string queue_topic;
    int max_in_flight = 1000;
    int acks = -1;  // -1 means all replicas must acknowledge
    std::string compression_type = "snappy";
    int retry_backoff_ms = 100;
    int max_retries = 3;

    static IngesterConfig fromEnv() {
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

        const char* max_in_flight_str = std::getenv("MAX_IN_FLIGHT");
        if (max_in_flight_str) {
            config.max_in_flight = std::atoi(max_in_flight_str);
        }

        const char* acks_str = std::getenv("PRODUCER_ACKS");
        if (acks_str) {
            config.acks = std::atoi(acks_str);
        }

        const char* compression = std::getenv("PRODUCER_COMPRESSION");
        if (compression) {
            config.compression_type = compression;
        }

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

        return config;
    }
};

#endif // CONFIG_HPP

