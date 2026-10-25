#ifndef APPENDER_CONFIG_HPP
#define APPENDER_CONFIG_HPP

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>
#include <string_view>

namespace appender {

// Partition spec of the logs table (on the event timestamp)
enum class PartitionSpec { Day, Hour, None };

struct AppenderConfig {
    // Name the Iceberg catalog is attached under inside DuckDB
    static constexpr std::string_view kCatalogAlias = "lake";

    // Kafka
    std::string brokers;
    std::string topic = "otel-logs";
    std::string consumer_group = "otel-appender";
    std::string dlq_topic;  // defaults to "<topic>-dlq"
    int max_poll_interval_ms = 300'000;
    // librdkafka statistics (rdkafka_* metrics, consumer and commit lag); 0 disables them
    int kafka_stats_interval_ms = 15'000;
    // librdkafka prefetch per partition (queued.max.messages.kbytes)
    int kafka_queued_max_kbytes = 16 * 1024;

    // Iceberg REST catalog
    std::string catalog_uri;
    std::string warehouse;  // ATTACH target; defaults to s3_bucket
    std::string iceberg_namespace = "default";
    std::string table_name = "logs";
    std::string offsets_table_name = "_appender_offsets";
    PartitionSpec partition_spec = PartitionSpec::Day;
    // "none": write with the S3 credentials below; "vended_credentials": use
    // the storage config the catalog returns per table
    std::string access_delegation_mode = "none";
    // Attribute keys stored as their own VARCHAR columns (k8s.pod.name ->
    // k8s_pod_name), so they get file statistics and bloom filters
    std::vector<std::string> promoted_attributes;

    // S3-compatible storage
    std::string s3_endpoint;  // host:port; an http:// or https:// prefix sets s3_use_ssl
    std::string s3_access_key;
    std::string s3_secret_key;
    std::string s3_bucket;  // optional; only used as the default warehouse
    std::string s3_region = "us-east-1";
    std::string s3_url_style = "path";
    bool s3_use_ssl = false;

    // Buffering: a commit starts when either threshold is reached
    std::size_t flush_bytes = std::size_t{100} * 1024 * 1024;
    std::chrono::seconds flush_interval{60};
    // Staged bytes beyond which pipelines block (backpressure)
    std::size_t max_staged_bytes = std::size_t{512} * 1024 * 1024;
    unsigned pipeline_threads = 0;  // 0 = hardware threads, capped at 16
    std::size_t pipeline_queue_bytes = std::size_t{64} * 1024 * 1024;

    // Iceberg commits
    int commit_retries = 5;
    std::chrono::milliseconds retry_base_delay{200};
    std::chrono::milliseconds retry_max_delay{10'000};
    std::chrono::seconds revoke_timeout{60};
    std::chrono::seconds offsets_maintenance_interval{3600};

    // DuckDB installs extensions under its home directory, which must be writable
    std::string duckdb_home_directory = "/tmp";
    // Where DuckDB spills when staged rows or a commit outgrow its memory
    // limit; must be writable. Empty in fromEnv: <home>/duckdb_tmp
    std::string duckdb_temp_directory;
    // DuckDB's own caps; 0 keeps its defaults (80 % of the memory it detects,
    // one thread per core it sees, often the node's rather than the pod's)
    std::size_t duckdb_memory_limit_bytes = 0;
    unsigned duckdb_threads = 0;

    std::uint16_t health_port = 8080;
    std::string appender_id;  // recorded with each offsets row; defaults to the hostname

    // lake."<namespace>"."<table>"
    std::string qualifiedLogsTable() const;
    std::string qualifiedOffsetsTable() const;

    static AppenderConfig fromEnv();
};

}  // namespace appender

#endif  // APPENDER_CONFIG_HPP
