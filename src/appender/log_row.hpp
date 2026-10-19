#ifndef APPENDER_LOG_ROW_HPP
#define APPENDER_LOG_ROW_HPP

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace appender {

using Attributes = std::vector<std::pair<std::string, std::string>>;

// Fields shared by every record of one ResourceLogs
struct ResourceFields {
    std::string service_name;
    std::string service_namespace;
    std::string service_instance_id;
    std::string deployment_environment;
    std::string host_name;
    Attributes attributes;  // all resource attributes, including the promoted ones above
};

// Fields shared by every record of one ScopeLogs
struct ScopeFields {
    std::string name;
    std::string version;
};

// One OTLP log record flattened to the logs table's columns. The Kafka topic
// and _ingested_at are constant per commit and are added when the row is
// written.
struct LogRow {
    std::int32_t kafka_partition = 0;
    std::int64_t kafka_offset = 0;
    std::int32_t record_index = 0;  // position within the Kafka message

    // Event time in microseconds since the epoch: time_unix_nano, else
    // observed_time_unix_nano, else unset (NULL)
    std::optional<std::int64_t> timestamp_us;
    std::uint64_t time_unix_nano = 0;
    std::uint64_t observed_time_unix_nano = 0;

    std::int32_t severity_number = 0;
    std::string severity_text;  // the record's text, else derived from severity_number
    std::string body;           // strings verbatim; structured values as compact JSON
    std::string trace_id;       // lowercase hex; empty when absent
    std::string span_id;
    std::uint32_t flags = 0;
    std::string event_name;

    std::shared_ptr<const ResourceFields> resource;
    std::shared_ptr<const ScopeFields> scope;
    Attributes attributes;
    std::uint32_t dropped_attributes_count = 0;
};

}  // namespace appender

#endif  // APPENDER_LOG_ROW_HPP
