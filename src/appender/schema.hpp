#ifndef APPENDER_SCHEMA_HPP
#define APPENDER_SCHEMA_HPP

#include <array>
#include <string_view>

namespace appender::schema {

struct Column {
    std::string_view name;
    std::string_view type;  // DuckDB type as DESCRIBE reports it
};

// The logs table. _kafka_topic and _ingested_at are the same for every row of
// a commit, so they are not staged; the commit adds them (see kStagedColumns).
inline constexpr std::array kLogsColumns{
    Column{"_kafka_topic", "VARCHAR"},
    Column{"_kafka_partition", "INTEGER"},
    Column{"_kafka_offset", "BIGINT"},
    Column{"_ingested_at", "TIMESTAMP WITH TIME ZONE"},
    Column{"timestamp", "TIMESTAMP WITH TIME ZONE"},
    Column{"time_unix_nano", "BIGINT"},
    Column{"observed_time_unix_nano", "BIGINT"},
    Column{"severity_number", "INTEGER"},
    Column{"severity_text", "VARCHAR"},
    Column{"body", "VARCHAR"},
    Column{"trace_id", "VARCHAR"},
    Column{"span_id", "VARCHAR"},
    Column{"flags", "BIGINT"},
    Column{"event_name", "VARCHAR"},
    Column{"service_name", "VARCHAR"},
    Column{"service_namespace", "VARCHAR"},
    Column{"service_instance_id", "VARCHAR"},
    Column{"deployment_environment", "VARCHAR"},
    Column{"host_name", "VARCHAR"},
    Column{"scope_name", "VARCHAR"},
    Column{"scope_version", "VARCHAR"},
    Column{"resource_attributes", "MAP(VARCHAR, VARCHAR)"},
    Column{"log_attributes", "MAP(VARCHAR, VARCHAR)"},
    Column{"dropped_attributes_count", "BIGINT"},
};

// Columns of the per-partition staging tables, in append order
inline constexpr auto kStagedColumns = [] {
    std::array<Column, kLogsColumns.size() - 2> out{};
    std::size_t i = 0;
    for (const auto& column : kLogsColumns) {
        if (column.name != "_kafka_topic" && column.name != "_ingested_at") out[i++] = column;
    }
    return out;
}();

// Append-only record of what each commit made durable; the source of truth
// for where to resume a partition
inline constexpr std::array kOffsetsColumns{
    Column{"consumer_group", "VARCHAR"},
    Column{"topic", "VARCHAR"},
    Column{"partition", "INTEGER"},
    Column{"last_offset", "BIGINT"},
    Column{"committed_at", "TIMESTAMP WITH TIME ZONE"},
    Column{"appender_id", "VARCHAR"},
};

}  // namespace appender::schema

#endif  // APPENDER_SCHEMA_HPP
