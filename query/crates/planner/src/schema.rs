//! Column names of the logs table the appender writes
//! (`src/appender/schema.hpp`).

pub const TIMESTAMP: &str = "timestamp";
pub const TOPIC: &str = "_kafka_topic";
pub const PARTITION: &str = "_kafka_partition";
pub const OFFSET: &str = "_kafka_offset";
pub const RECORD_INDEX: &str = "_record_index";
pub const BODY: &str = "body";
pub const RESOURCE_ATTRIBUTES: &str = "resource_attributes";
pub const LOG_ATTRIBUTES: &str = "log_attributes";

/// The row's sort and cursor key, in order; unique and stable per row
pub const ROW_KEY: [&str; 5] = [TIMESTAMP, TOPIC, PARTITION, OFFSET, RECORD_INDEX];

/// Columns returned by every search (those present in the table)
pub const DEFAULT_FIELDS: &[&str] = &[
    TIMESTAMP,
    "severity_text",
    "severity_number",
    BODY,
    "service_name",
    "host_name",
    "trace_id",
    "span_id",
    RESOURCE_ATTRIBUTES,
    LOG_ATTRIBUTES,
    TOPIC,
    PARTITION,
    OFFSET,
    RECORD_INDEX,
];

/// Columns the appender extracts from resource attributes, so
/// `resource.<key>` can use the column instead of the map
pub const RESOURCE_COLUMNS: &[&str] =
    &["service_name", "service_namespace", "service_instance_id", "deployment_environment", "host_name"];
