//! An in-memory logs table with the Arrow schema iceberg-rust produces for
//! the appender's table. Used by this crate's and the server's tests.

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, Int32Array, Int64Array, MapBuilder, MapFieldNames, RecordBatch, StringArray, StringBuilder,
    TimestampMicrosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::datasource::MemTable;

pub const BASE_US: i64 = 1_792_281_600_000_000; // 2026-10-18T00:00:00Z
pub const ROWS: usize = 30;

fn map_builder() -> MapBuilder<StringBuilder, StringBuilder> {
    let names = MapFieldNames { entry: "key_value".into(), key: "key".into(), value: "value".into() };
    MapBuilder::new(Some(names), StringBuilder::new(), StringBuilder::new())
}

/// The Arrow schema iceberg-rust produces for the appender's logs table
pub fn logs_schema() -> Schema {
    let map = map_builder().finish().data_type().clone();
    let ts = DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into()));
    let utf8 = |name: &str| Field::new(name, DataType::Utf8, true);
    Schema::new(vec![
        utf8("_kafka_topic"),
        Field::new("_kafka_partition", DataType::Int32, true),
        Field::new("_kafka_offset", DataType::Int64, true),
        Field::new("_record_index", DataType::Int32, true),
        Field::new("_ingested_at", ts.clone(), true),
        Field::new("timestamp", ts, true),
        Field::new("severity_number", DataType::Int32, true),
        utf8("severity_text"),
        utf8("body"),
        utf8("trace_id"),
        utf8("span_id"),
        utf8("service_name"),
        utf8("host_name"),
        utf8("deployment_environment"),
        Field::new("resource_attributes", map.clone(), true),
        Field::new("log_attributes", map, true),
    ])
}

/// Row i: four rows per second (timestamp ties), one Kafka message per
/// six rows across two partitions, services cycling checkout/cart/auth,
/// every fifth body a timeout, a "user" attribute on every fourth row.
pub fn logs_table() -> Arc<MemTable> {
    let schema = Arc::new(logs_schema());
    let services = ["checkout", "cart", "auth"];
    let batch = |range: std::ops::Range<usize>| {
        let mut resource = map_builder();
        let mut attrs = map_builder();
        for i in range.clone() {
            resource.keys().append_value("service.name");
            resource.values().append_value(services[i % 3]);
            resource.keys().append_value("k8s.pod.name");
            resource.values().append_value(format!("pod-{}", i % 2));
            resource.append(true).unwrap();
            attrs.keys().append_value("seq");
            attrs.values().append_value(i.to_string());
            attrs.keys().append_value("http.status_code");
            attrs.values().append_value(["200", "500", "404"][i % 3]);
            if i % 4 == 0 {
                attrs.keys().append_value("user");
                attrs.values().append_value(format!("u{i}"));
            }
            attrs.append(true).unwrap();
        }
        let ts: Vec<i64> = range.clone().map(|i| BASE_US + (i as i64 / 4) * 1_000_000).collect();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from_iter_values(range.clone().map(|_| "otel-logs"))),
            Arc::new(Int32Array::from_iter_values(range.clone().map(|i| (i % 2) as i32))),
            Arc::new(Int64Array::from_iter_values(range.clone().map(|i| (i / 6) as i64))),
            Arc::new(Int32Array::from_iter_values(range.clone().map(|i| ((i / 2) % 3) as i32))),
            Arc::new(TimestampMicrosecondArray::from(ts.clone()).with_timezone("+00:00")),
            Arc::new(TimestampMicrosecondArray::from(ts).with_timezone("+00:00")),
            Arc::new(Int32Array::from_iter_values(range.clone().map(|i| [9, 13, 17, 21][i % 4]))),
            Arc::new(StringArray::from_iter_values(range.clone().map(|i| ["INFO", "WARN", "ERROR", "FATAL"][i % 4]))),
            Arc::new(StringArray::from_iter_values(
                range.clone().map(|i| format!("request {i} {}", if i % 5 == 0 { "Timeout after 30s" } else { "ok" })),
            )),
            Arc::new(StringArray::from_iter_values(range.clone().map(|i| format!("{i:032x}")))),
            Arc::new(StringArray::from_iter_values(range.clone().map(|i| format!("{i:016x}")))),
            Arc::new(StringArray::from_iter_values(range.clone().map(|i| services[i % 3]))),
            Arc::new(StringArray::from_iter_values(range.clone().map(|_| "host-a"))),
            Arc::new(StringArray::from_iter_values(range.clone().map(|_| "prod"))),
            Arc::new(resource.finish()),
            Arc::new(attrs.finish()),
        ];
        RecordBatch::try_new(schema.clone(), columns).unwrap()
    };
    // Two partitions so the plan really merges
    Arc::new(MemTable::try_new(schema.clone(), vec![vec![batch(0..13)], vec![batch(13..ROWS)]]).unwrap())
}
