//! Record batches to response bytes

use arrow_json::writer::{JsonArray, LineDelimited, WriterBuilder};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::error::ArrowError;

// Timestamps as RFC 3339 in UTC, the same text the JSON API accepts
const TIMESTAMP_TZ_FORMAT: &str = "%Y-%m-%dT%H:%M:%S%.6fZ";

fn builder() -> WriterBuilder {
    WriterBuilder::new().with_explicit_nulls(false).with_timestamp_tz_format(TIMESTAMP_TZ_FORMAT.to_string())
}

/// `[{...}, ...]`; NULL fields are omitted, maps become objects
pub fn json_array(batches: &[RecordBatch]) -> Result<Vec<u8>, ArrowError> {
    let mut writer = builder().build::<_, JsonArray>(Vec::new());
    for batch in batches {
        writer.write(batch)?;
    }
    writer.finish()?;
    let bytes = writer.into_inner();
    // An empty writer produces no bytes at all
    Ok(if bytes.is_empty() { b"[]".to_vec() } else { bytes })
}

/// One JSON object per line
pub fn ndjson(batch: &RecordBatch) -> Result<Vec<u8>, ArrowError> {
    let mut writer = builder().build::<_, LineDelimited>(Vec::new());
    writer.write(batch)?;
    writer.finish()?;
    Ok(writer.into_inner())
}
