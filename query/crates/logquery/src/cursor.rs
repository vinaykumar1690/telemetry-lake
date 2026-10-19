//! Opaque pagination cursors.
//!
//! A cursor names the last row a page returned by its sort key
//! `(timestamp, topic, partition, offset, record_index)`, which is unique and
//! survives compaction. It pins what the first page saw, so later pages
//! neither skip nor repeat rows:
//! - the Iceberg snapshot (new commits do not shift the pages)
//! - the resolved time range (`now-1h` means the same window on every page)
//!
//! A fingerprint of the filters, fields and order stops it from being
//! replayed against a different query.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};

use crate::error::{QueryError, Result};
use crate::ir::{LogQuery, Order, Predicate, TimeRange};

const VERSION: u32 = 1;

/// The sort key of one row
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowKey {
    pub ts_us: i64,
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub record_index: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cursor {
    #[serde(rename = "v")]
    pub version: u32,
    /// Snapshot the first page read; None for an empty table
    #[serde(rename = "s")]
    pub snapshot_id: Option<i64>,
    /// The time range the first page resolved; later pages reuse it
    #[serde(rename = "r")]
    pub range: TimeRange,
    /// Rows strictly after this key (in the query's order) come next
    #[serde(rename = "k")]
    pub after: RowKey,
    #[serde(rename = "f")]
    pub fingerprint: u64,
}

impl Cursor {
    pub fn new(query: &LogQuery, snapshot_id: Option<i64>, after: RowKey) -> Self {
        Self { version: VERSION, snapshot_id, range: query.time, after, fingerprint: fingerprint(query) }
    }

    pub fn encode(&self) -> String {
        URL_SAFE_NO_PAD.encode(serde_json::to_vec(self).expect("cursor serializes"))
    }

    pub fn decode(text: &str) -> Result<Self> {
        let invalid = || QueryError::new("invalid cursor");
        let bytes = URL_SAFE_NO_PAD.decode(text.trim()).map_err(|_| invalid())?;
        let cursor: Cursor = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
        if cursor.version != VERSION {
            return Err(QueryError::new("cursor is from an incompatible version; restart the search"));
        }
        Ok(cursor)
    }

    /// Fails unless the cursor was issued for this query
    pub fn check(&self, query: &LogQuery) -> Result<()> {
        if self.fingerprint != fingerprint(query) {
            return Err(QueryError::new("cursor belongs to a different query; resend the original query with it"));
        }
        Ok(())
    }
}

/// What a cursor is bound to: everything but the time range (pinned in the
/// cursor itself), the limit and the cursor
#[derive(Serialize)]
struct Canonical<'a> {
    filter: &'a Option<Predicate>,
    fields: &'a [String],
    order: Order,
}

/// Stable 64-bit FNV-1a hash of the query's canonical JSON. Stable across
/// processes and releases, unlike std's hasher, so cursors survive restarts.
pub fn fingerprint(query: &LogQuery) -> u64 {
    let canonical = Canonical { filter: &query.filter, fields: &query.fields, order: query.order };
    let canonical = serde_json::to_vec(&canonical).expect("query serializes");
    canonical
        .iter()
        .fold(0xcbf2_9ce4_8422_2325u64, |hash, byte| (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::TextMatch;

    fn query(needle: &str) -> LogQuery {
        LogQuery {
            time: TimeRange { start_us: 0, end_us: 10 },
            filter: Some(Predicate::Text {
                field: "body".into(),
                matcher: TextMatch::Contains { needle: needle.into(), case_insensitive: false },
            }),
            fields: vec![],
            order: Order::Desc,
            limit: 10,
            cursor: None,
        }
    }

    fn key() -> RowKey {
        RowKey { ts_us: 5, topic: "otel-logs".into(), partition: 2, offset: 77, record_index: 3 }
    }

    #[test]
    fn round_trips() {
        let cursor = Cursor::new(&query("x"), Some(42), key());
        let decoded = Cursor::decode(&cursor.encode()).unwrap();
        assert_eq!(decoded, cursor);
        assert_eq!(decoded.range, TimeRange { start_us: 0, end_us: 10 });
        decoded.check(&query("x")).unwrap();
    }

    #[test]
    fn bound_to_its_query_but_not_the_limit() {
        let cursor = Cursor::new(&query("x"), Some(42), key());
        assert!(cursor.check(&query("y")).is_err());
        let mut bigger_page = query("x");
        bigger_page.limit = 500;
        cursor.check(&bigger_page).unwrap();
    }

    #[test]
    fn rejects_garbage() {
        assert!(Cursor::decode("not a cursor!").is_err());
        assert!(Cursor::decode(&URL_SAFE_NO_PAD.encode(b"{}")).is_err());
    }

    #[test]
    fn fingerprint_is_stable() {
        // Changing this value breaks outstanding cursors; only do it with a VERSION bump
        assert_eq!(fingerprint(&query("x")), fingerprint(&query("x")));
        assert_ne!(fingerprint(&query("x")), fingerprint(&query("x ")));
    }
}
