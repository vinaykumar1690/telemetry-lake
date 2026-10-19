//! The JSON search frontend: `POST /v1/search` request bodies to [`LogQuery`].
//!
//! ```json
//! {
//!   "start": "now-1h", "end": "now",
//!   "filters": {"service.name": "checkout", "attr.http.status_code": ["500", "503"]},
//!   "where": {"or": [{"field": "severity_number", "op": ">=", "value": 17},
//!                    {"field": "body", "op": "icontains", "value": "panic"}]},
//!   "grep": "timeout",
//!   "fields": ["k8s.pod.name"],
//!   "limit": 100,
//!   "cursor": "..."
//! }
//! ```
//!
//! `filters` (equality, or IN for a list), `where`, `grep` and `regex` are
//! ANDed together.

use std::collections::BTreeMap;

use serde::Deserialize;
use serde_json::Value as Json;

use crate::cursor::Cursor;
use crate::error::{QueryError, Result};
use crate::ir::{CompareOp, LogQuery, Order, Predicate, TextMatch, TimeRange, Value};
use crate::time::parse_instant;

/// The body field grep and regex apply to
pub const BODY_FIELD: &str = "body";
const MAX_FILTER_DEPTH: usize = 16;
const MAX_FILTER_NODES: usize = 256;

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub default_limit: usize,
    pub max_limit: usize,
    /// Longest allowed time range, microseconds
    pub max_range_us: i64,
}

impl Default for Limits {
    fn default() -> Self {
        Self { default_limit: 100, max_limit: 1000, max_range_us: 7 * 86_400 * 1_000_000 }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearchRequest {
    pub start: TimeInput,
    #[serde(default)]
    pub end: Option<TimeInput>,
    #[serde(default)]
    pub filters: BTreeMap<String, Json>,
    #[serde(default, rename = "where")]
    pub filter: Option<Json>,
    #[serde(default)]
    pub grep: Option<String>,
    #[serde(default)]
    pub grep_ignore_case: bool,
    #[serde(default)]
    pub regex: Option<String>,
    #[serde(default)]
    pub fields: Vec<String>,
    #[serde(default)]
    pub order: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub cursor: Option<String>,
}

/// RFC 3339, `now[-<duration>]`, or epoch milliseconds
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum TimeInput {
    Text(String),
    Millis(i64),
}

impl TimeInput {
    fn resolve(&self, now_us: i64) -> Result<i64> {
        match self {
            TimeInput::Text(text) => parse_instant(text, now_us),
            TimeInput::Millis(ms) => ms.checked_mul(1000).ok_or_else(|| QueryError::new("time is out of range")),
        }
    }
}

impl SearchRequest {
    pub fn parse(body: &[u8]) -> Result<Self> {
        serde_json::from_slice(body).map_err(|e| QueryError::new(format!("invalid request: {e}")))
    }

    /// Validates the request and lowers it to the IR
    pub fn into_query(self, now_us: i64, limits: &Limits) -> Result<LogQuery> {
        let start_us = self.start.resolve(now_us)?;
        let end_us = match &self.end {
            Some(end) => end.resolve(now_us)?,
            None => now_us,
        };
        if start_us >= end_us {
            return Err(QueryError::new("start must be before end"));
        }
        if end_us - start_us > limits.max_range_us {
            return Err(QueryError::new(format!(
                "time range exceeds the maximum of {} hours",
                limits.max_range_us / 3_600_000_000
            )));
        }

        let mut parts = Vec::new();
        for (field, value) in &self.filters {
            check_field(field)?;
            parts.push(match value {
                Json::Array(items) => {
                    Predicate::In { field: field.clone(), values: items.iter().map(scalar).collect::<Result<_>>()? }
                }
                other => Predicate::Compare { field: field.clone(), op: CompareOp::Eq, value: scalar(other)? },
            });
        }
        if let Some(tree) = &self.filter {
            let mut budget = MAX_FILTER_NODES;
            parts.push(filter_node(tree, 0, &mut budget)?);
        }
        if let Some(needle) = self.grep {
            if needle.is_empty() {
                return Err(QueryError::new("grep must not be empty"));
            }
            parts.push(Predicate::Text {
                field: BODY_FIELD.into(),
                matcher: TextMatch::Contains { needle, case_insensitive: self.grep_ignore_case },
            });
        }
        if let Some(pattern) = self.regex {
            if pattern.is_empty() {
                return Err(QueryError::new("regex must not be empty"));
            }
            parts.push(Predicate::Text {
                field: BODY_FIELD.into(),
                matcher: TextMatch::Regex { pattern, case_insensitive: false },
            });
        }

        for field in &self.fields {
            check_field(field)?;
        }
        let order = match self.order.as_deref() {
            None | Some("desc") => Order::Desc,
            Some("asc") => Order::Asc,
            Some(other) => return Err(QueryError::new(format!("order must be asc or desc, got '{other}'"))),
        };
        let limit = self.limit.unwrap_or(limits.default_limit);
        if limit == 0 || limit > limits.max_limit {
            return Err(QueryError::new(format!("limit must be between 1 and {}", limits.max_limit)));
        }

        let mut query = LogQuery {
            time: TimeRange { start_us, end_us },
            filter: Predicate::and(parts),
            fields: self.fields,
            order,
            limit,
            cursor: None,
        };
        if let Some(text) = &self.cursor {
            let cursor = Cursor::decode(text)?;
            cursor.check(&query)?;
            // Relative times ("now-1h") would move between pages; keep the first page's window
            query.time = cursor.range;
            query.cursor = Some(cursor);
        }
        Ok(query)
    }
}

fn check_field(field: &str) -> Result<()> {
    if field.trim().is_empty() {
        return Err(QueryError::new("field names must not be empty"));
    }
    Ok(())
}

fn scalar(value: &Json) -> Result<Value> {
    match value {
        Json::String(s) => Ok(Value::String(s.clone())),
        Json::Bool(b) => Ok(Value::Bool(*b)),
        Json::Number(n) => n
            .as_i64()
            .map(Value::Int)
            .or_else(|| n.as_f64().map(Value::Float))
            .ok_or_else(|| QueryError::new(format!("number {n} is out of range"))),
        other => Err(QueryError::new(format!("expected a string, number or boolean, got {other}"))),
    }
}

fn filter_node(node: &Json, depth: usize, budget: &mut usize) -> Result<Predicate> {
    if depth > MAX_FILTER_DEPTH {
        return Err(QueryError::new(format!("filter is nested deeper than {MAX_FILTER_DEPTH} levels")));
    }
    if *budget == 0 {
        return Err(QueryError::new(format!("filter has more than {MAX_FILTER_NODES} nodes")));
    }
    *budget -= 1;
    let Json::Object(object) = node else {
        return Err(QueryError::new(format!("filter nodes must be objects, got {node}")));
    };

    for combinator in ["and", "or", "not"] {
        if let Some(inner) = object.get(combinator) {
            if object.len() != 1 {
                return Err(QueryError::new(format!("'{combinator}' must be the only key of its object")));
            }
            return match (combinator, inner) {
                ("not", child) => Ok(Predicate::Not(Box::new(filter_node(child, depth + 1, budget)?))),
                (_, Json::Array(children)) if !children.is_empty() => {
                    let children =
                        children.iter().map(|c| filter_node(c, depth + 1, budget)).collect::<Result<Vec<_>>>()?;
                    Ok(if combinator == "and" { Predicate::And(children) } else { Predicate::Or(children) })
                }
                _ => Err(QueryError::new(format!("'{combinator}' takes a non-empty array"))),
            };
        }
    }

    let field = match object.get("field") {
        Some(Json::String(f)) => f.clone(),
        _ => return Err(QueryError::new(format!("filter needs a string 'field': {node}"))),
    };
    check_field(&field)?;
    let op = match object.get("op") {
        Some(Json::String(op)) => op.as_str(),
        None => "=",
        Some(other) => return Err(QueryError::new(format!("'op' must be a string, got {other}"))),
    };
    for key in object.keys() {
        if !matches!(key.as_str(), "field" | "op" | "value" | "values") {
            return Err(QueryError::new(format!("unknown filter key '{key}'")));
        }
    }
    let value = || -> Result<&Json> {
        object.get("value").ok_or_else(|| QueryError::new(format!("operator '{op}' needs a 'value'")))
    };
    let text = || -> Result<String> {
        match value()? {
            Json::String(s) if !s.is_empty() => Ok(s.clone()),
            other => Err(QueryError::new(format!("operator '{op}' needs a non-empty string, got {other}"))),
        }
    };
    let compare =
        |op| -> Result<Predicate> { Ok(Predicate::Compare { field: field.clone(), op, value: scalar(value()?)? }) };

    Ok(match op {
        "=" | "==" | "eq" => compare(CompareOp::Eq)?,
        "!=" | "ne" => compare(CompareOp::Ne)?,
        "<" | "lt" => compare(CompareOp::Lt)?,
        "<=" | "le" => compare(CompareOp::Le)?,
        ">" | "gt" => compare(CompareOp::Gt)?,
        ">=" | "ge" => compare(CompareOp::Ge)?,
        "in" | "not_in" => {
            let values = match object.get("values") {
                Some(Json::Array(items)) if !items.is_empty() => items.iter().map(scalar).collect::<Result<_>>()?,
                _ => return Err(QueryError::new(format!("operator '{op}' needs a non-empty 'values' array"))),
            };
            let membership = Predicate::In { field: field.clone(), values };
            if op == "in" { membership } else { Predicate::Not(Box::new(membership)) }
        }
        "exists" => Predicate::Exists { field },
        "not_exists" => Predicate::Not(Box::new(Predicate::Exists { field })),
        "contains" | "icontains" => Predicate::Text {
            field: field.clone(),
            matcher: TextMatch::Contains { needle: text()?, case_insensitive: op == "icontains" },
        },
        "prefix" => Predicate::Text { field: field.clone(), matcher: TextMatch::Prefix(text()?) },
        "regex" | "iregex" => Predicate::Text {
            field: field.clone(),
            matcher: TextMatch::Regex { pattern: text()?, case_insensitive: op == "iregex" },
        },
        other => return Err(QueryError::new(format!("unknown operator '{other}'"))),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cursor::RowKey;

    const NOW: i64 = 1_792_000_000_000_000;

    fn lower(body: &str) -> Result<LogQuery> {
        SearchRequest::parse(body.as_bytes())?.into_query(NOW, &Limits::default())
    }

    #[test]
    fn minimal_request_defaults() {
        let q = lower(r#"{"start": "now-1h"}"#).unwrap();
        assert_eq!(q.time, TimeRange { start_us: NOW - 3_600_000_000, end_us: NOW });
        assert_eq!(q.filter, None);
        assert_eq!(q.order, Order::Desc);
        assert_eq!(q.limit, 100);
    }

    #[test]
    fn shorthand_filters_where_and_grep_are_anded() {
        let q = lower(
            r#"{"start": 1000, "end": 2000,
                "filters": {"service.name": "checkout", "attr.code": ["500", 503]},
                "where": {"or": [{"field": "severity_number", "op": ">=", "value": 17},
                                 {"not": {"field": "host.name", "op": "exists"}}]},
                "grep": "time out", "grep_ignore_case": true}"#,
        )
        .unwrap();
        assert_eq!(q.time, TimeRange { start_us: 1_000_000, end_us: 2_000_000 });
        let Some(Predicate::And(parts)) = q.filter else { panic!("expected And") };
        assert_eq!(parts.len(), 4);
        // BTreeMap order: attr.code before service.name
        assert_eq!(
            parts[0],
            Predicate::In { field: "attr.code".into(), values: vec![Value::String("500".into()), Value::Int(503)] }
        );
        assert_eq!(
            parts[1],
            Predicate::Compare {
                field: "service.name".into(),
                op: CompareOp::Eq,
                value: Value::String("checkout".into())
            }
        );
        assert!(matches!(&parts[2], Predicate::Or(children) if children.len() == 2));
        assert_eq!(
            parts[3],
            Predicate::Text {
                field: "body".into(),
                matcher: TextMatch::Contains { needle: "time out".into(), case_insensitive: true }
            }
        );
    }

    #[test]
    fn every_operator() {
        for (op, extra) in [
            ("=", r#""value": "a""#),
            ("!=", r#""value": 1"#),
            ("<", r#""value": 1.5"#),
            ("<=", r#""value": 1"#),
            (">", r#""value": 1"#),
            (">=", r#""value": true"#),
            ("in", r#""values": ["a"]"#),
            ("not_in", r#""values": [1, 2]"#),
            ("exists", ""),
            ("not_exists", ""),
            ("contains", r#""value": "x""#),
            ("icontains", r#""value": "x""#),
            ("prefix", r#""value": "x""#),
            ("regex", r#""value": "a.*b""#),
            ("iregex", r#""value": "a""#),
        ] {
            let sep = if extra.is_empty() { "" } else { ", " };
            let body = format!(r#"{{"start": "now-1h", "where": {{"field": "f", "op": "{op}"{sep}{extra}}}}}"#);
            assert!(lower(&body).is_ok(), "{op}: {:?}", lower(&body));
        }
    }

    #[test]
    fn validation_errors() {
        for (body, message) in [
            (r#"{"start": "now", "end": "now-1h"}"#, "start must be before end"),
            (r#"{"start": "now-30d"}"#, "exceeds the maximum"),
            (r#"{"start": "now-1h", "limit": 0}"#, "limit must be between"),
            (r#"{"start": "now-1h", "limit": 5000}"#, "limit must be between"),
            (r#"{"start": "now-1h", "grep": ""}"#, "grep must not be empty"),
            (r#"{"start": "now-1h", "order": "up"}"#, "order must be"),
            (r#"{"start": "now-1h", "bogus": 1}"#, "unknown field"),
            (r#"{"start": "now-1h", "filters": {"a": {"b": 1}}}"#, "expected a string"),
            (r#"{"start": "now-1h", "where": {"field": "a", "op": "~"}}"#, "unknown operator"),
            (r#"{"start": "now-1h", "where": {"field": "a", "op": "in", "values": []}}"#, "non-empty 'values'"),
            (r#"{"start": "now-1h", "where": {"and": []}}"#, "non-empty array"),
            (r#"{"start": "now-1h", "where": {"and": [{"field": "a", "value": 1}], "or": []}}"#, "only key"),
            (r#"{"start": "now-1h", "where": {"field": "a", "valu": 1}}"#, "unknown filter key"),
            (r#"{"start": "now-1h", "where": {"field": " ", "value": 1}}"#, "must not be empty"),
            (r#"{"start": "now-1h", "cursor": "zzz"}"#, "invalid cursor"),
        ] {
            let err = lower(body).expect_err(body);
            assert!(err.0.contains(message), "{body}: {err}");
        }
    }

    #[test]
    fn deep_filters_are_rejected() {
        let mut node = r#"{"field": "a", "value": 1}"#.to_string();
        for _ in 0..20 {
            node = format!(r#"{{"not": {node}}}"#);
        }
        let err = lower(&format!(r#"{{"start": "now-1h", "where": {node}}}"#)).unwrap_err();
        assert!(err.0.contains("nested deeper"), "{err}");
    }

    #[test]
    fn cursor_must_match_the_query() {
        let body = r#"{"start": 1000, "end": 2000, "grep": "x"}"#;
        let q = lower(body).unwrap();
        let key = RowKey { ts_us: 1_500_000, topic: "t".into(), partition: 0, offset: 1, record_index: 0 };
        let cursor = Cursor::new(&q, Some(7), key).encode();
        let same = lower(&format!(r#"{{"start": 1000, "end": 2000, "grep": "x", "limit": 5, "cursor": "{cursor}"}}"#));
        assert_eq!(same.unwrap().cursor.unwrap().snapshot_id, Some(7));
        let other = lower(&format!(r#"{{"start": 1000, "end": 2000, "grep": "y", "cursor": "{cursor}"}}"#));
        assert!(other.unwrap_err().0.contains("different query"));
    }

    #[test]
    fn cursor_pins_a_relative_time_range() {
        let first = lower(r#"{"start": "now-1h", "grep": "x"}"#).unwrap();
        let key = RowKey { ts_us: NOW - 1, topic: "t".into(), partition: 0, offset: 1, record_index: 0 };
        let cursor = Cursor::new(&first, None, key).encode();
        // The next page is requested a minute later: "now" has moved
        let later =
            SearchRequest::parse(format!(r#"{{"start": "now-1h", "grep": "x", "cursor": "{cursor}"}}"#).as_bytes())
                .unwrap()
                .into_query(NOW + 60_000_000, &Limits::default())
                .unwrap();
        assert_eq!(later.time, first.time);
    }
}
