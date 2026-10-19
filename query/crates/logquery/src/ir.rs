use serde::{Deserialize, Serialize};

use crate::cursor::Cursor;

/// A log search: rows of one table in a time range, filtered, ordered by
/// time and paginated by cursor.
#[derive(Debug, Clone, PartialEq)]
pub struct LogQuery {
    pub time: TimeRange,
    pub filter: Option<Predicate>,
    /// Extra fields to return besides the default set; unresolved names
    pub fields: Vec<String>,
    pub order: Order,
    pub limit: usize,
    pub cursor: Option<Cursor>,
}

/// Half-open event-time range `[start_us, end_us)`, microseconds since the epoch
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimeRange {
    pub start_us: i64,
    pub end_us: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum Order {
    /// Newest first (the default for log search)
    Desc,
    Asc,
}

/// Boolean filter over fields. A field that is absent on a row (a missing map
/// key, a NULL column) never satisfies a positive comparison; `Ne` and `Not`
/// treat it as "not equal", so `Not(p)` always selects exactly the rows `p`
/// does not.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum Predicate {
    And(Vec<Predicate>),
    Or(Vec<Predicate>),
    Not(Box<Predicate>),
    Compare { field: String, op: CompareOp, value: Value },
    In { field: String, values: Vec<Value> },
    Exists { field: String },
    Text { field: String, matcher: TextMatch },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum CompareOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

/// String matching on a field (grep when the field is the body)
#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum TextMatch {
    Contains { needle: String, case_insensitive: bool },
    Prefix(String),
    Regex { pattern: String, case_insensitive: bool },
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum Value {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
}

impl Value {
    /// The value as map attributes store it (all attribute values are strings)
    pub fn to_attribute_string(&self) -> String {
        match self {
            Value::String(s) => s.clone(),
            Value::Int(i) => i.to_string(),
            Value::Float(f) => f.to_string(),
            Value::Bool(b) => b.to_string(),
        }
    }
}

impl Predicate {
    /// Conjunction that flattens nested Ands and drops empty ones
    pub fn and(parts: impl IntoIterator<Item = Predicate>) -> Option<Predicate> {
        let mut flat = Vec::new();
        for part in parts {
            match part {
                Predicate::And(inner) => flat.extend(inner),
                other => flat.push(other),
            }
        }
        match flat.len() {
            0 => None,
            1 => flat.pop(),
            _ => Some(Predicate::And(flat)),
        }
    }
}
