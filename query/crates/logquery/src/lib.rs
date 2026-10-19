//! The log query IR and its frontends.
//!
//! Every frontend (the JSON search API today, LogQL later) lowers into
//! [`LogQuery`]. Field names stay unresolved strings here; mapping them to
//! columns or map keys is the planner's job, so all frontends share one field
//! namespace. Nothing in this crate depends on the query engine.

pub mod cursor;
pub mod error;
pub mod ir;
pub mod json;
pub mod time;

pub use cursor::{Cursor, RowKey};
pub use error::QueryError;
pub use ir::{CompareOp, LogQuery, Order, Predicate, TextMatch, TimeRange, Value};
