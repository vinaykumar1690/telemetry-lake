//! Turns a [`logquery::LogQuery`] into a DataFusion plan over the logs table.
//!
//! - [`FieldResolver`] maps the shared field namespace (`service.name`,
//!   `attr.http.method`, `resource.k8s.pod.name`, ...) to columns or map
//!   lookups. Every frontend goes through it.
//! - [`filters`] lowers IR predicates to DataFusion expressions shaped so
//!   DataFusion and iceberg-rust can push them into the scan.
//! - [`search`] builds and runs the newest-first, cursor-paginated search.

pub mod error;
pub mod fields;
pub mod filters;
#[cfg(any(test, feature = "test-fixtures"))]
pub mod fixtures;
pub mod schema;
pub mod search;

pub use error::PlanError;
pub use fields::{FieldResolver, ResolvedField};
pub use search::{SearchOutput, SearchPlanner};
