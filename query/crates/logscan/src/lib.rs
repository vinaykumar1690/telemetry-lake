//! A partitioned Iceberg scan for the logs table.
//!
//! iceberg-datafusion's scan reads every planned file in one stream (one
//! DataFusion partition), so a query uses one core for decoding and there is
//! nothing to hand to other machines. Here the provider plans the
//! `FileScanTask`s itself (partition and file pruning from manifest
//! statistics), bin-packs them into partitions by size, and [`LogsScanExec`]
//! reads each partition with iceberg-rust's Arrow reader, which matches
//! columns by Iceberg field id. The tasks are self-contained (file path,
//! byte range, projected field ids, bound predicate, delete files), so
//! [`LogsScanCodec`] can ship a scan to a worker that only needs a `FileIO`.
//!
//! Planned files can be dropped before grouping by a [`FileFilter`]; the
//! file-skipping index (design phase 4) plugs in there.

pub mod codec;
pub mod exec;
#[cfg(any(test, feature = "test-fixtures"))]
pub mod fixtures;
pub mod provider;

pub use codec::LogsScanCodec;
pub use exec::LogsScanExec;
pub use provider::{FileFilter, LogsTableProvider};
