//! The log query service: the JSON search API and read-only SQL over the
//! Iceberg logs table, executed in-process by DataFusion.

pub mod api;
pub mod config;
pub mod error;
pub mod metrics;
pub mod output;
pub mod source;
pub mod sql;

use std::sync::Arc;

use datafusion::error::Result;
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};

/// One runtime for all queries, so the memory limit is process-wide
pub fn runtime(memory_limit_bytes: usize) -> Result<Arc<RuntimeEnv>> {
    RuntimeEnvBuilder::new().with_memory_pool(Arc::new(FairSpillPool::new(memory_limit_bytes))).build_arc()
}
