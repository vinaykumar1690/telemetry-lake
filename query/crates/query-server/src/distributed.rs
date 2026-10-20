//! Distributed execution with datafusion-distributed.
//!
//! The coordinator (the HTTP service) plans every query as before; for scans
//! of the logs table large enough to be worth it, the distributed planner
//! splits the [`LogsScanExec`] across workers, which stream Arrow batches
//! back over gRPC. Search plans become scan-on-workers, network coalesce,
//! TopK on the coordinator, so ordering and cursors are unchanged.
//!
//! - [`scan_task_count`]: how many worker tasks a scan gets. Below
//!   `QUERY_DISTRIBUTE_MIN_BYTES` exactly one, which keeps the query in this
//!   process with no network stage.
//! - [`scale_up_scan`]: splits the scan's files into disjoint per-task scans.
//! - [`WorkerResolver`] implementations: a fixed list, or a headless
//!   Service's DNS name resolved every few seconds.
//! - [`LogsWorkerSession`]: what every worker's sessions need (the scan codec
//!   and the memory-limited runtime).

use std::sync::{Arc, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::SessionState;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::SessionConfig;
use datafusion_distributed::{
    DesiredTaskCountEvent, DesiredTaskCountEventResponse, DistributedExt, DistributedLeafExec, ScaleUpLeafNodeEvent,
    ScaleUpLeafNodeEventResponse, SessionStateBuilderExt, WorkerQueryContext, WorkerResolver, WorkerSessionBuilder,
};
use iceberg::io::FileIO;
use logscan::{LogsScanCodec, LogsScanExec};
use url::Url;

use crate::config::Config;

/// Task-count policy for [`LogsScanExec`] leaves
#[derive(Debug, Clone, Copy)]
pub struct ScanPolicy {
    pub min_bytes: u64,
    pub bytes_per_task: u64,
}

impl ScanPolicy {
    pub fn from_config(config: &Config) -> Self {
        Self { min_bytes: config.distribute_min_bytes, bytes_per_task: config.bytes_per_task }
    }

    /// 1 means "run here"; the planner also caps the result at the number of workers
    pub fn tasks_for(&self, bytes: u64) -> usize {
        if bytes < self.min_bytes || bytes == 0 { 1 } else { bytes.div_ceil(self.bytes_per_task).max(1) as usize }
    }
}

pub fn scan_task_count(
    policy: ScanPolicy,
) -> impl for<'a> Fn(DesiredTaskCountEvent<'a>) -> Option<Result<DesiredTaskCountEventResponse>> + Send + Sync + 'static
{
    move |ev| {
        let scan = ev.plan.downcast_ref::<LogsScanExec>()?;
        Some(Ok(match policy.tasks_for(scan.total_bytes()) {
            1 => DesiredTaskCountEventResponse::maximum(1),
            n => DesiredTaskCountEventResponse::desired(n),
        }))
    }
}

pub fn scale_up_scan(ev: ScaleUpLeafNodeEvent) -> Option<Result<ScaleUpLeafNodeEventResponse>> {
    let scan = ev.plan.downcast_ref::<LogsScanExec>()?;
    if ev.task_count <= 1 {
        return None;
    }
    let variants = scan.split(ev.task_count).into_iter().map(|v| Arc::new(v) as _);
    Some(
        DistributedLeafExec::try_new(Arc::clone(ev.plan), variants)
            .map(|leaf| ScaleUpLeafNodeEventResponse::new(Arc::new(leaf))),
    )
}

/// Adds distributed planning to a coordinator session
pub fn coordinator_state(
    config: SessionConfig,
    runtime: Arc<RuntimeEnv>,
    resolver: Arc<dyn WorkerResolver>,
    file_io: FileIO,
    policy: ScanPolicy,
) -> SessionState {
    SessionStateBuilder::new()
        .with_default_features()
        .with_config(config)
        .with_runtime_env(runtime)
        .with_distributed_planner()
        .with_distributed_worker_resolver(SharedResolver(resolver))
        .with_distributed_user_codec(LogsScanCodec::new(file_io))
        .with_distributed_desired_task_count_handler(scan_task_count(policy))
        .with_distributed_scale_up_leaf_node_handler(scale_up_scan)
        .build()
}

/// Session setup for every query a worker executes
#[derive(Clone)]
pub struct LogsWorkerSession {
    pub file_io: FileIO,
    pub runtime: Arc<RuntimeEnv>,
}

#[async_trait]
impl WorkerSessionBuilder for LogsWorkerSession {
    async fn build_session_state(&self, ctx: WorkerQueryContext) -> Result<SessionState, DataFusionError> {
        Ok(ctx
            .builder
            .with_runtime_env(Arc::clone(&self.runtime))
            .with_distributed_user_codec(LogsScanCodec::new(self.file_io.clone()))
            .build())
    }
}

struct SharedResolver(Arc<dyn WorkerResolver>);

impl WorkerResolver for SharedResolver {
    fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
        self.0.get_urls()
    }
}

/// Workers from `QUERY_WORKERS`: `http://a:50051,http://b:50051`, or
/// `dns:///query-workers.ns.svc:50051` (every address of the name, refreshed
/// every 5 s in the background; `get_urls` only reads a snapshot)
pub struct Workers {
    urls: Arc<RwLock<Vec<Url>>>,
}

impl Workers {
    pub fn parse(spec: &str) -> Result<Self, String> {
        let spec = spec.trim();
        if let Some(target) = spec.strip_prefix("dns:///") {
            if !target.contains(':') {
                return Err(format!("QUERY_WORKERS {spec}: expected dns:///host:port"));
            }
            let urls = Arc::new(RwLock::new(Vec::new()));
            let refreshed = Arc::clone(&urls);
            let target = target.to_string();
            tokio::spawn(async move {
                loop {
                    match tokio::net::lookup_host(target.as_str()).await {
                        Ok(addresses) => {
                            let mut found: Vec<Url> =
                                addresses.filter_map(|a| Url::parse(&format!("http://{a}")).ok()).collect();
                            found.sort();
                            *refreshed.write().expect("resolver lock") = found;
                        }
                        Err(error) => tracing::warn!(%error, %target, "resolving workers failed"),
                    }
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            });
            return Ok(Self { urls });
        }
        let urls = spec
            .split(',')
            .filter(|s| !s.trim().is_empty())
            .map(|s| Url::parse(s.trim()).map_err(|e| format!("QUERY_WORKERS: '{s}': {e}")))
            .collect::<Result<Vec<_>, _>>()?;
        if urls.is_empty() {
            return Err("QUERY_WORKERS is empty".to_string());
        }
        Ok(Self { urls: Arc::new(RwLock::new(urls)) })
    }

    pub fn count(&self) -> usize {
        self.urls.read().expect("resolver lock").len()
    }
}

impl WorkerResolver for Workers {
    fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
        Ok(self.urls.read().expect("resolver lock").clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn policy() {
        let p = ScanPolicy { min_bytes: 2 << 30, bytes_per_task: 1 << 30 };
        assert_eq!(p.tasks_for(0), 1);
        assert_eq!(p.tasks_for(1 << 30), 1, "below the threshold stays local");
        assert_eq!(p.tasks_for(2 << 30), 2);
        assert_eq!(p.tasks_for((5 << 30) + 1), 6);
        let always = ScanPolicy { min_bytes: 0, bytes_per_task: 1 << 20 };
        assert_eq!(always.tasks_for(10), 1);
        assert_eq!(always.tasks_for(3 << 20), 3);
    }

    #[test]
    fn static_worker_lists() {
        assert_eq!(Workers::parse("http://a:1, http://b:2").unwrap().count(), 2);
        assert!(Workers::parse(" , ").is_err());
        assert!(Workers::parse("not a url").is_err());
        assert!(Workers::parse("dns:///no-port").is_err());
    }
}
