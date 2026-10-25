//! The controller owns the run the UI starts. Without workers it drives its
//! own node; with workers (a headless Service) it resolves them once a second,
//! splits the rate evenly across the ones that answer, and sums their stats.
//! Scaling the worker Deployment mid-run rebalances within a few seconds.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::header::CONTENT_TYPE;
use hyper::{Method, Request};
use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;

use crate::generate::{Generator, Rng};
use crate::node::{Assignment, HttpClient, Node, NodeState, NodeStats, http_client, now_ms};
use crate::spec::{Limits, RunSpec};
use crate::stats::{Counters, Rates};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunState {
    Running,
    Stopped,
    Finished,
}

#[derive(Clone, Debug, Serialize)]
pub struct RunInfo {
    pub id: String,
    pub spec: RunSpec,
    pub state: RunState,
    pub started_ms: u64,
    pub ended_ms: Option<u64>,
    /// Bytes of one sample request before compression
    pub sample_request_bytes: usize,
}

/// One sample request of a spec, and what the target rate makes of it
#[derive(Clone, Debug, Serialize)]
pub struct Estimate {
    /// Before compression
    pub request_bytes: usize,
    pub request_wire_bytes: usize,
    pub requests_per_second: f64,
    pub raw_bytes_per_second: f64,
    pub wire_bytes_per_second: f64,
}

#[derive(Clone, Debug, Serialize)]
pub struct WorkerReport {
    pub address: String,
    pub up: bool,
    pub state: Option<NodeState>,
    pub records_per_second: f64,
    pub in_flight: i64,
    pub rates: Rates,
    pub totals: Counters,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Report {
    pub run: Option<RunInfo>,
    pub totals: Counters,
    pub rates: Rates,
    pub in_flight: i64,
    pub workers: Vec<WorkerReport>,
    /// `local` (the controller sends) or `workers`
    pub mode: &'static str,
}

/// Fields the UI may change while a run is going
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunPatch {
    pub records_per_second: Option<u64>,
    pub max_in_flight: Option<u32>,
}

#[derive(Debug, thiserror::Error)]
pub enum ControlError {
    #[error("{0}")]
    Invalid(String),
    #[error("a run is already going; stop it first")]
    Busy,
    #[error("no run is going")]
    NotRunning,
}

pub struct Workers {
    pub dns: String,
    pub port: u16,
}

struct WorkerView {
    stats: Option<NodeStats>,
    up: bool,
    error: Option<String>,
}

enum Mode {
    Local(Arc<Node>),
    Remote { workers: Workers, client: HttpClient, views: Mutex<BTreeMap<SocketAddr, WorkerView>> },
}

pub struct Controller {
    mode: Mode,
    limits: Limits,
    run: Mutex<Option<RunInfo>>,
}

impl Controller {
    pub fn local(node: Arc<Node>, limits: Limits) -> Arc<Controller> {
        Arc::new(Controller { mode: Mode::Local(node), limits, run: Mutex::new(None) })
    }

    pub fn remote(workers: Workers, limits: Limits) -> Arc<Controller> {
        Arc::new(Controller {
            mode: Mode::Remote { workers, client: http_client(64), views: Mutex::new(BTreeMap::new()) },
            limits,
            run: Mutex::new(None),
        })
    }

    pub fn limits(&self) -> &Limits {
        &self.limits
    }

    /// Validates the spec, including the size of a sample request against the
    /// ingester's limit, and sizes that request
    pub fn check(&self, spec: &RunSpec) -> Result<Estimate, ControlError> {
        spec.validate(&self.limits).map_err(ControlError::Invalid)?;
        let sample = Generator::new(Arc::new(spec.clone()), "run-00000000".into(), now_ms(), spec.seed)
            .next(now_ms() * 1_000_000);
        let estimate = Estimate {
            request_bytes: sample.raw_bytes,
            request_wire_bytes: sample.body.len(),
            requests_per_second: spec.requests_per_second(),
            raw_bytes_per_second: spec.requests_per_second() * sample.raw_bytes as f64,
            wire_bytes_per_second: spec.requests_per_second() * sample.body.len() as f64,
        };
        // Records vary in size; leave headroom over the sample
        if sample.raw_bytes + sample.raw_bytes / 5 > self.limits.max_request_bytes {
            return Err(ControlError::Invalid(format!(
                "one request would be about {} bytes, over the ingester's {} byte limit; send fewer records per request",
                sample.raw_bytes, self.limits.max_request_bytes
            )));
        }
        Ok(estimate)
    }

    pub async fn start(self: &Arc<Self>, spec: RunSpec) -> Result<RunInfo, ControlError> {
        let sample_request_bytes = self.check(&spec)?.request_bytes;
        let info = {
            let mut run = self.run.lock().unwrap();
            if run.as_ref().is_some_and(|r| r.state == RunState::Running) {
                return Err(ControlError::Busy);
            }
            let mut rng = Rng::new(now_ms() ^ u64::from(std::process::id()));
            let id = format!("run-{:08x}", rng.next_u64() as u32);
            let info = RunInfo {
                id,
                spec,
                state: RunState::Running,
                started_ms: now_ms(),
                ended_ms: None,
                sample_request_bytes,
            };
            *run = Some(info.clone());
            info
        };
        tracing::info!(run_id = %info.id, records_per_second = info.spec.records_per_second, "run requested");
        self.reconcile().await;
        Ok(info)
    }

    pub async fn update(self: &Arc<Self>, patch: RunPatch) -> Result<RunInfo, ControlError> {
        let info = {
            let mut run = self.run.lock().unwrap();
            let Some(info) = run.as_mut().filter(|r| r.state == RunState::Running) else {
                return Err(ControlError::NotRunning);
            };
            let mut spec = info.spec.clone();
            if let Some(rate) = patch.records_per_second {
                spec.records_per_second = rate;
            }
            if let Some(in_flight) = patch.max_in_flight {
                spec.max_in_flight = in_flight;
            }
            spec.validate(&self.limits).map_err(ControlError::Invalid)?;
            info.spec = spec;
            info.clone()
        };
        self.reconcile().await;
        Ok(info)
    }

    pub async fn stop(self: &Arc<Self>) -> Result<RunInfo, ControlError> {
        let info = {
            let mut run = self.run.lock().unwrap();
            let Some(info) = run.as_mut().filter(|r| r.state == RunState::Running) else {
                return Err(ControlError::NotRunning);
            };
            info.state = RunState::Stopped;
            info.ended_ms = Some(now_ms());
            info.clone()
        };
        self.reconcile().await;
        Ok(info)
    }

    /// Ends a run past its duration
    fn expire(&self) -> Option<RunInfo> {
        let mut run = self.run.lock().unwrap();
        let info = run.as_mut()?;
        if info.state == RunState::Running
            && let Some(seconds) = info.spec.duration_seconds
            && now_ms() >= info.started_ms + seconds * 1000
        {
            info.state = RunState::Finished;
            info.ended_ms = Some(info.started_ms + seconds * 1000);
            tracing::info!(run_id = %info.id, "run finished");
        }
        Some(info.clone())
    }

    fn assignment(info: &RunInfo, share: usize, of: usize) -> Assignment {
        let of = of.max(1);
        Assignment {
            run_id: info.id.clone(),
            spec: info.spec.clone(),
            started_ms: info.started_ms,
            ends_at_ms: info.spec.duration_seconds.map(|s| info.started_ms + s * 1000),
            records_per_second: info.spec.records_per_second as f64 / of as f64,
            max_in_flight: info.spec.max_in_flight.div_ceil(of as u32).max(1),
            seed: info.spec.seed.wrapping_add(share as u64 + 1),
        }
    }

    /// Brings the nodes in line with the run: called on every change and once
    /// a second by `spawn_reconciler`
    pub async fn reconcile(self: &Arc<Self>) {
        let run = self.expire();
        match &self.mode {
            Mode::Local(node) => match run {
                Some(info) if info.state == RunState::Running => node.start(Self::assignment(&info, 0, 1)),
                _ => node.stop(),
            },
            Mode::Remote { workers, client, views } => {
                let addresses = match tokio::net::lookup_host((workers.dns.as_str(), workers.port)).await {
                    Ok(found) => {
                        let mut list: Vec<SocketAddr> = found.collect();
                        list.sort();
                        list.dedup();
                        list
                    }
                    Err(error) => {
                        tracing::debug!(%error, dns = %workers.dns, "no workers resolved");
                        Vec::new()
                    }
                };
                let polled = poll_all(client, &addresses).await;
                let alive: Vec<SocketAddr> = polled.iter().filter(|(_, r)| r.is_ok()).map(|(a, _)| *a).collect();
                {
                    let mut views = views.lock().unwrap();
                    for view in views.values_mut() {
                        view.up = false;
                    }
                    for (address, result) in &polled {
                        let view = views.entry(*address).or_insert(WorkerView { stats: None, up: false, error: None });
                        match result {
                            Ok(stats) => {
                                view.up = true;
                                view.error = None;
                                view.stats = Some(stats.clone());
                            }
                            Err(error) => view.error = Some(error.clone()),
                        }
                    }
                    // Forget workers gone for good unless they hold this run's totals
                    let current = run.as_ref().map(|r| r.id.clone());
                    views.retain(|_, v| {
                        v.up || v.stats.as_ref().is_some_and(|s| s.run_id.is_some() && s.run_id == current)
                    });
                }
                let mut calls = JoinSet::new();
                for (index, address) in alive.iter().enumerate() {
                    let stats = polled.iter().find(|(a, _)| a == address).and_then(|(_, r)| r.as_ref().ok()).cloned();
                    let Some(stats) = stats else { continue };
                    let client = client.clone();
                    let address = *address;
                    match &run {
                        Some(info) if info.state == RunState::Running => {
                            let wanted = Self::assignment(info, index, alive.len());
                            let in_line = stats.state == NodeState::Running
                                && stats.run_id.as_deref() == Some(info.id.as_str())
                                && stats.records_per_second == wanted.records_per_second
                                && stats.max_in_flight == wanted.max_in_flight;
                            // (the poll above already renewed the worker's lease)
                            if !in_line {
                                calls.spawn(async move {
                                    (address, call(&client, address, Method::POST, Some(&wanted)).await)
                                });
                            }
                        }
                        _ if stats.state == NodeState::Running => {
                            calls.spawn(
                                async move { (address, call::<()>(&client, address, Method::DELETE, None).await) },
                            );
                        }
                        _ => {}
                    }
                }
                while let Some(joined) = calls.join_next().await {
                    if let Ok((address, Err(error))) = joined {
                        tracing::warn!(%address, %error, "worker call failed");
                    }
                }
            }
        }
    }

    pub fn spawn_reconciler(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let controller = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                interval.tick().await;
                let Some(controller) = controller.upgrade() else { return };
                controller.reconcile().await;
            }
        })
    }

    pub fn report(&self) -> Report {
        let run = self.run.lock().unwrap().clone();
        let run_id = run.as_ref().map(|r| r.id.clone());
        let mut totals = Counters::default();
        let mut recent_rates = Rates::default();
        let mut in_flight = 0;
        let mut workers = Vec::new();
        let mut add = |address: String, up: bool, stats: Option<&NodeStats>, error: Option<String>| {
            let rates = stats.map(NodeStats::rates).unwrap_or_default();
            let this_run = stats.is_some_and(|s| s.run_id.is_some() && s.run_id == run_id);
            if this_run {
                totals.add(&stats.unwrap().totals);
            }
            if up {
                if let Some(s) = stats {
                    in_flight += s.in_flight;
                }
                sum_rates(&mut recent_rates, &rates);
            }
            workers.push(WorkerReport {
                address,
                up,
                state: stats.map(|s| s.state),
                records_per_second: stats.filter(|_| this_run).map_or(0.0, |s| s.records_per_second),
                in_flight: stats.map_or(0, |s| s.in_flight),
                rates,
                totals: stats.filter(|_| this_run).map(|s| s.totals.clone()).unwrap_or_default(),
                error,
            });
        };
        let mode = match &self.mode {
            Mode::Local(node) => {
                let stats = node.report();
                add("local".into(), true, Some(&stats), None);
                "local"
            }
            Mode::Remote { views, .. } => {
                for (address, view) in views.lock().unwrap().iter() {
                    add(address.to_string(), view.up, view.stats.as_ref(), view.error.clone());
                }
                "workers"
            }
        };
        // Percentiles do not add up; take them from the merged recent buckets
        let merged = match &self.mode {
            Mode::Local(node) => Some(node.report().rates()),
            Mode::Remote { views, .. } => {
                let mut recent = Counters::default();
                for view in views.lock().unwrap().values().filter(|v| v.up) {
                    if let Some(s) = &view.stats {
                        recent.add(&s.recent);
                    }
                }
                Some(Rates::from_window(&recent, 1.0))
            }
        };
        if let Some(merged) = merged {
            recent_rates.latency_p50_seconds = merged.latency_p50_seconds;
            recent_rates.latency_p99_seconds = merged.latency_p99_seconds;
        }
        Report { run, totals, rates: recent_rates, in_flight, workers, mode }
    }
}

fn sum_rates(sum: &mut Rates, rates: &Rates) {
    sum.records_per_second += rates.records_per_second;
    sum.accepted_records_per_second += rates.accepted_records_per_second;
    sum.requests_per_second += rates.requests_per_second;
    sum.wire_bytes_per_second += rates.wire_bytes_per_second;
    sum.raw_bytes_per_second += rates.raw_bytes_per_second;
    sum.failed_requests_per_second += rates.failed_requests_per_second;
    sum.behind_per_second += rates.behind_per_second;
}

async fn poll_all(client: &HttpClient, addresses: &[SocketAddr]) -> Vec<(SocketAddr, Result<NodeStats, String>)> {
    let mut polls = JoinSet::new();
    for address in addresses {
        let (client, address) = (client.clone(), *address);
        polls.spawn(async move { (address, call::<()>(&client, address, Method::GET, None).await) });
    }
    let mut out = Vec::new();
    while let Some(joined) = polls.join_next().await {
        if let Ok((address, result)) = joined {
            let parsed = result.and_then(|body| serde_json::from_slice::<NodeStats>(&body).map_err(|e| e.to_string()));
            out.push((address, parsed));
        }
    }
    out.sort_by_key(|(a, _)| *a);
    out
}

/// One call to a worker's /internal/run: GET (stats, renews its lease), POST
/// (assign), DELETE (stop)
async fn call<T: Serialize>(
    client: &HttpClient,
    address: SocketAddr,
    method: Method,
    body: Option<&T>,
) -> Result<Bytes, String> {
    let body = body.map(|b| serde_json::to_vec(b).expect("serializable")).unwrap_or_default();
    let request = Request::builder()
        .method(method)
        .uri(format!("http://{address}/internal/run"))
        .header(CONTENT_TYPE, "application/json")
        .body(Full::new(Bytes::from(body)))
        .map_err(|e| e.to_string())?;
    let response = tokio::time::timeout(Duration::from_millis(800), client.request(request))
        .await
        .map_err(|_| "timed out".to_string())?
        .map_err(|e| e.to_string())?;
    let status = response.status();
    let body = response.into_body().collect().await.map_err(|e| e.to_string())?.to_bytes();
    if !status.is_success() {
        return Err(format!("HTTP {status}: {}", String::from_utf8_lossy(&body)));
    }
    Ok(body)
}
