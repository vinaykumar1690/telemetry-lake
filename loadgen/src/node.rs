//! A node generates and sends load: a worker pod, or the controller itself
//! when there are no workers. The rate loop is open-model: requests go out on
//! schedule whatever the responses, and a tick that finds the in-flight limit
//! reached is counted as behind instead of queueing.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::header::{CONTENT_ENCODING, CONTENT_TYPE};
use hyper::{Request, Uri};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::generate::{Generator, content_type};
use crate::spec::{Compression, RunSpec};
use crate::stats::{Counters, Rates, Stats, Window};

pub type HttpClient = Client<HttpConnector, Full<Bytes>>;

pub fn http_client(max_idle: usize) -> HttpClient {
    let mut connector = HttpConnector::new();
    connector.set_nodelay(true);
    connector.set_connect_timeout(Some(Duration::from_secs(5)));
    Client::builder(TokioExecutor::new())
        .pool_max_idle_per_host(max_idle)
        .pool_idle_timeout(Duration::from_secs(30))
        .build(connector)
}

pub fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_millis() as u64)
}

fn now_ns() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_nanos() as u64)
}

/// This node's share of a run
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Assignment {
    pub run_id: String,
    pub spec: RunSpec,
    pub started_ms: u64,
    pub ends_at_ms: Option<u64>,
    pub records_per_second: f64,
    pub max_in_flight: u32,
    pub seed: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeState {
    Idle,
    Running,
    Finished,
}

/// What a node reports to the controller
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NodeStats {
    pub state: NodeState,
    pub run_id: Option<String>,
    pub records_per_second: f64,
    pub max_in_flight: u32,
    pub in_flight: i64,
    /// Since this node started its share of the run
    pub totals: Counters,
    /// The change over the last `window_seconds`
    pub recent: Counters,
    pub window_seconds: f64,
}

impl NodeStats {
    pub fn rates(&self) -> Rates {
        Rates::from_window(&self.recent, self.window_seconds)
    }
}

#[derive(Clone)]
pub struct NodeConfig {
    pub ingest_url: Uri,
    pub tasks: usize,
    pub request_timeout: Duration,
}

/// Live settings the rate loops read on every tick
struct Control {
    records_per_second: AtomicU64,
    max_in_flight: AtomicU64,
    stopped: AtomicBool,
    stop: Notify,
}

impl Control {
    fn rate(&self) -> f64 {
        f64::from_bits(self.records_per_second.load(Relaxed))
    }
}

struct Active {
    assignment: Assignment,
    control: Arc<Control>,
    tasks: Vec<JoinHandle<()>>,
    baseline: Counters,
}

pub struct Node {
    config: NodeConfig,
    client: HttpClient,
    stats: Arc<Stats>,
    active: Mutex<Option<Active>>,
    window: Mutex<Window>,
}

impl Node {
    pub fn new(config: NodeConfig, max_in_flight: usize) -> Arc<Node> {
        Arc::new(Node {
            client: http_client(max_in_flight),
            config,
            stats: Arc::new(Stats::default()),
            active: Mutex::new(None),
            window: Mutex::new(Window::new(Duration::from_secs(5))),
        })
    }

    pub fn stats(&self) -> &Stats {
        &self.stats
    }

    /// Samples the counters once a second for the windowed rates
    pub fn spawn_sampler(self: &Arc<Self>) -> JoinHandle<()> {
        let node = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                let Some(node) = node.upgrade() else { return };
                let snapshot = node.stats.snapshot();
                node.window.lock().unwrap().push(Instant::now(), snapshot);
            }
        })
    }

    /// Starts the assignment, or retunes the rate and in-flight limit when it
    /// is the run already going
    pub fn start(self: &Arc<Self>, assignment: Assignment) {
        let mut active = self.active.lock().unwrap();
        if let Some(current) = active.as_mut() {
            let same_run = current.assignment.run_id == assignment.run_id && current.assignment.spec == assignment.spec;
            if same_run && !current.control.stopped.load(Relaxed) {
                current.control.records_per_second.store(assignment.records_per_second.to_bits(), Relaxed);
                current.control.max_in_flight.store(u64::from(assignment.max_in_flight), Relaxed);
                current.assignment = assignment;
                return;
            }
            Self::halt(current);
        }
        let control = Arc::new(Control {
            records_per_second: AtomicU64::new(assignment.records_per_second.to_bits()),
            max_in_flight: AtomicU64::new(u64::from(assignment.max_in_flight)),
            stopped: AtomicBool::new(false),
            stop: Notify::new(),
        });
        let spec = Arc::new(assignment.spec.clone());
        let tasks = self.config.tasks.max(1);
        let handles = (0..tasks)
            .map(|task| {
                let generator = Generator::new(
                    spec.clone(),
                    assignment.run_id.clone(),
                    assignment.started_ms,
                    assignment.seed.wrapping_mul(0x9e37_79b9).wrapping_add(task as u64),
                );
                tokio::spawn(self.clone().rate_loop(
                    generator,
                    spec.clone(),
                    control.clone(),
                    tasks,
                    assignment.ends_at_ms,
                ))
            })
            .collect();
        tracing::info!(
            run_id = %assignment.run_id,
            records_per_second = assignment.records_per_second,
            max_in_flight = assignment.max_in_flight,
            tasks,
            "run started"
        );
        self.window.lock().unwrap().clear();
        *active = Some(Active { assignment, control, tasks: handles, baseline: self.stats.snapshot() });
    }

    fn halt(active: &mut Active) {
        active.control.stopped.store(true, Relaxed);
        active.control.stop.notify_waiters();
        for task in active.tasks.drain(..) {
            task.abort();
        }
    }

    pub fn stop(&self) {
        if let Some(active) = self.active.lock().unwrap().as_mut() {
            if !active.control.stopped.load(Relaxed) {
                tracing::info!(run_id = %active.assignment.run_id, "run stopped");
            }
            Self::halt(active);
        }
    }

    pub fn report(&self) -> NodeStats {
        let (recent, window_seconds) = self.window.lock().unwrap().delta();
        let in_flight = self.stats.in_flight.load(Relaxed);
        let active = self.active.lock().unwrap();
        match active.as_ref() {
            None => NodeStats {
                state: NodeState::Idle,
                run_id: None,
                records_per_second: 0.0,
                max_in_flight: 0,
                in_flight,
                totals: Counters::default(),
                recent,
                window_seconds,
            },
            Some(active) => {
                let finished = active.control.stopped.load(Relaxed) || active.tasks.iter().all(|t| t.is_finished());
                NodeStats {
                    state: if finished { NodeState::Finished } else { NodeState::Running },
                    run_id: Some(active.assignment.run_id.clone()),
                    records_per_second: active.control.rate(),
                    max_in_flight: active.control.max_in_flight.load(Relaxed) as u32,
                    in_flight,
                    totals: self.stats.snapshot().since(&active.baseline),
                    recent,
                    window_seconds,
                }
            }
        }
    }

    async fn rate_loop(
        self: Arc<Self>,
        mut generator: Generator,
        spec: Arc<RunSpec>,
        control: Arc<Control>,
        tasks: usize,
        ends_at_ms: Option<u64>,
    ) {
        let records_per_request = f64::from(spec.records_per_request);
        let content_type = content_type(spec.wire);
        let gzip = spec.compression == Compression::Gzip;
        let mut next = tokio::time::Instant::now();
        loop {
            if control.stopped.load(Relaxed) || ends_at_ms.is_some_and(|end| now_ms() >= end) {
                return;
            }
            // Requests per second for this task
            let rate = control.rate() / records_per_request / tasks as f64;
            if rate <= 0.0 {
                tokio::time::sleep(Duration::from_millis(100)).await;
                next = tokio::time::Instant::now();
                continue;
            }
            let interval = Duration::from_secs_f64(1.0 / rate);
            let now = tokio::time::Instant::now();
            if next > now {
                tokio::select! {
                    () = tokio::time::sleep_until(next) => {}
                    () = control.stop.notified() => return,
                }
            } else {
                let lag = now - next;
                if lag > Duration::from_secs(1) {
                    // Generating or sending cannot keep up; drop the backlog
                    // instead of bursting it
                    self.stats.behind((lag.as_secs_f64() * rate) as u64);
                    next = now;
                }
                // Let the send tasks run on this thread too
                tokio::task::yield_now().await;
            }
            next += interval;

            if self.stats.in_flight.load(Relaxed) >= control.max_in_flight.load(Relaxed) as i64 {
                self.stats.behind(1);
                continue;
            }
            let payload = generator.next(now_ns());
            self.stats.sent(payload.records, payload.body.len(), payload.raw_bytes);
            self.stats.in_flight.fetch_add(1, Relaxed);
            let mut request = Request::post(self.config.ingest_url.clone()).header(CONTENT_TYPE, content_type);
            if gzip {
                request = request.header(CONTENT_ENCODING, "gzip");
            }
            let request = request.body(Full::new(payload.body)).expect("a valid request");
            let node = self.clone();
            let records = payload.records;
            tokio::spawn(async move {
                let started = Instant::now();
                let result = tokio::time::timeout(node.config.request_timeout, async {
                    let response = node.client.request(request).await?;
                    let status = response.status().as_u16();
                    // Read the body so the connection goes back to the pool
                    response.into_body().collect().await?;
                    Ok::<_, Box<dyn std::error::Error + Send + Sync>>(status)
                })
                .await;
                match result {
                    Ok(Ok(status)) => node.stats.response(status, records, started.elapsed()),
                    _ => node.stats.network_error(records, started.elapsed()),
                }
                node.stats.in_flight.fetch_sub(1, Relaxed);
            });
        }
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.stop();
    }
}
