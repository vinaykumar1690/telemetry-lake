//! HTTP API: `POST /v1/search`, `POST /v1/sql`, `/health`, `/ready`, `/metrics`

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::{SessionConfig, SessionContext};
use iceberg::io::FileIO;
use logquery::json::SearchRequest;
use serde_json::json;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::config::Config;
use crate::distributed::{ScanPolicy, Workers, coordinator_state};
use crate::error::ApiError;
use crate::metrics::{Kind, Metrics};
use crate::output;
use crate::source::TableSource;
use crate::sql;

const MAX_BODY_BYTES: usize = 1 << 20;
/// How long a request waits for a free query slot before getting 429
const QUEUE_WAIT: Duration = Duration::from_secs(2);

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub source: Arc<dyn TableSource>,
    pub runtime: Arc<RuntimeEnv>,
    pub metrics: Arc<Metrics>,
    pub permits: Arc<Semaphore>,
    pub distributed: Option<Distributed>,
}

/// Distributed execution settings (QUERY_WORKERS set)
#[derive(Clone)]
pub struct Distributed {
    pub workers: Arc<Workers>,
    /// For the scan codec (encoding only, on the coordinator)
    pub file_io: FileIO,
    pub policy: ScanPolicy,
}

impl AppState {
    pub fn new(config: Config, source: Arc<dyn TableSource>, runtime: Arc<RuntimeEnv>) -> Self {
        let permits = Arc::new(Semaphore::new(config.max_concurrent));
        Self { config: Arc::new(config), source, runtime, metrics: Arc::default(), permits, distributed: None }
    }

    pub fn with_distributed(mut self, distributed: Distributed) -> Self {
        self.distributed = Some(distributed);
        self
    }

    /// A fresh session per query; the runtime (memory pool) is shared. With
    /// workers, large scans are distributed (small ones still run here).
    pub fn session(&self) -> SessionContext {
        let config =
            SessionConfig::new().with_target_partitions(self.config.target_partitions).with_information_schema(false);
        match &self.distributed {
            Some(d) => SessionContext::from(coordinator_state(
                config,
                self.runtime.clone(),
                d.workers.clone(),
                d.file_io.clone(),
                d.policy,
            )),
            None => SessionContext::new_with_config_rt(config, self.runtime.clone()),
        }
    }

    /// Waits briefly for a query slot; the permit is held for the whole query
    pub async fn admit(&self) -> Result<InFlight, ApiError> {
        match tokio::time::timeout(QUEUE_WAIT, self.permits.clone().acquire_owned()).await {
            Ok(Ok(permit)) => {
                self.metrics.in_flight.fetch_add(1, Ordering::Relaxed);
                Ok(InFlight { _permit: permit, metrics: self.metrics.clone() })
            }
            _ => {
                self.metrics.rejected.fetch_add(1, Ordering::Relaxed);
                Err(ApiError::overloaded())
            }
        }
    }
}

pub struct InFlight {
    _permit: OwnedSemaphorePermit,
    metrics: Arc<Metrics>,
}

impl Drop for InFlight {
    fn drop(&mut self) {
        self.metrics.in_flight.fetch_sub(1, Ordering::Relaxed);
    }
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/v1/search", post(search))
        .route("/v1/sql", post(sql::handle))
        .route("/health", get(|| async { "ok" }))
        .route("/ready", get(ready))
        .route("/metrics", get(metrics))
        .layer(axum::extract::DefaultBodyLimit::max(MAX_BODY_BYTES))
        .with_state(state)
}

async fn ready(State(state): State<AppState>) -> Response {
    if state.source.is_ready() {
        (StatusCode::OK, "ready").into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "logs table not loaded yet").into_response()
    }
}

async fn metrics(State(state): State<AppState>) -> Response {
    let mut body = state.metrics.render(state.source.as_ref());
    if let Some(d) = &state.distributed {
        body.push_str(&format!("query_workers_available {}\n", d.workers.count()));
    }
    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], body).into_response()
}

async fn search(State(state): State<AppState>, body: Bytes) -> Response {
    let started = Instant::now();
    let result = run_search(&state, &body, started).await;
    let (status, rows) = match &result {
        Ok((_, rows)) => (200, *rows),
        Err(e) => (e.status.as_u16(), 0),
    };
    state.metrics.record(Kind::Search, status, rows, started.elapsed());
    match result {
        Ok((response, _)) => response,
        Err(error) => error.into_response(),
    }
}

async fn run_search(state: &AppState, body: &[u8], started: Instant) -> Result<(Response, usize), ApiError> {
    let now_us = chrono::Utc::now().timestamp_micros();
    let query = SearchRequest::parse(body)?.into_query(now_us, &state.config.limits)?;
    let _slot = state.admit().await?;

    let work = async {
        let handle = state.source.table(query.cursor.as_ref().and_then(|c| c.snapshot_id)).await?;
        let ctx = state.session();
        let page = handle.planner.run(&ctx, handle.provider.clone(), &query, handle.snapshot_id).await?;
        Ok::<_, ApiError>((handle.snapshot_id, page))
    };
    // Dropping the future on timeout (or client disconnect) cancels the scan
    let (snapshot_id, page) = tokio::time::timeout(state.config.timeout, work)
        .await
        .map_err(|_| ApiError::timeout(state.config.timeout))??;

    let rows = output::json_array(&page.batches)
        .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "internal", e.to_string()))?;
    let tail = json!({
        "next_cursor": page.next_cursor.map(|c| c.encode()),
        "snapshot_id": snapshot_id,
        "stats": {"rows": page.rows, "elapsed_ms": started.elapsed().as_millis() as u64},
    });
    // {"rows": <batches as JSON>, ...tail}; the rows are not re-parsed
    let tail = serde_json::to_vec(&tail).expect("serializable");
    let mut out = Vec::with_capacity(rows.len() + tail.len() + 16);
    out.extend_from_slice(b"{\"rows\":");
    out.extend_from_slice(&rows);
    out.push(b',');
    out.extend_from_slice(&tail[1..]);

    let mut response = Response::new(Body::from(out));
    response.headers_mut().insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    Ok((response, page.rows))
}
