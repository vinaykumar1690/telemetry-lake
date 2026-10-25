//! HTTP API: /v1/run for the UI (controller), /internal/run for the
//! controller (workers), and /health, /ready, /metrics on both

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Duration;

use axum::extract::State;
use axum::http::StatusCode;
use axum::http::header::CONTENT_TYPE;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde_json::json;

use crate::controller::{ControlError, Controller, RunPatch};
use crate::metrics;
use crate::node::{Assignment, Node, now_ms};
use crate::spec::RunSpec;

impl IntoResponse for ControlError {
    fn into_response(self) -> Response {
        let status = match self {
            ControlError::Invalid(_) => StatusCode::BAD_REQUEST,
            ControlError::Busy => StatusCode::CONFLICT,
            ControlError::NotRunning => StatusCode::NOT_FOUND,
        };
        (status, Json(json!({ "error": self.to_string() }))).into_response()
    }
}

fn base() -> Router {
    Router::new().route("/health", get(|| async { "ok" })).route("/ready", get(|| async { "ok" }))
}

/// The controller: the UI's API, plus /metrics for its own node when it
/// sends the load itself
pub fn controller_router(controller: Arc<Controller>, node: Option<Arc<Node>>) -> Router {
    let api = Router::new()
        .route("/v1/run", get(get_run).post(start_run).patch(update_run).delete(stop_run))
        .route("/v1/limits", get(get_limits))
        .route("/v1/estimate", axum::routing::post(estimate))
        .with_state(controller.clone());
    let metrics = Router::new()
        .route("/metrics", get(render_metrics))
        .with_state(MetricsState { node, controller: Some(controller) });
    base().merge(api).merge(metrics)
}

async fn get_run(State(controller): State<Arc<Controller>>) -> impl IntoResponse {
    Json(controller.report())
}

async fn get_limits(State(controller): State<Arc<Controller>>) -> impl IntoResponse {
    let limits = controller.limits();
    Json(json!({
        "max_records_per_second": limits.max_records_per_second,
        "max_in_flight": limits.max_in_flight,
        "max_request_bytes": limits.max_request_bytes,
    }))
}

async fn start_run(
    State(controller): State<Arc<Controller>>,
    body: Result<Json<RunSpec>, axum::extract::rejection::JsonRejection>,
) -> Response {
    let Json(spec) = match body {
        Ok(spec) => spec,
        Err(rejection) => return ControlError::Invalid(rejection.body_text()).into_response(),
    };
    match controller.start(spec).await {
        Ok(run) => (StatusCode::CREATED, Json(run)).into_response(),
        Err(error) => error.into_response(),
    }
}

async fn estimate(
    State(controller): State<Arc<Controller>>,
    body: Result<Json<RunSpec>, axum::extract::rejection::JsonRejection>,
) -> Response {
    let Json(spec) = match body {
        Ok(spec) => spec,
        Err(rejection) => return ControlError::Invalid(rejection.body_text()).into_response(),
    };
    match controller.check(&spec) {
        Ok(estimate) => Json(estimate).into_response(),
        Err(error) => error.into_response(),
    }
}

async fn update_run(
    State(controller): State<Arc<Controller>>,
    body: Result<Json<RunPatch>, axum::extract::rejection::JsonRejection>,
) -> Response {
    let Json(patch) = match body {
        Ok(patch) => patch,
        Err(rejection) => return ControlError::Invalid(rejection.body_text()).into_response(),
    };
    match controller.update(patch).await {
        Ok(run) => Json(run).into_response(),
        Err(error) => error.into_response(),
    }
}

async fn stop_run(State(controller): State<Arc<Controller>>) -> Response {
    match controller.stop().await {
        Ok(run) => Json(run).into_response(),
        Err(error) => error.into_response(),
    }
}

#[derive(Clone)]
struct MetricsState {
    node: Option<Arc<Node>>,
    controller: Option<Arc<Controller>>,
}

async fn render_metrics(State(state): State<MetricsState>) -> Response {
    let body = metrics::render(state.node.as_deref(), state.controller.as_deref());
    ([(CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")], body).into_response()
}

/// A worker stops its run when the controller has not been heard from for
/// this long, so a lost controller cannot leave load running
pub const LEASE: Duration = Duration::from_secs(15);

#[derive(Clone)]
struct WorkerState {
    node: Arc<Node>,
    last_contact_ms: Arc<AtomicU64>,
}

pub fn worker_router(node: Arc<Node>) -> Router {
    let last_contact_ms = Arc::new(AtomicU64::new(now_ms()));
    spawn_lease_watch(node.clone(), last_contact_ms.clone());
    let internal = Router::new()
        .route("/internal/run", get(worker_stats).post(worker_start).delete(worker_stop))
        .with_state(WorkerState { node: node.clone(), last_contact_ms });
    let metrics = Router::new()
        .route("/metrics", get(render_metrics))
        .with_state(MetricsState { node: Some(node), controller: None });
    base().merge(internal).merge(metrics)
}

fn spawn_lease_watch(node: Arc<Node>, last_contact_ms: Arc<AtomicU64>) {
    let node = Arc::downgrade(&node);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            let Some(node) = node.upgrade() else { return };
            if now_ms().saturating_sub(last_contact_ms.load(Relaxed)) > LEASE.as_millis() as u64
                && node.report().state == crate::node::NodeState::Running
            {
                tracing::warn!("no word from the controller; stopping the run");
                node.stop();
            }
        }
    });
}

async fn worker_stats(State(state): State<WorkerState>) -> impl IntoResponse {
    state.last_contact_ms.store(now_ms(), Relaxed);
    Json(state.node.report())
}

async fn worker_start(State(state): State<WorkerState>, Json(assignment): Json<Assignment>) -> impl IntoResponse {
    state.last_contact_ms.store(now_ms(), Relaxed);
    state.node.start(assignment);
    StatusCode::NO_CONTENT
}

async fn worker_stop(State(state): State<WorkerState>) -> impl IntoResponse {
    state.last_contact_ms.store(now_ms(), Relaxed);
    state.node.stop();
    StatusCode::NO_CONTENT
}
