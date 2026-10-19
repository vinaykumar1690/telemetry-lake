//! `POST /v1/sql`: read-only SQL over the `logs` table for power users.
//!
//! Body `{"sql": "SELECT ...", "max_rows": 1000}`. Results stream as NDJSON,
//! or as an Arrow IPC stream with `Accept: application/vnd.apache.arrow.stream`.
//! Only queries are allowed (no DDL, DML or SET), and there is no access to
//! files or object stores other than the logs table.
//!
//! SQL goes straight to DataFusion rather than through the IR: the filter
//! shapes the scan and future indexes act on are the same either way.

use std::time::Instant;

use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::execution::context::SQLOptions;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;
use serde::Deserialize;

use crate::api::{AppState, InFlight};
use crate::error::ApiError;
use crate::metrics::Kind;
use crate::output;

pub const TABLE_NAME: &str = "logs";
const ARROW_STREAM: &str = "application/vnd.apache.arrow.stream";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SqlRequest {
    sql: String,
    #[serde(default)]
    max_rows: Option<usize>,
}

#[derive(Clone, Copy, PartialEq)]
enum Format {
    Ndjson,
    Arrow,
}

pub async fn handle(State(state): State<AppState>, headers: HeaderMap, body: Bytes) -> Response {
    let started = Instant::now();
    match start(&state, &headers, &body).await {
        Ok(response) => {
            // Rows are counted as they stream; the request itself succeeded
            state.metrics.record(Kind::Sql, 200, 0, started.elapsed());
            response
        }
        Err(error) => {
            state.metrics.record(Kind::Sql, error.status.as_u16(), 0, started.elapsed());
            error.into_response()
        }
    }
}

async fn start(state: &AppState, headers: &HeaderMap, body: &[u8]) -> Result<Response, ApiError> {
    let request: SqlRequest = serde_json::from_slice(body)
        .map_err(|e| ApiError::new(StatusCode::BAD_REQUEST, "invalid_request", format!("invalid request: {e}")))?;
    let max_rows = request.max_rows.unwrap_or(state.config.sql_max_rows).min(state.config.sql_max_rows);
    let format = match headers.get(header::ACCEPT).and_then(|v| v.to_str().ok()) {
        Some(accept) if accept.contains(ARROW_STREAM) => Format::Arrow,
        _ => Format::Ndjson,
    };
    let slot = state.admit().await?;

    let deadline = tokio::time::Instant::now() + state.config.timeout;
    let plan = async {
        let handle = state.source.table(None).await?;
        let ctx = state.session();
        ctx.register_table(TABLE_NAME, handle.provider).map_err(ApiError::from_sql)?;
        let options = SQLOptions::new().with_allow_ddl(false).with_allow_dml(false).with_allow_statements(false);
        let frame = ctx.sql_with_options(&request.sql, options).await.map_err(ApiError::from_sql)?;
        frame.execute_stream().await.map_err(ApiError::from_sql)
    };
    let stream =
        tokio::time::timeout_at(deadline, plan).await.map_err(|_| ApiError::timeout(state.config.timeout))??;

    let content_type = match format {
        Format::Ndjson => "application/x-ndjson",
        Format::Arrow => ARROW_STREAM,
    };
    let body = Body::from_stream(encode(stream, format, max_rows, deadline, state.config.timeout, slot));
    let mut response = Response::new(body);
    response.headers_mut().insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    Ok(response)
}

/// Streams batches until `max_rows`; the query slot is released when the
/// stream ends or the client goes away. Errors after the first byte cannot
/// change the status: NDJSON ends with an `{"error": ...}` line, Arrow ends
/// the stream early.
fn encode(
    mut stream: SendableRecordBatchStream,
    format: Format,
    max_rows: usize,
    deadline: tokio::time::Instant,
    timeout: std::time::Duration,
    slot: InFlight,
) -> impl futures::Stream<Item = Result<Bytes, std::io::Error>> {
    async_stream(move |tx| async move {
        let _slot = slot;
        let schema = stream.schema();
        let mut arrow = match format {
            Format::Arrow => match StreamWriter::try_new(Vec::new(), &schema) {
                Ok(w) => Some(w),
                Err(e) => return tx.error(e.to_string()).await,
            },
            Format::Ndjson => None,
        };
        let mut sent = 0usize;
        loop {
            let polled = tokio::select! {
                // Client gone: stop scanning now rather than at the next send
                _ = tx.0.closed() => return,
                polled = tokio::time::timeout_at(deadline, stream.next()) => polled,
            };
            let next = match polled {
                Err(_) => return tx.fail(format, ApiError::timeout(timeout).message).await,
                Ok(None) => break,
                Ok(Some(Err(e))) => return tx.fail(format, e.to_string()).await,
                Ok(Some(Ok(batch))) => batch,
            };
            let room = max_rows - sent;
            let batch: RecordBatch = if next.num_rows() > room { next.slice(0, room) } else { next };
            sent += batch.num_rows();
            let bytes = match &mut arrow {
                Some(writer) => writer.write(&batch).map(|_| std::mem::take(writer.get_mut())),
                None => output::ndjson(&batch),
            };
            match bytes {
                Ok(bytes) if !bytes.is_empty() => {
                    if !tx.send(bytes).await {
                        return; // client disconnected: dropping the stream cancels the query
                    }
                }
                Ok(_) => {}
                Err(e) => return tx.fail(format, e.to_string()).await,
            }
            if sent >= max_rows {
                break;
            }
        }
        if let Some(mut writer) = arrow
            && writer.finish().is_ok()
        {
            let _ = tx.send(writer.into_inner().unwrap_or_default()).await;
        }
    })
}

/// Minimal channel-backed stream so the encoder can be written as a loop
struct Sender(tokio::sync::mpsc::Sender<Result<Bytes, std::io::Error>>);

impl Sender {
    async fn send(&self, bytes: Vec<u8>) -> bool {
        self.0.send(Ok(Bytes::from(bytes))).await.is_ok()
    }

    async fn error(&self, message: String) {
        let _ = self.0.send(Err(std::io::Error::other(message))).await;
    }

    async fn fail(&self, format: Format, message: String) {
        tracing::warn!(%message, "sql stream failed");
        match format {
            Format::Ndjson => {
                let line = serde_json::json!({"error": {"code": "query_failed", "message": message}});
                let _ = self.send(format!("{line}\n").into_bytes()).await;
            }
            Format::Arrow => self.error(message).await,
        }
    }
}

fn async_stream<F, Fut>(body: F) -> impl futures::Stream<Item = Result<Bytes, std::io::Error>>
where
    F: FnOnce(Sender) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    tokio::spawn(body(Sender(tx)));
    futures::stream::unfold(rx, |mut rx| async move { rx.recv().await.map(|item| (item, rx)) })
}
