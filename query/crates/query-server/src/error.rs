//! Errors as HTTP responses: `{"error": {"code": "...", "message": "..."}}`

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use datafusion::error::DataFusionError;
use logquery::QueryError;
use planner::PlanError;
use serde_json::json;

use crate::source::SourceError;

#[derive(Debug)]
pub struct ApiError {
    pub status: StatusCode,
    pub code: &'static str,
    pub message: String,
}

impl ApiError {
    pub fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self { status, code, message: message.into() }
    }

    pub fn timeout(limit: std::time::Duration) -> Self {
        Self::new(StatusCode::GATEWAY_TIMEOUT, "timeout", format!("query exceeded {}s", limit.as_secs()))
    }

    pub fn overloaded() -> Self {
        Self::new(StatusCode::TOO_MANY_REQUESTS, "overloaded", "too many concurrent queries; retry shortly")
    }

    /// Engine errors from user SQL: planning problems are the caller's
    pub fn from_sql(error: DataFusionError) -> Self {
        match unwrap(&error) {
            DataFusionError::SQL(..)
            | DataFusionError::Plan(_)
            | DataFusionError::SchemaError(..)
            | DataFusionError::NotImplemented(_) => {
                Self::new(StatusCode::BAD_REQUEST, "invalid_sql", error.to_string())
            }
            _ => Self::from_engine(error),
        }
    }

    fn from_engine(error: DataFusionError) -> Self {
        match unwrap(&error) {
            DataFusionError::ResourcesExhausted(_) => {
                Self::new(StatusCode::SERVICE_UNAVAILABLE, "resources_exhausted", error.to_string())
            }
            _ => {
                tracing::error!(%error, "query failed");
                Self::new(StatusCode::INTERNAL_SERVER_ERROR, "internal", error.to_string())
            }
        }
    }
}

fn unwrap(error: &DataFusionError) -> &DataFusionError {
    match error {
        DataFusionError::Context(_, inner) => unwrap(inner),
        DataFusionError::Shared(inner) => unwrap(inner),
        DataFusionError::Diagnostic(_, inner) => unwrap(inner),
        DataFusionError::Collection(errors) if !errors.is_empty() => unwrap(&errors[0]),
        other => other,
    }
}

impl From<QueryError> for ApiError {
    fn from(error: QueryError) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "invalid_query", error.0)
    }
}

impl From<PlanError> for ApiError {
    fn from(error: PlanError) -> Self {
        match error {
            PlanError::Query(q) => q.into(),
            PlanError::Schema(m) => Self::new(StatusCode::SERVICE_UNAVAILABLE, "table_schema", m),
            PlanError::Engine(e) => Self::from_engine(e),
        }
    }
}

impl From<SourceError> for ApiError {
    fn from(error: SourceError) -> Self {
        let message = error.to_string();
        match error {
            SourceError::Unavailable(_) => Self::new(StatusCode::SERVICE_UNAVAILABLE, "catalog_unavailable", message),
            SourceError::Schema(_) => Self::new(StatusCode::SERVICE_UNAVAILABLE, "table_schema", message),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = Json(json!({"error": {"code": self.code, "message": self.message}}));
        (self.status, body).into_response()
    }
}
