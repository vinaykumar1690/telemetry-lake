use datafusion::error::DataFusionError;
use logquery::QueryError;

#[derive(Debug, thiserror::Error)]
pub enum PlanError {
    /// The query is invalid for this table (HTTP 400)
    #[error(transparent)]
    Query(#[from] QueryError),
    /// The table does not have the shape the planner needs (HTTP 503: deploy/config problem)
    #[error("logs table schema: {0}")]
    Schema(String),
    /// Planning or execution failed (HTTP 500)
    #[error(transparent)]
    Engine(#[from] DataFusionError),
}

impl PlanError {
    pub(crate) fn query(message: impl Into<String>) -> Self {
        PlanError::Query(QueryError::new(message))
    }
}

pub type Result<T> = std::result::Result<T, PlanError>;
