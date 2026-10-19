/// A query the caller must fix; maps to HTTP 400
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
#[error("{0}")]
pub struct QueryError(pub String);

impl QueryError {
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

pub type Result<T> = std::result::Result<T, QueryError>;
