//! API error type.
//!
//! Database errors are logged server-side and reported as a generic 500.
//! Leaking an SQL message to an HTTP client tells an attacker the schema and
//! tells an operator nothing useful.

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

#[derive(Debug)]
pub enum ApiError {
    Db(sqlx::Error),
    BadRequest(String),
    /// Well-formed request, but the row already exists.
    Conflict(String),
    NotFound(String),
}

impl From<sqlx::Error> for ApiError {
    fn from(e: sqlx::Error) -> Self {
        Self::Db(e)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            Self::Db(e) => {
                tracing::error!("api: database error: {e}");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal error".to_string(),
                )
            }
            Self::BadRequest(m) => (StatusCode::BAD_REQUEST, m),
            Self::Conflict(m) => (StatusCode::CONFLICT, m),
            Self::NotFound(m) => (StatusCode::NOT_FOUND, m),
        };
        (status, Json(serde_json::json!({ "error": message }))).into_response()
    }
}
