//! Error mapping.
//!
//! Every response body follows the contract's error envelope.

use anystore_application::dto::error_json;
use anystore_domain::RequestId;
use anystore_domain::error::DomainError;
use axum::body::Body;
use axum::http::{StatusCode, header};
use axum::response::Response;

pub fn error_response(error: &DomainError, request_id: &RequestId) -> Response {
    let status = StatusCode::from_u16(error.status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let body = serde_json::to_vec(&error_json(error, request_id.as_str()))
        .unwrap_or_else(|_| b"{\"error\":\"internal_error\"}".to_vec());

    // Internal detail is logged but never returned.
    match error {
        DomainError::Internal(source) => {
            tracing::error!(request_id = %request_id, "internal error: {source}");
        }
        DomainError::StorageError(source) => {
            tracing::error!(request_id = %request_id, "storage error: {source}");
        }
        _ => {}
    }

    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .header("x-request-id", request_id.as_str())
        .body(Body::from(body))
        .expect("error response is always well formed")
}
