//! HTTP adapter.
//!
//! Handlers call application services only. There is no SQL and no provider
//! protocol code in this crate.

mod changes;
mod content;
mod error;
mod maintenance;
mod metrics;
mod middleware;
mod objects;
mod query;
mod request;
mod uploads;

pub use error::error_response;
pub use metrics::Metrics;
pub use middleware::AuthConfig;

use crate::error::status_error_response;
use crate::request::RequestMeta;
use anystore_application::{AppState, MaintenanceService, Metrics as AppMetrics};
use anystore_domain::RequestId;
use anystore_domain::error::{DomainError, DomainResult};
use axum::Extension;
use axum::Router;
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::{delete, get, post};
use std::future::Future;
use std::sync::Arc;
use tower_http::catch_panic::CatchPanicLayer;

/// Shared state for HTTP handlers.
#[derive(Clone)]
pub struct HttpState {
    pub app: Arc<AppState>,
    pub auth: Arc<AuthConfig>,
    pub metrics: Arc<Metrics>,
    /// Shared with the in-process scheduler so passes never overlap.
    pub maintenance: MaintenanceService,
    /// Time budget of a triggered pass; keep it below the platform's request
    /// timeout.
    pub maintenance_budget: std::time::Duration,
}

/// Maximum JSON request body. Metadata is untrusted input, so it is bounded.
pub const MAX_BODY_BYTES: usize = 1024 * 1024;

async fn not_found(Extension(meta): Extension<RequestMeta>) -> Response {
    status_error_response(
        StatusCode::NOT_FOUND,
        "invalid_request",
        "Route not found.",
        &meta.request_id,
    )
}

async fn method_not_allowed(Extension(meta): Extension<RequestMeta>) -> Response {
    status_error_response(
        StatusCode::METHOD_NOT_ALLOWED,
        "invalid_request",
        "Method not allowed.",
        &meta.request_id,
    )
}

/// Runs a handler body and converts a domain error into the contract's error
/// envelope, recording the matching metric on the way out.
pub(crate) async fn respond<F>(metrics: Arc<AppMetrics>, request_id: &RequestId, fut: F) -> Response
where
    F: Future<Output = DomainResult<Response>>,
{
    match fut.await {
        Ok(response) => response,
        Err(error) => {
            record_error(&metrics, &error);
            error_response(&error, request_id)
        }
    }
}

fn record_error(metrics: &AppMetrics, error: &DomainError) {
    match error {
        DomainError::RevisionConflict { .. } => AppMetrics::incr(&metrics.revision_conflicts_total),
        DomainError::StorageError(_) => AppMetrics::incr(&metrics.blob_provider_errors_total),
        DomainError::Internal(_) => AppMetrics::incr(&metrics.db_errors_total),
        _ => {}
    }
}

/// Keeps the contract's error envelope even when a handler panics. The request
/// id is not reachable here; the pipeline still sets `x-request-id`.
fn panic_response(_panic: Box<dyn std::any::Any + Send + 'static>) -> Response {
    tracing::error!("handler panicked");
    let body = serde_json::to_vec(&serde_json::json!({
        "error": "internal_error",
        "message": "Internal error.",
    }))
    .unwrap_or_default();
    let mut response = Response::new(axum::body::Body::from(body));
    *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static("application/json"),
    );
    response
}

pub fn router(state: HttpState) -> Router {
    let api = Router::new()
        .route("/objects", post(objects::create))
        .route(
            "/objects/{id}",
            get(objects::get)
                .patch(objects::patch)
                .delete(objects::delete),
        )
        .route("/objects/{id}/children", get(objects::children))
        .route(
            "/objects/{id}/content",
            get(content::get_content).head(content::head_content),
        )
        .route("/resolve", get(objects::resolve))
        .route("/query", post(query::query))
        .route("/uploads", post(uploads::create))
        .route("/uploads/{id}", delete(uploads::abort))
        .route("/uploads/{id}/parts", post(uploads::parts))
        .route("/uploads/{id}/complete", post(uploads::complete))
        .route("/changes", get(changes::read));

    Router::new()
        .nest("/api/v1", api)
        .route("/internal/maintenance", post(maintenance::run))
        .route("/healthz", get(middleware::healthz))
        .route("/metrics", get(metrics::render))
        .fallback(not_found)
        .method_not_allowed_fallback(method_not_allowed)
        .layer(CatchPanicLayer::custom(panic_response))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            middleware::pipeline,
        ))
        .with_state(state)
}
