//! Metrics endpoint.

use anystore_application::Metrics as AppMetrics;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};

use crate::HttpState;

/// Re-exported so the server binary can construct shared counters.
pub type Metrics = AppMetrics;

pub async fn render(State(state): State<HttpState>) -> Response {
    let body = state.app.metrics.render();
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        body,
    )
        .into_response()
}
