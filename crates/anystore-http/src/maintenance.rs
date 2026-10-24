//! Maintenance trigger.
//!
//! Not part of the public API contract. It exists so an external scheduler can
//! run maintenance on a deployment that scales to zero, where no in-process
//! timer is alive at the scheduled time. It is authenticated like every other
//! non-health route.

use axum::Extension;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Response;
use chrono::Utc;
use serde_json::json;
use std::sync::Arc;
use std::time::Instant;

use crate::HttpState;
use crate::request::{RequestMeta, json_response};
use crate::respond;

pub async fn run(
    State(state): State<HttpState>,
    Extension(meta): Extension<RequestMeta>,
) -> Response {
    let request_id = meta.request_id.clone();
    respond(Arc::clone(&state.app.metrics), &request_id, async move {
        let deadline = Instant::now() + state.maintenance_budget;
        let Some(report) = state.maintenance.run(Utc::now(), deadline).await? else {
            return Ok(json_response(
                StatusCode::ACCEPTED,
                &json!({"status": "already_running"}),
                &meta.request_id,
            ));
        };
        tracing::info!(request_id = %meta.request_id, ?report, "triggered maintenance pass complete");
        Ok(json_response(
            StatusCode::OK,
            &json!({
                "status": "completed",
                "drained": report.drained,
                "uploads_expired": report.uploads_expired,
                "blobs_deleted": report.blobs_deleted,
                "blobs_failed": report.blobs_failed,
                "idempotency_purged": report.idempotency_purged,
                "cursors_purged": report.cursors_purged,
                "changes_purged": report.changes_purged,
            }),
            &meta.request_id,
        ))
    })
    .await
}
