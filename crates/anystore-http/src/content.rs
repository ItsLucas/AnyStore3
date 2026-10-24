//! Content handlers.
//!
//! `GET` redirects to a short-lived signed URL so file bytes never pass through
//! the application.

use anystore_application::content::ContentService;
use anystore_domain::error::DomainError;
use axum::Extension;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::Response;
use std::sync::Arc;

use crate::HttpState;
use crate::request::{RequestMeta, object_id};
use crate::respond;

pub async fn get_content(
    State(state): State<HttpState>,
    Extension(meta): Extension<RequestMeta>,
    Path(id): Path<String>,
) -> Response {
    let request_id = meta.request_id.clone();
    respond(Arc::clone(&state.app.metrics), &request_id, async move {
        let service = ContentService::new(Arc::clone(&state.app));
        let location = service.location(&object_id(&id)?).await?;

        Ok(Response::builder()
            .status(StatusCode::TEMPORARY_REDIRECT)
            .header(header::LOCATION, location.url)
            .header(header::CACHE_CONTROL, "no-store")
            .header("x-request-id", meta.request_id.as_str())
            .body(Body::empty())
            .expect("redirect response is always well formed"))
    })
    .await
}

pub async fn head_content(
    State(state): State<HttpState>,
    Extension(meta): Extension<RequestMeta>,
    Path(id): Path<String>,
) -> Response {
    let request_id = meta.request_id.clone();
    respond(Arc::clone(&state.app.metrics), &request_id, async move {
        let service = ContentService::new(Arc::clone(&state.app));
        let head = service.head(&object_id(&id)?).await?;

        let mut builder = Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_LENGTH, head.size.to_string())
            .header("x-request-id", meta.request_id.as_str());

        // Stored values are validated on write, but rows written before that
        // validation existed must not be able to turn a HEAD into a panic.
        for (name, value) in [
            (header::CONTENT_TYPE.as_str(), head.content_type),
            ("x-anystore-sha256", head.sha256),
        ] {
            let Some(value) = value else { continue };
            match HeaderValue::from_str(&value) {
                Ok(value) => builder = builder.header(name, value),
                Err(_) => tracing::warn!(
                    request_id = %meta.request_id,
                    "omitting stored {name} that is not a valid header value"
                ),
            }
        }

        builder
            .body(Body::empty())
            .map_err(|e| DomainError::internal(format!("head response: {e}")))
    })
    .await
}
