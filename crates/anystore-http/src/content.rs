//! Content handlers.
//!
//! `GET` redirects to a short-lived signed URL so file bytes never pass through
//! the application.

use anystore_application::content::ContentService;
use axum::Extension;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{StatusCode, header};
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

        if let Some(content_type) = head.content_type {
            builder = builder.header(header::CONTENT_TYPE, content_type);
        }
        if let Some(sha256) = head.sha256 {
            builder = builder.header("x-anystore-sha256", sha256);
        }

        Ok(builder
            .body(Body::empty())
            .expect("head response is always well formed"))
    })
    .await
}
