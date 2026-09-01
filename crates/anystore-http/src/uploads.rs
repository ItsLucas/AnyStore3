//! Upload handlers.

use anystore_application::uploads::{
    AllocatePartsRequest, CompleteUploadRequest, CompletedPartInput, CreateUploadRequest,
    UploadService,
};
use anystore_domain::error::{DomainError, DomainResult};
use anystore_domain::ObjectId;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::response::Response;
use axum::Extension;
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;

use crate::HttpState;
use crate::request::{RequestMeta, context, parse_json, stored_response, upload_id};
use crate::respond;

#[derive(Debug, Deserialize)]
struct CreateBody {
    object_id: String,
    size: u64,
    #[serde(default)]
    content_type: Option<String>,
    #[serde(default)]
    sha256: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PartsBody {
    part_numbers: Vec<u32>,
}

fn parse_completed_parts(value: &Value) -> DomainResult<Vec<CompletedPartInput>> {
    let Some(parts) = value.get("parts") else {
        return Ok(Vec::new());
    };
    let Value::Array(parts) = parts else {
        return Err(DomainError::InvalidRequest(
            "parts must be an array.".into(),
        ));
    };

    parts
        .iter()
        .map(|part| {
            let part_number = part
                .get("part_number")
                .and_then(Value::as_u64)
                .and_then(|n| u32::try_from(n).ok())
                .ok_or_else(|| {
                    DomainError::InvalidRequest("part_number must be a positive integer.".into())
                })?;
            let etag = part
                .get("etag")
                .and_then(Value::as_str)
                .ok_or_else(|| DomainError::InvalidRequest("etag is required.".into()))?
                .to_owned();
            Ok(CompletedPartInput { part_number, etag })
        })
        .collect()
}

pub async fn create(
    State(state): State<HttpState>,
    Extension(meta): Extension<RequestMeta>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let request_id = meta.request_id.clone();
    respond(Arc::clone(&state.app.metrics), &request_id, async move {
        let ctx = context(
            &meta,
            &state.app,
            &headers,
            "POST",
            "/uploads",
            "",
            &[],
            &body,
        )?;

        let parsed: CreateBody = serde_json::from_value(parse_json(&body)?)
            .map_err(|e| DomainError::InvalidRequest(format!("Invalid request body: {e}")))?;

        let service = UploadService::new(Arc::clone(&state.app));
        let stored = service
            .create(
                CreateUploadRequest {
                    object_id: ObjectId::new(parsed.object_id),
                    size: parsed.size,
                    content_type: parsed.content_type,
                    sha256: parsed.sha256,
                },
                &ctx,
            )
            .await?;

        Ok(stored_response(stored, &ctx.request_id))
    })
    .await
}

pub async fn parts(
    State(state): State<HttpState>,
    Extension(meta): Extension<RequestMeta>,
    Path(id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let request_id = meta.request_id.clone();
    respond(Arc::clone(&state.app.metrics), &request_id, async move {
        let ctx = context(
            &meta,
            &state.app,
            &headers,
            "POST",
            "/uploads/{id}/parts",
            &id,
            &[],
            &body,
        )?;

        let parsed: PartsBody = serde_json::from_value(parse_json(&body)?)
            .map_err(|e| DomainError::InvalidRequest(format!("Invalid request body: {e}")))?;

        let service = UploadService::new(Arc::clone(&state.app));
        let stored = service
            .allocate_parts(
                upload_id(&id)?,
                AllocatePartsRequest {
                    part_numbers: parsed.part_numbers,
                },
                &ctx,
            )
            .await?;

        Ok(stored_response(stored, &ctx.request_id))
    })
    .await
}

pub async fn complete(
    State(state): State<HttpState>,
    Extension(meta): Extension<RequestMeta>,
    Path(id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let request_id = meta.request_id.clone();
    respond(Arc::clone(&state.app.metrics), &request_id, async move {
        let ctx = context(
            &meta,
            &state.app,
            &headers,
            "POST",
            "/uploads/{id}/complete",
            &id,
            &[],
            &body,
        )?;

        let parsed = parse_json(&body)?;
        let service = UploadService::new(Arc::clone(&state.app));
        let stored = service
            .complete(
                upload_id(&id)?,
                CompleteUploadRequest {
                    parts: parse_completed_parts(&parsed)?,
                },
                &ctx,
            )
            .await?;

        Ok(stored_response(stored, &ctx.request_id))
    })
    .await
}

pub async fn abort(
    State(state): State<HttpState>,
    Extension(meta): Extension<RequestMeta>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let request_id = meta.request_id.clone();
    respond(Arc::clone(&state.app.metrics), &request_id, async move {
        let ctx = context(
            &meta,
            &state.app,
            &headers,
            "DELETE",
            "/uploads/{id}",
            &id,
            &[],
            &[],
        )?;

        let service = UploadService::new(Arc::clone(&state.app));
        let stored = service.abort(upload_id(&id)?, &ctx).await?;

        Ok(stored_response(stored, &ctx.request_id))
    })
    .await
}
