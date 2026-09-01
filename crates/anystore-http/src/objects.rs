//! Object handlers.

use anystore_application::objects::{
    CreateObjectRequest, ListChildrenRequest, ObjectService, PatchObjectRequest,
};
use anystore_domain::error::{DomainError, DomainResult};
use anystore_domain::metadata::MetadataPatch;
use anystore_domain::object::ObjectKind;
use anystore_domain::ObjectId;
use anystore_metastore::commands::{ListOrder, OrderBy};
use axum::body::Bytes;
use axum::extract::{Path, RawQuery, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use axum::Extension;
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;

use crate::HttpState;
use crate::request::{
    RequestMeta, context, find, json_response, object_id, parse_bool, parse_json, parse_limit,
    query_pairs, stored_response,
};
use crate::respond;

#[derive(Debug, Deserialize)]
struct CreateBody {
    kind: String,
    name: String,
    #[serde(default)]
    parent_id: Option<String>,
    #[serde(default)]
    content_type: Option<String>,
    #[serde(default)]
    metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct PatchBody {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    parent_id: Option<String>,
    #[serde(default)]
    metadata: Option<MetadataPatch>,
}

fn parse_kind(raw: &str) -> DomainResult<ObjectKind> {
    ObjectKind::parse(raw)
        .ok_or_else(|| DomainError::InvalidRequest("kind must be 'file' or 'folder'.".into()))
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
            "/objects",
            "",
            &[],
            &body,
        )?;

        let parsed: CreateBody = serde_json::from_value(parse_json(&body)?)
            .map_err(|e| DomainError::InvalidRequest(format!("Invalid request body: {e}")))?;

        let service = ObjectService::new(Arc::clone(&state.app));
        let stored = service
            .create(
                CreateObjectRequest {
                    kind: parse_kind(&parsed.kind)?,
                    name: parsed.name,
                    parent_id: parsed.parent_id.map(ObjectId::new),
                    content_type: parsed.content_type,
                    metadata: parsed.metadata,
                },
                &ctx,
            )
            .await?;

        Ok(stored_response(stored, &ctx.request_id))
    })
    .await
}

pub async fn get(
    State(state): State<HttpState>,
    Extension(meta): Extension<RequestMeta>,
    Path(id): Path<String>,
) -> Response {
    let request_id = meta.request_id.clone();
    respond(Arc::clone(&state.app.metrics), &request_id, async move {
        let service = ObjectService::new(Arc::clone(&state.app));
        let value = service.get(&object_id(&id)?).await?;
        Ok(json_response(StatusCode::OK, &value, &meta.request_id))
    })
    .await
}

pub async fn patch(
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
            "PATCH",
            "/objects/{id}",
            &id,
            &[],
            &body,
        )?;

        let parsed: PatchBody = serde_json::from_value(parse_json(&body)?)
            .map_err(|e| DomainError::InvalidRequest(format!("Invalid request body: {e}")))?;

        let service = ObjectService::new(Arc::clone(&state.app));
        let stored = service
            .patch(
                object_id(&id)?,
                PatchObjectRequest {
                    name: parsed.name,
                    parent_id: parsed.parent_id.map(ObjectId::new),
                    metadata: parsed.metadata,
                },
                &ctx,
            )
            .await?;

        Ok(stored_response(stored, &ctx.request_id))
    })
    .await
}

pub async fn delete(
    State(state): State<HttpState>,
    Extension(meta): Extension<RequestMeta>,
    Path(id): Path<String>,
    RawQuery(raw_query): RawQuery,
    headers: HeaderMap,
) -> Response {
    let request_id = meta.request_id.clone();
    respond(Arc::clone(&state.app.metrics), &request_id, async move {
        let pairs = query_pairs(raw_query.as_deref());
        let ctx = context(
            &meta,
            &state.app,
            &headers,
            "DELETE",
            "/objects/{id}",
            &id,
            &pairs,
            &[],
        )?;

        let recursive = parse_bool(&pairs, "recursive")?;
        let service = ObjectService::new(Arc::clone(&state.app));
        let stored = service.delete(object_id(&id)?, recursive, &ctx).await?;

        Ok(stored_response(stored, &ctx.request_id))
    })
    .await
}

pub async fn children(
    State(state): State<HttpState>,
    Extension(meta): Extension<RequestMeta>,
    Path(id): Path<String>,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let request_id = meta.request_id.clone();
    respond(Arc::clone(&state.app.metrics), &request_id, async move {
        let pairs = query_pairs(raw_query.as_deref());

        let order_by = match find(&pairs, "order_by") {
            Some(raw) => OrderBy::parse(raw).ok_or_else(|| {
                DomainError::InvalidRequest(
                    "order_by must be name, created_at, updated_at or size.".into(),
                )
            })?,
            None => OrderBy::Name,
        };
        let order = match find(&pairs, "order") {
            Some(raw) => ListOrder::parse(raw)
                .ok_or_else(|| DomainError::InvalidRequest("order must be asc or desc.".into()))?,
            None => ListOrder::Asc,
        };

        let service = ObjectService::new(Arc::clone(&state.app));
        let value = service
            .list_children(ListChildrenRequest {
                parent_id: object_id(&id)?,
                limit: parse_limit(&pairs)?,
                cursor: find(&pairs, "cursor").map(str::to_owned),
                order_by,
                order,
            })
            .await?;

        Ok(json_response(StatusCode::OK, &value, &meta.request_id))
    })
    .await
}

pub async fn resolve(
    State(state): State<HttpState>,
    Extension(meta): Extension<RequestMeta>,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let request_id = meta.request_id.clone();
    respond(Arc::clone(&state.app.metrics), &request_id, async move {
        let pairs = query_pairs(raw_query.as_deref());
        let path = find(&pairs, "path")
            .ok_or_else(|| DomainError::InvalidRequest("path is required.".into()))?;

        let service = ObjectService::new(Arc::clone(&state.app));
        let value = service.resolve(path).await?;
        Ok(json_response(StatusCode::OK, &value, &meta.request_id))
    })
    .await
}
