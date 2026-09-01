//! Object lifecycle: create, read, list, resolve, patch and delete.

use anystore_domain::ObjectId;
use anystore_domain::error::{DomainError, DomainResult};
use anystore_domain::metadata::{
    MetadataPatch, validate_metadata_document, validate_metadata_patch,
};
use anystore_domain::name::validate_name;
use anystore_domain::object::ObjectKind;
use anystore_domain::page::clamp_limit;
use anystore_metastore::commands::{
    CreateObjectCommit, DeleteObjectCommit, ListChildren, ListOrder, MutationContext,
    MutationOutcome, OrderBy, PatchObjectCommit,
};
use anystore_metastore::idempotency::IdempotencyContext;
use anystore_metastore::response::{ResponseRenderer, StoredResponse};
use serde_json::Value;
use std::sync::Arc;

use crate::context::RequestContext;
use crate::dto::{object_json, page_json};
use crate::idempotency;
use crate::state::AppState;

#[derive(Clone, Debug)]
pub struct CreateObjectRequest {
    pub kind: ObjectKind,
    pub name: String,
    pub parent_id: Option<ObjectId>,
    pub content_type: Option<String>,
    pub metadata: Option<Value>,
}

#[derive(Clone, Debug, Default)]
pub struct PatchObjectRequest {
    pub name: Option<String>,
    pub parent_id: Option<ObjectId>,
    pub metadata: Option<MetadataPatch>,
}

impl PatchObjectRequest {
    pub fn is_empty(&self) -> bool {
        self.name.is_none() && self.parent_id.is_none() && self.metadata.is_none()
    }
}

#[derive(Clone, Debug)]
pub struct ListChildrenRequest {
    pub parent_id: ObjectId,
    pub limit: Option<u32>,
    pub cursor: Option<String>,
    pub order_by: OrderBy,
    pub order: ListOrder,
}

#[derive(Clone)]
pub struct ObjectService {
    state: Arc<AppState>,
}

/// Builds the shared mutation context, including the renderer the store will
/// invoke inside its transaction.
pub(crate) fn mutation_context(
    ctx: &RequestContext,
    state: &AppState,
    ictx: Option<IdempotencyContext>,
    render: ResponseRenderer,
) -> MutationContext {
    MutationContext {
        now: ctx.now,
        request_id: ctx.request_id.clone(),
        idempotency_key: ctx.idempotency_key.clone(),
        idempotency: ictx,
        idempotency_expires_at: ctx.now + state.config.idempotency_retention,
        render,
    }
}

fn render_object(status: u16) -> ResponseRenderer {
    Arc::new(move |outcome: &MutationOutcome| {
        let view = outcome
            .object
            .as_ref()
            .ok_or_else(|| DomainError::internal("mutation produced no object"))?;
        let body = serde_json::to_vec(&object_json(view))
            .map_err(|e| DomainError::internal(format!("response encoding failed: {e}")))?;
        Ok(StoredResponse::json(status, body))
    })
}

fn render_no_content() -> ResponseRenderer {
    Arc::new(|_outcome: &MutationOutcome| Ok(StoredResponse::no_content()))
}

impl ObjectService {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }

    pub async fn create(
        &self,
        req: CreateObjectRequest,
        ctx: &RequestContext,
    ) -> DomainResult<StoredResponse> {
        validate_name(&req.name)?;

        let metadata = req
            .metadata
            .unwrap_or_else(|| Value::Object(Default::default()));
        validate_metadata_document(&metadata)?;

        if req.kind == ObjectKind::Folder && req.content_type.is_some() {
            return Err(DomainError::InvalidRequest(
                "content_type is only valid for files.".into(),
            ));
        }

        let parent_id = req.parent_id.unwrap_or_else(ObjectId::root);
        let state = Arc::clone(&self.state);

        idempotency::run(&state, ctx, |ictx| {
            let state = Arc::clone(&state);
            let ctx = ctx.clone();
            async move {
                state
                    .meta
                    .create_object(CreateObjectCommit {
                        id: ObjectId::generate(),
                        kind: req.kind,
                        name: req.name,
                        parent_id,
                        content_type: req.content_type,
                        metadata,
                        ctx: mutation_context(&ctx, &state, ictx, render_object(201)),
                    })
                    .await
            }
        })
        .await
    }

    pub async fn get(&self, id: &ObjectId) -> DomainResult<Value> {
        let view = self
            .state
            .meta
            .get_object(id)
            .await?
            .ok_or(DomainError::ObjectNotFound)?;
        Ok(object_json(&view))
    }

    pub async fn list_children(&self, req: ListChildrenRequest) -> DomainResult<Value> {
        // The parent must exist and be a folder before we page over children.
        let parent = self
            .state
            .meta
            .get_object(&req.parent_id)
            .await?
            .ok_or(DomainError::ObjectNotFound)?;
        if !parent.object.is_folder() {
            return Err(DomainError::NotAFolder);
        }

        let page = self
            .state
            .meta
            .list_children(ListChildren {
                parent_id: req.parent_id,
                limit: clamp_limit(
                    req.limit,
                    self.state.config.list_default_limit,
                    self.state.config.list_max_limit,
                ),
                cursor: req.cursor,
                order_by: req.order_by,
                order: req.order,
            })
            .await?;

        Ok(page_json(
            page.items.iter().map(object_json).collect(),
            page.next_cursor.as_deref(),
            page.has_more,
        ))
    }

    pub async fn resolve(&self, path: &str) -> DomainResult<Value> {
        let view = self
            .state
            .meta
            .resolve_path(path)
            .await?
            .ok_or(DomainError::ObjectNotFound)?;
        Ok(object_json(&view))
    }

    pub async fn patch(
        &self,
        id: ObjectId,
        req: PatchObjectRequest,
        ctx: &RequestContext,
    ) -> DomainResult<StoredResponse> {
        if let Some(name) = &req.name {
            validate_name(name)?;
        }
        if let Some(patch) = &req.metadata {
            validate_metadata_patch(patch)?;
        }

        let state = Arc::clone(&self.state);
        idempotency::run(&state, ctx, |ictx| {
            let state = Arc::clone(&state);
            let ctx = ctx.clone();
            async move {
                state
                    .meta
                    .patch_object(PatchObjectCommit {
                        id,
                        if_match: ctx.if_match,
                        new_name: req.name,
                        new_parent_id: req.parent_id,
                        metadata_patch: req.metadata,
                        ctx: mutation_context(&ctx, &state, ictx, render_object(200)),
                    })
                    .await
            }
        })
        .await
    }

    pub async fn delete(
        &self,
        id: ObjectId,
        recursive: bool,
        ctx: &RequestContext,
    ) -> DomainResult<StoredResponse> {
        let state = Arc::clone(&self.state);
        idempotency::run(&state, ctx, |ictx| {
            let state = Arc::clone(&state);
            let ctx = ctx.clone();
            async move {
                state
                    .meta
                    .delete_object(DeleteObjectCommit {
                        id,
                        if_match: ctx.if_match,
                        recursive,
                        ctx: mutation_context(&ctx, &state, ictx, render_no_content()),
                    })
                    .await
            }
        })
        .await
    }
}
