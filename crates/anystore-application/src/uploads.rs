//! Upload session lifecycle.
//!
//! File bytes never transit this service: it only creates sessions, signs
//! provider access, verifies completion and commits metadata.

use anystore_blobstore::{
    AbortBlobUpload, CompleteBlobUpload, CompletedPart, PrepareUpload, SignParts,
};
use anystore_domain::error::{DomainError, DomainResult};
use anystore_domain::upload::{UploadMode, UploadRecord, UploadState};
use anystore_domain::{ObjectId, UploadId};
use anystore_metastore::commands::{CommitContent, MutationOutcome};
use anystore_metastore::response::{ResponseRenderer, StoredResponse};
use anystore_metastore::uploads::{AbortUploadRecord, CreateUploadRecord, UpdateUploadState};
use std::sync::Arc;
use std::time::Duration as StdDuration;

use crate::context::RequestContext;
use crate::dto::{parts_json, upload_complete_json, upload_session_json};
use crate::idempotency::{self, derived_id};
use crate::metrics::Metrics;
use crate::objects::mutation_context;
use crate::state::AppState;

/// Provider limit shared by S3-compatible multipart APIs.
const MAX_PART_NUMBER: u32 = 10_000;

#[derive(Clone, Debug)]
pub struct CreateUploadRequest {
    pub object_id: ObjectId,
    pub size: u64,
    pub content_type: Option<String>,
    pub sha256: Option<String>,
}

#[derive(Clone, Debug)]
pub struct AllocatePartsRequest {
    pub part_numbers: Vec<u32>,
}

/// A part the client reports as uploaded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompletedPartInput {
    pub part_number: u32,
    pub etag: String,
}

#[derive(Clone, Debug, Default)]
pub struct CompleteUploadRequest {
    pub parts: Vec<CompletedPartInput>,
}

#[derive(Clone)]
pub struct UploadService {
    state: Arc<AppState>,
}

fn to_std(duration: chrono::Duration) -> StdDuration {
    duration.to_std().unwrap_or(StdDuration::from_secs(600))
}

fn render_upload_complete() -> ResponseRenderer {
    Arc::new(move |outcome: &MutationOutcome| {
        let view = outcome
            .object
            .as_ref()
            .ok_or_else(|| DomainError::internal("content commit produced no object"))?;
        let body = serde_json::to_vec(&upload_complete_json(view))
            .map_err(|e| DomainError::internal(format!("response encoding failed: {e}")))?;
        Ok(StoredResponse::json(200, body))
    })
}

impl UploadService {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }

    async fn load_active_upload(&self, id: &UploadId) -> DomainResult<UploadRecord> {
        let upload = self
            .state
            .meta
            .get_upload(id)
            .await?
            .ok_or(DomainError::UploadNotFound)?;
        if matches!(upload.state, UploadState::Aborted | UploadState::Expired) {
            return Err(DomainError::UploadNotFound);
        }
        Ok(upload)
    }

    pub async fn create(
        &self,
        req: CreateUploadRequest,
        ctx: &RequestContext,
    ) -> DomainResult<StoredResponse> {
        let target = self
            .state
            .meta
            .get_object(&req.object_id)
            .await?
            .ok_or(DomainError::ObjectNotFound)?;
        if !target.object.is_file() {
            return Err(DomainError::NotAFile);
        }

        let content_type = req
            .content_type
            .or_else(|| target.object.content_type.clone())
            .unwrap_or_else(|| "application/octet-stream".to_owned());

        let config = self.state.config.clone();
        let mode = if req.size > config.multipart_threshold {
            UploadMode::Multipart
        } else {
            UploadMode::Single
        };

        let state = Arc::clone(&self.state);
        idempotency::run_with_completion(&state, ctx, |ictx| {
            let state = Arc::clone(&state);
            let ctx = ctx.clone();
            async move {
                // Deriving the id from the idempotency scope makes a retry after
                // a crash resume the same session instead of creating a second.
                let upload_id = match &ictx {
                    Some(ictx) => UploadId::new(derived_id("upload_", ictx)),
                    None => UploadId::generate(),
                };

                let blobs = state.blobs.default_store();
                let blob_ref = blobs.blob_ref_for(upload_id.as_str());

                // The row is written before the provider is contacted, so a
                // crash leaves a record the maintenance sweep can clean up.
                state
                    .meta
                    .create_upload_record(CreateUploadRecord {
                        id: upload_id.clone(),
                        object_id: req.object_id.clone(),
                        mode,
                        blob_backend: blobs.backend_id().to_owned(),
                        blob_ref: blob_ref.as_str().to_owned(),
                        expected_size: req.size,
                        content_type: content_type.clone(),
                        expected_sha256: req.sha256.clone(),
                        created_at: ctx.now,
                        expires_at: ctx.now + config.upload_expiry,
                    })
                    .await?;
                Metrics::incr(&state.metrics.upload_sessions_total);

                let prepared = blobs
                    .prepare_upload(PrepareUpload {
                        upload_id: upload_id.as_str().to_owned(),
                        mode,
                        expected_size: req.size,
                        content_type: content_type.clone(),
                        expected_sha256: req.sha256.clone(),
                        part_size: config.part_size,
                        expires_in: to_std(config.upload_url_expiry),
                    })
                    .await?;

                state
                    .meta
                    .update_upload_state(UpdateUploadState {
                        id: upload_id.clone(),
                        state: UploadState::Ready,
                        provider_upload_id: prepared.provider_upload_id.clone(),
                        provider_completed: None,
                    })
                    .await?;

                let body = serde_json::to_vec(&upload_session_json(
                    upload_id.as_str(),
                    req.object_id.as_str(),
                    &prepared,
                ))
                .map_err(|e| DomainError::internal(format!("response encoding failed: {e}")))?;

                Ok(StoredResponse::json(201, body))
            }
        })
        .await
    }

    pub async fn allocate_parts(
        &self,
        id: UploadId,
        req: AllocatePartsRequest,
        ctx: &RequestContext,
    ) -> DomainResult<StoredResponse> {
        let upload = self.load_active_upload(&id).await?;
        if upload.mode != UploadMode::Multipart {
            return Err(DomainError::InvalidRequest(
                "This upload session is not multipart.".into(),
            ));
        }
        if req.part_numbers.is_empty() {
            return Err(DomainError::InvalidRequest(
                "part_numbers must not be empty.".into(),
            ));
        }
        if req.part_numbers.len() > self.state.config.max_part_numbers {
            return Err(DomainError::InvalidRequest(
                "Too many part numbers requested.".into(),
            ));
        }
        if req
            .part_numbers
            .iter()
            .any(|n| *n == 0 || *n > MAX_PART_NUMBER)
        {
            return Err(DomainError::InvalidRequest(format!(
                "part_numbers must be between 1 and {MAX_PART_NUMBER}."
            )));
        }

        let provider_upload_id = upload
            .provider_upload_id
            .clone()
            .ok_or_else(|| DomainError::internal("multipart upload has no provider id"))?;

        let state = Arc::clone(&self.state);
        let config = state.config.clone();
        idempotency::run_with_completion(&state, ctx, |_ictx| {
            let state = Arc::clone(&state);
            async move {
                let blobs = state.blobs.get(&upload.blob_backend)?;
                let parts = blobs
                    .sign_parts(SignParts {
                        blob_ref: anystore_blobstore::BlobRef::new(upload.blob_ref.clone()),
                        provider_upload_id,
                        part_numbers: req.part_numbers,
                        expires_in: to_std(config.upload_url_expiry),
                    })
                    .await?;

                let body = serde_json::to_vec(&parts_json(&parts))
                    .map_err(|e| DomainError::internal(format!("response encoding failed: {e}")))?;
                Ok(StoredResponse::json(200, body))
            }
        })
        .await
    }

    pub async fn complete(
        &self,
        id: UploadId,
        req: CompleteUploadRequest,
        ctx: &RequestContext,
    ) -> DomainResult<StoredResponse> {
        let state = Arc::clone(&self.state);
        let config = state.config.clone();

        let result = idempotency::run(&state, ctx, |ictx| {
            let state = Arc::clone(&state);
            let ctx = ctx.clone();
            async move {
                let upload = state
                    .meta
                    .get_upload(&id)
                    .await?
                    .ok_or(DomainError::UploadNotFound)?;
                if !upload.state.is_completable() {
                    return Err(DomainError::UploadNotFound);
                }

                let target = state
                    .meta
                    .get_object(&upload.object_id)
                    .await?
                    .ok_or(DomainError::ObjectNotFound)?;
                if !target.object.is_file() {
                    return Err(DomainError::NotAFile);
                }

                // Fail fast on a stale revision so no provider work is done,
                // leaving the currently readable content untouched. The
                // authoritative check happens again inside the commit.
                if let Some(expected) = ctx.if_match
                    && expected != target.object.revision
                {
                    return Err(DomainError::RevisionConflict {
                        current_revision: target.object.revision.get(),
                    });
                }

                state
                    .meta
                    .update_upload_state(UpdateUploadState {
                        id: id.clone(),
                        state: UploadState::Completing,
                        provider_upload_id: None,
                        provider_completed: None,
                    })
                    .await?;

                let blobs = state.blobs.get(&upload.blob_backend)?;
                let blob_ref = anystore_blobstore::BlobRef::new(upload.blob_ref.clone());

                // Safe to retry: an already-finalised provider upload converges
                // instead of failing.
                let stat = blobs
                    .ensure_upload_completed(CompleteBlobUpload {
                        blob_ref: blob_ref.clone(),
                        mode: upload.mode,
                        provider_upload_id: upload.provider_upload_id.clone(),
                        parts: req
                            .parts
                            .into_iter()
                            .map(|p| CompletedPart {
                                part_number: p.part_number,
                                etag: p.etag,
                            })
                            .collect(),
                    })
                    .await?;

                if stat.size != upload.expected_size {
                    return Err(DomainError::ChecksumMismatch(format!(
                        "Uploaded size {} does not match the declared size {}.",
                        stat.size, upload.expected_size
                    )));
                }

                if let (Some(expected), Some(actual)) = (&upload.expected_sha256, &stat.sha256)
                    && !expected.eq_ignore_ascii_case(actual)
                {
                    return Err(DomainError::ChecksumMismatch(
                        "Uploaded content does not match the supplied SHA-256.".into(),
                    ));
                }

                // When the backend cannot verify server-side, the stored hash
                // is the client's declaration, per the architecture document.
                let sha256 = stat
                    .sha256
                    .clone()
                    .or_else(|| upload.expected_sha256.clone());

                state
                    .meta
                    .commit_content(CommitContent {
                        upload_id: id.clone(),
                        object_id: upload.object_id.clone(),
                        if_match: ctx.if_match,
                        blob_backend: upload.blob_backend.clone(),
                        blob_ref: upload.blob_ref.clone(),
                        size: stat.size,
                        content_type: upload.content_type.clone(),
                        sha256,
                        gc_not_before: ctx.now + config.gc_grace,
                        ctx: mutation_context(&ctx, &state, ictx, render_upload_complete()),
                    })
                    .await
            }
        })
        .await;

        if result.is_err() {
            Metrics::incr(&self.state.metrics.upload_complete_failures_total);
        }
        result
    }

    pub async fn abort(&self, id: UploadId, ctx: &RequestContext) -> DomainResult<StoredResponse> {
        let state = Arc::clone(&self.state);

        idempotency::run_with_completion(&state, ctx, |_ictx| {
            let state = Arc::clone(&state);
            let ctx = ctx.clone();
            async move {
                let upload = state
                    .meta
                    .get_upload(&id)
                    .await?
                    .ok_or(DomainError::UploadNotFound)?;

                if upload.state != UploadState::Completed {
                    let blobs = state.blobs.get(&upload.blob_backend)?;
                    // Provider cleanup is best effort; the GC queue is the
                    // durable path and abort must stay idempotent.
                    if let Err(error) = blobs
                        .abort_upload(AbortBlobUpload {
                            blob_ref: anystore_blobstore::BlobRef::new(upload.blob_ref.clone()),
                            mode: upload.mode,
                            provider_upload_id: upload.provider_upload_id.clone(),
                        })
                        .await
                    {
                        tracing::warn!(
                            upload_id = %id,
                            blob_backend = %upload.blob_backend,
                            "provider abort failed; leaving blob to garbage collection: {error}"
                        );
                    }

                    state
                        .meta
                        .abort_upload_record(AbortUploadRecord {
                            id: id.clone(),
                            now: ctx.now,
                            gc_not_before: ctx.now,
                        })
                        .await?;
                }

                Ok(StoredResponse::no_content())
            }
        })
        .await
    }
}
