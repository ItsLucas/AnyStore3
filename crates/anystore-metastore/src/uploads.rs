//! Upload session persistence port.

use anystore_domain::error::DomainResult;
use anystore_domain::upload::{UploadMode, UploadRecord, UploadState};
use anystore_domain::{ObjectId, UploadId};
use async_trait::async_trait;
use chrono::{DateTime, Utc};

#[derive(Clone, Debug)]
pub struct CreateUploadRecord {
    pub id: UploadId,
    pub object_id: ObjectId,
    pub mode: UploadMode,
    pub blob_backend: String,
    pub blob_ref: String,
    pub expected_size: u64,
    pub content_type: String,
    pub expected_sha256: Option<String>,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct CreateUploadResult {
    pub upload: UploadRecord,
    pub created: bool,
}

#[derive(Clone, Debug)]
pub struct UpdateUploadState {
    pub id: UploadId,
    pub state: UploadState,
    pub provider_upload_id: Option<String>,
    pub provider_completed: Option<bool>,
}

#[derive(Clone, Debug)]
pub struct MarkUploadCompleting {
    pub id: UploadId,
    pub provider_completed: bool,
}

#[derive(Clone, Debug)]
pub struct AbortUploadRecord {
    pub id: UploadId,
    pub now: DateTime<Utc>,
    /// Grace period before the abandoned blob becomes GC-eligible.
    pub gc_not_before: DateTime<Utc>,
}

#[async_trait]
pub trait UploadRepository: Send + Sync {
    async fn create_upload_record(
        &self,
        cmd: CreateUploadRecord,
    ) -> DomainResult<CreateUploadResult>;
    async fn get_upload(&self, id: &UploadId) -> DomainResult<Option<UploadRecord>>;
    async fn update_upload_state(&self, cmd: UpdateUploadState) -> DomainResult<()>;
    /// Aborts the session and enqueues its blob for garbage collection. Never
    /// changes the target object's revision and emits no Change.
    async fn abort_upload_record(&self, cmd: AbortUploadRecord) -> DomainResult<()>;
}
