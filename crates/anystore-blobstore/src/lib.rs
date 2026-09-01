//! `BlobStore` port.
//!
//! The application uses opaque [`BlobRef`] values only. Buckets, object keys,
//! regions, provider upload ids, signing formats and credentials never cross
//! this boundary.

pub mod registry;

pub use registry::BlobRegistry;

use anystore_domain::error::DomainResult;
use anystore_domain::upload::UploadMode;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::collections::BTreeMap;
use std::time::Duration;

/// Opaque handle to an immutable blob. Its internal form belongs to the adapter.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct BlobRef(String);

impl BlobRef {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_inner(self) -> String {
        self.0
    }
}

/// A signed request the client performs directly against the provider.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SignedRequest {
    pub method: String,
    pub url: String,
    pub headers: BTreeMap<String, String>,
}

#[derive(Clone, Debug)]
pub struct PrepareUpload {
    /// Used to derive a fresh, immutable provider key. Never contains the
    /// object's logical path or name.
    pub upload_id: String,
    pub mode: UploadMode,
    pub expected_size: u64,
    pub content_type: String,
    pub expected_sha256: Option<String>,
    pub part_size: u64,
    pub expires_in: Duration,
}

#[derive(Clone, Debug)]
pub struct PreparedUpload {
    pub blob_ref: BlobRef,
    pub mode: UploadMode,
    /// Provider-side multipart identifier, when applicable.
    pub provider_upload_id: Option<String>,
    /// Present for single-shot uploads.
    pub single: Option<SignedRequest>,
    /// Present for multipart uploads.
    pub part_size: Option<u64>,
    pub expires_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct SignParts {
    pub blob_ref: BlobRef,
    pub provider_upload_id: String,
    pub part_numbers: Vec<u32>,
    pub expires_in: Duration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SignedPart {
    pub part_number: u32,
    pub method: String,
    pub url: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompletedPart {
    pub part_number: u32,
    pub etag: String,
}

#[derive(Clone, Debug)]
pub struct CompleteBlobUpload {
    pub blob_ref: BlobRef,
    pub mode: UploadMode,
    pub provider_upload_id: Option<String>,
    pub parts: Vec<CompletedPart>,
}

#[derive(Clone, Debug)]
pub struct AbortBlobUpload {
    pub blob_ref: BlobRef,
    pub mode: UploadMode,
    pub provider_upload_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobStat {
    pub size: u64,
    pub content_type: Option<String>,
    /// Provider entity tag. Internal only; not part of the public contract.
    pub etag: Option<String>,
    /// Set only when the backend can verify SHA-256 without downloading the
    /// blob through the application.
    pub sha256: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SignedDownload {
    pub url: String,
    pub expires_at: DateTime<Utc>,
}

#[async_trait]
pub trait BlobStore: Send + Sync {
    /// Stable identifier persisted with objects and uploads so that a future
    /// migration never has to change public object ids.
    fn backend_id(&self) -> &'static str;

    /// True when this backend verifies SHA-256 server-side. When false the
    /// stored hash is client-declared, per the architecture document.
    fn verifies_sha256(&self) -> bool {
        false
    }

    /// Derives the immutable blob location for an upload session.
    ///
    /// Deterministic so the upload row can be persisted *before* the provider
    /// is contacted: a crash then leaves a recoverable record rather than an
    /// untracked orphan. The logical folder path and file name are never
    /// encoded into the result, which is why rename and move touch no blob.
    fn blob_ref_for(&self, upload_id: &str) -> BlobRef;

    async fn prepare_upload(&self, req: PrepareUpload) -> DomainResult<PreparedUpload>;

    async fn sign_parts(&self, req: SignParts) -> DomainResult<Vec<SignedPart>>;

    /// Finalises the provider-side upload. Must be safe to call repeatedly: if
    /// the provider upload was already finalised, it must converge rather than
    /// fail.
    ///
    /// Returns [`DomainError::ContentNotReady`] when the client never uploaded
    /// the bytes; that is a caller mistake, not a provider fault.
    ///
    /// [`DomainError::ContentNotReady`]: anystore_domain::error::DomainError::ContentNotReady
    async fn ensure_upload_completed(&self, req: CompleteBlobUpload) -> DomainResult<BlobStat>;

    async fn abort_upload(&self, req: AbortBlobUpload) -> DomainResult<()>;

    async fn stat(&self, blob: &BlobRef) -> DomainResult<BlobStat>;

    async fn sign_download(
        &self,
        blob: &BlobRef,
        filename: &str,
        expires_in: Duration,
    ) -> DomainResult<SignedDownload>;

    /// Deletes a blob. A missing blob counts as success so GC stays idempotent.
    async fn delete_blob(&self, blob: &BlobRef) -> DomainResult<()>;
}
