//! Internal maintenance port. No public API contract depends on it.

use anystore_domain::error::DomainResult;
use anystore_domain::upload::UploadRecord;
use async_trait::async_trait;
use chrono::{DateTime, Utc};

/// A blob that is no longer referenced by any live object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobGcEntry {
    pub blob_backend: String,
    pub blob_ref: String,
    pub attempts: i32,
}

#[async_trait]
pub trait MaintenanceStore: Send + Sync {
    /// Claims up to `limit` blobs that are due for deletion.
    async fn claim_gc_batch(
        &self,
        now: DateTime<Utc>,
        limit: u32,
    ) -> DomainResult<Vec<BlobGcEntry>>;

    /// Removes a successfully deleted blob from the queue.
    async fn finish_gc(&self, entry: &BlobGcEntry) -> DomainResult<()>;

    /// Records a failed attempt and backs the entry off.
    async fn fail_gc(
        &self,
        entry: &BlobGcEntry,
        error: &str,
        retry_at: DateTime<Utc>,
    ) -> DomainResult<()>;

    /// Returns upload sessions past their expiry that still hold provider state.
    async fn claim_expired_uploads(
        &self,
        now: DateTime<Utc>,
        limit: u32,
    ) -> DomainResult<Vec<UploadRecord>>;

    /// Deletes idempotency records past their retention window.
    async fn purge_idempotency_records(&self, now: DateTime<Utc>) -> DomainResult<u64>;

    /// Deletes expired Changes cursors.
    async fn purge_change_cursors(&self, now: DateTime<Utc>) -> DomainResult<u64>;

    /// Deletes Changes older than `older_than` and advances the retention
    /// watermark so cursors pointing before it can still be rejected with
    /// `changes_cursor_expired`.
    async fn purge_changes(&self, older_than: DateTime<Utc>) -> DomainResult<u64>;

    /// Number of blobs currently awaiting collection, for observability.
    async fn count_pending_gc(&self) -> DomainResult<u64>;
}
