//! Background maintenance.
//!
//! Internal only: no public API contract depends on it, and a failure here can
//! never affect API correctness.

use anystore_blobstore::{AbortBlobUpload, BlobRef};
use anystore_domain::error::DomainResult;
use chrono::{DateTime, Duration, Utc};
use std::sync::Arc;

use crate::state::AppState;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MaintenanceReport {
    pub blobs_deleted: u32,
    pub blobs_failed: u32,
    pub uploads_expired: u32,
    pub idempotency_purged: u64,
    pub cursors_purged: u64,
    pub changes_purged: u64,
}

#[derive(Clone)]
pub struct MaintenanceService {
    state: Arc<AppState>,
    gc_batch_size: u32,
}

impl MaintenanceService {
    pub fn new(state: Arc<AppState>) -> Self {
        Self {
            state,
            gc_batch_size: 50,
        }
    }

    pub async fn run_once(&self, now: DateTime<Utc>) -> DomainResult<MaintenanceReport> {
        let uploads_expired = self.abort_expired_uploads(now).await?;
        let (blobs_deleted, blobs_failed) = self.collect_blobs(now).await?;

        Ok(MaintenanceReport {
            blobs_deleted,
            blobs_failed,
            uploads_expired,
            idempotency_purged: self.state.meta.purge_idempotency_records(now).await?,
            cursors_purged: self.state.meta.purge_change_cursors(now).await?,
            changes_purged: self
                .state
                .meta
                .purge_changes(now - self.state.config.changes_retention)
                .await?,
        })
    }

    async fn abort_expired_uploads(&self, now: DateTime<Utc>) -> DomainResult<u32> {
        let expired = self
            .state
            .meta
            .claim_expired_uploads(now, self.gc_batch_size)
            .await?;

        for upload in &expired {
            let Ok(blobs) = self.state.blobs.get(&upload.blob_backend) else {
                continue;
            };
            if let Err(error) = blobs
                .abort_upload(AbortBlobUpload {
                    blob_ref: BlobRef::new(upload.blob_ref.clone()),
                    mode: upload.mode,
                    provider_upload_id: upload.provider_upload_id.clone(),
                })
                .await
            {
                tracing::warn!(upload_id = %upload.id, "aborting stale provider upload failed: {error}");
            }
        }

        Ok(expired.len() as u32)
    }

    async fn collect_blobs(&self, now: DateTime<Utc>) -> DomainResult<(u32, u32)> {
        let batch = self
            .state
            .meta
            .claim_gc_batch(now, self.gc_batch_size)
            .await?;

        let mut deleted = 0;
        let mut failed = 0;

        for entry in batch {
            let store = match self.state.blobs.get(&entry.blob_backend) {
                Ok(store) => store,
                Err(_) => {
                    failed += 1;
                    continue;
                }
            };

            match store
                .delete_blob(&BlobRef::new(entry.blob_ref.clone()))
                .await
            {
                Ok(()) => {
                    self.state.meta.finish_gc(&entry).await?;
                    deleted += 1;
                }
                Err(error) => {
                    // Exponential backoff; a failure here never affects the API.
                    let backoff = Duration::seconds(
                        60i64.saturating_mul(1i64 << entry.attempts.clamp(0, 10)),
                    );
                    self.state
                        .meta
                        .fail_gc(&entry, &error.to_string(), now + backoff)
                        .await?;
                    failed += 1;
                }
            }
        }

        Ok((deleted, failed))
    }
}
