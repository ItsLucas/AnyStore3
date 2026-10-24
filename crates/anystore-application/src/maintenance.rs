//! Background maintenance.
//!
//! Internal only: no public API contract depends on it, and a failure here can
//! never affect API correctness.

use anystore_blobstore::{AbortBlobUpload, BlobRef};
use anystore_domain::error::DomainResult;
use chrono::{DateTime, Duration, Utc};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use crate::metrics::Metrics;
use crate::state::AppState;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MaintenanceReport {
    pub blobs_deleted: u32,
    pub blobs_failed: u32,
    pub uploads_expired: u32,
    pub idempotency_purged: u64,
    pub cursors_purged: u64,
    pub changes_purged: u64,
    /// False when the time budget ran out with expiry or GC work left over;
    /// the next pass resumes it.
    pub drained: bool,
}

/// Runs maintenance passes. Clones share one in-process guard, so a scheduled
/// pass and a triggered pass never overlap within a process; across processes
/// every claim is `SKIP LOCKED`, so overlapping passes are merely redundant.
#[derive(Clone)]
pub struct MaintenanceService {
    state: Arc<AppState>,
    gc_batch_size: u32,
    running: Arc<AtomicBool>,
}

/// Clears the in-process guard however the pass ends.
struct RunningGuard(Arc<AtomicBool>);

impl Drop for RunningGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

impl MaintenanceService {
    pub fn new(state: Arc<AppState>) -> Self {
        Self {
            state,
            gc_batch_size: 50,
            running: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn with_batch_size(mut self, gc_batch_size: u32) -> Self {
        self.gc_batch_size = gc_batch_size.max(1);
        self
    }

    /// Runs one pass. Upload expiry and blob collection repeat in batches until
    /// their backlog is empty or `deadline` passes, so an infrequent schedule
    /// still keeps up; the first batch of each always runs.
    ///
    /// Returns `None` when another pass is already running in this process.
    pub async fn run(
        &self,
        now: DateTime<Utc>,
        deadline: Instant,
    ) -> DomainResult<Option<MaintenanceReport>> {
        if self.running.swap(true, Ordering::AcqRel) {
            return Ok(None);
        }
        let _guard = RunningGuard(Arc::clone(&self.running));

        let mut report = MaintenanceReport {
            drained: true,
            ..MaintenanceReport::default()
        };

        loop {
            let expired = self.abort_expired_uploads(now).await?;
            report.uploads_expired += expired;
            if expired < self.gc_batch_size {
                break;
            }
            if Instant::now() >= deadline {
                report.drained = false;
                break;
            }
        }

        loop {
            let (deleted, failed) = self.collect_blobs(now).await?;
            report.blobs_deleted += deleted;
            report.blobs_failed += failed;
            if deleted + failed < self.gc_batch_size {
                break;
            }
            if Instant::now() >= deadline {
                report.drained = false;
                break;
            }
        }

        report.idempotency_purged = self.state.meta.purge_idempotency_records(now).await?;
        report.cursors_purged = self.state.meta.purge_change_cursors(now).await?;
        report.changes_purged = self
            .state
            .meta
            .purge_changes(now - self.state.config.changes_retention)
            .await?;

        if let Ok(pending) = self.state.meta.count_pending_gc().await {
            Metrics::set(&self.state.metrics.blob_gc_pending, pending);
        }
        Ok(Some(report))
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
