//! Internal maintenance operations.

use anystore_domain::error::DomainResult;
use anystore_domain::upload::UploadRecord;
use anystore_metastore::MaintenanceStore;
use anystore_metastore::maintenance::BlobGcEntry;
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use sqlx::Row;

use crate::PostgresMetaStore;
use crate::errors::map_sqlx;
use crate::rows::decode_upload;

/// How long a claimed GC entry stays invisible to other workers.
const GC_LEASE_MINUTES: i64 = 5;

#[async_trait]
impl MaintenanceStore for PostgresMetaStore {
    async fn claim_gc_batch(
        &self,
        now: DateTime<Utc>,
        limit: u32,
    ) -> DomainResult<Vec<BlobGcEntry>> {
        // Pushing `not_before` forward is the claim: concurrent instances never
        // pick up the same blob, and a crashed worker's entry becomes visible
        // again once the lease elapses.
        let rows = sqlx::query(
            "UPDATE blob_gc_queue
             SET not_before = $2
             WHERE (blob_backend, blob_ref) IN (
                 SELECT blob_backend, blob_ref
                 FROM blob_gc_queue
                 WHERE not_before <= $1
                 ORDER BY not_before
                 LIMIT $3
                 FOR UPDATE SKIP LOCKED
             )
             RETURNING blob_backend, blob_ref, attempts",
        )
        .bind(now)
        .bind(now + Duration::minutes(GC_LEASE_MINUTES))
        .bind(i64::from(limit))
        .fetch_all(self.pool())
        .await
        .map_err(map_sqlx)?;

        rows.iter()
            .map(|row| {
                Ok(BlobGcEntry {
                    blob_backend: row.try_get("blob_backend").map_err(map_sqlx)?,
                    blob_ref: row.try_get("blob_ref").map_err(map_sqlx)?,
                    attempts: row.try_get("attempts").map_err(map_sqlx)?,
                })
            })
            .collect()
    }

    async fn finish_gc(&self, entry: &BlobGcEntry) -> DomainResult<()> {
        sqlx::query("DELETE FROM blob_gc_queue WHERE blob_backend = $1 AND blob_ref = $2")
            .bind(&entry.blob_backend)
            .bind(&entry.blob_ref)
            .execute(self.pool())
            .await
            .map_err(map_sqlx)?;
        Ok(())
    }

    async fn fail_gc(
        &self,
        entry: &BlobGcEntry,
        error: &str,
        retry_at: DateTime<Utc>,
    ) -> DomainResult<()> {
        sqlx::query(
            "UPDATE blob_gc_queue
             SET attempts = attempts + 1, last_error = $3, not_before = $4
             WHERE blob_backend = $1 AND blob_ref = $2",
        )
        .bind(&entry.blob_backend)
        .bind(&entry.blob_ref)
        .bind(error)
        .bind(retry_at)
        .execute(self.pool())
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }

    async fn claim_expired_uploads(
        &self,
        now: DateTime<Utc>,
        limit: u32,
    ) -> DomainResult<Vec<UploadRecord>> {
        let mut tx = self.pool().begin().await.map_err(map_sqlx)?;

        let rows = sqlx::query(
            "UPDATE uploads
             SET state = 'expired'
             WHERE id IN (
                 SELECT id FROM uploads
                 WHERE expires_at <= $1
                   AND state IN ('initiating', 'ready', 'completing')
                 ORDER BY expires_at
                 LIMIT $2
                 FOR UPDATE SKIP LOCKED
             )
             RETURNING id, object_id, state, mode, blob_backend, blob_ref,
                       provider_upload_id, expected_size, content_type,
                       expected_sha256, provider_completed, created_at,
                       expires_at, completed_at, aborted_at",
        )
        .bind(now)
        .bind(i64::from(limit))
        .fetch_all(&mut *tx)
        .await
        .map_err(map_sqlx)?;

        let uploads = rows
            .iter()
            .map(decode_upload)
            .collect::<DomainResult<Vec<UploadRecord>>>()?;

        for upload in &uploads {
            sqlx::query(
                "INSERT INTO blob_gc_queue (blob_backend, blob_ref, not_before)
                 VALUES ($1, $2, $3)
                 ON CONFLICT (blob_backend, blob_ref) DO NOTHING",
            )
            .bind(&upload.blob_backend)
            .bind(&upload.blob_ref)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx)?;
        }

        tx.commit().await.map_err(map_sqlx)?;
        Ok(uploads)
    }

    async fn purge_idempotency_records(&self, now: DateTime<Utc>) -> DomainResult<u64> {
        let result = sqlx::query("DELETE FROM idempotency_records WHERE expires_at <= $1")
            .bind(now)
            .execute(self.pool())
            .await
            .map_err(map_sqlx)?;
        Ok(result.rows_affected())
    }

    async fn purge_change_cursors(&self, now: DateTime<Utc>) -> DomainResult<u64> {
        let result = sqlx::query("DELETE FROM change_cursors WHERE expires_at <= $1")
            .bind(now)
            .execute(self.pool())
            .await
            .map_err(map_sqlx)?;
        Ok(result.rows_affected())
    }

    async fn purge_changes(&self, older_than: DateTime<Utc>) -> DomainResult<u64> {
        let mut tx = self.pool().begin().await.map_err(map_sqlx)?;

        // The watermark outlives the rows so that a cursor pointing into purged
        // history can still be rejected with `changes_cursor_expired`.
        let purged: Option<i64> = sqlx::query_scalar(
            "WITH deleted AS (
                 DELETE FROM changes WHERE changed_at < $1 RETURNING seq
             )
             SELECT MAX(seq) FROM deleted",
        )
        .bind(older_than)
        .fetch_one(&mut *tx)
        .await
        .map_err(map_sqlx)?;

        let count = match purged {
            Some(max_seq) => {
                let result = sqlx::query(
                    "UPDATE changes_retention
                     SET purged_through_seq = GREATEST(purged_through_seq, $1)
                     WHERE singleton",
                )
                .bind(max_seq)
                .execute(&mut *tx)
                .await
                .map_err(map_sqlx)?;
                result.rows_affected()
            }
            None => 0,
        };

        tx.commit().await.map_err(map_sqlx)?;
        Ok(count)
    }

    async fn count_pending_gc(&self) -> DomainResult<u64> {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM blob_gc_queue")
            .fetch_one(self.pool())
            .await
            .map_err(map_sqlx)?;
        Ok(count as u64)
    }
}
