//! Upload session persistence.

use anystore_domain::UploadId;
use anystore_domain::error::{DomainError, DomainResult};
use anystore_domain::upload::UploadRecord;
use anystore_metastore::UploadRepository;
use anystore_metastore::uploads::{AbortUploadRecord, CreateUploadRecord, UpdateUploadState};
use async_trait::async_trait;
use sqlx::{AssertSqlSafe, Row};

use crate::PostgresMetaStore;
use crate::errors::map_sqlx;
use crate::rows::decode_upload;

const UPLOAD_COLUMNS: &str = "id, object_id, state, mode, blob_backend, blob_ref, \
     provider_upload_id, expected_size, content_type, expected_sha256, provider_completed, \
     created_at, expires_at, completed_at, aborted_at";

#[async_trait]
impl UploadRepository for PostgresMetaStore {
    async fn create_upload_record(&self, cmd: CreateUploadRecord) -> DomainResult<UploadRecord> {
        let sql = format!(
            "INSERT INTO uploads (
                 id, object_id, state, mode, blob_backend, blob_ref,
                 expected_size, content_type, expected_sha256, created_at, expires_at)
             VALUES ($1, $2, 'initiating', $3, $4, $5, $6, $7, $8, $9, $10)
             RETURNING {UPLOAD_COLUMNS}"
        );

        let row = sqlx::query(AssertSqlSafe(sql))
            .bind(cmd.id.as_str())
            .bind(cmd.object_id.as_str())
            .bind(cmd.mode.as_str())
            .bind(&cmd.blob_backend)
            .bind(&cmd.blob_ref)
            .bind(cmd.expected_size as i64)
            .bind(&cmd.content_type)
            .bind(cmd.expected_sha256.as_deref())
            .bind(cmd.created_at)
            .bind(cmd.expires_at)
            .fetch_one(self.pool())
            .await
            .map_err(map_sqlx)?;

        decode_upload(&row)
    }

    async fn get_upload(&self, id: &UploadId) -> DomainResult<Option<UploadRecord>> {
        let sql = format!("SELECT {UPLOAD_COLUMNS} FROM uploads WHERE id = $1");
        let row = sqlx::query(AssertSqlSafe(sql))
            .bind(id.as_str())
            .fetch_optional(self.pool())
            .await
            .map_err(map_sqlx)?;

        row.as_ref().map(decode_upload).transpose()
    }

    async fn update_upload_state(&self, cmd: UpdateUploadState) -> DomainResult<()> {
        let result = sqlx::query(
            "UPDATE uploads
             SET state = $2,
                 provider_upload_id = COALESCE($3, provider_upload_id),
                 provider_completed = COALESCE($4, provider_completed)
             WHERE id = $1",
        )
        .bind(cmd.id.as_str())
        .bind(cmd.state.as_str())
        .bind(cmd.provider_upload_id.as_deref())
        .bind(cmd.provider_completed)
        .execute(self.pool())
        .await
        .map_err(map_sqlx)?;

        if result.rows_affected() == 0 {
            return Err(DomainError::UploadNotFound);
        }
        Ok(())
    }

    async fn abort_upload_record(&self, cmd: AbortUploadRecord) -> DomainResult<()> {
        let mut tx = self.pool().begin().await.map_err(map_sqlx)?;

        // A completed session is never rolled back: its blob is live content.
        let row = sqlx::query(
            "UPDATE uploads
             SET state = 'aborted', aborted_at = $2
             WHERE id = $1 AND state <> 'completed'
             RETURNING blob_backend, blob_ref",
        )
        .bind(cmd.id.as_str())
        .bind(cmd.now)
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlx)?;

        if let Some(row) = row {
            let backend: String = row.try_get("blob_backend").map_err(map_sqlx)?;
            let blob_ref: String = row.try_get("blob_ref").map_err(map_sqlx)?;
            sqlx::query(
                "INSERT INTO blob_gc_queue (blob_backend, blob_ref, not_before)
                 VALUES ($1, $2, $3)
                 ON CONFLICT (blob_backend, blob_ref) DO NOTHING",
            )
            .bind(&backend)
            .bind(&blob_ref)
            .bind(cmd.gc_not_before)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx)?;
        }

        tx.commit().await.map_err(map_sqlx)?;
        Ok(())
    }
}
