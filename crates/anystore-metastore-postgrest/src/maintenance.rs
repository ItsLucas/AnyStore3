//! Internal maintenance operations.

use anystore_domain::error::DomainResult;
use anystore_domain::upload::UploadRecord;
use anystore_metastore::MaintenanceStore;
use anystore_metastore::maintenance::BlobGcEntry;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::{Value, json};

use crate::PostgrestMetaStore;
use crate::decode;

fn gc_entry(value: &Value) -> DomainResult<BlobGcEntry> {
    Ok(BlobGcEntry {
        blob_backend: decode::text(value, "blob_backend")?.to_owned(),
        blob_ref: decode::text(value, "blob_ref")?.to_owned(),
        attempts: i32::try_from(decode::integer(value, "attempts")?).unwrap_or(i32::MAX),
    })
}

fn count(value: &Value) -> DomainResult<u64> {
    value.as_u64().ok_or_else(|| {
        anystore_domain::error::DomainError::internal("rpc maintenance returned no count")
    })
}

fn entries(value: &Value) -> DomainResult<&Vec<Value>> {
    value.as_array().ok_or_else(|| {
        anystore_domain::error::DomainError::internal("rpc maintenance returned no list")
    })
}

#[async_trait]
impl MaintenanceStore for PostgrestMetaStore {
    async fn claim_gc_batch(
        &self,
        now: DateTime<Utc>,
        limit: u32,
    ) -> DomainResult<Vec<BlobGcEntry>> {
        let data = self
            .client()
            .call(
                "claim_gc_batch",
                json!({"now": decode::timestamp(now), "limit": limit}),
            )
            .await?;
        entries(&data)?.iter().map(gc_entry).collect()
    }

    async fn finish_gc(&self, entry: &BlobGcEntry) -> DomainResult<()> {
        self.client()
            .call_unit(
                "finish_gc",
                json!({
                    "blob_backend": entry.blob_backend,
                    "blob_ref": entry.blob_ref,
                }),
            )
            .await
    }

    async fn fail_gc(
        &self,
        entry: &BlobGcEntry,
        error: &str,
        retry_at: DateTime<Utc>,
    ) -> DomainResult<()> {
        self.client()
            .call_unit(
                "fail_gc",
                json!({
                    "blob_backend": entry.blob_backend,
                    "blob_ref": entry.blob_ref,
                    "error": error,
                    "retry_at": decode::timestamp(retry_at),
                }),
            )
            .await
    }

    async fn claim_expired_uploads(
        &self,
        now: DateTime<Utc>,
        limit: u32,
    ) -> DomainResult<Vec<UploadRecord>> {
        let data = self
            .client()
            .call(
                "claim_expired_uploads",
                json!({"now": decode::timestamp(now), "limit": limit}),
            )
            .await?;
        entries(&data)?.iter().map(decode::upload_record).collect()
    }

    async fn purge_idempotency_records(&self, now: DateTime<Utc>) -> DomainResult<u64> {
        let data = self
            .client()
            .call(
                "purge_idempotency_records",
                json!({"now": decode::timestamp(now)}),
            )
            .await?;
        count(&data)
    }

    async fn purge_change_cursors(&self, now: DateTime<Utc>) -> DomainResult<u64> {
        let data = self
            .client()
            .call(
                "purge_change_cursors",
                json!({"now": decode::timestamp(now)}),
            )
            .await?;
        count(&data)
    }

    async fn purge_changes(&self, older_than: DateTime<Utc>) -> DomainResult<u64> {
        let data = self
            .client()
            .call(
                "purge_changes",
                json!({"older_than": decode::timestamp(older_than)}),
            )
            .await?;
        count(&data)
    }

    async fn count_pending_gc(&self) -> DomainResult<u64> {
        let data = self.client().call("count_pending_gc", json!({})).await?;
        count(&data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gc_entries_decode_their_attempt_counter() {
        let entry = gc_entry(&json!({
            "blob_backend": "tencent_cos",
            "blob_ref": "blobs/upload_1",
            "attempts": 3
        }))
        .unwrap();
        assert_eq!(entry.attempts, 3);
        assert_eq!(entry.blob_ref, "blobs/upload_1");
    }

    #[test]
    fn counts_and_lists_reject_the_wrong_shape() {
        assert_eq!(count(&json!(7)).unwrap(), 7);
        assert_eq!(count(&json!("7")).unwrap_err().code(), "internal_error");
        assert!(entries(&json!([])).unwrap().is_empty());
        assert_eq!(entries(&json!({})).unwrap_err().code(), "internal_error");
    }
}
