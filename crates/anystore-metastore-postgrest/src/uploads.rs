//! Upload session persistence.

use anystore_domain::UploadId;
use anystore_domain::error::DomainResult;
use anystore_domain::upload::UploadRecord;
use anystore_metastore::UploadRepository;
use anystore_metastore::uploads::{
    AbortUploadRecord, CreateUploadRecord, CreateUploadResult, UpdateUploadState,
};
use async_trait::async_trait;
use serde_json::json;

use crate::PostgrestMetaStore;
use crate::decode;

#[async_trait]
impl UploadRepository for PostgrestMetaStore {
    async fn create_upload_record(
        &self,
        cmd: CreateUploadRecord,
    ) -> DomainResult<CreateUploadResult> {
        // Conflict-safe insert: a retry after a crash resumes the same session
        // instead of creating a second one.
        let data = self
            .client()
            .call(
                "create_upload_record",
                json!({
                    "id": cmd.id.as_str(),
                    "object_id": cmd.object_id.as_str(),
                    "mode": cmd.mode.as_str(),
                    "blob_backend": cmd.blob_backend,
                    "blob_ref": cmd.blob_ref,
                    "expected_size": cmd.expected_size,
                    "content_type": cmd.content_type,
                    "expected_sha256": cmd.expected_sha256,
                    "created_at": decode::timestamp(cmd.created_at),
                    "expires_at": decode::timestamp(cmd.expires_at),
                }),
            )
            .await?;

        Ok(CreateUploadResult {
            upload: decode::upload_record(data.get("upload").ok_or_else(|| {
                anystore_domain::error::DomainError::internal(
                    "rpc create_upload_record returned no upload",
                )
            })?)?,
            created: decode::boolean(&data, "created")?,
        })
    }

    async fn get_upload(&self, id: &UploadId) -> DomainResult<Option<UploadRecord>> {
        let data = self
            .client()
            .call("get_upload", json!({"id": id.as_str()}))
            .await?;
        decode::optional_upload_record(&data)
    }

    async fn update_upload_state(&self, cmd: UpdateUploadState) -> DomainResult<()> {
        self.client()
            .call_unit(
                "update_upload_state",
                json!({
                    "id": cmd.id.as_str(),
                    "state": cmd.state.as_str(),
                    "provider_upload_id": cmd.provider_upload_id,
                    "provider_completed": cmd.provider_completed,
                }),
            )
            .await
    }

    async fn abort_upload_record(&self, cmd: AbortUploadRecord) -> DomainResult<()> {
        self.client()
            .call_unit(
                "abort_upload_record",
                json!({
                    "id": cmd.id.as_str(),
                    "now": decode::timestamp(cmd.now),
                    "gc_not_before": decode::timestamp(cmd.gc_not_before),
                }),
            )
            .await
    }
}
