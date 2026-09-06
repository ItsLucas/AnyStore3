//! Transactional object mutations.
//!
//! A `ResponseRenderer` closure cannot cross HTTP, so the database function
//! renders the operation's public response and commits it with the mutation,
//! the Change records and the idempotency record in one transaction. The bytes
//! returned here are the bytes that were persisted, which is what keeps an
//! idempotent replay byte-identical.

use anystore_domain::error::DomainResult;
use anystore_metastore::ObjectMutationStore;
use anystore_metastore::commands::{
    CommitContent, CreateObjectCommit, DeleteObjectCommit, MutationContext, PatchObjectCommit,
};
use anystore_metastore::idempotency::IdempotencyContext;
use anystore_metastore::response::StoredResponse;
use async_trait::async_trait;
use serde_json::{Value, json};

use crate::PostgrestMetaStore;
use crate::decode;

pub(crate) fn idempotency_context(context: &IdempotencyContext) -> Value {
    json!({
        "principal_id": context.principal_id.as_str(),
        "operation": context.operation,
        "key": context.key.as_str(),
        "request_hash": context.request_hash,
        "owner_token": context.owner_token,
        "resource_token": context.resource_token,
    })
}

fn mutation_context(ctx: &MutationContext) -> Value {
    json!({
        "now": decode::timestamp(ctx.now),
        "request_id": ctx.request_id.as_str(),
        "idempotency_key": ctx.idempotency_key.as_ref().map(|key| key.as_str()),
        "idempotency": ctx.idempotency.as_ref().map(idempotency_context),
        "idempotency_expires_at": decode::timestamp(ctx.idempotency_expires_at),
    })
}

async fn mutate(
    store: &PostgrestMetaStore,
    op: &str,
    payload: Value,
) -> DomainResult<StoredResponse> {
    let data = store.client().call(op, payload).await?;
    decode::stored_response(data.get("response").ok_or_else(|| {
        anystore_domain::error::DomainError::internal("rpc mutation returned no response")
    })?)
}

#[async_trait]
impl ObjectMutationStore for PostgrestMetaStore {
    async fn create_object(&self, cmd: CreateObjectCommit) -> DomainResult<StoredResponse> {
        mutate(
            self,
            "create_object",
            json!({
                "id": cmd.id.as_str(),
                "kind": cmd.kind.as_str(),
                "name": cmd.name,
                "parent_id": cmd.parent_id.as_str(),
                "content_type": cmd.content_type,
                "metadata": cmd.metadata,
                "ctx": mutation_context(&cmd.ctx),
            }),
        )
        .await
    }

    async fn patch_object(&self, cmd: PatchObjectCommit) -> DomainResult<StoredResponse> {
        mutate(
            self,
            "patch_object",
            json!({
                "id": cmd.id.as_str(),
                "if_match": cmd.if_match.map(|revision| revision.get()),
                "new_name": cmd.new_name,
                "new_parent_id": cmd.new_parent_id.as_ref().map(|id| id.as_str()),
                "metadata_patch": cmd.metadata_patch.as_ref().map(|patch| {
                    json!({"set": patch.set, "remove": patch.remove})
                }),
                "ctx": mutation_context(&cmd.ctx),
            }),
        )
        .await
    }

    async fn delete_object(&self, cmd: DeleteObjectCommit) -> DomainResult<StoredResponse> {
        mutate(
            self,
            "delete_object",
            json!({
                "id": cmd.id.as_str(),
                "if_match": cmd.if_match.map(|revision| revision.get()),
                "recursive": cmd.recursive,
                "ctx": mutation_context(&cmd.ctx),
            }),
        )
        .await
    }

    async fn commit_content(&self, cmd: CommitContent) -> DomainResult<StoredResponse> {
        mutate(
            self,
            "commit_content",
            json!({
                "upload_id": cmd.upload_id.as_str(),
                "object_id": cmd.object_id.as_str(),
                "if_match": cmd.if_match.map(|revision| revision.get()),
                "blob_backend": cmd.blob_backend,
                "blob_ref": cmd.blob_ref,
                "size": cmd.size,
                "content_type": cmd.content_type,
                "sha256": cmd.sha256,
                "gc_not_before": decode::timestamp(cmd.gc_not_before),
                "ctx": mutation_context(&cmd.ctx),
            }),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anystore_domain::metadata::MetadataPatch;
    use anystore_domain::object::{ObjectKind, Revision};
    use anystore_domain::{IdempotencyKey, ObjectId, PrincipalId, RequestId};
    use anystore_metastore::response::StoredResponse;
    use chrono::{Duration, TimeZone, Utc};
    use std::sync::Arc;

    fn context(with_key: bool) -> MutationContext {
        let now = Utc.with_ymd_and_hms(2026, 9, 1, 10, 0, 0).unwrap();
        MutationContext {
            now,
            request_id: RequestId::new("req_1"),
            idempotency_key: with_key.then(|| IdempotencyKey::new("key-1")),
            idempotency: with_key.then(|| IdempotencyContext {
                principal_id: PrincipalId::local(),
                operation: "POST /objects".into(),
                key: IdempotencyKey::new("key-1"),
                request_hash: "hash".into(),
                owner_token: "owner".into(),
                resource_token: "token".into(),
            }),
            idempotency_expires_at: now + Duration::hours(24),
            render: Arc::new(|_| Ok(StoredResponse::no_content())),
        }
    }

    #[test]
    fn a_request_without_a_key_sends_no_idempotency_context() {
        let value = mutation_context(&context(false));
        assert_eq!(value["now"], json!("2026-09-01T10:00:00.000000Z"));
        assert_eq!(value["request_id"], json!("req_1"));
        assert_eq!(value["idempotency_key"], Value::Null);
        assert_eq!(value["idempotency"], Value::Null);
        assert_eq!(
            value["idempotency_expires_at"],
            json!("2026-09-02T10:00:00.000000Z")
        );
    }

    #[test]
    fn an_idempotent_request_sends_the_whole_scope() {
        let value = mutation_context(&context(true));
        assert_eq!(
            value["idempotency"],
            json!({
                "principal_id": "local",
                "operation": "POST /objects",
                "key": "key-1",
                "request_hash": "hash",
                "owner_token": "owner",
                "resource_token": "token"
            })
        );
    }

    #[test]
    fn a_patch_distinguishes_absent_fields_from_values() {
        let empty = PatchObjectCommit {
            id: ObjectId::new("obj_1"),
            if_match: None,
            new_name: None,
            new_parent_id: None,
            metadata_patch: None,
            ctx: context(false),
        };
        let payload = json!({
            "id": empty.id.as_str(),
            "if_match": empty.if_match.map(|r| r.get()),
            "new_name": empty.new_name,
            "new_parent_id": empty.new_parent_id.as_ref().map(|id| id.as_str()),
            "metadata_patch": empty.metadata_patch.as_ref().map(|patch| {
                json!({"set": patch.set, "remove": patch.remove})
            }),
        });
        assert_eq!(payload["if_match"], Value::Null);
        assert_eq!(payload["new_name"], Value::Null);
        assert_eq!(payload["new_parent_id"], Value::Null);
        assert_eq!(payload["metadata_patch"], Value::Null);

        let patch = MetadataPatch {
            set: json!({"a": 1}).as_object().unwrap().clone(),
            remove: vec!["b".into()],
        };
        let filled = json!({
            "if_match": Some(Revision(4)).map(|r| r.get()),
            "new_name": Some("renamed".to_owned()),
            "new_parent_id": Some(ObjectId::root()).as_ref().map(|id| id.as_str()),
            "metadata_patch": json!({"set": patch.set, "remove": patch.remove}),
            "kind": ObjectKind::File.as_str(),
        });
        assert_eq!(filled["if_match"], json!(4));
        assert_eq!(filled["new_name"], json!("renamed"));
        assert_eq!(filled["new_parent_id"], json!("root"));
        assert_eq!(
            filled["metadata_patch"],
            json!({"set": {"a": 1}, "remove": ["b"]})
        );
        assert_eq!(filled["kind"], json!("file"));
    }
}
