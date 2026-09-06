//! Idempotency records.
//!
//! Claiming is its own request so concurrent callers observe it immediately.
//! Finalisation for a mutation happens inside the mutation's own RPC call, so
//! the record and the mutation always commit together.

use anystore_domain::error::{DomainError, DomainResult};
use anystore_metastore::idempotency::{
    IdempotencyAcquire, IdempotencyComplete, IdempotencyDecision, IdempotencyRelease,
    IdempotencyStore,
};
use async_trait::async_trait;
use serde_json::json;
use std::time::Duration;

use crate::PostgrestMetaStore;
use crate::decode;
use crate::objects_write::idempotency_context;

/// How long to wait for a concurrent owner to finish before giving up.
const CONTENTION_POLLS: usize = 10;
const CONTENTION_INTERVAL: Duration = Duration::from_millis(200);

#[async_trait]
impl IdempotencyStore for PostgrestMetaStore {
    async fn acquire(&self, req: IdempotencyAcquire) -> DomainResult<IdempotencyDecision> {
        let payload = json!({
            "context": idempotency_context(&req.context),
            "now": decode::timestamp(req.now),
            "lease_until": decode::timestamp(req.lease_until),
            "expires_at": decode::timestamp(req.expires_at),
        });

        for _ in 0..CONTENTION_POLLS {
            let data = self
                .client()
                .call("idempotency_acquire", payload.clone())
                .await?;

            match decode::text(&data, "decision")? {
                "owner" => {
                    return Ok(IdempotencyDecision::Owner {
                        resource_token: decode::text(&data, "resource_token")?.to_owned(),
                    });
                }
                "replay" => {
                    let response = data.get("response").ok_or_else(|| {
                        DomainError::internal("rpc replay decision carries no response")
                    })?;
                    return Ok(IdempotencyDecision::Replay(decode::stored_response(
                        response,
                    )?));
                }
                "conflict" => return Ok(IdempotencyDecision::Conflict),
                // The record vanished between the insert and the read.
                "retry" => continue,
                "busy" => tokio::time::sleep(CONTENTION_INTERVAL).await,
                other => {
                    return Err(DomainError::internal(format!(
                        "unknown idempotency decision {other:?}"
                    )));
                }
            }
        }

        // Another attempt is still running. The client should retry with the
        // same key rather than receive a speculative result.
        Err(DomainError::RateLimited)
    }

    async fn complete(&self, req: IdempotencyComplete) -> DomainResult<()> {
        self.client()
            .call_unit(
                "idempotency_complete",
                json!({
                    "context": idempotency_context(&req.context),
                    "response": decode::encode_response(&req.response),
                    "now": decode::timestamp(req.now),
                    "expires_at": decode::timestamp(req.expires_at),
                }),
            )
            .await
    }

    async fn fail_or_release(&self, req: IdempotencyRelease) -> DomainResult<()> {
        self.client()
            .call_unit(
                "idempotency_release",
                json!({"context": idempotency_context(&req.context)}),
            )
            .await
    }
}
