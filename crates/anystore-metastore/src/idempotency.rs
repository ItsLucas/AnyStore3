//! Idempotency port.

use anystore_domain::error::DomainResult;
use anystore_domain::{IdempotencyKey, PrincipalId};
use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::response::StoredResponse;

/// Identifies an idempotent operation attempt.
///
/// Key scope is `principal + operation`, per the API contract.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IdempotencyContext {
    pub principal_id: PrincipalId,
    /// Route template, e.g. `POST /objects` or `POST /uploads/{id}/complete`.
    pub operation: String,
    pub key: IdempotencyKey,
    /// SHA-256 of the canonical request representation.
    pub request_hash: String,
    /// Identifies this attempt so a crashed owner's lease can be taken over.
    pub owner_token: String,
    /// Stable for the lifetime of one retained idempotency record. Resource ids
    /// derived from it survive lease takeover but change after retention purge.
    pub resource_token: String,
}

#[derive(Clone, Debug)]
pub struct IdempotencyAcquire {
    pub context: IdempotencyContext,
    pub now: DateTime<Utc>,
    pub lease_until: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub enum IdempotencyDecision {
    /// The caller owns the key and must execute the operation.
    Owner { resource_token: String },
    /// The operation already completed; return this response verbatim.
    Replay(StoredResponse),
    /// The key was used with a different request.
    Conflict,
}

#[derive(Clone, Debug)]
pub struct IdempotencyComplete {
    pub context: IdempotencyContext,
    pub response: StoredResponse,
    pub now: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct IdempotencyRelease {
    pub context: IdempotencyContext,
}

#[async_trait]
pub trait IdempotencyStore: Send + Sync {
    /// Claims the key, replays a completed result, or reports a conflict.
    async fn acquire(&self, req: IdempotencyAcquire) -> DomainResult<IdempotencyDecision>;

    /// Finalises a key outside a mutation transaction. Mutation paths finalise
    /// inside their own transaction instead.
    async fn complete(&self, req: IdempotencyComplete) -> DomainResult<()>;

    /// Releases a claimed but unfinished key so the operation can be retried.
    async fn fail_or_release(&self, req: IdempotencyRelease) -> DomainResult<()>;
}
