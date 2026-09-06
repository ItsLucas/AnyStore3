//! Idempotency orchestration.
//!
//! A completed retry is served here, *before* any validation or `If-Match`
//! evaluation, exactly as the API contract requires.

use anystore_domain::error::{DomainError, DomainResult};
use anystore_metastore::idempotency::{
    IdempotencyAcquire, IdempotencyComplete, IdempotencyContext, IdempotencyDecision,
    IdempotencyRelease,
};
use anystore_metastore::response::StoredResponse;
use sha2::{Digest, Sha256};
use std::future::Future;

use crate::context::RequestContext;
use crate::metrics::Metrics;
use crate::state::AppState;

fn owner_token() -> String {
    ulid::Ulid::generate().to_string()
}

pub fn idempotency_context(ctx: &RequestContext) -> Option<IdempotencyContext> {
    ctx.idempotency_key.as_ref().map(|key| IdempotencyContext {
        principal_id: ctx.principal_id.clone(),
        operation: ctx.operation.clone(),
        key: key.clone(),
        request_hash: ctx.request_hash.clone(),
        owner_token: owner_token(),
        resource_token: ctx.now.timestamp_micros().to_string(),
    })
}

/// Derives a stable resource id from the idempotency scope.
///
/// Lets a retry after a crash resume the same upload session instead of
/// creating a second one.
pub fn derived_id(prefix: &str, ctx: &IdempotencyContext) -> String {
    let mut hasher = Sha256::new();
    hasher.update(ctx.principal_id.as_str().as_bytes());
    hasher.update(b"\0");
    hasher.update(ctx.operation.as_bytes());
    hasher.update(b"\0");
    hasher.update(ctx.key.as_str().as_bytes());
    hasher.update(b"\0");
    hasher.update(ctx.request_hash.as_bytes());
    hasher.update(b"\0");
    hasher.update(ctx.resource_token.as_bytes());
    format!("{prefix}{}", hex::encode(&hasher.finalize()[..16]))
}

/// Outcome of claiming an idempotency key.
pub enum Claim {
    /// No key supplied, or the key is now owned by this attempt.
    Proceed(Option<IdempotencyContext>),
    /// The operation already completed; return this response verbatim.
    Replay(StoredResponse),
}

pub async fn claim(state: &AppState, ctx: &RequestContext) -> DomainResult<Claim> {
    let Some(mut ictx) = idempotency_context(ctx) else {
        return Ok(Claim::Proceed(None));
    };

    let decision = state
        .meta
        .acquire(IdempotencyAcquire {
            context: ictx.clone(),
            now: ctx.now,
            lease_until: ctx.now + state.config.idempotency_lease,
            expires_at: ctx.now + state.config.idempotency_retention,
        })
        .await?;

    match decision {
        IdempotencyDecision::Owner { resource_token } => {
            ictx.resource_token = resource_token;
            Ok(Claim::Proceed(Some(ictx)))
        }
        IdempotencyDecision::Replay(response) => {
            Metrics::incr(&state.metrics.idempotency_hits_total);
            Ok(Claim::Replay(response))
        }
        IdempotencyDecision::Conflict => {
            Metrics::incr(&state.metrics.idempotency_conflicts_total);
            Err(DomainError::IdempotencyKeyReused)
        }
    }
}

/// Runs an operation that finalises its own idempotency record, typically
/// inside a mutation transaction.
pub async fn run<F, Fut>(
    state: &AppState,
    ctx: &RequestContext,
    operation: F,
) -> DomainResult<StoredResponse>
where
    F: FnOnce(Option<IdempotencyContext>) -> Fut,
    Fut: Future<Output = DomainResult<StoredResponse>>,
{
    let claimed = claim(state, ctx).await?;
    let ictx = match claimed {
        Claim::Replay(response) => return Ok(response),
        Claim::Proceed(ictx) => ictx,
    };

    match operation(ictx.clone()).await {
        Ok(response) => Ok(response),
        Err(error) => {
            // Releasing the claim lets the caller retry the same key rather
            // than being permanently blocked by a failed attempt.
            if let Some(ictx) = ictx {
                let _ = state
                    .meta
                    .fail_or_release(IdempotencyRelease { context: ictx })
                    .await;
            }
            Err(error)
        }
    }
}

/// Runs an operation that has no object mutation to piggyback on, finalising
/// the idempotency record separately once the operation succeeds.
pub async fn run_with_completion<F, Fut>(
    state: &AppState,
    ctx: &RequestContext,
    operation: F,
) -> DomainResult<StoredResponse>
where
    F: FnOnce(Option<IdempotencyContext>) -> Fut,
    Fut: Future<Output = DomainResult<StoredResponse>>,
{
    let claimed = claim(state, ctx).await?;
    let ictx = match claimed {
        Claim::Replay(response) => return Ok(response),
        Claim::Proceed(ictx) => ictx,
    };

    match operation(ictx.clone()).await {
        Ok(response) => {
            if let Some(ictx) = ictx {
                state
                    .meta
                    .complete(IdempotencyComplete {
                        context: ictx,
                        response: response.clone(),
                        now: ctx.now,
                        expires_at: ctx.now + state.config.idempotency_retention,
                    })
                    .await?;
            }
            Ok(response)
        }
        Err(error) => {
            if let Some(ictx) = ictx {
                let _ = state
                    .meta
                    .fail_or_release(IdempotencyRelease { context: ictx })
                    .await;
            }
            Err(error)
        }
    }
}
