//! Idempotency records.
//!
//! `acquire` runs in its own transaction so concurrent callers observe the
//! claim immediately. Finalisation, in contrast, happens inside the mutation
//! transaction via [`finalize_in_tx`].

use anystore_domain::error::{DomainError, DomainResult};
use anystore_metastore::idempotency::{
    IdempotencyAcquire, IdempotencyComplete, IdempotencyContext, IdempotencyDecision,
    IdempotencyRelease, IdempotencyStore,
};
use anystore_metastore::response::StoredResponse;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{PgConnection, PgExecutor, Row, postgres::PgRow};
use std::time::Duration;

use crate::PostgresMetaStore;
use crate::errors::map_sqlx;

/// How long to wait for a concurrent owner to finish before giving up.
const CONTENTION_POLLS: usize = 10;
const CONTENTION_INTERVAL: Duration = Duration::from_millis(200);

const INSERT_CLAIM: &str = "
INSERT INTO idempotency_records (
    principal_id, operation, idempotency_key, request_hash,
    state, owner_token, lease_until, created_at, expires_at)
VALUES ($1, $2, $3, $4, 'in_progress', $5, $6, $7, $8)
ON CONFLICT (principal_id, operation, idempotency_key) DO NOTHING
";

const SELECT_RECORD: &str = "
SELECT request_hash, state, owner_token, lease_until, status_code, response_headers, response_body
FROM idempotency_records
WHERE principal_id = $1 AND operation = $2 AND idempotency_key = $3
";

/// Takes over a record whose owner's lease has expired, which prevents a
/// crashed serverless instance from blocking a key forever.
const STEAL_LEASE: &str = "
UPDATE idempotency_records
SET owner_token = $4, lease_until = $5, request_hash = $6
WHERE principal_id = $1 AND operation = $2 AND idempotency_key = $3
  AND state = 'in_progress'
  AND lease_until <= $7
";

const FINALIZE: &str = "
UPDATE idempotency_records
SET state = 'completed',
    status_code = $4,
    response_headers = $5,
    response_body = $6,
    completed_at = $7,
    expires_at = $8,
    owner_token = NULL,
    lease_until = NULL
WHERE principal_id = $1 AND operation = $2 AND idempotency_key = $3
  AND state = 'in_progress'
  AND owner_token = $9
";

fn encode_headers(headers: &[(String, String)]) -> Value {
    Value::Array(
        headers
            .iter()
            .map(|(k, v)| Value::Array(vec![Value::String(k.clone()), Value::String(v.clone())]))
            .collect(),
    )
}

fn decode_headers(value: Option<Value>) -> Vec<(String, String)> {
    let Some(Value::Array(items)) = value else {
        return Vec::new();
    };
    items
        .into_iter()
        .filter_map(|item| match item {
            Value::Array(pair) if pair.len() == 2 => {
                let k = pair[0].as_str()?.to_owned();
                let v = pair[1].as_str()?.to_owned();
                Some((k, v))
            }
            _ => None,
        })
        .collect()
}

fn decode_stored_response(row: &PgRow) -> DomainResult<StoredResponse> {
    let status: Option<i32> = row.try_get("status_code").map_err(map_sqlx)?;
    let headers: Option<Value> = row.try_get("response_headers").map_err(map_sqlx)?;
    let body: Option<Vec<u8>> = row.try_get("response_body").map_err(map_sqlx)?;
    Ok(StoredResponse {
        status: status.unwrap_or(200) as u16,
        headers: decode_headers(headers),
        body: body.unwrap_or_default(),
    })
}

/// Finalises the idempotency record inside an ongoing mutation transaction.
///
/// Committing the record with the mutation is what guarantees a retry can never
/// produce a second object, revision or Change.
pub(crate) async fn finalize_in_tx(
    tx: &mut PgConnection,
    context: Option<&IdempotencyContext>,
    response: &StoredResponse,
    now: DateTime<Utc>,
    expires_at: DateTime<Utc>,
) -> DomainResult<()> {
    let Some(ctx) = context else {
        return Ok(());
    };

    let result = sqlx::query(FINALIZE)
        .bind(ctx.principal_id.as_str())
        .bind(&ctx.operation)
        .bind(ctx.key.as_str())
        .bind(i32::from(response.status))
        .bind(encode_headers(&response.headers))
        .bind(&response.body)
        .bind(now)
        .bind(expires_at)
        .bind(&ctx.owner_token)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx)?;

    if result.rows_affected() != 1 {
        // The lease was taken over by another attempt; abandoning the
        // transaction is the only way to keep the operation exactly-once.
        return Err(DomainError::internal(
            "idempotency lease was lost during the mutation",
        ));
    }
    Ok(())
}

async fn read_record<'e, E>(executor: E, ctx: &IdempotencyContext) -> DomainResult<Option<PgRow>>
where
    E: PgExecutor<'e>,
{
    sqlx::query(SELECT_RECORD)
        .bind(ctx.principal_id.as_str())
        .bind(&ctx.operation)
        .bind(ctx.key.as_str())
        .fetch_optional(executor)
        .await
        .map_err(map_sqlx)
}

#[async_trait]
impl IdempotencyStore for PostgresMetaStore {
    async fn acquire(&self, req: IdempotencyAcquire) -> DomainResult<IdempotencyDecision> {
        let ctx = &req.context;

        for _ in 0..CONTENTION_POLLS {
            let inserted = sqlx::query(INSERT_CLAIM)
                .bind(ctx.principal_id.as_str())
                .bind(&ctx.operation)
                .bind(ctx.key.as_str())
                .bind(&ctx.request_hash)
                .bind(&ctx.owner_token)
                .bind(req.lease_until)
                .bind(req.now)
                .bind(req.expires_at)
                .execute(self.pool())
                .await
                .map_err(map_sqlx)?;

            if inserted.rows_affected() == 1 {
                return Ok(IdempotencyDecision::Owner);
            }

            let Some(row) = read_record(self.pool(), ctx).await? else {
                // Record vanished between the insert and the read; try again.
                continue;
            };

            let request_hash: String = row.try_get("request_hash").map_err(map_sqlx)?;
            if request_hash != ctx.request_hash {
                return Ok(IdempotencyDecision::Conflict);
            }

            let state: String = row.try_get("state").map_err(map_sqlx)?;
            if state == "completed" {
                return Ok(IdempotencyDecision::Replay(decode_stored_response(&row)?));
            }

            let lease_until: Option<DateTime<Utc>> =
                row.try_get("lease_until").map_err(map_sqlx)?;
            let lease_expired = lease_until.is_none_or(|until| until <= Utc::now());

            if lease_expired {
                let stolen = sqlx::query(STEAL_LEASE)
                    .bind(ctx.principal_id.as_str())
                    .bind(&ctx.operation)
                    .bind(ctx.key.as_str())
                    .bind(&ctx.owner_token)
                    .bind(req.lease_until)
                    .bind(&ctx.request_hash)
                    .bind(Utc::now())
                    .execute(self.pool())
                    .await
                    .map_err(map_sqlx)?;
                if stolen.rows_affected() == 1 {
                    return Ok(IdempotencyDecision::Owner);
                }
            }

            tokio::time::sleep(CONTENTION_INTERVAL).await;
        }

        // Another attempt is still running. The client should retry with the
        // same key rather than receive a speculative result.
        Err(DomainError::RateLimited)
    }

    async fn complete(&self, req: IdempotencyComplete) -> DomainResult<()> {
        let mut conn = self.pool().acquire().await.map_err(map_sqlx)?;
        finalize_in_tx(
            &mut conn,
            Some(&req.context),
            &req.response,
            req.now,
            req.expires_at,
        )
        .await
    }

    async fn fail_or_release(&self, req: IdempotencyRelease) -> DomainResult<()> {
        let ctx = req.context;
        sqlx::query(
            "DELETE FROM idempotency_records
             WHERE principal_id = $1 AND operation = $2 AND idempotency_key = $3
               AND state = 'in_progress' AND owner_token = $4",
        )
        .bind(ctx.principal_id.as_str())
        .bind(&ctx.operation)
        .bind(ctx.key.as_str())
        .bind(&ctx.owner_token)
        .execute(self.pool())
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }
}
