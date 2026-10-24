//! PostgreSQL `MetaStore` adapter.
//!
//! All SQL for AnyStore lives in this crate. The application and HTTP layers
//! never see a query.

mod changes;
mod errors;
mod idempotency;
mod maintenance;
mod objects_read;
mod objects_write;
mod rows;
mod uploads;

pub use errors::map_sqlx;

use anystore_domain::error::{DomainError, DomainResult};
use sqlx::AssertSqlSafe;
use sqlx::postgres::{PgPool, PgPoolOptions};
use std::time::Duration;

/// Production `MetaStore` implementation backed by PostgreSQL.
#[derive(Clone)]
pub struct PostgresMetaStore {
    pool: PgPool,
}

/// Connection pool settings.
///
/// Serverless containers are short-lived and numerous, so the default pool is
/// deliberately small.
///
/// Every write serialises on one Change-order lock, so a stalled statement or
/// an abandoned transaction would block all writers. The session timeouts bound
/// both; lock waits give up first and surface as a retryable `429`.
#[derive(Clone, Debug)]
pub struct PoolConfig {
    pub min_connections: u32,
    pub max_connections: u32,
    pub acquire_timeout: Duration,
    pub statement_timeout: Duration,
    pub lock_timeout: Duration,
    pub idle_in_transaction_timeout: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            min_connections: 0,
            max_connections: 5,
            acquire_timeout: Duration::from_secs(10),
            statement_timeout: Duration::from_secs(15),
            lock_timeout: Duration::from_secs(5),
            idle_in_transaction_timeout: Duration::from_secs(30),
        }
    }
}

impl PostgresMetaStore {
    pub async fn connect(url: &str, config: PoolConfig) -> DomainResult<Self> {
        // Session settings rather than startup `options`, which some poolers
        // reject.
        let session = format!(
            "SET statement_timeout = {}; SET lock_timeout = {}; \
             SET idle_in_transaction_session_timeout = {}",
            config.statement_timeout.as_millis(),
            config.lock_timeout.as_millis(),
            config.idle_in_transaction_timeout.as_millis(),
        );
        let pool = PgPoolOptions::new()
            .min_connections(config.min_connections)
            .max_connections(config.max_connections)
            .acquire_timeout(config.acquire_timeout)
            .after_connect(move |conn, _meta| {
                let session = session.clone();
                Box::pin(async move {
                    sqlx::raw_sql(AssertSqlSafe(session)).execute(conn).await?;
                    Ok(())
                })
            })
            .connect(url)
            .await
            .map_err(map_sqlx)?;
        Ok(Self { pool })
    }

    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Applies pending migrations. Execution is an explicit deploy-time policy
    /// decision, so this is never called implicitly by request handling.
    pub async fn migrate(&self) -> DomainResult<()> {
        // Request-sized timeouts do not fit schema changes. The connection is
        // detached afterwards so the cleared setting never serves a request.
        let mut conn = self.pool.acquire().await.map_err(map_sqlx)?.detach();
        sqlx::raw_sql("SET statement_timeout = 0; SET lock_timeout = 0")
            .execute(&mut conn)
            .await
            .map_err(map_sqlx)?;
        sqlx::migrate!("../../migrations")
            .run(&mut conn)
            .await
            .map_err(|e| DomainError::internal(format!("migration failed: {e}")))
    }

    /// Verifies the schema is present before serving traffic.
    pub async fn check_schema(&self) -> DomainResult<()> {
        let exists: bool = sqlx::query_scalar("SELECT to_regclass('public.objects') IS NOT NULL")
            .fetch_one(&self.pool)
            .await
            .map_err(map_sqlx)?;
        if !exists {
            return Err(DomainError::internal(
                "database schema is not initialised; run migrations",
            ));
        }
        Ok(())
    }
}
