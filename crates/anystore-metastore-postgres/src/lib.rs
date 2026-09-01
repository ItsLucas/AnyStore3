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
#[derive(Clone, Debug)]
pub struct PoolConfig {
    pub min_connections: u32,
    pub max_connections: u32,
    pub acquire_timeout: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            min_connections: 0,
            max_connections: 5,
            acquire_timeout: Duration::from_secs(10),
        }
    }
}

impl PostgresMetaStore {
    pub async fn connect(url: &str, config: PoolConfig) -> DomainResult<Self> {
        let pool = PgPoolOptions::new()
            .min_connections(config.min_connections)
            .max_connections(config.max_connections)
            .acquire_timeout(config.acquire_timeout)
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
        sqlx::migrate!("../../migrations")
            .run(&self.pool)
            .await
            .map_err(|e| DomainError::internal(format!("migration failed: {e}")))
    }

    /// Verifies the schema is present before serving traffic.
    pub async fn check_schema(&self) -> DomainResult<()> {
        let exists: bool =
            sqlx::query_scalar("SELECT to_regclass('public.objects') IS NOT NULL")
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
