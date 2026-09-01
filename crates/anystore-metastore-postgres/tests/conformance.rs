//! Runs the shared `MetaStore` conformance suite against PostgreSQL.
//!
//! Skipped unless `ANYSTORE_TEST_DATABASE_URL` is set, so `cargo test` works
//! without a database. Running the identical suite here and against
//! `InMemoryMetaStore` is what proves the port abstraction holds.

use anystore_metastore_postgres::{PoolConfig, PostgresMetaStore};
use anystore_testkit::conformance::metastore::{self, MetaStoreFactory};
use async_trait::async_trait;
use std::sync::Arc;

struct PostgresFactory {
    store: Arc<PostgresMetaStore>,
}

impl PostgresFactory {
    async fn connect(url: &str) -> Self {
        let store = PostgresMetaStore::connect(url, PoolConfig::default())
            .await
            .expect("connect");
        store.migrate().await.expect("migrate");
        Self {
            store: Arc::new(store),
        }
    }
}

#[async_trait]
impl MetaStoreFactory for PostgresFactory {
    type Store = PostgresMetaStore;

    async fn create(&self) -> Arc<Self::Store> {
        // Each case starts from a pristine tree.
        sqlx::raw_sql(
            "TRUNCATE objects, uploads, idempotency_records, changes, change_cursors,
                      blob_gc_queue RESTART IDENTITY CASCADE;
             ALTER SEQUENCE changes_seq RESTART;
             UPDATE changes_retention SET purged_through_seq = 0;
             INSERT INTO objects (id, revision, kind, name, parent_id, metadata,
                                  created_at, updated_at)
             VALUES ('root', 1, 'folder', '', NULL, '{}'::jsonb, now(), now());",
        )
        .execute(self.store.pool())
        .await
        .expect("reset");

        Arc::clone(&self.store)
    }
}

#[tokio::test]
async fn postgres_metastore_is_conformant() {
    let Ok(url) = std::env::var("ANYSTORE_TEST_DATABASE_URL") else {
        eprintln!("skipping: ANYSTORE_TEST_DATABASE_URL is not set");
        return;
    };

    let factory = PostgresFactory::connect(&url).await;
    metastore::run_all(&factory).await;
}
