//! Runs the shared `MetaStore` conformance suite against PostgreSQL.
//!
//! Skipped unless `ANYSTORE_TEST_DATABASE_URL` is set, so `cargo test` works
//! without a database. Running the identical suite here and against
//! `InMemoryMetaStore` is what proves the port abstraction holds.

use anystore_domain::change::ChangeAction;
use anystore_domain::object::ObjectKind;
use anystore_domain::{ChangeId, ObjectId, RequestId};
use anystore_metastore::ObjectMutationStore;
use anystore_metastore::changes::{ChangeStore, ReadChanges};
use anystore_metastore::commands::{CreateObjectCommit, MutationContext};
use anystore_metastore::response::StoredResponse;
use anystore_metastore_postgres::{PoolConfig, PostgresMetaStore};
use anystore_testkit::conformance::metastore::{self, MetaStoreFactory};
use async_trait::async_trait;
use chrono::{Duration, Utc};
use serde_json::json;
use std::sync::Arc;

static POSTGRES_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Serialises against the PostgREST conformance suite, which truncates the same
/// tables from a separate test binary.
async fn lock_database(url: &str) -> sqlx::PgConnection {
    use sqlx::Connection as _;
    let mut connection = sqlx::PgConnection::connect(url)
        .await
        .expect("lock connection");
    sqlx::query("SELECT pg_advisory_lock(hashtext('anystore:test:conformance')::bigint)")
        .execute(&mut connection)
        .await
        .expect("advisory lock");
    connection
}

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
    let _guard = POSTGRES_TEST_LOCK.lock().await;
    let _database = lock_database(&url).await;

    let factory = PostgresFactory::connect(&url).await;
    metastore::run_all(&factory).await;
}

#[tokio::test]
async fn change_sequences_cannot_commit_out_of_order() {
    let Ok(url) = std::env::var("ANYSTORE_TEST_DATABASE_URL") else {
        eprintln!("skipping: ANYSTORE_TEST_DATABASE_URL is not set");
        return;
    };
    let _guard = POSTGRES_TEST_LOCK.lock().await;
    let _database = lock_database(&url).await;

    let factory = PostgresFactory::connect(&url).await;
    let store = factory.create().await;
    let mut stalled = store.pool().begin().await.expect("begin stalled tx");
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext('anystore:changes:append')::bigint)")
        .execute(&mut *stalled)
        .await
        .expect("take ordering lock");
    let seq: i64 = sqlx::query_scalar("SELECT nextval('changes_seq')")
        .fetch_one(&mut *stalled)
        .await
        .expect("reserve stalled sequence");
    let stalled_id = ObjectId::new("obj_stalled_change");
    sqlx::query(
        "INSERT INTO changes (
             seq, change_id, object_id, revision, action, changed_at,
             request_id, idempotency_key, tombstone)
         VALUES ($1, $2, $3, 1, 'created', $4, $5, NULL, FALSE)",
    )
    .bind(seq)
    .bind(ChangeId::from_seq(seq).as_str())
    .bind(stalled_id.as_str())
    .bind(Utc::now())
    .bind(RequestId::generate().as_str())
    .execute(&mut *stalled)
    .await
    .expect("insert stalled Change");

    let concurrent_id = ObjectId::generate();
    let concurrent_store = Arc::clone(&store);
    let concurrent_object_id = concurrent_id.clone();
    let mutation = tokio::spawn(async move {
        let now = Utc::now();
        concurrent_store
            .create_object(CreateObjectCommit {
                id: concurrent_object_id,
                kind: ObjectKind::Folder,
                name: "after-stalled-change".into(),
                parent_id: ObjectId::root(),
                content_type: None,
                metadata: json!({}),
                ctx: MutationContext {
                    now,
                    request_id: RequestId::generate(),
                    idempotency_key: None,
                    idempotency: None,
                    idempotency_expires_at: now + Duration::hours(24),
                    render: Arc::new(|_| Ok(StoredResponse::no_content())),
                },
            })
            .await
    });

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(
        !mutation.is_finished(),
        "a later Change must wait until the earlier sequence commits"
    );
    stalled.commit().await.expect("commit stalled Change");
    mutation
        .await
        .expect("mutation task")
        .expect("concurrent mutation");

    let now = Utc::now();
    let page = store
        .read_changes(ReadChanges {
            cursor: None,
            limit: 100,
            now,
            cursor_expires_at: now + Duration::days(1),
        })
        .await
        .expect("read Changes");
    assert_eq!(page.items.len(), 2);
    assert_eq!(page.items[0].object_id, stalled_id);
    assert_eq!(page.items[0].action, ChangeAction::Created);
    assert_eq!(page.items[1].object_id, concurrent_id);
}
