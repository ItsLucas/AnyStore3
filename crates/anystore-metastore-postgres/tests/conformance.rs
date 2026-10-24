//! Runs the shared `MetaStore` conformance suite against PostgreSQL.
//!
//! Skipped unless `ANYSTORE_TEST_DATABASE_URL` is set, so `cargo test` works
//! without a database. Running the identical suite here and against
//! `InMemoryMetaStore` is what proves the port abstraction holds.

use anystore_domain::change::ChangeAction;
use anystore_domain::error::DomainError;
use anystore_domain::object::ObjectKind;
use anystore_domain::{ChangeId, ObjectId, RequestId};
use anystore_metastore::changes::{ChangeStore, ReadChanges};
use anystore_metastore::commands::{CreateObjectCommit, MutationContext, PatchObjectCommit};
use anystore_metastore::response::StoredResponse;
use anystore_metastore::{ObjectMutationStore, ObjectRepository};
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

fn plain_context() -> MutationContext {
    let now = Utc::now();
    MutationContext {
        now,
        request_id: RequestId::generate(),
        idempotency_key: None,
        idempotency: None,
        idempotency_expires_at: now + Duration::hours(24),
        render: Arc::new(|_| Ok(StoredResponse::no_content())),
    }
}

async fn create_folder(store: &PostgresMetaStore, name: &str, parent: &ObjectId) -> ObjectId {
    let id = ObjectId::generate();
    store
        .create_object(CreateObjectCommit {
            id: id.clone(),
            kind: ObjectKind::Folder,
            name: name.into(),
            parent_id: parent.clone(),
            content_type: None,
            metadata: json!({}),
            ctx: plain_context(),
        })
        .await
        .expect("create folder");
    id
}

fn move_into(id: &ObjectId, destination: &ObjectId) -> PatchObjectCommit {
    PatchObjectCommit {
        id: id.clone(),
        if_match: None,
        new_name: None,
        new_parent_id: Some(destination.clone()),
        metadata_patch: None,
        ctx: plain_context(),
    }
}

/// Two moves that are each valid alone but together form a cycle: A into B's
/// subtree and B into A's. Both are held at the Change-order lock after their
/// unlocked ancestor walks, so without a locked re-check both would commit.
#[tokio::test]
async fn concurrent_folder_moves_cannot_create_a_cycle() {
    let Ok(url) = std::env::var("ANYSTORE_TEST_DATABASE_URL") else {
        eprintln!("skipping: ANYSTORE_TEST_DATABASE_URL is not set");
        return;
    };
    let _guard = POSTGRES_TEST_LOCK.lock().await;
    let _database = lock_database(&url).await;

    let factory = PostgresFactory::connect(&url).await;
    let store = factory.create().await;
    let root = ObjectId::root();
    let a = create_folder(&store, "a", &root).await;
    let d = create_folder(&store, "d", &a).await;
    let b = create_folder(&store, "b", &root).await;
    let c = create_folder(&store, "c", &b).await;

    let mut stalled = store.pool().begin().await.expect("begin stalled tx");
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext('anystore:changes:append')::bigint)")
        .execute(&mut *stalled)
        .await
        .expect("take ordering lock");

    let first = tokio::spawn({
        let store = Arc::clone(&store);
        let command = move_into(&a, &c);
        async move { store.patch_object(command).await }
    });
    let second = tokio::spawn({
        let store = Arc::clone(&store);
        let command = move_into(&b, &d);
        async move { store.patch_object(command).await }
    });

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(!first.is_finished() && !second.is_finished());
    stalled.commit().await.expect("release ordering lock");

    let results = [
        first.await.expect("first move task"),
        second.await.expect("second move task"),
    ];
    let succeeded = results.iter().filter(|r| r.is_ok()).count();
    let rejected = results
        .iter()
        .filter(|r| matches!(r, Err(DomainError::InvalidMove(_))))
        .count();
    assert_eq!(
        (succeeded, rejected),
        (1, 1),
        "exactly one move may win: {results:?}"
    );

    let a_path = store.get_object(&a).await.expect("get a").expect("a").path;
    let b_path = store.get_object(&b).await.expect("get b").expect("b").path;
    assert!(
        (a_path == "/b/c/a" && b_path == "/b") || (a_path == "/a" && b_path == "/a/d/b"),
        "tree must stay rooted: a={a_path} b={b_path}"
    );
}

/// A writer stuck behind the Change-order lock gives up after `lock_timeout`
/// with a retryable error instead of holding a pooled connection indefinitely.
#[tokio::test]
async fn a_blocked_writer_gives_up_with_a_retryable_error() {
    let Ok(url) = std::env::var("ANYSTORE_TEST_DATABASE_URL") else {
        eprintln!("skipping: ANYSTORE_TEST_DATABASE_URL is not set");
        return;
    };
    let _guard = POSTGRES_TEST_LOCK.lock().await;
    let _database = lock_database(&url).await;

    let factory = PostgresFactory::connect(&url).await;
    let pristine = factory.create().await;
    let store = PostgresMetaStore::connect(
        &url,
        PoolConfig {
            lock_timeout: std::time::Duration::from_millis(300),
            ..PoolConfig::default()
        },
    )
    .await
    .expect("connect with a short lock timeout");

    let mut stalled = pristine.pool().begin().await.expect("begin stalled tx");
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext('anystore:changes:append')::bigint)")
        .execute(&mut *stalled)
        .await
        .expect("take ordering lock");

    let id = ObjectId::generate();
    let error = store
        .create_object(CreateObjectCommit {
            id: id.clone(),
            kind: ObjectKind::Folder,
            name: "blocked".into(),
            parent_id: ObjectId::root(),
            content_type: None,
            metadata: json!({}),
            ctx: plain_context(),
        })
        .await
        .expect_err("the write must not wait indefinitely");
    assert!(matches!(error, DomainError::RateLimited), "{error:?}");
    stalled.rollback().await.expect("release ordering lock");

    assert!(store.get_object(&id).await.expect("get").is_none());
}

/// Defence in depth: should a parent cycle ever be persisted, reads that walk
/// the tree must still terminate.
#[tokio::test]
async fn tree_walks_terminate_on_a_parent_cycle() {
    let Ok(url) = std::env::var("ANYSTORE_TEST_DATABASE_URL") else {
        eprintln!("skipping: ANYSTORE_TEST_DATABASE_URL is not set");
        return;
    };
    let _guard = POSTGRES_TEST_LOCK.lock().await;
    let _database = lock_database(&url).await;

    let factory = PostgresFactory::connect(&url).await;
    let store = factory.create().await;
    let a = create_folder(&store, "a", &ObjectId::root()).await;
    let b = create_folder(&store, "b", &a).await;
    sqlx::query("UPDATE objects SET parent_id = $1 WHERE id = $2")
        .bind(b.as_str())
        .bind(a.as_str())
        .execute(store.pool())
        .await
        .expect("force a cycle");

    let read = tokio::time::timeout(std::time::Duration::from_secs(5), store.get_object(&a))
        .await
        .expect("path rendering must terminate");
    assert!(read.expect("get_object").is_some());
}
