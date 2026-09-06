//! Runs the shared `MetaStore` conformance suite through the HTTP adapter
//! against the real RPC functions.
//!
//! A local PostgREST-shaped proxy executes `anystore_rpc(op, payload)` over
//! sqlx, so the suite exercises the migration's SQL *and* the adapter's request
//! formation, transport and decoding. Skipped unless
//! `ANYSTORE_TEST_DATABASE_URL` is set, so `cargo test` works without a
//! database.

use anystore_application::dto;
use anystore_domain::ObjectId;
use anystore_domain::object::ObjectKind;
use anystore_domain::{IdempotencyKey, PrincipalId, RequestId};
use anystore_metastore::ObjectMutationStore;
use anystore_metastore::commands::{CreateObjectCommit, MutationContext, MutationOutcome};
use anystore_metastore::idempotency::{
    IdempotencyAcquire, IdempotencyContext, IdempotencyDecision,
};
use anystore_metastore::response::StoredResponse;
use anystore_metastore::{IdempotencyStore, MaintenanceStore, ObjectRepository};
use anystore_metastore_postgres::{PoolConfig, PostgresMetaStore};
use anystore_metastore_postgrest::{PostgrestConfig, PostgrestMetaStore};
use anystore_testkit::conformance::metastore::{self, MetaStoreFactory};
use async_trait::async_trait;
use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use axum::routing::post;
use chrono::{Duration, Utc};
use serde_json::{Value, json};
use sqlx::postgres::PgPool;
use std::sync::Arc;

/// The migration under review. `include_str!` makes a rename a compile error.
const RPC_MIGRATION: &str =
    include_str!("../../../cloudbase/migrations/20260901213000_anystore_postgrest_rpc.sql");

const TEST_API_KEY: &str = "conformance-api-key";

static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Serialises against the sqlx conformance suite, which truncates the same
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

#[derive(Clone)]
struct ProxyState {
    pool: PgPool,
}

/// Minimal PostgREST-compatible RPC endpoint.
async fn handle(State(state): State<ProxyState>, headers: HeaderMap, body: Bytes) -> Response {
    let authorization = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if authorization != format!("Bearer {TEST_API_KEY}") {
        return text_response(401, "unauthorized".into());
    }

    let request: Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(_) => return text_response(400, "malformed request".into()),
    };
    let Some(op) = request.get("op").and_then(Value::as_str) else {
        return text_response(400, "missing op".into());
    };
    let payload = request.get("payload").cloned().unwrap_or(json!({}));

    match sqlx::query_scalar::<_, Value>("SELECT anystore_rpc($1, $2)")
        .bind(op)
        .bind(payload)
        .fetch_one(&state.pool)
        .await
    {
        Ok(value) => text_response(200, value.to_string()),
        // PostgREST answers an unhandled database error with a 5xx.
        Err(error) => text_response(500, json!({"message": error.to_string()}).to_string()),
    }
}

fn text_response(status: u16, body: String) -> Response {
    Response::builder()
        .status(StatusCode::from_u16(status).expect("status"))
        .header("content-type", "application/json")
        .body(body.into())
        .expect("response")
}

struct ProxyFactory {
    pool: PgPool,
    store: Arc<PostgrestMetaStore>,
}

impl ProxyFactory {
    async fn start(url: &str) -> Self {
        let migrator = PostgresMetaStore::connect(url, PoolConfig::default())
            .await
            .expect("connect");
        migrator.migrate().await.expect("base migrations");
        sqlx::raw_sql(RPC_MIGRATION)
            .execute(migrator.pool())
            .await
            .expect("rpc migration");

        let pool = migrator.pool().clone();
        let router = Router::new()
            .route("/v1/rdb/rest/rpc/anystore_rpc", post(handle))
            .with_state(ProxyState { pool: pool.clone() });

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind proxy");
        let address = listener.local_addr().expect("proxy address");
        tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });

        let store = PostgrestMetaStore::connect(PostgrestConfig::new(
            format!("http://{address}"),
            TEST_API_KEY,
        ))
        .expect("connect adapter");
        store.check_schema().await.expect("rpc surface");

        Self {
            pool,
            store: Arc::new(store),
        }
    }
}

#[async_trait]
impl MetaStoreFactory for ProxyFactory {
    type Store = PostgrestMetaStore;

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
        .execute(&self.pool)
        .await
        .expect("reset");

        Arc::clone(&self.store)
    }
}

fn database_url() -> Option<String> {
    std::env::var("ANYSTORE_TEST_DATABASE_URL").ok()
}

#[tokio::test]
async fn postgrest_metastore_is_conformant() {
    let Some(url) = database_url() else {
        eprintln!("skipping: ANYSTORE_TEST_DATABASE_URL is not set");
        return;
    };
    let _guard = TEST_LOCK.lock().await;
    let _database = lock_database(&url).await;

    let factory = ProxyFactory::start(&url).await;
    metastore::run_all(&factory).await;
}

/// The database renders mutation responses because a `ResponseRenderer` closure
/// cannot cross HTTP. This pins the rendered body to the application's public
/// JSON, so the two cannot drift apart unnoticed.
#[tokio::test]
async fn rendered_bodies_match_the_application_representation() {
    let Some(url) = database_url() else {
        eprintln!("skipping: ANYSTORE_TEST_DATABASE_URL is not set");
        return;
    };
    let _guard = TEST_LOCK.lock().await;
    let _database = lock_database(&url).await;

    let factory = ProxyFactory::start(&url).await;
    let store = factory.create().await;

    for (kind, name, content_type, metadata) in [
        (ObjectKind::Folder, "reports", None, json!({})),
        (
            ObjectKind::File,
            "notes.txt",
            Some("text/plain".to_owned()),
            json!({"owner": "alice", "tags": ["a", "b"], "nested": {"deep": true}}),
        ),
    ] {
        let id = ObjectId::generate();
        let now = Utc::now();
        let response = store
            .create_object(CreateObjectCommit {
                id: id.clone(),
                kind,
                name: name.into(),
                parent_id: ObjectId::root(),
                content_type,
                metadata,
                ctx: MutationContext {
                    now,
                    request_id: RequestId::generate(),
                    idempotency_key: None,
                    idempotency: None,
                    idempotency_expires_at: now + Duration::hours(24),
                    render: Arc::new(|_: &MutationOutcome| {
                        panic!("the adapter must not render in process")
                    }),
                },
            })
            .await
            .expect("create_object");

        assert_eq!(response.status, 201);
        assert_eq!(
            response.headers,
            vec![("content-type".to_owned(), "application/json".to_owned())]
        );

        let rendered: Value = serde_json::from_slice(&response.body).expect("rendered body");
        let view = store
            .get_object(&id)
            .await
            .expect("get_object")
            .expect("present");
        assert_eq!(
            rendered,
            dto::object_json(&view),
            "the database rendering must match anystore-application/src/dto.rs"
        );

        for leak in ["blob_ref", "blob_backend", "deleted_at"] {
            assert!(
                !rendered.to_string().contains(leak),
                "rendered body leaked {leak}"
            );
        }
    }
}

/// A retry must return the original bytes, not a freshly rendered equivalent.
#[tokio::test]
async fn stored_responses_replay_byte_for_byte() {
    let Some(url) = database_url() else {
        eprintln!("skipping: ANYSTORE_TEST_DATABASE_URL is not set");
        return;
    };
    let _guard = TEST_LOCK.lock().await;
    let _database = lock_database(&url).await;

    let factory = ProxyFactory::start(&url).await;
    let store = factory.create().await;

    let context = IdempotencyContext {
        principal_id: PrincipalId::local(),
        operation: "POST /objects".into(),
        key: IdempotencyKey::new("replay-key"),
        request_hash: "hash".into(),
        owner_token: "owner-1".into(),
        resource_token: String::new(),
    };
    let now = Utc::now();
    let decision = store
        .acquire(IdempotencyAcquire {
            context: context.clone(),
            now,
            lease_until: now + Duration::seconds(30),
            expires_at: now + Duration::hours(24),
        })
        .await
        .expect("acquire");
    assert!(matches!(decision, IdempotencyDecision::Owner { .. }));

    let first = store
        .create_object(CreateObjectCommit {
            id: ObjectId::generate(),
            kind: ObjectKind::Folder,
            name: "replayed".into(),
            parent_id: ObjectId::root(),
            content_type: None,
            metadata: json!({"z": 1, "a": 2}),
            ctx: MutationContext {
                now,
                request_id: RequestId::generate(),
                idempotency_key: Some(IdempotencyKey::new("replay-key")),
                idempotency: Some(context.clone()),
                idempotency_expires_at: now + Duration::hours(24),
                render: Arc::new(|_: &MutationOutcome| Ok(StoredResponse::no_content())),
            },
        })
        .await
        .expect("create_object");

    let replay = store
        .acquire(IdempotencyAcquire {
            context,
            now,
            lease_until: now + Duration::seconds(30),
            expires_at: now + Duration::hours(24),
        })
        .await
        .expect("acquire");

    match replay {
        IdempotencyDecision::Replay(stored) => assert_eq!(stored, first),
        other => panic!("expected a replay, got {other:?}"),
    }
}

/// The GC queue functions use data-modifying CTEs and a leased claim, none of
/// which the shared suite drives end to end.
#[tokio::test]
async fn the_blob_gc_queue_leases_backs_off_and_drains() {
    let Some(url) = database_url() else {
        eprintln!("skipping: ANYSTORE_TEST_DATABASE_URL is not set");
        return;
    };
    let _guard = TEST_LOCK.lock().await;
    let _database = lock_database(&url).await;

    let factory = ProxyFactory::start(&url).await;
    let store = factory.create().await;

    let now = Utc::now();
    sqlx::query(
        "INSERT INTO blob_gc_queue (blob_backend, blob_ref, not_before)
         VALUES ('test', 'blobs/gc-one', $1), ('test', 'blobs/gc-two', $1)",
    )
    .bind(now - Duration::minutes(1))
    .execute(&factory.pool)
    .await
    .expect("seed queue");

    assert_eq!(store.count_pending_gc().await.expect("count"), 2);

    let claimed = store.claim_gc_batch(now, 10).await.expect("claim");
    assert_eq!(claimed.len(), 2);
    assert!(claimed.iter().all(|entry| entry.attempts == 0));

    // The lease hides the batch from a concurrent worker.
    assert!(
        store
            .claim_gc_batch(now, 10)
            .await
            .expect("claim")
            .is_empty(),
        "a claimed blob must not be handed out twice"
    );

    store.finish_gc(&claimed[0]).await.expect("finish");
    store
        .fail_gc(&claimed[1], "provider refused", now + Duration::minutes(1))
        .await
        .expect("fail");

    assert_eq!(store.count_pending_gc().await.expect("count"), 1);
    let retried = store
        .claim_gc_batch(now + Duration::minutes(2), 10)
        .await
        .expect("claim");
    assert_eq!(retried.len(), 1);
    assert_eq!(
        retried[0].attempts, 1,
        "the attempt counter drives exponential backoff"
    );

    store.finish_gc(&retried[0]).await.expect("finish");
    assert_eq!(store.count_pending_gc().await.expect("count"), 0);
}
