//! Request formation and response decoding against a mock PostgREST gateway.
//!
//! Runs without any real credential or database: the mock records what the
//! adapter sent and replies with canned RPC envelopes.

use anystore_domain::change::ChangeAction;
use anystore_domain::error::DomainError;
use anystore_domain::metadata::MetadataPatch;
use anystore_domain::object::{ObjectKind, Revision};
use anystore_domain::upload::{UploadMode, UploadState};
use anystore_domain::{IdempotencyKey, ObjectId, PrincipalId, RequestId, UploadId};
use anystore_metastore::changes::{ChangeStore, ReadChanges};
use anystore_metastore::commands::{
    CommitContent, CreateObjectCommit, DeleteObjectCommit, ListChildren, ListOrder,
    MetadataCondition, MutationContext, MutationOutcome, ObjectQuery, OrderBy, PatchObjectCommit,
};
use anystore_metastore::idempotency::{
    IdempotencyAcquire, IdempotencyComplete, IdempotencyContext, IdempotencyDecision,
    IdempotencyRelease, IdempotencyStore,
};
use anystore_metastore::maintenance::BlobGcEntry;
use anystore_metastore::response::StoredResponse;
use anystore_metastore::uploads::{
    AbortUploadRecord, CreateUploadRecord, UpdateUploadState, UploadRepository,
};
use anystore_metastore::{MaintenanceStore, ObjectMutationStore, ObjectRepository};
use anystore_metastore_postgrest::{PostgrestConfig, PostgrestMetaStore};
use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use axum::routing::post;
use chrono::{Duration, TimeZone, Utc};
use serde_json::{Value, json};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

const API_KEY: &str = "mock-api-key";

#[derive(Clone, Default)]
struct MockState {
    requests: Arc<Mutex<Vec<Recorded>>>,
    replies: Arc<Mutex<VecDeque<(u16, String)>>>,
}

#[derive(Clone, Debug)]
struct Recorded {
    authorization: Option<String>,
    content_type: Option<String>,
    accept: Option<String>,
    body: Value,
}

struct Gateway {
    state: MockState,
    store: PostgrestMetaStore,
}

impl Gateway {
    /// Starts a mock gateway that answers with `replies` in order.
    async fn start(replies: Vec<Value>) -> Self {
        Self::start_raw(
            replies
                .into_iter()
                .map(|value| (200u16, value.to_string()))
                .collect(),
        )
        .await
    }

    async fn start_raw(replies: Vec<(u16, String)>) -> Self {
        let state = MockState {
            requests: Arc::new(Mutex::new(Vec::new())),
            replies: Arc::new(Mutex::new(replies.into())),
        };

        let router = Router::new()
            .route("/v1/rdb/rest/rpc/anystore_rpc", post(handle))
            .with_state(state.clone());

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock gateway");
        let address = listener.local_addr().expect("mock address");
        tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });

        let store =
            PostgrestMetaStore::connect(PostgrestConfig::new(format!("http://{address}"), API_KEY))
                .expect("connect");

        Self { state, store }
    }

    fn requests(&self) -> Vec<Recorded> {
        self.state.requests.lock().expect("requests").clone()
    }

    /// The single request the adapter issued.
    fn only_request(&self) -> Recorded {
        let requests = self.requests();
        assert_eq!(requests.len(), 1, "expected exactly one request");
        requests.into_iter().next().expect("request")
    }

    fn op(&self) -> String {
        self.only_request().body["op"]
            .as_str()
            .expect("op")
            .to_owned()
    }

    fn payload(&self) -> Value {
        self.only_request().body["payload"].clone()
    }
}

async fn handle(State(state): State<MockState>, headers: HeaderMap, body: Bytes) -> Response {
    let header = |name: &str| {
        headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned)
    };
    state.requests.lock().expect("requests").push(Recorded {
        authorization: header("authorization"),
        content_type: header("content-type"),
        accept: header("accept"),
        body: serde_json::from_slice(&body).unwrap_or(Value::Null),
    });

    let (status, payload) = state
        .replies
        .lock()
        .expect("replies")
        .pop_front()
        .unwrap_or_else(|| (200, json!({"ok": true, "data": null}).to_string()));

    Response::builder()
        .status(StatusCode::from_u16(status).expect("status"))
        .header("content-type", "application/json")
        .body(payload.into())
        .expect("response")
}

fn ok(data: Value) -> Value {
    json!({"ok": true, "data": data})
}

fn err(code: &str, message: &str) -> Value {
    json!({"ok": false, "error": {"code": code, "message": message}})
}

fn file_view(id: &str, name: &str) -> Value {
    json!({
        "id": id,
        "revision": 2,
        "kind": "file",
        "name": name,
        "parent_id": "root",
        "content_type": "text/plain",
        "size": 11,
        "sha256": null,
        "content_state": "ready",
        "metadata": {"owner": "alice"},
        "created_at": "2026-09-01T10:00:00.000000Z",
        "updated_at": "2026-09-01T10:00:01.000000Z",
        "path": format!("/{name}")
    })
}

fn upload_json(id: &str, state: &str) -> Value {
    json!({
        "id": id,
        "object_id": "obj_1",
        "state": state,
        "mode": "single",
        "blob_backend": "tencent_cos",
        "blob_ref": format!("blobs/{id}"),
        "provider_upload_id": null,
        "expected_size": 11,
        "content_type": "text/plain",
        "expected_sha256": null,
        "provider_completed": false,
        "created_at": "2026-09-01T10:00:00.000000Z",
        "expires_at": "2026-09-02T10:00:00.000000Z",
        "completed_at": null,
        "aborted_at": null
    })
}

fn rendered(status: u16, body: &str) -> Value {
    json!({
        "status": status,
        "headers": [["content-type", "application/json"]],
        // PostgreSQL wraps base64 at 76 characters.
        "body_b64": base64_with_newlines(body)
    })
}

fn base64_with_newlines(body: &str) -> String {
    use base64::Engine as _;
    let encoded = base64::engine::general_purpose::STANDARD.encode(body.as_bytes());
    encoded
        .as_bytes()
        .chunks(76)
        .map(|chunk| String::from_utf8_lossy(chunk).into_owned())
        .collect::<Vec<_>>()
        .join("\n")
}

fn mutation_context() -> MutationContext {
    let now = Utc.with_ymd_and_hms(2026, 9, 1, 10, 0, 0).unwrap();
    MutationContext {
        now,
        request_id: RequestId::new("req_1"),
        idempotency_key: Some(IdempotencyKey::new("key-1")),
        idempotency: Some(idempotency_context()),
        idempotency_expires_at: now + Duration::hours(24),
        // Deliberately explosive: the adapter must never invoke it, because the
        // database renders the response inside the mutation transaction.
        render: Arc::new(|_: &MutationOutcome| {
            panic!("the postgrest adapter must not render responses in process")
        }),
    }
}

fn idempotency_context() -> IdempotencyContext {
    IdempotencyContext {
        principal_id: PrincipalId::local(),
        operation: "POST /objects".into(),
        key: IdempotencyKey::new("key-1"),
        request_hash: "hash-1".into(),
        owner_token: "owner-1".into(),
        resource_token: "resource-1".into(),
    }
}

// ---------------------------------------------------------------------------
// Transport
// ---------------------------------------------------------------------------

#[tokio::test]
async fn every_request_carries_the_bearer_credential_and_json_headers() {
    let gateway = Gateway::start(vec![ok(Value::Null)]).await;
    gateway
        .store
        .get_object(&ObjectId::new("obj_1"))
        .await
        .expect("get_object");

    let request = gateway.only_request();
    assert_eq!(
        request.authorization.as_deref(),
        Some(format!("Bearer {API_KEY}").as_str())
    );
    assert_eq!(request.content_type.as_deref(), Some("application/json"));
    assert_eq!(request.accept.as_deref(), Some("application/json"));
    assert_eq!(request.body["op"], json!("get_object"));
}

#[tokio::test]
async fn the_schema_check_reads_the_root_object() {
    let gateway = Gateway::start(vec![ok(json!({"id": "root"}))]).await;
    gateway.store.check_schema().await.expect("check_schema");
    assert_eq!(gateway.op(), "get_object");
    assert_eq!(
        gateway.only_request().body["payload"],
        json!({"id": "root"})
    );
}

#[tokio::test]
async fn a_missing_rpc_surface_fails_the_schema_check() {
    let gateway = Gateway::start_raw(vec![(404, "not found".into())]).await;
    let error = gateway.store.check_schema().await.unwrap_err();
    assert_eq!(error.code(), "internal_error");
    assert!(error.to_string().contains("Internal error."));
}

#[tokio::test]
async fn gateway_statuses_map_onto_the_taxonomy() {
    for (status, code) in [
        (401u16, "internal_error"),
        (403, "internal_error"),
        (404, "internal_error"),
        (429, "rate_limited"),
        (500, "internal_error"),
        (503, "internal_error"),
    ] {
        let gateway = Gateway::start_raw(vec![(status, "gateway said no".into())]).await;
        let error = gateway
            .store
            .get_object(&ObjectId::new("obj_1"))
            .await
            .unwrap_err();
        assert_eq!(error.code(), code, "status {status}");
    }
}

#[tokio::test]
async fn a_non_json_body_is_an_internal_error() {
    let gateway = Gateway::start_raw(vec![(200, "<html>proxy</html>".into())]).await;
    let error = gateway
        .store
        .get_object(&ObjectId::new("obj_1"))
        .await
        .unwrap_err();
    assert_eq!(error.code(), "internal_error");
}

// ---------------------------------------------------------------------------
// Reads
// ---------------------------------------------------------------------------

#[tokio::test]
async fn get_object_decodes_a_view_and_absence() {
    let gateway = Gateway::start(vec![ok(file_view("obj_1", "notes.txt"))]).await;
    let view = gateway
        .store
        .get_object(&ObjectId::new("obj_1"))
        .await
        .expect("get_object")
        .expect("present");
    assert_eq!(gateway.payload(), json!({"id": "obj_1"}));
    assert_eq!(view.object.revision, Revision(2));
    assert_eq!(view.object.kind, ObjectKind::File);
    assert_eq!(view.path, "/notes.txt");
    assert_eq!(view.object.metadata, json!({"owner": "alice"}));

    let gateway = Gateway::start(vec![ok(Value::Null)]).await;
    assert!(
        gateway
            .store
            .get_object(&ObjectId::new("obj_missing"))
            .await
            .expect("get_object")
            .is_none()
    );
}

#[tokio::test]
async fn content_pointers_stay_internal_and_optional() {
    let gateway = Gateway::start(vec![ok(json!({
        "blob_backend": "tencent_cos",
        "blob_ref": "blobs/upload_1"
    }))])
    .await;
    let pointer = gateway
        .store
        .content_pointer(&ObjectId::new("obj_1"))
        .await
        .expect("content_pointer")
        .expect("present");
    assert_eq!(pointer.blob_backend, "tencent_cos");
    assert_eq!(pointer.blob_ref, "blobs/upload_1");

    let gateway = Gateway::start(vec![ok(Value::Null)]).await;
    assert!(
        gateway
            .store
            .content_pointer(&ObjectId::new("obj_1"))
            .await
            .expect("content_pointer")
            .is_none()
    );
}

#[tokio::test]
async fn list_children_asks_for_one_extra_row_and_returns_a_cursor() {
    let gateway = Gateway::start(vec![ok(json!({
        "items": [file_view("obj_a", "a"), file_view("obj_b", "b")]
    }))])
    .await;

    let page = gateway
        .store
        .list_children(ListChildren {
            parent_id: ObjectId::root(),
            limit: 1,
            cursor: None,
            order_by: OrderBy::Name,
            order: ListOrder::Asc,
        })
        .await
        .expect("list_children");

    assert_eq!(
        gateway.payload(),
        json!({
            "parent_id": "root",
            "limit": 2,
            "order_by": "name",
            "order": "asc",
            "cursor_key": null,
            "cursor_id": null
        })
    );
    assert_eq!(page.items.len(), 1);
    assert!(page.has_more);
    assert!(page.next_cursor.is_some());
}

#[tokio::test]
async fn list_children_forwards_a_normalised_cursor() {
    let first = Gateway::start(vec![ok(json!({
        "items": [file_view("obj_a", "a"), file_view("obj_b", "b")]
    }))])
    .await;
    let page = first
        .store
        .list_children(ListChildren {
            parent_id: ObjectId::root(),
            limit: 1,
            cursor: None,
            order_by: OrderBy::UpdatedAt,
            order: ListOrder::Desc,
        })
        .await
        .expect("list_children");

    let second = Gateway::start(vec![ok(json!({"items": []}))]).await;
    second
        .store
        .list_children(ListChildren {
            parent_id: ObjectId::root(),
            limit: 1,
            cursor: page.next_cursor.clone(),
            order_by: OrderBy::UpdatedAt,
            order: ListOrder::Desc,
        })
        .await
        .expect("list_children");

    let payload = second.payload();
    assert_eq!(payload["order"], json!("desc"));
    assert_eq!(payload["order_by"], json!("updated_at"));
    assert_eq!(payload["cursor_id"], json!("obj_a"));
    assert_eq!(payload["cursor_key"], json!("2026-09-01T10:00:01.000000Z"));
}

#[tokio::test]
async fn a_malformed_cursor_never_reaches_the_gateway() {
    let gateway = Gateway::start(vec![]).await;
    let error = gateway
        .store
        .list_children(ListChildren {
            parent_id: ObjectId::root(),
            limit: 10,
            cursor: Some("!!!not-base64!!!".into()),
            order_by: OrderBy::Name,
            order: ListOrder::Asc,
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), "invalid_request");
    assert!(gateway.requests().is_empty());
}

#[tokio::test]
async fn resolve_path_sends_validated_segments() {
    let gateway = Gateway::start(vec![ok(file_view("obj_1", "notes.txt"))]).await;
    gateway
        .store
        .resolve_path("/reports//2026/notes.txt")
        .await
        .expect("resolve_path");
    assert_eq!(gateway.op(), "resolve_path");
    assert_eq!(
        gateway.payload(),
        json!({"segments": ["reports", "2026", "notes.txt"]})
    );

    let gateway = Gateway::start(vec![]).await;
    assert_eq!(
        gateway
            .store
            .resolve_path("/a/../b")
            .await
            .unwrap_err()
            .code(),
        "invalid_request"
    );
    assert!(gateway.requests().is_empty());
}

#[tokio::test]
async fn query_objects_sends_every_filter() {
    let gateway = Gateway::start(vec![ok(json!({"items": []}))]).await;
    gateway
        .store
        .query_objects(ObjectQuery {
            kind: Some(ObjectKind::File),
            name: Some("notes.txt".into()),
            parent_id: Some(ObjectId::root()),
            metadata: vec![
                ("owner".into(), MetadataCondition::Eq(json!("alice"))),
                ("archived".into(), MetadataCondition::Exists(false)),
            ],
            limit: 50,
            cursor: None,
        })
        .await
        .expect("query_objects");

    assert_eq!(
        gateway.payload(),
        json!({
            "kind": "file",
            "name": "notes.txt",
            "parent_id": "root",
            "metadata": [
                {"key": "owner", "op": "eq", "value": "alice"},
                {"key": "archived", "op": "exists", "value": false}
            ],
            "limit": 51,
            "cursor_id": null
        })
    );
}

// ---------------------------------------------------------------------------
// Mutations
// ---------------------------------------------------------------------------

#[tokio::test]
async fn create_object_sends_the_command_and_returns_the_rendered_response() {
    let body = r#"{"id":"obj_1","revision":1}"#;
    let gateway = Gateway::start(vec![ok(json!({
        "response": rendered(201, body),
        "changes": []
    }))])
    .await;

    let response = gateway
        .store
        .create_object(CreateObjectCommit {
            id: ObjectId::new("obj_1"),
            kind: ObjectKind::Folder,
            name: "reports".into(),
            parent_id: ObjectId::root(),
            content_type: None,
            metadata: json!({"team": "ops"}),
            ctx: mutation_context(),
        })
        .await
        .expect("create_object");

    assert_eq!(gateway.op(), "create_object");
    let payload = gateway.payload();
    assert_eq!(payload["id"], json!("obj_1"));
    assert_eq!(payload["kind"], json!("folder"));
    assert_eq!(payload["name"], json!("reports"));
    assert_eq!(payload["parent_id"], json!("root"));
    assert_eq!(payload["content_type"], Value::Null);
    assert_eq!(payload["metadata"], json!({"team": "ops"}));
    assert_eq!(payload["ctx"]["now"], json!("2026-09-01T10:00:00.000000Z"));
    assert_eq!(
        payload["ctx"]["idempotency"]["owner_token"],
        json!("owner-1")
    );
    assert_eq!(
        payload["ctx"]["idempotency"]["resource_token"],
        json!("resource-1")
    );

    assert_eq!(response.status, 201);
    assert_eq!(
        response.headers,
        vec![("content-type".to_owned(), "application/json".to_owned())]
    );
    assert_eq!(response.body, body.as_bytes());
}

#[tokio::test]
async fn patch_object_sends_if_match_and_the_metadata_patch() {
    let gateway = Gateway::start(vec![ok(json!({
        "response": rendered(200, "{}"),
        "changes": []
    }))])
    .await;

    gateway
        .store
        .patch_object(PatchObjectCommit {
            id: ObjectId::new("obj_1"),
            if_match: Some(Revision(4)),
            new_name: Some("renamed".into()),
            new_parent_id: Some(ObjectId::new("obj_folder")),
            metadata_patch: Some(MetadataPatch {
                set: json!({"a": 1}).as_object().expect("object").clone(),
                remove: vec!["b".into()],
            }),
            ctx: mutation_context(),
        })
        .await
        .expect("patch_object");

    assert_eq!(
        gateway.payload()["if_match"],
        json!(4),
        "If-Match must reach the database"
    );
    assert_eq!(gateway.payload()["new_name"], json!("renamed"));
    assert_eq!(gateway.payload()["new_parent_id"], json!("obj_folder"));
    assert_eq!(
        gateway.payload()["metadata_patch"],
        json!({"set": {"a": 1}, "remove": ["b"]})
    );
}

#[tokio::test]
async fn a_revision_conflict_carries_the_current_revision() {
    let gateway = Gateway::start(vec![json!({
        "ok": false,
        "error": {
            "code": "revision_conflict",
            "message": "Object has been modified.",
            "current_revision": 7
        }
    })])
    .await;

    let error = gateway
        .store
        .patch_object(PatchObjectCommit {
            id: ObjectId::new("obj_1"),
            if_match: Some(Revision(4)),
            new_name: None,
            new_parent_id: None,
            metadata_patch: None,
            ctx: mutation_context(),
        })
        .await
        .unwrap_err();

    match error {
        DomainError::RevisionConflict { current_revision } => assert_eq!(current_revision, 7),
        other => panic!("expected a revision conflict, got {other:?}"),
    }
}

#[tokio::test]
async fn delete_object_returns_no_content() {
    let gateway = Gateway::start(vec![ok(json!({
        "response": {"status": 204, "headers": [], "body_b64": ""},
        "changes": []
    }))])
    .await;

    let response = gateway
        .store
        .delete_object(DeleteObjectCommit {
            id: ObjectId::new("obj_1"),
            if_match: None,
            recursive: true,
            ctx: mutation_context(),
        })
        .await
        .expect("delete_object");

    assert_eq!(gateway.payload()["recursive"], json!(true));
    assert_eq!(response, StoredResponse::no_content());
}

#[tokio::test]
async fn commit_content_sends_the_verified_blob_facts() {
    let body = r#"{"object_id":"obj_1","revision":3}"#;
    let gateway = Gateway::start(vec![ok(json!({
        "response": rendered(200, body),
        "changes": []
    }))])
    .await;

    let now = Utc.with_ymd_and_hms(2026, 9, 1, 11, 0, 0).unwrap();
    let response = gateway
        .store
        .commit_content(CommitContent {
            upload_id: UploadId::new("upload_1"),
            object_id: ObjectId::new("obj_1"),
            if_match: None,
            blob_backend: "tencent_cos".into(),
            blob_ref: "blobs/upload_1".into(),
            size: 11,
            content_type: "text/plain".into(),
            sha256: Some("a".repeat(64)),
            gc_not_before: now,
            ctx: mutation_context(),
        })
        .await
        .expect("commit_content");

    let payload = gateway.payload();
    assert_eq!(payload["upload_id"], json!("upload_1"));
    assert_eq!(payload["size"], json!(11));
    assert_eq!(
        payload["gc_not_before"],
        json!("2026-09-01T11:00:00.000000Z")
    );
    assert_eq!(response.body, body.as_bytes());
}

#[tokio::test]
async fn a_mutation_without_a_response_is_an_internal_error() {
    let gateway = Gateway::start(vec![ok(json!({"changes": []}))]).await;
    let error = gateway
        .store
        .delete_object(DeleteObjectCommit {
            id: ObjectId::new("obj_1"),
            if_match: None,
            recursive: false,
            ctx: mutation_context(),
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), "internal_error");
}

// ---------------------------------------------------------------------------
// Uploads
// ---------------------------------------------------------------------------

#[tokio::test]
async fn upload_creation_reports_whether_it_inserted() {
    let gateway = Gateway::start(vec![ok(json!({
        "upload": upload_json("upload_1", "initiating"),
        "created": false
    }))])
    .await;

    let now = Utc.with_ymd_and_hms(2026, 9, 1, 10, 0, 0).unwrap();
    let result = gateway
        .store
        .create_upload_record(CreateUploadRecord {
            id: UploadId::new("upload_1"),
            object_id: ObjectId::new("obj_1"),
            mode: UploadMode::Single,
            blob_backend: "tencent_cos".into(),
            blob_ref: "blobs/upload_1".into(),
            expected_size: 11,
            content_type: "text/plain".into(),
            expected_sha256: None,
            created_at: now,
            expires_at: now + Duration::hours(24),
        })
        .await
        .expect("create_upload_record");

    assert!(
        !result.created,
        "a resumed session must not report creation"
    );
    assert_eq!(result.upload.state, UploadState::Initiating);
    assert_eq!(result.upload.blob_ref, "blobs/upload_1");
    assert_eq!(gateway.payload()["expected_size"], json!(11));
    assert_eq!(
        gateway.payload()["expires_at"],
        json!("2026-09-02T10:00:00.000000Z")
    );
}

#[tokio::test]
async fn get_upload_decodes_presence_and_absence() {
    let gateway = Gateway::start(vec![ok(upload_json("upload_1", "completed"))]).await;
    let upload = gateway
        .store
        .get_upload(&UploadId::new("upload_1"))
        .await
        .expect("get_upload")
        .expect("present");
    assert_eq!(upload.state, UploadState::Completed);

    let gateway = Gateway::start(vec![ok(Value::Null)]).await;
    assert!(
        gateway
            .store
            .get_upload(&UploadId::new("upload_1"))
            .await
            .expect("get_upload")
            .is_none()
    );
}

#[tokio::test]
async fn an_illegal_upload_transition_is_reported_as_not_found() {
    let gateway = Gateway::start(vec![err("upload_not_found", "Upload session not found.")]).await;
    let error = gateway
        .store
        .update_upload_state(UpdateUploadState {
            id: UploadId::new("upload_1"),
            state: UploadState::Ready,
            provider_upload_id: Some("provider-1".into()),
            provider_completed: None,
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), "upload_not_found");
    assert_eq!(error.status(), 404);
    assert_eq!(gateway.payload()["provider_upload_id"], json!("provider-1"));
    assert_eq!(gateway.payload()["provider_completed"], Value::Null);
}

#[tokio::test]
async fn aborting_sends_both_the_abort_time_and_the_gc_grace() {
    let gateway = Gateway::start(vec![ok(Value::Null)]).await;
    let now = Utc.with_ymd_and_hms(2026, 9, 1, 10, 0, 0).unwrap();
    gateway
        .store
        .abort_upload_record(AbortUploadRecord {
            id: UploadId::new("upload_1"),
            now,
            gc_not_before: now + Duration::hours(1),
        })
        .await
        .expect("abort_upload_record");

    assert_eq!(
        gateway.payload(),
        json!({
            "id": "upload_1",
            "now": "2026-09-01T10:00:00.000000Z",
            "gc_not_before": "2026-09-01T11:00:00.000000Z"
        })
    );
}

// ---------------------------------------------------------------------------
// Idempotency
// ---------------------------------------------------------------------------

fn acquire_request() -> IdempotencyAcquire {
    let now = Utc.with_ymd_and_hms(2026, 9, 1, 10, 0, 0).unwrap();
    IdempotencyAcquire {
        context: idempotency_context(),
        now,
        lease_until: now + Duration::seconds(30),
        expires_at: now + Duration::hours(24),
    }
}

#[tokio::test]
async fn acquiring_returns_the_resource_token() {
    let gateway = Gateway::start(vec![ok(json!({
        "decision": "owner",
        "resource_token": "1788267600000000"
    }))])
    .await;

    match gateway.store.acquire(acquire_request()).await.unwrap() {
        IdempotencyDecision::Owner { resource_token } => {
            assert_eq!(resource_token, "1788267600000000")
        }
        other => panic!("expected ownership, got {other:?}"),
    }
    assert_eq!(
        gateway.payload()["context"],
        json!({
            "principal_id": "local",
            "operation": "POST /objects",
            "key": "key-1",
            "request_hash": "hash-1",
            "owner_token": "owner-1",
            "resource_token": "resource-1"
        })
    );
}

#[tokio::test]
async fn a_completed_key_replays_the_stored_bytes() {
    let body = r#"{"id":"obj_1"}"#;
    let gateway = Gateway::start(vec![ok(json!({
        "decision": "replay",
        "response": rendered(201, body)
    }))])
    .await;

    match gateway.store.acquire(acquire_request()).await.unwrap() {
        IdempotencyDecision::Replay(response) => {
            assert_eq!(response.status, 201);
            assert_eq!(response.body, body.as_bytes());
        }
        other => panic!("expected a replay, got {other:?}"),
    }
}

#[tokio::test]
async fn a_reused_key_with_another_request_is_a_conflict() {
    let gateway = Gateway::start(vec![ok(json!({"decision": "conflict"}))]).await;
    assert!(matches!(
        gateway.store.acquire(acquire_request()).await.unwrap(),
        IdempotencyDecision::Conflict
    ));
}

#[tokio::test]
async fn a_busy_key_is_polled_until_it_frees_up() {
    let gateway = Gateway::start(vec![
        ok(json!({"decision": "busy"})),
        ok(json!({"decision": "retry"})),
        ok(json!({"decision": "owner", "resource_token": "7"})),
    ])
    .await;

    assert!(matches!(
        gateway.store.acquire(acquire_request()).await.unwrap(),
        IdempotencyDecision::Owner { .. }
    ));
    assert_eq!(gateway.requests().len(), 3);
}

#[tokio::test]
async fn a_permanently_busy_key_is_rate_limited() {
    let replies = (0..12).map(|_| ok(json!({"decision": "busy"}))).collect();
    let gateway = Gateway::start(replies).await;
    let error = gateway.store.acquire(acquire_request()).await.unwrap_err();
    assert_eq!(error.code(), "rate_limited");
    assert_eq!(error.status(), 429);
}

#[tokio::test]
async fn completing_sends_the_response_bytes_verbatim() {
    let gateway = Gateway::start(vec![ok(Value::Null)]).await;
    let now = Utc.with_ymd_and_hms(2026, 9, 1, 10, 0, 0).unwrap();
    gateway
        .store
        .complete(IdempotencyComplete {
            context: idempotency_context(),
            response: StoredResponse::json(201, br#"{"id":"obj_1"}"#.to_vec()),
            now,
            expires_at: now + Duration::hours(24),
        })
        .await
        .expect("complete");

    let payload = gateway.payload();
    assert_eq!(payload["response"]["status"], json!(201));
    assert_eq!(
        payload["response"]["headers"],
        json!([["content-type", "application/json"]])
    );
    assert_eq!(
        payload["response"]["body_b64"],
        json!("eyJpZCI6Im9ial8xIn0=")
    );
}

#[tokio::test]
async fn releasing_only_needs_the_scope() {
    let gateway = Gateway::start(vec![ok(Value::Null)]).await;
    gateway
        .store
        .fail_or_release(IdempotencyRelease {
            context: idempotency_context(),
        })
        .await
        .expect("release");
    assert_eq!(gateway.op(), "idempotency_release");
    assert_eq!(gateway.payload()["context"]["key"], json!("key-1"));
}

// ---------------------------------------------------------------------------
// Changes
// ---------------------------------------------------------------------------

#[tokio::test]
async fn read_changes_mints_a_cursor_and_decodes_the_page() {
    let gateway = Gateway::start(vec![ok(json!({
        "items": [{
            "change_id": "chg_00000000000000000001",
            "object_id": "obj_1",
            "revision": 1,
            "action": "created",
            "changed_at": "2026-09-01T10:00:00.000000Z",
            "request_id": "req_1",
            "idempotency_key": null,
            "tombstone": false
        }],
        "next_cursor": "chgcur_01jz",
        "has_more": true,
        "lag": 12
    }))])
    .await;

    let now = Utc.with_ymd_and_hms(2026, 9, 1, 10, 0, 0).unwrap();
    let page = gateway
        .store
        .read_changes(ReadChanges {
            cursor: None,
            limit: 100,
            now,
            cursor_expires_at: now + Duration::days(30),
        })
        .await
        .expect("read_changes");

    let payload = gateway.payload();
    assert_eq!(payload["cursor"], Value::Null);
    assert_eq!(payload["limit"], json!(100));
    assert!(
        payload["new_cursor_id"]
            .as_str()
            .expect("cursor id")
            .starts_with("chgcur_"),
        "the adapter mints the continuation cursor id"
    );

    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].action, ChangeAction::Created);
    assert_eq!(page.next_cursor.as_str(), "chgcur_01jz");
    assert!(page.has_more);
    assert_eq!(page.lag, 12);
}

#[tokio::test]
async fn a_malformed_changes_cursor_is_a_bad_request() {
    let gateway = Gateway::start(vec![]).await;
    let now = Utc::now();
    let error = gateway
        .store
        .read_changes(ReadChanges {
            cursor: Some(anystore_domain::ids::CursorId::new("garbage")),
            limit: 10,
            now,
            cursor_expires_at: now + Duration::days(1),
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), "invalid_request");
    assert!(gateway.requests().is_empty());
}

#[tokio::test]
async fn an_expired_cursor_is_reported_as_gone() {
    let gateway = Gateway::start(vec![err("changes_cursor_expired", "Outside retention.")]).await;
    let now = Utc::now();
    let error = gateway
        .store
        .read_changes(ReadChanges {
            cursor: Some(anystore_domain::ids::CursorId::new("chgcur_01jz")),
            limit: 10,
            now,
            cursor_expires_at: now + Duration::days(1),
        })
        .await
        .unwrap_err();
    assert_eq!(error.status(), 410);
}

// ---------------------------------------------------------------------------
// Maintenance
// ---------------------------------------------------------------------------

#[tokio::test]
async fn gc_claims_decode_their_attempt_counters() {
    let gateway = Gateway::start(vec![ok(json!([
        {"blob_backend": "tencent_cos", "blob_ref": "blobs/one", "attempts": 0},
        {"blob_backend": "tencent_cos", "blob_ref": "blobs/two", "attempts": 3}
    ]))])
    .await;

    let now = Utc.with_ymd_and_hms(2026, 9, 1, 10, 0, 0).unwrap();
    let batch = gateway
        .store
        .claim_gc_batch(now, 50)
        .await
        .expect("claim_gc_batch");
    assert_eq!(batch.len(), 2);
    assert_eq!(batch[1].attempts, 3);
    assert_eq!(
        gateway.payload(),
        json!({"now": "2026-09-01T10:00:00.000000Z", "limit": 50})
    );
}

#[tokio::test]
async fn failing_a_blob_sends_the_backoff_deadline() {
    let gateway = Gateway::start(vec![ok(Value::Null)]).await;
    let retry_at = Utc.with_ymd_and_hms(2026, 9, 1, 12, 0, 0).unwrap();
    gateway
        .store
        .fail_gc(
            &BlobGcEntry {
                blob_backend: "tencent_cos".into(),
                blob_ref: "blobs/one".into(),
                attempts: 1,
            },
            "provider refused",
            retry_at,
        )
        .await
        .expect("fail_gc");

    assert_eq!(
        gateway.payload(),
        json!({
            "blob_backend": "tencent_cos",
            "blob_ref": "blobs/one",
            "error": "provider refused",
            "retry_at": "2026-09-01T12:00:00.000000Z"
        })
    );
}

#[tokio::test]
async fn finishing_a_blob_only_identifies_it() {
    let gateway = Gateway::start(vec![ok(Value::Null)]).await;
    gateway
        .store
        .finish_gc(&BlobGcEntry {
            blob_backend: "tencent_cos".into(),
            blob_ref: "blobs/one".into(),
            attempts: 0,
        })
        .await
        .expect("finish_gc");
    assert_eq!(gateway.op(), "finish_gc");
    assert_eq!(gateway.payload()["blob_ref"], json!("blobs/one"));
}

#[tokio::test]
async fn expired_upload_claims_decode_full_records() {
    let gateway = Gateway::start(vec![ok(json!([upload_json("upload_1", "expired")]))]).await;
    let uploads = gateway
        .store
        .claim_expired_uploads(Utc::now(), 50)
        .await
        .expect("claim_expired_uploads");
    assert_eq!(uploads.len(), 1);
    assert_eq!(uploads[0].state, UploadState::Expired);
    assert_eq!(uploads[0].id.as_str(), "upload_1");
}

#[tokio::test]
async fn purges_and_counts_return_exact_row_counts() {
    let now = Utc::now();

    let gateway = Gateway::start(vec![ok(json!(4))]).await;
    assert_eq!(
        gateway
            .store
            .purge_idempotency_records(now)
            .await
            .expect("purge"),
        4
    );

    let gateway = Gateway::start(vec![ok(json!(2))]).await;
    assert_eq!(
        gateway
            .store
            .purge_change_cursors(now)
            .await
            .expect("purge"),
        2
    );

    let gateway = Gateway::start(vec![ok(json!(9))]).await;
    assert_eq!(gateway.store.purge_changes(now).await.expect("purge"), 9);
    assert_eq!(gateway.op(), "purge_changes");
    assert!(gateway.payload()["older_than"].is_string());

    let gateway = Gateway::start(vec![ok(json!(17))]).await;
    assert_eq!(
        gateway.store.count_pending_gc().await.expect("count"),
        17,
        "pending GC depth feeds the metrics gauge"
    );
}

#[tokio::test]
async fn a_malformed_count_is_an_internal_error() {
    let gateway = Gateway::start(vec![ok(json!("many"))]).await;
    assert_eq!(
        gateway.store.count_pending_gc().await.unwrap_err().code(),
        "internal_error"
    );
}
