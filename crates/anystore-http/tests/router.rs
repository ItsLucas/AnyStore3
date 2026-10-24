//! Router-level checks against the in-memory adapters.

use anystore_application::{AppConfig, AppState, MaintenanceService, Metrics};
use anystore_blobstore::BlobRegistry;
use anystore_http::{AuthConfig, HttpState, router};
use anystore_testkit::{InMemoryBlobStore, InMemoryMetaStore};
use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{HeaderValue, Request, StatusCode};
use serde_json::{Value, json};
use std::sync::Arc;
use tower::ServiceExt;

fn app() -> Router {
    app_with_auth(AuthConfig::disabled())
}

fn app_with_auth(auth: AuthConfig) -> Router {
    let metrics = Arc::new(Metrics::default());
    let app = Arc::new(AppState::new(
        Arc::new(InMemoryMetaStore::new()),
        Arc::new(BlobRegistry::new(Arc::new(InMemoryBlobStore::new()))),
        AppConfig::default(),
        Arc::clone(&metrics),
    ));
    router(HttpState {
        maintenance: MaintenanceService::new(Arc::clone(&app)),
        maintenance_budget: std::time::Duration::from_secs(5),
        app,
        auth: Arc::new(auth),
        metrics,
    })
}

async fn send(
    app: &Router,
    method: &str,
    uri: &str,
    headers: &[(&str, HeaderValue)],
    body: Value,
) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json");
    for (name, value) in headers {
        request = request.header(*name, value.clone());
    }
    let response = app
        .clone()
        .oneshot(request.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

async fn create_folder(app: &Router, name: &str) -> Value {
    let (status, body) = send(
        app,
        "POST",
        "/api/v1/objects",
        &[],
        json!({"kind": "folder", "name": name, "parent_id": "root"}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    body
}

#[tokio::test]
async fn header_injecting_content_type_is_rejected() {
    let app = app();
    let (status, body) = send(
        &app,
        "POST",
        "/api/v1/objects",
        &[],
        json!({"kind": "file", "name": "a.txt", "content_type": "text/plain\nX: 1"}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "invalid_request");
}

#[tokio::test]
async fn unreadable_if_match_does_not_become_an_unconditional_write() {
    let app = app();
    let folder = create_folder(&app, "docs").await;
    let id = folder["id"].as_str().unwrap();

    let (status, body) = send(
        &app,
        "PATCH",
        &format!("/api/v1/objects/{id}"),
        &[("if-match", HeaderValue::from_bytes(b"1\xe2").unwrap())],
        json!({"metadata": {"set": {"k": "v"}}}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"], "invalid_request");

    let (_, current) = send(
        &app,
        "GET",
        &format!("/api/v1/objects/{id}"),
        &[],
        Value::Null,
    )
    .await;
    assert_eq!(current["revision"], folder["revision"]);

    // The same body is otherwise valid, so the 400 above came from the header.
    let (status, body) = send(
        &app,
        "PATCH",
        &format!("/api/v1/objects/{id}"),
        &[],
        json!({"metadata": {"set": {"k": "v"}}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

#[tokio::test]
async fn unreadable_idempotency_key_is_rejected() {
    let app = app();
    let (status, body) = send(
        &app,
        "POST",
        "/api/v1/objects",
        &[(
            "idempotency-key",
            HeaderValue::from_bytes(b"key-\xe2").unwrap(),
        )],
        json!({"kind": "folder", "name": "docs"}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"], "invalid_request");
}

#[tokio::test]
async fn the_maintenance_trigger_requires_the_api_token_and_runs_a_pass() {
    let app = app_with_auth(AuthConfig::bearer("s3cret"));

    let (status, body) = send(&app, "POST", "/internal/maintenance", &[], Value::Null).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");

    let (status, body) = send(
        &app,
        "POST",
        "/internal/maintenance",
        &[("authorization", HeaderValue::from_static("Bearer s3cret"))],
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["status"], "completed");
    assert_eq!(body["drained"], true);
}
