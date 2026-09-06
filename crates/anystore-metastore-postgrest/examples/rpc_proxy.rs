//! Local stand-in for the CloudBase PostgREST gateway.
//!
//! Serves `POST /v1/rdb/rest/rpc/anystore_rpc` from a normal PostgreSQL
//! connection so the production `cloudbase_postgrest` backend can be exercised
//! end to end without a CloudBase environment. Development tooling only.
//!
//! ```text
//! cargo run -p anystore-metastore-postgrest --example rpc_proxy -- --migrate
//! ```
//!
//! Configuration: `ANYSTORE_TEST_DATABASE_URL`, `ANYSTORE_RPC_PROXY_PORT`
//! (default 8099) and `ANYSTORE_RPC_PROXY_API_KEY` (default
//! `local-proxy-key`).

use anystore_metastore_postgres::{PoolConfig, PostgresMetaStore};
use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use axum::routing::post;
use serde_json::{Value, json};
use sqlx::postgres::PgPool;

const RPC_MIGRATION: &str =
    include_str!("../../../cloudbase/migrations/20260901213000_anystore_postgrest_rpc.sql");

#[derive(Clone)]
struct ProxyState {
    pool: PgPool,
    api_key: String,
}

#[tokio::main]
async fn main() {
    let url = std::env::var("ANYSTORE_TEST_DATABASE_URL")
        .expect("ANYSTORE_TEST_DATABASE_URL must be set");
    let port: u16 = std::env::var("ANYSTORE_RPC_PROXY_PORT")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(8099);
    let api_key = std::env::var("ANYSTORE_RPC_PROXY_API_KEY")
        .unwrap_or_else(|_| "local-proxy-key".to_owned());

    let store = PostgresMetaStore::connect(&url, PoolConfig::default())
        .await
        .expect("connect");

    if std::env::args().any(|arg| arg == "--migrate") {
        store.migrate().await.expect("base migrations");
        sqlx::raw_sql(RPC_MIGRATION)
            .execute(store.pool())
            .await
            .expect("rpc migration");
        eprintln!("applied the base schema and the anystore RPC surface");
    }

    let router = Router::new()
        .route("/v1/rdb/rest/rpc/anystore_rpc", post(handle))
        .with_state(ProxyState {
            pool: store.pool().clone(),
            api_key,
        });

    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
        .await
        .expect("bind");
    eprintln!("rpc proxy listening on http://127.0.0.1:{port}");
    axum::serve(listener, router).await.expect("serve");
}

async fn handle(State(state): State<ProxyState>, headers: HeaderMap, body: Bytes) -> Response {
    let authorization = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if authorization != format!("Bearer {}", state.api_key) {
        return reply(401, json!({"message": "unauthorized"}).to_string());
    }

    let Ok(request) = serde_json::from_slice::<Value>(&body) else {
        return reply(400, json!({"message": "malformed request"}).to_string());
    };
    let Some(op) = request.get("op").and_then(Value::as_str) else {
        return reply(400, json!({"message": "missing op"}).to_string());
    };

    match sqlx::query_scalar::<_, Value>("SELECT anystore_rpc($1, $2)")
        .bind(op)
        .bind(request.get("payload").cloned().unwrap_or(json!({})))
        .fetch_one(&state.pool)
        .await
    {
        Ok(value) => reply(200, value.to_string()),
        Err(error) => reply(500, json!({"message": error.to_string()}).to_string()),
    }
}

fn reply(status: u16, body: String) -> Response {
    Response::builder()
        .status(StatusCode::from_u16(status).expect("status"))
        .header("content-type", "application/json")
        .body(body.into())
        .expect("response")
}
