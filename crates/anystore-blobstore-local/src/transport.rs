//! Development blob transport.
//!
//! Serves the signed PUT and GET URLs handed to clients. Mounted by the server
//! binary alongside the API so the upload and download paths behave like a real
//! provider endpoint.

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{DefaultBodyLimit, Path, RawQuery, State};
use axum::http::{StatusCode, header};
use axum::response::Response;
use axum::routing::get;
use chrono::Utc;
use std::sync::Arc;

use crate::{LocalFsBlobStore, safe_join, signing};

#[derive(Clone)]
struct TransportState {
    store: Arc<LocalFsBlobStore>,
}

pub fn router(store: Arc<LocalFsBlobStore>) -> Router {
    Router::new()
        .route("/dev-blobs/{*path}", get(download).put(upload))
        // Blob transfers are not JSON API calls, so the API body limit must not
        // apply to them.
        .layer(DefaultBodyLimit::disable())
        .with_state(TransportState { store })
}

fn param(query: Option<&str>, key: &str) -> Option<String> {
    let query = query?;
    query.split('&').find_map(|pair| {
        let (k, v) = pair.split_once('=')?;
        if k == key { Some(decode(v)) } else { None }
    })
}

fn decode(value: &str) -> String {
    percent_encoding::percent_decode_str(value)
        .decode_utf8_lossy()
        .into_owned()
}

fn deny(status: StatusCode, message: &str) -> Response {
    Response::builder()
        .status(status)
        .body(Body::from(message.to_owned()))
        .expect("static response is well formed")
}

fn check_signature(
    state: &TransportState,
    method: &str,
    path: &str,
    query: Option<&str>,
) -> Result<Option<String>, Box<Response>> {
    let expires: i64 = param(query, "exp")
        .and_then(|v| v.parse().ok())
        .ok_or_else(|| Box::new(deny(StatusCode::FORBIDDEN, "missing expiry")))?;
    let signature = param(query, "sig")
        .ok_or_else(|| Box::new(deny(StatusCode::FORBIDDEN, "missing signature")))?;
    let filename = param(query, "filename");

    if Utc::now().timestamp() > expires {
        return Err(Box::new(deny(StatusCode::FORBIDDEN, "signature expired")));
    }
    if !signing::verify(
        &state.store.inner.secret,
        method,
        path,
        expires,
        filename.as_deref(),
        &signature,
    ) {
        return Err(Box::new(deny(StatusCode::FORBIDDEN, "invalid signature")));
    }
    Ok(filename)
}

async fn upload(
    State(state): State<TransportState>,
    Path(path): Path<String>,
    RawQuery(query): RawQuery,
    body: Bytes,
) -> Response {
    if let Err(response) = check_signature(&state, "PUT", &path, query.as_deref()) {
        return *response;
    }

    let Ok(target) = safe_join(&state.store.inner.root, &path) else {
        return deny(StatusCode::BAD_REQUEST, "invalid path");
    };
    if let Some(parent) = target.parent()
        && tokio::fs::create_dir_all(parent).await.is_err()
    {
        return deny(StatusCode::INTERNAL_SERVER_ERROR, "cannot create directory");
    }
    if tokio::fs::write(&target, &body).await.is_err() {
        return deny(StatusCode::INTERNAL_SERVER_ERROR, "cannot write blob");
    }

    Response::builder()
        .status(StatusCode::OK)
        .header(header::ETAG, format!("\"{}\"", body.len()))
        .body(Body::empty())
        .expect("upload response is well formed")
}

async fn download(
    State(state): State<TransportState>,
    Path(path): Path<String>,
    RawQuery(query): RawQuery,
) -> Response {
    let filename = match check_signature(&state, "GET", &path, query.as_deref()) {
        Ok(filename) => filename,
        Err(response) => return *response,
    };

    let Ok(target) = safe_join(&state.store.inner.root, &path) else {
        return deny(StatusCode::BAD_REQUEST, "invalid path");
    };
    let Ok(bytes) = tokio::fs::read(&target).await else {
        return deny(StatusCode::NOT_FOUND, "not found");
    };

    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_LENGTH, bytes.len().to_string());

    // The download must present the object's current name, so the filename
    // travels with the signature rather than with the stored blob.
    if let Some(filename) = filename {
        builder = builder.header(
            header::CONTENT_DISPOSITION,
            format!(
                "attachment; filename*=UTF-8''{}",
                signing::encode_component(&filename)
            ),
        );
    }

    builder
        .body(Body::from(bytes))
        .expect("download response is well formed")
}
