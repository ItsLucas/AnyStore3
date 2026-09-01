//! Request plumbing shared by handlers.

use anystore_application::{AppState, RequestContext, canonical_request_hash};
use anystore_domain::error::{DomainError, DomainResult};
use anystore_domain::object::Revision;
use anystore_domain::{IdempotencyKey, ObjectId, PrincipalId, RequestId, UploadId};
use anystore_metastore::response::StoredResponse;
use axum::body::Body;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::Response;
use chrono::Utc;
use serde_json::Value;

/// Per-request identity established by the middleware.
#[derive(Clone, Debug)]
pub struct RequestMeta {
    pub request_id: RequestId,
    pub principal_id: PrincipalId,
}

pub const IDEMPOTENCY_HEADER: &str = "idempotency-key";
pub const IF_MATCH_HEADER: &str = "if-match";

fn header_value(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(str::to_owned)
}

/// Parses `If-Match`.
///
/// AnyStore revisions are plain integers; a quoted ETag form is tolerated so
/// generic HTTP clients interoperate.
pub fn parse_if_match(headers: &HeaderMap) -> DomainResult<Option<Revision>> {
    let Some(raw) = header_value(headers, IF_MATCH_HEADER) else {
        return Ok(None);
    };
    let cleaned = raw.trim_matches('"');
    cleaned
        .parse::<u64>()
        .map(|value| Some(Revision(value)))
        .map_err(|_| {
            DomainError::InvalidRequest("If-Match must be an object revision number.".into())
        })
}

pub fn idempotency_key(headers: &HeaderMap) -> Option<IdempotencyKey> {
    header_value(headers, IDEMPOTENCY_HEADER).map(IdempotencyKey::new)
}

/// Assembles the application request context, including the canonical hash used
/// for idempotency.
#[allow(clippy::too_many_arguments)]
pub fn context(
    meta: &RequestMeta,
    _state: &AppState,
    headers: &HeaderMap,
    method: &str,
    route: &str,
    target: &str,
    query: &[(String, String)],
    body: &[u8],
) -> DomainResult<RequestContext> {
    let if_match = parse_if_match(headers)?;
    let request_hash = canonical_request_hash(
        &meta.principal_id,
        method,
        route,
        target,
        query,
        body,
        if_match,
    );

    Ok(RequestContext {
        request_id: meta.request_id.clone(),
        principal_id: meta.principal_id.clone(),
        operation: format!("{method} {route}"),
        idempotency_key: idempotency_key(headers),
        request_hash,
        if_match,
        now: Utc::now(),
    })
}

/// Parses a JSON request body, tolerating an empty body as `{}`.
pub fn parse_json(body: &[u8]) -> DomainResult<Value> {
    if body.iter().all(|b| b.is_ascii_whitespace()) {
        return Ok(Value::Object(Default::default()));
    }
    serde_json::from_slice(body)
        .map_err(|e| DomainError::InvalidRequest(format!("Invalid JSON body: {e}")))
}

pub fn object_id(raw: &str) -> DomainResult<ObjectId> {
    if raw.is_empty() {
        return Err(DomainError::InvalidRequest("Missing object id.".into()));
    }
    Ok(ObjectId::new(raw))
}

pub fn upload_id(raw: &str) -> DomainResult<UploadId> {
    if raw.is_empty() {
        return Err(DomainError::InvalidRequest("Missing upload id.".into()));
    }
    Ok(UploadId::new(raw))
}

/// Emits a stored mutation response verbatim, which is what makes an idempotent
/// replay byte-identical to the original.
pub fn stored_response(stored: StoredResponse, request_id: &RequestId) -> Response {
    let status = StatusCode::from_u16(stored.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let mut builder = Response::builder()
        .status(status)
        .header("x-request-id", request_id.as_str());

    for (name, value) in &stored.headers {
        builder = builder.header(name, value);
    }

    builder
        .body(Body::from(stored.body))
        .expect("stored response is always well formed")
}

pub fn json_response(status: StatusCode, value: &Value, request_id: &RequestId) -> Response {
    let body = serde_json::to_vec(value).unwrap_or_default();
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .header("x-request-id", request_id.as_str())
        .body(Body::from(body))
        .expect("json response is always well formed")
}

/// Collects query parameters, preserving repeats for the canonical hash.
pub fn query_pairs(raw: Option<&str>) -> Vec<(String, String)> {
    let Some(raw) = raw else {
        return Vec::new();
    };
    raw.split('&')
        .filter(|p| !p.is_empty())
        .map(|pair| match pair.split_once('=') {
            Some((k, v)) => (decode(k), decode(v)),
            None => (decode(pair), String::new()),
        })
        .collect()
}

fn decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or("");
                match u8::from_str_radix(hex, 16) {
                    Ok(byte) => {
                        out.push(byte);
                        i += 3;
                    }
                    Err(_) => {
                        out.push(bytes[i]);
                        i += 1;
                    }
                }
            }
            other => {
                out.push(other);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

pub fn find<'a>(pairs: &'a [(String, String)], key: &str) -> Option<&'a str> {
    pairs
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_str())
        .filter(|v| !v.is_empty())
}

pub fn parse_limit(pairs: &[(String, String)]) -> DomainResult<Option<u32>> {
    match find(pairs, "limit") {
        Some(raw) => raw
            .parse::<u32>()
            .map(Some)
            .map_err(|_| DomainError::InvalidRequest("limit must be a positive integer.".into())),
        None => Ok(None),
    }
}

pub fn parse_bool(pairs: &[(String, String)], key: &str) -> DomainResult<bool> {
    match find(pairs, key) {
        None => Ok(false),
        Some("true") => Ok(true),
        Some("false") => Ok(false),
        Some(_) => Err(DomainError::InvalidRequest(format!(
            "{key} must be true or false."
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn if_match_accepts_bare_and_quoted_revisions() {
        let mut headers = HeaderMap::new();
        headers.insert(IF_MATCH_HEADER, "7".parse().unwrap());
        assert_eq!(parse_if_match(&headers).unwrap(), Some(Revision(7)));

        headers.insert(IF_MATCH_HEADER, "\"7\"".parse().unwrap());
        assert_eq!(parse_if_match(&headers).unwrap(), Some(Revision(7)));
    }

    #[test]
    fn if_match_rejects_nonsense() {
        let mut headers = HeaderMap::new();
        headers.insert(IF_MATCH_HEADER, "abc".parse().unwrap());
        assert_eq!(
            parse_if_match(&headers).unwrap_err().code(),
            "invalid_request"
        );
    }

    #[test]
    fn query_pairs_are_percent_decoded() {
        let pairs = query_pairs(Some("path=%2Freports%2F2026&limit=10"));
        assert_eq!(find(&pairs, "path"), Some("/reports/2026"));
        assert_eq!(find(&pairs, "limit"), Some("10"));
    }

    #[test]
    fn empty_body_is_an_empty_object() {
        assert_eq!(parse_json(b"").unwrap(), serde_json::json!({}));
        assert_eq!(parse_json(b"  \n").unwrap(), serde_json::json!({}));
    }
}
