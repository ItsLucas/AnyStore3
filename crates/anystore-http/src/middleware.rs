//! Request pipeline: request id, authentication, metrics and tracing.

use anystore_application::Metrics;
use anystore_domain::error::DomainError;
use anystore_domain::{PrincipalId, RequestId};
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use std::time::Instant;

use crate::HttpState;
use crate::error::error_response;
use crate::request::RequestMeta;

/// Static bearer token authentication.
///
/// When no token is configured, authentication is disabled and every request
/// runs as principal `local`.
#[derive(Clone, Debug, Default)]
pub struct AuthConfig {
    pub token: Option<String>,
}

impl AuthConfig {
    pub fn disabled() -> Self {
        Self { token: None }
    }

    pub fn bearer(token: impl Into<String>) -> Self {
        Self {
            token: Some(token.into()),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.token.is_some()
    }
}

/// Constant-time comparison so a token cannot be recovered byte by byte.
fn secret_eq(a: &str, b: &str) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.bytes()
        .zip(b.bytes())
        .fold(0u8, |acc, (x, y)| acc | (x ^ y))
        == 0
}

fn authenticate(config: &AuthConfig, headers: &HeaderMap) -> Result<PrincipalId, DomainError> {
    let Some(expected) = &config.token else {
        return Ok(PrincipalId::local());
    };

    let presented = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::trim)
        .unwrap_or_default();

    if secret_eq(presented, expected) {
        // A single shared token maps to one identity, so the idempotency scope
        // is unchanged whether or not authentication is enabled.
        Ok(PrincipalId::local())
    } else {
        Err(DomainError::Unauthorized)
    }
}

/// Paths served before authentication so orchestrators can probe the service.
fn is_public(path: &str) -> bool {
    matches!(path, "/healthz" | "/metrics")
}

pub async fn pipeline(State(state): State<HttpState>, mut req: Request, next: Next) -> Response {
    // Every request is identified before any database or blob call.
    let request_id = RequestId::generate();
    let path = req.uri().path().to_owned();
    let method = req.method().clone();

    let principal = if is_public(&path) {
        PrincipalId::local()
    } else {
        match authenticate(&state.auth, req.headers()) {
            Ok(principal) => principal,
            Err(error) => return error_response(&error, &request_id),
        }
    };

    req.extensions_mut().insert(RequestMeta {
        request_id: request_id.clone(),
        principal_id: principal,
    });

    let started = Instant::now();
    let mut response = next.run(req).await;
    let elapsed_ms = started.elapsed().as_millis() as u64;

    Metrics::incr(&state.app.metrics.http_requests_total);
    Metrics::add(
        &state.app.metrics.http_request_duration_ms_total,
        elapsed_ms,
    );

    if let Ok(value) = HeaderValue::from_str(request_id.as_str()) {
        response.headers_mut().insert("x-request-id", value);
    }

    tracing::info!(
        request_id = %request_id,
        method = %method,
        path = %path,
        status = response.status().as_u16(),
        latency_ms = elapsed_ms,
        "request completed"
    );

    response
}

pub async fn healthz() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers_with(auth: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(header::AUTHORIZATION, auth.parse().unwrap());
        headers
    }

    #[test]
    fn disabled_auth_yields_the_local_principal() {
        let principal = authenticate(&AuthConfig::disabled(), &HeaderMap::new()).unwrap();
        assert_eq!(principal.as_str(), "local");
    }

    #[test]
    fn correct_bearer_token_is_accepted() {
        let config = AuthConfig::bearer("s3cret");
        assert!(authenticate(&config, &headers_with("Bearer s3cret")).is_ok());
    }

    #[test]
    fn wrong_or_missing_token_is_rejected() {
        let config = AuthConfig::bearer("s3cret");
        assert!(authenticate(&config, &headers_with("Bearer nope")).is_err());
        assert!(authenticate(&config, &HeaderMap::new()).is_err());
        assert!(authenticate(&config, &headers_with("Basic s3cret")).is_err());
    }

    #[test]
    fn health_endpoints_bypass_authentication() {
        assert!(is_public("/healthz"));
        assert!(is_public("/metrics"));
        assert!(!is_public("/api/v1/objects"));
    }
}
