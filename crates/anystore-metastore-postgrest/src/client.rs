//! RPC transport.
//!
//! One HTTP request carries one AnyStore persistence operation. Nothing here
//! ever logs, formats or otherwise reveals the API key.

use anystore_domain::error::{DomainError, DomainResult};
use reqwest::Client;
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue};
use serde_json::Value;
use std::time::Duration;

use crate::PostgrestConfig;
use crate::envelope::{decode_envelope, map_http_status};

/// CloudBase exposes PostgREST RPC under this prefix.
pub const DEFAULT_RPC_PATH: &str = "/v1/rdb/rest/rpc";

/// Dispatcher installed by the AnyStore RPC migration.
pub const DEFAULT_FUNCTION_NAME: &str = "anystore_rpc";

/// Bounded so a stalled gateway can never pin a request handler.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(20);

/// Truncation bound for gateway error bodies quoted in an internal error.
const MAX_QUOTED_BODY: usize = 512;

pub(crate) struct RpcClient {
    http: Client,
    endpoint: String,
    api_key: String,
}

impl RpcClient {
    pub(crate) fn new(config: PostgrestConfig) -> DomainResult<Self> {
        let base = config.base_url.trim().trim_end_matches('/');
        if base.is_empty() {
            return Err(DomainError::internal(
                "cloudbase postgrest base URL is required",
            ));
        }
        let api_key = config.api_key.expose().trim().to_owned();
        if api_key.is_empty() {
            return Err(DomainError::internal(
                "cloudbase postgrest API key is required",
            ));
        }

        let rpc_path = config.rpc_path.trim().trim_end_matches('/');
        let function = config.function_name.trim().trim_matches('/');
        if function.is_empty() {
            return Err(DomainError::internal(
                "cloudbase postgrest function name is required",
            ));
        }
        let endpoint = format!("{base}{rpc_path}/{function}");

        // The credential lives in a default header so no call site has to
        // handle it, and so it can never be interpolated into a log line.
        let mut authorization =
            HeaderValue::from_str(&format!("Bearer {api_key}")).map_err(|_| {
                DomainError::internal("cloudbase postgrest API key is not a valid header")
            })?;
        authorization.set_sensitive(true);

        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, authorization);
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        headers.insert(ACCEPT, HeaderValue::from_static("application/json"));

        let http = Client::builder()
            .timeout(config.timeout)
            .default_headers(headers)
            .build()
            .map_err(|e| DomainError::internal(format!("HTTP client build failed: {e}")))?;

        Ok(Self {
            http,
            endpoint,
            api_key,
        })
    }

    pub(crate) fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Defence in depth: a transport error must never echo the credential even
    /// if a future dependency starts including request headers in its message.
    fn redact(&self, text: String) -> String {
        text.replace(&self.api_key, "[redacted]")
    }

    /// Calls one operation and returns its `data` payload.
    pub(crate) async fn call(&self, op: &str, payload: Value) -> DomainResult<Value> {
        let body = serde_json::to_vec(&serde_json::json!({ "op": op, "payload": payload }))
            .map_err(|e| DomainError::internal(format!("rpc request encoding failed: {e}")))?;

        let response = self
            .http
            .post(&self.endpoint)
            .body(body)
            .send()
            .await
            .map_err(|e| {
                DomainError::internal(
                    self.redact(format!("cloudbase postgrest request failed: {e}")),
                )
            })?;

        let status = response.status().as_u16();
        let bytes = response.bytes().await.map_err(|e| {
            DomainError::internal(self.redact(format!("cloudbase postgrest read failed: {e}")))
        })?;

        if !(200..300).contains(&status) {
            let quoted: String = String::from_utf8_lossy(&bytes)
                .chars()
                .take(MAX_QUOTED_BODY)
                .collect();
            return Err(map_http_status(status, &self.redact(quoted)));
        }

        let value: Value = serde_json::from_slice(&bytes).map_err(|_| {
            DomainError::internal("cloudbase postgrest returned a non-JSON response")
        })?;
        decode_envelope(value)
    }

    /// Calls one operation that returns no payload.
    pub(crate) async fn call_unit(&self, op: &str, payload: Value) -> DomainResult<()> {
        self.call(op, payload).await.map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ApiKey;

    fn config() -> PostgrestConfig {
        PostgrestConfig {
            base_url: "https://env.api.tcloudbasegateway.com".into(),
            api_key: ApiKey::new("k-secret"),
            rpc_path: DEFAULT_RPC_PATH.into(),
            function_name: DEFAULT_FUNCTION_NAME.into(),
            timeout: DEFAULT_TIMEOUT,
        }
    }

    #[test]
    fn endpoint_is_built_from_base_path_and_function() {
        let client = RpcClient::new(config()).expect("client");
        assert_eq!(
            client.endpoint(),
            "https://env.api.tcloudbasegateway.com/v1/rdb/rest/rpc/anystore_rpc"
        );
    }

    #[test]
    fn a_custom_function_name_is_honoured() {
        let mut config = config();
        config.function_name = "custom_rpc".into();
        config.rpc_path = "/rest/v1/rpc/".into();
        let client = RpcClient::new(config).expect("client");
        assert_eq!(
            client.endpoint(),
            "https://env.api.tcloudbasegateway.com/rest/v1/rpc/custom_rpc"
        );
    }

    #[test]
    fn transport_errors_are_redacted() {
        let client = RpcClient::new(config()).expect("client");
        let message = client.redact("connect to host with k-secret failed".to_owned());
        assert!(!message.contains("k-secret"), "{message}");
        assert!(message.contains("[redacted]"), "{message}");
    }
}
