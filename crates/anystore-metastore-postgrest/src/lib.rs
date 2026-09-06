//! CloudBase PostgREST `MetaStore` adapter.
//!
//! CloudBase shared PostgreSQL is reachable over HTTP only, so this adapter
//! cannot open a SQL session and cannot drive its own transactions. Ordinary
//! PostgREST table CRUD would split "revision check, mutation, Change append
//! and idempotency finalisation" across several requests, which is exactly the
//! atomicity the architecture forbids losing.
//!
//! Every persistence operation is therefore one call to the `anystore_rpc`
//! database function installed by
//! `cloudbase/migrations/20260901213000_anystore_postgrest_rpc.sql`. A single
//! function call is atomic, so each operation keeps the same one-transaction
//! guarantee the sqlx adapter gets from an explicit `BEGIN`/`COMMIT`.
//!
//! Because a `ResponseRenderer` closure cannot cross HTTP, mutation responses
//! are rendered by the database function and returned as opaque bytes. The very
//! same bytes are persisted with the idempotency record, so an idempotent
//! replay stays byte-identical.

mod changes;
mod client;
mod decode;
mod envelope;
mod idempotency;
mod maintenance;
mod objects_read;
mod objects_write;
mod uploads;

pub use client::{DEFAULT_FUNCTION_NAME, DEFAULT_RPC_PATH, DEFAULT_TIMEOUT};
pub use envelope::{RpcError, decode_envelope, map_http_status};

use anystore_domain::error::{DomainError, DomainResult};
use client::RpcClient;
use std::fmt;
use std::time::Duration;

/// A credential that never renders itself.
///
/// The API key is a deployment secret: it must not reach logs, `Debug` output
/// or an error message.
#[derive(Clone)]
pub struct ApiKey(String);

impl ApiKey {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// The only way to read the secret, named so that call sites are auditable.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ApiKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ApiKey(redacted)")
    }
}

impl From<String> for ApiKey {
    fn from(value: String) -> Self {
        Self(value)
    }
}

#[derive(Clone)]
pub struct PostgrestConfig {
    /// Gateway origin, e.g. `https://<env>.api.tcloudbasegateway.com`.
    pub base_url: String,
    pub api_key: ApiKey,
    /// PostgREST RPC prefix; `/v1/rdb/rest/rpc` on CloudBase.
    pub rpc_path: String,
    /// Database function that dispatches every AnyStore operation.
    pub function_name: String,
    pub timeout: Duration,
}

impl PostgrestConfig {
    pub fn new(base_url: impl Into<String>, api_key: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            api_key: ApiKey::new(api_key),
            rpc_path: DEFAULT_RPC_PATH.to_owned(),
            function_name: DEFAULT_FUNCTION_NAME.to_owned(),
            timeout: DEFAULT_TIMEOUT,
        }
    }
}

impl fmt::Debug for PostgrestConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgrestConfig")
            .field("base_url", &self.base_url)
            .field("rpc_path", &self.rpc_path)
            .field("function_name", &self.function_name)
            .field("timeout", &self.timeout)
            .field("api_key", &self.api_key)
            .finish()
    }
}

/// Production `MetaStore` implementation backed by CloudBase PostgreSQL over
/// PostgREST.
pub struct PostgrestMetaStore {
    client: RpcClient,
}

impl fmt::Debug for PostgrestMetaStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgrestMetaStore")
            .field("endpoint", &self.client.endpoint())
            .finish_non_exhaustive()
    }
}

impl PostgrestMetaStore {
    pub fn connect(config: PostgrestConfig) -> DomainResult<Self> {
        Ok(Self {
            client: RpcClient::new(config)?,
        })
    }

    pub(crate) fn client(&self) -> &RpcClient {
        &self.client
    }

    /// Endpoint the adapter posts to. Contains no credential.
    pub fn endpoint(&self) -> &str {
        self.client.endpoint()
    }

    /// Verifies the RPC surface is installed before serving traffic.
    ///
    /// A missing dispatcher means the RPC migration has not been applied, which
    /// must fail startup rather than every request.
    pub async fn check_schema(&self) -> DomainResult<()> {
        let value = self
            .client
            .call("get_object", serde_json::json!({"id": "root"}))
            .await
            .map_err(|error| match error {
                DomainError::Internal(inner) => DomainError::internal(format!(
                    "cloudbase postgrest RPC surface is unavailable; \
                     apply the anystore RPC migration ({inner})"
                )),
                other => other,
            })?;
        if value.get("id").and_then(serde_json::Value::as_str) != Some("root") {
            return Err(DomainError::internal(
                "cloudbase postgrest schema check could not read the root object",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credentials_never_render() {
        let config = PostgrestConfig::new("https://example.test", "super-secret-key");
        let rendered = format!("{config:?}");
        assert!(!rendered.contains("super-secret-key"), "{rendered}");
        assert!(rendered.contains("ApiKey(redacted)"), "{rendered}");
        assert_eq!(format!("{:?}", config.api_key), "ApiKey(redacted)");
    }

    #[test]
    fn store_debug_shows_only_the_endpoint() {
        let store =
            PostgrestMetaStore::connect(PostgrestConfig::new("https://example.test", "secret"))
                .expect("connect");
        let rendered = format!("{store:?}");
        assert!(!rendered.contains("secret"), "{rendered}");
        assert!(
            rendered.contains("/v1/rdb/rest/rpc/anystore_rpc"),
            "{rendered}"
        );
    }

    #[test]
    fn endpoint_tolerates_a_trailing_slash() {
        let store =
            PostgrestMetaStore::connect(PostgrestConfig::new("https://example.test/", "secret"))
                .expect("connect");
        assert_eq!(
            store.endpoint(),
            "https://example.test/v1/rdb/rest/rpc/anystore_rpc"
        );
    }

    #[test]
    fn an_empty_base_url_is_rejected() {
        assert!(PostgrestMetaStore::connect(PostgrestConfig::new("  ", "secret")).is_err());
    }

    #[test]
    fn an_empty_api_key_is_rejected() {
        assert!(
            PostgrestMetaStore::connect(PostgrestConfig::new("https://example.test", " ")).is_err()
        );
    }
}
