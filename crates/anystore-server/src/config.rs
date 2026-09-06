//! Deployment configuration.
//!
//! Secrets are read from the environment and never logged or returned.

use anystore_application::AppConfig;
use anystore_domain::error::{DomainError, DomainResult};
use anystore_metastore_postgrest::{
    ApiKey, DEFAULT_FUNCTION_NAME, DEFAULT_RPC_PATH, PostgrestConfig,
};
use chrono::Duration;
use std::env;
use std::fmt;
use std::time::Duration as StdDuration;

#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub port: u16,
    pub database: DatabaseBackend,
    pub auth_token: Option<String>,
    pub public_base_url: String,
    pub blob_backend: BlobBackend,
    pub maintenance_interval_seconds: u64,
    pub app: AppConfig,
}

/// Where object metadata lives.
///
/// CloudBase shared PostgreSQL is reachable over HTTP only, so production uses
/// the PostgREST RPC adapter; the sqlx adapter stays for local development and
/// for the conformance suites.
#[derive(Clone)]
pub enum DatabaseBackend {
    Postgres { url: String, migrate_on_start: bool },
    CloudBasePostgrest(PostgrestConfig),
}

impl fmt::Debug for DatabaseBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // The connection string embeds a password.
            Self::Postgres {
                migrate_on_start, ..
            } => f
                .debug_struct("Postgres")
                .field("url", &"[redacted]")
                .field("migrate_on_start", migrate_on_start)
                .finish(),
            Self::CloudBasePostgrest(config) => {
                f.debug_tuple("CloudBasePostgrest").field(config).finish()
            }
        }
    }
}

#[derive(Clone)]
pub enum BlobBackend {
    LocalFs {
        root: String,
        secret: String,
    },
    TencentCos {
        bucket: String,
        endpoint: String,
        secret_id: String,
        secret_key: String,
        session_token: Option<String>,
    },
}

impl fmt::Debug for BlobBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LocalFs { root, .. } => f
                .debug_struct("LocalFs")
                .field("root", root)
                .finish_non_exhaustive(),
            Self::TencentCos {
                bucket, endpoint, ..
            } => f
                .debug_struct("TencentCos")
                .field("bucket", bucket)
                .field("endpoint", endpoint)
                .finish_non_exhaustive(),
        }
    }
}

/// Reads one configuration value. Injectable so the parsing rules can be tested
/// without mutating the process environment.
pub trait ConfigSource {
    fn get(&self, key: &str) -> Option<String>;
}

struct EnvSource;

impl ConfigSource for EnvSource {
    fn get(&self, key: &str) -> Option<String> {
        env::var(key).ok()
    }
}

fn var(source: &dyn ConfigSource, key: &str) -> Option<String> {
    source.get(key).filter(|v| !v.trim().is_empty())
}

fn required(source: &dyn ConfigSource, key: &str) -> DomainResult<String> {
    var(source, key).ok_or_else(|| DomainError::internal(format!("{key} must be set")))
}

fn parse<T: std::str::FromStr>(
    source: &dyn ConfigSource,
    key: &str,
    default: T,
) -> DomainResult<T> {
    match var(source, key) {
        None => Ok(default),
        Some(raw) => raw
            .trim()
            .parse()
            .map_err(|_| DomainError::internal(format!("{key} is not a valid value"))),
    }
}

impl ServerConfig {
    pub fn from_env() -> DomainResult<Self> {
        Self::from_source(&EnvSource)
    }

    pub fn from_source(source: &dyn ConfigSource) -> DomainResult<Self> {
        let backend_name =
            var(source, "ANYSTORE_BLOB_BACKEND").unwrap_or_else(|| "local_fs".to_owned());
        let blob_backend = match backend_name.as_str() {
            "local_fs" => BlobBackend::LocalFs {
                root: var(source, "ANYSTORE_LOCAL_BLOB_ROOT")
                    .unwrap_or_else(|| "./.anystore-blobs".to_owned()),
                secret: var(source, "ANYSTORE_LOCAL_BLOB_SECRET")
                    .unwrap_or_else(|| "anystore-development-secret".to_owned()),
            },
            "tencent_cos" => BlobBackend::TencentCos {
                bucket: required(source, "ANYSTORE_COS_BUCKET")?,
                endpoint: required(source, "ANYSTORE_COS_ENDPOINT")?,
                secret_id: required(source, "ANYSTORE_COS_SECRET_ID")?,
                secret_key: required(source, "ANYSTORE_COS_SECRET_KEY")?,
                session_token: var(source, "ANYSTORE_COS_SESSION_TOKEN"),
            },
            other => {
                return Err(DomainError::internal(format!(
                    "unsupported ANYSTORE_BLOB_BACKEND {other:?}"
                )));
            }
        };

        let database_backend =
            var(source, "ANYSTORE_DATABASE_BACKEND").unwrap_or_else(|| "postgres".to_owned());
        let database = match database_backend.as_str() {
            "postgres" => DatabaseBackend::Postgres {
                url: required(source, "ANYSTORE_DATABASE_URL")?,
                migrate_on_start: parse(source, "ANYSTORE_MIGRATE_ON_START", true)?,
            },
            // Migrations are applied out of band for this backend: the HTTP
            // gateway offers no migration channel.
            "cloudbase_postgrest" => DatabaseBackend::CloudBasePostgrest(PostgrestConfig {
                base_url: required(source, "ANYSTORE_CLOUDBASE_PG_BASE_URL")?,
                api_key: ApiKey::new(required(source, "ANYSTORE_CLOUDBASE_PG_API_KEY")?),
                rpc_path: var(source, "ANYSTORE_CLOUDBASE_PG_RPC_PATH")
                    .unwrap_or_else(|| DEFAULT_RPC_PATH.to_owned()),
                function_name: var(source, "ANYSTORE_CLOUDBASE_PG_FUNCTION")
                    .unwrap_or_else(|| DEFAULT_FUNCTION_NAME.to_owned()),
                timeout: StdDuration::from_secs(parse(
                    source,
                    "ANYSTORE_CLOUDBASE_PG_TIMEOUT_SECONDS",
                    20u64,
                )?),
            }),
            other => {
                return Err(DomainError::internal(format!(
                    "unsupported ANYSTORE_DATABASE_BACKEND {other:?}"
                )));
            }
        };

        // CloudBase Run and most PaaS hosts inject PORT.
        let port: u16 = match var(source, "ANYSTORE_PORT") {
            Some(_) => parse(source, "ANYSTORE_PORT", 8088)?,
            None => parse(source, "PORT", 8088)?,
        };

        let app = AppConfig {
            idempotency_retention: Duration::hours(parse(
                source,
                "ANYSTORE_IDEMPOTENCY_RETENTION_HOURS",
                24i64,
            )?),
            changes_retention: Duration::days(parse(
                source,
                "ANYSTORE_CHANGES_RETENTION_DAYS",
                30i64,
            )?),
            changes_cursor_ttl: Duration::days(parse(
                source,
                "ANYSTORE_CHANGES_CURSOR_TTL_DAYS",
                30i64,
            )?),
            upload_expiry: Duration::hours(parse(source, "ANYSTORE_UPLOAD_EXPIRY_HOURS", 24i64)?),
            download_url_expiry: Duration::seconds(parse(
                source,
                "ANYSTORE_DOWNLOAD_URL_EXPIRY_SECONDS",
                600i64,
            )?),
            upload_url_expiry: Duration::seconds(parse(
                source,
                "ANYSTORE_UPLOAD_URL_EXPIRY_SECONDS",
                21_600i64,
            )?),
            multipart_threshold: parse(
                source,
                "ANYSTORE_MULTIPART_THRESHOLD_BYTES",
                64u64 * 1024 * 1024,
            )?,
            part_size: parse(source, "ANYSTORE_PART_SIZE_BYTES", 16u64 * 1024 * 1024)?,
            ..AppConfig::default()
        };

        Ok(Self {
            port,
            database,
            auth_token: var(source, "ANYSTORE_AUTH_TOKEN"),
            public_base_url: var(source, "ANYSTORE_PUBLIC_BASE_URL")
                .unwrap_or_else(|| format!("http://127.0.0.1:{port}")),
            blob_backend,
            maintenance_interval_seconds: parse(
                source,
                "ANYSTORE_MAINTENANCE_INTERVAL_SECONDS",
                300u64,
            )?,
            app,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    struct MapSource(HashMap<String, String>);

    impl MapSource {
        fn new(pairs: &[(&str, &str)]) -> Self {
            Self(
                pairs
                    .iter()
                    .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                    .collect(),
            )
        }
    }

    impl ConfigSource for MapSource {
        fn get(&self, key: &str) -> Option<String> {
            self.0.get(key).cloned()
        }
    }

    /// Configuration failures are internal errors, whose public message is
    /// deliberately opaque; the offending variable is in the source.
    fn detail(error: &DomainError) -> String {
        std::error::Error::source(error)
            .map(|source| source.to_string())
            .unwrap_or_default()
    }

    #[test]
    fn postgres_stays_the_default_backend() {
        let config = ServerConfig::from_source(&MapSource::new(&[(
            "ANYSTORE_DATABASE_URL",
            "postgres://user:pw@localhost/anystore",
        )]))
        .expect("config");
        match config.database {
            DatabaseBackend::Postgres {
                url,
                migrate_on_start,
            } => {
                assert_eq!(url, "postgres://user:pw@localhost/anystore");
                assert!(migrate_on_start);
            }
            other => panic!("expected postgres, got {other:?}"),
        }
        assert_eq!(config.port, 8088);
    }

    #[test]
    fn postgres_requires_a_database_url() {
        let error = ServerConfig::from_source(&MapSource::new(&[])).unwrap_err();
        assert!(detail(&error).contains("ANYSTORE_DATABASE_URL"));
    }

    #[test]
    fn cloudbase_postgrest_reads_its_own_settings() {
        let config = ServerConfig::from_source(&MapSource::new(&[
            ("ANYSTORE_DATABASE_BACKEND", "cloudbase_postgrest"),
            (
                "ANYSTORE_CLOUDBASE_PG_BASE_URL",
                "https://env.api.tcloudbasegateway.com",
            ),
            ("ANYSTORE_CLOUDBASE_PG_API_KEY", "top-secret"),
            ("ANYSTORE_CLOUDBASE_PG_TIMEOUT_SECONDS", "45"),
        ]))
        .expect("config");

        match &config.database {
            DatabaseBackend::CloudBasePostgrest(postgrest) => {
                assert_eq!(postgrest.base_url, "https://env.api.tcloudbasegateway.com");
                assert_eq!(postgrest.rpc_path, "/v1/rdb/rest/rpc");
                assert_eq!(postgrest.function_name, "anystore_rpc");
                assert_eq!(postgrest.timeout, StdDuration::from_secs(45));
                assert_eq!(postgrest.api_key.expose(), "top-secret");
            }
            other => panic!("expected cloudbase postgrest, got {other:?}"),
        }
    }

    #[test]
    fn cloudbase_postgrest_does_not_need_a_database_url() {
        assert!(
            ServerConfig::from_source(&MapSource::new(&[
                ("ANYSTORE_DATABASE_BACKEND", "cloudbase_postgrest"),
                ("ANYSTORE_CLOUDBASE_PG_BASE_URL", "https://env.example"),
                ("ANYSTORE_CLOUDBASE_PG_API_KEY", "k"),
            ]))
            .is_ok()
        );
    }

    #[test]
    fn cloudbase_postgrest_requires_its_credential_and_endpoint() {
        let error = ServerConfig::from_source(&MapSource::new(&[
            ("ANYSTORE_DATABASE_BACKEND", "cloudbase_postgrest"),
            ("ANYSTORE_CLOUDBASE_PG_BASE_URL", "https://env.example"),
        ]))
        .unwrap_err();
        assert!(detail(&error).contains("ANYSTORE_CLOUDBASE_PG_API_KEY"));

        let error = ServerConfig::from_source(&MapSource::new(&[
            ("ANYSTORE_DATABASE_BACKEND", "cloudbase_postgrest"),
            ("ANYSTORE_CLOUDBASE_PG_API_KEY", "k"),
        ]))
        .unwrap_err();
        assert!(detail(&error).contains("ANYSTORE_CLOUDBASE_PG_BASE_URL"));
    }

    #[test]
    fn an_unknown_database_backend_is_rejected() {
        let error =
            ServerConfig::from_source(&MapSource::new(&[("ANYSTORE_DATABASE_BACKEND", "mysql")]))
                .unwrap_err();
        assert!(detail(&error).contains("ANYSTORE_DATABASE_BACKEND"));
    }

    #[test]
    fn configuration_debug_output_holds_no_secret() {
        let config = ServerConfig::from_source(&MapSource::new(&[
            ("ANYSTORE_DATABASE_BACKEND", "cloudbase_postgrest"),
            ("ANYSTORE_CLOUDBASE_PG_BASE_URL", "https://env.example"),
            ("ANYSTORE_CLOUDBASE_PG_API_KEY", "top-secret"),
            ("ANYSTORE_BLOB_BACKEND", "tencent_cos"),
            ("ANYSTORE_COS_BUCKET", "anystore-125"),
            ("ANYSTORE_COS_ENDPOINT", "cos.ap-shanghai.myqcloud.com"),
            ("ANYSTORE_COS_SECRET_ID", "cos-id"),
            ("ANYSTORE_COS_SECRET_KEY", "cos-key"),
        ]))
        .expect("config");

        let rendered = format!("{config:?}");
        for secret in ["top-secret", "cos-id", "cos-key"] {
            assert!(
                !rendered.contains(secret),
                "{secret} leaked into {rendered}"
            );
        }
    }

    #[test]
    fn the_postgres_url_is_redacted_in_debug_output() {
        let config = ServerConfig::from_source(&MapSource::new(&[(
            "ANYSTORE_DATABASE_URL",
            "postgres://user:hunter2@localhost/anystore",
        )]))
        .expect("config");
        assert!(!format!("{config:?}").contains("hunter2"));
    }

    #[test]
    fn the_platform_port_is_honoured_when_anystore_port_is_unset() {
        let config = ServerConfig::from_source(&MapSource::new(&[
            ("ANYSTORE_DATABASE_URL", "postgres://localhost/anystore"),
            ("PORT", "9090"),
        ]))
        .expect("config");
        assert_eq!(config.port, 9090);
        assert_eq!(config.public_base_url, "http://127.0.0.1:9090");
    }

    #[test]
    fn an_unparsable_number_is_rejected() {
        let error = ServerConfig::from_source(&MapSource::new(&[
            ("ANYSTORE_DATABASE_URL", "postgres://localhost/anystore"),
            ("ANYSTORE_CHANGES_RETENTION_DAYS", "forever"),
        ]))
        .unwrap_err();
        assert!(detail(&error).contains("ANYSTORE_CHANGES_RETENTION_DAYS"));
    }
}
