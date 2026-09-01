//! Deployment configuration.
//!
//! Secrets are read from the environment and never logged or returned.

use anystore_application::AppConfig;
use anystore_domain::error::{DomainError, DomainResult};
use chrono::Duration;
use std::env;

#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub port: u16,
    pub database_url: String,
    pub migrate_on_start: bool,
    pub auth_token: Option<String>,
    pub public_base_url: String,
    pub blob_backend: BlobBackend,
    pub maintenance_interval_seconds: u64,
    pub app: AppConfig,
}

#[derive(Clone, Debug)]
pub enum BlobBackend {
    LocalFs {
        root: String,
        secret: String,
    },
    TencentCos {
        bucket: String,
        endpoint: String,
        region: Option<String>,
        secret_id: String,
        secret_key: String,
        session_token: Option<String>,
    },
}

fn var(key: &str) -> Option<String> {
    env::var(key).ok().filter(|v| !v.trim().is_empty())
}

fn required(key: &str) -> DomainResult<String> {
    var(key).ok_or_else(|| DomainError::internal(format!("{key} must be set")))
}

fn parse<T: std::str::FromStr>(key: &str, default: T) -> DomainResult<T> {
    match var(key) {
        None => Ok(default),
        Some(raw) => raw
            .parse()
            .map_err(|_| DomainError::internal(format!("{key} is not a valid value"))),
    }
}

impl ServerConfig {
    pub fn from_env() -> DomainResult<Self> {
        let backend_name = var("ANYSTORE_BLOB_BACKEND").unwrap_or_else(|| "local_fs".to_owned());
        let blob_backend = match backend_name.as_str() {
            "local_fs" => BlobBackend::LocalFs {
                root: var("ANYSTORE_LOCAL_BLOB_ROOT")
                    .unwrap_or_else(|| "./.anystore-blobs".to_owned()),
                secret: var("ANYSTORE_LOCAL_BLOB_SECRET")
                    .unwrap_or_else(|| "anystore-development-secret".to_owned()),
            },
            "tencent_cos" => BlobBackend::TencentCos {
                bucket: required("ANYSTORE_COS_BUCKET")?,
                endpoint: required("ANYSTORE_COS_ENDPOINT")?,
                region: var("ANYSTORE_COS_REGION"),
                secret_id: required("ANYSTORE_COS_SECRET_ID")?,
                secret_key: required("ANYSTORE_COS_SECRET_KEY")?,
                session_token: var("ANYSTORE_COS_SESSION_TOKEN"),
            },
            other => {
                return Err(DomainError::internal(format!(
                    "unsupported ANYSTORE_BLOB_BACKEND {other:?}"
                )));
            }
        };

        let database_backend =
            var("ANYSTORE_DATABASE_BACKEND").unwrap_or_else(|| "postgres".to_owned());
        if database_backend != "postgres" {
            return Err(DomainError::internal(format!(
                "unsupported ANYSTORE_DATABASE_BACKEND {database_backend:?}"
            )));
        }

        let port: u16 = parse("ANYSTORE_PORT", 8088)?;

        let app = AppConfig {
            idempotency_retention: Duration::hours(parse("ANYSTORE_IDEMPOTENCY_RETENTION_HOURS", 24i64)?),
            changes_retention: Duration::days(parse("ANYSTORE_CHANGES_RETENTION_DAYS", 30i64)?),
            changes_cursor_ttl: Duration::days(parse("ANYSTORE_CHANGES_CURSOR_TTL_DAYS", 30i64)?),
            upload_expiry: Duration::hours(parse("ANYSTORE_UPLOAD_EXPIRY_HOURS", 24i64)?),
            download_url_expiry: Duration::seconds(parse(
                "ANYSTORE_DOWNLOAD_URL_EXPIRY_SECONDS",
                600i64,
            )?),
            upload_url_expiry: Duration::seconds(parse(
                "ANYSTORE_UPLOAD_URL_EXPIRY_SECONDS",
                21_600i64,
            )?),
            multipart_threshold: parse("ANYSTORE_MULTIPART_THRESHOLD_BYTES", 64u64 * 1024 * 1024)?,
            part_size: parse("ANYSTORE_PART_SIZE_BYTES", 16u64 * 1024 * 1024)?,
            ..AppConfig::default()
        };

        Ok(Self {
            port,
            database_url: required("ANYSTORE_DATABASE_URL")?,
            migrate_on_start: parse("ANYSTORE_MIGRATE_ON_START", true)?,
            auth_token: var("ANYSTORE_AUTH_TOKEN"),
            public_base_url: var("ANYSTORE_PUBLIC_BASE_URL")
                .unwrap_or_else(|| format!("http://127.0.0.1:{port}")),
            blob_backend,
            maintenance_interval_seconds: parse("ANYSTORE_MAINTENANCE_INTERVAL_SECONDS", 300u64)?,
            app,
        })
    }
}
