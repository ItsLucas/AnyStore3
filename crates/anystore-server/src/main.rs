//! AnyStore server entry point.

mod config;

use anystore_application::{AppState, MaintenanceService, Metrics};
use anystore_blobstore::{BlobRegistry, BlobStore};
use anystore_blobstore_cos::{CosConfig, TencentCosBlobStore};
use anystore_blobstore_local::LocalFsBlobStore;
use anystore_domain::error::DomainResult;
use anystore_http::{AuthConfig, HttpState};
use anystore_metastore::MetaStore;
use anystore_metastore_postgres::{PoolConfig, PostgresMetaStore};
use anystore_metastore_postgrest::PostgrestMetaStore;
use axum::Router;
use chrono::Utc;
use config::{BlobBackend, DatabaseBackend, ServerConfig};
use std::sync::Arc;
use std::time::Duration;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        // The message is deliberately free of credentials.
        eprintln!("anystore-server failed to start: {error}");
        std::process::exit(1);
    }
}

async fn run() -> DomainResult<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_env("ANYSTORE_LOG").unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let config = ServerConfig::from_env()?;

    let meta = connect_meta_store(&config.database).await?;

    let (blob_store, dev_router): (Arc<dyn BlobStore>, Option<Router>) = match &config.blob_backend
    {
        BlobBackend::LocalFs { root, secret } => {
            tokio::fs::create_dir_all(root).await.ok();
            let store = Arc::new(LocalFsBlobStore::new(
                root.clone(),
                config.public_base_url.clone(),
                secret.as_bytes(),
            ));
            tracing::warn!("using the local filesystem blob backend; not suitable for production");
            (
                Arc::clone(&store) as Arc<dyn BlobStore>,
                Some(anystore_blobstore_local::router(store)),
            )
        }
        BlobBackend::TencentCos {
            bucket,
            endpoint,
            secret_id,
            secret_key,
            session_token,
        } => {
            let store = TencentCosBlobStore::new(CosConfig {
                bucket: bucket.clone(),
                endpoint: endpoint.clone(),
                secret_id: secret_id.clone(),
                secret_key: secret_key.clone(),
                session_token: session_token.clone(),
            })?;
            (Arc::new(store) as Arc<dyn BlobStore>, None)
        }
    };

    let metrics = Arc::new(Metrics::new());
    let app = Arc::new(AppState::new(
        meta,
        Arc::new(BlobRegistry::new(blob_store)),
        config.app.clone(),
        Arc::clone(&metrics),
    ));

    let http_state = HttpState {
        app: Arc::clone(&app),
        auth: Arc::new(match &config.auth_token {
            Some(token) => AuthConfig::bearer(token.clone()),
            None => AuthConfig::disabled(),
        }),
        metrics: Arc::clone(&metrics),
    };

    let mut router = anystore_http::router(http_state);
    if let Some(dev) = dev_router {
        router = router.merge(dev);
    }

    spawn_maintenance(Arc::clone(&app), config.maintenance_interval_seconds);

    let address = format!("0.0.0.0:{}", config.port);
    let listener = tokio::net::TcpListener::bind(&address)
        .await
        .map_err(|e| anystore_domain::error::DomainError::internal(format!("bind failed: {e}")))?;

    tracing::info!(
        address = %address,
        auth = config.auth_token.is_some(),
        "anystore listening"
    );

    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(|e| anystore_domain::error::DomainError::internal(format!("serve failed: {e}")))
}

/// Builds the configured `MetaStore` and verifies its schema before any traffic
/// is served.
///
/// The application only ever sees the port, so which adapter answers is a
/// deployment decision rather than a code path anything else depends on.
async fn connect_meta_store(backend: &DatabaseBackend) -> DomainResult<Arc<dyn MetaStore>> {
    match backend {
        DatabaseBackend::Postgres {
            url,
            migrate_on_start,
        } => {
            let meta = PostgresMetaStore::connect(url, PoolConfig::default()).await?;
            if *migrate_on_start {
                meta.migrate().await?;
            }
            meta.check_schema().await?;
            Ok(Arc::new(meta))
        }
        DatabaseBackend::CloudBasePostgrest(config) => {
            let meta = PostgrestMetaStore::connect(config.clone())?;
            // The gateway offers no migration channel, so a missing RPC surface
            // must fail startup rather than every request.
            meta.check_schema().await?;
            tracing::info!(endpoint = %meta.endpoint(), "using CloudBase PostgreSQL over PostgREST");
            Ok(Arc::new(meta))
        }
    }
}

/// Runs maintenance on a schedule. Failures are logged and never surfaced to
/// the API.
fn spawn_maintenance(app: Arc<AppState>, interval_seconds: u64) {
    if interval_seconds == 0 {
        return;
    }
    tokio::spawn(async move {
        let service = MaintenanceService::new(Arc::clone(&app));
        let mut ticker = tokio::time::interval(Duration::from_secs(interval_seconds));
        ticker.tick().await;
        loop {
            ticker.tick().await;
            match service.run_once(Utc::now()).await {
                Ok(report) => {
                    if let Ok(pending) = app.meta.count_pending_gc().await {
                        Metrics::set(&app.metrics.blob_gc_pending, pending);
                    }
                    tracing::debug!(?report, "maintenance pass complete");
                }
                Err(error) => tracing::warn!("maintenance pass failed: {error}"),
            }
        }
    });
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("shutting down");
}
