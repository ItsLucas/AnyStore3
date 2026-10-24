//! AnyStore server entry point.

mod config;
mod schedule;

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
use schedule::DailySchedule;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        // `Internal` displays an opaque public message; the operator needs the
        // underlying cause, which is deliberately free of credentials.
        match std::error::Error::source(&error) {
            Some(cause) => eprintln!("anystore-server failed to start: {cause}"),
            None => eprintln!("anystore-server failed to start: {error}"),
        }
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
    for key in &config.ignored_settings {
        tracing::warn!("{key} is no longer read and can be removed");
    }

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

    let maintenance = MaintenanceService::new(Arc::clone(&app));

    let http_state = HttpState {
        app: Arc::clone(&app),
        maintenance: maintenance.clone(),
        maintenance_budget: config.maintenance_budget,
        auth: Arc::new(match &config.auth_token {
            Some(token) => AuthConfig::bearer(token.clone()),
            None => {
                tracing::warn!("authentication is disabled (ANYSTORE_ALLOW_ANONYMOUS=true)");
                AuthConfig::disabled()
            }
        }),
        metrics: Arc::clone(&metrics),
    };

    let mut router = anystore_http::router(http_state);
    if let Some(dev) = dev_router {
        router = router.merge(dev);
    }

    if let Some(schedule) = config.maintenance_schedule {
        spawn_maintenance(maintenance, schedule, config.maintenance_budget);
    }

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

/// Runs one maintenance pass a day at the scheduled wall-clock time.
///
/// Only fires while this process is alive at that time; a deployment that
/// scales to zero must also call `POST /internal/maintenance` from an external
/// scheduler. Failures are logged and never surfaced to the API.
fn spawn_maintenance(service: MaintenanceService, schedule: DailySchedule, budget: Duration) {
    tokio::spawn(async move {
        loop {
            let next = schedule.next_after(Utc::now());
            tracing::info!(next_run = %next, "maintenance scheduled");
            let wait = (next - Utc::now()).to_std().unwrap_or_default();
            tokio::time::sleep(wait).await;

            match service.run(Utc::now(), Instant::now() + budget).await {
                Ok(Some(report)) => tracing::info!(?report, "scheduled maintenance pass complete"),
                Ok(None) => tracing::info!("maintenance already running; skipping scheduled pass"),
                Err(error) => tracing::warn!("maintenance pass failed: {error}"),
            }
        }
    });
}

/// Resolves on SIGINT or SIGTERM. The binary runs as PID 1 in its container,
/// where the kernel ignores SIGTERM unless a handler is installed, so without
/// this every scale-in would wait out the grace period and end in SIGKILL.
async fn shutdown_signal() {
    let interrupt = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(error) => {
                tracing::warn!("cannot listen for SIGTERM: {error}");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = interrupt => tracing::info!("SIGINT received; draining in-flight requests"),
        () = terminate => tracing::info!("SIGTERM received; draining in-flight requests"),
    }
}
