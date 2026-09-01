//! Composition of the ports the application depends on.

use anystore_blobstore::BlobRegistry;
use anystore_metastore::MetaStore;
use std::sync::Arc;

use crate::config::AppConfig;
use crate::metrics::Metrics;

#[derive(Clone)]
pub struct AppState {
    pub meta: Arc<dyn MetaStore>,
    pub blobs: Arc<BlobRegistry>,
    pub config: AppConfig,
    pub metrics: Arc<Metrics>,
}

impl AppState {
    pub fn new(
        meta: Arc<dyn MetaStore>,
        blobs: Arc<BlobRegistry>,
        config: AppConfig,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            meta,
            blobs,
            config,
            metrics,
        }
    }
}
