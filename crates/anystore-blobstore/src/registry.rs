//! Backend registry.
//!
//! A deployment may configure a single backend, but every object and upload
//! still records which backend holds its bytes.

use anystore_domain::error::{DomainError, DomainResult};
use std::collections::HashMap;
use std::sync::Arc;

use crate::BlobStore;

pub struct BlobRegistry {
    default_backend: String,
    backends: HashMap<String, Arc<dyn BlobStore>>,
}

impl BlobRegistry {
    pub fn new(default: Arc<dyn BlobStore>) -> Self {
        let id = default.backend_id().to_owned();
        let mut backends = HashMap::new();
        backends.insert(id.clone(), default);
        Self {
            default_backend: id,
            backends,
        }
    }

    pub fn with(mut self, store: Arc<dyn BlobStore>) -> Self {
        self.backends.insert(store.backend_id().to_owned(), store);
        self
    }

    pub fn default_backend_id(&self) -> &str {
        &self.default_backend
    }

    pub fn default_store(&self) -> Arc<dyn BlobStore> {
        Arc::clone(&self.backends[&self.default_backend])
    }

    /// Resolves the backend recorded on an object or upload.
    pub fn get(&self, backend_id: &str) -> DomainResult<Arc<dyn BlobStore>> {
        self.backends.get(backend_id).cloned().ok_or_else(|| {
            DomainError::internal(format!("blob backend {backend_id:?} is not configured"))
        })
    }
}
