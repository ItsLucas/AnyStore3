//! Runs the port conformance suites against the in-memory adapters.

use anystore_blobstore::{SignedPart, SignedRequest};
use anystore_testkit::conformance::blobstore::{self, BlobUploader};
use anystore_testkit::conformance::metastore::{self, MetaStoreFactory};
use anystore_testkit::{InMemoryBlobStore, InMemoryMetaStore};
use async_trait::async_trait;
use std::sync::Arc;

struct InMemoryFactory;

#[async_trait]
impl MetaStoreFactory for InMemoryFactory {
    type Store = InMemoryMetaStore;

    async fn create(&self) -> Arc<Self::Store> {
        Arc::new(InMemoryMetaStore::new())
    }
}

/// Writes directly into the store, standing in for a client PUT to a signed URL.
struct MemoryUploader {
    store: Arc<InMemoryBlobStore>,
}

fn blob_from_url(url: &str) -> anystore_blobstore::BlobRef {
    let path = url.trim_start_matches("memory://");
    let path = path.split('?').next().unwrap_or(path);
    let path = path.split("/parts/").next().unwrap_or(path);
    anystore_blobstore::BlobRef::new(path)
}

fn part_number_from_url(url: &str) -> u32 {
    url.rsplit("/parts/")
        .next()
        .and_then(|n| n.parse().ok())
        .unwrap_or(1)
}

#[async_trait]
impl BlobUploader for MemoryUploader {
    async fn put(&self, request: &SignedRequest, bytes: &[u8]) {
        self.store.put(&blob_from_url(&request.url), bytes);
    }

    async fn put_part(&self, part: &SignedPart, bytes: &[u8]) -> String {
        self.store.put_part(
            &blob_from_url(&part.url),
            part_number_from_url(&part.url),
            bytes,
        );
        format!("memory-etag-{}", part.part_number)
    }
}

#[tokio::test]
async fn in_memory_metastore_is_conformant() {
    metastore::run_all(&InMemoryFactory).await;
}

#[tokio::test]
async fn in_memory_blobstore_is_conformant() {
    let store = Arc::new(InMemoryBlobStore::new());
    let uploader = MemoryUploader {
        store: Arc::clone(&store),
    };
    blobstore::run_all(&*store, &uploader).await;
}
