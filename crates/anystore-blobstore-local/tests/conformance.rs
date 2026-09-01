//! Runs the shared `BlobStore` conformance suite against the local adapter.

use anystore_blobstore::{BlobStore, SignedPart, SignedRequest};
use anystore_blobstore_local::LocalFsBlobStore;
use anystore_testkit::conformance::blobstore::{self, BlobUploader};
use async_trait::async_trait;
use std::path::PathBuf;

/// Writes to the path the signed URL points at, standing in for the client's
/// direct PUT.
struct FsUploader {
    root: PathBuf,
}

impl FsUploader {
    fn target(&self, url: &str) -> PathBuf {
        let path = url
            .split("/dev-blobs/")
            .nth(1)
            .expect("signed URL contains a blob path");
        let path = path.split('?').next().unwrap_or(path);
        self.root.join(path)
    }

    async fn write(&self, url: &str, bytes: &[u8]) {
        let target = self.target(url);
        if let Some(parent) = target.parent() {
            tokio::fs::create_dir_all(parent).await.unwrap();
        }
        tokio::fs::write(&target, bytes).await.unwrap();
    }
}

#[async_trait]
impl BlobUploader for FsUploader {
    async fn put(&self, request: &SignedRequest, bytes: &[u8]) {
        self.write(&request.url, bytes).await;
    }

    async fn put_part(&self, part: &SignedPart, bytes: &[u8]) {
        self.write(&part.url, bytes).await;
    }
}

#[tokio::test]
async fn local_blobstore_is_conformant() {
    let root = std::env::temp_dir().join(format!("anystore-conformance-{}", std::process::id()));
    tokio::fs::create_dir_all(&root).await.unwrap();

    let store = LocalFsBlobStore::new(root.clone(), "http://127.0.0.1:0", b"test-secret");
    let uploader = FsUploader { root: root.clone() };

    blobstore::run_all(&store as &dyn BlobStore, &uploader).await;

    tokio::fs::remove_dir_all(&root).await.ok();
}
