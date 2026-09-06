//! Runs the shared `BlobStore` suite against a real COS-compatible bucket.
//!
//! Set the `ANYSTORE_TEST_COS_*` variables to enable this integration test.

use anystore_blobstore::{BlobStore, SignedPart, SignedRequest};
use anystore_blobstore_cos::{CosConfig, TencentCosBlobStore};
use anystore_testkit::conformance::blobstore::{self, BlobUploader};
use async_trait::async_trait;

struct HttpUploader {
    client: reqwest::Client,
}

impl HttpUploader {
    async fn put_bytes(&self, url: &str, bytes: &[u8]) -> reqwest::header::HeaderMap {
        let response = self
            .client
            .put(url)
            .body(bytes.to_vec())
            .send()
            .await
            .expect("signed COS PUT must be reachable");
        assert!(
            response.status().is_success(),
            "signed COS PUT failed with {}",
            response.status()
        );
        response.headers().clone()
    }
}

#[async_trait]
impl BlobUploader for HttpUploader {
    async fn put(&self, request: &SignedRequest, bytes: &[u8]) {
        self.put_bytes(&request.url, bytes).await;
    }

    async fn put_part(&self, part: &SignedPart, bytes: &[u8]) -> String {
        let headers = self.put_bytes(&part.url, bytes).await;
        headers
            .get(reqwest::header::ETAG)
            .and_then(|value| value.to_str().ok())
            .map(|value| value.trim_matches('"').to_owned())
            .expect("COS multipart PUT must return an ETag")
    }
}

fn required(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

#[tokio::test]
async fn cos_blobstore_is_conformant() {
    let Some(bucket) = required("ANYSTORE_TEST_COS_BUCKET") else {
        eprintln!("skipping: ANYSTORE_TEST_COS_BUCKET is not set");
        return;
    };
    let endpoint = required("ANYSTORE_TEST_COS_ENDPOINT")
        .expect("ANYSTORE_TEST_COS_ENDPOINT must accompany the bucket");
    let secret_id = required("ANYSTORE_TEST_COS_SECRET_ID")
        .expect("ANYSTORE_TEST_COS_SECRET_ID must accompany the bucket");
    let secret_key = required("ANYSTORE_TEST_COS_SECRET_KEY")
        .expect("ANYSTORE_TEST_COS_SECRET_KEY must accompany the bucket");

    let store = TencentCosBlobStore::new(CosConfig {
        bucket,
        endpoint,
        secret_id,
        secret_key,
        session_token: required("ANYSTORE_TEST_COS_SESSION_TOKEN"),
    })
    .expect("COS configuration must be valid");
    let uploader = HttpUploader {
        client: reqwest::Client::new(),
    };

    blobstore::run_all(&store as &dyn BlobStore, &uploader).await;
}
