//! Local filesystem `BlobStore` for development.
//!
//! Mirrors the production shape rather than shortcutting it: the client still
//! receives a short-lived signed URL and still PUTs and GETs bytes over HTTP
//! without them passing through the AnyStore application. This adapter is not
//! for production use — a stateless deployment must not depend on local disk.

mod signing;
mod transport;

pub use transport::router;

use anystore_blobstore::{
    AbortBlobUpload, BlobRef, BlobStat, BlobStore, CompleteBlobUpload, PrepareUpload,
    PreparedUpload, SignParts, SignedDownload, SignedPart, SignedRequest,
};
use anystore_domain::error::{DomainError, DomainResult};
use anystore_domain::upload::UploadMode;
use async_trait::async_trait;
use chrono::Utc;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncReadExt;

pub const BACKEND_ID: &str = "local_fs";

#[derive(Clone)]
pub struct LocalFsBlobStore {
    inner: Arc<Inner>,
}

struct Inner {
    root: PathBuf,
    base_url: String,
    secret: Vec<u8>,
}

impl LocalFsBlobStore {
    pub fn new(root: impl Into<PathBuf>, base_url: impl Into<String>, secret: &[u8]) -> Self {
        Self {
            inner: Arc::new(Inner {
                root: root.into(),
                base_url: base_url.into().trim_end_matches('/').to_owned(),
                secret: secret.to_vec(),
            }),
        }
    }

    fn blob_path(&self, blob: &BlobRef) -> DomainResult<PathBuf> {
        safe_join(&self.inner.root, blob.as_str())
    }

    fn parts_dir(&self, blob: &BlobRef) -> DomainResult<PathBuf> {
        safe_join(&self.inner.root, &format!("{}.parts", blob.as_str()))
    }

    fn signed_url(
        &self,
        method: &str,
        path: &str,
        expires_in: Duration,
        filename: Option<&str>,
    ) -> String {
        let expires = Utc::now().timestamp() + expires_in.as_secs() as i64;
        let signature = signing::sign(&self.inner.secret, method, path, expires, filename);
        let mut url = format!(
            "{}/dev-blobs/{path}?exp={expires}&sig={signature}",
            self.inner.base_url
        );
        if let Some(filename) = filename {
            url.push_str("&filename=");
            url.push_str(&signing::encode_component(filename));
        }
        url
    }
}

/// Rejects any path that could escape the blob root.
fn safe_join(root: &Path, relative: &str) -> DomainResult<PathBuf> {
    if relative.is_empty() || relative.starts_with('/') {
        return Err(DomainError::storage("invalid blob reference"));
    }
    for segment in relative.split('/') {
        if segment.is_empty() || segment == "." || segment == ".." {
            return Err(DomainError::storage("invalid blob reference"));
        }
        if segment.contains('\\') || segment.contains('\0') {
            return Err(DomainError::storage("invalid blob reference"));
        }
    }
    Ok(root.join(relative))
}

async fn hash_and_size(path: &Path) -> DomainResult<(u64, String)> {
    let mut file = tokio::fs::File::open(path)
        .await
        .map_err(|e| DomainError::storage(format!("open blob failed: {e}")))?;

    let mut hasher = Sha256::new();
    let mut size = 0u64;
    let mut buffer = vec![0u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .await
            .map_err(|e| DomainError::storage(format!("read blob failed: {e}")))?;
        if read == 0 {
            break;
        }
        size += read as u64;
        hasher.update(&buffer[..read]);
    }

    Ok((size, hex::encode(hasher.finalize())))
}

#[async_trait]
impl BlobStore for LocalFsBlobStore {
    fn backend_id(&self) -> &'static str {
        BACKEND_ID
    }

    fn verifies_sha256(&self) -> bool {
        // Local files can be hashed without an egress cost.
        true
    }

    fn blob_ref_for(&self, upload_id: &str) -> BlobRef {
        BlobRef::new(format!("blobs/{upload_id}"))
    }

    async fn prepare_upload(&self, req: PrepareUpload) -> DomainResult<PreparedUpload> {
        let blob_ref = self.blob_ref_for(&req.upload_id);
        let path = self.blob_path(&blob_ref)?;
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| DomainError::storage(format!("create blob dir failed: {e}")))?;
        }

        let expires_at = Utc::now()
            + chrono::Duration::from_std(req.expires_in)
                .unwrap_or_else(|_| chrono::Duration::hours(6));

        Ok(match req.mode {
            UploadMode::Single => PreparedUpload {
                single: Some(SignedRequest {
                    method: "PUT".into(),
                    url: self.signed_url("PUT", blob_ref.as_str(), req.expires_in, None),
                    headers: BTreeMap::new(),
                }),
                part_size: None,
                provider_upload_id: None,
                blob_ref,
                mode: req.mode,
                expires_at,
            },
            UploadMode::Multipart => {
                let parts_dir = self.parts_dir(&blob_ref)?;
                tokio::fs::create_dir_all(&parts_dir)
                    .await
                    .map_err(|e| DomainError::storage(format!("create parts dir failed: {e}")))?;
                PreparedUpload {
                    single: None,
                    part_size: Some(req.part_size),
                    provider_upload_id: Some(req.upload_id.clone()),
                    blob_ref,
                    mode: req.mode,
                    expires_at,
                }
            }
        })
    }

    async fn sign_parts(&self, req: SignParts) -> DomainResult<Vec<SignedPart>> {
        Ok(req
            .part_numbers
            .iter()
            .map(|number| {
                let path = format!("{}.parts/{number}", req.blob_ref.as_str());
                SignedPart {
                    part_number: *number,
                    method: "PUT".into(),
                    url: self.signed_url("PUT", &path, req.expires_in, None),
                }
            })
            .collect())
    }

    async fn ensure_upload_completed(&self, req: CompleteBlobUpload) -> DomainResult<BlobStat> {
        let path = self.blob_path(&req.blob_ref)?;

        // Converge rather than fail when the blob is already finalised: the
        // process may have crashed after the provider call but before commit.
        if req.mode == UploadMode::Multipart && !path.exists() {
            let parts_dir = self.parts_dir(&req.blob_ref)?;
            let mut numbers: Vec<u32> = req.parts.iter().map(|p| p.part_number).collect();
            numbers.sort_unstable();

            let mut assembled = Vec::new();
            for number in numbers {
                let part_path = parts_dir.join(number.to_string());
                let bytes = tokio::fs::read(&part_path).await.map_err(|e| {
                    DomainError::storage(format!("missing uploaded part {number}: {e}"))
                })?;
                assembled.extend_from_slice(&bytes);
            }
            tokio::fs::write(&path, &assembled)
                .await
                .map_err(|e| DomainError::storage(format!("assemble blob failed: {e}")))?;
            let _ = tokio::fs::remove_dir_all(&parts_dir).await;
        }

        if !path.exists() {
            return Err(DomainError::ContentNotReady);
        }

        self.stat(&req.blob_ref).await
    }

    async fn abort_upload(&self, req: AbortBlobUpload) -> DomainResult<()> {
        let parts_dir = self.parts_dir(&req.blob_ref)?;
        let _ = tokio::fs::remove_dir_all(&parts_dir).await;
        Ok(())
    }

    async fn stat(&self, blob: &BlobRef) -> DomainResult<BlobStat> {
        let path = self.blob_path(blob)?;
        if !path.exists() {
            return Err(DomainError::ContentNotReady);
        }
        let (size, sha256) = hash_and_size(&path).await?;
        Ok(BlobStat {
            size,
            content_type: None,
            etag: Some(sha256.clone()),
            sha256: Some(sha256),
        })
    }

    async fn sign_download(
        &self,
        blob: &BlobRef,
        filename: &str,
        expires_in: Duration,
    ) -> DomainResult<SignedDownload> {
        let expires_at = Utc::now()
            + chrono::Duration::from_std(expires_in)
                .unwrap_or_else(|_| chrono::Duration::seconds(600));

        Ok(SignedDownload {
            url: self.signed_url("GET", blob.as_str(), expires_in, Some(filename)),
            expires_at,
        })
    }

    async fn delete_blob(&self, blob: &BlobRef) -> DomainResult<()> {
        let path = self.blob_path(blob)?;
        match tokio::fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            // A missing blob counts as success so garbage collection converges.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(DomainError::storage(format!("delete blob failed: {e}"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn traversal_is_rejected() {
        let root = Path::new("/tmp/anystore");
        for candidate in ["../etc/passwd", "blobs/../../etc", "/etc/passwd", ""] {
            assert!(
                safe_join(root, candidate).is_err(),
                "{candidate} must be rejected"
            );
        }
        assert!(safe_join(root, "blobs/upload_1").is_ok());
    }

    #[test]
    fn blob_keys_never_encode_object_names() {
        let store = LocalFsBlobStore::new("/tmp/anystore", "http://localhost", b"secret");
        let blob = store.blob_ref_for("upload_01");
        assert_eq!(blob.as_str(), "blobs/upload_01");
    }
}
