//! In-memory `BlobStore` for tests.

use anystore_blobstore::{
    AbortBlobUpload, BlobRef, BlobStat, BlobStore, CompleteBlobUpload, PrepareUpload,
    PreparedUpload, SignParts, SignedDownload, SignedPart, SignedRequest,
};
use anystore_domain::error::{DomainError, DomainResult};
use anystore_domain::upload::UploadMode;
use async_trait::async_trait;
use chrono::Utc;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use std::time::Duration;

pub const BACKEND_ID: &str = "in_memory";

#[derive(Default)]
struct State {
    blobs: HashMap<String, Vec<u8>>,
    parts: HashMap<String, BTreeMap<u32, Vec<u8>>>,
    multipart: HashMap<String, String>,
}

#[derive(Default)]
pub struct InMemoryBlobStore {
    state: Mutex<State>,
}

impl InMemoryBlobStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Simulates a client uploading bytes to a signed URL.
    pub fn put(&self, blob: &BlobRef, bytes: &[u8]) {
        self.state
            .lock()
            .unwrap()
            .blobs
            .insert(blob.as_str().to_owned(), bytes.to_vec());
    }

    /// Simulates a client uploading one part.
    pub fn put_part(&self, blob: &BlobRef, part_number: u32, bytes: &[u8]) {
        self.state
            .lock()
            .unwrap()
            .parts
            .entry(blob.as_str().to_owned())
            .or_default()
            .insert(part_number, bytes.to_vec());
    }

    pub fn contains(&self, blob: &BlobRef) -> bool {
        self.state.lock().unwrap().blobs.contains_key(blob.as_str())
    }

    pub fn len(&self) -> usize {
        self.state.lock().unwrap().blobs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

fn stat_of(bytes: &[u8]) -> BlobStat {
    let digest = hex::encode(Sha256::digest(bytes));
    BlobStat {
        size: bytes.len() as u64,
        content_type: None,
        etag: Some(digest.clone()),
        sha256: Some(digest),
    }
}

#[async_trait]
impl BlobStore for InMemoryBlobStore {
    fn backend_id(&self) -> &'static str {
        BACKEND_ID
    }

    fn verifies_sha256(&self) -> bool {
        true
    }

    fn blob_ref_for(&self, upload_id: &str) -> BlobRef {
        BlobRef::new(format!("blobs/{upload_id}"))
    }

    async fn prepare_upload(&self, req: PrepareUpload) -> DomainResult<PreparedUpload> {
        let blob_ref = self.blob_ref_for(&req.upload_id);
        let expires_at = Utc::now() + chrono::Duration::hours(6);

        Ok(match req.mode {
            UploadMode::Single => PreparedUpload {
                single: Some(SignedRequest {
                    method: "PUT".into(),
                    url: format!("memory://{}", blob_ref.as_str()),
                    headers: BTreeMap::new(),
                }),
                part_size: None,
                provider_upload_id: None,
                blob_ref,
                mode: req.mode,
                expires_at,
            },
            UploadMode::Multipart => {
                let provider_upload_id = format!("mpu_{}", req.upload_id);
                self.state
                    .lock()
                    .unwrap()
                    .multipart
                    .insert(blob_ref.as_str().to_owned(), provider_upload_id.clone());
                PreparedUpload {
                    single: None,
                    part_size: Some(req.part_size),
                    provider_upload_id: Some(provider_upload_id),
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
            .map(|number| SignedPart {
                part_number: *number,
                method: "PUT".into(),
                url: format!("memory://{}/parts/{number}", req.blob_ref.as_str()),
            })
            .collect())
    }

    async fn ensure_upload_completed(&self, req: CompleteBlobUpload) -> DomainResult<BlobStat> {
        let mut state = self.state.lock().unwrap();
        let key = req.blob_ref.as_str().to_owned();

        // Converges when the blob was already finalised before a crash.
        if !state.blobs.contains_key(&key) && req.mode == UploadMode::Multipart {
            let Some(parts) = state.parts.remove(&key) else {
                return Err(DomainError::ContentNotReady);
            };
            let mut assembled = Vec::new();
            for number in req.parts.iter().map(|p| p.part_number) {
                let Some(bytes) = parts.get(&number) else {
                    return Err(DomainError::ContentNotReady);
                };
                assembled.extend_from_slice(bytes);
            }
            state.blobs.insert(key.clone(), assembled);
        }

        state
            .blobs
            .get(&key)
            .map(|bytes| stat_of(bytes))
            .ok_or(DomainError::ContentNotReady)
    }

    async fn abort_upload(&self, req: AbortBlobUpload) -> DomainResult<()> {
        let mut state = self.state.lock().unwrap();
        state.parts.remove(req.blob_ref.as_str());
        state.multipart.remove(req.blob_ref.as_str());
        Ok(())
    }

    async fn stat(&self, blob: &BlobRef) -> DomainResult<BlobStat> {
        self.state
            .lock()
            .unwrap()
            .blobs
            .get(blob.as_str())
            .map(|bytes| stat_of(bytes))
            .ok_or(DomainError::ContentNotReady)
    }

    async fn sign_download(
        &self,
        blob: &BlobRef,
        filename: &str,
        expires_in: Duration,
    ) -> DomainResult<SignedDownload> {
        Ok(SignedDownload {
            url: format!("memory://{}?filename={filename}", blob.as_str()),
            expires_at: Utc::now()
                + chrono::Duration::from_std(expires_in)
                    .unwrap_or_else(|_| chrono::Duration::seconds(600)),
        })
    }

    async fn delete_blob(&self, blob: &BlobRef) -> DomainResult<()> {
        // Missing counts as success so garbage collection is idempotent.
        self.state.lock().unwrap().blobs.remove(blob.as_str());
        Ok(())
    }
}
