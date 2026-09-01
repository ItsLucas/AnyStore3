//! Content access.
//!
//! Downloads are redirected to a short-lived signed provider URL so the bytes
//! never pass through the application.

use anystore_domain::ObjectId;
use anystore_domain::error::{DomainError, DomainResult};
use anystore_domain::object::ObjectView;
use chrono::{DateTime, Utc};
use std::sync::Arc;
use std::time::Duration as StdDuration;

use crate::state::AppState;

#[derive(Clone, Debug)]
pub struct ContentLocation {
    pub url: String,
    pub expires_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct ContentHead {
    pub content_type: Option<String>,
    pub size: u64,
    pub sha256: Option<String>,
}

#[derive(Clone)]
pub struct ContentService {
    state: Arc<AppState>,
}

impl ContentService {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }

    async fn readable_file(&self, id: &ObjectId) -> DomainResult<ObjectView> {
        let view = self
            .state
            .meta
            .get_object(id)
            .await?
            .ok_or(DomainError::ObjectNotFound)?;
        if !view.object.is_file() {
            return Err(DomainError::NotAFile);
        }
        if !view.object.has_ready_content() {
            return Err(DomainError::ContentNotReady);
        }
        Ok(view)
    }

    pub async fn location(&self, id: &ObjectId) -> DomainResult<ContentLocation> {
        let view = self.readable_file(id).await?;
        let pointer = self
            .state
            .meta
            .content_pointer(id)
            .await?
            .ok_or(DomainError::ContentNotReady)?;
        let blobs = self.state.blobs.get(&pointer.blob_backend)?;

        let expires_in = self
            .state
            .config
            .download_url_expiry
            .to_std()
            .unwrap_or(StdDuration::from_secs(600));

        // The download must present the object's current name, which is why the
        // filename is applied at signing time rather than stored with the blob.
        let signed = blobs
            .sign_download(
                &anystore_blobstore::BlobRef::new(pointer.blob_ref),
                &view.object.name,
                expires_in,
            )
            .await?;

        Ok(ContentLocation {
            url: signed.url,
            expires_at: signed.expires_at,
        })
    }

    pub async fn head(&self, id: &ObjectId) -> DomainResult<ContentHead> {
        let view = self.readable_file(id).await?;
        Ok(ContentHead {
            content_type: view.object.content_type.clone(),
            size: view.object.size.unwrap_or(0),
            sha256: view.object.sha256.clone(),
        })
    }
}
