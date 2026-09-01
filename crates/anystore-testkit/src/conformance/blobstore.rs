//! `BlobStore` conformance suite.
//!
//! Covers the checklist in the architecture document: single and multipart
//! preparation, part signing, completion, recovery of an already finalised
//! upload, stat, signed download, abort, delete, UTF-8 filenames, zero-byte
//! objects and error mapping.

use anystore_blobstore::{
    AbortBlobUpload, BlobRef, BlobStore, CompleteBlobUpload, CompletedPart, PrepareUpload,
    SignParts, SignedPart, SignedRequest,
};
use anystore_domain::upload::UploadMode;
use async_trait::async_trait;
use std::time::Duration;

/// Performs the client half of an upload for a given backend.
#[async_trait]
pub trait BlobUploader: Send + Sync {
    async fn put(&self, request: &SignedRequest, bytes: &[u8]);
    async fn put_part(&self, part: &SignedPart, bytes: &[u8]);
}

fn prepare(upload_id: &str, mode: UploadMode, size: u64) -> PrepareUpload {
    PrepareUpload {
        upload_id: upload_id.to_owned(),
        mode,
        expected_size: size,
        content_type: "application/octet-stream".to_owned(),
        expected_sha256: None,
        part_size: 5 * 1024 * 1024,
        expires_in: Duration::from_secs(600),
    }
}

pub async fn blob_refs_are_derived_and_opaque<S: BlobStore + ?Sized>(store: &S) {
    let a = store.blob_ref_for("upload_01");
    let b = store.blob_ref_for("upload_01");
    assert_eq!(
        a, b,
        "blob refs must be deterministic so a retry can resume"
    );
    assert_ne!(a, store.blob_ref_for("upload_02"));

    // The logical name and path must never be encoded into the key.
    assert!(!a.as_str().contains("example.pdf"));
}

pub async fn single_upload_round_trips<S: BlobStore + ?Sized, U: BlobUploader>(
    store: &S,
    uploader: &U,
) {
    let payload = b"hello world";
    let prepared = store
        .prepare_upload(prepare(
            "single_01",
            UploadMode::Single,
            payload.len() as u64,
        ))
        .await
        .unwrap();

    assert_eq!(prepared.mode, UploadMode::Single);
    let request = prepared.single.as_ref().expect("single upload plan");
    assert_eq!(request.method, "PUT");

    // Completion before the bytes arrive is a client condition, not a fault.
    let error = store
        .ensure_upload_completed(CompleteBlobUpload {
            blob_ref: prepared.blob_ref.clone(),
            mode: UploadMode::Single,
            provider_upload_id: None,
            parts: vec![],
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), "content_not_ready");

    uploader.put(request, payload).await;

    let stat = store
        .ensure_upload_completed(CompleteBlobUpload {
            blob_ref: prepared.blob_ref.clone(),
            mode: UploadMode::Single,
            provider_upload_id: None,
            parts: vec![],
        })
        .await
        .unwrap();
    assert_eq!(stat.size, payload.len() as u64);

    let restated = store.stat(&prepared.blob_ref).await.unwrap();
    assert_eq!(restated.size, stat.size);

    store.delete_blob(&prepared.blob_ref).await.unwrap();
}

pub async fn multipart_upload_round_trips<S: BlobStore + ?Sized, U: BlobUploader>(
    store: &S,
    uploader: &U,
) {
    let first = vec![b'A'; 1024];
    let second = vec![b'B'; 512];
    let total = (first.len() + second.len()) as u64;

    let prepared = store
        .prepare_upload(prepare("multi_01", UploadMode::Multipart, total))
        .await
        .unwrap();
    assert_eq!(prepared.mode, UploadMode::Multipart);
    assert!(
        prepared.part_size.is_some(),
        "multipart reports a part size"
    );

    let provider_upload_id = prepared
        .provider_upload_id
        .clone()
        .expect("multipart reports a provider upload id");

    let parts = store
        .sign_parts(SignParts {
            blob_ref: prepared.blob_ref.clone(),
            provider_upload_id: provider_upload_id.clone(),
            part_numbers: vec![1, 2],
            expires_in: Duration::from_secs(600),
        })
        .await
        .unwrap();
    assert_eq!(parts.len(), 2);
    assert_eq!(parts[0].method, "PUT");

    uploader.put_part(&parts[0], &first).await;
    uploader.put_part(&parts[1], &second).await;

    let completion = CompleteBlobUpload {
        blob_ref: prepared.blob_ref.clone(),
        mode: UploadMode::Multipart,
        provider_upload_id: Some(provider_upload_id),
        parts: vec![
            CompletedPart {
                part_number: 1,
                etag: "etag-1".into(),
            },
            CompletedPart {
                part_number: 2,
                etag: "etag-2".into(),
            },
        ],
    };

    let stat = store
        .ensure_upload_completed(completion.clone())
        .await
        .unwrap();
    assert_eq!(stat.size, total, "parts are assembled in order");

    // Completion must converge when the provider upload is already finalised,
    // which is what makes recovery after a crashed commit possible.
    let again = store.ensure_upload_completed(completion).await.unwrap();
    assert_eq!(again.size, total);

    store.delete_blob(&prepared.blob_ref).await.unwrap();
}

pub async fn zero_byte_objects_are_supported<S: BlobStore + ?Sized, U: BlobUploader>(
    store: &S,
    uploader: &U,
) {
    let prepared = store
        .prepare_upload(prepare("empty_01", UploadMode::Single, 0))
        .await
        .unwrap();
    uploader.put(prepared.single.as_ref().unwrap(), b"").await;

    let stat = store
        .ensure_upload_completed(CompleteBlobUpload {
            blob_ref: prepared.blob_ref.clone(),
            mode: UploadMode::Single,
            provider_upload_id: None,
            parts: vec![],
        })
        .await
        .unwrap();
    assert_eq!(stat.size, 0);

    store.delete_blob(&prepared.blob_ref).await.unwrap();
}

pub async fn downloads_are_signed_with_the_current_name<S: BlobStore + ?Sized, U: BlobUploader>(
    store: &S,
    uploader: &U,
) {
    let prepared = store
        .prepare_upload(prepare("download_01", UploadMode::Single, 2))
        .await
        .unwrap();
    uploader.put(prepared.single.as_ref().unwrap(), b"hi").await;
    store
        .ensure_upload_completed(CompleteBlobUpload {
            blob_ref: prepared.blob_ref.clone(),
            mode: UploadMode::Single,
            provider_upload_id: None,
            parts: vec![],
        })
        .await
        .unwrap();

    for filename in ["plain.txt", "报告 2026.pdf", "quote\"name.txt"] {
        let signed = store
            .sign_download(&prepared.blob_ref, filename, Duration::from_secs(600))
            .await
            .unwrap();
        assert!(!signed.url.is_empty());
        assert!(signed.expires_at > chrono::Utc::now());
    }

    store.delete_blob(&prepared.blob_ref).await.unwrap();
}

pub async fn aborting_and_deleting_are_idempotent<S: BlobStore + ?Sized>(store: &S) {
    let prepared = store
        .prepare_upload(prepare("abort_01", UploadMode::Multipart, 10))
        .await
        .unwrap();

    let abort = AbortBlobUpload {
        blob_ref: prepared.blob_ref.clone(),
        mode: UploadMode::Multipart,
        provider_upload_id: prepared.provider_upload_id.clone(),
    };
    store.abort_upload(abort.clone()).await.unwrap();
    store.abort_upload(abort).await.unwrap();

    // A missing blob deletes successfully so garbage collection converges.
    let missing = BlobRef::new("blobs/never-existed");
    store.delete_blob(&missing).await.unwrap();
    store.delete_blob(&missing).await.unwrap();
}

pub async fn missing_blobs_report_content_not_ready<S: BlobStore + ?Sized>(store: &S) {
    let error = store.stat(&BlobRef::new("blobs/absent")).await.unwrap_err();
    assert_eq!(error.code(), "content_not_ready");
}

pub async fn run_all<S: BlobStore + ?Sized, U: BlobUploader>(store: &S, uploader: &U) {
    blob_refs_are_derived_and_opaque(store).await;
    single_upload_round_trips(store, uploader).await;
    multipart_upload_round_trips(store, uploader).await;
    zero_byte_objects_are_supported(store, uploader).await;
    downloads_are_signed_with_the_current_name(store, uploader).await;
    aborting_and_deleting_are_idempotent(store).await;
    missing_blobs_report_content_not_ready(store).await;
}
