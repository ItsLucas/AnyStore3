//! Maintenance passes against the in-memory adapters.

use anystore_application::{AppConfig, AppState, MaintenanceService, Metrics};
use anystore_blobstore::BlobRegistry;
use anystore_domain::object::ObjectKind;
use anystore_domain::upload::UploadMode;
use anystore_domain::{ObjectId, RequestId, UploadId};
use anystore_metastore::ObjectMutationStore;
use anystore_metastore::commands::{CreateObjectCommit, MutationContext};
use anystore_metastore::response::StoredResponse;
use anystore_metastore::uploads::CreateUploadRecord;
use anystore_metastore::uploads::UploadRepository;
use anystore_testkit::blobstore::BACKEND_ID;
use anystore_testkit::{InMemoryBlobStore, InMemoryMetaStore};
use chrono::{Duration, Utc};
use serde_json::json;
use std::sync::Arc;
use std::time::Instant;

/// A store holding `count` upload sessions that expired an hour ago.
async fn state_with_expired_uploads(count: usize) -> Arc<AppState> {
    let meta = Arc::new(InMemoryMetaStore::new());
    let now = Utc::now();
    let file = ObjectId::generate();
    meta.create_object(CreateObjectCommit {
        id: file.clone(),
        kind: ObjectKind::File,
        name: "report.pdf".into(),
        parent_id: ObjectId::root(),
        content_type: Some("application/pdf".into()),
        metadata: json!({}),
        ctx: MutationContext {
            now,
            request_id: RequestId::generate(),
            idempotency_key: None,
            idempotency: None,
            idempotency_expires_at: now + Duration::hours(24),
            render: Arc::new(|_| Ok(StoredResponse::no_content())),
        },
    })
    .await
    .expect("create file");

    for n in 0..count {
        meta.create_upload_record(CreateUploadRecord {
            id: UploadId::new(format!("upl_expired_{n}")),
            object_id: file.clone(),
            mode: UploadMode::Single,
            blob_backend: BACKEND_ID.into(),
            blob_ref: format!("blobs/upl_expired_{n}"),
            expected_size: 1,
            content_type: "application/pdf".into(),
            expected_sha256: None,
            created_at: now - Duration::hours(25),
            expires_at: now - Duration::hours(1),
        })
        .await
        .expect("create upload");
    }

    Arc::new(AppState::new(
        meta,
        Arc::new(BlobRegistry::new(Arc::new(InMemoryBlobStore::new()))),
        AppConfig::default(),
        Arc::new(Metrics::default()),
    ))
}

#[tokio::test]
async fn a_pass_drains_backlogs_larger_than_one_batch() {
    let state = state_with_expired_uploads(5).await;
    let service = MaintenanceService::new(state).with_batch_size(2);

    let report = service
        .run(
            Utc::now(),
            Instant::now() + std::time::Duration::from_secs(30),
        )
        .await
        .expect("pass")
        .expect("not already running");

    assert_eq!(report.uploads_expired, 5);
    assert_eq!(report.blobs_deleted, 5);
    assert!(report.drained);
}

#[tokio::test]
async fn an_exhausted_budget_leaves_the_rest_for_the_next_pass() {
    let state = state_with_expired_uploads(5).await;
    let service = MaintenanceService::new(state).with_batch_size(2);

    let first = service
        .run(Utc::now(), Instant::now())
        .await
        .expect("pass")
        .expect("not already running");
    assert_eq!(first.uploads_expired, 2);
    assert!(!first.drained);

    let second = service
        .run(
            Utc::now(),
            Instant::now() + std::time::Duration::from_secs(30),
        )
        .await
        .expect("pass")
        .expect("not already running");
    assert_eq!(second.uploads_expired, 3);
    assert!(second.drained);
}
