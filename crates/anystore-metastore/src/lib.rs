//! `MetaStore` ports.
//!
//! These traits abstract AnyStore *persistence semantics*, not SQL syntax.
//! There is deliberately no generic `execute_sql()`: each adapter implements
//! these operations with its backend's native transaction features, which is
//! what makes "revision check + mutation + Change append + idempotency
//! finalisation" a single atomic unit.

pub mod changes;
pub mod commands;
pub mod idempotency;
pub mod maintenance;
pub mod response;
pub mod uploads;

pub use changes::{ChangePage, ChangeStore, ReadChanges};
pub use commands::{
    CommitContent, CreateObjectCommit, DeleteObjectCommit, ListChildren, ListOrder, ObjectQuery,
    OrderBy, PatchObjectCommit, MetadataCondition, MutationOutcome,
};
pub use idempotency::{
    IdempotencyAcquire, IdempotencyComplete, IdempotencyContext, IdempotencyDecision,
    IdempotencyRelease, IdempotencyStore,
};
pub use maintenance::{BlobGcEntry, MaintenanceStore};
pub use response::{ResponseRenderer, StoredResponse};
pub use uploads::{
    AbortUploadRecord, CreateUploadRecord, MarkUploadCompleting, UpdateUploadState, UploadRepository,
};

use anystore_domain::error::DomainResult;
use anystore_domain::object::ObjectView;
use anystore_domain::{ObjectId, Page};
use async_trait::async_trait;

/// Read-only object access.
#[async_trait]
pub trait ObjectRepository: Send + Sync {
    async fn get_object(&self, id: &ObjectId) -> DomainResult<Option<ObjectView>>;
    async fn list_children(&self, q: ListChildren) -> DomainResult<Page<ObjectView>>;
    async fn resolve_path(&self, path: &str) -> DomainResult<Option<ObjectView>>;
    async fn query_objects(&self, q: ObjectQuery) -> DomainResult<Page<ObjectView>>;
}

/// Transactional object mutations.
///
/// Every method commits the object mutation, the resulting Change record(s) and
/// the idempotency response in exactly one backend transaction.
#[async_trait]
pub trait ObjectMutationStore: Send + Sync {
    async fn create_object(&self, cmd: CreateObjectCommit) -> DomainResult<StoredResponse>;
    async fn patch_object(&self, cmd: PatchObjectCommit) -> DomainResult<StoredResponse>;
    async fn delete_object(&self, cmd: DeleteObjectCommit) -> DomainResult<StoredResponse>;
    async fn commit_content(&self, cmd: CommitContent) -> DomainResult<StoredResponse>;
}

/// Aggregate port. The application depends on this, never on a concrete store.
pub trait MetaStore:
    ObjectRepository
    + ObjectMutationStore
    + UploadRepository
    + IdempotencyStore
    + ChangeStore
    + MaintenanceStore
{
}

impl<T> MetaStore for T where
    T: ObjectRepository
        + ObjectMutationStore
        + UploadRepository
        + IdempotencyStore
        + ChangeStore
        + MaintenanceStore
{
}
