//! Query and mutation command types for the `MetaStore` port.

use anystore_domain::change::ChangeRecord;
use anystore_domain::metadata::MetadataPatch;
use anystore_domain::object::{ObjectKind, ObjectView, Revision};
use anystore_domain::{IdempotencyKey, ObjectId, RequestId};
use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::idempotency::IdempotencyContext;
use crate::response::ResponseRenderer;

// ---------------------------------------------------------------------------
// Reads
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrderBy {
    Name,
    CreatedAt,
    UpdatedAt,
    Size,
}

impl OrderBy {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "name" => Some(Self::Name),
            "created_at" => Some(Self::CreatedAt),
            "updated_at" => Some(Self::UpdatedAt),
            "size" => Some(Self::Size),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ListOrder {
    Asc,
    Desc,
}

impl ListOrder {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "asc" => Some(Self::Asc),
            "desc" => Some(Self::Desc),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ListChildren {
    pub parent_id: ObjectId,
    pub limit: u32,
    pub cursor: Option<String>,
    pub order_by: OrderBy,
    pub order: ListOrder,
}

/// A single top-level metadata condition. V1 supports `eq` and `exists` only.
#[derive(Clone, Debug, PartialEq)]
pub enum MetadataCondition {
    Eq(Value),
    Exists(bool),
}

#[derive(Clone, Debug)]
pub struct ObjectQuery {
    pub kind: Option<ObjectKind>,
    pub name: Option<String>,
    pub parent_id: Option<ObjectId>,
    pub metadata: Vec<(String, MetadataCondition)>,
    pub limit: u32,
    pub cursor: Option<String>,
}

// ---------------------------------------------------------------------------
// Mutations
// ---------------------------------------------------------------------------

/// The committed result of a mutation, handed to the [`ResponseRenderer`].
#[derive(Clone, Debug)]
pub struct MutationOutcome {
    /// Resulting object. `None` for delete.
    pub object: Option<ObjectView>,
    /// Change records appended by this mutation, in emission order.
    pub changes: Vec<ChangeRecord>,
    /// True when the request was a no-op: no revision bump, no Change.
    pub no_op: bool,
}

/// Fields shared by every mutation command.
#[derive(Clone)]
pub struct MutationContext {
    pub now: DateTime<Utc>,
    pub request_id: RequestId,
    pub idempotency_key: Option<IdempotencyKey>,
    /// Present when the request carried an `Idempotency-Key`; the store
    /// finalises this record in the same transaction as the mutation.
    pub idempotency: Option<IdempotencyContext>,
    pub idempotency_expires_at: DateTime<Utc>,
    pub render: ResponseRenderer,
}

#[derive(Clone)]
pub struct CreateObjectCommit {
    pub id: ObjectId,
    pub kind: ObjectKind,
    pub name: String,
    pub parent_id: ObjectId,
    pub content_type: Option<String>,
    pub metadata: Value,
    pub ctx: MutationContext,
}

#[derive(Clone)]
pub struct PatchObjectCommit {
    pub id: ObjectId,
    pub if_match: Option<Revision>,
    pub new_name: Option<String>,
    pub new_parent_id: Option<ObjectId>,
    pub metadata_patch: Option<MetadataPatch>,
    pub ctx: MutationContext,
}

#[derive(Clone)]
pub struct DeleteObjectCommit {
    pub id: ObjectId,
    pub if_match: Option<Revision>,
    pub recursive: bool,
    pub ctx: MutationContext,
}

/// Atomic content-pointer replacement after a verified blob upload.
#[derive(Clone)]
pub struct CommitContent {
    pub upload_id: anystore_domain::UploadId,
    pub object_id: ObjectId,
    pub if_match: Option<Revision>,
    pub blob_backend: String,
    pub blob_ref: String,
    pub size: u64,
    pub content_type: String,
    pub sha256: Option<String>,
    /// Grace period before the replaced blob becomes GC-eligible.
    pub gc_not_before: DateTime<Utc>,
    pub ctx: MutationContext,
}
