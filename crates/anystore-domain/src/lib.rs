//! AnyStore domain layer.
//!
//! This crate holds pure domain types, validation, and business rules. It must
//! not depend on Axum, SQLx, HTTP, PostgreSQL, or any cloud provider SDK.

pub mod change;
pub mod error;
pub mod ids;
pub mod metadata;
pub mod name;
pub mod object;
pub mod page;
pub mod path;
pub mod upload;

pub use change::{ChangeAction, ChangeRecord};
pub use error::{DomainError, DomainResult};
pub use ids::{ChangeId, CursorId, IdempotencyKey, ObjectId, PrincipalId, RequestId, UploadId};
pub use metadata::{MetadataPatch, apply_metadata_patch};
pub use name::validate_name;
pub use object::{ContentState, Object, ObjectKind, Revision};
pub use page::Page;
pub use upload::{UploadMode, UploadRecord, UploadState};
