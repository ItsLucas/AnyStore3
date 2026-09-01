//! Application services.
//!
//! Depends on domain types and port traits only; never on a concrete database
//! or blob provider.

pub mod changes;
pub mod config;
pub mod content;
pub mod context;
pub mod dto;
pub mod idempotency;
pub mod maintenance;
pub mod metrics;
pub mod objects;
pub mod query;
pub mod state;
pub mod uploads;

pub use changes::ChangeService;
pub use config::AppConfig;
pub use content::{ContentHead, ContentLocation, ContentService};
pub use context::{RequestContext, canonical_request_hash};
pub use maintenance::{MaintenanceReport, MaintenanceService};
pub use metrics::Metrics;
pub use objects::{CreateObjectRequest, ListChildrenRequest, ObjectService, PatchObjectRequest};
pub use query::{QueryRequest, QueryService};
pub use state::AppState;
pub use uploads::{
    AllocatePartsRequest, CompleteUploadRequest, CompletedPartInput, CreateUploadRequest,
    UploadService,
};
