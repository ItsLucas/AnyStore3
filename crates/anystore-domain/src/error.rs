//! The AnyStore error taxonomy.
//!
//! Every variant maps to exactly one `(HTTP status, error code)` pair from the
//! API contract, so the HTTP adapter never has to invent a mapping.

use thiserror::Error;

pub type DomainResult<T> = Result<T, DomainError>;

#[derive(Debug, Error)]
pub enum DomainError {
    #[error("{0}")]
    InvalidRequest(String),

    #[error("{0}")]
    InvalidName(String),

    #[error("{0}")]
    InvalidMetadata(String),

    #[error("Object not found.")]
    ObjectNotFound,

    #[error("Upload session not found.")]
    UploadNotFound,

    #[error("An object with the same name already exists in this folder.")]
    NameConflict,

    #[error("Folder contains child objects.")]
    FolderNotEmpty,

    #[error("{0}")]
    InvalidMove(String),

    #[error("Target object is not a folder.")]
    NotAFolder,

    #[error("Target object is not a file.")]
    NotAFile,

    #[error("Object has no ready content.")]
    ContentNotReady,

    #[error("Idempotency-Key was already used with a different request.")]
    IdempotencyKeyReused,

    #[error("The requested Changes cursor is outside the retention window.")]
    ChangesCursorExpired,

    #[error("Object has been modified.")]
    RevisionConflict { current_revision: u64 },

    #[error("{0}")]
    ChecksumMismatch(String),

    #[error("Too many requests.")]
    RateLimited,

    #[error("Authentication is required.")]
    Unauthorized,

    #[error("Internal error.")]
    Internal(#[source] anyhow_lite::BoxError),

    #[error("Storage backend error.")]
    StorageError(#[source] anyhow_lite::BoxError),
}

/// Minimal boxed-error helper so the domain crate does not need `anyhow`.
pub mod anyhow_lite {
    pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

    pub fn boxed(e: impl Into<BoxError>) -> BoxError {
        e.into()
    }

    pub fn msg(m: impl Into<String>) -> BoxError {
        struct Msg(String);
        impl std::fmt::Debug for Msg {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(&self.0)
            }
        }
        impl std::fmt::Display for Msg {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(&self.0)
            }
        }
        impl std::error::Error for Msg {}
        Box::new(Msg(m.into()))
    }
}

impl DomainError {
    pub fn internal(m: impl Into<String>) -> Self {
        Self::Internal(anyhow_lite::msg(m))
    }

    pub fn storage(m: impl Into<String>) -> Self {
        Self::StorageError(anyhow_lite::msg(m))
    }

    /// Stable machine-readable code from the API contract error table.
    pub fn code(&self) -> &'static str {
        match self {
            Self::InvalidRequest(_) => "invalid_request",
            Self::InvalidName(_) => "invalid_name",
            Self::InvalidMetadata(_) => "invalid_metadata",
            Self::ObjectNotFound => "object_not_found",
            Self::UploadNotFound => "upload_not_found",
            Self::NameConflict => "name_conflict",
            Self::FolderNotEmpty => "folder_not_empty",
            Self::InvalidMove(_) => "invalid_move",
            Self::NotAFolder => "not_a_folder",
            Self::NotAFile => "not_a_file",
            Self::ContentNotReady => "content_not_ready",
            Self::IdempotencyKeyReused => "idempotency_key_reused",
            Self::ChangesCursorExpired => "changes_cursor_expired",
            Self::RevisionConflict { .. } => "revision_conflict",
            Self::ChecksumMismatch(_) => "checksum_mismatch",
            Self::RateLimited => "rate_limited",
            Self::Unauthorized => "unauthorized",
            Self::Internal(_) => "internal_error",
            Self::StorageError(_) => "storage_error",
        }
    }

    pub fn status(&self) -> u16 {
        match self {
            Self::InvalidRequest(_) | Self::InvalidName(_) | Self::InvalidMetadata(_) => 400,
            Self::Unauthorized => 401,
            Self::ObjectNotFound | Self::UploadNotFound => 404,
            Self::NameConflict
            | Self::FolderNotEmpty
            | Self::InvalidMove(_)
            | Self::NotAFolder
            | Self::NotAFile
            | Self::ContentNotReady
            | Self::IdempotencyKeyReused => 409,
            Self::ChangesCursorExpired => 410,
            Self::RevisionConflict { .. } => 412,
            Self::ChecksumMismatch(_) => 422,
            Self::RateLimited => 429,
            Self::Internal(_) => 500,
            Self::StorageError(_) => 502,
        }
    }

    /// Public message. Internal and storage errors are deliberately opaque so
    /// provider details never leak through the public API.
    pub fn public_message(&self) -> String {
        match self {
            Self::Internal(_) => "Internal error.".to_owned(),
            Self::StorageError(_) => "Storage backend error.".to_owned(),
            other => other.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contract_error_table_is_respected() {
        assert_eq!(DomainError::NameConflict.status(), 409);
        assert_eq!(DomainError::NameConflict.code(), "name_conflict");
        assert_eq!(
            DomainError::RevisionConflict {
                current_revision: 8
            }
            .status(),
            412
        );
        assert_eq!(DomainError::ChangesCursorExpired.status(), 410);
        assert_eq!(DomainError::ChecksumMismatch("x".into()).status(), 422);
        assert_eq!(DomainError::storage("boom").status(), 502);
    }

    #[test]
    fn internal_errors_do_not_leak_details() {
        let e = DomainError::storage("bucket anystore-1250000000 credentials rejected");
        assert_eq!(e.public_message(), "Storage backend error.");
    }
}
