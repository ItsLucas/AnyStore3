//! Upload session domain types.

use crate::ids::{ObjectId, UploadId};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum UploadMode {
    Single,
    Multipart,
}

impl UploadMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Single => "single",
            Self::Multipart => "multipart",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "single" => Some(Self::Single),
            "multipart" => Some(Self::Multipart),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum UploadState {
    Initiating,
    Ready,
    Completing,
    Completed,
    Aborted,
    Expired,
}

impl UploadState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Initiating => "initiating",
            Self::Ready => "ready",
            Self::Completing => "completing",
            Self::Completed => "completed",
            Self::Aborted => "aborted",
            Self::Expired => "expired",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "initiating" => Some(Self::Initiating),
            "ready" => Some(Self::Ready),
            "completing" => Some(Self::Completing),
            "completed" => Some(Self::Completed),
            "aborted" => Some(Self::Aborted),
            "expired" => Some(Self::Expired),
            _ => None,
        }
    }

    /// An upload may still be completed from these states. `Completed` is
    /// included because completion is resumable and must converge.
    pub fn is_completable(self) -> bool {
        matches!(self, Self::Ready | Self::Completing | Self::Completed)
    }

    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Aborted | Self::Expired)
    }
}

/// Durable upload session state.
///
/// `blob_backend` and `blob_ref` are internal and must never be exposed.
#[derive(Clone, Debug, PartialEq)]
pub struct UploadRecord {
    pub id: UploadId,
    pub object_id: ObjectId,
    pub state: UploadState,
    pub mode: UploadMode,
    pub blob_backend: String,
    pub blob_ref: String,
    pub provider_upload_id: Option<String>,
    pub expected_size: u64,
    pub content_type: String,
    pub expected_sha256: Option<String>,
    pub provider_completed: bool,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub aborted_at: Option<DateTime<Utc>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completion_is_resumable_from_completed() {
        assert!(UploadState::Completed.is_completable());
        assert!(UploadState::Completing.is_completable());
        assert!(!UploadState::Aborted.is_completable());
        assert!(!UploadState::Expired.is_completable());
    }

    #[test]
    fn states_round_trip() {
        for s in [
            UploadState::Initiating,
            UploadState::Ready,
            UploadState::Completing,
            UploadState::Completed,
            UploadState::Aborted,
            UploadState::Expired,
        ] {
            assert_eq!(UploadState::parse(s.as_str()), Some(s));
        }
    }
}
