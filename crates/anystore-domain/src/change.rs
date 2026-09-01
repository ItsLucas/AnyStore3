//! Change feed records.

use crate::ids::{ChangeId, IdempotencyKey, ObjectId, RequestId};
use crate::object::Revision;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeAction {
    Created,
    MetadataUpdated,
    Renamed,
    Moved,
    ContentReady,
    ContentReplaced,
    Deleted,
}

impl ChangeAction {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::MetadataUpdated => "metadata_updated",
            Self::Renamed => "renamed",
            Self::Moved => "moved",
            Self::ContentReady => "content_ready",
            Self::ContentReplaced => "content_replaced",
            Self::Deleted => "deleted",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "created" => Some(Self::Created),
            "metadata_updated" => Some(Self::MetadataUpdated),
            "renamed" => Some(Self::Renamed),
            "moved" => Some(Self::Moved),
            "content_ready" => Some(Self::ContentReady),
            "content_replaced" => Some(Self::ContentReplaced),
            "deleted" => Some(Self::Deleted),
            _ => None,
        }
    }

    /// Deterministic ordering for multiple Change rows emitted by a single
    /// PATCH, as required by the architecture document.
    pub fn emission_order(self) -> u8 {
        match self {
            Self::Created => 0,
            Self::Renamed => 1,
            Self::Moved => 2,
            Self::MetadataUpdated => 3,
            Self::ContentReady | Self::ContentReplaced => 4,
            Self::Deleted => 5,
        }
    }
}

/// A record appended to the globally ordered Changes feed.
///
/// Change records deliberately carry no file bytes and no metadata document.
#[derive(Clone, Debug, PartialEq)]
pub struct ChangeRecord {
    pub change_id: ChangeId,
    pub object_id: ObjectId,
    pub revision: Revision,
    pub action: ChangeAction,
    pub changed_at: DateTime<Utc>,
    pub request_id: RequestId,
    pub idempotency_key: Option<IdempotencyKey>,
    pub tombstone: bool,
}

/// A Change that is about to be appended inside a mutation transaction.
#[derive(Clone, Debug, PartialEq)]
pub struct PendingChange {
    pub object_id: ObjectId,
    pub revision: Revision,
    pub action: ChangeAction,
    pub tombstone: bool,
}

impl PendingChange {
    pub fn new(object_id: ObjectId, revision: Revision, action: ChangeAction) -> Self {
        Self {
            object_id,
            revision,
            action,
            tombstone: action == ChangeAction::Deleted,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn combined_patch_changes_sort_rename_move_metadata() {
        let mut actions = vec![
            ChangeAction::MetadataUpdated,
            ChangeAction::Moved,
            ChangeAction::Renamed,
        ];
        actions.sort_by_key(|a| a.emission_order());
        assert_eq!(
            actions,
            vec![
                ChangeAction::Renamed,
                ChangeAction::Moved,
                ChangeAction::MetadataUpdated
            ]
        );
    }

    #[test]
    fn delete_is_always_a_tombstone() {
        let c = PendingChange::new(ObjectId::root(), Revision(2), ChangeAction::Deleted);
        assert!(c.tombstone);
        let c = PendingChange::new(ObjectId::root(), Revision(2), ChangeAction::Renamed);
        assert!(!c.tombstone);
    }

    #[test]
    fn action_strings_round_trip() {
        for a in [
            ChangeAction::Created,
            ChangeAction::MetadataUpdated,
            ChangeAction::Renamed,
            ChangeAction::Moved,
            ChangeAction::ContentReady,
            ChangeAction::ContentReplaced,
            ChangeAction::Deleted,
        ] {
            assert_eq!(ChangeAction::parse(a.as_str()), Some(a));
        }
    }
}
