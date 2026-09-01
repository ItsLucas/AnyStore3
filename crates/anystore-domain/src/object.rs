//! Core object types.

use crate::ids::ObjectId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Monotonic per-object revision. New objects start at 1.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Revision(pub u64);

impl Revision {
    pub const INITIAL: Revision = Revision(1);

    pub fn next(self) -> Self {
        Revision(self.0 + 1)
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for Revision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ObjectKind {
    File,
    Folder,
}

impl ObjectKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::Folder => "folder",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "file" => Some(Self::File),
            "folder" => Some(Self::Folder),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ContentState {
    None,
    Ready,
}

impl ContentState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Ready => "ready",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "none" => Some(Self::None),
            "ready" => Some(Self::Ready),
            _ => None,
        }
    }
}

/// An AnyStore object.
///
/// `path` is intentionally absent: it is derived from the folder tree rather
/// than stored as authoritative state.
#[derive(Clone, Debug, PartialEq)]
pub struct Object {
    pub id: ObjectId,
    pub revision: Revision,
    pub kind: ObjectKind,
    pub name: String,
    pub parent_id: Option<ObjectId>,
    pub content_type: Option<String>,
    pub size: Option<u64>,
    pub sha256: Option<String>,
    pub content_state: Option<ContentState>,
    pub metadata: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Object {
    pub fn is_folder(&self) -> bool {
        self.kind == ObjectKind::Folder
    }

    pub fn is_file(&self) -> bool {
        self.kind == ObjectKind::File
    }

    pub fn is_root(&self) -> bool {
        self.id.is_root()
    }

    pub fn has_ready_content(&self) -> bool {
        self.content_state == Some(ContentState::Ready)
    }
}

/// An object plus its rendered path, as returned by the public API.
#[derive(Clone, Debug, PartialEq)]
pub struct ObjectView {
    pub object: Object,
    pub path: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn revisions_start_at_one_and_increment_by_one() {
        assert_eq!(Revision::INITIAL.get(), 1);
        assert_eq!(Revision::INITIAL.next().get(), 2);
    }

    #[test]
    fn enum_round_trips() {
        assert_eq!(ObjectKind::parse("folder"), Some(ObjectKind::Folder));
        assert_eq!(ObjectKind::parse("directory"), None);
        assert_eq!(ContentState::parse("ready"), Some(ContentState::Ready));
    }
}
