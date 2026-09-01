//! Identifier newtypes.
//!
//! API-visible prefixes are added here, in the domain layer, so that adapters
//! never invent their own identifier formats.

use serde::{Deserialize, Serialize};
use std::fmt;

macro_rules! string_id {
    ($name:ident) => {
        #[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }

            pub fn into_inner(self) -> String {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}({})", stringify!($name), self.0)
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self(value)
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self(value.to_owned())
            }
        }
    };
}

string_id!(ObjectId);
string_id!(UploadId);
string_id!(ChangeId);
string_id!(RequestId);
string_id!(CursorId);
string_id!(PrincipalId);
string_id!(IdempotencyKey);

/// Identifier of the fixed root folder.
pub const ROOT_ID: &str = "root";

fn ulid() -> String {
    ulid::Ulid::generate().to_string().to_ascii_lowercase()
}

impl ObjectId {
    pub fn root() -> Self {
        Self(ROOT_ID.to_owned())
    }

    pub fn generate() -> Self {
        Self(format!("obj_{}", ulid()))
    }

    pub fn is_root(&self) -> bool {
        self.0 == ROOT_ID
    }
}

impl UploadId {
    pub fn generate() -> Self {
        Self(format!("upload_{}", ulid()))
    }
}

impl ChangeId {
    /// Derives the API-visible change id from the internal global sequence.
    ///
    /// Zero padding keeps `change_id` lexicographically monotonic in Change
    /// order, as the API contract requires.
    pub fn from_seq(seq: i64) -> Self {
        Self(format!("chg_{seq:020}"))
    }
}

impl RequestId {
    pub fn generate() -> Self {
        Self(format!("req_{}", ulid()))
    }
}

impl CursorId {
    pub const PREFIX: &'static str = "chgcur_";

    pub fn generate() -> Self {
        Self(format!("{}{}", Self::PREFIX, ulid()))
    }

    /// Returns true when the value is shaped like a Changes cursor. A value that
    /// fails this check is malformed rather than expired.
    pub fn is_well_formed(&self) -> bool {
        self.0.starts_with(Self::PREFIX) && self.0.len() > Self::PREFIX.len()
    }
}

impl PrincipalId {
    /// Principal used when authentication is disabled.
    pub fn local() -> Self {
        Self("local".to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_ids_use_documented_prefixes() {
        assert!(ObjectId::generate().as_str().starts_with("obj_"));
        assert!(UploadId::generate().as_str().starts_with("upload_"));
        assert!(RequestId::generate().as_str().starts_with("req_"));
        assert!(CursorId::generate().is_well_formed());
    }

    #[test]
    fn change_ids_are_lexicographically_monotonic() {
        let a = ChangeId::from_seq(9);
        let b = ChangeId::from_seq(10);
        let c = ChangeId::from_seq(1_000_000);
        assert!(a.as_str() < b.as_str());
        assert!(b.as_str() < c.as_str());
        assert_eq!(a.as_str(), "chg_00000000000000000009");
    }

    #[test]
    fn root_is_recognised() {
        assert!(ObjectId::root().is_root());
        assert!(!ObjectId::generate().is_root());
    }
}
