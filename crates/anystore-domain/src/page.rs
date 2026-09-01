//! Pagination primitives and opaque cursor encoding.

use crate::error::{DomainError, DomainResult};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

#[derive(Clone, Debug, PartialEq)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
    pub has_more: bool,
}

impl<T> Page<T> {
    pub fn new(items: Vec<T>, next_cursor: Option<String>, has_more: bool) -> Self {
        Self {
            items,
            next_cursor,
            has_more,
        }
    }

    pub fn empty() -> Self {
        Self {
            items: Vec::new(),
            next_cursor: None,
            has_more: false,
        }
    }
}

/// Keyset cursor for object listing and metadata query.
///
/// Encoded opaquely so clients cannot depend on its structure.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct KeysetCursor {
    /// Sort key of the last item on the previous page, as a string.
    pub k: Option<String>,
    /// Object id of the last item on the previous page (tie-breaker).
    pub i: String,
}

pub fn encode_cursor<T: Serialize>(value: &T) -> DomainResult<String> {
    let bytes = serde_json::to_vec(value)
        .map_err(|e| DomainError::internal(format!("cursor encode failed: {e}")))?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

pub fn decode_cursor<T: DeserializeOwned>(cursor: &str) -> DomainResult<T> {
    let bytes = URL_SAFE_NO_PAD
        .decode(cursor.as_bytes())
        .map_err(|_| DomainError::InvalidRequest("Invalid cursor.".into()))?;
    serde_json::from_slice(&bytes)
        .map_err(|_| DomainError::InvalidRequest("Invalid cursor.".into()))
}

/// Bounds a client-supplied page limit.
pub fn clamp_limit(limit: Option<u32>, default: u32, max: u32) -> u32 {
    limit.unwrap_or(default).clamp(1, max)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursors_round_trip_and_are_opaque() {
        let cursor = KeysetCursor {
            k: Some("example.pdf".into()),
            i: "obj_01".into(),
        };
        let encoded = encode_cursor(&cursor).unwrap();
        assert!(!encoded.contains("example.pdf"));
        assert_eq!(decode_cursor::<KeysetCursor>(&encoded).unwrap(), cursor);
    }

    #[test]
    fn malformed_cursors_are_invalid_requests() {
        assert_eq!(
            decode_cursor::<KeysetCursor>("!!!!").unwrap_err().code(),
            "invalid_request"
        );
    }

    #[test]
    fn limits_are_clamped() {
        assert_eq!(clamp_limit(None, 100, 1000), 100);
        assert_eq!(clamp_limit(Some(0), 100, 1000), 1);
        assert_eq!(clamp_limit(Some(5000), 100, 1000), 1000);
    }
}
