//! Metadata semantics.
//!
//! Metadata is an arbitrary JSON object. AnyStore stores it but never
//! interprets business meaning, and the application layer must not hard-code
//! metadata key names.

use crate::error::{DomainError, DomainResult};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// `set`/`remove` patch document from `PATCH /objects/{id}`.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct MetadataPatch {
    #[serde(default)]
    pub set: Map<String, Value>,
    #[serde(default)]
    pub remove: Vec<String>,
}

impl MetadataPatch {
    pub fn is_empty(&self) -> bool {
        self.set.is_empty() && self.remove.is_empty()
    }
}

/// Validates that a create-time metadata document is a JSON object.
pub fn validate_metadata_document(value: &Value) -> DomainResult<()> {
    match value {
        Value::Object(map) => {
            for key in map.keys() {
                validate_metadata_key(key)?;
            }
            Ok(())
        }
        _ => Err(DomainError::InvalidMetadata(
            "metadata must be a JSON object.".into(),
        )),
    }
}

fn validate_metadata_key(key: &str) -> DomainResult<()> {
    if key.is_empty() {
        return Err(DomainError::InvalidMetadata(
            "metadata keys must not be empty.".into(),
        ));
    }
    if key.contains('\0') {
        return Err(DomainError::InvalidMetadata(
            "metadata keys must not contain NUL.".into(),
        ));
    }
    Ok(())
}

pub fn validate_metadata_patch(patch: &MetadataPatch) -> DomainResult<()> {
    for key in patch.set.keys().chain(patch.remove.iter()) {
        validate_metadata_key(key)?;
    }
    Ok(())
}

/// Applies a metadata patch, returning the resulting document.
///
/// `remove` is applied after `set`, so a key present in both ends up removed.
pub fn apply_metadata_patch(current: &Value, patch: &MetadataPatch) -> DomainResult<Value> {
    let mut map = match current {
        Value::Object(map) => map.clone(),
        Value::Null => Map::new(),
        _ => {
            return Err(DomainError::InvalidMetadata(
                "stored metadata is not a JSON object.".into(),
            ));
        }
    };

    for (key, value) in &patch.set {
        map.insert(key.clone(), value.clone());
    }
    for key in &patch.remove {
        map.remove(key);
    }

    Ok(Value::Object(map))
}

/// Structural equality for metadata documents, used for no-op detection.
pub fn metadata_equal(a: &Value, b: &Value) -> bool {
    normalize(a) == normalize(b)
}

fn normalize(value: &Value) -> Value {
    match value {
        Value::Null => Value::Object(Map::new()),
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn set_then_remove_is_applied_in_order() {
        let current = json!({"a": 1, "old": true});
        let patch = MetadataPatch {
            set: json!({"b": 2, "old": "ignored"})
                .as_object()
                .unwrap()
                .clone(),
            remove: vec!["old".into()],
        };
        let result = apply_metadata_patch(&current, &patch).unwrap();
        assert_eq!(result, json!({"a": 1, "b": 2}));
    }

    #[test]
    fn removing_a_missing_key_is_a_no_op() {
        let current = json!({"a": 1});
        let patch = MetadataPatch {
            set: Map::new(),
            remove: vec!["nope".into()],
        };
        let result = apply_metadata_patch(&current, &patch).unwrap();
        assert!(metadata_equal(&result, &current));
    }

    #[test]
    fn any_json_value_is_accepted_as_a_metadata_value() {
        let doc = json!({"n": null, "arr": [1, {"k": "v"}], "nested": {"deep": true}});
        assert!(validate_metadata_document(&doc).is_ok());
    }

    #[test]
    fn non_object_metadata_is_rejected() {
        assert_eq!(
            validate_metadata_document(&json!([1, 2]))
                .unwrap_err()
                .code(),
            "invalid_metadata"
        );
    }

    #[test]
    fn null_and_empty_object_metadata_compare_equal() {
        assert!(metadata_equal(&Value::Null, &json!({})));
    }
}
