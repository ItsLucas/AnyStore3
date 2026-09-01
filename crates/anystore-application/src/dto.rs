//! Public JSON representations.
//!
//! These live in the application layer because mutation responses must be
//! rendered *inside* the mutation transaction so an idempotent replay can
//! return the original bytes exactly.

use anystore_blobstore::{PreparedUpload, SignedPart};
use anystore_domain::change::ChangeRecord;
use anystore_domain::error::DomainError;
use anystore_domain::object::ObjectView;
use anystore_domain::upload::UploadMode;
use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{Map, Value, json};

pub fn timestamp(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(SecondsFormat::Micros, true)
}

/// Renders an Object.
///
/// Folders omit `content_type`, `size`, `sha256` and `content_state`. No
/// internal storage pointer is ever included.
pub fn object_json(view: &ObjectView) -> Value {
    let object = &view.object;
    let mut map = Map::new();

    map.insert("id".into(), json!(object.id.as_str()));
    map.insert("revision".into(), json!(object.revision.get()));
    map.insert("kind".into(), json!(object.kind.as_str()));
    map.insert("name".into(), json!(object.name));
    map.insert(
        "parent_id".into(),
        match &object.parent_id {
            Some(id) => json!(id.as_str()),
            None => Value::Null,
        },
    );
    map.insert("path".into(), json!(view.path));

    if object.is_file() {
        map.insert("content_type".into(), json!(object.content_type));
        map.insert("size".into(), json!(object.size));
        map.insert("sha256".into(), json!(object.sha256));
        map.insert(
            "content_state".into(),
            json!(object.content_state.map(|s| s.as_str())),
        );
    }

    map.insert("metadata".into(), object.metadata.clone());
    map.insert("created_at".into(), json!(timestamp(object.created_at)));
    map.insert("updated_at".into(), json!(timestamp(object.updated_at)));

    Value::Object(map)
}

/// Renders a Change record.
///
/// Never contains file bytes or the metadata document.
pub fn change_json(record: &ChangeRecord) -> Value {
    let mut map = Map::new();
    map.insert("change_id".into(), json!(record.change_id.as_str()));
    map.insert("object_id".into(), json!(record.object_id.as_str()));
    map.insert("revision".into(), json!(record.revision.get()));
    map.insert("action".into(), json!(record.action.as_str()));
    map.insert("changed_at".into(), json!(timestamp(record.changed_at)));
    map.insert("request_id".into(), json!(record.request_id.as_str()));
    map.insert(
        "idempotency_key".into(),
        match &record.idempotency_key {
            Some(key) => json!(key.as_str()),
            None => Value::Null,
        },
    );
    if record.tombstone {
        map.insert("tombstone".into(), json!(true));
    }
    Value::Object(map)
}

pub fn page_json(items: Vec<Value>, next_cursor: Option<&str>, has_more: bool) -> Value {
    json!({
        "items": items,
        "next_cursor": next_cursor,
        "has_more": has_more,
    })
}

pub fn error_json(error: &DomainError, request_id: &str) -> Value {
    let mut map = Map::new();
    map.insert("error".into(), json!(error.code()));
    map.insert("message".into(), json!(error.public_message()));
    map.insert("request_id".into(), json!(request_id));
    if let DomainError::RevisionConflict { current_revision } = error {
        map.insert("current_revision".into(), json!(current_revision));
    }
    Value::Object(map)
}

pub fn upload_session_json(
    upload_id: &str,
    object_id: &str,
    prepared: &PreparedUpload,
) -> Value {
    let mut map = Map::new();
    map.insert("id".into(), json!(upload_id));
    map.insert("object_id".into(), json!(object_id));
    map.insert("mode".into(), json!(prepared.mode.as_str()));

    match prepared.mode {
        UploadMode::Single => {
            if let Some(single) = &prepared.single {
                map.insert(
                    "upload".into(),
                    json!({
                        "method": single.method,
                        "url": single.url,
                        "headers": single.headers,
                    }),
                );
            }
        }
        UploadMode::Multipart => {
            map.insert("part_size".into(), json!(prepared.part_size));
        }
    }

    map.insert("expires_at".into(), json!(timestamp(prepared.expires_at)));
    Value::Object(map)
}

pub fn parts_json(parts: &[SignedPart]) -> Value {
    json!({
        "parts": parts
            .iter()
            .map(|p| json!({
                "part_number": p.part_number,
                "method": p.method,
                "url": p.url,
            }))
            .collect::<Vec<_>>(),
    })
}

pub fn upload_complete_json(view: &ObjectView) -> Value {
    let object = &view.object;
    json!({
        "object_id": object.id.as_str(),
        "revision": object.revision.get(),
        "content_state": object.content_state.map(|s| s.as_str()),
        "size": object.size,
        "sha256": object.sha256,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use anystore_domain::object::{ContentState, Object, ObjectKind, Revision};
    use anystore_domain::ObjectId;
    use serde_json::json;

    fn object(kind: ObjectKind) -> ObjectView {
        ObjectView {
            object: Object {
                id: ObjectId::new("obj_1"),
                revision: Revision(3),
                kind,
                name: "example.pdf".into(),
                parent_id: Some(ObjectId::root()),
                content_type: Some("application/pdf".into()),
                size: Some(12),
                sha256: Some("abc".into()),
                content_state: Some(ContentState::Ready),
                metadata: json!({"k": "v"}),
                created_at: Utc::now(),
                updated_at: Utc::now(),
            },
            path: "/example.pdf".into(),
        }
    }

    #[test]
    fn folders_omit_content_fields() {
        let value = object_json(&object(ObjectKind::Folder));
        for key in ["content_type", "size", "sha256", "content_state"] {
            assert!(value.get(key).is_none(), "folder must omit {key}");
        }
    }

    #[test]
    fn files_include_content_fields() {
        let value = object_json(&object(ObjectKind::File));
        assert_eq!(value["content_state"], json!("ready"));
        assert_eq!(value["size"], json!(12));
    }

    #[test]
    fn objects_never_expose_storage_pointers() {
        let text = object_json(&object(ObjectKind::File)).to_string();
        for leak in ["blob_ref", "blob_backend", "bucket", "region"] {
            assert!(!text.contains(leak), "object payload leaked {leak}");
        }
    }

    #[test]
    fn revision_conflict_reports_the_current_revision() {
        let err = DomainError::RevisionConflict {
            current_revision: 8,
        };
        let value = error_json(&err, "req_1");
        assert_eq!(value["error"], json!("revision_conflict"));
        assert_eq!(value["current_revision"], json!(8));
    }

    #[test]
    fn storage_errors_stay_opaque() {
        let err = DomainError::storage("cos bucket anystore-125 denied");
        let value = error_json(&err, "req_1");
        assert_eq!(value["message"], json!("Storage backend error."));
    }
}
