//! Decodes RPC payloads into domain types.
//!
//! Every unexpected shape becomes an internal error rather than a default, so a
//! database drift shows up immediately instead of silently corrupting a read.

use anystore_domain::change::{ChangeAction, ChangeRecord};
use anystore_domain::error::{DomainError, DomainResult};
use anystore_domain::object::{ContentState, Object, ObjectKind, ObjectView, Revision};
use anystore_domain::upload::{UploadMode, UploadRecord, UploadState};
use anystore_domain::{ChangeId, IdempotencyKey, ObjectId, RequestId, UploadId};
use anystore_metastore::ContentPointer;
use anystore_metastore::response::StoredResponse;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::Value;

/// Wire format for every timestamp the adapter sends.
///
/// Microseconds match PostgreSQL's `timestamptz` resolution, so a value read
/// back is exactly the value that was written.
pub(crate) fn timestamp(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(SecondsFormat::Micros, true)
}

fn missing(field: &str) -> DomainError {
    DomainError::internal(format!("rpc payload is missing {field:?}"))
}

pub(crate) fn text<'a>(value: &'a Value, field: &str) -> DomainResult<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| missing(field))
}

pub(crate) fn optional_text<'a>(value: &'a Value, field: &str) -> Option<&'a str> {
    value.get(field).and_then(Value::as_str)
}

pub(crate) fn integer(value: &Value, field: &str) -> DomainResult<u64> {
    value
        .get(field)
        .and_then(Value::as_u64)
        .ok_or_else(|| missing(field))
}

fn optional_integer(value: &Value, field: &str) -> DomainResult<Option<u64>> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(other) => other
            .as_u64()
            .map(Some)
            .ok_or_else(|| DomainError::internal(format!("rpc field {field:?} is not an integer"))),
    }
}

pub(crate) fn boolean(value: &Value, field: &str) -> DomainResult<bool> {
    value
        .get(field)
        .and_then(Value::as_bool)
        .ok_or_else(|| missing(field))
}

pub(crate) fn moment(value: &Value, field: &str) -> DomainResult<DateTime<Utc>> {
    let raw = text(value, field)?;
    DateTime::parse_from_rfc3339(raw)
        .map(|ts| ts.with_timezone(&Utc))
        .map_err(|_| DomainError::internal(format!("rpc field {field:?} is not an RFC 3339 time")))
}

fn optional_moment(value: &Value, field: &str) -> DomainResult<Option<DateTime<Utc>>> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(_) => moment(value, field).map(Some),
    }
}

pub(crate) fn array<'a>(value: &'a Value, field: &str) -> DomainResult<&'a Vec<Value>> {
    value
        .get(field)
        .and_then(Value::as_array)
        .ok_or_else(|| missing(field))
}

pub(crate) fn is_null(value: &Value) -> bool {
    value.is_null()
}

pub(crate) fn object_view(value: &Value) -> DomainResult<ObjectView> {
    let kind_raw = text(value, "kind")?;
    let kind = ObjectKind::parse(kind_raw)
        .ok_or_else(|| DomainError::internal(format!("unknown object kind {kind_raw:?}")))?;

    let content_state = match optional_text(value, "content_state") {
        None => None,
        Some(raw) => Some(
            ContentState::parse(raw)
                .ok_or_else(|| DomainError::internal(format!("unknown content_state {raw:?}")))?,
        ),
    };

    Ok(ObjectView {
        object: Object {
            id: ObjectId::new(text(value, "id")?),
            revision: Revision(integer(value, "revision")?),
            kind,
            name: text(value, "name")?.to_owned(),
            parent_id: optional_text(value, "parent_id").map(ObjectId::new),
            content_type: optional_text(value, "content_type").map(str::to_owned),
            size: optional_integer(value, "size")?,
            sha256: optional_text(value, "sha256").map(str::to_owned),
            content_state,
            metadata: value.get("metadata").cloned().unwrap_or(Value::Null),
            created_at: moment(value, "created_at")?,
            updated_at: moment(value, "updated_at")?,
        },
        path: text(value, "path")?.to_owned(),
    })
}

pub(crate) fn optional_object_view(value: &Value) -> DomainResult<Option<ObjectView>> {
    if is_null(value) {
        return Ok(None);
    }
    object_view(value).map(Some)
}

pub(crate) fn object_views(items: &[Value]) -> DomainResult<Vec<ObjectView>> {
    items.iter().map(object_view).collect()
}

pub(crate) fn content_pointer(value: &Value) -> DomainResult<Option<ContentPointer>> {
    if is_null(value) {
        return Ok(None);
    }
    Ok(Some(ContentPointer {
        blob_backend: text(value, "blob_backend")?.to_owned(),
        blob_ref: text(value, "blob_ref")?.to_owned(),
    }))
}

pub(crate) fn upload_record(value: &Value) -> DomainResult<UploadRecord> {
    let state_raw = text(value, "state")?;
    let mode_raw = text(value, "mode")?;

    Ok(UploadRecord {
        id: UploadId::new(text(value, "id")?),
        object_id: ObjectId::new(text(value, "object_id")?),
        state: UploadState::parse(state_raw)
            .ok_or_else(|| DomainError::internal(format!("unknown upload state {state_raw:?}")))?,
        mode: UploadMode::parse(mode_raw)
            .ok_or_else(|| DomainError::internal(format!("unknown upload mode {mode_raw:?}")))?,
        blob_backend: text(value, "blob_backend")?.to_owned(),
        blob_ref: text(value, "blob_ref")?.to_owned(),
        provider_upload_id: optional_text(value, "provider_upload_id").map(str::to_owned),
        expected_size: integer(value, "expected_size")?,
        content_type: text(value, "content_type")?.to_owned(),
        expected_sha256: optional_text(value, "expected_sha256").map(str::to_owned),
        provider_completed: boolean(value, "provider_completed")?,
        created_at: moment(value, "created_at")?,
        expires_at: moment(value, "expires_at")?,
        completed_at: optional_moment(value, "completed_at")?,
        aborted_at: optional_moment(value, "aborted_at")?,
    })
}

pub(crate) fn optional_upload_record(value: &Value) -> DomainResult<Option<UploadRecord>> {
    if is_null(value) {
        return Ok(None);
    }
    upload_record(value).map(Some)
}

pub(crate) fn change_record(value: &Value) -> DomainResult<ChangeRecord> {
    let action_raw = text(value, "action")?;
    Ok(ChangeRecord {
        change_id: ChangeId::new(text(value, "change_id")?),
        object_id: ObjectId::new(text(value, "object_id")?),
        revision: Revision(integer(value, "revision")?),
        action: ChangeAction::parse(action_raw).ok_or_else(|| {
            DomainError::internal(format!("unknown change action {action_raw:?}"))
        })?,
        changed_at: moment(value, "changed_at")?,
        request_id: RequestId::new(text(value, "request_id")?),
        idempotency_key: optional_text(value, "idempotency_key").map(IdempotencyKey::new),
        tombstone: boolean(value, "tombstone")?,
    })
}

/// Decodes a response the database rendered inside the mutation transaction.
pub(crate) fn stored_response(value: &Value) -> DomainResult<StoredResponse> {
    let status = u16::try_from(integer(value, "status")?)
        .map_err(|_| DomainError::internal("rpc response status is out of range"))?;

    let headers = array(value, "headers")?
        .iter()
        .map(|pair| {
            let pair = pair
                .as_array()
                .filter(|pair| pair.len() == 2)
                .ok_or_else(|| DomainError::internal("rpc response header is malformed"))?;
            let name = pair[0]
                .as_str()
                .ok_or_else(|| DomainError::internal("rpc response header name is malformed"))?;
            let value = pair[1]
                .as_str()
                .ok_or_else(|| DomainError::internal("rpc response header value is malformed"))?;
            Ok((name.to_owned(), value.to_owned()))
        })
        .collect::<DomainResult<Vec<(String, String)>>>()?;

    Ok(StoredResponse {
        status,
        headers,
        body: base64_bytes(text(value, "body_b64")?)?,
    })
}

pub(crate) fn encode_response(response: &StoredResponse) -> Value {
    serde_json::json!({
        "status": response.status,
        "headers": response
            .headers
            .iter()
            .map(|(name, value)| serde_json::json!([name, value]))
            .collect::<Vec<Value>>(),
        "body_b64": STANDARD.encode(&response.body),
    })
}

/// PostgreSQL's `encode(..., 'base64')` wraps at 76 characters, so line breaks
/// are stripped before decoding.
fn base64_bytes(raw: &str) -> DomainResult<Vec<u8>> {
    let compact: String = raw.chars().filter(|c| !c.is_ascii_whitespace()).collect();
    STANDARD
        .decode(compact.as_bytes())
        .map_err(|_| DomainError::internal("rpc response body is not valid base64"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn file_view() -> Value {
        json!({
            "id": "obj_1",
            "revision": 3,
            "kind": "file",
            "name": "example.pdf",
            "parent_id": "root",
            "content_type": "application/pdf",
            "size": 12,
            "sha256": "abc",
            "content_state": "ready",
            "metadata": {"k": "v"},
            "created_at": "2026-09-01T10:00:00.000001Z",
            "updated_at": "2026-09-01T10:00:00.000002Z",
            "path": "/example.pdf"
        })
    }

    #[test]
    fn object_views_decode_every_field() {
        let view = object_view(&file_view()).unwrap();
        assert_eq!(view.object.id.as_str(), "obj_1");
        assert_eq!(view.object.revision, Revision(3));
        assert_eq!(view.object.kind, ObjectKind::File);
        assert_eq!(view.object.parent_id.as_ref().unwrap().as_str(), "root");
        assert_eq!(view.object.size, Some(12));
        assert_eq!(view.object.content_state, Some(ContentState::Ready));
        assert_eq!(view.object.metadata, json!({"k": "v"}));
        assert_eq!(view.path, "/example.pdf");
        assert_eq!(view.object.created_at.timestamp_micros() % 1_000_000, 1);
    }

    #[test]
    fn root_decodes_without_a_parent_or_content() {
        let value = json!({
            "id": "root", "revision": 1, "kind": "folder", "name": "",
            "parent_id": null, "content_type": null, "size": null, "sha256": null,
            "content_state": null, "metadata": {},
            "created_at": "2026-09-01T10:00:00.000000Z",
            "updated_at": "2026-09-01T10:00:00.000000Z",
            "path": "/"
        });
        let view = object_view(&value).unwrap();
        assert!(view.object.parent_id.is_none());
        assert!(view.object.content_state.is_none());
        assert_eq!(view.path, "/");
    }

    #[test]
    fn a_null_object_decodes_as_absent() {
        assert!(optional_object_view(&Value::Null).unwrap().is_none());
    }

    #[test]
    fn unknown_enum_values_are_internal_errors() {
        let mut value = file_view();
        value["kind"] = json!("directory");
        assert_eq!(object_view(&value).unwrap_err().code(), "internal_error");

        let mut value = file_view();
        value["content_state"] = json!("half");
        assert_eq!(object_view(&value).unwrap_err().code(), "internal_error");
    }

    #[test]
    fn missing_fields_are_internal_errors() {
        let mut value = file_view();
        value.as_object_mut().unwrap().remove("path");
        assert_eq!(object_view(&value).unwrap_err().code(), "internal_error");
    }

    #[test]
    fn upload_records_decode_optional_timestamps() {
        let value = json!({
            "id": "upload_1", "object_id": "obj_1", "state": "ready", "mode": "multipart",
            "blob_backend": "test", "blob_ref": "blobs/upload_1",
            "provider_upload_id": null, "expected_size": 42,
            "content_type": "text/plain", "expected_sha256": null,
            "provider_completed": false,
            "created_at": "2026-09-01T10:00:00.000000Z",
            "expires_at": "2026-09-02T10:00:00.000000Z",
            "completed_at": null, "aborted_at": null
        });
        let record = upload_record(&value).unwrap();
        assert_eq!(record.state, UploadState::Ready);
        assert_eq!(record.mode, UploadMode::Multipart);
        assert_eq!(record.expected_size, 42);
        assert!(record.completed_at.is_none());
        assert!(record.aborted_at.is_none());
    }

    #[test]
    fn change_records_decode_tombstones() {
        let value = json!({
            "change_id": "chg_00000000000000000007",
            "object_id": "obj_1", "revision": 2, "action": "deleted",
            "changed_at": "2026-09-01T10:00:00.000000Z",
            "request_id": "req_1", "idempotency_key": "key-1", "tombstone": true
        });
        let record = change_record(&value).unwrap();
        assert_eq!(record.action, ChangeAction::Deleted);
        assert!(record.tombstone);
        assert_eq!(record.idempotency_key.unwrap().as_str(), "key-1");
    }

    #[test]
    fn stored_responses_round_trip_byte_for_byte() {
        let original = StoredResponse::json(201, br#"{"id":"obj_1"}"#.to_vec());
        let decoded = stored_response(&encode_response(&original)).unwrap();
        assert_eq!(decoded, original);

        let empty = StoredResponse::no_content();
        assert_eq!(stored_response(&encode_response(&empty)).unwrap(), empty);
    }

    #[test]
    fn wrapped_base64_from_postgres_is_accepted() {
        let value = json!({
            "status": 200,
            "headers": [["content-type", "application/json"]],
            "body_b64": "eyJpZCI6ICJvYmpfYSIsICJraW5kIjogImZvbGRlciIsICJuYW1lIjogInJlcG9y\ndHMifQ=="
        });
        let decoded = stored_response(&value).unwrap();
        assert_eq!(
            String::from_utf8(decoded.body).unwrap(),
            r#"{"id": "obj_a", "kind": "folder", "name": "reports"}"#
        );
    }

    #[test]
    fn a_malformed_body_is_an_internal_error() {
        let value = json!({"status": 200, "headers": [], "body_b64": "!!!"});
        assert_eq!(
            stored_response(&value).unwrap_err().code(),
            "internal_error"
        );
    }

    #[test]
    fn timestamps_are_sent_with_microsecond_precision() {
        let value = DateTime::parse_from_rfc3339("2026-09-01T10:00:00.123456789Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(timestamp(value), "2026-09-01T10:00:00.123456Z");
    }
}
