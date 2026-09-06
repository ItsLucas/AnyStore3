//! RPC envelope and error decoding.
//!
//! The database function answers with `{"ok": true, "data": ...}` or
//! `{"ok": false, "error": {"code", "message", ...}}`. Mapping `code` back onto
//! the domain taxonomy here is what keeps a name conflict a 409 instead of an
//! opaque 500.

use anystore_domain::error::{DomainError, DomainResult};
use serde_json::Value;

/// Structured failure reported by the database function.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RpcError {
    pub code: String,
    pub message: String,
    pub current_revision: Option<u64>,
}

impl RpcError {
    /// Maps the stable error code from the API contract onto the taxonomy.
    pub fn into_domain(self) -> DomainError {
        match self.code.as_str() {
            "invalid_request" => DomainError::InvalidRequest(self.message),
            "invalid_name" => DomainError::InvalidName(self.message),
            "invalid_metadata" => DomainError::InvalidMetadata(self.message),
            "object_not_found" => DomainError::ObjectNotFound,
            "upload_not_found" => DomainError::UploadNotFound,
            "name_conflict" => DomainError::NameConflict,
            "folder_not_empty" => DomainError::FolderNotEmpty,
            "invalid_move" => DomainError::InvalidMove(self.message),
            "not_a_folder" => DomainError::NotAFolder,
            "not_a_file" => DomainError::NotAFile,
            "content_not_ready" => DomainError::ContentNotReady,
            "idempotency_key_reused" => DomainError::IdempotencyKeyReused,
            "changes_cursor_expired" => DomainError::ChangesCursorExpired,
            "revision_conflict" => DomainError::RevisionConflict {
                // A conflict without the current revision would break the
                // contract's `current_revision` field, so it degrades to an
                // internal error instead of guessing.
                current_revision: match self.current_revision {
                    Some(revision) => revision,
                    None => {
                        return DomainError::internal("revision_conflict without current_revision");
                    }
                },
            },
            "checksum_mismatch" => DomainError::ChecksumMismatch(self.message),
            "rate_limited" => DomainError::RateLimited,
            "unauthorized" => DomainError::Unauthorized,
            "storage_error" => DomainError::storage(self.message),
            // Unknown codes stay opaque rather than leaking database detail.
            _ => DomainError::internal(format!("{}: {}", self.code, self.message)),
        }
    }
}

/// Unwraps the RPC envelope, returning the `data` payload.
///
/// Tolerates the single-row array and `{"data": ...}` shapes some PostgREST
/// gateways add, so a gateway change cannot silently look like a domain error.
pub fn decode_envelope(value: Value) -> DomainResult<Value> {
    let envelope = locate_envelope(value)
        .ok_or_else(|| DomainError::internal("cloudbase postgrest returned no rpc envelope"))?;

    let Value::Object(mut map) = envelope else {
        return Err(DomainError::internal(
            "cloudbase postgrest returned no rpc envelope",
        ));
    };

    let ok = map
        .get("ok")
        .and_then(Value::as_bool)
        .ok_or_else(|| DomainError::internal("rpc envelope is missing 'ok'"))?;

    if ok {
        return Ok(map.remove("data").unwrap_or(Value::Null));
    }

    let error = map.remove("error").unwrap_or(Value::Null);
    Err(decode_error(&error).into_domain())
}

fn locate_envelope(value: Value) -> Option<Value> {
    match value {
        Value::Object(ref map) if map.contains_key("ok") => Some(value),
        // A gateway that wraps the PostgREST body once.
        Value::Object(map) => map.get("data").cloned().and_then(locate_envelope),
        // PostgREST renders some function results as a single-row array.
        Value::Array(items) if items.len() == 1 => locate_envelope(items.into_iter().next()?),
        _ => None,
    }
}

fn decode_error(value: &Value) -> RpcError {
    RpcError {
        code: value
            .get("code")
            .and_then(Value::as_str)
            .unwrap_or("internal_error")
            .to_owned(),
        message: value
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("Internal error.")
            .to_owned(),
        current_revision: value.get("current_revision").and_then(Value::as_u64),
    }
}

/// Maps a gateway status onto the taxonomy.
///
/// A metadata-store failure is never the caller's fault, so credential and
/// deployment problems surface as internal errors rather than as a 401 or 404
/// that a client might act on.
pub fn map_http_status(status: u16, body: &str) -> DomainError {
    match status {
        401 | 403 => DomainError::internal(
            "cloudbase postgrest rejected the service credential; \
             check ANYSTORE_CLOUDBASE_PG_API_KEY",
        ),
        404 => DomainError::internal(
            "cloudbase postgrest rpc endpoint is missing; \
             check ANYSTORE_CLOUDBASE_PG_BASE_URL and apply the anystore RPC migration",
        ),
        408 | 504 => DomainError::internal("cloudbase postgrest request timed out"),
        429 => DomainError::RateLimited,
        400 | 422 => DomainError::internal(format!(
            "cloudbase postgrest rejected the rpc request: {body}"
        )),
        500..=599 => {
            DomainError::internal(format!("cloudbase postgrest failed ({status}): {body}"))
        }
        other => DomainError::internal(format!(
            "cloudbase postgrest returned an unexpected status {other}: {body}"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn success_envelopes_yield_their_data() {
        let data = decode_envelope(json!({"ok": true, "data": {"id": "obj_1"}})).unwrap();
        assert_eq!(data, json!({"id": "obj_1"}));
    }

    #[test]
    fn a_missing_data_field_decodes_as_null() {
        assert_eq!(decode_envelope(json!({"ok": true})).unwrap(), Value::Null);
    }

    #[test]
    fn single_row_arrays_are_unwrapped() {
        let data = decode_envelope(json!([{"ok": true, "data": 7}])).unwrap();
        assert_eq!(data, json!(7));
    }

    #[test]
    fn gateway_wrappers_are_unwrapped() {
        let data = decode_envelope(json!({"data": {"ok": true, "data": "x"}})).unwrap();
        assert_eq!(data, json!("x"));
    }

    #[test]
    fn a_body_without_an_envelope_is_internal() {
        let error = decode_envelope(json!({"message": "no route"})).unwrap_err();
        assert_eq!(error.code(), "internal_error");
        assert_eq!(decode_envelope(json!(null)).unwrap_err().status(), 500);
    }

    #[test]
    fn every_contract_error_code_round_trips() {
        let cases = [
            ("invalid_request", 400),
            ("invalid_name", 400),
            ("invalid_metadata", 400),
            ("unauthorized", 401),
            ("object_not_found", 404),
            ("upload_not_found", 404),
            ("name_conflict", 409),
            ("folder_not_empty", 409),
            ("invalid_move", 409),
            ("not_a_folder", 409),
            ("not_a_file", 409),
            ("content_not_ready", 409),
            ("idempotency_key_reused", 409),
            ("changes_cursor_expired", 410),
            ("checksum_mismatch", 422),
            ("rate_limited", 429),
            ("internal_error", 500),
            ("storage_error", 502),
        ];
        for (code, status) in cases {
            let error = decode_envelope(json!({
                "ok": false,
                "error": {"code": code, "message": "boom"}
            }))
            .unwrap_err();
            assert_eq!(error.code(), code, "code {code}");
            assert_eq!(error.status(), status, "status for {code}");
        }
    }

    #[test]
    fn revision_conflict_carries_the_current_revision() {
        let error = decode_envelope(json!({
            "ok": false,
            "error": {
                "code": "revision_conflict",
                "message": "Object has been modified.",
                "current_revision": 9
            }
        }))
        .unwrap_err();
        assert_eq!(error.status(), 412);
        match error {
            DomainError::RevisionConflict { current_revision } => {
                assert_eq!(current_revision, 9)
            }
            other => panic!("expected revision conflict, got {other:?}"),
        }
    }

    #[test]
    fn revision_conflict_without_a_revision_is_internal() {
        let error = decode_envelope(json!({
            "ok": false,
            "error": {"code": "revision_conflict", "message": "x"}
        }))
        .unwrap_err();
        assert_eq!(error.code(), "internal_error");
    }

    #[test]
    fn unknown_error_codes_stay_opaque() {
        let error = decode_envelope(json!({
            "ok": false,
            "error": {"code": "42P01", "message": "relation objects does not exist"}
        }))
        .unwrap_err();
        assert_eq!(error.code(), "internal_error");
        assert_eq!(error.public_message(), "Internal error.");
    }

    #[test]
    fn domain_messages_survive_the_round_trip() {
        let error = decode_envelope(json!({
            "ok": false,
            "error": {"code": "invalid_move", "message": "Root cannot be moved."}
        }))
        .unwrap_err();
        assert_eq!(error.public_message(), "Root cannot be moved.");
    }

    #[test]
    fn gateway_statuses_map_tightly() {
        assert_eq!(map_http_status(401, "").status(), 500);
        assert_eq!(map_http_status(403, "").status(), 500);
        assert_eq!(map_http_status(404, "").status(), 500);
        assert_eq!(map_http_status(429, "").code(), "rate_limited");
        assert_eq!(map_http_status(502, "bad gateway").status(), 500);
        assert_eq!(map_http_status(504, "").status(), 500);
        assert_eq!(map_http_status(418, "teapot").status(), 500);
    }

    #[test]
    fn gateway_failures_never_leak_their_body_publicly() {
        let error = map_http_status(500, "password=hunter2");
        assert_eq!(error.public_message(), "Internal error.");
    }
}
