//! Metadata query handler.

use anystore_application::query::{QueryRequest, QueryService};
use anystore_domain::error::{DomainError, DomainResult};
use anystore_domain::object::ObjectKind;
use anystore_domain::ObjectId;
use anystore_metastore::commands::MetadataCondition;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Response;
use axum::Extension;
use serde_json::Value;
use std::sync::Arc;

use crate::HttpState;
use crate::request::{RequestMeta, json_response, parse_json};
use crate::respond;

/// Parses the V1 metadata condition set: `eq` and `exists`, all ANDed.
fn parse_conditions(value: Option<&Value>) -> DomainResult<Vec<(String, MetadataCondition)>> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let Value::Object(map) = value else {
        return Err(DomainError::InvalidRequest(
            "metadata must be an object of conditions.".into(),
        ));
    };

    let mut conditions = Vec::with_capacity(map.len());
    for (key, condition) in map {
        let Value::Object(condition) = condition else {
            return Err(DomainError::InvalidRequest(format!(
                "metadata condition for {key:?} must be an object."
            )));
        };
        if condition.len() != 1 {
            return Err(DomainError::InvalidRequest(format!(
                "metadata condition for {key:?} must specify exactly one operator."
            )));
        }
        let (operator, operand) = condition.iter().next().expect("length checked above");
        let parsed = match operator.as_str() {
            "eq" => MetadataCondition::Eq(operand.clone()),
            "exists" => MetadataCondition::Exists(operand.as_bool().ok_or_else(|| {
                DomainError::InvalidRequest("exists must be a boolean.".into())
            })?),
            other => {
                return Err(DomainError::InvalidRequest(format!(
                    "unsupported metadata operator {other:?}; v1 supports eq and exists."
                )));
            }
        };
        conditions.push((key.clone(), parsed));
    }
    Ok(conditions)
}

pub async fn query(
    State(state): State<HttpState>,
    Extension(meta): Extension<RequestMeta>,
    body: Bytes,
) -> Response {
    let request_id = meta.request_id.clone();
    respond(Arc::clone(&state.app.metrics), &request_id, async move {
        let parsed = parse_json(&body)?;

        let kind = match parsed.get("kind").and_then(Value::as_str) {
            Some(raw) => Some(ObjectKind::parse(raw).ok_or_else(|| {
                DomainError::InvalidRequest("kind must be 'file' or 'folder'.".into())
            })?),
            None => None,
        };

        let limit = match parsed.get("limit") {
            Some(Value::Null) | None => None,
            Some(value) => Some(value.as_u64().and_then(|v| u32::try_from(v).ok()).ok_or_else(
                || DomainError::InvalidRequest("limit must be a positive integer.".into()),
            )?),
        };

        let service = QueryService::new(Arc::clone(&state.app));
        let value = service
            .query(QueryRequest {
                kind,
                name: parsed
                    .get("name")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                parent_id: parsed
                    .get("parent_id")
                    .and_then(Value::as_str)
                    .map(ObjectId::new),
                metadata: parse_conditions(parsed.get("metadata"))?,
                limit,
                cursor: parsed
                    .get("cursor")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            })
            .await?;

        Ok(json_response(StatusCode::OK, &value, &meta.request_id))
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn eq_and_exists_are_parsed() {
        let value = json!({"foo": {"eq": "bar"}, "year": {"exists": true}});
        let mut parsed = parse_conditions(Some(&value)).unwrap();
        parsed.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(parsed[0].0, "foo");
        assert_eq!(parsed[0].1, MetadataCondition::Eq(json!("bar")));
        assert_eq!(parsed[1].1, MetadataCondition::Exists(true));
    }

    #[test]
    fn unsupported_operators_are_rejected() {
        let value = json!({"foo": {"gt": 3}});
        assert_eq!(
            parse_conditions(Some(&value)).unwrap_err().code(),
            "invalid_request"
        );
    }

    #[test]
    fn conditions_must_name_exactly_one_operator() {
        let value = json!({"foo": {"eq": 1, "exists": true}});
        assert!(parse_conditions(Some(&value)).is_err());
    }
}
