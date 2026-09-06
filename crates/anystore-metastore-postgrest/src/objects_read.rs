//! Read-side object access.

use anystore_domain::error::{DomainError, DomainResult};
use anystore_domain::object::ObjectView;
use anystore_domain::page::{KeysetCursor, decode_cursor, encode_cursor};
use anystore_domain::path::split_path;
use anystore_domain::{ObjectId, Page};
use anystore_metastore::ContentPointer;
use anystore_metastore::ObjectRepository;
use anystore_metastore::commands::{
    ListChildren, ListOrder, MetadataCondition, ObjectQuery, OrderBy,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::{Value, json};

use crate::PostgrestMetaStore;
use crate::decode;

fn order_by_name(order_by: OrderBy) -> &'static str {
    match order_by {
        OrderBy::Name => "name",
        OrderBy::CreatedAt => "created_at",
        OrderBy::UpdatedAt => "updated_at",
        OrderBy::Size => "size",
    }
}

fn order_name(order: ListOrder) -> &'static str {
    match order {
        ListOrder::Asc => "asc",
        ListOrder::Desc => "desc",
    }
}

/// Normalises the cursor sort key into the text form the RPC casts.
///
/// Validating here keeps a malformed cursor a `400 invalid_request` instead of
/// a database cast failure.
fn normalize_sort_key(order_by: OrderBy, key: Option<&str>) -> DomainResult<String> {
    let key = key.ok_or_else(|| DomainError::InvalidRequest("Invalid cursor.".into()))?;
    match order_by {
        OrderBy::Name => Ok(key.to_owned()),
        OrderBy::CreatedAt | OrderBy::UpdatedAt => {
            let moment: DateTime<Utc> = DateTime::parse_from_rfc3339(key)
                .map_err(|_| DomainError::InvalidRequest("Invalid cursor.".into()))?
                .with_timezone(&Utc);
            Ok(decode::timestamp(moment))
        }
        OrderBy::Size => {
            let size: i64 = key
                .parse()
                .map_err(|_| DomainError::InvalidRequest("Invalid cursor.".into()))?;
            Ok(size.to_string())
        }
    }
}

fn sort_key_of(view: &ObjectView, order_by: Option<OrderBy>) -> Option<String> {
    let object = &view.object;
    match order_by? {
        OrderBy::Name => Some(object.name.clone()),
        OrderBy::CreatedAt => Some(object.created_at.to_rfc3339()),
        OrderBy::UpdatedAt => Some(object.updated_at.to_rfc3339()),
        OrderBy::Size => Some(object.size.map(|s| s as i64).unwrap_or(-1).to_string()),
    }
}

/// Trims the look-ahead row and builds the opaque continuation cursor.
fn finish_page(
    mut items: Vec<ObjectView>,
    limit: u32,
    order_by: Option<OrderBy>,
) -> DomainResult<Page<ObjectView>> {
    let has_more = items.len() > limit as usize;
    items.truncate(limit as usize);

    let next_cursor = if has_more {
        items
            .last()
            .map(|last| {
                encode_cursor(&KeysetCursor {
                    k: sort_key_of(last, order_by),
                    i: last.object.id.to_string(),
                })
            })
            .transpose()?
    } else {
        None
    };

    Ok(Page::new(items, next_cursor, has_more))
}

fn metadata_conditions(conditions: &[(String, MetadataCondition)]) -> Value {
    Value::Array(
        conditions
            .iter()
            .map(|(key, condition)| match condition {
                MetadataCondition::Eq(value) => json!({"key": key, "op": "eq", "value": value}),
                MetadataCondition::Exists(present) => {
                    json!({"key": key, "op": "exists", "value": present})
                }
            })
            .collect(),
    )
}

#[async_trait]
impl ObjectRepository for PostgrestMetaStore {
    async fn get_object(&self, id: &ObjectId) -> DomainResult<Option<ObjectView>> {
        let data = self
            .client()
            .call("get_object", json!({"id": id.as_str()}))
            .await?;
        decode::optional_object_view(&data)
    }

    async fn content_pointer(&self, id: &ObjectId) -> DomainResult<Option<ContentPointer>> {
        let data = self
            .client()
            .call("content_pointer", json!({"id": id.as_str()}))
            .await?;
        decode::content_pointer(&data)
    }

    async fn list_children(&self, q: ListChildren) -> DomainResult<Page<ObjectView>> {
        let cursor = q
            .cursor
            .as_deref()
            .map(decode_cursor::<KeysetCursor>)
            .transpose()?;
        let cursor_key = match &cursor {
            Some(cursor) => Some(normalize_sort_key(q.order_by, cursor.k.as_deref())?),
            None => None,
        };

        let data = self
            .client()
            .call(
                "list_children",
                json!({
                    "parent_id": q.parent_id.as_str(),
                    // One extra row decides `has_more` without a second query.
                    "limit": u64::from(q.limit) + 1,
                    "order_by": order_by_name(q.order_by),
                    "order": order_name(q.order),
                    "cursor_key": cursor_key,
                    "cursor_id": cursor.as_ref().map(|c| c.i.clone()),
                }),
            )
            .await?;

        let items = decode::object_views(decode::array(&data, "items")?)?;
        finish_page(items, q.limit, Some(q.order_by))
    }

    async fn resolve_path(&self, path: &str) -> DomainResult<Option<ObjectView>> {
        let segments = split_path(path)?;
        let data = self
            .client()
            .call("resolve_path", json!({"segments": segments}))
            .await?;
        decode::optional_object_view(&data)
    }

    async fn query_objects(&self, q: ObjectQuery) -> DomainResult<Page<ObjectView>> {
        let cursor = q
            .cursor
            .as_deref()
            .map(decode_cursor::<KeysetCursor>)
            .transpose()?;

        let data = self
            .client()
            .call(
                "query_objects",
                json!({
                    "kind": q.kind.map(|kind| kind.as_str()),
                    "name": q.name,
                    "parent_id": q.parent_id.as_ref().map(|id| id.to_string()),
                    "metadata": metadata_conditions(&q.metadata),
                    "limit": u64::from(q.limit) + 1,
                    "cursor_id": cursor.as_ref().map(|c| c.i.clone()),
                }),
            )
            .await?;

        let items = decode::object_views(decode::array(&data, "items")?)?;
        finish_page(items, q.limit, None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anystore_domain::object::{ContentState, Object, ObjectKind, Revision};

    fn view(name: &str, size: Option<u64>) -> ObjectView {
        ObjectView {
            object: Object {
                id: ObjectId::new(format!("obj_{name}")),
                revision: Revision(1),
                kind: ObjectKind::File,
                name: name.to_owned(),
                parent_id: Some(ObjectId::root()),
                content_type: None,
                size,
                sha256: None,
                content_state: Some(ContentState::None),
                metadata: json!({}),
                created_at: Utc::now(),
                updated_at: Utc::now(),
            },
            path: format!("/{name}"),
        }
    }

    #[test]
    fn a_full_page_yields_a_cursor_and_drops_the_look_ahead_row() {
        let page = finish_page(
            vec![view("a", None), view("b", None), view("c", None)],
            2,
            Some(OrderBy::Name),
        )
        .unwrap();
        assert_eq!(page.items.len(), 2);
        assert!(page.has_more);
        let cursor: KeysetCursor = decode_cursor(page.next_cursor.as_deref().unwrap()).unwrap();
        assert_eq!(cursor.k.as_deref(), Some("b"));
        assert_eq!(cursor.i, "obj_b");
    }

    #[test]
    fn a_short_page_has_no_cursor() {
        let page = finish_page(vec![view("a", None)], 10, Some(OrderBy::Name)).unwrap();
        assert!(!page.has_more);
        assert!(page.next_cursor.is_none());
    }

    #[test]
    fn folders_sort_below_every_size() {
        assert_eq!(
            sort_key_of(&view("a", None), Some(OrderBy::Size)).unwrap(),
            "-1"
        );
        assert_eq!(
            sort_key_of(&view("a", Some(7)), Some(OrderBy::Size)).unwrap(),
            "7"
        );
    }

    #[test]
    fn cursor_keys_are_validated_before_they_reach_the_database() {
        assert_eq!(
            normalize_sort_key(OrderBy::CreatedAt, Some("not-a-time"))
                .unwrap_err()
                .code(),
            "invalid_request"
        );
        assert_eq!(
            normalize_sort_key(OrderBy::Size, Some("huge"))
                .unwrap_err()
                .code(),
            "invalid_request"
        );
        assert_eq!(
            normalize_sort_key(OrderBy::Name, None).unwrap_err().code(),
            "invalid_request"
        );
        assert_eq!(
            normalize_sort_key(OrderBy::CreatedAt, Some("2026-09-01T10:00:00+02:00")).unwrap(),
            "2026-09-01T08:00:00.000000Z"
        );
    }

    #[test]
    fn metadata_conditions_serialise_as_the_rpc_expects() {
        let conditions = vec![
            ("owner".to_owned(), MetadataCondition::Eq(json!("alice"))),
            ("archived".to_owned(), MetadataCondition::Exists(false)),
        ];
        assert_eq!(
            metadata_conditions(&conditions),
            json!([
                {"key": "owner", "op": "eq", "value": "alice"},
                {"key": "archived", "op": "exists", "value": false}
            ])
        );
    }
}
