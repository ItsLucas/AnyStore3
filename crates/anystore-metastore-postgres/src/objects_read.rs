//! Read-side object queries.

use anystore_domain::error::{DomainError, DomainResult};
use anystore_domain::object::{ObjectKind, ObjectView};
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
use sqlx::{AssertSqlSafe, PgConnection, Postgres, QueryBuilder, Row};

use crate::PostgresMetaStore;
use crate::errors::map_sqlx;
use crate::rows::{OBJECT_COLUMNS, ObjectRow, attach_paths, decode_object};

/// Resolves a path by walking one indexed `(parent_id, name)` lookup per
/// segment, rather than storing a materialised path on every descendant.
const RESOLVE_QUERY: &str = "
WITH RECURSIVE walk(idx, id) AS (
        SELECT 0, 'root'
    UNION ALL
        SELECT w.idx + 1, o.id
        FROM walk w
        JOIN objects o
          ON o.parent_id = w.id
         AND o.name = ($1::text[])[w.idx + 1]
         AND o.deleted_at IS NULL
        WHERE w.idx < array_length($1::text[], 1)
)
SELECT id FROM walk WHERE idx = array_length($1::text[], 1)
";

pub(crate) async fn load_view(
    conn: &mut PgConnection,
    id: &ObjectId,
) -> DomainResult<Option<ObjectView>> {
    let sql = format!("SELECT {OBJECT_COLUMNS} FROM objects WHERE id = $1 AND deleted_at IS NULL");
    let row = sqlx::query(AssertSqlSafe(sql))
        .bind(id.as_str())
        .fetch_optional(&mut *conn)
        .await
        .map_err(map_sqlx)?;

    let Some(row) = row else {
        return Ok(None);
    };
    let decoded = decode_object(&row)?;
    Ok(attach_paths(conn, vec![decoded]).await?.pop())
}

#[async_trait]
impl ObjectRepository for PostgresMetaStore {
    async fn get_object(&self, id: &ObjectId) -> DomainResult<Option<ObjectView>> {
        let mut conn = self.pool().acquire().await.map_err(map_sqlx)?;
        load_view(&mut conn, id).await
    }

    async fn content_pointer(&self, id: &ObjectId) -> DomainResult<Option<ContentPointer>> {
        let row = sqlx::query(
            "SELECT blob_backend, blob_ref FROM objects
             WHERE id = $1 AND deleted_at IS NULL AND content_state = 'ready'",
        )
        .bind(id.as_str())
        .fetch_optional(self.pool())
        .await
        .map_err(map_sqlx)?;

        let Some(row) = row else {
            return Ok(None);
        };
        let blob_backend: Option<String> = row.try_get("blob_backend").map_err(map_sqlx)?;
        let blob_ref: Option<String> = row.try_get("blob_ref").map_err(map_sqlx)?;
        Ok(match (blob_backend, blob_ref) {
            (Some(blob_backend), Some(blob_ref)) => Some(ContentPointer {
                blob_backend,
                blob_ref,
            }),
            _ => None,
        })
    }

    async fn list_children(&self, q: ListChildren) -> DomainResult<Page<ObjectView>> {
        let cursor = q
            .cursor
            .as_deref()
            .map(decode_cursor::<KeysetCursor>)
            .transpose()?;

        let sort_expr = order_expression(q.order_by);
        let comparison = match q.order {
            ListOrder::Asc => ">",
            ListOrder::Desc => "<",
        };
        let direction = match q.order {
            ListOrder::Asc => "ASC",
            ListOrder::Desc => "DESC",
        };

        let mut builder: QueryBuilder<Postgres> = QueryBuilder::new("SELECT ");
        builder.push(OBJECT_COLUMNS);
        builder.push(" FROM objects WHERE deleted_at IS NULL AND parent_id = ");
        builder.push_bind(q.parent_id.to_string());

        if let Some(cursor) = &cursor {
            builder.push(" AND (");
            builder.push(sort_expr);
            builder.push(", id) ");
            builder.push(comparison);
            builder.push(" (");
            bind_sort_key(&mut builder, q.order_by, cursor.k.as_deref())?;
            builder.push(", ");
            builder.push_bind(cursor.i.clone());
            builder.push(")");
        }

        builder.push(" ORDER BY ");
        builder.push(sort_expr);
        builder.push(" ");
        builder.push(direction);
        builder.push(", id ");
        builder.push(direction);
        builder.push(" LIMIT ");
        builder.push_bind(i64::from(q.limit) + 1);

        let mut conn = self.pool().acquire().await.map_err(map_sqlx)?;
        let rows = builder
            .build()
            .fetch_all(&mut *conn)
            .await
            .map_err(map_sqlx)?;

        let decoded = rows
            .iter()
            .map(decode_object)
            .collect::<DomainResult<Vec<ObjectRow>>>()?;

        finish_page(&mut conn, decoded, q.limit, Some(q.order_by)).await
    }

    async fn resolve_path(&self, path: &str) -> DomainResult<Option<ObjectView>> {
        let segments = split_path(path)?;
        if segments.is_empty() {
            return self.get_object(&ObjectId::root()).await;
        }

        let id: Option<String> = sqlx::query_scalar(RESOLVE_QUERY)
            .bind(&segments)
            .fetch_optional(self.pool())
            .await
            .map_err(map_sqlx)?;

        match id {
            Some(id) => self.get_object(&ObjectId::new(id)).await,
            None => Ok(None),
        }
    }

    async fn query_objects(&self, q: ObjectQuery) -> DomainResult<Page<ObjectView>> {
        let cursor = q
            .cursor
            .as_deref()
            .map(decode_cursor::<KeysetCursor>)
            .transpose()?;

        let mut builder: QueryBuilder<Postgres> = QueryBuilder::new("SELECT ");
        builder.push(OBJECT_COLUMNS);
        builder.push(" FROM objects WHERE deleted_at IS NULL");

        if let Some(kind) = q.kind {
            builder.push(" AND kind = ");
            builder.push_bind(kind.as_str().to_owned());
        }
        if let Some(name) = &q.name {
            builder.push(" AND name = ");
            builder.push_bind(name.clone());
        }
        if let Some(parent_id) = &q.parent_id {
            builder.push(" AND parent_id = ");
            builder.push_bind(parent_id.to_string());
        }

        // All supplied conditions are ANDed, on top-level metadata keys only.
        for (key, condition) in &q.metadata {
            match condition {
                MetadataCondition::Eq(value) => {
                    builder.push(" AND metadata -> ");
                    builder.push_bind(key.clone());
                    builder.push(" = ");
                    builder.push_bind(value.clone());
                }
                MetadataCondition::Exists(true) => {
                    builder.push(" AND jsonb_exists(metadata, ");
                    builder.push_bind(key.clone());
                    builder.push(")");
                }
                MetadataCondition::Exists(false) => {
                    builder.push(" AND NOT jsonb_exists(metadata, ");
                    builder.push_bind(key.clone());
                    builder.push(")");
                }
            }
        }

        if let Some(cursor) = &cursor {
            builder.push(" AND id > ");
            builder.push_bind(cursor.i.clone());
        }

        builder.push(" ORDER BY id ASC LIMIT ");
        builder.push_bind(i64::from(q.limit) + 1);

        let mut conn = self.pool().acquire().await.map_err(map_sqlx)?;
        let rows = builder
            .build()
            .fetch_all(&mut *conn)
            .await
            .map_err(map_sqlx)?;

        let decoded = rows
            .iter()
            .map(decode_object)
            .collect::<DomainResult<Vec<ObjectRow>>>()?;

        finish_page(&mut conn, decoded, q.limit, None).await
    }
}

fn order_expression(order_by: OrderBy) -> &'static str {
    match order_by {
        OrderBy::Name => "name",
        OrderBy::CreatedAt => "created_at",
        OrderBy::UpdatedAt => "updated_at",
        // Folders have no size; a total ordering keeps keyset pagination sound.
        OrderBy::Size => "COALESCE(size_bytes, -1)",
    }
}

fn bind_sort_key(
    builder: &mut QueryBuilder<Postgres>,
    order_by: OrderBy,
    key: Option<&str>,
) -> DomainResult<()> {
    let key = key.ok_or_else(|| DomainError::InvalidRequest("Invalid cursor.".into()))?;
    match order_by {
        OrderBy::Name => {
            builder.push_bind(key.to_owned());
        }
        OrderBy::CreatedAt | OrderBy::UpdatedAt => {
            let ts: DateTime<Utc> = DateTime::parse_from_rfc3339(key)
                .map_err(|_| DomainError::InvalidRequest("Invalid cursor.".into()))?
                .with_timezone(&Utc);
            builder.push_bind(ts);
        }
        OrderBy::Size => {
            let size: i64 = key
                .parse()
                .map_err(|_| DomainError::InvalidRequest("Invalid cursor.".into()))?;
            builder.push_bind(size);
        }
    }
    Ok(())
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
async fn finish_page(
    conn: &mut PgConnection,
    mut rows: Vec<ObjectRow>,
    limit: u32,
    order_by: Option<OrderBy>,
) -> DomainResult<Page<ObjectView>> {
    let has_more = rows.len() > limit as usize;
    rows.truncate(limit as usize);

    let items = attach_paths(conn, rows).await?;
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

/// Kind of a live object, used by write paths for parent validation.
pub(crate) async fn live_kind(
    conn: &mut PgConnection,
    id: &ObjectId,
    lock: bool,
) -> DomainResult<Option<ObjectKind>> {
    let sql = if lock {
        "SELECT kind FROM objects WHERE id = $1 AND deleted_at IS NULL FOR SHARE"
    } else {
        "SELECT kind FROM objects WHERE id = $1 AND deleted_at IS NULL"
    };
    let row = sqlx::query(sql)
        .bind(id.as_str())
        .fetch_optional(&mut *conn)
        .await
        .map_err(map_sqlx)?;

    row.map(|row| {
        let kind: String = row.try_get("kind").map_err(map_sqlx)?;
        ObjectKind::parse(&kind)
            .ok_or_else(|| DomainError::internal(format!("unknown object kind {kind:?}")))
    })
    .transpose()
}
