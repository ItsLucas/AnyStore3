//! Row decoding and path rendering.

use anystore_domain::error::{DomainError, DomainResult};
use anystore_domain::object::{ContentState, Object, ObjectKind, ObjectView, Revision};
use anystore_domain::upload::{UploadMode, UploadRecord, UploadState};
use anystore_domain::{ObjectId, UploadId};
use sqlx::{PgConnection, Row, postgres::PgRow};
use std::collections::HashMap;

use crate::errors::map_sqlx;

pub const OBJECT_COLUMNS: &str = "id, revision, kind, name, parent_id, content_type, size_bytes, \
     sha256, content_state, blob_backend, blob_ref, metadata, created_at, updated_at";

/// An object row including internal-only storage pointers.
#[derive(Clone, Debug)]
pub struct ObjectRow {
    pub object: Object,
    pub blob_backend: Option<String>,
    pub blob_ref: Option<String>,
}

pub fn decode_object(row: &PgRow) -> DomainResult<ObjectRow> {
    let kind: String = row.try_get("kind").map_err(map_sqlx)?;
    let kind = ObjectKind::parse(&kind)
        .ok_or_else(|| DomainError::internal(format!("unknown object kind {kind:?}")))?;

    let content_state: Option<String> = row.try_get("content_state").map_err(map_sqlx)?;
    let content_state = content_state
        .as_deref()
        .map(|s| {
            ContentState::parse(s)
                .ok_or_else(|| DomainError::internal(format!("unknown content_state {s:?}")))
        })
        .transpose()?;

    let revision: i64 = row.try_get("revision").map_err(map_sqlx)?;
    let size: Option<i64> = row.try_get("size_bytes").map_err(map_sqlx)?;
    let parent_id: Option<String> = row.try_get("parent_id").map_err(map_sqlx)?;

    Ok(ObjectRow {
        object: Object {
            id: ObjectId::new(row.try_get::<String, _>("id").map_err(map_sqlx)?),
            revision: Revision(revision as u64),
            kind,
            name: row.try_get("name").map_err(map_sqlx)?,
            parent_id: parent_id.map(ObjectId::new),
            content_type: row.try_get("content_type").map_err(map_sqlx)?,
            size: size.map(|s| s as u64),
            sha256: row.try_get("sha256").map_err(map_sqlx)?,
            content_state,
            metadata: row.try_get("metadata").map_err(map_sqlx)?,
            created_at: row.try_get("created_at").map_err(map_sqlx)?,
            updated_at: row.try_get("updated_at").map_err(map_sqlx)?,
        },
        blob_backend: row.try_get("blob_backend").map_err(map_sqlx)?,
        blob_ref: row.try_get("blob_ref").map_err(map_sqlx)?,
    })
}

pub fn decode_upload(row: &PgRow) -> DomainResult<UploadRecord> {
    let state: String = row.try_get("state").map_err(map_sqlx)?;
    let mode: String = row.try_get("mode").map_err(map_sqlx)?;
    let expected_size: i64 = row.try_get("expected_size").map_err(map_sqlx)?;

    Ok(UploadRecord {
        id: UploadId::new(row.try_get::<String, _>("id").map_err(map_sqlx)?),
        object_id: ObjectId::new(row.try_get::<String, _>("object_id").map_err(map_sqlx)?),
        state: UploadState::parse(&state)
            .ok_or_else(|| DomainError::internal(format!("unknown upload state {state:?}")))?,
        mode: UploadMode::parse(&mode)
            .ok_or_else(|| DomainError::internal(format!("unknown upload mode {mode:?}")))?,
        blob_backend: row.try_get("blob_backend").map_err(map_sqlx)?,
        blob_ref: row.try_get("blob_ref").map_err(map_sqlx)?,
        provider_upload_id: row.try_get("provider_upload_id").map_err(map_sqlx)?,
        expected_size: expected_size as u64,
        content_type: row.try_get("content_type").map_err(map_sqlx)?,
        expected_sha256: row.try_get("expected_sha256").map_err(map_sqlx)?,
        provider_completed: row.try_get("provider_completed").map_err(map_sqlx)?,
        created_at: row.try_get("created_at").map_err(map_sqlx)?,
        expires_at: row.try_get("expires_at").map_err(map_sqlx)?,
        completed_at: row.try_get("completed_at").map_err(map_sqlx)?,
        aborted_at: row.try_get("aborted_at").map_err(map_sqlx)?,
    })
}

/// Renders `path` for each id by walking parents to the root.
///
/// `path` is derived state: it is never stored on descendants, so renaming or
/// moving a folder requires no descendant rewrite.
const PATH_QUERY: &str = "
WITH RECURSIVE chain(root_id, cur_id, parent_id, name, depth) AS (
        SELECT o.id, o.id, o.parent_id, o.name, 0
        FROM objects o
        WHERE o.id = ANY($1)
    UNION ALL
        SELECT c.root_id, p.id, p.parent_id, p.name, c.depth + 1
        FROM chain c
        JOIN objects p ON p.id = c.parent_id
)
SELECT root_id,
       '/' || COALESCE(
           array_to_string(
               array_agg(name ORDER BY depth DESC) FILTER (WHERE name <> ''),
               '/'),
           '') AS path
FROM chain
GROUP BY root_id
";

pub async fn fetch_paths(
    conn: &mut PgConnection,
    ids: &[String],
) -> DomainResult<HashMap<String, String>> {
    if ids.is_empty() {
        return Ok(HashMap::new());
    }
    let rows = sqlx::query(PATH_QUERY)
        .bind(ids)
        .fetch_all(&mut *conn)
        .await
        .map_err(map_sqlx)?;

    let mut map = HashMap::with_capacity(rows.len());
    for row in rows {
        let id: String = row.try_get("root_id").map_err(map_sqlx)?;
        let path: String = row.try_get("path").map_err(map_sqlx)?;
        map.insert(id, path);
    }
    Ok(map)
}

/// Attaches rendered paths to decoded object rows, preserving order.
pub async fn attach_paths(
    conn: &mut PgConnection,
    rows: Vec<ObjectRow>,
) -> DomainResult<Vec<ObjectView>> {
    let ids: Vec<String> = rows.iter().map(|r| r.object.id.to_string()).collect();
    let paths = fetch_paths(conn, &ids).await?;
    rows.into_iter()
        .map(|r| {
            let path = paths
                .get(r.object.id.as_str())
                .cloned()
                .ok_or_else(|| DomainError::internal("failed to render object path"))?;
            Ok(ObjectView {
                object: r.object,
                path,
            })
        })
        .collect()
}
