//! Changes feed with stable, materialised cursors.
//!
//! A cursor is created unmaterialised. On its first read it atomically binds
//! the page limit and snapshots the current maximum sequence, so re-reading it
//! later returns exactly the same page even after new Changes are appended.

use anystore_domain::change::{ChangeAction, ChangeRecord};
use anystore_domain::error::{DomainError, DomainResult};
use anystore_domain::ids::CursorId;
use anystore_domain::object::Revision;
use anystore_domain::{ChangeId, IdempotencyKey, ObjectId, RequestId};
use anystore_metastore::changes::{ChangePage, ChangeStore, ReadChanges};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{AssertSqlSafe, PgConnection, Row, postgres::PgRow};

use crate::PostgresMetaStore;
use crate::errors::map_sqlx;

const CHANGE_COLUMNS: &str = "seq, change_id, object_id, revision, action, changed_at, request_id, idempotency_key, tombstone";

fn decode_change(row: &PgRow) -> DomainResult<(i64, ChangeRecord)> {
    let seq: i64 = row.try_get("seq").map_err(map_sqlx)?;
    let action: String = row.try_get("action").map_err(map_sqlx)?;
    let revision: i64 = row.try_get("revision").map_err(map_sqlx)?;
    let key: Option<String> = row.try_get("idempotency_key").map_err(map_sqlx)?;

    Ok((
        seq,
        ChangeRecord {
            change_id: ChangeId::new(row.try_get::<String, _>("change_id").map_err(map_sqlx)?),
            object_id: ObjectId::new(row.try_get::<String, _>("object_id").map_err(map_sqlx)?),
            revision: Revision(revision as u64),
            action: ChangeAction::parse(&action).ok_or_else(|| {
                DomainError::internal(format!("unknown change action {action:?}"))
            })?,
            changed_at: row.try_get("changed_at").map_err(map_sqlx)?,
            request_id: RequestId::new(row.try_get::<String, _>("request_id").map_err(map_sqlx)?),
            idempotency_key: key.map(IdempotencyKey::new),
            tombstone: row.try_get("tombstone").map_err(map_sqlx)?,
        },
    ))
}

struct BuiltPage {
    items: Vec<ChangeRecord>,
    page_end_seq: i64,
    snapshot_max_seq: i64,
    has_more: bool,
    lag: u64,
}

async fn count_changes_after(conn: &mut PgConnection, after_seq: i64) -> DomainResult<u64> {
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM changes WHERE seq > $1")
        .bind(after_seq)
        .fetch_one(&mut *conn)
        .await
        .map_err(map_sqlx)?;
    Ok(count as u64)
}

/// Computes the page bounded by `(after_seq, snapshot_max_seq]`.
async fn build_page(
    conn: &mut PgConnection,
    after_seq: i64,
    limit: u32,
) -> DomainResult<BuiltPage> {
    let snapshot_max_seq: i64 = sqlx::query_scalar("SELECT COALESCE(MAX(seq), $1) FROM changes")
        .bind(after_seq)
        .fetch_one(&mut *conn)
        .await
        .map_err(map_sqlx)?;
    let snapshot_max_seq = snapshot_max_seq.max(after_seq);

    let sql = format!(
        "SELECT {CHANGE_COLUMNS} FROM changes
         WHERE seq > $1 AND seq <= $2
         ORDER BY seq ASC
         LIMIT $3"
    );
    let rows = sqlx::query(AssertSqlSafe(sql))
        .bind(after_seq)
        .bind(snapshot_max_seq)
        .bind(i64::from(limit))
        .fetch_all(&mut *conn)
        .await
        .map_err(map_sqlx)?;

    let decoded = rows
        .iter()
        .map(decode_change)
        .collect::<DomainResult<Vec<(i64, ChangeRecord)>>>()?;

    let page_end_seq = decoded.last().map(|(seq, _)| *seq).unwrap_or(after_seq);
    let has_more: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM changes WHERE seq > $1 AND seq <= $2)")
            .bind(page_end_seq)
            .bind(snapshot_max_seq)
            .fetch_one(&mut *conn)
            .await
            .map_err(map_sqlx)?;
    let lag = count_changes_after(conn, page_end_seq).await?;

    Ok(BuiltPage {
        items: decoded.into_iter().map(|(_, record)| record).collect(),
        page_end_seq,
        snapshot_max_seq,
        has_more,
        lag,
    })
}

/// Creates the unmaterialised continuation cursor returned to the client.
async fn create_cursor(
    conn: &mut PgConnection,
    after_seq: i64,
    now: DateTime<Utc>,
    expires_at: DateTime<Utc>,
) -> DomainResult<CursorId> {
    let id = CursorId::generate();
    sqlx::query(
        "INSERT INTO change_cursors (cursor_id, after_seq, created_at, expires_at)
         VALUES ($1, $2, $3, $4)",
    )
    .bind(id.as_str())
    .bind(after_seq)
    .bind(now)
    .bind(expires_at)
    .execute(&mut *conn)
    .await
    .map_err(map_sqlx)?;
    Ok(id)
}

async fn purged_through(conn: &mut PgConnection) -> DomainResult<i64> {
    sqlx::query_scalar("SELECT purged_through_seq FROM changes_retention WHERE singleton")
        .fetch_one(&mut *conn)
        .await
        .map_err(map_sqlx)
}

#[async_trait]
impl ChangeStore for PostgresMetaStore {
    async fn read_changes(&self, req: ReadChanges) -> DomainResult<ChangePage> {
        let mut tx = self.pool().begin().await.map_err(map_sqlx)?;
        let watermark = purged_through(&mut tx).await?;

        let page = match &req.cursor {
            // An initial read starts from the oldest retained Change.
            None => {
                let built = build_page(&mut tx, watermark, req.limit).await?;
                let next =
                    create_cursor(&mut tx, built.page_end_seq, req.now, req.cursor_expires_at)
                        .await?;
                ChangePage {
                    items: built.items,
                    next_cursor: next,
                    has_more: built.has_more,
                    lag: built.lag,
                }
            }
            Some(cursor) => {
                if !cursor.is_well_formed() {
                    return Err(DomainError::InvalidRequest("Invalid cursor.".into()));
                }

                let row = sqlx::query(
                    "SELECT after_seq, materialized, page_limit, page_end_seq,
                            has_more, next_cursor_id, expires_at
                     FROM change_cursors WHERE cursor_id = $1 FOR UPDATE",
                )
                .bind(cursor.as_str())
                .fetch_optional(&mut *tx)
                .await
                .map_err(map_sqlx)?;

                // An unknown cursor is indistinguishable from one whose record
                // has already been purged, so both are reported as expired.
                let row = row.ok_or(DomainError::ChangesCursorExpired)?;

                let expires_at: DateTime<Utc> = row.try_get("expires_at").map_err(map_sqlx)?;
                let after_seq: i64 = row.try_get("after_seq").map_err(map_sqlx)?;
                if expires_at <= req.now || after_seq < watermark {
                    return Err(DomainError::ChangesCursorExpired);
                }

                let materialized: bool = row.try_get("materialized").map_err(map_sqlx)?;
                if materialized {
                    let page_limit: i32 = row.try_get("page_limit").map_err(map_sqlx)?;
                    if page_limit != req.limit as i32 {
                        return Err(DomainError::InvalidRequest(
                            "This cursor was already read with a different limit.".into(),
                        ));
                    }
                    let page_end_seq: i64 = row.try_get("page_end_seq").map_err(map_sqlx)?;
                    let has_more: bool = row.try_get("has_more").map_err(map_sqlx)?;
                    let next_cursor_id: String = row.try_get("next_cursor_id").map_err(map_sqlx)?;

                    let sql = format!(
                        "SELECT {CHANGE_COLUMNS} FROM changes
                         WHERE seq > $1 AND seq <= $2
                         ORDER BY seq ASC"
                    );
                    let rows = sqlx::query(AssertSqlSafe(sql))
                        .bind(after_seq)
                        .bind(page_end_seq)
                        .fetch_all(&mut *tx)
                        .await
                        .map_err(map_sqlx)?;

                    let items = rows
                        .iter()
                        .map(|r| decode_change(r).map(|(_, record)| record))
                        .collect::<DomainResult<Vec<ChangeRecord>>>()?;
                    let lag = count_changes_after(&mut tx, page_end_seq).await?;

                    ChangePage {
                        items,
                        next_cursor: CursorId::new(next_cursor_id),
                        has_more,
                        lag,
                    }
                } else {
                    let built = build_page(&mut tx, after_seq, req.limit).await?;
                    let next =
                        create_cursor(&mut tx, built.page_end_seq, req.now, req.cursor_expires_at)
                            .await?;

                    sqlx::query(
                        "UPDATE change_cursors
                         SET materialized = TRUE,
                             page_limit = $2,
                             snapshot_max_seq = $3,
                             page_end_seq = $4,
                             has_more = $5,
                             next_cursor_id = $6
                         WHERE cursor_id = $1",
                    )
                    .bind(cursor.as_str())
                    .bind(req.limit as i32)
                    .bind(built.snapshot_max_seq)
                    .bind(built.page_end_seq)
                    .bind(built.has_more)
                    .bind(next.as_str())
                    .execute(&mut *tx)
                    .await
                    .map_err(map_sqlx)?;

                    ChangePage {
                        items: built.items,
                        next_cursor: next,
                        has_more: built.has_more,
                        lag: built.lag,
                    }
                }
            }
        };

        tx.commit().await.map_err(map_sqlx)?;
        Ok(page)
    }
}
