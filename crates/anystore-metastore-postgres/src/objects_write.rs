//! Transactional object mutations.
//!
//! Each method below performs the revision check, the object mutation, the
//! Change append, the GC enqueue and the idempotency finalisation in exactly
//! one transaction.

use anystore_domain::change::{ChangeAction, ChangeRecord, PendingChange};
use anystore_domain::error::{DomainError, DomainResult};
use anystore_domain::metadata::{apply_metadata_patch, metadata_equal};
use anystore_domain::object::{ContentState, ObjectKind, Revision};
use anystore_domain::{ChangeId, IdempotencyKey, ObjectId, RequestId};
use anystore_metastore::ObjectMutationStore;
use anystore_metastore::commands::{
    CommitContent, CreateObjectCommit, DeleteObjectCommit, MutationContext, MutationOutcome,
    PatchObjectCommit,
};
use anystore_metastore::response::StoredResponse;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{AssertSqlSafe, PgConnection, Row};
use std::collections::HashMap;

use crate::PostgresMetaStore;
use crate::errors::{is_name_conflict, map_sqlx};
use crate::idempotency::finalize_in_tx;
use crate::objects_read::{live_kind, load_view};
use crate::rows::{OBJECT_COLUMNS, decode_object};

/// Bounded retries for the lock-then-verify loop used by recursive delete.
const DESCENDANT_LOCK_ATTEMPTS: usize = 5;

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

struct LockedObject {
    row: crate::rows::ObjectRow,
    deleted: bool,
}

async fn lock_object(conn: &mut PgConnection, id: &ObjectId) -> DomainResult<Option<LockedObject>> {
    let sql =
        format!("SELECT {OBJECT_COLUMNS}, deleted_at FROM objects WHERE id = $1 FOR UPDATE");
    let row = sqlx::query(AssertSqlSafe(sql))
        .bind(id.as_str())
        .fetch_optional(&mut *conn)
        .await
        .map_err(map_sqlx)?;

    let Some(row) = row else {
        return Ok(None);
    };
    let deleted: Option<DateTime<Utc>> = row.try_get("deleted_at").map_err(map_sqlx)?;
    Ok(Some(LockedObject {
        row: decode_object(&row)?,
        deleted: deleted.is_some(),
    }))
}

/// Enforces `If-Match`, reporting the current revision on mismatch.
fn check_if_match(current: Revision, if_match: Option<Revision>) -> DomainResult<()> {
    match if_match {
        Some(expected) if expected != current => Err(DomainError::RevisionConflict {
            current_revision: current.get(),
        }),
        _ => Ok(()),
    }
}

const INSERT_CHANGES: &str = "
INSERT INTO changes (
    seq, change_id, object_id, revision, action,
    changed_at, request_id, idempotency_key, tombstone)
SELECT * FROM UNNEST(
    $1::bigint[], $2::text[], $3::text[], $4::bigint[], $5::text[],
    $6::timestamptz[], $7::text[], $8::text[], $9::boolean[])
";

/// Appends Change records, reserving the global sequence first so `change_id`
/// stays monotonic in Change order.
async fn append_changes(
    conn: &mut PgConnection,
    pending: &[PendingChange],
    request_id: &RequestId,
    idempotency_key: Option<&IdempotencyKey>,
    changed_at: DateTime<Utc>,
) -> DomainResult<Vec<ChangeRecord>> {
    if pending.is_empty() {
        return Ok(Vec::new());
    }

    let seqs: Vec<i64> =
        sqlx::query_scalar("SELECT nextval('changes_seq') FROM generate_series(1, $1)")
            .bind(pending.len() as i32)
            .fetch_all(&mut *conn)
            .await
            .map_err(map_sqlx)?;

    let records: Vec<ChangeRecord> = pending
        .iter()
        .zip(&seqs)
        .map(|(p, seq)| ChangeRecord {
            change_id: ChangeId::from_seq(*seq),
            object_id: p.object_id.clone(),
            revision: p.revision,
            action: p.action,
            changed_at,
            request_id: request_id.clone(),
            idempotency_key: idempotency_key.cloned(),
            tombstone: p.tombstone,
        })
        .collect();

    let change_ids: Vec<String> = records.iter().map(|r| r.change_id.to_string()).collect();
    let object_ids: Vec<String> = records.iter().map(|r| r.object_id.to_string()).collect();
    let revisions: Vec<i64> = records.iter().map(|r| r.revision.get() as i64).collect();
    let actions: Vec<String> = records.iter().map(|r| r.action.as_str().to_owned()).collect();
    let changed_ats: Vec<DateTime<Utc>> = records.iter().map(|r| r.changed_at).collect();
    let request_ids: Vec<String> = records.iter().map(|r| r.request_id.to_string()).collect();
    let keys: Vec<Option<String>> = records
        .iter()
        .map(|r| r.idempotency_key.as_ref().map(|k| k.to_string()))
        .collect();
    let tombstones: Vec<bool> = records.iter().map(|r| r.tombstone).collect();

    sqlx::query(INSERT_CHANGES)
        .bind(&seqs)
        .bind(&change_ids)
        .bind(&object_ids)
        .bind(&revisions)
        .bind(&actions)
        .bind(&changed_ats)
        .bind(&request_ids)
        .bind(&keys)
        .bind(&tombstones)
        .execute(&mut *conn)
        .await
        .map_err(map_sqlx)?;

    Ok(records)
}

/// Enqueues a blob for deletion. Enqueued only after the pointer change is part
/// of the committing transaction, and never on the critical path of a read.
async fn enqueue_blob_gc(
    conn: &mut PgConnection,
    backend: &str,
    blob_ref: &str,
    not_before: DateTime<Utc>,
) -> DomainResult<()> {
    sqlx::query(
        "INSERT INTO blob_gc_queue (blob_backend, blob_ref, not_before)
         VALUES ($1, $2, $3)
         ON CONFLICT (blob_backend, blob_ref) DO NOTHING",
    )
    .bind(backend)
    .bind(blob_ref)
    .bind(not_before)
    .execute(&mut *conn)
    .await
    .map_err(map_sqlx)?;
    Ok(())
}

/// Renders the response and finalises idempotency inside the transaction.
async fn finish(
    conn: &mut PgConnection,
    ctx: &MutationContext,
    outcome: MutationOutcome,
) -> DomainResult<StoredResponse> {
    let response = (ctx.render)(&outcome)?;
    finalize_in_tx(
        conn,
        ctx.idempotency.as_ref(),
        &response,
        ctx.now,
        ctx.idempotency_expires_at,
    )
    .await?;
    Ok(response)
}

// ---------------------------------------------------------------------------
// ObjectMutationStore
// ---------------------------------------------------------------------------

#[async_trait]
impl ObjectMutationStore for PostgresMetaStore {
    async fn create_object(&self, cmd: CreateObjectCommit) -> DomainResult<StoredResponse> {
        let mut tx = self.pool().begin().await.map_err(map_sqlx)?;

        // FOR SHARE keeps the parent alive for the duration of the insert, so a
        // concurrent delete can never leave a live orphan behind.
        match live_kind(&mut tx, &cmd.parent_id, true).await? {
            None => return Err(DomainError::ObjectNotFound),
            Some(ObjectKind::File) => return Err(DomainError::NotAFolder),
            Some(ObjectKind::Folder) => {}
        }

        let content_state = match cmd.kind {
            ObjectKind::File => Some(ContentState::None.as_str()),
            ObjectKind::Folder => None,
        };

        let insert = sqlx::query(
            "INSERT INTO objects (
                 id, revision, kind, name, parent_id, content_type,
                 content_state, metadata, created_at, updated_at)
             VALUES ($1, 1, $2, $3, $4, $5, $6, $7, $8, $8)",
        )
        .bind(cmd.id.as_str())
        .bind(cmd.kind.as_str())
        .bind(&cmd.name)
        .bind(cmd.parent_id.as_str())
        .bind(cmd.content_type.as_deref())
        .bind(content_state)
        .bind(&cmd.metadata)
        .bind(cmd.ctx.now)
        .execute(&mut *tx)
        .await;

        if let Err(e) = insert {
            if is_name_conflict(&e) {
                return Err(DomainError::NameConflict);
            }
            return Err(map_sqlx(e));
        }

        let view = load_view(&mut tx, &cmd.id)
            .await?
            .ok_or_else(|| DomainError::internal("created object disappeared"))?;

        let changes = append_changes(
            &mut tx,
            &[PendingChange::new(
                cmd.id.clone(),
                Revision::INITIAL,
                ChangeAction::Created,
            )],
            &cmd.ctx.request_id,
            cmd.ctx.idempotency_key.as_ref(),
            cmd.ctx.now,
        )
        .await?;

        let response = finish(
            &mut tx,
            &cmd.ctx,
            MutationOutcome {
                object: Some(view),
                changes,
                no_op: false,
            },
        )
        .await?;

        tx.commit().await.map_err(map_sqlx)?;
        Ok(response)
    }

    async fn patch_object(&self, cmd: PatchObjectCommit) -> DomainResult<StoredResponse> {
        let mut tx = self.pool().begin().await.map_err(map_sqlx)?;

        let locked = lock_object(&mut tx, &cmd.id)
            .await?
            .ok_or(DomainError::ObjectNotFound)?;
        if locked.deleted {
            return Err(DomainError::ObjectNotFound);
        }
        let current = locked.row.object;
        check_if_match(current.revision, cmd.if_match)?;

        if current.id.is_root() {
            if cmd.new_parent_id.is_some() {
                return Err(DomainError::InvalidMove("Root cannot be moved.".into()));
            }
            if cmd.new_name.is_some() {
                return Err(DomainError::InvalidRequest(
                    "Root cannot be renamed.".into(),
                ));
            }
        }

        let target_name = cmd.new_name.clone().unwrap_or_else(|| current.name.clone());
        let target_parent = cmd
            .new_parent_id
            .clone()
            .or_else(|| current.parent_id.clone());
        let target_metadata = match &cmd.metadata_patch {
            Some(patch) => apply_metadata_patch(&current.metadata, patch)?,
            None => current.metadata.clone(),
        };

        let name_changed = target_name != current.name;
        let parent_changed = target_parent != current.parent_id;
        let metadata_changed = !metadata_equal(&target_metadata, &current.metadata);

        // A no-op must not increment the revision or emit a Change.
        if !name_changed && !parent_changed && !metadata_changed {
            let view = load_view(&mut tx, &cmd.id)
                .await?
                .ok_or(DomainError::ObjectNotFound)?;
            let response = finish(
                &mut tx,
                &cmd.ctx,
                MutationOutcome {
                    object: Some(view),
                    changes: Vec::new(),
                    no_op: true,
                },
            )
            .await?;
            tx.commit().await.map_err(map_sqlx)?;
            return Ok(response);
        }

        if parent_changed {
            let destination = target_parent
                .clone()
                .ok_or_else(|| DomainError::InvalidMove("Destination is required.".into()))?;
            match live_kind(&mut tx, &destination, true).await? {
                None => return Err(DomainError::ObjectNotFound),
                Some(ObjectKind::File) => return Err(DomainError::NotAFolder),
                Some(ObjectKind::Folder) => {}
            }
            if current.is_folder() && is_self_or_descendant(&mut tx, &destination, &current.id).await?
            {
                return Err(DomainError::InvalidMove(
                    "A folder cannot be moved into itself or one of its descendants.".into(),
                ));
            }
        }

        let update = sqlx::query(
            "UPDATE objects
             SET revision = revision + 1,
                 name = $2,
                 parent_id = $3,
                 metadata = $4,
                 updated_at = $5
             WHERE id = $1 AND deleted_at IS NULL
             RETURNING revision",
        )
        .bind(cmd.id.as_str())
        .bind(&target_name)
        .bind(target_parent.as_ref().map(|p| p.to_string()))
        .bind(&target_metadata)
        .bind(cmd.ctx.now)
        .fetch_optional(&mut *tx)
        .await;

        let row = match update {
            Ok(Some(row)) => row,
            Ok(None) => return Err(DomainError::ObjectNotFound),
            Err(e) if is_name_conflict(&e) => return Err(DomainError::NameConflict),
            Err(e) => return Err(map_sqlx(e)),
        };
        let new_revision = Revision(row.try_get::<i64, _>("revision").map_err(map_sqlx)? as u64);

        // One PATCH increments the revision once but may emit several Changes,
        // in a deterministic order.
        let mut pending = Vec::new();
        if name_changed {
            pending.push(PendingChange::new(
                cmd.id.clone(),
                new_revision,
                ChangeAction::Renamed,
            ));
        }
        if parent_changed {
            pending.push(PendingChange::new(
                cmd.id.clone(),
                new_revision,
                ChangeAction::Moved,
            ));
        }
        if metadata_changed {
            pending.push(PendingChange::new(
                cmd.id.clone(),
                new_revision,
                ChangeAction::MetadataUpdated,
            ));
        }
        pending.sort_by_key(|p| p.action.emission_order());

        let changes = append_changes(
            &mut tx,
            &pending,
            &cmd.ctx.request_id,
            cmd.ctx.idempotency_key.as_ref(),
            cmd.ctx.now,
        )
        .await?;

        let view = load_view(&mut tx, &cmd.id)
            .await?
            .ok_or(DomainError::ObjectNotFound)?;

        let response = finish(
            &mut tx,
            &cmd.ctx,
            MutationOutcome {
                object: Some(view),
                changes,
                no_op: false,
            },
        )
        .await?;

        tx.commit().await.map_err(map_sqlx)?;
        Ok(response)
    }

    async fn delete_object(&self, cmd: DeleteObjectCommit) -> DomainResult<StoredResponse> {
        let mut tx = self.pool().begin().await.map_err(map_sqlx)?;

        let locked = lock_object(&mut tx, &cmd.id)
            .await?
            .ok_or(DomainError::ObjectNotFound)?;
        if locked.deleted {
            return Err(DomainError::ObjectNotFound);
        }
        let current = locked.row.object;
        check_if_match(current.revision, cmd.if_match)?;

        if current.id.is_root() {
            return Err(DomainError::InvalidRequest(
                "Root cannot be deleted.".into(),
            ));
        }

        let targets = if current.is_folder() && cmd.recursive {
            collect_and_lock_descendants(&mut tx, &cmd.id).await?
        } else {
            if current.is_folder() && has_live_children(&mut tx, &cmd.id).await? {
                return Err(DomainError::FolderNotEmpty);
            }
            vec![cmd.id.to_string()]
        };

        let rows = sqlx::query(
            "UPDATE objects
             SET revision = revision + 1, deleted_at = $2, updated_at = $2
             WHERE id = ANY($1) AND deleted_at IS NULL
             RETURNING id, revision, blob_backend, blob_ref",
        )
        .bind(&targets)
        .bind(cmd.ctx.now)
        .fetch_all(&mut *tx)
        .await
        .map_err(map_sqlx)?;

        let mut revisions: HashMap<String, i64> = HashMap::with_capacity(rows.len());
        for row in &rows {
            let id: String = row.try_get("id").map_err(map_sqlx)?;
            let revision: i64 = row.try_get("revision").map_err(map_sqlx)?;
            revisions.insert(id, revision);

            let backend: Option<String> = row.try_get("blob_backend").map_err(map_sqlx)?;
            let blob_ref: Option<String> = row.try_get("blob_ref").map_err(map_sqlx)?;
            if let (Some(backend), Some(blob_ref)) = (backend, blob_ref) {
                enqueue_blob_gc(&mut tx, &backend, &blob_ref, cmd.ctx.now).await?;
            }
        }

        // One tombstone per deleted object, in the deterministic order the
        // descendant walk produced.
        let pending: Vec<PendingChange> = targets
            .iter()
            .filter_map(|id| {
                revisions.get(id).map(|revision| {
                    PendingChange::new(
                        ObjectId::new(id.clone()),
                        Revision(*revision as u64),
                        ChangeAction::Deleted,
                    )
                })
            })
            .collect();

        let changes = append_changes(
            &mut tx,
            &pending,
            &cmd.ctx.request_id,
            cmd.ctx.idempotency_key.as_ref(),
            cmd.ctx.now,
        )
        .await?;

        let response = finish(
            &mut tx,
            &cmd.ctx,
            MutationOutcome {
                object: None,
                changes,
                no_op: false,
            },
        )
        .await?;

        tx.commit().await.map_err(map_sqlx)?;
        Ok(response)
    }

    async fn commit_content(&self, cmd: CommitContent) -> DomainResult<StoredResponse> {
        let mut tx = self.pool().begin().await.map_err(map_sqlx)?;

        let locked = lock_object(&mut tx, &cmd.object_id)
            .await?
            .ok_or(DomainError::ObjectNotFound)?;
        if locked.deleted {
            return Err(DomainError::ObjectNotFound);
        }
        let current = locked.row.object;
        if !current.is_file() {
            return Err(DomainError::NotAFile);
        }
        check_if_match(current.revision, cmd.if_match)?;

        let previous_state = current.content_state.unwrap_or(ContentState::None);
        let action = match previous_state {
            ContentState::None => ChangeAction::ContentReady,
            ContentState::Ready => ChangeAction::ContentReplaced,
        };

        let row = sqlx::query(
            "UPDATE objects
             SET revision = revision + 1,
                 blob_backend = $2,
                 blob_ref = $3,
                 size_bytes = $4,
                 content_type = $5,
                 sha256 = $6,
                 content_state = 'ready',
                 updated_at = $7
             WHERE id = $1 AND deleted_at IS NULL
             RETURNING revision",
        )
        .bind(cmd.object_id.as_str())
        .bind(&cmd.blob_backend)
        .bind(&cmd.blob_ref)
        .bind(cmd.size as i64)
        .bind(&cmd.content_type)
        .bind(cmd.sha256.as_deref())
        .bind(cmd.ctx.now)
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlx)?
        .ok_or(DomainError::ObjectNotFound)?;

        let new_revision = Revision(row.try_get::<i64, _>("revision").map_err(map_sqlx)? as u64);

        // The superseded blob is queued only now that the pointer swap is part
        // of the committing transaction.
        if let (Some(backend), Some(blob_ref)) = (locked.row.blob_backend, locked.row.blob_ref)
            && blob_ref != cmd.blob_ref
        {
            enqueue_blob_gc(&mut tx, &backend, &blob_ref, cmd.gc_not_before).await?;
        }

        sqlx::query(
            "UPDATE uploads
             SET state = 'completed', provider_completed = TRUE, completed_at = $2
             WHERE id = $1",
        )
        .bind(cmd.upload_id.as_str())
        .bind(cmd.ctx.now)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx)?;

        let changes = append_changes(
            &mut tx,
            &[PendingChange::new(cmd.object_id.clone(), new_revision, action)],
            &cmd.ctx.request_id,
            cmd.ctx.idempotency_key.as_ref(),
            cmd.ctx.now,
        )
        .await?;

        let view = load_view(&mut tx, &cmd.object_id)
            .await?
            .ok_or(DomainError::ObjectNotFound)?;

        let response = finish(
            &mut tx,
            &cmd.ctx,
            MutationOutcome {
                object: Some(view),
                changes,
                no_op: false,
            },
        )
        .await?;

        tx.commit().await.map_err(map_sqlx)?;
        Ok(response)
    }
}

// ---------------------------------------------------------------------------
// Tree helpers
// ---------------------------------------------------------------------------

/// True when `candidate` is `subject` itself or lies beneath it.
const ANCESTOR_QUERY: &str = "
WITH RECURSIVE up(id, parent_id) AS (
        SELECT id, parent_id FROM objects WHERE id = $1
    UNION ALL
        SELECT o.id, o.parent_id
        FROM objects o
        JOIN up ON o.id = up.parent_id
)
SELECT EXISTS(SELECT 1 FROM up WHERE id = $2)
";

async fn is_self_or_descendant(
    conn: &mut PgConnection,
    candidate: &ObjectId,
    subject: &ObjectId,
) -> DomainResult<bool> {
    sqlx::query_scalar(ANCESTOR_QUERY)
        .bind(candidate.as_str())
        .bind(subject.as_str())
        .fetch_one(&mut *conn)
        .await
        .map_err(map_sqlx)
}

const DESCENDANTS_QUERY: &str = "
WITH RECURSIVE down(id, depth) AS (
        SELECT id, 0 FROM objects WHERE id = $1 AND deleted_at IS NULL
    UNION ALL
        SELECT o.id, d.depth + 1
        FROM objects o
        JOIN down d ON o.parent_id = d.id
        WHERE o.deleted_at IS NULL
)
SELECT id FROM down ORDER BY depth DESC, id ASC
";

async fn has_live_children(conn: &mut PgConnection, id: &ObjectId) -> DomainResult<bool> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM objects WHERE parent_id = $1 AND deleted_at IS NULL)",
    )
    .bind(id.as_str())
    .fetch_one(&mut *conn)
    .await
    .map_err(map_sqlx)
}

/// Collects the subtree and locks every member.
///
/// Recursive CTEs cannot take row locks directly, so the set is locked and then
/// re-derived: if a concurrent insert added a child in between, the walk is
/// repeated. This prevents a live object surviving under a deleted parent.
async fn collect_and_lock_descendants(
    conn: &mut PgConnection,
    root: &ObjectId,
) -> DomainResult<Vec<String>> {
    let mut previous: Vec<String> = Vec::new();

    for _ in 0..DESCENDANT_LOCK_ATTEMPTS {
        let ids: Vec<String> = sqlx::query_scalar(DESCENDANTS_QUERY)
            .bind(root.as_str())
            .fetch_all(&mut *conn)
            .await
            .map_err(map_sqlx)?;

        if ids == previous {
            return Ok(ids);
        }

        sqlx::query("SELECT id FROM objects WHERE id = ANY($1) FOR UPDATE")
            .bind(&ids)
            .execute(&mut *conn)
            .await
            .map_err(map_sqlx)?;

        previous = ids;
    }

    Err(DomainError::internal(
        "subtree kept changing while acquiring delete locks",
    ))
}
