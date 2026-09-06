//! In-memory `MetaStore`.
//!
//! Backed by a single lock, so "one transaction" is structural. Behaviour is
//! deliberately identical to the PostgreSQL adapter; the shared conformance
//! suite is what keeps them in step.

use anystore_domain::change::{ChangeAction, ChangeRecord, PendingChange};
use anystore_domain::error::{DomainError, DomainResult};
use anystore_domain::ids::CursorId;
use anystore_domain::metadata::{apply_metadata_patch, metadata_equal};
use anystore_domain::object::{ContentState, Object, ObjectKind, ObjectView, Revision};
use anystore_domain::page::{KeysetCursor, decode_cursor, encode_cursor};
use anystore_domain::path::split_path;
use anystore_domain::upload::{UploadRecord, UploadState};
use anystore_domain::{ChangeId, IdempotencyKey, ObjectId, Page, PrincipalId, UploadId};
use anystore_metastore::changes::{ChangePage, ChangeStore, ReadChanges};
use anystore_metastore::commands::{
    CommitContent, CreateObjectCommit, DeleteObjectCommit, ListChildren, ListOrder,
    MetadataCondition, MutationContext, MutationOutcome, ObjectQuery, OrderBy, PatchObjectCommit,
};
use anystore_metastore::idempotency::{
    IdempotencyAcquire, IdempotencyComplete, IdempotencyDecision, IdempotencyRelease,
    IdempotencyStore,
};
use anystore_metastore::maintenance::{BlobGcEntry, MaintenanceStore};
use anystore_metastore::response::StoredResponse;
use anystore_metastore::uploads::{
    AbortUploadRecord, CreateUploadRecord, CreateUploadResult, UpdateUploadState, UploadRepository,
};
use anystore_metastore::{ContentPointer, ObjectMutationStore, ObjectRepository};
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

#[derive(Clone, Debug)]
struct StoredObject {
    object: Object,
    blob_backend: Option<String>,
    blob_ref: Option<String>,
    deleted_at: Option<DateTime<Utc>>,
}

impl StoredObject {
    fn is_live(&self) -> bool {
        self.deleted_at.is_none()
    }
}

#[derive(Clone, Debug)]
struct IdempotencyRow {
    request_hash: String,
    created_at: DateTime<Utc>,
    completed: bool,
    owner_token: Option<String>,
    lease_until: Option<DateTime<Utc>>,
    response: Option<StoredResponse>,
    expires_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
struct CursorRow {
    after_seq: i64,
    materialized: bool,
    page_limit: Option<u32>,
    page_end_seq: Option<i64>,
    has_more: Option<bool>,
    next_cursor_id: Option<CursorId>,
    expires_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
struct GcRow {
    blob_backend: String,
    blob_ref: String,
    not_before: DateTime<Utc>,
    attempts: i32,
}

#[derive(Default)]
struct State {
    objects: HashMap<String, StoredObject>,
    /// Index equals `seq - 1`.
    changes: Vec<ChangeRecord>,
    cursors: HashMap<String, CursorRow>,
    idempotency: HashMap<(String, String, String), IdempotencyRow>,
    uploads: HashMap<String, UploadRecord>,
    gc: Vec<GcRow>,
    purged_through_seq: i64,
}

pub struct InMemoryMetaStore {
    state: Mutex<State>,
}

impl Default for InMemoryMetaStore {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryMetaStore {
    pub fn new() -> Self {
        let mut objects = HashMap::new();
        let now = Utc::now();
        objects.insert(
            "root".to_owned(),
            StoredObject {
                object: Object {
                    id: ObjectId::root(),
                    revision: Revision::INITIAL,
                    kind: ObjectKind::Folder,
                    name: String::new(),
                    parent_id: None,
                    content_type: None,
                    size: None,
                    sha256: None,
                    content_state: None,
                    metadata: serde_json::json!({}),
                    created_at: now,
                    updated_at: now,
                },
                blob_backend: None,
                blob_ref: None,
                deleted_at: None,
            },
        );

        Self {
            state: Mutex::new(State {
                objects,
                ..State::default()
            }),
        }
    }
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn render_path(state: &State, id: &ObjectId) -> DomainResult<String> {
    let mut segments = Vec::new();
    let mut current = id.clone();
    // Bounded so a corrupt cycle cannot hang a caller.
    for _ in 0..1024 {
        let Some(stored) = state.objects.get(current.as_str()) else {
            return Err(DomainError::ObjectNotFound);
        };
        match &stored.object.parent_id {
            None => {
                segments.reverse();
                return Ok(if segments.is_empty() {
                    "/".to_owned()
                } else {
                    format!("/{}", segments.join("/"))
                });
            }
            Some(parent) => {
                segments.push(stored.object.name.clone());
                current = parent.clone();
            }
        }
    }
    Err(DomainError::internal("object tree is cyclic"))
}

fn view_of(state: &State, id: &ObjectId) -> DomainResult<ObjectView> {
    let stored = state
        .objects
        .get(id.as_str())
        .filter(|s| s.is_live())
        .ok_or(DomainError::ObjectNotFound)?;
    Ok(ObjectView {
        object: stored.object.clone(),
        path: render_path(state, id)?,
    })
}

fn name_taken(state: &State, parent: Option<&ObjectId>, name: &str, except: &str) -> bool {
    state.objects.values().any(|s| {
        s.is_live()
            && s.object.id.as_str() != except
            && s.object.parent_id.as_deref_id() == parent.map(ObjectId::as_str)
            && s.object.name == name
    })
}

/// Small helper so `Option<ObjectId>` can be compared with `Option<&str>`.
trait AsDerefId {
    fn as_deref_id(&self) -> Option<&str>;
}

impl AsDerefId for Option<ObjectId> {
    fn as_deref_id(&self) -> Option<&str> {
        self.as_ref().map(ObjectId::as_str)
    }
}

fn is_self_or_descendant(state: &State, candidate: &ObjectId, subject: &ObjectId) -> bool {
    let mut current = Some(candidate.clone());
    for _ in 0..1024 {
        let Some(id) = current else {
            return false;
        };
        if id.as_str() == subject.as_str() {
            return true;
        }
        current = state
            .objects
            .get(id.as_str())
            .and_then(|s| s.object.parent_id.clone());
    }
    false
}

fn descendants(state: &State, root: &ObjectId) -> Vec<(u32, String)> {
    let mut result = vec![(0u32, root.to_string())];
    let mut frontier = vec![(0u32, root.to_string())];

    while let Some((depth, id)) = frontier.pop() {
        for stored in state.objects.values() {
            if stored.is_live() && stored.object.parent_id.as_deref_id() == Some(id.as_str()) {
                let entry = (depth + 1, stored.object.id.to_string());
                result.push(entry.clone());
                frontier.push(entry);
            }
        }
    }

    // Children before parents, then by id, so tombstone order is deterministic.
    result.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
    result
}

fn append_changes(
    state: &mut State,
    pending: &[PendingChange],
    ctx: &MutationContext,
) -> Vec<ChangeRecord> {
    pending
        .iter()
        .map(|p| {
            let seq = state.changes.len() as i64 + 1;
            let record = ChangeRecord {
                change_id: ChangeId::from_seq(seq),
                object_id: p.object_id.clone(),
                revision: p.revision,
                action: p.action,
                changed_at: ctx.now,
                request_id: ctx.request_id.clone(),
                idempotency_key: ctx.idempotency_key.clone(),
                tombstone: p.tombstone,
            };
            state.changes.push(record.clone());
            record
        })
        .collect()
}

fn enqueue_gc(state: &mut State, backend: &str, blob_ref: &str, not_before: DateTime<Utc>) {
    if state
        .gc
        .iter()
        .any(|g| g.blob_backend == backend && g.blob_ref == blob_ref)
    {
        return;
    }
    state.gc.push(GcRow {
        blob_backend: backend.to_owned(),
        blob_ref: blob_ref.to_owned(),
        not_before,
        attempts: 0,
    });
}

fn finalize_idempotency(
    state: &mut State,
    ctx: &MutationContext,
    response: &StoredResponse,
) -> DomainResult<()> {
    let Some(ictx) = &ctx.idempotency else {
        return Ok(());
    };
    let key = (
        ictx.principal_id.to_string(),
        ictx.operation.clone(),
        ictx.key.to_string(),
    );
    let Some(row) = state.idempotency.get_mut(&key) else {
        return Err(DomainError::internal("idempotency record disappeared"));
    };
    if row.owner_token.as_deref() != Some(ictx.owner_token.as_str()) {
        return Err(DomainError::internal(
            "idempotency lease was lost during the mutation",
        ));
    }
    row.completed = true;
    row.response = Some(response.clone());
    row.owner_token = None;
    row.lease_until = None;
    row.expires_at = ctx.idempotency_expires_at;
    Ok(())
}

fn check_if_match(current: Revision, if_match: Option<Revision>) -> DomainResult<()> {
    match if_match {
        Some(expected) if expected != current => Err(DomainError::RevisionConflict {
            current_revision: current.get(),
        }),
        _ => Ok(()),
    }
}

fn sort_key(view: &ObjectView, order_by: Option<OrderBy>) -> Option<String> {
    match order_by? {
        OrderBy::Name => Some(view.object.name.clone()),
        OrderBy::CreatedAt => Some(view.object.created_at.to_rfc3339()),
        OrderBy::UpdatedAt => Some(view.object.updated_at.to_rfc3339()),
        OrderBy::Size => Some(view.object.size.map(|s| s as i64).unwrap_or(-1).to_string()),
    }
}

fn paginate(
    mut items: Vec<ObjectView>,
    limit: u32,
    cursor: Option<String>,
    order_by: Option<OrderBy>,
) -> DomainResult<Page<ObjectView>> {
    if let Some(cursor) = cursor {
        let cursor: KeysetCursor = decode_cursor(&cursor)?;
        let position = items
            .iter()
            .position(|item| item.object.id.as_str() == cursor.i);
        items = match position {
            Some(index) => items.split_off(index + 1),
            None => Vec::new(),
        };
    }

    let has_more = items.len() > limit as usize;
    items.truncate(limit as usize);

    let next_cursor = if has_more {
        items
            .last()
            .map(|last| {
                encode_cursor(&KeysetCursor {
                    k: sort_key(last, order_by),
                    i: last.object.id.to_string(),
                })
            })
            .transpose()?
    } else {
        None
    };

    Ok(Page::new(items, next_cursor, has_more))
}

// ---------------------------------------------------------------------------
// ObjectRepository
// ---------------------------------------------------------------------------

#[async_trait]
impl ObjectRepository for InMemoryMetaStore {
    async fn get_object(&self, id: &ObjectId) -> DomainResult<Option<ObjectView>> {
        let state = self.state.lock().unwrap();
        Ok(view_of(&state, id).ok())
    }

    async fn content_pointer(&self, id: &ObjectId) -> DomainResult<Option<ContentPointer>> {
        let state = self.state.lock().unwrap();
        Ok(state
            .objects
            .get(id.as_str())
            .filter(|s| s.is_live() && s.object.has_ready_content())
            .and_then(|s| match (&s.blob_backend, &s.blob_ref) {
                (Some(blob_backend), Some(blob_ref)) => Some(ContentPointer {
                    blob_backend: blob_backend.clone(),
                    blob_ref: blob_ref.clone(),
                }),
                _ => None,
            }))
    }

    async fn list_children(&self, q: ListChildren) -> DomainResult<Page<ObjectView>> {
        let state = self.state.lock().unwrap();
        let mut items: Vec<ObjectView> = state
            .objects
            .values()
            .filter(|s| {
                s.is_live() && s.object.parent_id.as_deref_id() == Some(q.parent_id.as_str())
            })
            .map(|s| {
                Ok(ObjectView {
                    object: s.object.clone(),
                    path: render_path(&state, &s.object.id)?,
                })
            })
            .collect::<DomainResult<Vec<_>>>()?;

        items.sort_by(|a, b| {
            let ordering = match q.order_by {
                OrderBy::Name => a.object.name.cmp(&b.object.name),
                OrderBy::CreatedAt => a.object.created_at.cmp(&b.object.created_at),
                OrderBy::UpdatedAt => a.object.updated_at.cmp(&b.object.updated_at),
                OrderBy::Size => a
                    .object
                    .size
                    .map(|s| s as i64)
                    .unwrap_or(-1)
                    .cmp(&b.object.size.map(|s| s as i64).unwrap_or(-1)),
            }
            .then(a.object.id.cmp(&b.object.id));

            match q.order {
                ListOrder::Asc => ordering,
                ListOrder::Desc => ordering.reverse(),
            }
        });

        paginate(items, q.limit, q.cursor, Some(q.order_by))
    }

    async fn resolve_path(&self, path: &str) -> DomainResult<Option<ObjectView>> {
        let segments = split_path(path)?;
        let state = self.state.lock().unwrap();

        let mut current = ObjectId::root();
        for segment in segments {
            let found = state.objects.values().find(|s| {
                s.is_live()
                    && s.object.parent_id.as_deref_id() == Some(current.as_str())
                    && s.object.name == segment
            });
            match found {
                Some(stored) => current = stored.object.id.clone(),
                None => return Ok(None),
            }
        }
        Ok(view_of(&state, &current).ok())
    }

    async fn query_objects(&self, q: ObjectQuery) -> DomainResult<Page<ObjectView>> {
        let state = self.state.lock().unwrap();
        let mut items: Vec<ObjectView> = state
            .objects
            .values()
            .filter(|s| s.is_live())
            .filter(|s| q.kind.is_none_or(|kind| s.object.kind == kind))
            .filter(|s| q.name.as_ref().is_none_or(|name| &s.object.name == name))
            .filter(|s| {
                q.parent_id
                    .as_ref()
                    .is_none_or(|parent| s.object.parent_id.as_ref() == Some(parent))
            })
            .filter(|s| {
                // All supplied conditions are ANDed, on top-level keys only.
                q.metadata.iter().all(|(key, condition)| {
                    let value = s.object.metadata.get(key);
                    match condition {
                        MetadataCondition::Eq(expected) => value == Some(expected),
                        MetadataCondition::Exists(expected) => value.is_some() == *expected,
                    }
                })
            })
            .map(|s| {
                Ok(ObjectView {
                    object: s.object.clone(),
                    path: render_path(&state, &s.object.id)?,
                })
            })
            .collect::<DomainResult<Vec<_>>>()?;

        items.sort_by(|a, b| a.object.id.cmp(&b.object.id));
        paginate(items, q.limit, q.cursor, None)
    }
}

// ---------------------------------------------------------------------------
// ObjectMutationStore
// ---------------------------------------------------------------------------

#[async_trait]
impl ObjectMutationStore for InMemoryMetaStore {
    async fn create_object(&self, cmd: CreateObjectCommit) -> DomainResult<StoredResponse> {
        let mut state = self.state.lock().unwrap();

        match state
            .objects
            .get(cmd.parent_id.as_str())
            .filter(|s| s.is_live())
        {
            None => return Err(DomainError::ObjectNotFound),
            Some(parent) if parent.object.kind != ObjectKind::Folder => {
                return Err(DomainError::NotAFolder);
            }
            Some(_) => {}
        }

        if name_taken(&state, Some(&cmd.parent_id), &cmd.name, cmd.id.as_str()) {
            return Err(DomainError::NameConflict);
        }

        state.objects.insert(
            cmd.id.to_string(),
            StoredObject {
                object: Object {
                    id: cmd.id.clone(),
                    revision: Revision::INITIAL,
                    kind: cmd.kind,
                    name: cmd.name.clone(),
                    parent_id: Some(cmd.parent_id.clone()),
                    content_type: cmd.content_type.clone(),
                    size: None,
                    sha256: None,
                    content_state: match cmd.kind {
                        ObjectKind::File => Some(ContentState::None),
                        ObjectKind::Folder => None,
                    },
                    metadata: cmd.metadata.clone(),
                    created_at: cmd.ctx.now,
                    updated_at: cmd.ctx.now,
                },
                blob_backend: None,
                blob_ref: None,
                deleted_at: None,
            },
        );

        let changes = append_changes(
            &mut state,
            &[PendingChange::new(
                cmd.id.clone(),
                Revision::INITIAL,
                ChangeAction::Created,
            )],
            &cmd.ctx,
        );

        let outcome = MutationOutcome {
            object: Some(view_of(&state, &cmd.id)?),
            changes,
            no_op: false,
        };
        let response = (cmd.ctx.render)(&outcome)?;
        finalize_idempotency(&mut state, &cmd.ctx, &response)?;
        Ok(response)
    }

    async fn patch_object(&self, cmd: PatchObjectCommit) -> DomainResult<StoredResponse> {
        let mut state = self.state.lock().unwrap();

        let stored = state
            .objects
            .get(cmd.id.as_str())
            .filter(|s| s.is_live())
            .ok_or(DomainError::ObjectNotFound)?
            .clone();
        check_if_match(stored.object.revision, cmd.if_match)?;

        if stored.object.id.is_root() {
            if cmd.new_parent_id.is_some() {
                return Err(DomainError::InvalidMove("Root cannot be moved.".into()));
            }
            if cmd.new_name.is_some() {
                return Err(DomainError::InvalidRequest(
                    "Root cannot be renamed.".into(),
                ));
            }
        }

        let target_name = cmd
            .new_name
            .clone()
            .unwrap_or_else(|| stored.object.name.clone());
        let target_parent = cmd
            .new_parent_id
            .clone()
            .or_else(|| stored.object.parent_id.clone());
        let target_metadata = match &cmd.metadata_patch {
            Some(patch) => apply_metadata_patch(&stored.object.metadata, patch)?,
            None => stored.object.metadata.clone(),
        };

        let name_changed = target_name != stored.object.name;
        let parent_changed = target_parent != stored.object.parent_id;
        let metadata_changed = !metadata_equal(&target_metadata, &stored.object.metadata);

        if !name_changed && !parent_changed && !metadata_changed {
            let outcome = MutationOutcome {
                object: Some(view_of(&state, &cmd.id)?),
                changes: Vec::new(),
                no_op: true,
            };
            let response = (cmd.ctx.render)(&outcome)?;
            finalize_idempotency(&mut state, &cmd.ctx, &response)?;
            return Ok(response);
        }

        if parent_changed {
            let destination = target_parent
                .clone()
                .ok_or_else(|| DomainError::InvalidMove("Destination is required.".into()))?;
            match state
                .objects
                .get(destination.as_str())
                .filter(|s| s.is_live())
            {
                None => return Err(DomainError::ObjectNotFound),
                Some(dest) if dest.object.kind != ObjectKind::Folder => {
                    return Err(DomainError::NotAFolder);
                }
                Some(_) => {}
            }
            if stored.object.is_folder()
                && is_self_or_descendant(&state, &destination, &stored.object.id)
            {
                return Err(DomainError::InvalidMove(
                    "A folder cannot be moved into itself or one of its descendants.".into(),
                ));
            }
        }

        if name_taken(
            &state,
            target_parent.as_ref(),
            &target_name,
            cmd.id.as_str(),
        ) {
            return Err(DomainError::NameConflict);
        }

        let new_revision = stored.object.revision.next();
        {
            let entry = state
                .objects
                .get_mut(cmd.id.as_str())
                .ok_or(DomainError::ObjectNotFound)?;
            entry.object.revision = new_revision;
            entry.object.name = target_name;
            entry.object.parent_id = target_parent;
            entry.object.metadata = target_metadata;
            entry.object.updated_at = cmd.ctx.now;
        }

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

        let changes = append_changes(&mut state, &pending, &cmd.ctx);
        let outcome = MutationOutcome {
            object: Some(view_of(&state, &cmd.id)?),
            changes,
            no_op: false,
        };
        let response = (cmd.ctx.render)(&outcome)?;
        finalize_idempotency(&mut state, &cmd.ctx, &response)?;
        Ok(response)
    }

    async fn delete_object(&self, cmd: DeleteObjectCommit) -> DomainResult<StoredResponse> {
        let mut state = self.state.lock().unwrap();

        let stored = state
            .objects
            .get(cmd.id.as_str())
            .filter(|s| s.is_live())
            .ok_or(DomainError::ObjectNotFound)?
            .clone();
        check_if_match(stored.object.revision, cmd.if_match)?;

        if stored.object.id.is_root() {
            return Err(DomainError::InvalidRequest(
                "Root cannot be deleted.".into(),
            ));
        }

        let targets: Vec<String> = if stored.object.is_folder() && cmd.recursive {
            descendants(&state, &cmd.id)
                .into_iter()
                .map(|(_, id)| id)
                .collect()
        } else {
            let has_children = state
                .objects
                .values()
                .any(|s| s.is_live() && s.object.parent_id.as_deref_id() == Some(cmd.id.as_str()));
            if stored.object.is_folder() && has_children {
                return Err(DomainError::FolderNotEmpty);
            }
            vec![cmd.id.to_string()]
        };

        let mut pending = Vec::new();
        let mut collectable = Vec::new();
        for id in &targets {
            let Some(entry) = state.objects.get_mut(id) else {
                continue;
            };
            if !entry.is_live() {
                continue;
            }
            entry.object.revision = entry.object.revision.next();
            entry.object.updated_at = cmd.ctx.now;
            entry.deleted_at = Some(cmd.ctx.now);

            pending.push(PendingChange::new(
                ObjectId::new(id.clone()),
                entry.object.revision,
                ChangeAction::Deleted,
            ));
            if let (Some(backend), Some(blob_ref)) = (&entry.blob_backend, &entry.blob_ref) {
                collectable.push((backend.clone(), blob_ref.clone()));
            }
        }

        for (backend, blob_ref) in collectable {
            enqueue_gc(&mut state, &backend, &blob_ref, cmd.ctx.now);
        }

        let changes = append_changes(&mut state, &pending, &cmd.ctx);
        let outcome = MutationOutcome {
            object: None,
            changes,
            no_op: false,
        };
        let response = (cmd.ctx.render)(&outcome)?;
        finalize_idempotency(&mut state, &cmd.ctx, &response)?;
        Ok(response)
    }

    async fn commit_content(&self, cmd: CommitContent) -> DomainResult<StoredResponse> {
        let mut state = self.state.lock().unwrap();

        let upload_state = state
            .uploads
            .get(cmd.upload_id.as_str())
            .ok_or(DomainError::UploadNotFound)?
            .state;
        if !matches!(
            upload_state,
            UploadState::Ready | UploadState::Completing | UploadState::Completed
        ) {
            return Err(DomainError::UploadNotFound);
        }

        let stored = state
            .objects
            .get(cmd.object_id.as_str())
            .filter(|s| s.is_live())
            .ok_or(DomainError::ObjectNotFound)?
            .clone();
        if !stored.object.is_file() {
            return Err(DomainError::NotAFile);
        }
        check_if_match(stored.object.revision, cmd.if_match)?;

        if upload_state == UploadState::Completed {
            let outcome = MutationOutcome {
                object: Some(view_of(&state, &cmd.object_id)?),
                changes: Vec::new(),
                no_op: true,
            };
            let response = (cmd.ctx.render)(&outcome)?;
            finalize_idempotency(&mut state, &cmd.ctx, &response)?;
            return Ok(response);
        }

        let action = match stored.object.content_state.unwrap_or(ContentState::None) {
            ContentState::None => ChangeAction::ContentReady,
            ContentState::Ready => ChangeAction::ContentReplaced,
        };

        let new_revision = stored.object.revision.next();
        {
            let entry = state
                .objects
                .get_mut(cmd.object_id.as_str())
                .ok_or(DomainError::ObjectNotFound)?;
            entry.object.revision = new_revision;
            entry.object.size = Some(cmd.size);
            entry.object.sha256 = cmd.sha256.clone();
            entry.object.content_type = Some(cmd.content_type.clone());
            entry.object.content_state = Some(ContentState::Ready);
            entry.object.updated_at = cmd.ctx.now;
            entry.blob_backend = Some(cmd.blob_backend.clone());
            entry.blob_ref = Some(cmd.blob_ref.clone());
        }

        if let (Some(backend), Some(previous)) = (&stored.blob_backend, &stored.blob_ref)
            && previous != &cmd.blob_ref
        {
            enqueue_gc(&mut state, backend, previous, cmd.gc_not_before);
        }

        if let Some(upload) = state.uploads.get_mut(cmd.upload_id.as_str()) {
            upload.state = UploadState::Completed;
            upload.provider_completed = true;
            upload.completed_at = Some(cmd.ctx.now);
        }
        state
            .gc
            .retain(|row| row.blob_backend != cmd.blob_backend || row.blob_ref != cmd.blob_ref);

        let changes = append_changes(
            &mut state,
            &[PendingChange::new(
                cmd.object_id.clone(),
                new_revision,
                action,
            )],
            &cmd.ctx,
        );
        let outcome = MutationOutcome {
            object: Some(view_of(&state, &cmd.object_id)?),
            changes,
            no_op: false,
        };
        let response = (cmd.ctx.render)(&outcome)?;
        finalize_idempotency(&mut state, &cmd.ctx, &response)?;
        Ok(response)
    }
}

// ---------------------------------------------------------------------------
// UploadRepository
// ---------------------------------------------------------------------------

#[async_trait]
impl UploadRepository for InMemoryMetaStore {
    async fn create_upload_record(
        &self,
        cmd: CreateUploadRecord,
    ) -> DomainResult<CreateUploadResult> {
        let mut state = self.state.lock().unwrap();
        // Resuming a crashed attempt must not create a second session.
        if let Some(existing) = state.uploads.get(cmd.id.as_str()) {
            return Ok(CreateUploadResult {
                upload: existing.clone(),
                created: false,
            });
        }

        let record = UploadRecord {
            id: cmd.id.clone(),
            object_id: cmd.object_id,
            state: UploadState::Initiating,
            mode: cmd.mode,
            blob_backend: cmd.blob_backend,
            blob_ref: cmd.blob_ref,
            provider_upload_id: None,
            expected_size: cmd.expected_size,
            content_type: cmd.content_type,
            expected_sha256: cmd.expected_sha256,
            provider_completed: false,
            created_at: cmd.created_at,
            expires_at: cmd.expires_at,
            completed_at: None,
            aborted_at: None,
        };
        state.uploads.insert(cmd.id.to_string(), record.clone());
        Ok(CreateUploadResult {
            upload: record,
            created: true,
        })
    }

    async fn get_upload(&self, id: &UploadId) -> DomainResult<Option<UploadRecord>> {
        Ok(self.state.lock().unwrap().uploads.get(id.as_str()).cloned())
    }

    async fn update_upload_state(&self, cmd: UpdateUploadState) -> DomainResult<()> {
        let mut state = self.state.lock().unwrap();
        let upload = state
            .uploads
            .get_mut(cmd.id.as_str())
            .ok_or(DomainError::UploadNotFound)?;
        let transition_allowed = match cmd.state {
            UploadState::Ready => {
                matches!(upload.state, UploadState::Initiating | UploadState::Ready)
            }
            UploadState::Completing => {
                matches!(upload.state, UploadState::Ready | UploadState::Completing)
            }
            _ => false,
        };
        if !transition_allowed {
            return Err(DomainError::UploadNotFound);
        }
        upload.state = cmd.state;
        if let Some(provider_upload_id) = cmd.provider_upload_id {
            upload.provider_upload_id = Some(provider_upload_id);
        }
        if let Some(provider_completed) = cmd.provider_completed {
            upload.provider_completed = provider_completed;
        }
        Ok(())
    }

    async fn abort_upload_record(&self, cmd: AbortUploadRecord) -> DomainResult<()> {
        let mut state = self.state.lock().unwrap();
        let Some(upload) = state.uploads.get_mut(cmd.id.as_str()) else {
            return Ok(());
        };
        if upload.state == UploadState::Completed {
            return Ok(());
        }
        upload.state = UploadState::Aborted;
        upload.aborted_at = Some(cmd.now);
        let (backend, blob_ref) = (upload.blob_backend.clone(), upload.blob_ref.clone());
        enqueue_gc(&mut state, &backend, &blob_ref, cmd.gc_not_before);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// IdempotencyStore
// ---------------------------------------------------------------------------

#[async_trait]
impl IdempotencyStore for InMemoryMetaStore {
    async fn acquire(&self, req: IdempotencyAcquire) -> DomainResult<IdempotencyDecision> {
        let mut state = self.state.lock().unwrap();
        let ctx = &req.context;
        let key = (
            ctx.principal_id.to_string(),
            ctx.operation.clone(),
            ctx.key.to_string(),
        );

        match state.idempotency.get_mut(&key) {
            None => {
                state.idempotency.insert(
                    key,
                    IdempotencyRow {
                        request_hash: ctx.request_hash.clone(),
                        created_at: req.now,
                        completed: false,
                        owner_token: Some(ctx.owner_token.clone()),
                        lease_until: Some(req.lease_until),
                        response: None,
                        expires_at: req.expires_at,
                    },
                );
                Ok(IdempotencyDecision::Owner {
                    resource_token: req.now.timestamp_micros().to_string(),
                })
            }
            Some(row) => {
                if row.request_hash != ctx.request_hash {
                    return Ok(IdempotencyDecision::Conflict);
                }
                if row.completed {
                    return Ok(IdempotencyDecision::Replay(
                        row.response
                            .clone()
                            .unwrap_or_else(StoredResponse::no_content),
                    ));
                }
                // Take over a lease abandoned by a crashed attempt.
                if row.lease_until.is_none_or(|until| until <= req.now) {
                    row.owner_token = Some(ctx.owner_token.clone());
                    row.lease_until = Some(req.lease_until);
                    return Ok(IdempotencyDecision::Owner {
                        resource_token: row.created_at.timestamp_micros().to_string(),
                    });
                }
                Err(DomainError::RateLimited)
            }
        }
    }

    async fn complete(&self, req: IdempotencyComplete) -> DomainResult<()> {
        let mut state = self.state.lock().unwrap();
        let ctx = &req.context;
        let key = (
            ctx.principal_id.to_string(),
            ctx.operation.clone(),
            ctx.key.to_string(),
        );
        let row = state
            .idempotency
            .get_mut(&key)
            .ok_or_else(|| DomainError::internal("idempotency record disappeared"))?;
        if row.owner_token.as_deref() != Some(ctx.owner_token.as_str()) {
            return Err(DomainError::internal(
                "idempotency lease was lost during the operation",
            ));
        }
        row.completed = true;
        row.response = Some(req.response);
        row.owner_token = None;
        row.lease_until = None;
        row.expires_at = req.expires_at;
        Ok(())
    }

    async fn fail_or_release(&self, req: IdempotencyRelease) -> DomainResult<()> {
        let mut state = self.state.lock().unwrap();
        let ctx = req.context;
        let key = (
            ctx.principal_id.to_string(),
            ctx.operation.clone(),
            ctx.key.to_string(),
        );
        if let Some(row) = state.idempotency.get(&key)
            && !row.completed
            && row.owner_token.as_deref() == Some(ctx.owner_token.as_str())
        {
            state.idempotency.remove(&key);
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// ChangeStore
// ---------------------------------------------------------------------------

#[async_trait]
impl ChangeStore for InMemoryMetaStore {
    async fn read_changes(&self, req: ReadChanges) -> DomainResult<ChangePage> {
        let mut state = self.state.lock().unwrap();
        let watermark = state.purged_through_seq;

        let (after_seq, materialized) = match &req.cursor {
            None => (watermark, None),
            Some(cursor) => {
                if !cursor.is_well_formed() {
                    return Err(DomainError::InvalidRequest("Invalid cursor.".into()));
                }
                let row = state
                    .cursors
                    .get(cursor.as_str())
                    .cloned()
                    .ok_or(DomainError::ChangesCursorExpired)?;
                if row.expires_at <= req.now || row.after_seq < watermark {
                    return Err(DomainError::ChangesCursorExpired);
                }
                (row.after_seq, Some((cursor.clone(), row)))
            }
        };

        if let Some((_, row)) = &materialized
            && row.materialized
        {
            if row.page_limit != Some(req.limit) {
                return Err(DomainError::InvalidRequest(
                    "This cursor was already read with a different limit.".into(),
                ));
            }
            let end = row.page_end_seq.unwrap_or(after_seq);
            let items = state
                .changes
                .iter()
                .enumerate()
                .filter(|(index, _)| {
                    let seq = *index as i64 + 1;
                    seq > after_seq && seq <= end
                })
                .map(|(_, record)| record.clone())
                .collect();
            return Ok(ChangePage {
                items,
                next_cursor: row.next_cursor_id.clone().expect("materialised cursor"),
                has_more: row.has_more.unwrap_or(false),
                lag: (state.changes.len() as i64 - end).max(0) as u64,
            });
        }

        // Snapshot the feed so later appends cannot change this page.
        let snapshot_max_seq = state.changes.len() as i64;
        let items: Vec<ChangeRecord> = state
            .changes
            .iter()
            .enumerate()
            .filter(|(index, _)| {
                let seq = *index as i64 + 1;
                seq > after_seq && seq <= snapshot_max_seq
            })
            .take(req.limit as usize)
            .map(|(_, record)| record.clone())
            .collect();

        let page_end_seq = after_seq + items.len() as i64;
        let has_more = snapshot_max_seq > page_end_seq;

        let next_cursor = CursorId::generate();
        state.cursors.insert(
            next_cursor.to_string(),
            CursorRow {
                after_seq: page_end_seq,
                materialized: false,
                page_limit: None,
                page_end_seq: None,
                has_more: None,
                next_cursor_id: None,
                expires_at: req.cursor_expires_at,
            },
        );

        if let Some((cursor, _)) = &materialized {
            let row = state
                .cursors
                .get_mut(cursor.as_str())
                .expect("cursor was loaded above");
            row.materialized = true;
            row.page_limit = Some(req.limit);
            row.page_end_seq = Some(page_end_seq);
            row.has_more = Some(has_more);
            row.next_cursor_id = Some(next_cursor.clone());
        }

        Ok(ChangePage {
            items,
            next_cursor,
            has_more,
            lag: (snapshot_max_seq - page_end_seq).max(0) as u64,
        })
    }
}

// ---------------------------------------------------------------------------
// MaintenanceStore
// ---------------------------------------------------------------------------

#[async_trait]
impl MaintenanceStore for InMemoryMetaStore {
    async fn claim_gc_batch(
        &self,
        now: DateTime<Utc>,
        limit: u32,
    ) -> DomainResult<Vec<BlobGcEntry>> {
        let mut state = self.state.lock().unwrap();
        let live_blobs: HashSet<(String, String)> = state
            .objects
            .values()
            .filter(|object| object.is_live())
            .filter_map(|object| Some((object.blob_backend.clone()?, object.blob_ref.clone()?)))
            .collect();
        let mut claimed = Vec::new();
        for row in state.gc.iter_mut() {
            if claimed.len() >= limit as usize {
                break;
            }
            if row.not_before <= now
                && !live_blobs.contains(&(row.blob_backend.clone(), row.blob_ref.clone()))
            {
                row.not_before = now + Duration::minutes(5);
                claimed.push(BlobGcEntry {
                    blob_backend: row.blob_backend.clone(),
                    blob_ref: row.blob_ref.clone(),
                    attempts: row.attempts,
                });
            }
        }
        Ok(claimed)
    }

    async fn finish_gc(&self, entry: &BlobGcEntry) -> DomainResult<()> {
        let mut state = self.state.lock().unwrap();
        state
            .gc
            .retain(|g| !(g.blob_backend == entry.blob_backend && g.blob_ref == entry.blob_ref));
        Ok(())
    }

    async fn fail_gc(
        &self,
        entry: &BlobGcEntry,
        _error: &str,
        retry_at: DateTime<Utc>,
    ) -> DomainResult<()> {
        let mut state = self.state.lock().unwrap();
        if let Some(row) = state
            .gc
            .iter_mut()
            .find(|g| g.blob_backend == entry.blob_backend && g.blob_ref == entry.blob_ref)
        {
            row.attempts += 1;
            row.not_before = retry_at;
        }
        Ok(())
    }

    async fn claim_expired_uploads(
        &self,
        now: DateTime<Utc>,
        limit: u32,
    ) -> DomainResult<Vec<UploadRecord>> {
        let mut state = self.state.lock().unwrap();
        let expired: Vec<UploadRecord> = state
            .uploads
            .values_mut()
            .filter(|u| {
                u.expires_at <= now
                    && matches!(
                        u.state,
                        UploadState::Initiating | UploadState::Ready | UploadState::Completing
                    )
            })
            .take(limit as usize)
            .map(|u| {
                u.state = UploadState::Expired;
                u.clone()
            })
            .collect();

        for upload in &expired {
            enqueue_gc(&mut state, &upload.blob_backend, &upload.blob_ref, now);
        }
        Ok(expired)
    }

    async fn purge_idempotency_records(&self, now: DateTime<Utc>) -> DomainResult<u64> {
        let mut state = self.state.lock().unwrap();
        let before = state.idempotency.len();
        state.idempotency.retain(|_, row| row.expires_at > now);
        Ok((before - state.idempotency.len()) as u64)
    }

    async fn purge_change_cursors(&self, now: DateTime<Utc>) -> DomainResult<u64> {
        let mut state = self.state.lock().unwrap();
        let before = state.cursors.len();
        state.cursors.retain(|_, row| row.expires_at > now);
        Ok((before - state.cursors.len()) as u64)
    }

    async fn purge_changes(&self, older_than: DateTime<Utc>) -> DomainResult<u64> {
        let mut state = self.state.lock().unwrap();
        // Changes keep their sequence positions; only the watermark advances so
        // that stale cursors can still be rejected.
        let mut purged = 0;
        let mut watermark = state.purged_through_seq;
        for (index, record) in state.changes.iter().enumerate() {
            if record.changed_at < older_than {
                watermark = watermark.max(index as i64 + 1);
                purged += 1;
            } else {
                break;
            }
        }
        state.purged_through_seq = watermark;
        Ok(purged)
    }

    async fn count_pending_gc(&self) -> DomainResult<u64> {
        Ok(self.state.lock().unwrap().gc.len() as u64)
    }
}

/// Convenience for tests that need to inspect the recorded principal scope.
pub fn local_principal() -> PrincipalId {
    PrincipalId::local()
}

/// Exposes the raw Changes feed for assertions.
impl InMemoryMetaStore {
    pub fn all_changes(&self) -> Vec<ChangeRecord> {
        self.state.lock().unwrap().changes.clone()
    }

    pub fn changes_for(&self, id: &ObjectId) -> Vec<ChangeRecord> {
        self.all_changes()
            .into_iter()
            .filter(|c| c.object_id == *id)
            .collect()
    }

    pub fn pending_gc(&self) -> usize {
        self.state.lock().unwrap().gc.len()
    }

    pub fn idempotency_key_count(&self) -> usize {
        self.state.lock().unwrap().idempotency.len()
    }

    pub fn record_for(&self, key: &IdempotencyKey) -> bool {
        self.state
            .lock()
            .unwrap()
            .idempotency
            .keys()
            .any(|(_, _, k)| k == key.as_str())
    }
}
