//! `MetaStore` conformance suite.

use anystore_domain::change::ChangeAction;
use anystore_domain::error::DomainResult;
use anystore_domain::metadata::MetadataPatch;
use anystore_domain::object::{ObjectKind, Revision};
use anystore_domain::upload::{UploadMode, UploadState};
use anystore_domain::{IdempotencyKey, ObjectId, PrincipalId, RequestId, UploadId};
use anystore_metastore::changes::ReadChanges;
use anystore_metastore::commands::{
    CommitContent, CreateObjectCommit, DeleteObjectCommit, ListChildren, ListOrder,
    MetadataCondition, MutationContext, MutationOutcome, ObjectQuery, OrderBy, PatchObjectCommit,
};
use anystore_metastore::idempotency::{
    IdempotencyAcquire, IdempotencyComplete, IdempotencyContext, IdempotencyDecision,
};
use anystore_metastore::response::{ResponseRenderer, StoredResponse};
use anystore_metastore::uploads::{CreateUploadRecord, UpdateUploadState};
use anystore_metastore::{
    ChangeStore, IdempotencyStore, MaintenanceStore, MetaStore, ObjectMutationStore,
    ObjectRepository, UploadRepository,
};
use async_trait::async_trait;
use chrono::{Duration, Utc};
use serde_json::json;
use std::sync::Arc;

/// Produces a store with a pristine object tree.
#[async_trait]
pub trait MetaStoreFactory: Send + Sync {
    type Store: MetaStore + 'static;
    async fn create(&self) -> Arc<Self::Store>;
}

fn renderer(status: u16) -> ResponseRenderer {
    Arc::new(move |outcome: &MutationOutcome| {
        let body = match &outcome.object {
            Some(view) => serde_json::to_vec(&json!({
                "id": view.object.id.as_str(),
                "revision": view.object.revision.get(),
                "name": view.object.name,
                "path": view.path,
                "metadata": view.object.metadata,
            }))
            .unwrap(),
            None => Vec::new(),
        };
        Ok(StoredResponse::json(status, body))
    })
}

fn no_content_renderer() -> ResponseRenderer {
    Arc::new(|_: &MutationOutcome| Ok(StoredResponse::no_content()))
}

fn context(key: Option<&str>) -> (MutationContext, Option<IdempotencyContext>) {
    let now = Utc::now();
    let ictx = key.map(|key| IdempotencyContext {
        principal_id: PrincipalId::local(),
        operation: "TEST".to_owned(),
        key: IdempotencyKey::new(key),
        request_hash: format!("hash-of-{key}"),
        owner_token: format!("owner-{key}"),
        resource_token: now.timestamp_micros().to_string(),
    });

    (
        MutationContext {
            now,
            request_id: RequestId::generate(),
            idempotency_key: key.map(IdempotencyKey::new),
            idempotency: ictx.clone(),
            idempotency_expires_at: now + Duration::hours(24),
            render: renderer(200),
        },
        ictx,
    )
}

/// Claims the key first, exactly as the application layer does.
async fn claim<M: MetaStore + ?Sized>(store: &M, ictx: &Option<IdempotencyContext>) {
    let Some(ictx) = ictx else { return };
    let now = Utc::now();
    let decision = store
        .acquire(IdempotencyAcquire {
            context: ictx.clone(),
            now,
            lease_until: now + Duration::seconds(30),
            expires_at: now + Duration::hours(24),
        })
        .await
        .expect("acquire must not fail");
    assert!(
        matches!(decision, IdempotencyDecision::Owner { .. }),
        "the first attempt must own the key"
    );
}

async fn create_folder<M: MetaStore + ?Sized>(
    store: &M,
    name: &str,
    parent: &ObjectId,
    key: Option<&str>,
) -> DomainResult<(ObjectId, StoredResponse)> {
    let id = ObjectId::generate();
    let (mut ctx, ictx) = context(key);
    ctx.render = renderer(201);
    claim(store, &ictx).await;

    let response = store
        .create_object(CreateObjectCommit {
            id: id.clone(),
            kind: ObjectKind::Folder,
            name: name.to_owned(),
            parent_id: parent.clone(),
            content_type: None,
            metadata: json!({}),
            ctx,
        })
        .await?;
    Ok((id, response))
}

async fn create_file<M: MetaStore + ?Sized>(
    store: &M,
    name: &str,
    parent: &ObjectId,
    metadata: serde_json::Value,
) -> DomainResult<ObjectId> {
    let id = ObjectId::generate();
    let (mut ctx, ictx) = context(None);
    ctx.render = renderer(201);
    claim(store, &ictx).await;

    store
        .create_object(CreateObjectCommit {
            id: id.clone(),
            kind: ObjectKind::File,
            name: name.to_owned(),
            parent_id: parent.clone(),
            content_type: Some("text/plain".to_owned()),
            metadata,
            ctx,
        })
        .await?;
    Ok(id)
}

async fn create_ready_upload<M: MetaStore + ?Sized>(
    store: &M,
    object_id: &ObjectId,
    blob_ref: &str,
    expires_at: chrono::DateTime<Utc>,
) -> UploadId {
    let id = UploadId::generate();
    let created = store
        .create_upload_record(CreateUploadRecord {
            id: id.clone(),
            object_id: object_id.clone(),
            mode: UploadMode::Single,
            blob_backend: "test".into(),
            blob_ref: blob_ref.to_owned(),
            expected_size: 11,
            content_type: "text/plain".into(),
            expected_sha256: Some("a".repeat(64)),
            created_at: Utc::now(),
            expires_at,
        })
        .await
        .unwrap();
    assert!(created.created);
    store
        .update_upload_state(UpdateUploadState {
            id: id.clone(),
            state: UploadState::Ready,
            provider_upload_id: None,
            provider_completed: None,
        })
        .await
        .unwrap();
    id
}

async fn revision_of<M: MetaStore + ?Sized>(store: &M, id: &ObjectId) -> Revision {
    store
        .get_object(id)
        .await
        .unwrap()
        .expect("object should exist")
        .object
        .revision
}

async fn all_changes<M: MetaStore + ?Sized>(
    store: &M,
) -> Vec<anystore_domain::change::ChangeRecord> {
    let now = Utc::now();
    let mut items = Vec::new();
    let mut page = store
        .read_changes(ReadChanges {
            cursor: None,
            limit: 1000,
            now,
            cursor_expires_at: now + Duration::days(30),
        })
        .await
        .unwrap();
    items.extend(page.items.clone());
    while page.has_more {
        page = store
            .read_changes(ReadChanges {
                cursor: Some(page.next_cursor.clone()),
                limit: 1000,
                now,
                cursor_expires_at: now + Duration::days(30),
            })
            .await
            .unwrap();
        items.extend(page.items.clone());
    }
    items
}

async fn changes_for<M: MetaStore + ?Sized>(
    store: &M,
    id: &ObjectId,
) -> Vec<anystore_domain::change::ChangeRecord> {
    all_changes(store)
        .await
        .into_iter()
        .filter(|c| c.object_id == *id)
        .collect()
}

// ---------------------------------------------------------------------------
// Cases
// ---------------------------------------------------------------------------

pub async fn new_objects_start_at_revision_one<F: MetaStoreFactory>(factory: &F) {
    let store = factory.create().await;
    let (id, _) = create_folder(&*store, "a", &ObjectId::root(), None)
        .await
        .unwrap();
    assert_eq!(revision_of(&*store, &id).await, Revision::INITIAL);

    let changes = changes_for(&*store, &id).await;
    assert_eq!(changes.len(), 1, "creation emits exactly one Change");
    assert_eq!(changes[0].action, ChangeAction::Created);
}

pub async fn live_parent_and_name_are_unique<F: MetaStoreFactory>(factory: &F) {
    let store = factory.create().await;
    create_folder(&*store, "dup", &ObjectId::root(), None)
        .await
        .unwrap();
    let error = create_folder(&*store, "dup", &ObjectId::root(), None)
        .await
        .unwrap_err();
    assert_eq!(error.code(), "name_conflict");
}

pub async fn deleted_names_can_be_reused<F: MetaStoreFactory>(factory: &F) {
    let store = factory.create().await;
    let (id, _) = create_folder(&*store, "reuse", &ObjectId::root(), None)
        .await
        .unwrap();

    let (mut ctx, ictx) = context(None);
    ctx.render = no_content_renderer();
    claim(&*store, &ictx).await;
    store
        .delete_object(DeleteObjectCommit {
            id,
            if_match: None,
            recursive: false,
            ctx,
        })
        .await
        .unwrap();

    create_folder(&*store, "reuse", &ObjectId::root(), None)
        .await
        .expect("a deleted name becomes free again");
}

pub async fn stale_if_match_never_overwrites<F: MetaStoreFactory>(factory: &F) {
    let store = factory.create().await;
    let id = create_file(&*store, "f.txt", &ObjectId::root(), json!({"v": 1}))
        .await
        .unwrap();
    let original = revision_of(&*store, &id).await;

    let (ctx, ictx) = context(None);
    claim(&*store, &ictx).await;
    store
        .patch_object(PatchObjectCommit {
            id: id.clone(),
            if_match: Some(original),
            new_name: None,
            new_parent_id: None,
            metadata_patch: Some(MetadataPatch {
                set: json!({"v": 2}).as_object().unwrap().clone(),
                remove: vec![],
            }),
            ctx,
        })
        .await
        .unwrap();

    let updated = revision_of(&*store, &id).await;
    assert_eq!(updated.get(), original.get() + 1);

    let before = changes_for(&*store, &id).await.len();
    let (ctx, ictx) = context(None);
    claim(&*store, &ictx).await;
    let error = store
        .patch_object(PatchObjectCommit {
            id: id.clone(),
            if_match: Some(original),
            new_name: None,
            new_parent_id: None,
            metadata_patch: Some(MetadataPatch {
                set: json!({"v": 3}).as_object().unwrap().clone(),
                remove: vec![],
            }),
            ctx,
        })
        .await
        .unwrap_err();

    assert_eq!(error.code(), "revision_conflict");
    assert_eq!(revision_of(&*store, &id).await, updated);
    assert_eq!(changes_for(&*store, &id).await.len(), before);

    let view = store.get_object(&id).await.unwrap().unwrap();
    assert_eq!(view.object.metadata["v"], json!(2));
}

pub async fn concurrent_conditional_updates_admit_one_winner<F: MetaStoreFactory>(factory: &F) {
    let store = factory.create().await;
    let id = create_file(&*store, "race.txt", &ObjectId::root(), json!({}))
        .await
        .unwrap();
    let revision = revision_of(&*store, &id).await;

    let patch = |value: i32| {
        let store = Arc::clone(&store);
        let id = id.clone();
        async move {
            let (ctx, ictx) = context(None);
            claim(&*store, &ictx).await;
            store
                .patch_object(PatchObjectCommit {
                    id,
                    if_match: Some(revision),
                    new_name: None,
                    new_parent_id: None,
                    metadata_patch: Some(MetadataPatch {
                        set: json!({ "winner": value }).as_object().unwrap().clone(),
                        remove: vec![],
                    }),
                    ctx,
                })
                .await
        }
    };

    let (first, second) = join(patch(1), patch(2)).await;
    let succeeded = [first.is_ok(), second.is_ok()]
        .iter()
        .filter(|ok| **ok)
        .count();
    assert_eq!(succeeded, 1, "exactly one conditional update may win");

    assert_eq!(
        revision_of(&*store, &id).await.get(),
        revision.get() + 1,
        "the loser must not increment the revision"
    );
}

/// Polls two futures together, so the suite needs no `futures` dependency.
async fn join<A, B, TA, TB>(a: A, b: B) -> (TA, TB)
where
    A: std::future::Future<Output = TA>,
    B: std::future::Future<Output = TB>,
{
    let mut a = Box::pin(a);
    let mut b = Box::pin(b);
    let mut ra = None;
    let mut rb = None;

    std::future::poll_fn(|cx| {
        if ra.is_none()
            && let std::task::Poll::Ready(value) = a.as_mut().poll(cx)
        {
            ra = Some(value);
        }
        if rb.is_none()
            && let std::task::Poll::Ready(value) = b.as_mut().poll(cx)
        {
            rb = Some(value);
        }
        if ra.is_some() && rb.is_some() {
            std::task::Poll::Ready(())
        } else {
            std::task::Poll::Pending
        }
    })
    .await;

    (ra.unwrap(), rb.unwrap())
}

pub async fn one_patch_emits_ordered_changes_at_one_revision<F: MetaStoreFactory>(factory: &F) {
    let store = factory.create().await;
    let (destination, _) = create_folder(&*store, "dest", &ObjectId::root(), None)
        .await
        .unwrap();
    let id = create_file(&*store, "before.txt", &ObjectId::root(), json!({}))
        .await
        .unwrap();
    let revision = revision_of(&*store, &id).await;

    let (ctx, ictx) = context(Some("combined"));
    claim(&*store, &ictx).await;
    store
        .patch_object(PatchObjectCommit {
            id: id.clone(),
            if_match: Some(revision),
            new_name: Some("after.txt".to_owned()),
            new_parent_id: Some(destination),
            metadata_patch: Some(MetadataPatch {
                set: json!({"k": true}).as_object().unwrap().clone(),
                remove: vec![],
            }),
            ctx,
        })
        .await
        .unwrap();

    let new_revision = revision_of(&*store, &id).await;
    assert_eq!(
        new_revision.get(),
        revision.get() + 1,
        "one PATCH increments the revision once"
    );

    let records = changes_for(&*store, &id).await;
    let mutation: Vec<_> = records
        .iter()
        .filter(|c| c.action != ChangeAction::Created)
        .collect();

    assert_eq!(
        mutation.iter().map(|c| c.action).collect::<Vec<_>>(),
        vec![
            ChangeAction::Renamed,
            ChangeAction::Moved,
            ChangeAction::MetadataUpdated
        ]
    );
    assert!(mutation.iter().all(|c| c.revision == new_revision));
    assert_eq!(
        mutation
            .iter()
            .map(|c| c.request_id.to_string())
            .collect::<std::collections::HashSet<_>>()
            .len(),
        1,
        "all records share one request id"
    );
}

pub async fn no_op_patch_changes_nothing<F: MetaStoreFactory>(factory: &F) {
    let store = factory.create().await;
    let id = create_file(&*store, "same.txt", &ObjectId::root(), json!({"a": 1}))
        .await
        .unwrap();
    let revision = revision_of(&*store, &id).await;
    let before = changes_for(&*store, &id).await.len();

    let (ctx, ictx) = context(None);
    claim(&*store, &ictx).await;
    store
        .patch_object(PatchObjectCommit {
            id: id.clone(),
            if_match: Some(revision),
            new_name: Some("same.txt".to_owned()),
            new_parent_id: Some(ObjectId::root()),
            metadata_patch: Some(MetadataPatch {
                set: json!({"a": 1}).as_object().unwrap().clone(),
                remove: vec![],
            }),
            ctx,
        })
        .await
        .unwrap();

    assert_eq!(revision_of(&*store, &id).await, revision);
    assert_eq!(changes_for(&*store, &id).await.len(), before);
}

pub async fn folders_cannot_move_into_themselves<F: MetaStoreFactory>(factory: &F) {
    let store = factory.create().await;
    let (a, _) = create_folder(&*store, "a", &ObjectId::root(), None)
        .await
        .unwrap();
    let (b, _) = create_folder(&*store, "b", &a, None).await.unwrap();

    for destination in [a.clone(), b] {
        let before = changes_for(&*store, &a).await.len();
        let revision = revision_of(&*store, &a).await;

        let (ctx, ictx) = context(None);
        claim(&*store, &ictx).await;
        let error = store
            .patch_object(PatchObjectCommit {
                id: a.clone(),
                if_match: None,
                new_name: None,
                new_parent_id: Some(destination),
                metadata_patch: None,
                ctx,
            })
            .await
            .unwrap_err();

        assert_eq!(error.code(), "invalid_move");
        assert_eq!(revision_of(&*store, &a).await, revision);
        assert_eq!(changes_for(&*store, &a).await.len(), before);
    }
}

pub async fn moving_a_folder_does_not_rewrite_descendants<F: MetaStoreFactory>(factory: &F) {
    let store = factory.create().await;
    let (parent, _) = create_folder(&*store, "parent", &ObjectId::root(), None)
        .await
        .unwrap();
    let (child, _) = create_folder(&*store, "child", &parent, None)
        .await
        .unwrap();
    let leaf = create_file(&*store, "leaf.txt", &child, json!({}))
        .await
        .unwrap();
    let (destination, _) = create_folder(&*store, "dest", &ObjectId::root(), None)
        .await
        .unwrap();

    let leaf_revision = revision_of(&*store, &leaf).await;
    let child_revision = revision_of(&*store, &child).await;

    let (ctx, ictx) = context(None);
    claim(&*store, &ictx).await;
    store
        .patch_object(PatchObjectCommit {
            id: parent,
            if_match: None,
            new_name: None,
            new_parent_id: Some(destination),
            metadata_patch: None,
            ctx,
        })
        .await
        .unwrap();

    assert_eq!(revision_of(&*store, &leaf).await, leaf_revision);
    assert_eq!(revision_of(&*store, &child).await, child_revision);

    let view = store.get_object(&leaf).await.unwrap().unwrap();
    assert_eq!(view.path, "/dest/parent/child/leaf.txt");
    assert_eq!(view.object.id, leaf, "an id never changes");
}

pub async fn recursive_delete_tombstones_every_descendant<F: MetaStoreFactory>(factory: &F) {
    let store = factory.create().await;
    let (root, _) = create_folder(&*store, "tree", &ObjectId::root(), None)
        .await
        .unwrap();
    let (mid, _) = create_folder(&*store, "mid", &root, None).await.unwrap();
    let leaf = create_file(&*store, "leaf.txt", &mid, json!({}))
        .await
        .unwrap();

    let (mut ctx, ictx) = context(None);
    ctx.render = no_content_renderer();
    claim(&*store, &ictx).await;
    let error = store
        .delete_object(DeleteObjectCommit {
            id: root.clone(),
            if_match: None,
            recursive: false,
            ctx,
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), "folder_not_empty");

    let targets = [root.clone(), mid, leaf];
    let mut previous = Vec::new();
    for id in &targets {
        previous.push(revision_of(&*store, id).await);
    }

    let (mut ctx, ictx) = context(None);
    ctx.render = no_content_renderer();
    claim(&*store, &ictx).await;
    store
        .delete_object(DeleteObjectCommit {
            id: root,
            if_match: None,
            recursive: true,
            ctx,
        })
        .await
        .unwrap();

    for (id, before) in targets.iter().zip(previous) {
        assert!(store.get_object(id).await.unwrap().is_none());
        let tombstones: Vec<_> = changes_for(&*store, id)
            .await
            .into_iter()
            .filter(|c| c.action == ChangeAction::Deleted)
            .collect();
        assert_eq!(tombstones.len(), 1, "one tombstone per deleted object");
        assert!(tombstones[0].tombstone);
        assert_eq!(tombstones[0].revision.get(), before.get() + 1);
    }
}

pub async fn idempotent_replay_returns_the_original_response<F: MetaStoreFactory>(factory: &F) {
    let store = factory.create().await;
    let (_, first) = create_folder(&*store, "once", &ObjectId::root(), Some("k1"))
        .await
        .unwrap();

    let ictx = context(Some("k1")).1.unwrap();
    let now = Utc::now();
    let decision = store
        .acquire(IdempotencyAcquire {
            context: ictx.clone(),
            now,
            lease_until: now + Duration::seconds(30),
            expires_at: now + Duration::hours(24),
        })
        .await
        .unwrap();

    match decision {
        IdempotencyDecision::Replay(replayed) => {
            assert_eq!(replayed, first, "a replay is byte-identical")
        }
        other => panic!("expected a replay, got {other:?}"),
    }

    let mut different = ictx;
    different.request_hash = "another-hash".to_owned();
    let decision = store
        .acquire(IdempotencyAcquire {
            context: different,
            now,
            lease_until: now + Duration::seconds(30),
            expires_at: now + Duration::hours(24),
        })
        .await
        .unwrap();
    assert!(matches!(decision, IdempotencyDecision::Conflict));
}

pub async fn changes_cursor_pages_are_stable<F: MetaStoreFactory>(factory: &F) {
    let store = factory.create().await;
    for index in 0..5 {
        create_folder(&*store, &format!("f{index}"), &ObjectId::root(), None)
            .await
            .unwrap();
    }

    let read = |cursor: Option<anystore_domain::ids::CursorId>, limit: u32| {
        let store = Arc::clone(&store);
        async move {
            store
                .read_changes(ReadChanges {
                    cursor,
                    limit,
                    now: Utc::now(),
                    cursor_expires_at: Utc::now() + Duration::days(30),
                })
                .await
                .unwrap()
        }
    };

    let initial = read(None, 100).await;
    assert!(
        !initial.next_cursor.as_str().is_empty(),
        "every read returns a cursor so polling can continue"
    );

    let first = read(Some(initial.next_cursor.clone()), 2).await;

    // Append more Changes; a bound page must not move.
    create_folder(&*store, "later", &ObjectId::root(), None)
        .await
        .unwrap();

    let repeat = read(Some(initial.next_cursor.clone()), 2).await;
    let ids = |page: &anystore_metastore::changes::ChangePage| {
        page.items
            .iter()
            .map(|c| c.change_id.to_string())
            .collect::<Vec<_>>()
    };
    assert_eq!(
        ids(&first),
        ids(&repeat),
        "the same cursor returns the same page"
    );
    assert_eq!(first.next_cursor.as_str(), repeat.next_cursor.as_str());
    assert_eq!(first.has_more, repeat.has_more);

    let seen: std::collections::HashSet<String> = ids(&first).into_iter().collect();
    let continuation = read(Some(first.next_cursor.clone()), 2).await;
    assert!(
        continuation
            .items
            .iter()
            .all(|c| !seen.contains(c.change_id.as_str())),
        "the continuation must not replay"
    );
}

pub async fn changes_are_globally_ordered_and_monotonic<F: MetaStoreFactory>(factory: &F) {
    let store = factory.create().await;
    for index in 0..4 {
        create_folder(&*store, &format!("o{index}"), &ObjectId::root(), None)
            .await
            .unwrap();
    }
    let ids: Vec<String> = all_changes(&*store)
        .await
        .into_iter()
        .map(|c| c.change_id.to_string())
        .collect();

    let mut sorted = ids.clone();
    sorted.sort();
    assert_eq!(ids, sorted, "change ids are monotonic in Change order");
    assert_eq!(
        ids.iter().collect::<std::collections::HashSet<_>>().len(),
        ids.len(),
        "change ids are unique"
    );
}

pub async fn unknown_cursor_is_reported_as_expired<F: MetaStoreFactory>(factory: &F) {
    let store = factory.create().await;
    let now = Utc::now();
    let error = store
        .read_changes(ReadChanges {
            cursor: Some(anystore_domain::ids::CursorId::new(
                "chgcur_00000000000000000000000000",
            )),
            limit: 10,
            now,
            cursor_expires_at: now + Duration::days(30),
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), "changes_cursor_expired");
}

pub async fn metadata_eq_and_exists_filter_correctly<F: MetaStoreFactory>(factory: &F) {
    let store = factory.create().await;
    let matching = create_file(
        &*store,
        "match.txt",
        &ObjectId::root(),
        json!({"team": "core", "year": 2026}),
    )
    .await
    .unwrap();
    create_file(
        &*store,
        "other.txt",
        &ObjectId::root(),
        json!({"team": "ops"}),
    )
    .await
    .unwrap();

    let query = |metadata: Vec<(String, MetadataCondition)>| {
        let store = Arc::clone(&store);
        async move {
            store
                .query_objects(ObjectQuery {
                    kind: Some(ObjectKind::File),
                    name: None,
                    parent_id: None,
                    metadata,
                    limit: 100,
                    cursor: None,
                })
                .await
                .unwrap()
        }
    };

    let page = query(vec![
        ("team".into(), MetadataCondition::Eq(json!("core"))),
        ("year".into(), MetadataCondition::Exists(true)),
    ])
    .await;
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].object.id, matching);

    let page = query(vec![
        ("team".into(), MetadataCondition::Eq(json!("core"))),
        ("missing".into(), MetadataCondition::Exists(true)),
    ])
    .await;
    assert!(page.items.is_empty(), "conditions are ANDed");

    let page = query(vec![("year".into(), MetadataCondition::Eq(json!("2026")))]).await;
    assert!(page.items.is_empty(), "2026 must not equal \"2026\"");
}

pub async fn deleted_objects_leave_every_read_path<F: MetaStoreFactory>(factory: &F) {
    let store = factory.create().await;
    let (folder, _) = create_folder(&*store, "vis", &ObjectId::root(), None)
        .await
        .unwrap();
    let id = create_file(&*store, "gone.txt", &folder, json!({"gone": true}))
        .await
        .unwrap();

    let (mut ctx, ictx) = context(None);
    ctx.render = no_content_renderer();
    claim(&*store, &ictx).await;
    store
        .delete_object(DeleteObjectCommit {
            id: id.clone(),
            if_match: None,
            recursive: false,
            ctx,
        })
        .await
        .unwrap();

    assert!(store.get_object(&id).await.unwrap().is_none());
    assert!(store.resolve_path("/vis/gone.txt").await.unwrap().is_none());
    assert!(
        store
            .query_objects(ObjectQuery {
                kind: None,
                name: None,
                parent_id: None,
                metadata: vec![("gone".into(), MetadataCondition::Exists(true))],
                limit: 100,
                cursor: None,
            })
            .await
            .unwrap()
            .items
            .is_empty()
    );
    assert!(
        store
            .list_children(ListChildren {
                parent_id: folder,
                limit: 100,
                cursor: None,
                order_by: OrderBy::Name,
                order: ListOrder::Asc,
            })
            .await
            .unwrap()
            .items
            .is_empty()
    );
}

pub async fn content_commit_selects_the_right_action<F: MetaStoreFactory>(factory: &F) {
    let store = factory.create().await;
    let id = create_file(&*store, "c.bin", &ObjectId::root(), json!({}))
        .await
        .unwrap();

    for (blob, expected) in [
        ("blobs/one", ChangeAction::ContentReady),
        ("blobs/two", ChangeAction::ContentReplaced),
    ] {
        let upload_id =
            create_ready_upload(&*store, &id, blob, Utc::now() + Duration::hours(1)).await;
        let revision = revision_of(&*store, &id).await;
        let (ctx, ictx) = context(None);
        claim(&*store, &ictx).await;
        store
            .commit_content(CommitContent {
                upload_id,
                object_id: id.clone(),
                if_match: Some(revision),
                blob_backend: "test".into(),
                blob_ref: blob.to_owned(),
                size: 11,
                content_type: "text/plain".into(),
                sha256: Some("abc".into()),
                gc_not_before: Utc::now(),
                ctx,
            })
            .await
            .unwrap();

        let latest = changes_for(&*store, &id).await.pop().unwrap();
        assert_eq!(latest.action, expected);
        assert_eq!(latest.revision.get(), revision.get() + 1);
    }

    let view = store.get_object(&id).await.unwrap().unwrap();
    assert!(view.object.has_ready_content());
    assert_eq!(view.object.size, Some(11));

    let pending = store
        .claim_gc_batch(Utc::now() + Duration::hours(2), 10)
        .await
        .unwrap();
    assert!(
        pending.iter().any(|e| e.blob_ref == "blobs/one"),
        "the replaced blob becomes collectable"
    );
    assert!(
        !pending.iter().any(|e| e.blob_ref == "blobs/two"),
        "a live blob is never collectable"
    );
}

pub async fn upload_creation_resumes_the_existing_record<F: MetaStoreFactory>(factory: &F) {
    let store = factory.create().await;
    let object_id = create_file(&*store, "resume.bin", &ObjectId::root(), json!({}))
        .await
        .unwrap();
    let now = Utc::now();
    let command = CreateUploadRecord {
        id: UploadId::generate(),
        object_id,
        mode: UploadMode::Multipart,
        blob_backend: "test".into(),
        blob_ref: "blobs/resume".into(),
        expected_size: 42,
        content_type: "application/octet-stream".into(),
        expected_sha256: None,
        created_at: now,
        expires_at: now + Duration::hours(1),
    };

    let first = store.create_upload_record(command.clone()).await.unwrap();
    let second = store.create_upload_record(command).await.unwrap();
    assert!(first.created);
    assert!(!second.created);
    assert_eq!(second.upload, first.upload);
}

pub async fn resource_token_changes_after_idempotency_retention<F: MetaStoreFactory>(factory: &F) {
    let store = factory.create().await;
    let now = Utc::now();
    let first_context = IdempotencyContext {
        principal_id: PrincipalId::local(),
        operation: "POST /uploads".into(),
        key: IdempotencyKey::new("retained-key"),
        request_hash: "same-request".into(),
        owner_token: "owner-one".into(),
        resource_token: String::new(),
    };
    let first = store
        .acquire(IdempotencyAcquire {
            context: first_context.clone(),
            now,
            lease_until: now + Duration::seconds(30),
            expires_at: now + Duration::seconds(1),
        })
        .await
        .unwrap();
    let first_token = match first {
        IdempotencyDecision::Owner { resource_token } => resource_token,
        other => panic!("expected owner, got {other:?}"),
    };
    store
        .complete(IdempotencyComplete {
            context: first_context,
            response: StoredResponse::no_content(),
            now,
            expires_at: now + Duration::seconds(1),
        })
        .await
        .unwrap();
    assert_eq!(
        store
            .purge_idempotency_records(now + Duration::seconds(2))
            .await
            .unwrap(),
        1
    );

    let second_context = IdempotencyContext {
        principal_id: PrincipalId::local(),
        operation: "POST /uploads".into(),
        key: IdempotencyKey::new("retained-key"),
        request_hash: "same-request".into(),
        owner_token: "owner-two".into(),
        resource_token: String::new(),
    };
    let second = store
        .acquire(IdempotencyAcquire {
            context: second_context,
            now: now + Duration::seconds(3),
            lease_until: now + Duration::seconds(33),
            expires_at: now + Duration::hours(24),
        })
        .await
        .unwrap();
    let second_token = match second {
        IdempotencyDecision::Owner { resource_token } => resource_token,
        other => panic!("expected owner, got {other:?}"),
    };
    assert_ne!(first_token, second_token);
}

pub async fn completed_upload_commit_is_convergent<F: MetaStoreFactory>(factory: &F) {
    let store = factory.create().await;
    let object_id = create_file(&*store, "repeat.bin", &ObjectId::root(), json!({}))
        .await
        .unwrap();
    let upload_id = create_ready_upload(
        &*store,
        &object_id,
        "blobs/repeat",
        Utc::now() + Duration::hours(1),
    )
    .await;

    for _ in 0..2 {
        let (ctx, ictx) = context(None);
        claim(&*store, &ictx).await;
        store
            .commit_content(CommitContent {
                upload_id: upload_id.clone(),
                object_id: object_id.clone(),
                if_match: None,
                blob_backend: "test".into(),
                blob_ref: "blobs/repeat".into(),
                size: 11,
                content_type: "text/plain".into(),
                sha256: Some("a".repeat(64)),
                gc_not_before: Utc::now(),
                ctx,
            })
            .await
            .unwrap();
    }

    assert_eq!(revision_of(&*store, &object_id).await, Revision(2));
    let changes = changes_for(&*store, &object_id).await;
    assert_eq!(
        changes
            .iter()
            .filter(|change| change.action == ChangeAction::ContentReady)
            .count(),
        1
    );
    assert!(
        changes
            .iter()
            .all(|change| change.action != ChangeAction::ContentReplaced)
    );
}

pub async fn expired_upload_cannot_commit_content<F: MetaStoreFactory>(factory: &F) {
    let store = factory.create().await;
    let object_id = create_file(&*store, "expired.bin", &ObjectId::root(), json!({}))
        .await
        .unwrap();
    let now = Utc::now();
    let upload_id = create_ready_upload(
        &*store,
        &object_id,
        "blobs/expired",
        now - Duration::seconds(1),
    )
    .await;

    let expired = store.claim_expired_uploads(now, 10).await.unwrap();
    assert_eq!(expired.len(), 1);

    let (ctx, ictx) = context(None);
    claim(&*store, &ictx).await;
    let error = store
        .commit_content(CommitContent {
            upload_id,
            object_id: object_id.clone(),
            if_match: None,
            blob_backend: "test".into(),
            blob_ref: "blobs/expired".into(),
            size: 11,
            content_type: "text/plain".into(),
            sha256: Some("a".repeat(64)),
            gc_not_before: now,
            ctx,
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), "upload_not_found");
    assert!(
        !store
            .get_object(&object_id)
            .await
            .unwrap()
            .unwrap()
            .object
            .has_ready_content()
    );
}

pub async fn purge_changes_reports_the_deleted_row_count<F: MetaStoreFactory>(factory: &F) {
    let store = factory.create().await;
    create_file(&*store, "purge-a", &ObjectId::root(), json!({}))
        .await
        .unwrap();
    create_file(&*store, "purge-b", &ObjectId::root(), json!({}))
        .await
        .unwrap();

    let purged = store
        .purge_changes(Utc::now() + Duration::seconds(1))
        .await
        .unwrap();
    assert_eq!(purged, 2);
}

pub async fn paths_and_listing_reflect_the_tree<F: MetaStoreFactory>(factory: &F) {
    let store = factory.create().await;
    let (a, _) = create_folder(&*store, "reports", &ObjectId::root(), None)
        .await
        .unwrap();
    let (b, _) = create_folder(&*store, "2026", &a, None).await.unwrap();
    let file = create_file(&*store, "example.pdf", &b, json!({}))
        .await
        .unwrap();

    let root = store.get_object(&ObjectId::root()).await.unwrap().unwrap();
    assert_eq!(root.path, "/");
    assert_eq!(root.object.parent_id, None);

    assert_eq!(
        store.get_object(&file).await.unwrap().unwrap().path,
        "/reports/2026/example.pdf"
    );
    assert_eq!(
        store
            .resolve_path("/reports/2026/example.pdf")
            .await
            .unwrap()
            .unwrap()
            .object
            .id,
        file
    );
    assert!(store.resolve_path("/reports/nope").await.unwrap().is_none());

    let children = store
        .list_children(ListChildren {
            parent_id: a,
            limit: 100,
            cursor: None,
            order_by: OrderBy::Name,
            order: ListOrder::Asc,
        })
        .await
        .unwrap();
    assert_eq!(children.items.len(), 1, "only direct children are returned");
    assert_eq!(children.items[0].object.id, b);
}

pub async fn listing_paginates_without_gaps_or_repeats<F: MetaStoreFactory>(factory: &F) {
    let store = factory.create().await;
    let (parent, _) = create_folder(&*store, "page", &ObjectId::root(), None)
        .await
        .unwrap();
    for index in 0..7 {
        create_file(&*store, &format!("f{index:02}.txt"), &parent, json!({}))
            .await
            .unwrap();
    }

    let mut seen = Vec::new();
    let mut cursor = None;
    loop {
        let page = store
            .list_children(ListChildren {
                parent_id: parent.clone(),
                limit: 3,
                cursor: cursor.clone(),
                order_by: OrderBy::Name,
                order: ListOrder::Asc,
            })
            .await
            .unwrap();
        seen.extend(page.items.iter().map(|i| i.object.name.clone()));
        if !page.has_more {
            break;
        }
        cursor = page.next_cursor;
    }

    let expected: Vec<String> = (0..7).map(|i| format!("f{i:02}.txt")).collect();
    assert_eq!(seen, expected, "pagination is gapless and repeat-free");
}

/// Runs the whole suite against one implementation.
pub async fn run_all<F: MetaStoreFactory>(factory: &F) {
    new_objects_start_at_revision_one(factory).await;
    live_parent_and_name_are_unique(factory).await;
    deleted_names_can_be_reused(factory).await;
    stale_if_match_never_overwrites(factory).await;
    concurrent_conditional_updates_admit_one_winner(factory).await;
    one_patch_emits_ordered_changes_at_one_revision(factory).await;
    no_op_patch_changes_nothing(factory).await;
    folders_cannot_move_into_themselves(factory).await;
    moving_a_folder_does_not_rewrite_descendants(factory).await;
    recursive_delete_tombstones_every_descendant(factory).await;
    idempotent_replay_returns_the_original_response(factory).await;
    changes_cursor_pages_are_stable(factory).await;
    changes_are_globally_ordered_and_monotonic(factory).await;
    unknown_cursor_is_reported_as_expired(factory).await;
    metadata_eq_and_exists_filter_correctly(factory).await;
    deleted_objects_leave_every_read_path(factory).await;
    content_commit_selects_the_right_action(factory).await;
    upload_creation_resumes_the_existing_record(factory).await;
    resource_token_changes_after_idempotency_retention(factory).await;
    completed_upload_commit_is_convergent(factory).await;
    expired_upload_cannot_commit_content(factory).await;
    purge_changes_reports_the_deleted_row_count(factory).await;
    paths_and_listing_reflect_the_tree(factory).await;
    listing_paginates_without_gaps_or_repeats(factory).await;
}
