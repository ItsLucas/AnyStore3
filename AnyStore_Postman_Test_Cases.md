# AnyStore Postman Contract Tests

**Base variable:** `{{baseUrl}}`  
Example: `http://localhost:8080/api/v1`

## Variables

| Variable | Purpose |
|---|---|
| `baseUrl` | API base URL |
| `folderId` | created folder id |
| `folderRevision` | current folder revision |
| `fileId` | created file id |
| `fileRevision` | current file revision |
| `oldRevision` | stale revision |
| `folder2Id` | second folder |
| `uploadId` | upload session id |
| `changesCursor` | Changes cursor |

---

## Object / Idempotency / Revision

### T01 Create folder

```http
POST {{baseUrl}}/objects
Idempotency-Key: test-create-folder-001
Content-Type: application/json
```

```json
{
  "kind": "folder",
  "name": "postman-folder",
  "parent_id": "root",
  "metadata": {"suite": "postman"}
}
```

Expect:

- `201`
- `revision == 1`
- `kind == folder`
- `path == /postman-folder`
- save `id -> folderId`, `revision -> folderRevision`

### T02 Create folder idempotent retry

Repeat T01 exactly.

Expect:

- same HTTP status and response body
- same `id`
- no second object
- no second `created` Change

### T03 Idempotency-Key reused with different request

Use T01 key with a different body.

Expect:

- `409`
- `error == idempotency_key_reused`

### T04 Create file

```http
POST {{baseUrl}}/objects
Idempotency-Key: test-create-file-001
```

```json
{
  "kind": "file",
  "name": "example.txt",
  "parent_id": "{{folderId}}",
  "content_type": "text/plain",
  "metadata": {
    "suite": "postman",
    "version": 1
  }
}
```

Expect:

- `201`
- `revision == 1`
- `content_state == none`
- save `id -> fileId`, `revision -> fileRevision`

### T05 Duplicate name conflict

Create another object with same `parent_id + name`, new Idempotency-Key.

Expect:

- `409`
- `error == name_conflict`

### T06 Get object

```http
GET {{baseUrl}}/objects/{{fileId}}
```

Expect `200`, correct id and revision.

### T07 Conditional metadata update

Save current revision as `oldRevision`.

```http
PATCH {{baseUrl}}/objects/{{fileId}}
If-Match: {{fileRevision}}
Idempotency-Key: test-patch-metadata-001
```

```json
{
  "metadata": {
    "set": {
      "version": 2,
      "new_key": true
    },
    "remove": []
  }
}
```

Expect:

- `200`
- revision increases exactly once
- metadata changed
- one `metadata_updated` Change
- update `fileRevision`

### T08 Stale revision conflict

```http
PATCH {{baseUrl}}/objects/{{fileId}}
If-Match: {{oldRevision}}
Idempotency-Key: test-patch-stale-001
```

Expect:

- `412`
- `error == revision_conflict`
- `current_revision == fileRevision`
- object unchanged
- no new Change

### T09 Idempotent PATCH retry before revision validation

Repeat T07 exactly, including original old `If-Match` and Idempotency-Key.

Expect:

- original successful response
- NOT `412`
- no revision increment
- no duplicate Change

### T10 Combined rename + move + metadata PATCH

Create a second folder and save as `folder2Id`.

```http
PATCH {{baseUrl}}/objects/{{fileId}}
If-Match: {{fileRevision}}
Idempotency-Key: test-patch-combined-001
```

```json
{
  "name": "renamed.txt",
  "parent_id": "{{folder2Id}}",
  "metadata": {
    "set": {"combined": true},
    "remove": []
  }
}
```

Expect:

- `200`
- revision increases exactly once
- name, parent, metadata all changed
- exactly three Changes for this request: `renamed`, `moved`, `metadata_updated`
- all three use the same resulting revision and request id

### T11 No-op PATCH

PATCH current values to the same values.

Expect:

- `200`
- revision unchanged
- no Change

### T12 Resolve path

```http
GET {{baseUrl}}/resolve?path=/<folder2-name>/renamed.txt
```

Expect `200`, `id == fileId`.

### T13 List children

```http
GET {{baseUrl}}/objects/{{folder2Id}}/children?limit=100&order_by=name&order=asc
```

Expect:

- `200`
- file appears once
- only direct children returned

### T14 Metadata query

```http
POST {{baseUrl}}/query
```

```json
{
  "kind": "file",
  "metadata": {
    "combined": {"eq": true},
    "new_key": {"exists": true}
  },
  "limit": 100,
  "cursor": null
}
```

Expect `fileId` returned once.

---

## Upload

### T15 Create upload session

```http
POST {{baseUrl}}/uploads
Idempotency-Key: test-upload-create-001
```

```json
{
  "object_id": "{{fileId}}",
  "size": 11,
  "content_type": "text/plain",
  "sha256": "<sha256-of-test-content>"
}
```

Expect:

- `200` or `201` according to implementation convention
- upload id returned
- target object revision unchanged
- save `id -> uploadId`

### T16 Upload session idempotent retry

Repeat T15 exactly.

Expect same upload id and same original response.

### T17 Multipart part allocation idempotency

For multipart mode:

```http
POST {{baseUrl}}/uploads/{{uploadId}}/parts
Idempotency-Key: test-upload-parts-001
```

```json
{"part_numbers": [1, 2]}
```

Repeat exactly.

Expect identical original allocation result.

### T18 First upload complete

After bytes are uploaded to signed URL:

```http
POST {{baseUrl}}/uploads/{{uploadId}}/complete
If-Match: {{fileRevision}}
Idempotency-Key: test-upload-complete-001
```

Expect:

- `200`
- revision +1
- `content_state == ready`
- one `content_ready` Change
- update `fileRevision`

### T19 Upload complete idempotent retry

Repeat T18 exactly with old `If-Match`.

Expect:

- original success result
- NOT `412`
- no revision increment
- no duplicate `content_ready`

### T20 Stale revision during replacement

Create replacement upload session, upload bytes, then mutate the target object before completion. Complete using the stale revision and a new Idempotency-Key.

Expect:

- `412 revision_conflict`
- existing readable content unchanged
- object revision unchanged by failed completion
- no `content_replaced` Change

### T21 Successful atomic replacement

Create a fresh replacement upload and complete with current revision.

Expect:

- `200`
- revision +1
- `content_replaced`
- id/name/metadata unchanged
- old content remained readable until completion succeeded

### T22 HEAD content

```http
HEAD {{baseUrl}}/objects/{{fileId}}/content
```

Expect:

- `200`
- Content-Length equals object size
- X-AnyStore-SHA256 equals object SHA-256

### T23 GET content

```http
GET {{baseUrl}}/objects/{{fileId}}/content
```

Expect:

- `307` when redirect design is used
- temporary signed location
- download filename equals current Object name

---

## Changes

### T24 Initial Changes read

```http
GET {{baseUrl}}/changes?limit=500
```

Expect:

- `200`
- global append order
- unique monotonic `change_id`
- no file bytes
- no full metadata payload
- save `next_cursor -> changesCursor`

### T25 Idempotency does not duplicate Changes

Inspect create and upload retry operations.

Expect exactly one logical Change for each first successful mutation.

### T26 Combined PATCH Change semantics

For T10, expect exactly:

- one `renamed`
- one `moved`
- one `metadata_updated`

All share object id, resulting revision, request id, and T10 Idempotency-Key.

### T27 Cursor stability

1. Read `GET /changes?cursor={{changesCursor}}&limit=10`; save entire response A.
2. Create or modify another object.
3. Repeat the exact same GET.

Expect response B equals A: same items, order, and next_cursor.

### T28 Cursor continuation

Read using T27 `next_cursor`.

Expect:

- newer Changes can eventually be observed
- no replay before cursor
- no gaps within retained history

---

## Delete

### T29 Delete stale revision

```http
DELETE {{baseUrl}}/objects/{{fileId}}
If-Match: {{oldRevision}}
Idempotency-Key: test-delete-stale-001
```

Expect:

- `412`
- object still exists
- no tombstone

### T30 Delete success

```http
DELETE {{baseUrl}}/objects/{{fileId}}
If-Match: {{fileRevision}}
Idempotency-Key: test-delete-001
```

Expect:

- `204`
- GET -> `404`
- Query/Resolve do not return object
- exactly one `deleted` Change
- `tombstone == true`
- tombstone revision == previous revision + 1

### T31 Delete idempotent retry

Repeat T30 exactly.

Expect:

- original `204`
- NOT `404` or `412`
- no second tombstone
- no second revision increment

### T32 Reuse delete Idempotency-Key on another target

Expect `409 idempotency_key_reused`.

### T33 Delete non-empty folder

Expect:

- `409 folder_not_empty`
- no revision increment
- no Change

### T34 Recursive folder delete

```http
DELETE {{baseUrl}}/objects/{{folderId}}?recursive=true
If-Match: {{folderRevision}}
Idempotency-Key: test-delete-folder-recursive-001
```

Expect:

- `204`
- folder and descendants unavailable
- one tombstone Change per deleted object

---

## Invalid move

### T35 Move folder into itself

Expect `409 invalid_move`, no revision change, no Change.

### T36 Move folder into descendant

Create `A/B`, then PATCH `A.parent_id = B`.

Expect `409 invalid_move`, no revision change, no Change.

---

## Retention

### T37 Expired Changes cursor

Use a cursor older than configured retention.

Expect:

- `410`
- `error == changes_cursor_expired`

A test deployment MAY use a shortened retention value to exercise this case.

---

## Acceptance boundary

Implementation passes only if:

1. New Objects start at revision 1.
2. Each successful externally visible mutation increments revision exactly once.
3. Stale `If-Match` never overwrites newer state.
4. Idempotent retry returns original result before conflict validation.
5. Same Idempotency-Key with different request fails.
6. Retry never creates duplicate objects, sessions, revisions, or Changes.
7. Upload completion is atomic.
8. Failed replacement never destroys current readable content.
9. Rename/move never changes Object id.
10. Folder move does not rewrite descendants.
11. Deleted objects disappear from normal reads and queries.
12. Every deletion leaves a tombstone Change.
13. Changes are append-only and globally ordered.
14. Same Changes cursor + same limit returns a stable page.
15. Changes never contain file bytes or full metadata.
16. Expired cursors fail explicitly.
