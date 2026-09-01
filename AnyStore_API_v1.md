# AnyStore API Contract v1

**Base Path:** `/api/v1`  
**Content-Type:** `application/json` unless otherwise stated.

## 1. Object

```json
{
  "id": "obj_xxx",
  "revision": 1,
  "kind": "file",
  "name": "example.pdf",
  "parent_id": "root",
  "path": "/example.pdf",
  "content_type": "application/pdf",
  "size": 123456,
  "sha256": "hex-string",
  "content_state": "ready",
  "metadata": {
    "key": "any-json-value"
  },
  "created_at": "2026-08-31T15:00:00+08:00",
  "updated_at": "2026-08-31T15:00:00+08:00"
}
```

`kind`: `file | folder`.

`content_state` for files: `none | ready`.

Folder objects omit `content_type`, `size`, `sha256`, `content_state`.

### Root

Fixed id: `root`.

```json
{
  "id": "root",
  "kind": "folder",
  "name": "",
  "parent_id": null,
  "path": "/"
}
```

Root cannot be renamed, moved, or deleted.

### Name

`name` MUST:

- be valid UTF-8;
- be non-empty except for `root`;
- not contain `/` or NUL;
- not equal `.` or `..`;
- be unique under the same `parent_id`.

Duplicate `(parent_id, name)` returns `409 name_conflict`.

### Metadata

`metadata` is an arbitrary JSON object. Values MAY be any valid JSON value. AnyStore stores metadata but does not interpret business meaning.

### Identity and path

- `id` is immutable.
- `path` is derived from `parent_id + name`.
- Rename or move MUST NOT change `id`.
- Rename or move of a folder MUST NOT require rewriting descendants.
- Physical provider, bucket, region, object key, credentials, and permanent storage URI MUST NOT be exposed by the public API.

---

## 2. Revision and conditional update

### 2.1 Revision

- New objects start at `revision = 1`.
- Every successful externally visible mutation increments revision exactly once.
- A PATCH changing multiple fields increments revision once.
- Failed writes and no-op PATCH requests MUST NOT increment revision.
- Deletion increments revision once and emits a tombstone Change.

### 2.2 `If-Match`

Supported on:

- `PATCH /objects/{id}`
- `DELETE /objects/{id}`
- atomic content replacement
- `POST /uploads/{id}/complete`

Header:

```http
If-Match: 7
```

If present, the current object revision MUST equal the supplied revision.

Mismatch:

```http
412 Precondition Failed
```

```json
{
  "error": "revision_conflict",
  "message": "Object has been modified.",
  "current_revision": 8,
  "request_id": "req_xxx"
}
```

If omitted, the write is unconditional.

Idempotency lookup MUST occur before `If-Match` evaluation. A retry of an already completed idempotent request returns the original result even if the object revision has since changed.

---

## 3. Idempotency

All write operations support:

```http
Idempotency-Key: run_01-stage_03-output_02
```

Applies to:

- `POST /objects`
- `POST /uploads`
- `POST /uploads/{id}/parts`
- `POST /uploads/{id}/complete`
- `PATCH /objects/{id}`
- `DELETE /objects/{id}`

Rules:

1. Key scope is the authenticated caller + HTTP operation.
2. Same key + same effective request MUST return the original HTTP status and response body.
3. Retry MUST NOT create another object, upload session, part allocation, revision, mutation, or Change record.
4. Same key + different effective request returns `409 idempotency_key_reused`.
5. Idempotency records MUST be retained for at least **24 hours** after completion.
6. A completed idempotent retry MUST return the original result before normal validation or `If-Match` checks.

Conflict response:

```http
409 Conflict
```

```json
{
  "error": "idempotency_key_reused",
  "message": "Idempotency-Key was already used with a different request.",
  "request_id": "req_xxx"
}
```

---

## 4. Common conventions

### Pagination

```json
{
  "items": [],
  "next_cursor": "opaque-cursor",
  "has_more": false
}
```

Cursors are opaque.

### Errors

```json
{
  "error": "error_code",
  "message": "Human-readable message.",
  "request_id": "req_xxx"
}
```

| HTTP | error |
|---:|---|
| 400 | `invalid_request` |
| 400 | `invalid_name` |
| 400 | `invalid_metadata` |
| 404 | `object_not_found` |
| 404 | `upload_not_found` |
| 409 | `name_conflict` |
| 409 | `folder_not_empty` |
| 409 | `invalid_move` |
| 409 | `not_a_folder` |
| 409 | `not_a_file` |
| 409 | `content_not_ready` |
| 409 | `idempotency_key_reused` |
| 410 | `changes_cursor_expired` |
| 412 | `revision_conflict` |
| 422 | `checksum_mismatch` |
| 429 | `rate_limited` |
| 500 | `internal_error` |
| 502 | `storage_error` |

---

## 5. Object APIs

### 5.1 Create object

```http
POST /api/v1/objects
Idempotency-Key: <key>
```

Folder:

```json
{
  "kind": "folder",
  "name": "reports",
  "parent_id": "root",
  "metadata": {}
}
```

File:

```json
{
  "kind": "file",
  "name": "example.pdf",
  "parent_id": "root",
  "content_type": "application/pdf",
  "metadata": {
    "source": "example"
  }
}
```

Response: `201 Created` with Object. Successful creation emits one `created` Change.

### 5.2 Get object

```http
GET /api/v1/objects/{id}
```

Returns `200` with Object. Deleted objects return `404 object_not_found`.

### 5.3 Patch object

```http
PATCH /api/v1/objects/{id}
If-Match: <revision>
Idempotency-Key: <key>
```

Mutable fields:

- `name`
- `parent_id`
- `metadata`

Metadata patch:

```json
{
  "metadata": {
    "set": {
      "foo": "bar",
      "count": 1
    },
    "remove": ["old_key"]
  }
}
```

Combined patch:

```json
{
  "name": "new-name.pdf",
  "parent_id": "obj_folder",
  "metadata": {
    "set": {
      "state": "ready"
    },
    "remove": []
  }
}
```

Response: `200` with updated Object.

Change emission:

- metadata changed -> `metadata_updated`
- name changed -> `renamed`
- parent changed -> `moved`
- one PATCH may emit multiple Change records; all use the same resulting revision.

No-op PATCH returns the current Object, does not increment revision, and emits no Change.

### 5.4 Delete object

```http
DELETE /api/v1/objects/{id}
If-Match: <revision>
Idempotency-Key: <key>
```

File or empty folder: `204 No Content`.

Non-empty folder without recursive delete:

```http
409 Conflict
```

```json
{
  "error": "folder_not_empty",
  "message": "Folder contains child objects.",
  "request_id": "req_xxx"
}
```

Recursive delete:

```http
DELETE /api/v1/objects/{id}?recursive=true
```

Deletion:

- increments each deleted object's revision once;
- removes it from normal Object/Query/Resolve results;
- emits `deleted` with `tombstone: true`;
- MAY delete physical blobs asynchronously.

Recursive delete MUST emit one tombstone Change per deleted descendant.

### 5.5 List children

```http
GET /api/v1/objects/{id}/children?limit=100&cursor=<cursor>&order_by=name&order=asc
```

`order_by`: `name | created_at | updated_at | size`.

Returns direct children only.

### 5.6 Resolve path

```http
GET /api/v1/resolve?path=/reports/2026/example.pdf
```

Returns the resolved Object or `404 object_not_found`.

---

## 6. Metadata Query

```http
POST /api/v1/query
```

```json
{
  "kind": "file",
  "name": "example.pdf",
  "parent_id": "obj_folder",
  "metadata": {
    "foo": {
      "eq": "bar"
    },
    "year": {
      "exists": true
    }
  },
  "limit": 100,
  "cursor": null
}
```

All supplied conditions are ANDed.

V1 metadata operators:

- `eq`
- `exists`

Deleted objects MUST NOT be returned.

---

## 7. Upload APIs

### 7.1 Create upload session

```http
POST /api/v1/uploads
Idempotency-Key: <key>
```

```json
{
  "object_id": "obj_xxx",
  "size": 123456,
  "content_type": "application/pdf",
  "sha256": "optional-client-hash"
}
```

Target MUST be a file. Creating a session does not change object revision.

Single upload response:

```json
{
  "id": "upload_xxx",
  "object_id": "obj_xxx",
  "mode": "single",
  "upload": {
    "method": "PUT",
    "url": "https://temporary-signed-url",
    "headers": {}
  },
  "expires_at": "..."
}
```

Multipart response:

```json
{
  "id": "upload_xxx",
  "object_id": "obj_xxx",
  "mode": "multipart",
  "part_size": 16777216,
  "expires_at": "..."
}
```

### 7.2 Allocate multipart part URLs

```http
POST /api/v1/uploads/{id}/parts
Idempotency-Key: <key>
```

```json
{
  "part_numbers": [1, 2, 3]
}
```

Response:

```json
{
  "parts": [
    {
      "part_number": 1,
      "method": "PUT",
      "url": "https://..."
    }
  ]
}
```

Idempotent retry MUST return the original allocation result.

### 7.3 Complete upload

```http
POST /api/v1/uploads/{id}/complete
If-Match: <object-revision>
Idempotency-Key: <key>
```

Single body:

```json
{}
```

Multipart body:

```json
{
  "parts": [
    {
      "part_number": 1,
      "etag": "..."
    }
  ]
}
```

Completion MUST:

1. verify uploaded content exists;
2. verify size;
3. verify client SHA-256 when supplied;
4. complete multipart when applicable;
5. atomically replace the object's content pointer;
6. increment object revision exactly once.

Response:

```json
{
  "object_id": "obj_xxx",
  "revision": 2,
  "content_state": "ready",
  "size": 123456,
  "sha256": "..."
}
```

Change action:

- previous `content_state = none` -> `content_ready`
- previous `content_state = ready` -> `content_replaced`

Old content MUST remain readable until successful completion. Failed completion MUST NOT alter object content or revision.

### 7.4 Abort upload

```http
DELETE /api/v1/uploads/{id}
Idempotency-Key: <key>
```

Response: `204 No Content`.

Abort does not change target object revision and emits no Object Change.

---

## 8. Content APIs

### 8.1 Get content

```http
GET /api/v1/objects/{id}/content
```

Recommended ready-file response:

```http
307 Temporary Redirect
Location: <temporary-signed-url>
```

Download MUST preserve the current Object `name` as filename.

Folder -> `409 not_a_file`.  
No ready content -> `409 content_not_ready`.

### 8.2 Head content

```http
HEAD /api/v1/objects/{id}/content
```

Returns at least:

```http
Content-Type: application/pdf
Content-Length: 123456
X-AnyStore-SHA256: <sha256>
```

---

## 9. Changes API

```http
GET /api/v1/changes?cursor=<cursor>&limit=500
```

`limit`: default `100`, maximum `1000`.

Response:

```json
{
  "items": [
    {
      "change_id": "chg_01",
      "object_id": "obj_01",
      "revision": 7,
      "action": "metadata_updated",
      "changed_at": "2026-08-31T15:00:00+08:00",
      "request_id": "req_xxx",
      "idempotency_key": "run_01-output_02"
    },
    {
      "change_id": "chg_02",
      "object_id": "obj_02",
      "revision": 3,
      "action": "deleted",
      "changed_at": "2026-08-31T15:01:00+08:00",
      "request_id": "req_yyy",
      "idempotency_key": null,
      "tombstone": true
    }
  ],
  "next_cursor": "cursor_xxx",
  "has_more": false
}
```

Required `action` values:

- `created`
- `metadata_updated`
- `renamed`
- `moved`
- `content_ready`
- `content_replaced`
- `deleted`

Rules:

1. Change records are append-only and globally ordered.
2. `change_id` is unique and monotonic in Change order.
3. Change records MUST NOT contain file bytes or full metadata.
4. Every successful externally visible object mutation emits the corresponding Change record(s).
5. Idempotent replay MUST NOT emit duplicate Changes.
6. Delete emits `deleted` with `tombstone: true`.
7. One object revision MAY have multiple Change records when one PATCH changes multiple categories.
8. Change records MUST be retained for at least **30 days**.
9. Clients MAY persist cursors.
10. Re-reading the same cursor with the same `limit` MUST return the same page contents and same `next_cursor`, even if newer Changes have been appended.
11. Cursors are opaque and represent stable page/snapshot boundaries.
12. The server MUST return a new `next_cursor` on every successful read, including an empty page, so polling can continue.
13. Cursor outside retained history returns `410 changes_cursor_expired`.
14. Initial read without cursor starts from the oldest retained Change unless deployment documentation explicitly defines another default.

Expired cursor:

```http
410 Gone
```

```json
{
  "error": "changes_cursor_expired",
  "message": "The requested Changes cursor is outside the retention window.",
  "request_id": "req_xxx"
}
```

### Change generation matrix

| Operation | Revision | Change |
|---|---:|---|
| Create file/folder | `1` | `created` |
| PATCH metadata | `+1` | `metadata_updated` |
| PATCH name | `+1` | `renamed` |
| PATCH parent | `+1` | `moved` |
| PATCH name + parent + metadata | `+1` once | 3 records, same revision |
| First upload complete | `+1` | `content_ready` |
| Replace existing content | `+1` | `content_replaced` |
| Create upload session | unchanged | none |
| Allocate upload parts | unchanged | none |
| Abort upload | unchanged | none |
| Delete | `+1` | `deleted`, tombstone |

---

## 10. V2 Full-text Search

Only new path:

```http
POST /api/v1/search
```

```json
{
  "query": "hello world",
  "limit": 20,
  "cursor": null
}
```

```json
{
  "items": [
    {
      "object_id": "obj_xxx",
      "name": "example.md",
      "path": "/notes/example.md",
      "score": 8.42,
      "matches": [
        {
          "text": "...hello world..."
        }
      ]
    }
  ],
  "next_cursor": null,
  "has_more": false
}
```

`/query` remains metadata/system-field query. `/search` is content full-text search only.

Search index is derived data and MUST be rebuildable from AnyStore objects.
