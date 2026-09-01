# AnyStore Architecture v1

**Status:** Implementation Contract  
**Companion document:** `AnyStore_API_v1.md`

## 1. Scope

AnyStore v1 is a generic file/object service with:

- files and folders;
- arbitrary JSON metadata;
- metadata query;
- direct-to-blob upload/download;
- revision-based conditional writes;
- idempotent writes;
- ordered Changes feed.

The core MUST NOT contain business-specific entities or fields.

V1 durable state consists of:

1. metadata/state in a database;
2. file bytes in a blob store.

Both MUST be accessed through internal abstraction layers. HTTP handlers MUST NOT contain provider-specific database or blob-storage code.

---

## 2. Runtime topology

Production reference deployment:

```text
Client
  |
  | HTTPS / JSON
  v
CloudBase Run
Rust + Axum
  |
  +--------------------------+
  |                          |
  | SQL                      | provider API / signing
  v                          v
CloudBase PostgreSQL       LightCOS
metadata/state             immutable file blobs
```

Large file traffic MUST bypass the application container:

```text
Client  ==================>  LightCOS
          upload bytes

Client  <==================  LightCOS
          download bytes
```

The AnyStore service only creates upload sessions, signs access, validates completion, and commits metadata.

The service MUST be stateless. Local container disk MAY be used only for temporary files/caches and MUST NOT be required for correctness.

---

## 3. Technology baseline

Reference implementation:

- Language: Rust stable
- HTTP: Axum
- Async runtime: Tokio
- Serialization: Serde / serde_json
- Database adapter: SQLx PostgreSQL
- HTTP client for provider adapters: Reqwest
- XML parsing when required by COS API: quick-xml
- IDs: ULID or UUIDv7; API-visible prefixes are added by the domain layer
- Logging/tracing: tracing
- Error types: thiserror

Recommended workspace:

```text
anystore/
├── Cargo.toml
├── crates/
│   ├── anystore-domain/
│   ├── anystore-application/
│   ├── anystore-http/
│   ├── anystore-metastore/
│   ├── anystore-metastore-postgres/
│   ├── anystore-blobstore/
│   ├── anystore-blobstore-cos/
│   └── anystore-testkit/
└── migrations/
```

A single-crate implementation MAY be used initially, but the same module boundaries MUST be preserved.

---

## 4. Dependency rule

Dependencies point inward:

```text
HTTP Adapter
    |
    v
Application Service
    |
    v
Domain
    ^
    |
Ports / Traits
   ^     ^
   |     |
Postgres COS
Adapters
```

Rules:

- `domain` MUST NOT depend on Axum, SQLx, Tencent SDKs, COS, PostgreSQL, or CloudBase.
- `application` MAY depend on domain traits, but MUST NOT depend on concrete providers.
- HTTP handlers MUST call application services only.
- SQL MUST exist only inside database adapters.
- COS/S3/OSS-specific request signing and APIs MUST exist only inside blob adapters.

---

## 5. Core domain types

Minimum domain types:

```rust
struct ObjectId(String);
struct UploadId(String);
struct ChangeId(String);
struct Revision(u64);

#[derive(Clone, Copy, PartialEq, Eq)]
enum ObjectKind {
    File,
    Folder,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ContentState {
    None,
    Ready,
}

struct Object {
    id: ObjectId,
    revision: Revision,
    kind: ObjectKind,
    name: String,
    parent_id: Option<ObjectId>,
    content_type: Option<String>,
    size: Option<u64>,
    sha256: Option<String>,
    content_state: Option<ContentState>,
    metadata: serde_json::Value,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}
```

`path` is NOT stored as authoritative state. It is derived from the folder tree.

The public root object uses id `root`.

---

## 6. Database abstraction

### 6.1 Boundary

The database abstraction is called `MetaStore`.

It abstracts AnyStore persistence semantics, NOT SQL syntax. The application layer MUST NOT issue SQL directly.

Do not design a generic `execute_sql()` abstraction. Each backend adapter implements AnyStore operations using the backend's native transaction and query features.

Recommended port split:

```rust
#[async_trait]
pub trait ObjectRepository: Send + Sync {
    async fn get_object(&self, id: &ObjectId) -> Result<Option<Object>>;
    async fn list_children(&self, q: ListChildren) -> Result<Page<Object>>;
    async fn resolve_path(&self, path: &str) -> Result<Option<Object>>;
    async fn query_objects(&self, q: ObjectQuery) -> Result<Page<Object>>;
}

#[async_trait]
pub trait ObjectMutationStore: Send + Sync {
    async fn create_object(&self, cmd: CreateObjectCommit) -> Result<StoredResponse>;
    async fn patch_object(&self, cmd: PatchObjectCommit) -> Result<StoredResponse>;
    async fn delete_object(&self, cmd: DeleteObjectCommit) -> Result<StoredResponse>;
    async fn commit_content(&self, cmd: CommitContent) -> Result<StoredResponse>;
}

#[async_trait]
pub trait UploadRepository: Send + Sync {
    async fn create_upload_record(&self, cmd: CreateUploadRecord) -> Result<UploadRecord>;
    async fn get_upload(&self, id: &UploadId) -> Result<Option<UploadRecord>>;
    async fn update_upload_state(&self, cmd: UpdateUploadState) -> Result<()>;
    async fn abort_upload_record(&self, cmd: AbortUploadRecord) -> Result<StoredResponse>;
}

#[async_trait]
pub trait IdempotencyStore: Send + Sync {
    async fn acquire(&self, req: IdempotencyAcquire) -> Result<IdempotencyDecision>;
    async fn complete(&self, req: IdempotencyComplete) -> Result<()>;
    async fn fail_or_release(&self, req: IdempotencyRelease) -> Result<()>;
}

#[async_trait]
pub trait ChangeStore: Send + Sync {
    async fn read_changes(&self, req: ReadChanges) -> Result<ChangePage>;
}
```

Production implementation:

```text
PostgresMetaStore
```

Optional future implementations:

```text
MySqlMetaStore
SqliteMetaStore
InMemoryMetaStore   // tests only
```

The application MUST depend on traits, not `PostgresMetaStore`.

### 6.2 Atomicity requirement

The following MUST be one database transaction inside the database adapter:

```text
revision check
+ object mutation
+ Change append
+ idempotency result finalization
```

For example, PATCH success is logically:

```text
BEGIN
  validate idempotency ownership/result
  UPDATE object WHERE id=? AND revision=?
  INSERT change(s)
  save original response for Idempotency-Key
COMMIT
```

This is the primary reason the database abstraction is domain-oriented instead of exposing generic CRUD primitives.

---

## 7. PostgreSQL reference schema

The exact migration syntax MAY evolve, but the following logical schema is normative.

### 7.1 `objects`

```sql
CREATE TABLE objects (
    id              TEXT PRIMARY KEY,
    revision        BIGINT NOT NULL,
    kind            TEXT NOT NULL CHECK (kind IN ('file', 'folder')),
    name            TEXT NOT NULL,
    parent_id       TEXT NULL REFERENCES objects(id),

    content_type    TEXT NULL,
    size_bytes      BIGINT NULL,
    sha256          TEXT NULL,
    content_state   TEXT NULL CHECK (content_state IN ('none', 'ready')),
    blob_ref        TEXT NULL,

    metadata        JSONB NOT NULL DEFAULT '{}'::jsonb,

    created_at      TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL,
    deleted_at      TIMESTAMPTZ NULL
);
```

Required root row:

```text
id = 'root'
kind = 'folder'
name = ''
parent_id = NULL
revision = 1
```

Required uniqueness for live objects:

```sql
CREATE UNIQUE INDEX objects_live_parent_name_uq
ON objects(parent_id, name)
WHERE deleted_at IS NULL;
```

Required indexes:

```sql
CREATE INDEX objects_live_parent_idx
ON objects(parent_id)
WHERE deleted_at IS NULL;

CREATE INDEX objects_metadata_gin_idx
ON objects USING GIN(metadata);
```

`blob_ref` is internal and MUST NOT be returned by public APIs.

### 7.2 `uploads`

```sql
CREATE TABLE uploads (
    id                  TEXT PRIMARY KEY,
    object_id           TEXT NOT NULL REFERENCES objects(id),
    state               TEXT NOT NULL,
    mode                TEXT NOT NULL CHECK (mode IN ('single', 'multipart')),

    blob_backend        TEXT NOT NULL,
    blob_ref            TEXT NOT NULL,
    provider_upload_id  TEXT NULL,

    expected_size       BIGINT NOT NULL,
    content_type        TEXT NOT NULL,
    expected_sha256     TEXT NULL,

    provider_completed  BOOLEAN NOT NULL DEFAULT FALSE,
    created_at          TIMESTAMPTZ NOT NULL,
    expires_at          TIMESTAMPTZ NOT NULL,
    completed_at        TIMESTAMPTZ NULL,
    aborted_at          TIMESTAMPTZ NULL
);
```

Upload state minimum set:

```text
initiating
ready
completing
completed
aborted
expired
```

### 7.3 `idempotency_records`

```sql
CREATE TABLE idempotency_records (
    principal_id        TEXT NOT NULL,
    operation           TEXT NOT NULL,
    idempotency_key     TEXT NOT NULL,

    request_hash        TEXT NOT NULL,
    state               TEXT NOT NULL CHECK (state IN ('in_progress', 'completed')),
    owner_token         TEXT NULL,
    lease_until         TIMESTAMPTZ NULL,

    status_code         INTEGER NULL,
    response_headers    JSONB NULL,
    response_body       BYTEA NULL,

    created_at          TIMESTAMPTZ NOT NULL,
    completed_at        TIMESTAMPTZ NULL,
    expires_at          TIMESTAMPTZ NOT NULL,

    PRIMARY KEY (principal_id, operation, idempotency_key)
);
```

Retention MUST satisfy the API contract: at least 24 hours after completion.

When authentication is disabled, use the fixed principal id:

```text
local
```

### 7.4 `changes`

```sql
CREATE TABLE changes (
    seq                 BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    change_id           TEXT NOT NULL UNIQUE,
    object_id           TEXT NOT NULL,
    revision            BIGINT NOT NULL,
    action              TEXT NOT NULL,
    changed_at          TIMESTAMPTZ NOT NULL,
    request_id          TEXT NOT NULL,
    idempotency_key     TEXT NULL,
    tombstone           BOOLEAN NOT NULL DEFAULT FALSE
);
```

Required action values are defined by the API contract.

`seq` is the internal global ordering key. `change_id` is API-visible identity.

Changes are append-only.

### 7.5 `change_cursors`

Stable cursor semantics require persisted cursor state.

```sql
CREATE TABLE change_cursors (
    cursor_id           TEXT PRIMARY KEY,
    after_seq           BIGINT NOT NULL,

    materialized        BOOLEAN NOT NULL DEFAULT FALSE,
    page_limit          INTEGER NULL,
    snapshot_max_seq    BIGINT NULL,
    page_end_seq        BIGINT NULL,
    has_more            BOOLEAN NULL,
    next_cursor_id      TEXT NULL,

    created_at          TIMESTAMPTZ NOT NULL,
    expires_at          TIMESTAMPTZ NOT NULL
);
```

A cursor returned as `next_cursor` MAY initially be unmaterialized.

On its first read:

1. atomically bind the requested `limit`;
2. snapshot the current maximum Change `seq`;
3. determine the page bounded by `(after_seq, snapshot_max_seq]`;
4. persist page boundary and `next_cursor_id`.

Later reads using the same cursor and same limit MUST reuse the materialized boundary and therefore return the same page even when new Changes have been appended.

A different `limit` for an already-materialized cursor SHOULD return `400 invalid_request`.

An empty page still receives a fresh unmaterialized `next_cursor`, allowing the client to poll later without changing the result of the old cursor.

### 7.6 `blob_gc_queue`

```sql
CREATE TABLE blob_gc_queue (
    blob_backend    TEXT NOT NULL,
    blob_ref        TEXT NOT NULL,
    not_before      TIMESTAMPTZ NOT NULL,
    attempts        INTEGER NOT NULL DEFAULT 0,
    last_error      TEXT NULL,
    PRIMARY KEY (blob_backend, blob_ref)
);
```

Old blobs from content replacement and deletion are enqueued only after the database pointer change is committed.

GC failure MUST NOT roll back a successful Object mutation.

---

## 8. Blob storage abstraction

### 8.1 Boundary

The blob abstraction is called `BlobStore`.

The application MUST use opaque `BlobRef` values. It MUST NOT know COS bucket names, object keys, regions, provider upload ids, signing formats, or credentials.

Recommended interface:

```rust
#[async_trait]
pub trait BlobStore: Send + Sync {
    fn backend_id(&self) -> &'static str;

    async fn prepare_upload(
        &self,
        req: PrepareUpload,
    ) -> Result<PreparedUpload>;

    async fn sign_parts(
        &self,
        req: SignParts,
    ) -> Result<Vec<SignedPart>>;

    async fn ensure_upload_completed(
        &self,
        req: CompleteBlobUpload,
    ) -> Result<BlobStat>;

    async fn abort_upload(
        &self,
        req: AbortBlobUpload,
    ) -> Result<()>;

    async fn stat(
        &self,
        blob: &BlobRef,
    ) -> Result<BlobStat>;

    async fn sign_download(
        &self,
        blob: &BlobRef,
        filename: &str,
        expires_in: Duration,
    ) -> Result<SignedDownload>;

    async fn delete_blob(
        &self,
        blob: &BlobRef,
    ) -> Result<()>;
}
```

Production implementation:

```text
TencentCosBlobStore
```

Optional implementations:

```text
S3BlobStore
OssBlobStore
LocalFsBlobStore
InMemoryBlobStore
```

### 8.2 Blob identity

Every upload session receives a fresh immutable blob location.

Recommended logical provider key:

```text
blobs/{upload_id}
```

Do NOT encode the logical folder path or file name into the permanent provider key.

Moving or renaming an AnyStore object therefore does not perform any blob operation.

Content replacement writes a new blob and atomically changes only the database `blob_ref`.

### 8.3 Provider adapter conformance

Every `BlobStore` implementation MUST pass the same tests:

- prepare single upload;
- prepare multipart upload;
- upload part signing;
- complete multipart;
- repeat/recover completion when the provider upload is already finalized;
- stat;
- signed download;
- abort multipart;
- delete blob;
- filenames with UTF-8;
- zero-byte object;
- provider error mapping.

A Tencent adapter MAY use direct COS XML API, a compatible S3 SDK, or a maintained SDK internally. This choice MUST NOT escape the adapter boundary.

---

## 9. Application services

HTTP routes map to application commands.

Recommended services:

```text
ObjectService
UploadService
QueryService
ChangeService
ContentService
MaintenanceService
```

### `ObjectService`

Owns:

- object creation;
- get/list/resolve;
- rename/move;
- metadata patch;
- delete;
- revision validation semantics;
- folder cycle checks.

### `UploadService`

Owns:

- upload-session lifecycle;
- direct-upload plans;
- multipart plans;
- upload completion;
- content pointer replacement;
- abandoned upload cleanup.

### `ChangeService`

Owns:

- `/changes` cursor behavior only.

### `MaintenanceService`

Internal only; no public API contract.

Owns:

- stale multipart abort;
- orphan blob deletion;
- expired idempotency cleanup;
- expired Change cursor cleanup;
- Change retention cleanup.

It MAY run opportunistically after normal requests and/or through a scheduled serverless invocation.

---

## 10. Request processing pipeline

Recommended middleware order:

```text
Request ID
  -> Authentication / principal resolution
  -> Request size / JSON validation
  -> Idempotency context extraction
  -> Handler
  -> Error mapping
  -> Structured tracing
```

`If-Match` is parsed by the handler/application command, not by provider adapters.

Every request gets a `request_id` before any database or blob call.

---

## 11. Idempotency algorithm

### 11.1 Request hash

Canonical idempotency input MUST include:

- authenticated principal id;
- HTTP method;
- route template / target resource id;
- normalized relevant query parameters;
- request body;
- `If-Match` when present.

`Idempotency-Key` itself is excluded from the hash.

Hash algorithm:

```text
SHA-256(canonical request representation)
```

### 11.2 Acquire

For a new key:

1. insert `in_progress` record with request hash, owner token, lease expiry;
2. caller becomes owner.

For an existing key:

- different request hash -> `409 idempotency_key_reused`;
- `completed` -> return stored status/body immediately;
- `in_progress` with valid lease -> wait/poll briefly for completion;
- `in_progress` with expired lease -> atomically take ownership and resume.

The lease prevents a crashed serverless instance from permanently blocking the key.

### 11.3 Completion

For pure database mutations, object mutation + Change append + idempotency response completion MUST commit in one transaction.

For workflows that include an external blob call, the application first obtains idempotency ownership, then executes a resumable state machine. The final object commit and idempotency response completion MUST still be one database transaction.

A completed retry MUST be served from `idempotency_records` before `If-Match` or current-state validation.

---

## 12. Revision algorithm

### PATCH

Reference PostgreSQL mutation pattern:

```sql
UPDATE objects
SET
    revision = revision + 1,
    ...,
    updated_at = now()
WHERE id = $id
  AND deleted_at IS NULL
  AND ($if_match IS NULL OR revision = $if_match)
RETURNING *;
```

If no row is updated:

1. read current object;
2. not found/deleted -> `404`;
3. revision differs -> `412 revision_conflict`.

A no-op PATCH MUST be detected before UPDATE and MUST NOT increment revision.

### DELETE

Soft-delete in the metadata store:

```text
revision += 1
deleted_at = now()
```

Normal reads exclude `deleted_at IS NOT NULL`.

The final revision is retained for the tombstone Change.

---

## 13. Folder semantics

### Create

Parent MUST exist, be live, and have `kind=folder`.

### Move

Before changing `parent_id`:

- destination MUST be a live folder;
- object MUST NOT be root;
- folder MUST NOT be moved under itself;
- folder MUST NOT be moved under any descendant;
- destination `(parent_id, name)` MUST not conflict.

Cycle detection is performed inside the same database transaction as the move.

PostgreSQL MAY use a recursive CTE walking from destination toward root.

### Resolve path

`/resolve` SHOULD split the normalized path into segments and resolve each segment with indexed `(parent_id, name)` lookup.

Do not store a materialized authoritative full path in every descendant.

### Render path

To return `path`, walk parents to root using a recursive CTE or adapter-specific equivalent.

---

## 14. Metadata semantics

Metadata is stored as JSON/JSONB.

V1 query semantics are top-level keys only.

Examples:

```text
eq(key, JSON value)
exists(key)
```

PostgreSQL mapping:

```text
exists -> metadata ? $key
eq     -> metadata -> $key = $json_value
```

All query conditions are ANDed as specified by the API contract.

The application layer MUST NOT hard-code metadata key names.

---

## 15. Upload state machine

### 15.1 Create session

```text
POST /uploads
    |
    v
idempotency acquire
    |
    v
create DB upload row
    |
    v
BlobStore.prepare_upload
    |
    v
persist provider state + original response
    |
    v
return upload plan
```

Creating an upload does not change Object revision.

An orphan provider multipart created during a process crash is acceptable internal garbage and MUST be cleaned later. It MUST NOT create a second AnyStore Object or visible content version.

### 15.2 Direct upload

Client uploads bytes to the signed provider URL.

AnyStore does not proxy the body.

### 15.3 Complete session

```text
POST /uploads/{id}/complete
        |
        v
idempotency acquire
        |
        v
load upload + target Object
        |
        v
BlobStore.ensure_upload_completed
        |
        v
BlobStore.stat / validation
        |
        v
DB transaction:
  - If-Match revision check
  - swap object.blob_ref
  - set size/content_type/hash/state
  - revision += 1
  - append content_ready or content_replaced
  - enqueue previous blob for GC
  - persist idempotent response
        |
        v
return success
```

The new provider blob MUST be fully readable before the database pointer is swapped.

If the database commit fails, the new blob is orphaned and later GC'd. The old Object content remains valid.

If the database commit succeeds, the old blob MAY be deleted later; deletion is never on the critical atomic path.

### 15.4 Completion recovery

`BlobStore.ensure_upload_completed` MUST be safe to retry at the AnyStore abstraction level.

For multipart providers, if provider completion was already performed but the process crashed before database commit, the adapter SHOULD detect an existing finalized blob using `stat` and continue instead of treating the operation as a new upload.

### 15.5 SHA-256

If the client supplied `sha256`, the upload workflow MUST preserve it and validate it when the selected blob backend exposes a trustworthy equivalent verification mechanism.

If the backend cannot verify SHA-256 without downloading the complete blob, the adapter MUST NOT download large content through the application solely to compute it. In that case the stored SHA-256 is client-declared unless a provider-native verification path is available.

Provider checksums/ETags MAY be stored internally but are not part of the public v1 contract.

---

## 16. Changes implementation

Change append MUST occur in the same transaction as the object revision mutation.

One PATCH MAY append multiple Change rows sharing:

```text
object_id
revision
request_id
idempotency_key
changed_at
```

Ordering between multiple Change rows from one mutation MUST be deterministic. Recommended order:

```text
renamed
moved
metadata_updated
```

Content mutations emit exactly one of:

```text
content_ready
content_replaced
```

Delete emits `deleted` with `tombstone=true`.

Change records MUST NOT contain blob bytes or complete metadata.

Retention:

```text
minimum 30 days
```

Cleanup MUST retain enough information to return `410 changes_cursor_expired` for a cursor whose starting position is older than retained history.

---

## 17. Blob garbage collection

A blob becomes GC-eligible when:

- content is replaced and no live object points to the old blob;
- an object is deleted;
- an upload was abandoned/expired;
- an external provider operation succeeded but the corresponding database commit never happened.

Rules:

- never delete a blob referenced by a live Object;
- GC MUST be idempotent;
- provider `not found` on deletion counts as success;
- exponential retry SHOULD be used for transient provider errors;
- GC failures MUST NOT affect API correctness.

A conservative grace period SHOULD be used before deleting old committed content.

---

## 18. Provider registry and configuration

The application receives implementations at startup:

```rust
struct AppState<M, B> {
    meta: Arc<M>,
    blobs: Arc<BlobRegistry<B>>,
}
```

A deployment MAY initially configure one blob backend only.

Internal objects/uploads still record a `blob_backend` identifier so future migration/multi-backend support does not require changing public Object IDs.

Example configuration:

```text
ANYSTORE_DATABASE_BACKEND=postgres
ANYSTORE_DATABASE_URL=...

ANYSTORE_BLOB_BACKEND=tencent_cos
ANYSTORE_COS_BUCKET=...
ANYSTORE_COS_ENDPOINT=...
ANYSTORE_COS_SECRET_ID=...
ANYSTORE_COS_SECRET_KEY=...

ANYSTORE_IDEMPOTENCY_RETENTION_HOURS=24
ANYSTORE_CHANGES_RETENTION_DAYS=30
ANYSTORE_UPLOAD_EXPIRY_HOURS=24
ANYSTORE_DOWNLOAD_URL_EXPIRY_SECONDS=600
```

Secrets MUST come from environment/secret management and MUST NOT be returned by APIs or logged.

---

## 19. Security requirements

Minimum production requirements:

- blob bucket/container is private;
- clients never receive permanent cloud credentials;
- upload/download access uses short-lived signed URLs;
- database is not publicly exposed unless operationally required;
- database connections use TLS when supported;
- application logs MUST redact authorization headers, cloud credentials, signed URL query signatures, and database passwords;
- SQL parameters MUST use bound parameters only;
- metadata and filenames are untrusted user input;
- path construction MUST never map directly to local filesystem paths in production blob adapters.

Authentication is deployment-specific. If disabled, all requests use principal `local` for idempotency scope.

---

## 20. Serverless requirements

The service MUST remain correct when:

- an instance is terminated after any request;
- zero instances are running;
- multiple instances process requests concurrently;
- requests are retried on another instance;
- local disk contents disappear;
- requests arrive after a cold start.

Therefore:

- all durable state is external;
- no in-memory lock is used for correctness;
- no local SQLite file is used as production truth in CloudBase Run;
- DB connection pools are per instance and small;
- idempotency/revision semantics are database-backed.

Recommended initial pool per container:

```text
min_connections = 0
max_connections = 5
```

Tune only after measurement.

---

## 21. Failure model

### Database unavailable

Return mapped service error; no Object mutation is considered committed.

### Blob provider unavailable during upload creation

Upload creation may remain resumable/in-progress; idempotent retry resumes or returns the stored result.

### Blob provider unavailable during completion

Do not change Object content pointer or revision.

### Blob complete succeeds, DB commit fails

New blob becomes orphan; old Object content remains current. Retry may reuse the finalized blob. GC eventually removes unused blob.

### DB commit succeeds, old blob delete fails

API success remains valid. Old blob remains in GC queue.

### Client loses successful response

Retry with the same `Idempotency-Key`; return original result.

### Two clients update same revision

Exactly one conditional mutation may succeed. The other receives `412 revision_conflict`.

---

## 22. Observability

Every log/event SHOULD contain where applicable:

```text
request_id
object_id
upload_id
revision
idempotency_key hash/prefix (not secret content if treated sensitive)
blob_backend
operation
latency_ms
result/error
```

Metrics minimum set:

```text
http_requests_total
http_request_duration
revision_conflicts_total
idempotency_hits_total
idempotency_conflicts_total
upload_sessions_total
upload_complete_failures_total
blob_provider_errors_total
db_errors_total
changes_lag
blob_gc_pending
```

Logs MUST NOT include file content or full signed URLs.

---

## 23. Database migrations

Migrations are versioned and shipped with the binary/repository.

Reference implementation SHOULD use SQLx migrations.

Rules:

- migration execution is explicit at deploy/startup policy level;
- schema version is checked before serving traffic;
- destructive migrations require a backup;
- provider-independent domain behavior must be covered by contract tests before switching database adapters.

---

## 24. Test architecture

Three levels are required.

### 24.1 Domain/application unit tests

Use `InMemoryMetaStore` and `InMemoryBlobStore`.

Test:

- name validation;
- metadata patch semantics;
- no-op detection;
- revision rules;
- folder cycle rules;
- Change action selection;
- upload state transitions.

### 24.2 Adapter conformance tests

The same suite runs against every implementation of a port.

`MetaStore` conformance minimum:

- conditional update under concurrency;
- unique `(parent,name)`;
- soft delete;
- atomic object+Change commit;
- idempotency replay;
- Changes cursor stability;
- recursive delete tombstones;
- metadata `eq` / `exists`.

`BlobStore` conformance is defined in section 8.3.

### 24.3 HTTP contract tests

Use the companion:

```text
AnyStore_Postman_Test_Cases.md
AnyStore.postman_collection.json
```

The API is accepted only when those contract tests pass.

---

## 25. V2 search boundary

V2 full-text search is a derived subsystem and MUST NOT change the source-of-truth architecture.

Future shape:

```text
MetaStore + BlobStore
        |
        v
Text Extractor / Indexer
        |
        v
SearchIndex trait
```

Possible adapters:

```text
Postgres FTS
SQLite FTS5
Meilisearch
OpenSearch / Elasticsearch
```

Search indexes MUST be rebuildable from AnyStore Objects and MUST NOT become authoritative storage.

No V2 search dependency is required to implement V1.

---

## 26. Initial production implementation

Implement these concrete adapters first:

```text
MetaStore:
  PostgresMetaStore -> CloudBase PostgreSQL

BlobStore:
  TencentCosBlobStore -> LightCOS
```

Implement these test adapters:

```text
InMemoryMetaStore
InMemoryBlobStore
```

Optional local-development adapter:

```text
LocalFsBlobStore
```

Do NOT implement MySQL, S3, OSS, Redis, Elasticsearch, or another database merely to prove abstraction. The abstraction is validated by interfaces and conformance tests; additional production adapters are added only when needed.

---

## 27. Implementation order

1. Domain types and validation.
2. Port traits: MetaStore and BlobStore.
3. PostgreSQL migrations and `PostgresMetaStore`.
4. In-memory adapters and conformance tests.
5. Object create/get/list/resolve/query.
6. PATCH + Revision + Changes.
7. Idempotency framework.
8. Delete + recursive tombstones.
9. `TencentCosBlobStore` single upload/download.
10. Multipart upload.
11. Atomic content replacement and GC queue.
12. Changes stable cursor implementation.
13. Postman contract suite.
14. CloudBase Run deployment.

No V2 search work is required before V1 passes the complete contract suite.

---

## 28. Definition of done

V1 is complete when:

- the public behavior matches `AnyStore_API_v1.md`;
- the Postman contract tests pass;
- HTTP/application code contains no PostgreSQL SQL;
- HTTP/application code contains no Tencent COS-specific protocol/signing logic;
- replacing `PostgresMetaStore` or `TencentCosBlobStore` requires no API change;
- all durable correctness survives stateless serverless restarts;
- file bytes do not transit the AnyStore application in the normal upload/download path;
- Object mutation + Revision + Change + idempotency completion are transactionally consistent;
- content replacement cannot destroy the previously committed content on failure.
