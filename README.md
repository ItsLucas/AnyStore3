# AnyStore v1

A generic file/object service: files and folders, arbitrary JSON metadata,
metadata query, direct-to-blob upload and download, revision-based conditional
writes, idempotent writes and an ordered Changes feed.

Implements `AnyStore_API_v1.md` and `AnyStore_Architecture_v1.md`. The core
contains no business-specific entities or fields.

面向业务调用方的中文文档见
[`docs/API_接入指南.md`](docs/API_接入指南.md)。

## Connecting to your deployment

Deploy AnyStore in your own environment and configure its API origin and
credentials locally. Examples below use placeholder domains; environment IDs,
bucket names and instance settings are deployment-specific and do not belong
in this README.

The public API requires `Authorization: Bearer <ANYSTORE_AUTH_TOKEN>`.
Deployment credentials are kept in the ignored, mode-`0600` `secrets.env`
file and are injected into CloudBase Run; they are never packaged into the
container source bundle.

### Testing the deployed service

Run the repeatable remote acceptance checks from the repository root:

```bash
ANYSTORE_TEST_BASE_URL=https://your-api.example.com/api/v1 tests/run_remote.sh
```

The default run performs a self-cleaning end-to-end smoke test against
CloudBase PostgreSQL and COS, followed by the normalised Postman collection
through Newman. It loads `ANYSTORE_AUTH_TOKEN` from the ignored `secrets.env`;
the token is passed through a mode-`0600` temporary Postman environment file
and is not placed on the Newman command line.

The exhaustive contract suite is optional:

```bash
ANYSTORE_TEST_BASE_URL=https://your-api.example.com/api/v1 tests/run_remote.sh --full
```

`--full` additionally runs all contract checks and uploads a multipart object
larger than 64 MiB. Prefer a disposable staging environment for that mode
because the contract suite intentionally creates Change and idempotency
history.

For the Postman desktop app, generate an importable collection:

```bash
python3 tests/contract/normalize_collection.py \
  AnyStore.postman_collection.json /tmp/anystore-postman.json
```

Import `/tmp/anystore-postman.json`, then create a Postman environment with:

- `baseUrl` =
  `https://your-api.example.com/api/v1`
- `authToken` = the `ANYSTORE_AUTH_TOKEN` value from `secrets.env`

Use the AnyStore API token here, **not** `CLOUDBASE_PG_API_KEY` or a Tencent
Cloud SecretKey.

## Architecture

Dependencies point inward. The domain knows nothing about HTTP, SQL or any
cloud provider; adapters are reachable only through port traits.

```text
anystore-http ──▶ anystore-application ──▶ anystore-domain
                          │                      ▲
                          ▼                      │
                  anystore-metastore   anystore-blobstore   (ports)
                          ▲                      ▲
        ┌─────────────────┤                      └──────────────┐
        │                 │                                     │
anystore-metastore-       anystore-metastore-        anystore-blobstore-cos
   postgres (sqlx)           postgrest (HTTP)            / -local
```

| Crate | Responsibility |
|---|---|
| `anystore-domain` | Objects, revisions, names, metadata semantics, Change actions, error taxonomy |
| `anystore-metastore` | `MetaStore` port: object reads, transactional mutations, uploads, idempotency, Changes, maintenance |
| `anystore-blobstore` | `BlobStore` port and backend registry |
| `anystore-application` | Services, idempotency orchestration, public JSON shapes, metrics |
| `anystore-http` | Axum router, request pipeline, error envelope |
| `anystore-metastore-postgres` | SQL over a direct connection; local development and the conformance suites |
| `anystore-metastore-postgrest` | The production adapter: CloudBase PostgreSQL over PostgREST RPC |
| `anystore-blobstore-cos` | The only crate containing COS protocol and signing |
| `anystore-blobstore-local` | Development blob backend with its own signed-URL transport |
| `anystore-testkit` | In-memory adapters and the shared port conformance suites |
| `anystore-server` | Composition root: configuration, migrations, maintenance loop |

### Design decisions worth knowing

**Mutation responses are rendered inside the store transaction.** A mutation
command carries a `ResponseRenderer`; the adapter invokes it after applying the
change and persists the resulting bytes with the idempotency record in the same
transaction. An idempotent replay therefore returns the original status and body
byte-for-byte, and "object mutation + Change append + idempotency finalisation
is one transaction" is structural rather than a convention.

**`change_id` is derived from a commit-ordered sequence value.** Change-emitting
transactions hold a PostgreSQL transaction-scoped advisory lock from sequence
reservation through commit. This prevents a later sequence from becoming
visible before an earlier one and being skipped permanently by a polling
cursor.

**Changes cursors are materialised on first read.** A cursor is created
unmaterialised; its first read binds the page limit and snapshots the maximum
sequence, so re-reading it returns the same page even after new Changes arrive.
Every read returns a fresh `next_cursor`, including for an empty page, so
polling can continue.

**Paths are derived, never stored.** Rendering walks parents with a recursive
CTE. Renaming or moving a folder rewrites no descendant and performs no blob
operation, because blob keys are `blobs/{upload_id}` and never encode the
logical name or path.

**Recursive delete locks before it walks.** Recursive CTEs cannot take row
locks, so the subtree is collected, locked, then re-derived; if a concurrent
insert changed it, the walk repeats. This prevents a live object surviving under
a deleted parent.

**Upload ids are derived from the idempotency scope.** The upload row is written
before the provider is contacted, and a retry after a crash resumes the same
session rather than creating a second one. PostgreSQL inserts are conflict-safe,
and an already-prepared multipart session reuses its persisted provider upload
id instead of creating new provider state.

**CloudBase PostgreSQL is reached over PostgREST RPC, not table CRUD.** The
shared instance is HTTP-only, so the production adapter cannot open a SQL
session or drive its own transactions. Table-level CRUD would split "revision
check, mutation, Change append and idempotency finalisation" across several
requests and lose exactly the atomicity the architecture depends on. Instead a
single database function, `anystore_rpc(op, payload)`, implements every
persistence operation; one call is one transaction. It answers with
`{"ok": true, "data": ...}` or `{"ok": false, "error": {"code", ...}}`, and the
adapter maps `code` back onto the error taxonomy so a name conflict stays a
`409` rather than becoming an opaque `500`.

**Mutation responses are rendered in the database on that backend.** A
`ResponseRenderer` closure cannot cross HTTP, so the RPC function renders the
operation's public JSON and commits those exact bytes with the idempotency
record. The rendering is pinned to `anystore-application/src/dto.rs` by a
conformance test, and the replay guarantee stays byte-identical by construction.

## Running locally

```bash
docker compose up -d postgres

export ANYSTORE_DATABASE_URL=postgres://postgres:anystore@127.0.0.1:54329/anystore
export ANYSTORE_BLOB_BACKEND=local_fs
export ANYSTORE_LOCAL_BLOB_SECRET=local-development-secret
export ANYSTORE_ALLOW_ANONYMOUS=true
export ANYSTORE_PORT=8088

cargo run -p anystore-server
```

`GET /healthz` returns `ok`; `GET /metrics` exposes Prometheus counters.

## Tests

All three levels required by the architecture document:

```bash
tests/run_all.sh
```

Individually:

```bash
# Domain and application unit tests, plus in-memory conformance.
cargo test --workspace

# The same MetaStore conformance suite against PostgreSQL.
ANYSTORE_TEST_DATABASE_URL=postgres://postgres:anystore@127.0.0.1:54329/anystore \
  cargo test -p anystore-metastore-postgres

# The same suite again, this time over HTTP against the PostgREST RPC surface.
# A local proxy executes anystore_rpc() so the migration's SQL, the adapter's
# request formation and its decoding are all exercised together.
ANYSTORE_TEST_DATABASE_URL=postgres://postgres:anystore@127.0.0.1:54329/anystore \
  cargo test -p anystore-metastore-postgrest

# The shared BlobStore suite against a real COS-compatible test bucket.
ANYSTORE_TEST_COS_BUCKET=... \
ANYSTORE_TEST_COS_ENDPOINT=... \
ANYSTORE_TEST_COS_SECRET_ID=... \
ANYSTORE_TEST_COS_SECRET_KEY=... \
  cargo test -p anystore-blobstore-cos --test conformance

# The HTTP contract suite (T01-T37) against a running server.
python3 tests/contract/contract_suite.py http://127.0.0.1:8088/api/v1
```

To run the contract suite against the *production* backend without a CloudBase
environment, put the local PostgREST stand-in in front of PostgreSQL:

```bash
# Terminal 1: applies both migrations and serves /v1/rdb/rest/rpc/anystore_rpc.
ANYSTORE_TEST_DATABASE_URL=postgres://postgres:anystore@127.0.0.1:54329/anystore \
  cargo run -p anystore-metastore-postgrest --example rpc_proxy -- --migrate

# Terminal 2
ANYSTORE_DATABASE_BACKEND=cloudbase_postgrest \
ANYSTORE_CLOUDBASE_PG_BASE_URL=http://127.0.0.1:8099 \
ANYSTORE_CLOUDBASE_PG_API_KEY=local-proxy-key \
ANYSTORE_BLOB_BACKEND=local_fs \
ANYSTORE_LOCAL_BLOB_SECRET=local-development-secret \
ANYSTORE_ALLOW_ANONYMOUS=true \
  cargo run -p anystore-server
```

### About the shipped Postman collection

`AnyStore.postman_collection.json` stores each URL as an object with
`host: ["{{baseUrl}}"]` and no `path`. Postman's client falls back to `raw`, but
newman does not, so every request resolves to the base URL and returns 404.
`tests/contract/normalize_collection.py` writes a corrected copy for automated
runs; the original file is never modified.

The collection also covers only a subset of the documented cases, and its
`T18 Complete Upload` never uploads bytes to the signed URL. AnyStore answers
that with `409 content_not_ready`, which is inside the range the test allows.
`tests/contract/contract_suite.py` implements the full T01-T37 set, including
the upload, replacement and recursive-delete flows the collection omits.

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| `ANYSTORE_DATABASE_BACKEND` | `postgres` | `postgres` (direct SQL) or `cloudbase_postgrest` (HTTP) |
| `ANYSTORE_DATABASE_URL` | *(required for `postgres`)* | PostgreSQL connection string |
| `ANYSTORE_MIGRATE_ON_START` | `true` | Apply migrations before serving; `postgres` backend only |
| `ANYSTORE_CLOUDBASE_PG_BASE_URL` | *(required for `cloudbase_postgrest`)* | e.g. `https://<env>.api.tcloudbasegateway.com` |
| `ANYSTORE_CLOUDBASE_PG_API_KEY` | *(required for `cloudbase_postgrest`)* | Sent as `Authorization: Bearer`; never logged or returned |
| `ANYSTORE_CLOUDBASE_PG_RPC_PATH` | `/v1/rdb/rest/rpc` | PostgREST RPC prefix |
| `ANYSTORE_CLOUDBASE_PG_FUNCTION` | `anystore_rpc` | Dispatcher function name |
| `ANYSTORE_CLOUDBASE_PG_TIMEOUT_SECONDS` | `20` | Per-request gateway timeout |
| `ANYSTORE_BLOB_BACKEND` | *(required)* | `tencent_cos` or `local_fs`; there is no default |
| `ANYSTORE_COS_BUCKET` | | Bucket including the APPID suffix |
| `ANYSTORE_COS_ENDPOINT` | | e.g. `cos.ap-shanghai.myqcloud.com` or `light-cos.com` |
| `ANYSTORE_COS_SECRET_ID` / `_SECRET_KEY` | | Credentials; never logged or returned |
| `ANYSTORE_COS_SESSION_TOKEN` | | Set when using temporary credentials |
| `ANYSTORE_PORT` / `PORT` | `8088` | Listen port |
| `ANYSTORE_PUBLIC_BASE_URL` | `http://127.0.0.1:$PORT` | Base for development signed URLs |
| `ANYSTORE_AUTH_TOKEN` | *(required)* | Requires `Authorization: Bearer`; startup fails without it unless anonymous access is allowed |
| `ANYSTORE_ALLOW_ANONYMOUS` | `false` | `true` runs without authentication; local development only |
| `ANYSTORE_IDEMPOTENCY_RETENTION_HOURS` | `24` | Contract minimum is 24 |
| `ANYSTORE_CHANGES_RETENTION_DAYS` | `30` | Contract minimum is 30 |
| `ANYSTORE_CHANGES_CURSOR_TTL_DAYS` | `30` | Lower it to exercise T37 |
| `ANYSTORE_UPLOAD_EXPIRY_HOURS` | `24` | Upload session lifetime |
| `ANYSTORE_UPLOAD_URL_EXPIRY_SECONDS` | `21600` | Signed upload URL lifetime |
| `ANYSTORE_DOWNLOAD_URL_EXPIRY_SECONDS` | `600` | Signed download URL lifetime |
| `ANYSTORE_MULTIPART_THRESHOLD_BYTES` | `67108864` | Above this, uploads are multipart |
| `ANYSTORE_PART_SIZE_BYTES` | `16777216` | Multipart part size |
| `ANYSTORE_MAINTENANCE_DAILY_AT` | `03:00` | Daily in-process maintenance time (`HH:MM`), or `off` |
| `ANYSTORE_MAINTENANCE_UTC_OFFSET` | `+08:00` | UTC offset of `ANYSTORE_MAINTENANCE_DAILY_AT` |
| `ANYSTORE_MAINTENANCE_BUDGET_SECONDS` | `20` | Time budget of one pass; keep it below the platform request timeout |
| `ANYSTORE_LOG` | `info` | `tracing` filter |
| `ANYSTORE_LOCAL_BLOB_ROOT` | `./.anystore-blobs` | Development backend only |
| `ANYSTORE_LOCAL_BLOB_SECRET` | *(required for `local_fs`)* | Signs development URLs; at least 16 bytes |

## Deployment

### Maintenance

One maintenance pass expires stale upload sessions, deletes unreferenced blobs
and purges idempotency records, Changes cursors and old Changes. Expiry and
blob collection repeat in batches until their backlog is empty or the time
budget runs out; the next pass resumes whatever is left.

A pass runs daily at `ANYSTORE_MAINTENANCE_DAILY_AT` in every live process.
A deployment that scales to zero has no live process at that time, so it also
needs an external trigger: `POST /internal/maintenance` runs one pass and
returns its report. It is not part of the public API and requires the same
bearer token. `cloudbase/functions/anystore-maintenance` is a timer-triggered
cloud function that calls it daily at 03:00, retries while a cold start answers
502/503/504, and calls again while a pass reports `"drained": false`;
`cloudbaserc.example.json` shows its configuration. A pass that finds another one running in the same process
returns `202 {"status": "already_running"}`, and passes in different processes
only duplicate work, because every claim uses `SKIP LOCKED`.

The server handles SIGTERM by finishing in-flight requests before exiting.

```bash
docker build -t anystore:v1 .
```

The image runs as a non-root user and reads `PORT`, which CloudBase Run injects.
The service is stateless: local disk is used only by the development blob
backend, which must not be used in production.

Authentication is deployment-specific. Startup fails when `ANYSTORE_AUTH_TOKEN`
is unset unless `ANYSTORE_ALLOW_ANONYMOUS=true`, in which case every request
runs as principal `local`; the token maps to the same principal, so idempotency
scope is unaffected by enabling it. `ANYSTORE_BLOB_BACKEND` likewise has no
default, so a missing variable cannot silently select container-local storage.

### CloudBase PostgreSQL

Copy `cloudbaserc.example.json` to the ignored `cloudbaserc.json` and set your
own environment ID before using CloudBase tooling. Keep deployment-specific
configuration local.

Migrations live in two places, deliberately:

| Path | Applied by | Contents |
|---|---|---|
| `migrations/` | sqlx, for the `postgres` backend | Tables, indexes, sequences |
| `cloudbase/migrations/` | CloudBase tooling, out of band | The PostgREST RPC surface |

| Version | Name | Purpose |
|---|---|---|
| `20260901171500` | `initial_anystore_schema` | Create the initial schema |
| `20260901213000` | `anystore_postgrest_rpc` | Apply before deploying `cloudbase_postgrest` |
| `20260902002000` | `lock_down_anystore_rpc` | Restricts tables and RPC functions to the server-side `service_role` |
| `20261024120000` | `bound_tree_walks_and_lock_waits` | Cycle-safe tree walks, locked re-check of folder moves, bounded lock waits |

Apply pending versions with `tcb db pg migration up`; `tcb db pg migration list`
compares local and remote. The tooling records applied versions, so a change to
the RPC surface ships as a new version that replaces the affected functions;
an applied file is never edited.

Writes serialise on one Change-order lock, so lock waits are bounded: the
sqlx backend sets `statement_timeout` 15 s, `lock_timeout` 5 s and
`idle_in_transaction_session_timeout` 30 s per session, and `anystore_rpc` sets
`lock_timeout` 5 s. A lock timeout, deadlock or serialization failure is
returned as a retryable `429 rate_limited`. PostgreSQL ignores a function-level
`statement_timeout` for the statement already running, so on CloudBase set it
on the database role the gateway uses, below
`ANYSTORE_CLOUDBASE_PG_TIMEOUT_SECONDS`.

The gateway offers no migration channel, so `ANYSTORE_MIGRATE_ON_START` is
ignored for this backend and the RPC migration must be applied before the
service starts. Startup calls `anystore_rpc('ping', …)` and refuses to serve if
the surface is missing, which turns a forgotten migration into a failed deploy
rather than a stream of 500s.

Every function is `CREATE OR REPLACE`, so re-applying the migration is safe.
Replacing a function keeps its owner and grants, so later RPC versions do not
need the lock-down migration re-run. They run as `SECURITY INVOKER`. The lock-down migration revokes table, sequence
and RPC access from `PUBLIC`, `anon` and `authenticated`, enables RLS on all
internal tables, and grants only `service_role` the access required by the
server-side API key.

```bash
ANYSTORE_DATABASE_BACKEND=cloudbase_postgrest
ANYSTORE_CLOUDBASE_PG_BASE_URL=https://<env>.api.tcloudbasegateway.com
ANYSTORE_CLOUDBASE_PG_API_KEY=<environment API key>   # server-side secret
ANYSTORE_BLOB_BACKEND=tencent_cos
```

## Not implemented

V2 full-text search (`POST /api/v1/search`) is out of scope for V1. It is a
derived subsystem and requires no change to the source of truth.
