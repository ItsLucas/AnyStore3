# AnyStore v1

A generic file/object service: files and folders, arbitrary JSON metadata,
metadata query, direct-to-blob upload and download, revision-based conditional
writes, idempotent writes and an ordered Changes feed.

Implements `AnyStore_API_v1.md` and `AnyStore_Architecture_v1.md`. The core
contains no business-specific entities or fields.

## Architecture

Dependencies point inward. The domain knows nothing about HTTP, SQL or any
cloud provider; adapters are reachable only through port traits.

```text
anystore-http ──▶ anystore-application ──▶ anystore-domain
                          │                      ▲
                          ▼                      │
                  anystore-metastore   anystore-blobstore   (ports)
                          ▲                      ▲
        ┌─────────────────┘                      └──────────────┐
anystore-metastore-postgres            anystore-blobstore-cos / -local
```

| Crate | Responsibility |
|---|---|
| `anystore-domain` | Objects, revisions, names, metadata semantics, Change actions, error taxonomy |
| `anystore-metastore` | `MetaStore` port: object reads, transactional mutations, uploads, idempotency, Changes, maintenance |
| `anystore-blobstore` | `BlobStore` port and backend registry |
| `anystore-application` | Services, idempotency orchestration, public JSON shapes, metrics |
| `anystore-http` | Axum router, request pipeline, error envelope |
| `anystore-metastore-postgres` | The only crate containing SQL |
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

**`change_id` is derived from a reserved sequence value.** The transaction takes
`nextval('changes_seq')` before inserting, so `change_id` is unique and
lexicographically monotonic in Change order.

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
session rather than creating a second one.

## Running locally

```bash
docker compose up -d postgres

export ANYSTORE_DATABASE_URL=postgres://postgres:anystore@127.0.0.1:54329/anystore
export ANYSTORE_BLOB_BACKEND=local_fs
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

# The HTTP contract suite (T01-T37) against a running server.
python3 tests/contract/contract_suite.py http://127.0.0.1:8088/api/v1
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
| `ANYSTORE_DATABASE_BACKEND` | `postgres` | Only `postgres` is supported |
| `ANYSTORE_DATABASE_URL` | *(required)* | PostgreSQL connection string |
| `ANYSTORE_MIGRATE_ON_START` | `true` | Apply migrations before serving |
| `ANYSTORE_BLOB_BACKEND` | `local_fs` | `tencent_cos` or `local_fs` |
| `ANYSTORE_COS_BUCKET` | | Bucket including the APPID suffix |
| `ANYSTORE_COS_ENDPOINT` | | e.g. `cos.ap-shanghai.myqcloud.com` or `light-cos.com` |
| `ANYSTORE_COS_SECRET_ID` / `_SECRET_KEY` | | Credentials; never logged or returned |
| `ANYSTORE_COS_SESSION_TOKEN` | | Set when using temporary credentials |
| `ANYSTORE_PORT` / `PORT` | `8088` | Listen port |
| `ANYSTORE_PUBLIC_BASE_URL` | `http://127.0.0.1:$PORT` | Base for development signed URLs |
| `ANYSTORE_AUTH_TOKEN` | *(unset)* | When set, requires `Authorization: Bearer` |
| `ANYSTORE_IDEMPOTENCY_RETENTION_HOURS` | `24` | Contract minimum is 24 |
| `ANYSTORE_CHANGES_RETENTION_DAYS` | `30` | Contract minimum is 30 |
| `ANYSTORE_CHANGES_CURSOR_TTL_DAYS` | `30` | Lower it to exercise T37 |
| `ANYSTORE_UPLOAD_EXPIRY_HOURS` | `24` | Upload session lifetime |
| `ANYSTORE_UPLOAD_URL_EXPIRY_SECONDS` | `21600` | Signed upload URL lifetime |
| `ANYSTORE_DOWNLOAD_URL_EXPIRY_SECONDS` | `600` | Signed download URL lifetime |
| `ANYSTORE_MULTIPART_THRESHOLD_BYTES` | `67108864` | Above this, uploads are multipart |
| `ANYSTORE_PART_SIZE_BYTES` | `16777216` | Multipart part size |
| `ANYSTORE_MAINTENANCE_INTERVAL_SECONDS` | `300` | `0` disables the loop |
| `ANYSTORE_LOG` | `info` | `tracing` filter |
| `ANYSTORE_LOCAL_BLOB_ROOT` | `./.anystore-blobs` | Development backend only |
| `ANYSTORE_LOCAL_BLOB_SECRET` | *(development default)* | Signs development URLs |

## Deployment

```bash
docker build -t anystore:v1 .
```

The image runs as a non-root user and reads `PORT`, which CloudBase Run injects.
The service is stateless: local disk is used only by the development blob
backend, which must not be used in production.

Authentication is deployment-specific. With `ANYSTORE_AUTH_TOKEN` unset, every
request runs as principal `local`; the token maps to the same principal, so
idempotency scope is unaffected by enabling it.

## Not implemented

V2 full-text search (`POST /api/v1/search`) is out of scope for V1. It is a
derived subsystem and requires no change to the source of truth.
