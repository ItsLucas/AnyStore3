-- AnyStore v1 initial schema.
CREATE TABLE objects (
    id TEXT PRIMARY KEY,
    revision BIGINT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('file', 'folder')),
    name TEXT NOT NULL,
    parent_id TEXT NULL REFERENCES objects(id),
    content_type TEXT NULL,
    size_bytes BIGINT NULL,
    sha256 TEXT NULL,
    content_state TEXT NULL CHECK (content_state IN ('none', 'ready')),
    blob_backend TEXT NULL,
    blob_ref TEXT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    deleted_at TIMESTAMPTZ NULL
);
CREATE UNIQUE INDEX objects_live_parent_name_uq ON objects(parent_id, name) WHERE deleted_at IS NULL;
CREATE INDEX objects_live_parent_idx ON objects(parent_id) WHERE deleted_at IS NULL;
CREATE INDEX objects_metadata_gin_idx ON objects USING GIN(metadata);
CREATE INDEX objects_live_blob_ref_idx ON objects(blob_backend, blob_ref) WHERE deleted_at IS NULL AND content_state = 'ready';
INSERT INTO objects (id, revision, kind, name, parent_id, metadata, created_at, updated_at) VALUES ('root', 1, 'folder', '', NULL, '{}'::jsonb, now(), now());
CREATE TABLE uploads (
    id TEXT PRIMARY KEY,
    object_id TEXT NOT NULL REFERENCES objects(id),
    state TEXT NOT NULL CHECK (state IN ('initiating', 'ready', 'completing', 'completed', 'aborted', 'expired')),
    mode TEXT NOT NULL CHECK (mode IN ('single', 'multipart')),
    blob_backend TEXT NOT NULL,
    blob_ref TEXT NOT NULL,
    provider_upload_id TEXT NULL,
    expected_size BIGINT NOT NULL,
    content_type TEXT NOT NULL,
    expected_sha256 TEXT NULL,
    provider_completed BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ NULL,
    aborted_at TIMESTAMPTZ NULL
);
CREATE INDEX uploads_object_idx ON uploads(object_id);
CREATE INDEX uploads_expiry_idx ON uploads(expires_at) WHERE state IN ('initiating', 'ready', 'completing');
CREATE TABLE idempotency_records (
    principal_id TEXT NOT NULL,
    operation TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('in_progress', 'completed')),
    owner_token TEXT NULL,
    lease_until TIMESTAMPTZ NULL,
    status_code INTEGER NULL,
    response_headers JSONB NULL,
    response_body BYTEA NULL,
    created_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (principal_id, operation, idempotency_key)
);
CREATE INDEX idempotency_expiry_idx ON idempotency_records(expires_at);
CREATE SEQUENCE changes_seq AS BIGINT START WITH 1 INCREMENT BY 1;
CREATE TABLE changes (
    seq BIGINT PRIMARY KEY,
    change_id TEXT NOT NULL UNIQUE,
    object_id TEXT NOT NULL,
    revision BIGINT NOT NULL,
    action TEXT NOT NULL CHECK (action IN ('created', 'metadata_updated', 'renamed', 'moved', 'content_ready', 'content_replaced', 'deleted')),
    changed_at TIMESTAMPTZ NOT NULL,
    request_id TEXT NOT NULL,
    idempotency_key TEXT NULL,
    tombstone BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE INDEX changes_changed_at_idx ON changes(changed_at);
CREATE TABLE change_cursors (
    cursor_id TEXT PRIMARY KEY,
    after_seq BIGINT NOT NULL,
    materialized BOOLEAN NOT NULL DEFAULT FALSE,
    page_limit INTEGER NULL,
    snapshot_max_seq BIGINT NULL,
    page_end_seq BIGINT NULL,
    has_more BOOLEAN NULL,
    next_cursor_id TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX change_cursors_expiry_idx ON change_cursors(expires_at);
CREATE TABLE changes_retention (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    purged_through_seq BIGINT NOT NULL DEFAULT 0
);
INSERT INTO changes_retention (singleton, purged_through_seq) VALUES (TRUE, 0);
CREATE TABLE blob_gc_queue (
    blob_backend TEXT NOT NULL,
    blob_ref TEXT NOT NULL,
    not_before TIMESTAMPTZ NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT NULL,
    PRIMARY KEY (blob_backend, blob_ref)
);
CREATE INDEX blob_gc_due_idx ON blob_gc_queue(not_before);