-- AnyStore v1: PostgREST RPC surface for CloudBase shared PostgreSQL.
--
-- CloudBase shared PostgreSQL is reachable over HTTP only, so the production
-- adapter cannot open a SQL session and cannot drive its own transactions.
-- Ordinary PostgREST table CRUD would therefore split "revision check +
-- mutation + Change append + idempotency finalisation" into several requests,
-- which is exactly the atomicity the architecture forbids losing.
--
-- Every AnyStore persistence operation is instead exposed as one call to
-- `anystore_rpc(op, payload)`. A single function call is atomic, so each
-- operation keeps the same one-transaction guarantee the sqlx adapter gets from
-- an explicit `BEGIN`/`COMMIT`.
--
-- Depends on the schema created by `initial_anystore_schema` (20260901171500).
-- Every object here is `CREATE OR REPLACE`, so re-applying is safe.

-- Supports the live-blob guard in `claim_gc_batch` without a sequential scan.
CREATE INDEX IF NOT EXISTS objects_live_blob_ref_idx
    ON objects (blob_backend, blob_ref)
    WHERE deleted_at IS NULL AND content_state = 'ready';

-- ---------------------------------------------------------------------------
-- Error signalling
-- ---------------------------------------------------------------------------

-- Domain errors travel as SQLSTATE 'AS001' so the dispatcher can turn them into
-- a structured envelope. The Rust adapter maps `code` onto a `DomainError`
-- variant, which is what keeps a name conflict a 409 instead of an opaque 500.
CREATE OR REPLACE FUNCTION anystore_raise(
    p_code    text,
    p_message text,
    p_extra   jsonb DEFAULT NULL)
RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION USING
        ERRCODE = 'AS001',
        MESSAGE = p_code,
        DETAIL  = p_message,
        HINT    = COALESCE(p_extra::text, '');
END;
$$;

-- ---------------------------------------------------------------------------
-- Rendering helpers
-- ---------------------------------------------------------------------------

-- RFC 3339 with microsecond precision and a `Z` suffix, matching
-- `chrono::DateTime::to_rfc3339_opts(SecondsFormat::Micros, true)`.
CREATE OR REPLACE FUNCTION anystore_ts(p timestamptz)
RETURNS text
LANGUAGE sql STABLE AS $$
    SELECT to_char(p AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"');
$$;

-- Renders `path` by walking parents to the root. `path` is derived state, never
-- stored, so renaming or moving a folder rewrites no descendant.
CREATE OR REPLACE FUNCTION anystore_path(p_id text)
RETURNS text
LANGUAGE sql STABLE AS $$
    WITH RECURSIVE chain(cur_id, parent_id, name, depth) AS (
            SELECT o.id, o.parent_id, o.name, 0
            FROM objects o
            WHERE o.id = p_id
        UNION ALL
            SELECT p.id, p.parent_id, p.name, c.depth + 1
            FROM chain c
            JOIN objects p ON p.id = c.parent_id
    )
    SELECT '/' || COALESCE(
        array_to_string(
            array_agg(name ORDER BY depth DESC) FILTER (WHERE name <> ''),
            '/'),
        '')
    FROM chain;
$$;

-- Internal projection decoded by the Rust adapter into `ObjectView`.
-- Storage pointers stay out of it: they are served by `content_pointer` alone.
CREATE OR REPLACE FUNCTION anystore_object_view(o objects)
RETURNS jsonb
LANGUAGE sql STABLE AS $$
    SELECT jsonb_build_object(
        'id',            o.id,
        'revision',      o.revision,
        'kind',          o.kind,
        'name',          o.name,
        'parent_id',     o.parent_id,
        'content_type',  o.content_type,
        'size',          o.size_bytes,
        'sha256',        o.sha256,
        'content_state', o.content_state,
        'metadata',      o.metadata,
        'created_at',    anystore_ts(o.created_at),
        'updated_at',    anystore_ts(o.updated_at),
        'path',          anystore_path(o.id));
$$;

-- Public Object representation. Must stay identical to `object_json` in
-- `anystore-application/src/dto.rs`: folders omit the content fields and no
-- internal storage pointer is ever included.
CREATE OR REPLACE FUNCTION anystore_object_body(o objects)
RETURNS jsonb
LANGUAGE sql STABLE AS $$
    SELECT jsonb_build_object(
        'id',         o.id,
        'revision',   o.revision,
        'kind',       o.kind,
        'name',       o.name,
        'parent_id',  o.parent_id,
        'path',       anystore_path(o.id),
        'metadata',   o.metadata,
        'created_at', anystore_ts(o.created_at),
        'updated_at', anystore_ts(o.updated_at))
    || CASE WHEN o.kind = 'file' THEN jsonb_build_object(
            'content_type',  o.content_type,
            'size',          o.size_bytes,
            'sha256',        o.sha256,
            'content_state', o.content_state)
       ELSE '{}'::jsonb END;
$$;

-- Public body of `POST /uploads/{id}/complete`, matching `upload_complete_json`.
CREATE OR REPLACE FUNCTION anystore_upload_complete_body(o objects)
RETURNS jsonb
LANGUAGE sql STABLE AS $$
    SELECT jsonb_build_object(
        'object_id',     o.id,
        'revision',      o.revision,
        'content_state', o.content_state,
        'size',          o.size_bytes,
        'sha256',        o.sha256);
$$;

CREATE OR REPLACE FUNCTION anystore_upload_record(u uploads)
RETURNS jsonb
LANGUAGE sql STABLE AS $$
    SELECT jsonb_build_object(
        'id',                 u.id,
        'object_id',          u.object_id,
        'state',              u.state,
        'mode',               u.mode,
        'blob_backend',       u.blob_backend,
        'blob_ref',           u.blob_ref,
        'provider_upload_id', u.provider_upload_id,
        'expected_size',      u.expected_size,
        'content_type',       u.content_type,
        'expected_sha256',    u.expected_sha256,
        'provider_completed', u.provider_completed,
        'created_at',         anystore_ts(u.created_at),
        'expires_at',         anystore_ts(u.expires_at),
        'completed_at',       anystore_ts(u.completed_at),
        'aborted_at',         anystore_ts(u.aborted_at));
$$;

CREATE OR REPLACE FUNCTION anystore_change_record(c changes)
RETURNS jsonb
LANGUAGE sql STABLE AS $$
    SELECT jsonb_build_object(
        'change_id',       c.change_id,
        'object_id',       c.object_id,
        'revision',        c.revision,
        'action',          c.action,
        'changed_at',      anystore_ts(c.changed_at),
        'request_id',      c.request_id,
        'idempotency_key', c.idempotency_key,
        'tombstone',       c.tombstone);
$$;

-- A `StoredResponse`. The body is rendered here, inside the mutation
-- transaction, and the very same bytes are persisted with the idempotency
-- record, so a replay is byte-identical by construction.
CREATE OR REPLACE FUNCTION anystore_response(p_status int, p_body jsonb)
RETURNS jsonb
LANGUAGE sql IMMUTABLE AS $$
    SELECT jsonb_build_object(
        'status',  p_status,
        'headers', jsonb_build_array(
                       jsonb_build_array('content-type', 'application/json')),
        'body_b64', encode(convert_to(p_body::text, 'UTF8'), 'base64'));
$$;

CREATE OR REPLACE FUNCTION anystore_no_content()
RETURNS jsonb
LANGUAGE sql IMMUTABLE AS $$
    SELECT jsonb_build_object(
        'status', 204,
        'headers', '[]'::jsonb,
        'body_b64', '');
$$;

-- ---------------------------------------------------------------------------
-- Shared write helpers
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION anystore_check_if_match(
    p_current  bigint,
    p_if_match bigint)
RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    IF p_if_match IS NOT NULL AND p_if_match <> p_current THEN
        PERFORM anystore_raise(
            'revision_conflict',
            'Object has been modified.',
            jsonb_build_object('current_revision', p_current));
    END IF;
END;
$$;

-- Kind of a live object, used by write paths for parent validation. `FOR SHARE`
-- keeps the parent alive for the rest of the operation, so a concurrent delete
-- can never leave a live orphan behind.
CREATE OR REPLACE FUNCTION anystore_live_kind(p_id text, p_lock boolean)
RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
    v_kind text;
BEGIN
    IF p_lock THEN
        SELECT kind INTO v_kind FROM objects
        WHERE id = p_id AND deleted_at IS NULL FOR SHARE;
    ELSE
        SELECT kind INTO v_kind FROM objects
        WHERE id = p_id AND deleted_at IS NULL;
    END IF;
    RETURN v_kind;
END;
$$;

CREATE OR REPLACE FUNCTION anystore_require_live_folder(p_id text)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    v_kind text := anystore_live_kind(p_id, TRUE);
BEGIN
    IF v_kind IS NULL THEN
        PERFORM anystore_raise('object_not_found', 'Object not found.');
    ELSIF v_kind <> 'folder' THEN
        PERFORM anystore_raise('not_a_folder', 'Target object is not a folder.');
    END IF;
END;
$$;

-- True when `p_candidate` is `p_subject` itself or lies beneath it.
CREATE OR REPLACE FUNCTION anystore_is_self_or_descendant(
    p_candidate text,
    p_subject   text)
RETURNS boolean
LANGUAGE sql STABLE AS $$
    WITH RECURSIVE up(id, parent_id) AS (
            SELECT id, parent_id FROM objects WHERE id = p_candidate
        UNION ALL
            SELECT o.id, o.parent_id
            FROM objects o
            JOIN up ON o.id = up.parent_id
    )
    SELECT EXISTS(SELECT 1 FROM up WHERE id = p_subject);
$$;

-- Collects a subtree and locks every member.
--
-- Recursive CTEs cannot take row locks, so the set is locked and then
-- re-derived: if a concurrent insert added a child in between, the walk
-- repeats. This prevents a live object surviving under a deleted parent.
CREATE OR REPLACE FUNCTION anystore_lock_descendants(p_root text)
RETURNS text[]
LANGUAGE plpgsql AS $$
DECLARE
    v_previous text[] := NULL;
    v_ids      text[];
BEGIN
    FOR _attempt IN 1..5 LOOP
        SELECT COALESCE(array_agg(d.id ORDER BY d.depth DESC, d.id ASC),
                        ARRAY[]::text[])
        INTO v_ids
        FROM (
            WITH RECURSIVE down(id, depth) AS (
                    SELECT id, 0 FROM objects
                    WHERE id = p_root AND deleted_at IS NULL
                UNION ALL
                    SELECT o.id, d.depth + 1
                    FROM objects o
                    JOIN down d ON o.parent_id = d.id
                    WHERE o.deleted_at IS NULL
            )
            SELECT id, depth FROM down
        ) d;

        IF v_ids = v_previous THEN
            RETURN v_ids;
        END IF;

        PERFORM id FROM objects WHERE id = ANY(v_ids) FOR UPDATE;
        v_previous := v_ids;
    END LOOP;

    PERFORM anystore_raise(
        'internal_error',
        'subtree kept changing while acquiring delete locks');
    RETURN NULL;
END;
$$;

-- Enqueues a blob for deletion, only ever as part of the transaction that
-- removed the last live reference to it.
CREATE OR REPLACE FUNCTION anystore_enqueue_blob_gc(
    p_backend    text,
    p_blob_ref   text,
    p_not_before timestamptz)
RETURNS void
LANGUAGE sql AS $$
    INSERT INTO blob_gc_queue (blob_backend, blob_ref, not_before)
    VALUES (p_backend, p_blob_ref, p_not_before)
    ON CONFLICT (blob_backend, blob_ref) DO NOTHING;
$$;

-- Appends Change records while holding a transaction-scoped ordering lock.
--
-- PostgreSQL sequences are allocated before commit. Without the lock, a later
-- sequence can commit and advance a follower cursor before an earlier sequence
-- becomes visible, permanently skipping the earlier Change.
CREATE OR REPLACE FUNCTION anystore_append_changes(
    p_pending         jsonb,
    p_request_id      text,
    p_idempotency_key text,
    p_changed_at      timestamptz)
RETURNS jsonb
LANGUAGE plpgsql AS $$
DECLARE
    v_item    jsonb;
    v_seq     bigint;
    v_records jsonb := '[]'::jsonb;
BEGIN
    IF p_pending IS NULL
       OR jsonb_typeof(p_pending) <> 'array'
       OR jsonb_array_length(p_pending) = 0 THEN
        RETURN '[]'::jsonb;
    END IF;

    PERFORM pg_advisory_xact_lock(hashtext('anystore:changes:append')::bigint);

    FOR v_item IN
        SELECT value
        FROM jsonb_array_elements(p_pending) WITH ORDINALITY AS e(value, ord)
        ORDER BY ord
    LOOP
        v_seq := nextval('changes_seq');
        INSERT INTO changes (
            seq, change_id, object_id, revision, action,
            changed_at, request_id, idempotency_key, tombstone)
        VALUES (
            v_seq,
            'chg_' || lpad(v_seq::text, 20, '0'),
            v_item->>'object_id',
            (v_item->>'revision')::bigint,
            v_item->>'action',
            p_changed_at,
            p_request_id,
            p_idempotency_key,
            COALESCE((v_item->>'tombstone')::boolean, FALSE));

        v_records := v_records || jsonb_build_array(jsonb_build_object(
            'change_id',       'chg_' || lpad(v_seq::text, 20, '0'),
            'object_id',       v_item->>'object_id',
            'revision',        (v_item->>'revision')::bigint,
            'action',          v_item->>'action',
            'changed_at',      anystore_ts(p_changed_at),
            'request_id',      p_request_id,
            'idempotency_key', p_idempotency_key,
            'tombstone',       COALESCE((v_item->>'tombstone')::boolean, FALSE)));
    END LOOP;

    RETURN v_records;
END;
$$;

-- Finalises the idempotency record inside the ongoing mutation. Committing the
-- record with the mutation is what makes a retry unable to produce a second
-- object, revision or Change.
CREATE OR REPLACE FUNCTION anystore_finalize_idempotency(
    p_context    jsonb,
    p_response   jsonb,
    p_now        timestamptz,
    p_expires_at timestamptz)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    v_rows int;
BEGIN
    IF p_context IS NULL OR jsonb_typeof(p_context) <> 'object' THEN
        RETURN;
    END IF;

    UPDATE idempotency_records
    SET state            = 'completed',
        status_code      = (p_response->>'status')::int,
        response_headers = p_response->'headers',
        response_body    = decode(p_response->>'body_b64', 'base64'),
        completed_at     = p_now,
        expires_at       = p_expires_at,
        owner_token      = NULL,
        lease_until      = NULL
    WHERE principal_id    = p_context->>'principal_id'
      AND operation       = p_context->>'operation'
      AND idempotency_key = p_context->>'key'
      AND state           = 'in_progress'
      AND owner_token     = p_context->>'owner_token';

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    IF v_rows <> 1 THEN
        -- The lease was taken over by another attempt; abandoning the
        -- transaction is the only way to keep the operation exactly-once.
        PERFORM anystore_raise(
            'internal_error',
            'idempotency lease was lost during the mutation');
    END IF;
END;
$$;

-- Renders nothing itself: the caller supplies the already-rendered response so
-- the persisted bytes and the returned bytes cannot diverge.
CREATE OR REPLACE FUNCTION anystore_finish(p_ctx jsonb, p_response jsonb)
RETURNS jsonb
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM anystore_finalize_idempotency(
        p_ctx->'idempotency',
        p_response,
        (p_ctx->>'now')::timestamptz,
        (p_ctx->>'idempotency_expires_at')::timestamptz);
    RETURN p_response;
END;
$$;

-- ---------------------------------------------------------------------------
-- Reads
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION anystore_op_get_object(p jsonb)
RETURNS jsonb
LANGUAGE sql STABLE AS $$
    SELECT COALESCE(
        (SELECT anystore_object_view(o) FROM objects o
         WHERE o.id = p->>'id' AND o.deleted_at IS NULL),
        'null'::jsonb);
$$;

CREATE OR REPLACE FUNCTION anystore_op_content_pointer(p jsonb)
RETURNS jsonb
LANGUAGE sql STABLE AS $$
    SELECT COALESCE(
        (SELECT CASE
             WHEN o.blob_backend IS NULL OR o.blob_ref IS NULL THEN 'null'::jsonb
             ELSE jsonb_build_object(
                 'blob_backend', o.blob_backend,
                 'blob_ref', o.blob_ref)
         END
         FROM objects o
         WHERE o.id = p->>'id'
           AND o.deleted_at IS NULL
           AND o.content_state = 'ready'),
        'null'::jsonb);
$$;

-- Resolves a path by walking one indexed `(parent_id, name)` lookup per
-- segment, rather than storing a materialised path on every descendant.
CREATE OR REPLACE FUNCTION anystore_op_resolve_path(p jsonb)
RETURNS jsonb
LANGUAGE plpgsql STABLE AS $$
DECLARE
    v_segments text[] := ARRAY(SELECT jsonb_array_elements_text(p->'segments'));
    v_id       text   := 'root';
    v_segment  text;
BEGIN
    FOREACH v_segment IN ARRAY v_segments LOOP
        SELECT o.id INTO v_id
        FROM objects o
        WHERE o.parent_id = v_id AND o.name = v_segment AND o.deleted_at IS NULL;
        IF v_id IS NULL THEN
            RETURN 'null'::jsonb;
        END IF;
    END LOOP;

    RETURN anystore_op_get_object(jsonb_build_object('id', v_id));
END;
$$;

-- Keyset listing. The adapter asks for `limit + 1` rows and derives `has_more`
-- and the opaque continuation cursor, exactly like the sqlx adapter.
CREATE OR REPLACE FUNCTION anystore_op_list_children(p jsonb)
RETURNS jsonb
LANGUAGE plpgsql STABLE AS $$
DECLARE
    v_sort  text;
    v_dir   text;
    v_cmp   text;
    v_cast  text;
    v_items jsonb;
BEGIN
    v_sort := CASE p->>'order_by'
        WHEN 'name'       THEN 'o.name'
        WHEN 'created_at' THEN 'o.created_at'
        WHEN 'updated_at' THEN 'o.updated_at'
        -- Folders have no size; a total ordering keeps keyset pagination sound.
        WHEN 'size'       THEN 'COALESCE(o.size_bytes, -1)'
    END;
    v_cast := CASE p->>'order_by'
        WHEN 'name'       THEN '$2::text'
        WHEN 'created_at' THEN '$2::timestamptz'
        WHEN 'updated_at' THEN '$2::timestamptz'
        WHEN 'size'       THEN '$2::bigint'
    END;
    v_dir := CASE p->>'order' WHEN 'asc' THEN 'ASC' WHEN 'desc' THEN 'DESC' END;
    v_cmp := CASE p->>'order' WHEN 'asc' THEN '>'   WHEN 'desc' THEN '<'    END;

    IF v_sort IS NULL OR v_dir IS NULL THEN
        PERFORM anystore_raise('invalid_request', 'Invalid listing order.');
    END IF;

    EXECUTE format($sql$
        SELECT COALESCE(jsonb_agg(t.view ORDER BY t.ord), '[]'::jsonb)
        FROM (
            SELECT anystore_object_view(o) AS view,
                   row_number() OVER (ORDER BY %1$s %2$s, o.id %2$s) AS ord
            FROM objects o
            WHERE o.deleted_at IS NULL
              AND o.parent_id = $1
              AND ($3 IS NULL OR (%1$s, o.id) %3$s (%4$s, $3))
            ORDER BY %1$s %2$s, o.id %2$s
            LIMIT $4
        ) t
    $sql$, v_sort, v_dir, v_cmp, v_cast)
    INTO v_items
    USING p->>'parent_id',
          p->>'cursor_key',
          p->>'cursor_id',
          (p->>'limit')::bigint;

    RETURN jsonb_build_object('items', v_items);
END;
$$;

CREATE OR REPLACE FUNCTION anystore_op_query_objects(p jsonb)
RETURNS jsonb
LANGUAGE sql STABLE AS $$
    SELECT jsonb_build_object('items', COALESCE(
        (SELECT jsonb_agg(t.view ORDER BY t.id)
         FROM (
             SELECT o.id, anystore_object_view(o) AS view
             FROM objects o
             WHERE o.deleted_at IS NULL
               AND (p->>'kind' IS NULL OR o.kind = p->>'kind')
               AND (p->>'name' IS NULL OR o.name = p->>'name')
               AND (p->>'parent_id' IS NULL OR o.parent_id = p->>'parent_id')
               AND (p->>'cursor_id' IS NULL OR o.id > p->>'cursor_id')
               -- All supplied conditions are ANDed, on top-level keys only.
               AND NOT EXISTS (
                   SELECT 1
                   FROM jsonb_array_elements(
                            COALESCE(p->'metadata', '[]'::jsonb)) AS c(cond)
                   WHERE NOT CASE cond->>'op'
                       WHEN 'eq' THEN
                           (o.metadata -> (cond->>'key'))
                               IS NOT DISTINCT FROM (cond->'value')
                       WHEN 'exists' THEN
                           jsonb_exists(o.metadata, cond->>'key')
                               = (cond->>'value')::boolean
                       ELSE FALSE
                   END)
             ORDER BY o.id ASC
             LIMIT (p->>'limit')::bigint
         ) t),
        '[]'::jsonb));
$$;

-- ---------------------------------------------------------------------------
-- Object mutations
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION anystore_op_create_object(p jsonb)
RETURNS jsonb
LANGUAGE plpgsql AS $$
DECLARE
    v_ctx     jsonb := p->'ctx';
    v_now     timestamptz := (v_ctx->>'now')::timestamptz;
    v_id      text  := p->>'id';
    v_kind    text  := p->>'kind';
    v_object  objects;
    v_changes jsonb;
BEGIN
    PERFORM anystore_require_live_folder(p->>'parent_id');

    INSERT INTO objects (
        id, revision, kind, name, parent_id, content_type,
        content_state, metadata, created_at, updated_at)
    VALUES (
        v_id, 1, v_kind, p->>'name', p->>'parent_id', p->>'content_type',
        CASE WHEN v_kind = 'file' THEN 'none' END,
        COALESCE(p->'metadata', '{}'::jsonb), v_now, v_now);

    SELECT o.* INTO v_object FROM objects o WHERE o.id = v_id;

    v_changes := anystore_append_changes(
        jsonb_build_array(jsonb_build_object(
            'object_id', v_id, 'revision', 1,
            'action', 'created', 'tombstone', FALSE)),
        v_ctx->>'request_id',
        v_ctx->>'idempotency_key',
        v_now);

    RETURN jsonb_build_object(
        'response', anystore_finish(
            v_ctx, anystore_response(201, anystore_object_body(v_object))),
        'changes', v_changes);
END;
$$;

CREATE OR REPLACE FUNCTION anystore_op_patch_object(p jsonb)
RETURNS jsonb
LANGUAGE plpgsql AS $$
DECLARE
    v_ctx              jsonb := p->'ctx';
    v_now              timestamptz := (v_ctx->>'now')::timestamptz;
    v_id               text  := p->>'id';
    v_current          objects;
    v_target_name      text;
    v_target_parent    text;
    v_target_metadata  jsonb;
    v_name_changed     boolean;
    v_parent_changed   boolean;
    v_metadata_changed boolean;
    v_new_revision     bigint;
    v_pending          jsonb := '[]'::jsonb;
    v_changes          jsonb;
    v_remove           text;
BEGIN
    SELECT o.* INTO v_current FROM objects o WHERE o.id = v_id FOR UPDATE;
    IF NOT FOUND OR v_current.deleted_at IS NOT NULL THEN
        PERFORM anystore_raise('object_not_found', 'Object not found.');
    END IF;

    PERFORM anystore_check_if_match(v_current.revision, (p->>'if_match')::bigint);

    IF v_id = 'root' THEN
        IF p->>'new_parent_id' IS NOT NULL THEN
            PERFORM anystore_raise('invalid_move', 'Root cannot be moved.');
        END IF;
        IF p->>'new_name' IS NOT NULL THEN
            PERFORM anystore_raise('invalid_request', 'Root cannot be renamed.');
        END IF;
    END IF;

    v_target_name   := COALESCE(p->>'new_name', v_current.name);
    v_target_parent := COALESCE(p->>'new_parent_id', v_current.parent_id);

    v_target_metadata := v_current.metadata;
    IF jsonb_typeof(p->'metadata_patch') = 'object' THEN
        v_target_metadata := v_target_metadata
            || COALESCE(p->'metadata_patch'->'set', '{}'::jsonb);
        -- `remove` is applied after `set`, so a key in both ends up removed.
        FOR v_remove IN
            SELECT jsonb_array_elements_text(
                COALESCE(p->'metadata_patch'->'remove', '[]'::jsonb))
        LOOP
            v_target_metadata := v_target_metadata - v_remove;
        END LOOP;
    END IF;

    v_name_changed     := v_target_name IS DISTINCT FROM v_current.name;
    v_parent_changed   := v_target_parent IS DISTINCT FROM v_current.parent_id;
    v_metadata_changed := v_target_metadata IS DISTINCT FROM v_current.metadata;

    -- A no-op must not increment the revision or emit a Change.
    IF NOT v_name_changed AND NOT v_parent_changed AND NOT v_metadata_changed THEN
        RETURN jsonb_build_object(
            'response', anystore_finish(
                v_ctx, anystore_response(200, anystore_object_body(v_current))),
            'changes', '[]'::jsonb);
    END IF;

    IF v_parent_changed THEN
        IF v_target_parent IS NULL THEN
            PERFORM anystore_raise('invalid_move', 'Destination is required.');
        END IF;
        PERFORM anystore_require_live_folder(v_target_parent);
        IF v_current.kind = 'folder'
           AND anystore_is_self_or_descendant(v_target_parent, v_id) THEN
            PERFORM anystore_raise(
                'invalid_move',
                'A folder cannot be moved into itself or one of its descendants.');
        END IF;
    END IF;

    UPDATE objects
    SET revision   = revision + 1,
        name       = v_target_name,
        parent_id  = v_target_parent,
        metadata   = v_target_metadata,
        updated_at = v_now
    WHERE id = v_id AND deleted_at IS NULL
    RETURNING revision INTO v_new_revision;

    IF v_new_revision IS NULL THEN
        PERFORM anystore_raise('object_not_found', 'Object not found.');
    END IF;

    -- One PATCH increments the revision once but may emit several Changes, in a
    -- deterministic order: renamed, moved, metadata_updated.
    IF v_name_changed THEN
        v_pending := v_pending || jsonb_build_array(jsonb_build_object(
            'object_id', v_id, 'revision', v_new_revision,
            'action', 'renamed', 'tombstone', FALSE));
    END IF;
    IF v_parent_changed THEN
        v_pending := v_pending || jsonb_build_array(jsonb_build_object(
            'object_id', v_id, 'revision', v_new_revision,
            'action', 'moved', 'tombstone', FALSE));
    END IF;
    IF v_metadata_changed THEN
        v_pending := v_pending || jsonb_build_array(jsonb_build_object(
            'object_id', v_id, 'revision', v_new_revision,
            'action', 'metadata_updated', 'tombstone', FALSE));
    END IF;

    v_changes := anystore_append_changes(
        v_pending, v_ctx->>'request_id', v_ctx->>'idempotency_key', v_now);

    SELECT o.* INTO v_current FROM objects o WHERE o.id = v_id;

    RETURN jsonb_build_object(
        'response', anystore_finish(
            v_ctx, anystore_response(200, anystore_object_body(v_current))),
        'changes', v_changes);
END;
$$;

CREATE OR REPLACE FUNCTION anystore_op_delete_object(p jsonb)
RETURNS jsonb
LANGUAGE plpgsql AS $$
DECLARE
    v_ctx       jsonb := p->'ctx';
    v_now       timestamptz := (v_ctx->>'now')::timestamptz;
    v_id        text  := p->>'id';
    v_current   objects;
    v_targets   text[];
    v_target    text;
    v_deleted   jsonb := '{}'::jsonb;
    v_pending   jsonb := '[]'::jsonb;
    v_changes   jsonb;
    v_row       record;
    v_has_child boolean;
BEGIN
    SELECT o.* INTO v_current FROM objects o WHERE o.id = v_id FOR UPDATE;
    IF NOT FOUND OR v_current.deleted_at IS NOT NULL THEN
        PERFORM anystore_raise('object_not_found', 'Object not found.');
    END IF;

    PERFORM anystore_check_if_match(v_current.revision, (p->>'if_match')::bigint);

    IF v_id = 'root' THEN
        PERFORM anystore_raise('invalid_request', 'Root cannot be deleted.');
    END IF;

    IF v_current.kind = 'folder' AND COALESCE((p->>'recursive')::boolean, FALSE) THEN
        v_targets := anystore_lock_descendants(v_id);
    ELSE
        IF v_current.kind = 'folder' THEN
            SELECT EXISTS(
                SELECT 1 FROM objects
                WHERE parent_id = v_id AND deleted_at IS NULL)
            INTO v_has_child;
            IF v_has_child THEN
                PERFORM anystore_raise(
                    'folder_not_empty', 'Folder contains child objects.');
            END IF;
        END IF;
        v_targets := ARRAY[v_id];
    END IF;

    FOR v_row IN
        UPDATE objects
        SET revision = revision + 1, deleted_at = v_now, updated_at = v_now
        WHERE id = ANY(v_targets) AND deleted_at IS NULL
        RETURNING id, revision, blob_backend, blob_ref
    LOOP
        v_deleted := v_deleted || jsonb_build_object(v_row.id, v_row.revision);
        IF v_row.blob_backend IS NOT NULL AND v_row.blob_ref IS NOT NULL THEN
            PERFORM anystore_enqueue_blob_gc(
                v_row.blob_backend, v_row.blob_ref, v_now);
        END IF;
    END LOOP;

    -- One tombstone per deleted object, in the order the descendant walk
    -- produced (deepest first).
    FOREACH v_target IN ARRAY v_targets LOOP
        IF v_deleted ? v_target THEN
            v_pending := v_pending || jsonb_build_array(jsonb_build_object(
                'object_id', v_target,
                'revision',  (v_deleted->>v_target)::bigint,
                'action',    'deleted',
                'tombstone', TRUE));
        END IF;
    END LOOP;

    v_changes := anystore_append_changes(
        v_pending, v_ctx->>'request_id', v_ctx->>'idempotency_key', v_now);

    RETURN jsonb_build_object(
        'response', anystore_finish(v_ctx, anystore_no_content()),
        'changes', v_changes);
END;
$$;

CREATE OR REPLACE FUNCTION anystore_op_commit_content(p jsonb)
RETURNS jsonb
LANGUAGE plpgsql AS $$
DECLARE
    v_ctx          jsonb := p->'ctx';
    v_now          timestamptz := (v_ctx->>'now')::timestamptz;
    v_upload       uploads;
    v_current      objects;
    v_action       text;
    v_new_revision bigint;
    v_changes      jsonb;
    v_rows         int;
BEGIN
    SELECT u.* INTO v_upload FROM uploads u
    WHERE u.id = p->>'upload_id' FOR UPDATE;
    IF NOT FOUND THEN
        PERFORM anystore_raise('upload_not_found', 'Upload session not found.');
    END IF;
    IF v_upload.state NOT IN ('ready', 'completing', 'completed') THEN
        PERFORM anystore_raise('upload_not_found', 'Upload session not found.');
    END IF;
    IF v_upload.object_id    IS DISTINCT FROM p->>'object_id'
       OR v_upload.blob_backend IS DISTINCT FROM p->>'blob_backend'
       OR v_upload.blob_ref     IS DISTINCT FROM p->>'blob_ref' THEN
        PERFORM anystore_raise(
            'internal_error',
            'content commit does not match the upload record');
    END IF;

    SELECT o.* INTO v_current FROM objects o
    WHERE o.id = p->>'object_id' FOR UPDATE;
    IF NOT FOUND OR v_current.deleted_at IS NOT NULL THEN
        PERFORM anystore_raise('object_not_found', 'Object not found.');
    END IF;
    IF v_current.kind <> 'file' THEN
        PERFORM anystore_raise('not_a_file', 'Target object is not a file.');
    END IF;

    PERFORM anystore_check_if_match(v_current.revision, (p->>'if_match')::bigint);

    -- Completion is resumable: an already-completed session converges instead
    -- of bumping the revision or emitting a second Change.
    IF v_upload.state = 'completed' THEN
        RETURN jsonb_build_object(
            'response', anystore_finish(
                v_ctx,
                anystore_response(200, anystore_upload_complete_body(v_current))),
            'changes', '[]'::jsonb);
    END IF;

    v_action := CASE COALESCE(v_current.content_state, 'none')
        WHEN 'ready' THEN 'content_replaced'
        ELSE 'content_ready'
    END;

    UPDATE objects
    SET revision      = revision + 1,
        blob_backend  = p->>'blob_backend',
        blob_ref      = p->>'blob_ref',
        size_bytes    = (p->>'size')::bigint,
        content_type  = p->>'content_type',
        sha256        = p->>'sha256',
        content_state = 'ready',
        updated_at    = v_now
    WHERE id = p->>'object_id' AND deleted_at IS NULL
    RETURNING revision INTO v_new_revision;

    IF v_new_revision IS NULL THEN
        PERFORM anystore_raise('object_not_found', 'Object not found.');
    END IF;

    -- The superseded blob is queued only now that the pointer swap is part of
    -- the committing transaction.
    IF v_current.blob_backend IS NOT NULL
       AND v_current.blob_ref IS NOT NULL
       AND v_current.blob_ref <> p->>'blob_ref' THEN
        PERFORM anystore_enqueue_blob_gc(
            v_current.blob_backend,
            v_current.blob_ref,
            (p->>'gc_not_before')::timestamptz);
    END IF;

    UPDATE uploads
    SET state = 'completed', provider_completed = TRUE, completed_at = v_now
    WHERE id = p->>'upload_id' AND state IN ('ready', 'completing');
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    IF v_rows <> 1 THEN
        PERFORM anystore_raise('upload_not_found', 'Upload session not found.');
    END IF;

    -- A blob becoming live must not remain eligible for a stale GC entry.
    DELETE FROM blob_gc_queue
    WHERE blob_backend = p->>'blob_backend' AND blob_ref = p->>'blob_ref';

    v_changes := anystore_append_changes(
        jsonb_build_array(jsonb_build_object(
            'object_id', p->>'object_id',
            'revision',  v_new_revision,
            'action',    v_action,
            'tombstone', FALSE)),
        v_ctx->>'request_id',
        v_ctx->>'idempotency_key',
        v_now);

    SELECT o.* INTO v_current FROM objects o WHERE o.id = p->>'object_id';

    RETURN jsonb_build_object(
        'response', anystore_finish(
            v_ctx,
            anystore_response(200, anystore_upload_complete_body(v_current))),
        'changes', v_changes);
END;
$$;

-- ---------------------------------------------------------------------------
-- Uploads
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION anystore_op_create_upload_record(p jsonb)
RETURNS jsonb
LANGUAGE plpgsql AS $$
DECLARE
    v_rows   int;
    v_upload uploads;
BEGIN
    INSERT INTO uploads (
        id, object_id, state, mode, blob_backend, blob_ref,
        expected_size, content_type, expected_sha256, created_at, expires_at)
    VALUES (
        p->>'id', p->>'object_id', 'initiating', p->>'mode',
        p->>'blob_backend', p->>'blob_ref', (p->>'expected_size')::bigint,
        p->>'content_type', p->>'expected_sha256',
        (p->>'created_at')::timestamptz, (p->>'expires_at')::timestamptz)
    ON CONFLICT (id) DO NOTHING;
    GET DIAGNOSTICS v_rows = ROW_COUNT;

    SELECT u.* INTO v_upload FROM uploads u WHERE u.id = p->>'id';
    IF NOT FOUND THEN
        PERFORM anystore_raise('upload_not_found', 'Upload session not found.');
    END IF;

    RETURN jsonb_build_object(
        'upload',  anystore_upload_record(v_upload),
        'created', v_rows = 1);
END;
$$;

CREATE OR REPLACE FUNCTION anystore_op_get_upload(p jsonb)
RETURNS jsonb
LANGUAGE sql STABLE AS $$
    SELECT COALESCE(
        (SELECT anystore_upload_record(u) FROM uploads u WHERE u.id = p->>'id'),
        'null'::jsonb);
$$;

CREATE OR REPLACE FUNCTION anystore_op_update_upload_state(p jsonb)
RETURNS jsonb
LANGUAGE plpgsql AS $$
DECLARE
    v_state text := p->>'state';
    v_rows  int;
BEGIN
    UPDATE uploads
    SET state              = v_state,
        provider_upload_id = COALESCE(p->>'provider_upload_id', provider_upload_id),
        provider_completed = COALESCE(
            (p->>'provider_completed')::boolean, provider_completed)
    WHERE id = p->>'id'
      AND (   (v_state = 'ready'      AND state IN ('initiating', 'ready'))
           OR (v_state = 'completing' AND state IN ('ready', 'completing')));
    GET DIAGNOSTICS v_rows = ROW_COUNT;

    IF v_rows = 0 THEN
        PERFORM anystore_raise('upload_not_found', 'Upload session not found.');
    END IF;
    RETURN 'null'::jsonb;
END;
$$;

CREATE OR REPLACE FUNCTION anystore_op_abort_upload_record(p jsonb)
RETURNS jsonb
LANGUAGE plpgsql AS $$
DECLARE
    v_row record;
BEGIN
    -- A completed session is never rolled back: its blob is live content.
    UPDATE uploads
    SET state = 'aborted', aborted_at = (p->>'now')::timestamptz
    WHERE id = p->>'id' AND state <> 'completed'
    RETURNING blob_backend, blob_ref INTO v_row;

    IF FOUND THEN
        PERFORM anystore_enqueue_blob_gc(
            v_row.blob_backend,
            v_row.blob_ref,
            (p->>'gc_not_before')::timestamptz);
    END IF;
    RETURN 'null'::jsonb;
END;
$$;

-- ---------------------------------------------------------------------------
-- Idempotency
-- ---------------------------------------------------------------------------

-- One attempt at claiming a key. The adapter re-issues this while the answer is
-- `busy`, so a concurrent owner is waited for rather than raced.
CREATE OR REPLACE FUNCTION anystore_op_idempotency_acquire(p jsonb)
RETURNS jsonb
LANGUAGE plpgsql AS $$
DECLARE
    v_ctx   jsonb := p->'context';
    v_now   timestamptz := (p->>'now')::timestamptz;
    v_rows  int;
    v_rec   idempotency_records;
BEGIN
    INSERT INTO idempotency_records (
        principal_id, operation, idempotency_key, request_hash,
        state, owner_token, lease_until, created_at, expires_at)
    VALUES (
        v_ctx->>'principal_id', v_ctx->>'operation', v_ctx->>'key',
        v_ctx->>'request_hash', 'in_progress', v_ctx->>'owner_token',
        (p->>'lease_until')::timestamptz, v_now, (p->>'expires_at')::timestamptz)
    ON CONFLICT (principal_id, operation, idempotency_key) DO NOTHING;
    GET DIAGNOSTICS v_rows = ROW_COUNT;

    IF v_rows = 1 THEN
        RETURN jsonb_build_object(
            'decision', 'owner',
            'resource_token', anystore_resource_token(v_now));
    END IF;

    SELECT r.* INTO v_rec FROM idempotency_records r
    WHERE r.principal_id    = v_ctx->>'principal_id'
      AND r.operation       = v_ctx->>'operation'
      AND r.idempotency_key = v_ctx->>'key';
    IF NOT FOUND THEN
        -- Record vanished between the insert and the read; try again.
        RETURN jsonb_build_object('decision', 'retry');
    END IF;

    IF v_rec.request_hash IS DISTINCT FROM v_ctx->>'request_hash' THEN
        RETURN jsonb_build_object('decision', 'conflict');
    END IF;

    IF v_rec.state = 'completed' THEN
        RETURN jsonb_build_object('decision', 'replay', 'response',
            jsonb_build_object(
                'status',   COALESCE(v_rec.status_code, 200),
                'headers',  COALESCE(v_rec.response_headers, '[]'::jsonb),
                'body_b64', encode(COALESCE(v_rec.response_body, ''::bytea),
                                   'base64')));
    END IF;

    -- Taking over an expired lease prevents a crashed instance from blocking a
    -- key forever.
    IF v_rec.lease_until IS NULL OR v_rec.lease_until <= now() THEN
        UPDATE idempotency_records
        SET owner_token  = v_ctx->>'owner_token',
            lease_until  = (p->>'lease_until')::timestamptz,
            request_hash = v_ctx->>'request_hash'
        WHERE principal_id    = v_ctx->>'principal_id'
          AND operation       = v_ctx->>'operation'
          AND idempotency_key = v_ctx->>'key'
          AND state           = 'in_progress'
          AND lease_until    <= now();
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        IF v_rows = 1 THEN
            -- The resource token is stable for the life of one retained record,
            -- so ids derived from it survive lease takeover.
            RETURN jsonb_build_object(
                'decision', 'owner',
                'resource_token', anystore_resource_token(v_rec.created_at));
        END IF;
    END IF;

    RETURN jsonb_build_object('decision', 'busy');
END;
$$;

-- Microsecond Unix timestamp, matching `DateTime::timestamp_micros` in Rust.
CREATE OR REPLACE FUNCTION anystore_resource_token(p timestamptz)
RETURNS text
LANGUAGE sql IMMUTABLE AS $$
    SELECT ((EXTRACT(EPOCH FROM p) * 1000000)::bigint)::text;
$$;

CREATE OR REPLACE FUNCTION anystore_op_idempotency_complete(p jsonb)
RETURNS jsonb
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM anystore_finalize_idempotency(
        p->'context',
        p->'response',
        (p->>'now')::timestamptz,
        (p->>'expires_at')::timestamptz);
    RETURN 'null'::jsonb;
END;
$$;

CREATE OR REPLACE FUNCTION anystore_op_idempotency_release(p jsonb)
RETURNS jsonb
LANGUAGE plpgsql AS $$
DECLARE
    v_ctx jsonb := p->'context';
BEGIN
    DELETE FROM idempotency_records
    WHERE principal_id    = v_ctx->>'principal_id'
      AND operation       = v_ctx->>'operation'
      AND idempotency_key = v_ctx->>'key'
      AND state           = 'in_progress'
      AND owner_token     = v_ctx->>'owner_token';
    RETURN 'null'::jsonb;
END;
$$;

-- ---------------------------------------------------------------------------
-- Changes
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION anystore_build_change_page(
    p_after_seq bigint,
    p_limit     int)
RETURNS jsonb
LANGUAGE plpgsql STABLE AS $$
DECLARE
    v_snapshot bigint;
    v_items    jsonb;
    v_end      bigint;
    v_has_more boolean;
    v_lag      bigint;
BEGIN
    SELECT GREATEST(COALESCE(MAX(seq), p_after_seq), p_after_seq)
    INTO v_snapshot FROM changes;

    SELECT COALESCE(jsonb_agg(t.record ORDER BY t.seq), '[]'::jsonb),
           COALESCE(MAX(t.seq), p_after_seq)
    INTO v_items, v_end
    FROM (
        SELECT c.seq, anystore_change_record(c) AS record
        FROM changes c
        WHERE c.seq > p_after_seq AND c.seq <= v_snapshot
        ORDER BY c.seq ASC
        LIMIT p_limit
    ) t;

    SELECT EXISTS(
        SELECT 1 FROM changes WHERE seq > v_end AND seq <= v_snapshot)
    INTO v_has_more;
    SELECT count(*) INTO v_lag FROM changes WHERE seq > v_end;

    RETURN jsonb_build_object(
        'items',            v_items,
        'page_end_seq',     v_end,
        'snapshot_max_seq', v_snapshot,
        'has_more',         v_has_more,
        'lag',              v_lag);
END;
$$;

-- Cursors are created unmaterialised. The first read binds the page limit and
-- snapshots the maximum sequence, so re-reading returns exactly the same page
-- even after newer Changes have been appended.
CREATE OR REPLACE FUNCTION anystore_op_read_changes(p jsonb)
RETURNS jsonb
LANGUAGE plpgsql AS $$
DECLARE
    v_now        timestamptz := (p->>'now')::timestamptz;
    v_expires    timestamptz := (p->>'cursor_expires_at')::timestamptz;
    v_limit      int  := (p->>'limit')::int;
    v_new_cursor text := p->>'new_cursor_id';
    v_cursor     text := p->>'cursor';
    v_watermark  bigint;
    v_rec        change_cursors;
    v_page       jsonb;
    v_items      jsonb;
    v_lag        bigint;
BEGIN
    SELECT purged_through_seq INTO v_watermark
    FROM changes_retention WHERE singleton;

    IF v_cursor IS NULL THEN
        -- An initial read starts from the oldest retained Change.
        v_page := anystore_build_change_page(v_watermark, v_limit);
        INSERT INTO change_cursors (cursor_id, after_seq, created_at, expires_at)
        VALUES (v_new_cursor, (v_page->>'page_end_seq')::bigint, v_now, v_expires);
        RETURN jsonb_build_object(
            'items',       v_page->'items',
            'next_cursor', v_new_cursor,
            'has_more',    v_page->'has_more',
            'lag',         v_page->'lag');
    END IF;

    SELECT c.* INTO v_rec FROM change_cursors c
    WHERE c.cursor_id = v_cursor FOR UPDATE;
    -- An unknown cursor is indistinguishable from one whose record has already
    -- been purged, so both are reported as expired.
    IF NOT FOUND OR v_rec.expires_at <= v_now OR v_rec.after_seq < v_watermark THEN
        PERFORM anystore_raise(
            'changes_cursor_expired',
            'The requested Changes cursor is outside the retention window.');
    END IF;

    IF v_rec.materialized THEN
        IF v_rec.page_limit IS DISTINCT FROM v_limit THEN
            PERFORM anystore_raise(
                'invalid_request',
                'This cursor was already read with a different limit.');
        END IF;

        SELECT COALESCE(jsonb_agg(anystore_change_record(c) ORDER BY c.seq),
                        '[]'::jsonb)
        INTO v_items
        FROM changes c
        WHERE c.seq > v_rec.after_seq AND c.seq <= v_rec.page_end_seq;

        SELECT count(*) INTO v_lag FROM changes WHERE seq > v_rec.page_end_seq;

        RETURN jsonb_build_object(
            'items',       v_items,
            'next_cursor', v_rec.next_cursor_id,
            'has_more',    v_rec.has_more,
            'lag',         v_lag);
    END IF;

    v_page := anystore_build_change_page(v_rec.after_seq, v_limit);
    INSERT INTO change_cursors (cursor_id, after_seq, created_at, expires_at)
    VALUES (v_new_cursor, (v_page->>'page_end_seq')::bigint, v_now, v_expires);

    UPDATE change_cursors
    SET materialized     = TRUE,
        page_limit       = v_limit,
        snapshot_max_seq = (v_page->>'snapshot_max_seq')::bigint,
        page_end_seq     = (v_page->>'page_end_seq')::bigint,
        has_more         = (v_page->>'has_more')::boolean,
        next_cursor_id   = v_new_cursor
    WHERE cursor_id = v_cursor;

    RETURN jsonb_build_object(
        'items',       v_page->'items',
        'next_cursor', v_new_cursor,
        'has_more',    v_page->'has_more',
        'lag',         v_page->'lag');
END;
$$;

-- ---------------------------------------------------------------------------
-- Maintenance
-- ---------------------------------------------------------------------------

-- Pushing `not_before` forward is the claim: concurrent instances never pick up
-- the same blob, and a crashed worker's entry becomes visible again once the
-- lease elapses. A blob that is live content again is never claimed.
CREATE OR REPLACE FUNCTION anystore_op_claim_gc_batch(p jsonb)
RETURNS jsonb
LANGUAGE sql AS $$
    WITH claimed AS (
        UPDATE blob_gc_queue q
        SET not_before = (p->>'now')::timestamptz + interval '5 minutes'
        WHERE (q.blob_backend, q.blob_ref) IN (
            SELECT blob_backend, blob_ref
            FROM blob_gc_queue
            WHERE not_before <= (p->>'now')::timestamptz
              AND NOT EXISTS (
                  SELECT 1 FROM objects
                  WHERE deleted_at IS NULL
                    AND content_state = 'ready'
                    AND objects.blob_backend = blob_gc_queue.blob_backend
                    AND objects.blob_ref = blob_gc_queue.blob_ref)
            ORDER BY not_before
            LIMIT (p->>'limit')::bigint
            FOR UPDATE SKIP LOCKED)
        RETURNING q.blob_backend, q.blob_ref, q.attempts
    )
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'blob_backend', blob_backend,
        'blob_ref',     blob_ref,
        'attempts',     attempts)), '[]'::jsonb)
    FROM claimed;
$$;

CREATE OR REPLACE FUNCTION anystore_op_finish_gc(p jsonb)
RETURNS jsonb
LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM blob_gc_queue
    WHERE blob_backend = p->>'blob_backend' AND blob_ref = p->>'blob_ref';
    RETURN 'null'::jsonb;
END;
$$;

CREATE OR REPLACE FUNCTION anystore_op_fail_gc(p jsonb)
RETURNS jsonb
LANGUAGE plpgsql AS $$
BEGIN
    UPDATE blob_gc_queue
    SET attempts   = attempts + 1,
        last_error = p->>'error',
        not_before = (p->>'retry_at')::timestamptz
    WHERE blob_backend = p->>'blob_backend' AND blob_ref = p->>'blob_ref';
    RETURN 'null'::jsonb;
END;
$$;

CREATE OR REPLACE FUNCTION anystore_op_claim_expired_uploads(p jsonb)
RETURNS jsonb
LANGUAGE plpgsql AS $$
DECLARE
    v_now     timestamptz := (p->>'now')::timestamptz;
    v_uploads jsonb := '[]'::jsonb;
    v_row     uploads;
BEGIN
    FOR v_row IN
        UPDATE uploads
        SET state = 'expired'
        WHERE id IN (
            SELECT id FROM uploads
            WHERE expires_at <= v_now
              AND state IN ('initiating', 'ready', 'completing')
            ORDER BY expires_at
            LIMIT (p->>'limit')::bigint
            FOR UPDATE SKIP LOCKED)
        RETURNING *
    LOOP
        v_uploads := v_uploads || jsonb_build_array(anystore_upload_record(v_row));
        PERFORM anystore_enqueue_blob_gc(
            v_row.blob_backend, v_row.blob_ref, v_now);
    END LOOP;

    RETURN v_uploads;
END;
$$;

CREATE OR REPLACE FUNCTION anystore_op_purge_idempotency_records(p jsonb)
RETURNS jsonb
LANGUAGE sql AS $$
    WITH deleted AS (
        DELETE FROM idempotency_records
        WHERE expires_at <= (p->>'now')::timestamptz
        RETURNING 1)
    SELECT to_jsonb(count(*)) FROM deleted;
$$;

CREATE OR REPLACE FUNCTION anystore_op_purge_change_cursors(p jsonb)
RETURNS jsonb
LANGUAGE sql AS $$
    WITH deleted AS (
        DELETE FROM change_cursors
        WHERE expires_at <= (p->>'now')::timestamptz
        RETURNING 1)
    SELECT to_jsonb(count(*)) FROM deleted;
$$;

-- The watermark outlives the rows so that a cursor pointing into purged history
-- can still be rejected with `changes_cursor_expired`.
CREATE OR REPLACE FUNCTION anystore_op_purge_changes(p jsonb)
RETURNS jsonb
LANGUAGE plpgsql AS $$
DECLARE
    v_count   bigint;
    v_max_seq bigint;
BEGIN
    WITH deleted AS (
        DELETE FROM changes
        WHERE changed_at < (p->>'older_than')::timestamptz
        RETURNING seq)
    SELECT count(*), MAX(seq) INTO v_count, v_max_seq FROM deleted;

    IF v_max_seq IS NOT NULL THEN
        UPDATE changes_retention
        SET purged_through_seq = GREATEST(purged_through_seq, v_max_seq)
        WHERE singleton;
    END IF;

    RETURN to_jsonb(v_count);
END;
$$;

CREATE OR REPLACE FUNCTION anystore_op_count_pending_gc(p jsonb)
RETURNS jsonb
LANGUAGE sql STABLE AS $$
    SELECT to_jsonb(count(*)) FROM blob_gc_queue;
$$;

-- ---------------------------------------------------------------------------
-- Dispatcher
-- ---------------------------------------------------------------------------

-- The only entry point the HTTP adapter calls.
--
-- Every operation runs inside this function's block, so a domain error returned
-- from the handler rolls back everything the operation did: a failed mutation
-- can never leave a half-applied object, Change or idempotency record behind.
CREATE OR REPLACE FUNCTION anystore_rpc(op text, payload jsonb DEFAULT '{}'::jsonb)
RETURNS jsonb
LANGUAGE plpgsql AS $$
DECLARE
    v_data       jsonb;
    v_code       text;
    v_detail     text;
    v_hint       text;
    v_constraint text;
BEGIN
    BEGIN
        v_data := CASE op
            WHEN 'get_object'                THEN anystore_op_get_object(payload)
            WHEN 'content_pointer'           THEN anystore_op_content_pointer(payload)
            WHEN 'list_children'             THEN anystore_op_list_children(payload)
            WHEN 'resolve_path'              THEN anystore_op_resolve_path(payload)
            WHEN 'query_objects'             THEN anystore_op_query_objects(payload)
            WHEN 'create_object'             THEN anystore_op_create_object(payload)
            WHEN 'patch_object'              THEN anystore_op_patch_object(payload)
            WHEN 'delete_object'             THEN anystore_op_delete_object(payload)
            WHEN 'commit_content'            THEN anystore_op_commit_content(payload)
            WHEN 'create_upload_record'      THEN anystore_op_create_upload_record(payload)
            WHEN 'get_upload'                THEN anystore_op_get_upload(payload)
            WHEN 'update_upload_state'       THEN anystore_op_update_upload_state(payload)
            WHEN 'abort_upload_record'       THEN anystore_op_abort_upload_record(payload)
            WHEN 'idempotency_acquire'       THEN anystore_op_idempotency_acquire(payload)
            WHEN 'idempotency_complete'      THEN anystore_op_idempotency_complete(payload)
            WHEN 'idempotency_release'       THEN anystore_op_idempotency_release(payload)
            WHEN 'read_changes'              THEN anystore_op_read_changes(payload)
            WHEN 'claim_gc_batch'            THEN anystore_op_claim_gc_batch(payload)
            WHEN 'finish_gc'                 THEN anystore_op_finish_gc(payload)
            WHEN 'fail_gc'                   THEN anystore_op_fail_gc(payload)
            WHEN 'claim_expired_uploads'     THEN anystore_op_claim_expired_uploads(payload)
            WHEN 'purge_idempotency_records' THEN anystore_op_purge_idempotency_records(payload)
            WHEN 'purge_change_cursors'      THEN anystore_op_purge_change_cursors(payload)
            WHEN 'purge_changes'             THEN anystore_op_purge_changes(payload)
            WHEN 'count_pending_gc'          THEN anystore_op_count_pending_gc(payload)
            WHEN 'ping'                      THEN '"pong"'::jsonb
        END;

        IF v_data IS NULL THEN
            PERFORM anystore_raise(
                'internal_error', format('unknown anystore rpc op %L', op));
        END IF;

        RETURN jsonb_build_object('ok', TRUE, 'data', v_data);
    EXCEPTION
        WHEN SQLSTATE 'AS001' THEN
            GET STACKED DIAGNOSTICS
                v_code   = MESSAGE_TEXT,
                v_detail = PG_EXCEPTION_DETAIL,
                v_hint   = PG_EXCEPTION_HINT;
            RETURN jsonb_build_object('ok', FALSE, 'error',
                jsonb_build_object('code', v_code, 'message', v_detail)
                || COALESCE(NULLIF(v_hint, '')::jsonb, '{}'::jsonb));
        WHEN unique_violation THEN
            GET STACKED DIAGNOSTICS v_constraint = CONSTRAINT_NAME;
            IF v_constraint = 'objects_live_parent_name_uq' THEN
                RETURN jsonb_build_object('ok', FALSE, 'error',
                    jsonb_build_object(
                        'code', 'name_conflict',
                        'message',
                        'An object with the same name already exists in this folder.'));
            END IF;
            RAISE;
    END;
END;
$$;

COMMENT ON FUNCTION anystore_rpc(text, jsonb) IS
    'AnyStore v1 PostgREST entry point. One call = one transaction = one '
    'AnyStore persistence operation. Returns {"ok":true,"data":...} or '
    '{"ok":false,"error":{"code",...}}.';
