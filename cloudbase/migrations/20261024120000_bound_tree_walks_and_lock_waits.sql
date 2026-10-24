-- AnyStore v1: bound tree walks and lock waits on the RPC surface.
--
-- Replaces five functions from `anystore_postgrest_rpc` (20260901213000):
--
-- * `anystore_path`, `anystore_is_self_or_descendant` and
--   `anystore_lock_descendants` stop on a repeated id, so a parent cycle can
--   no longer recurse without bound.
-- * `anystore_op_patch_object` re-checks a folder move under the Change-order
--   lock, so two concurrent moves can no longer commit a cycle together.
-- * `anystore_rpc` bounds lock waits and reports lock timeouts, deadlocks and
--   serialization failures as a retryable `rate_limited`.
--
-- Every statement is `CREATE OR REPLACE` with an unchanged signature, so owners
-- and the grants from `lock_down_anystore_rpc` (20260902002000) are kept.

-- Renders `path` by walking parents to the root. `path` is derived state, never
-- stored, so renaming or moving a folder rewrites no descendant.
--
-- Every recursive walk in this file carries the ids it has visited and stops on
-- a repeat, so a parent cycle can never turn a query into unbounded recursion.
CREATE OR REPLACE FUNCTION anystore_path(p_id text)
RETURNS text
LANGUAGE sql STABLE AS $$
    WITH RECURSIVE chain(cur_id, parent_id, name, depth, visited) AS (
            SELECT o.id, o.parent_id, o.name, 0, ARRAY[o.id]
            FROM objects o
            WHERE o.id = p_id
        UNION ALL
            SELECT p.id, p.parent_id, p.name, c.depth + 1, c.visited || p.id
            FROM chain c
            JOIN objects p ON p.id = c.parent_id
            WHERE p.id <> ALL(c.visited)
    )
    SELECT '/' || COALESCE(
        array_to_string(
            array_agg(name ORDER BY depth DESC) FILTER (WHERE name <> ''),
            '/'),
        '')
    FROM chain;
$$;

-- ---------------------------------------------------------------------------

-- True when `p_candidate` is `p_subject` itself or lies beneath it.
CREATE OR REPLACE FUNCTION anystore_is_self_or_descendant(
    p_candidate text,
    p_subject   text)
RETURNS boolean
LANGUAGE sql STABLE AS $$
    WITH RECURSIVE up(id, parent_id, visited) AS (
            SELECT id, parent_id, ARRAY[id] FROM objects WHERE id = p_candidate
        UNION ALL
            SELECT o.id, o.parent_id, up.visited || o.id
            FROM objects o
            JOIN up ON o.id = up.parent_id
            WHERE o.id <> ALL(up.visited)
    )
    SELECT EXISTS(SELECT 1 FROM up WHERE id = p_subject);
$$;

-- ---------------------------------------------------------------------------

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
            WITH RECURSIVE down(id, depth, visited) AS (
                    SELECT id, 0, ARRAY[id] FROM objects
                    WHERE id = p_root AND deleted_at IS NULL
                UNION ALL
                    SELECT o.id, d.depth + 1, d.visited || o.id
                    FROM objects o
                    JOIN down d ON o.parent_id = d.id
                    WHERE o.deleted_at IS NULL AND o.id <> ALL(d.visited)
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

-- ---------------------------------------------------------------------------

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

    -- The ancestor walk above takes no row locks, so two concurrent moves (A
    -- into B's subtree, B into A's) can both pass it against the other's
    -- pre-move state and together commit a cycle. Re-check under the
    -- Change-order lock, which every move takes anyway: under READ COMMITTED
    -- the next statement's snapshot then includes every earlier move plus this
    -- one. Taking the lock only after the UPDATE keeps the usual "row locks,
    -- then Change lock" order, so this cannot deadlock against another writer.
    IF v_parent_changed AND v_current.kind = 'folder' THEN
        PERFORM pg_advisory_xact_lock(hashtext('anystore:changes:append')::bigint);
        IF anystore_is_self_or_descendant(v_target_parent, v_id) THEN
            PERFORM anystore_raise(
                'invalid_move',
                'A folder cannot be moved into itself or one of its descendants.');
        END IF;
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

-- ---------------------------------------------------------------------------

-- The only entry point the HTTP adapter calls.
--
-- Every operation runs inside this function's block, so a domain error returned
-- from the handler rolls back everything the operation did: a failed mutation
-- can never leave a half-applied object, Change or idempotency record behind.
--
-- Every write serialises on one Change-order lock, so lock waits are bounded
-- well below the adapter's HTTP timeout: a stalled writer then yields a
-- retryable `rate_limited` instead of a gateway timeout whose outcome the
-- client cannot know. `statement_timeout` cannot be set here (a function-level
-- setting does not apply to the statement already running); configure it on
-- the gateway's database role instead.
CREATE OR REPLACE FUNCTION anystore_rpc(op text, payload jsonb DEFAULT '{}'::jsonb)
RETURNS jsonb
LANGUAGE plpgsql
SET lock_timeout = '5s'
AS $$
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
        WHEN lock_not_available OR deadlock_detected OR serialization_failure THEN
            RETURN jsonb_build_object('ok', FALSE, 'error',
                jsonb_build_object(
                    'code', 'rate_limited',
                    'message', 'The store is busy; retry the request.'));
    END;
END;
$$;

COMMENT ON FUNCTION anystore_rpc(text, jsonb) IS
    'AnyStore v1 PostgREST entry point. One call = one transaction = one '
    'AnyStore persistence operation. Returns {"ok":true,"data":...} or '
    '{"ok":false,"error":{"code",...}}.';
