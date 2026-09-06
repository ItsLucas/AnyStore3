-- AnyStore is a server-side API. Browser and end-user database roles must not
-- bypass its revision, idempotency, tree, Change-feed or blob-pointer rules by
-- calling PostgREST tables/functions directly.

REVOKE ALL PRIVILEGES ON TABLE
    public.objects,
    public.uploads,
    public.idempotency_records,
    public.changes,
    public.change_cursors,
    public.changes_retention,
    public.blob_gc_queue
FROM PUBLIC;

REVOKE ALL PRIVILEGES ON SEQUENCE public.changes_seq FROM PUBLIC;

ALTER TABLE public.objects ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.uploads ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.idempotency_records ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.changes ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.change_cursors ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.changes_retention ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.blob_gc_queue ENABLE ROW LEVEL SECURITY;

DO $$
DECLARE
    role_name text;
    function_record record;
BEGIN
    FOREACH role_name IN ARRAY ARRAY['anon', 'authenticated']
    LOOP
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name) THEN
            EXECUTE format(
                'REVOKE ALL PRIVILEGES ON TABLE public.objects, public.uploads, '
                'public.idempotency_records, public.changes, public.change_cursors, '
                'public.changes_retention, public.blob_gc_queue FROM %I',
                role_name);
            EXECUTE format(
                'REVOKE ALL PRIVILEGES ON SEQUENCE public.changes_seq FROM %I',
                role_name);
        END IF;
    END LOOP;

    FOR function_record IN
        SELECT p.proname, pg_get_function_identity_arguments(p.oid) AS arguments
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'public'
          AND p.proname LIKE 'anystore\_%' ESCAPE '\'
    LOOP
        EXECUTE format(
            'REVOKE ALL PRIVILEGES ON FUNCTION public.%I(%s) FROM PUBLIC',
            function_record.proname,
            function_record.arguments);

        FOREACH role_name IN ARRAY ARRAY['anon', 'authenticated']
        LOOP
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name) THEN
                EXECUTE format(
                    'REVOKE ALL PRIVILEGES ON FUNCTION public.%I(%s) FROM %I',
                    function_record.proname,
                    function_record.arguments,
                    role_name);
            END IF;
        END LOOP;

        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'service_role') THEN
            EXECUTE format(
                'GRANT EXECUTE ON FUNCTION public.%I(%s) TO service_role',
                function_record.proname,
                function_record.arguments);
        END IF;
    END LOOP;

    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'service_role') THEN
        GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE
            public.objects,
            public.uploads,
            public.idempotency_records,
            public.changes,
            public.change_cursors,
            public.changes_retention,
            public.blob_gc_queue
        TO service_role;

        GRANT USAGE, SELECT ON SEQUENCE public.changes_seq TO service_role;
    END IF;
END;
$$;
