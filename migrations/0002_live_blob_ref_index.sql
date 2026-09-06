CREATE INDEX objects_live_blob_ref_idx
    ON objects(blob_backend, blob_ref)
    WHERE deleted_at IS NULL AND content_state = 'ready';
