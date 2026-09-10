CREATE TABLE artifact_gc_targets (
    artifact_id UUID NOT NULL REFERENCES artifacts(id) ON DELETE CASCADE,
    target_kind TEXT NOT NULL CHECK (target_kind IN ('STAGING', 'COMMITTED')),
    uri TEXT NOT NULL,
    eligible_at TIMESTAMPTZ NOT NULL,
    state TEXT NOT NULL DEFAULT 'PENDING' CHECK (
        state IN ('PENDING', 'BLOCKED', 'FAILED', 'DELETED')
    ),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMPTZ,
    PRIMARY KEY (artifact_id, target_kind)
);

CREATE INDEX artifact_gc_targets_work_idx
    ON artifact_gc_targets (state, eligible_at, created_at);

CREATE TABLE artifact_gc_cursor (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    last_created_at TIMESTAMPTZ,
    last_artifact_id UUID,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (
        (last_created_at IS NULL AND last_artifact_id IS NULL)
        OR (last_created_at IS NOT NULL AND last_artifact_id IS NOT NULL)
    )
);

INSERT INTO artifact_gc_cursor (singleton)
VALUES (TRUE)
ON CONFLICT (singleton) DO NOTHING;

-- Events remain append-only for ordinary application traffic. Retention GC may prune
-- old events only after opting into the narrow transaction-local maintenance mode.
CREATE OR REPLACE FUNCTION dataflow_reject_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE'
       AND TG_TABLE_NAME = 'events'
       AND current_setting('dataflow.retention_gc', TRUE) = 'on' THEN
        RETURN OLD;
    END IF;

    RAISE EXCEPTION '% is append-only/immutable', TG_TABLE_NAME;
END;
$$;
