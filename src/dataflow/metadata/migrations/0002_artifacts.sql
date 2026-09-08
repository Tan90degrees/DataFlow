CREATE TABLE artifacts (
    id UUID PRIMARY KEY,
    pipeline_run_id UUID NOT NULL REFERENCES pipeline_runs(id),
    node_id TEXT NOT NULL,
    execution_unit_id UUID NOT NULL REFERENCES execution_units(id),
    attempt_number INTEGER NOT NULL CHECK (attempt_number > 0),
    format TEXT NOT NULL CHECK (format IN ('parquet')),
    state TEXT NOT NULL CHECK (state IN ('STAGING', 'COMMITTED', 'ABORTED')),
    staging_uri TEXT NOT NULL,
    committed_uri TEXT NOT NULL,
    schema_json JSONB,
    size_bytes BIGINT CHECK (size_bytes IS NULL OR size_bytes >= 0),
    row_count BIGINT CHECK (row_count IS NULL OR row_count >= 0),
    content_hash TEXT,
    checkpoint BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    committed_at TIMESTAMPTZ,
    aborted_at TIMESTAMPTZ,
    UNIQUE (pipeline_run_id, node_id, attempt_number),
    FOREIGN KEY (execution_unit_id, attempt_number)
        REFERENCES execution_attempts(execution_unit_id, attempt_number)
);

CREATE UNIQUE INDEX uq_artifacts_committed_logical_output
ON artifacts (pipeline_run_id, node_id)
WHERE state = 'COMMITTED';

CREATE INDEX idx_artifacts_run_state
ON artifacts (pipeline_run_id, state, node_id);

CREATE OR REPLACE FUNCTION dataflow_validate_artifact_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'artifacts are append-only';
    END IF;

    IF OLD.state IN ('COMMITTED', 'ABORTED') THEN
        RAISE EXCEPTION 'terminal artifact % is immutable', OLD.id;
    END IF;

    IF OLD.state = 'STAGING' AND NEW.state NOT IN ('COMMITTED', 'ABORTED') THEN
        RAISE EXCEPTION 'invalid artifact transition: % -> %', OLD.state, NEW.state;
    END IF;

    IF NEW.pipeline_run_id <> OLD.pipeline_run_id
       OR NEW.node_id <> OLD.node_id
       OR NEW.execution_unit_id <> OLD.execution_unit_id
       OR NEW.attempt_number <> OLD.attempt_number
       OR NEW.staging_uri <> OLD.staging_uri
       OR NEW.committed_uri <> OLD.committed_uri
       OR NEW.format <> OLD.format THEN
        RAISE EXCEPTION 'artifact identity is immutable';
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER artifacts_state_machine
BEFORE UPDATE OR DELETE ON artifacts
FOR EACH ROW EXECUTE FUNCTION dataflow_validate_artifact_mutation();
