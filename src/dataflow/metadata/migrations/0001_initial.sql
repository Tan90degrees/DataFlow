CREATE TABLE pipelines (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL DEFAULT 'default',
    name TEXT NOT NULL,
    description TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, name)
);

CREATE TABLE pipeline_versions (
    id UUID PRIMARY KEY,
    pipeline_id UUID NOT NULL REFERENCES pipelines(id),
    version INTEGER NOT NULL CHECK (version > 0),
    spec_json JSONB NOT NULL,
    spec_hash CHAR(64) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (pipeline_id, version)
);

CREATE TABLE pipeline_runs (
    id UUID PRIMARY KEY,
    pipeline_version_id UUID NOT NULL REFERENCES pipeline_versions(id),
    status TEXT NOT NULL CHECK (
        status IN ('CREATED', 'QUEUED', 'PLANNING', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED')
    ),
    parameters_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    cluster_profile TEXT,
    created_by TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    queued_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ
);

CREATE TABLE execution_units (
    id UUID PRIMARY KEY,
    pipeline_run_id UUID NOT NULL REFERENCES pipeline_runs(id),
    unit_key TEXT NOT NULL,
    unit_type TEXT NOT NULL DEFAULT 'ray-data',
    plan_json JSONB NOT NULL,
    dependencies_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    cluster_profile TEXT NOT NULL,
    status TEXT NOT NULL CHECK (
        status IN (
            'PENDING', 'READY', 'SUBMITTING', 'RUNNING', 'RETRY_WAIT',
            'SUCCEEDED', 'FAILED', 'CANCELLED', 'UNKNOWN'
        )
    ),
    current_attempt INTEGER NOT NULL DEFAULT 0 CHECK (current_attempt >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    UNIQUE (pipeline_run_id, unit_key)
);

CREATE TABLE execution_attempts (
    id UUID PRIMARY KEY,
    execution_unit_id UUID NOT NULL REFERENCES execution_units(id),
    attempt_number INTEGER NOT NULL CHECK (attempt_number > 0),
    status TEXT NOT NULL CHECK (
        status IN ('PENDING', 'SUBMITTING', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED', 'UNKNOWN')
    ),
    external_job_id TEXT,
    error_code TEXT,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    UNIQUE (execution_unit_id, attempt_number)
);

CREATE TABLE node_runs (
    id UUID PRIMARY KEY,
    pipeline_run_id UUID NOT NULL REFERENCES pipeline_runs(id),
    node_id TEXT NOT NULL,
    execution_unit_id UUID NOT NULL REFERENCES execution_units(id),
    status TEXT NOT NULL CHECK (
        status IN (
            'PENDING', 'READY', 'SUBMITTING', 'RUNNING', 'RETRY_WAIT',
            'SUCCEEDED', 'FAILED', 'CANCELLED', 'UNKNOWN'
        )
    ),
    input_artifacts_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    output_artifacts_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    metrics_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    UNIQUE (pipeline_run_id, node_id)
);

CREATE TABLE events (
    id BIGSERIAL PRIMARY KEY,
    pipeline_run_id UUID REFERENCES pipeline_runs(id),
    aggregate_type TEXT NOT NULL,
    aggregate_id UUID NOT NULL,
    event_type TEXT NOT NULL,
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_pipeline_runs_status ON pipeline_runs(status);
CREATE INDEX idx_execution_units_run_status ON execution_units(pipeline_run_id, status);
CREATE INDEX idx_execution_attempts_unit ON execution_attempts(execution_unit_id, attempt_number);
CREATE INDEX idx_events_run_id ON events(pipeline_run_id, id);

CREATE OR REPLACE FUNCTION dataflow_reject_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION '% is append-only/immutable', TG_TABLE_NAME;
END;
$$;

CREATE TRIGGER pipeline_versions_immutable
BEFORE UPDATE OR DELETE ON pipeline_versions
FOR EACH ROW EXECUTE FUNCTION dataflow_reject_mutation();

CREATE TRIGGER events_append_only
BEFORE UPDATE OR DELETE ON events
FOR EACH ROW EXECUTE FUNCTION dataflow_reject_mutation();
