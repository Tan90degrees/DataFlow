CREATE TABLE execution_admissions (
    execution_unit_id UUID PRIMARY KEY REFERENCES execution_units(id) ON DELETE CASCADE,
    pipeline_run_id UUID NOT NULL REFERENCES pipeline_runs(id) ON DELETE CASCADE,
    cluster_profile TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('WAITING', 'ADMITTED', 'RELEASED')),
    admission_sequence BIGINT,
    queued_at TIMESTAMPTZ,
    admitted_at TIMESTAMPTZ,
    released_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX execution_admissions_state_idx
    ON execution_admissions (state, cluster_profile, pipeline_run_id);
CREATE INDEX execution_admissions_run_sequence_idx
    ON execution_admissions (pipeline_run_id, admission_sequence DESC);

CREATE TABLE admission_scheduler_state (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    next_sequence BIGINT NOT NULL DEFAULT 1 CHECK (next_sequence > 0)
);

INSERT INTO admission_scheduler_state (singleton, next_sequence)
VALUES (TRUE, 1)
ON CONFLICT (singleton) DO NOTHING;
