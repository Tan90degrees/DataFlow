CREATE TABLE cluster_profiles (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE cluster_profile_versions (
    id UUID PRIMARY KEY,
    cluster_profile_id UUID NOT NULL REFERENCES cluster_profiles(id),
    revision INTEGER NOT NULL CHECK (revision > 0),
    spec_json JSONB NOT NULL,
    spec_hash CHAR(64) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (cluster_profile_id, revision)
);

CREATE INDEX idx_cluster_profile_versions_profile_revision
ON cluster_profile_versions (cluster_profile_id, revision DESC);

CREATE OR REPLACE FUNCTION dataflow_reject_cluster_profile_version_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'cluster profile versions are immutable';
END;
$$;

CREATE TRIGGER cluster_profile_versions_immutable
BEFORE UPDATE OR DELETE ON cluster_profile_versions
FOR EACH ROW EXECUTE FUNCTION dataflow_reject_cluster_profile_version_mutation();
