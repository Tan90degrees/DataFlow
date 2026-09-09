"""PostgreSQL persistence for immutable ClusterProfile revisions."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID, uuid4

from psycopg.types.json import Jsonb

from dataflow.artifact_repository import PostgresArtifactRepository
from dataflow.cluster_profiles import ClusterProfileSnapshot, ClusterProfileSpec
from dataflow.metadata.repository import (
    MetadataNotFoundError,
    canonical_spec_hash,
)


@dataclass(frozen=True, slots=True)
class ClusterProfileRecord:
    id: UUID
    name: str
    created_at: datetime


@dataclass(frozen=True, slots=True)
class ClusterProfileVersionRecord:
    id: UUID
    cluster_profile_id: UUID
    revision: int
    spec: ClusterProfileSpec
    spec_hash: str
    created_at: datetime

    def snapshot(self) -> ClusterProfileSnapshot:
        return ClusterProfileSnapshot(
            name=self.spec.name,
            revision=self.revision,
            spec_hash=self.spec_hash,
            spec=self.spec,
        )


class PostgresClusterProfileRepository(PostgresArtifactRepository):
    def create_cluster_profile(self, spec: ClusterProfileSpec) -> ClusterProfileVersionRecord:
        profile_id = uuid4()
        version_id = uuid4()
        payload = spec.model_dump(mode="json")
        digest = canonical_spec_hash(payload)
        with self._connect() as connection, connection.transaction():
            connection.execute(
                "INSERT INTO cluster_profiles (id, name) VALUES (%s, %s)",
                (profile_id, spec.name),
            )
            row = connection.execute(
                """
                INSERT INTO cluster_profile_versions (
                    id, cluster_profile_id, revision, spec_json, spec_hash
                ) VALUES (%s, %s, 1, %s, %s)
                RETURNING *
                """,
                (version_id, profile_id, Jsonb(payload), digest),
            ).fetchone()
        assert row is not None
        return self._version_from_row(row)

    def create_cluster_profile_revision(
        self,
        name: str,
        spec: ClusterProfileSpec,
    ) -> ClusterProfileVersionRecord:
        if spec.name != name:
            raise ValueError(
                f"ClusterProfileSpec.name {spec.name!r} must match profile name {name!r}"
            )
        payload = spec.model_dump(mode="json")
        digest = canonical_spec_hash(payload)
        with self._connect() as connection, connection.transaction():
            profile = connection.execute(
                "SELECT * FROM cluster_profiles WHERE name = %s FOR UPDATE",
                (name,),
            ).fetchone()
            if profile is None:
                raise MetadataNotFoundError(f"cluster profile not found: {name}")
            revision = connection.execute(
                """
                SELECT COALESCE(MAX(revision), 0) + 1 AS next_revision
                FROM cluster_profile_versions
                WHERE cluster_profile_id = %s
                """,
                (profile["id"],),
            ).fetchone()["next_revision"]
            row = connection.execute(
                """
                INSERT INTO cluster_profile_versions (
                    id, cluster_profile_id, revision, spec_json, spec_hash
                ) VALUES (%s, %s, %s, %s, %s)
                RETURNING *
                """,
                (uuid4(), profile["id"], revision, Jsonb(payload), digest),
            ).fetchone()
        assert row is not None
        return self._version_from_row(row)

    def get_cluster_profile(self, name: str) -> ClusterProfileRecord:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM cluster_profiles WHERE name = %s",
                (name,),
            ).fetchone()
        if row is None:
            raise MetadataNotFoundError(f"cluster profile not found: {name}")
        return ClusterProfileRecord(
            id=row["id"],
            name=row["name"],
            created_at=row["created_at"],
        )

    def get_current_cluster_profile(self, name: str) -> ClusterProfileVersionRecord:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT v.*
                FROM cluster_profiles AS p
                JOIN cluster_profile_versions AS v ON v.cluster_profile_id = p.id
                WHERE p.name = %s
                ORDER BY v.revision DESC
                LIMIT 1
                """,
                (name,),
            ).fetchone()
        if row is None:
            raise MetadataNotFoundError(f"cluster profile not found: {name}")
        return self._version_from_row(row)

    def list_cluster_profile_versions(self, name: str) -> list[ClusterProfileVersionRecord]:
        profile = self.get_cluster_profile(name)
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM cluster_profile_versions
                WHERE cluster_profile_id = %s
                ORDER BY revision
                """,
                (profile.id,),
            ).fetchall()
        return [self._version_from_row(row) for row in rows]

    def list_current_cluster_profiles(self) -> list[ClusterProfileVersionRecord]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT DISTINCT ON (p.name) v.*
                FROM cluster_profiles AS p
                JOIN cluster_profile_versions AS v ON v.cluster_profile_id = p.id
                ORDER BY p.name, v.revision DESC
                """
            ).fetchall()
        return [self._version_from_row(row) for row in rows]

    @staticmethod
    def _version_from_row(row) -> ClusterProfileVersionRecord:
        return ClusterProfileVersionRecord(
            id=row["id"],
            cluster_profile_id=row["cluster_profile_id"],
            revision=row["revision"],
            spec=ClusterProfileSpec.model_validate(row["spec_json"]),
            spec_hash=row["spec_hash"].strip(),
            created_at=row["created_at"],
        )


__all__ = [
    "ClusterProfileRecord",
    "ClusterProfileVersionRecord",
    "PostgresClusterProfileRepository",
]
