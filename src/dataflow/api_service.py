"""Application service that composes compiler, metadata, and scheduler semantics."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID, uuid4

from dataflow.api_repository import ApiRepository
from dataflow.artifact_repository import ArtifactRecord
from dataflow.cluster_profile_repository import ClusterProfileVersionRecord
from dataflow.cluster_profiles import (
    ClusterProfileSnapshot,
    ClusterProfileSpec,
    default_cluster_profile,
)
from dataflow.compiler import PipelineCompiler, PipelineSpec
from dataflow.metadata.repository import (
    EventRecord,
    ExecutionAttemptRecord,
    ExecutionUnitRecord,
    MetadataNotFoundError,
    PipelineRecord,
    PipelineRunRecord,
    PipelineVersionRecord,
)
from dataflow.scheduler import Scheduler
from dataflow.state import PipelineRunStatus


@dataclass(frozen=True, slots=True)
class PipelineSnapshot:
    pipeline: PipelineRecord
    versions: list[PipelineVersionRecord]


@dataclass(frozen=True, slots=True)
class UnitSnapshot:
    unit: ExecutionUnitRecord
    attempts: list[ExecutionAttemptRecord]


@dataclass(frozen=True, slots=True)
class RunSnapshot:
    run: PipelineRunRecord
    units: list[UnitSnapshot]
    artifacts: list[ArtifactRecord]


@dataclass(frozen=True, slots=True)
class ClusterProfileHistory:
    current: ClusterProfileVersionRecord | ClusterProfileSnapshot
    versions: list[ClusterProfileVersionRecord]


class ControlPlaneService:
    """Thin use-case layer shared by HTTP handlers and future SDK adapters."""

    def __init__(
        self,
        repository: ApiRepository,
        *,
        compiler: PipelineCompiler | None = None,
    ) -> None:
        self._repository = repository
        self._compiler = compiler or PipelineCompiler()
        self._scheduler = Scheduler(repository)

    def ready(self) -> bool:
        return self._repository.ping()

    def create_pipeline(
        self,
        *,
        name: str,
        tenant_id: str = "default",
        description: str | None = None,
    ) -> PipelineRecord:
        return self._repository.create_pipeline(
            name=name,
            tenant_id=tenant_id,
            description=description,
        )

    def list_pipelines(
        self,
        *,
        tenant_id: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[PipelineRecord]:
        return self._repository.list_pipelines(
            tenant_id=tenant_id,
            limit=limit,
            offset=offset,
        )

    def get_pipeline(self, pipeline_id: UUID) -> PipelineSnapshot:
        pipeline = self._repository.get_pipeline(pipeline_id)
        versions = self._repository.list_pipeline_versions(pipeline_id)
        return PipelineSnapshot(pipeline=pipeline, versions=versions)

    def create_cluster_profile(
        self,
        spec: ClusterProfileSpec,
    ) -> ClusterProfileVersionRecord:
        return self._repository.create_cluster_profile(spec)

    def update_cluster_profile(
        self,
        name: str,
        spec: ClusterProfileSpec,
    ) -> ClusterProfileVersionRecord:
        return self._repository.create_cluster_profile_revision(name, spec)

    def list_cluster_profiles(self) -> list[ClusterProfileVersionRecord | ClusterProfileSnapshot]:
        current = self._repository.list_current_cluster_profiles()
        if not any(record.spec.name == "default" for record in current):
            return [default_cluster_profile(), *current]
        return current

    def get_cluster_profile(self, name: str) -> ClusterProfileHistory:
        try:
            current = self._repository.get_current_cluster_profile(name)
        except MetadataNotFoundError:
            if name != "default":
                raise
            return ClusterProfileHistory(current=default_cluster_profile(), versions=[])
        return ClusterProfileHistory(
            current=current,
            versions=self._repository.list_cluster_profile_versions(name),
        )

    def create_pipeline_version(
        self,
        pipeline_id: UUID,
        spec: PipelineSpec,
    ) -> PipelineVersionRecord:
        pipeline = self._repository.get_pipeline(pipeline_id)
        if spec.name != pipeline.name:
            raise ValueError(
                f"PipelineSpec.name {spec.name!r} must match pipeline name {pipeline.name!r}"
            )
        profiles = self._resolve_cluster_profiles(spec)
        self._preflight_compile(spec, profiles)
        return self._repository.create_pipeline_version(
            pipeline_id,
            spec.model_dump(mode="json", by_alias=True),
        )

    def create_run(
        self,
        pipeline_version_id: UUID,
        *,
        parameters: dict | None = None,
        created_by: str | None = None,
    ) -> RunSnapshot:
        version = self._repository.get_pipeline_version(pipeline_version_id)
        spec = PipelineSpec.model_validate(version.spec_json)
        profiles = self._resolve_cluster_profiles(spec)
        self._preflight_compile(spec, profiles)

        run = self._repository.create_pipeline_run(
            version.id,
            parameters=parameters,
            cluster_profile=spec.cluster_profile,
            created_by=created_by,
        )
        graph = self._compiler.compile(
            spec,
            run_id=str(run.id),
            cluster_profiles=profiles,
        )
        self._repository.create_execution_graph(run.id, graph)
        self._repository.transition_run_status(
            run.id,
            PipelineRunStatus.QUEUED,
            expected=PipelineRunStatus.CREATED,
        )
        self._repository.transition_run_status(
            run.id,
            PipelineRunStatus.PLANNING,
            expected=PipelineRunStatus.QUEUED,
        )
        self._repository.transition_run_status(
            run.id,
            PipelineRunStatus.RUNNING,
            expected=PipelineRunStatus.PLANNING,
        )
        self._scheduler.reconcile_run(run.id)
        return self.get_run(run.id)

    def get_run(self, run_id: UUID) -> RunSnapshot:
        run = self._repository.get_run(run_id)
        units = [
            UnitSnapshot(
                unit=unit,
                attempts=self._repository.list_attempts(unit.id),
            )
            for unit in self._repository.list_units(run_id)
        ]
        artifacts = self._repository.list_run_artifacts(run_id)
        return RunSnapshot(run=run, units=units, artifacts=artifacts)

    def cancel_run(self, run_id: UUID) -> RunSnapshot:
        self._scheduler.cancel_run(run_id)
        return self.get_run(run_id)

    def list_events(self, run_id: UUID) -> list[EventRecord]:
        self._repository.get_run(run_id)
        return self._repository.list_events(run_id)

    def list_artifacts(self, run_id: UUID) -> list[ArtifactRecord]:
        self._repository.get_run(run_id)
        return self._repository.list_run_artifacts(run_id)

    def _resolve_cluster_profiles(
        self,
        spec: PipelineSpec,
    ) -> dict[str, ClusterProfileSnapshot]:
        names = {spec.cluster_profile}
        names.update(node.cluster_profile for node in spec.nodes if node.cluster_profile)
        resolved: dict[str, ClusterProfileSnapshot] = {}
        for name in sorted(names):
            try:
                record = self._repository.get_current_cluster_profile(name)
            except MetadataNotFoundError:
                if name != "default":
                    raise
                resolved[name] = default_cluster_profile()
            else:
                resolved[name] = record.snapshot()
        return resolved

    def _preflight_compile(
        self,
        spec: PipelineSpec,
        cluster_profiles: dict[str, ClusterProfileSnapshot],
    ) -> None:
        self._compiler.compile(
            spec,
            run_id=str(uuid4()),
            cluster_profiles=cluster_profiles,
        )


__all__ = [
    "ClusterProfileHistory",
    "ControlPlaneService",
    "PipelineSnapshot",
    "RunSnapshot",
    "UnitSnapshot",
]
