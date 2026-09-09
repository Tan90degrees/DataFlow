"""Application service that composes compiler, metadata, and scheduler semantics."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID, uuid4

from dataflow.api_repository import ApiRepository
from dataflow.artifact_repository import ArtifactRecord
from dataflow.compiler import PipelineCompiler, PipelineSpec
from dataflow.metadata.repository import (
    EventRecord,
    ExecutionAttemptRecord,
    ExecutionUnitRecord,
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
        self._preflight_compile(spec)
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
        self._preflight_compile(spec)

        run = self._repository.create_pipeline_run(
            version.id,
            parameters=parameters,
            cluster_profile=spec.cluster_profile,
            created_by=created_by,
        )
        graph = self._compiler.compile(spec, run_id=str(run.id))
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

    def _preflight_compile(self, spec: PipelineSpec) -> None:
        self._compiler.compile(spec, run_id=str(uuid4()))


__all__ = [
    "ControlPlaneService",
    "PipelineSnapshot",
    "RunSnapshot",
    "UnitSnapshot",
]
