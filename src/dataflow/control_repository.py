"""Control-plane read model layered on the durable metadata repository."""

from __future__ import annotations

from uuid import UUID

from dataflow.metadata.repository import (
    ExecutionAttemptRecord,
    ExecutionUnitRecord,
    PipelineRunRecord,
    PostgresMetadataRepository,
)
from dataflow.state import TERMINAL_RUN_STATUSES


class PostgresControlRepository(PostgresMetadataRepository):
    """PostgreSQL repository with scheduler/reconciler-specific durable reads."""

    def list_units(self, run_id: UUID) -> list[ExecutionUnitRecord]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM execution_units
                WHERE pipeline_run_id = %s
                ORDER BY created_at, unit_key
                """,
                (run_id,),
            ).fetchall()
        return [self._unit_from_row(row) for row in rows]

    def get_current_attempt(self, unit_id: UUID) -> ExecutionAttemptRecord | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT a.*
                FROM execution_units AS u
                JOIN execution_attempts AS a
                  ON a.execution_unit_id = u.id
                 AND a.attempt_number = u.current_attempt
                WHERE u.id = %s
                """,
                (unit_id,),
            ).fetchone()
        if row is None:
            return None
        return self._attempt_from_row(row)

    def list_active_runs(self) -> list[PipelineRunRecord]:
        terminal = [status.value for status in TERMINAL_RUN_STATUSES]
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM pipeline_runs
                WHERE status <> ALL(%s)
                ORDER BY created_at, id
                """,
                (terminal,),
            ).fetchall()
        return [self._run_from_row(row) for row in rows]


__all__ = ["PostgresControlRepository"]
