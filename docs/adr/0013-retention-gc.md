# ADR 0013: Retention and resumable artifact garbage collection

English | [简体中文](../zh-CN/adr/0013-retention-gc.md)

## Status

Accepted

## Context

DataFlow intentionally persists workflow state, attempt history and artifact publication records in PostgreSQL. Long-running installations also accumulate attempt staging data, committed durable artifacts and events in object storage/PostgreSQL. Cleanup must not weaken durable recovery or delete data still referenced by active work.

The existing `artifacts` table is append-only and terminal artifact records are immutable. S3 publication is copy-on-commit and uses run-scoped, attempt-specific staging prefixes plus stable committed prefixes.

## Decision

Keep artifact metadata as durable audit state and garbage-collect physical object prefixes separately. GC maintains a durable PostgreSQL cursor plus per-artifact target records with `PENDING`, `BLOCKED`, `FAILED` and `DELETED` states.

A staging target becomes eligible only after its execution attempt is terminal and its staging retention window has elapsed. A committed target additionally requires the owning run to be terminal, the run/artifact retention windows to have elapsed, the artifact metadata to remain `COMMITTED`, and no non-terminal run to reference the committed URI.

Every target is revalidated immediately before object deletion. Object-prefix deletion is idempotent. Therefore a crash between physical deletion and PostgreSQL completion recording is recovered by safely repeating deletion.

Collectors serialize execution with a PostgreSQL advisory lock. Work is bounded by explicit scan and work batch sizes. Dry-run performs discovery/reporting only and does not create targets, move the cursor, prune events or touch object storage.

Old events for old terminal runs may be deleted only inside a transaction that sets `dataflow.retention_gc=on`. The normal event mutation trigger remains append-only outside that narrow maintenance transaction.

## Consequences

- Active runs win over retention pressure; referenced committed data is blocked rather than deleted.
- Failed/cancelled/successful terminal attempt staging debris can be reclaimed independently from durable metadata.
- Repeated and interrupted passes are safe without distributed leases in object storage.
- Artifact, attempt, execution-unit and pipeline-run rows remain available for audit/debugging. This phase does not implement hard deletion of those records.
- A blocked target is reconsidered on later passes and can become eligible once its active references disappear.
- Object-store lifecycle policies remain an operator concern and are not required for correctness.
