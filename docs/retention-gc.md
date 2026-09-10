# Retention and durable artifact garbage collection

DataFlow keeps orchestration metadata durable while allowing object-store data and old events to be reclaimed with explicit retention windows.

## Safety model

Artifact rows remain append-only for auditability. Garbage collection deletes only S3-compatible object prefixes after re-validating the owning attempt/run state immediately before deletion. A committed artifact is never deleted while a non-terminal run references its URI through an execution-unit plan or node-run inputs.

Deletion is idempotent. If a worker crashes after deleting objects but before recording completion in PostgreSQL, a later pass can safely retry the same prefix. PostgreSQL stores a durable scan cursor and per-artifact GC target state so bounded passes resume without relying on process memory.

## Policy

The collector reads these environment variables:

- `DATAFLOW_RETENTION_RUN_SECONDS`: minimum age of a terminal run before committed artifacts or run events may be reclaimed. Default: 7 days.
- `DATAFLOW_RETENTION_EVENT_SECONDS`: minimum event age. Default: 30 days.
- `DATAFLOW_RETENTION_STAGING_SECONDS`: minimum age after a terminal attempt before its staging prefix may be reclaimed. Default: 1 day.
- `DATAFLOW_RETENTION_ARTIFACT_SECONDS`: minimum age of a committed artifact before its committed prefix may be reclaimed. Default: 30 days.
- `DATAFLOW_GC_SCAN_BATCH_SIZE`: maximum artifact rows scanned per pass. Default: 256.
- `DATAFLOW_GC_WORK_BATCH_SIZE`: maximum pending GC targets processed per pass. Default: 64.

All retention windows may be set to zero for tests or intentionally aggressive cleanup. Batch sizes must be positive.

## Running GC

Run one bounded pass with:

```bash
dataflow-gc
```

Inspect eligible work without mutating PostgreSQL or object storage with:

```bash
dataflow-gc --dry-run --pretty
```

Only one collector performs work at a time. A PostgreSQL advisory lock makes overlapping invocations safe; a loser exits with `lock_acquired=false`.

## What is reclaimed

Terminal attempts may have their attempt-specific staging prefixes removed after the staging window, including failed and cancelled attempts and successful staging data left behind after publication. Committed prefixes require the artifact to remain `COMMITTED`, the owning run to be terminal and old enough, the committed artifact to be old enough, and no non-terminal run to reference the committed URI.

Old events for sufficiently old terminal runs are pruned in a transaction-local maintenance mode. The ordinary append-only trigger remains active for all other application traffic.

Pipeline-run, execution-unit, attempt and artifact rows are retained as durable audit metadata in this phase; the run retention window is the lower bound used before reclaiming associated committed data and events.

## Observability

Each pass emits a structured summary including scanned/discovered/processed/deleted/blocked/failed counts and object/event deletion totals. Prometheus exposes GC target outcomes, deleted object totals and pruned event totals.
