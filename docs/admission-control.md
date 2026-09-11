# Scheduling admission control

English | [简体中文](zh-CN/admission-control.md)

DataFlow separates DAG readiness from execution admission. The scheduler decides when an
ExecutionUnit is `READY`; the admission controller decides whether that ready unit may own an
execution slot and reach the Ray/KubeRay reconciler.

Admission state is stored in PostgreSQL in `execution_admissions`. It is therefore shared by
controller replicas and survives controller restarts. Admission decisions are serialized with
a PostgreSQL transaction advisory lock in addition to the controller's long-lived HA leader
lock. The second lock protects the narrow failover overlap window from over-admitting work.

## Configuration

The controller reads four environment variables relevant to admission and scheduler fan-out:

- `DATAFLOW_ADMISSION_GLOBAL_LIMIT`: maximum admitted units across all ClusterProfiles. Omit it
  for no global capacity ceiling.
- `DATAFLOW_ADMISSION_PROFILE_LIMITS`: JSON object mapping ClusterProfile names to maximum
  admitted units, for example `{"cpu": 20, "gpu": 4}`. Profiles not present in the object have
  no profile-specific ceiling.
- `DATAFLOW_ADMISSION_MAX_NEW_PER_PASS`: maximum number of new admissions made in one
  controller reconciliation pass. Defaults to `32` even when capacity itself is unlimited.
- `DATAFLOW_CONTROLLER_MAX_RUNS_PER_PASS`: maximum number of active PipelineRuns sent through
  scheduler reconciliation in one controller pass. Defaults to `128`.

An admission limit of `0` intentionally blocks new admissions at that scope while preserving
already running/recovered work. `DATAFLOW_CONTROLLER_MAX_RUNS_PER_PASS` must be positive.

## Bounded scheduler work

The controller applies the run limit as a rotating window over the stable active-run ordering.
When more runs are active than fit in one pass, the next pass resumes at the next run instead
of repeatedly selecting the first N. This bounds scheduler CPU/database work while ensuring a
long-lived run near the front of the ordering cannot starve later runs.

The same selected window is used for the post-reconcile scheduler pass, so one controller pass
performs at most two scheduler reconciliations per selected run. Admission fairness remains
separate and durable in PostgreSQL; the scheduler cursor only determines which runs receive
DAG state advancement in a given pass.

## Slot lifetime

An admission belongs to the logical ExecutionUnit rather than an individual RayJob attempt.
The reservation is retained while the unit is `READY`, `SUBMITTING`, `RUNNING`, `UNKNOWN`, or
`RETRY_WAIT`. It is released when the unit becomes terminal or otherwise leaves the reserved
state set.

Keeping the reservation through retry backoff is deliberate: a retry never obtains a second
slot, and a controller restart cannot double-count the same logical unit. This favors control
plane stability over maximizing utilization during retry backoff. A future policy can make
retry-slot yielding configurable if workloads need that tradeoff.

On startup/reconcile, units already in `SUBMITTING`, `RUNNING`, `UNKNOWN`, or `RETRY_WAIT` are
reconstructed as admitted if their admission row is missing. Recovered work may mean the
observed active count is temporarily above a newly lowered configured limit; DataFlow will not
kill existing work, but it will make no new admission decision until capacity returns below the
limit.

## Fairness

Every admission receives a monotonic durable sequence. When multiple runs have eligible work,
DataFlow chooses the run whose most recent admission sequence is oldest, with stable creation
and identifier tie breakers. Within a run, the oldest queued unit is selected first.

This least-recently-admitted policy produces deterministic round-robin-like progress without
requiring process-local cursors. A controller restart reads the same sequence history and
continues the same fairness order.

Candidates whose ClusterProfile is at capacity are skipped so a saturated profile does not
head-of-line block eligible work from another profile.

## Observability

Admission transitions append durable events to the existing run event stream:

- `ADMISSION_QUEUED`
- `ADMISSION_ADMITTED`
- `ADMISSION_RECOVERED`
- `ADMISSION_RELEASED`

Each controller pass also emits an `admission_reconciled` structured log containing active
slot counts, per-profile counts, newly admitted work, queued/throttled work, recovered slots,
released slots, and the configured global/per-pass limits. Controller reconcile logs include
both the active-run count and the number of runs selected by the scheduler window.
