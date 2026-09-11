# Durable artifacts and checkpoints

English | [简体中文](zh-CN/ARTIFACTS.md)

DataFlow keeps Ray Data flow transient inside an execution island and materializes only when the compiled DAG crosses an execution-unit boundary.

## Logical output identity

A durable logical output is identified by `(run_id, node_id)`. Retries do not change that identity.

For `artifact_base_uri: s3://bucket/dataflow`, the compiler produces:

```text
s3://bucket/dataflow/runs/<run-id>/artifacts/<node-id>/committed
```

Each workflow attempt writes to its own staging prefix:

```text
s3://bucket/dataflow/runs/<run-id>/artifacts/<node-id>/attempts/<NNN>/data
```

The runtime resolves `<NNN>` from `DATAFLOW_ATTEMPT_NUMBER`, which is injected by the attempt-specific KubeRay RayJob.

## Publication sequence

```text
Ray Data write_parquet
        |
        v
attempt staging prefix
        |
RayJob SUCCEEDED
        |
        v
ArtifactManager.commit_outputs
        |
        +--> copy data to stable committed prefix
        +--> write _dataflow_commit.json last
        +--> PostgreSQL STAGING -> COMMITTED
        |
        v
downstream unit becomes eligible after producer unit succeeds
```

PostgreSQL is the logical visibility source of truth. Downstream execution plans use only the stable committed URI and do not persist or recover from Ray `ObjectRef` values.

## Failure behavior

- RayJob failure or cancellation: artifact row becomes `ABORTED`; staging deletion is best effort.
- Transient object-store publication failure after RayJob success: unit/attempt become `UNKNOWN`; reconciliation retries publication without resubmitting the Ray computation.
- Permanent publication failure: staging output is aborted and the unit/attempt fail.
- Repeated successful reconciliation: an existing `COMMITTED` artifact is returned idempotently.

## Explicit checkpoint

Set a data edge boundary to `checkpoint` to force materialization even when adjacent operators would otherwise be fusible:

```json
{
  "from": "expensive_inference",
  "to": "enrich",
  "kind": "data",
  "boundary": "checkpoint"
}
```

The resulting artifact has `checkpoint=true` in PostgreSQL and can be used as the durable restart point after Ray cluster loss.

## S3-compatible control-plane dependency

Install the publisher dependency in the control plane:

```bash
pip install -e '.[artifacts]'
```

`Boto3S3ObjectClient` accepts normal boto3 client configuration, so S3-compatible endpoints can be supplied through the same client kwargs/credential mechanisms used by boto3.
