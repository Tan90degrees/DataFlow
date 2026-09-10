from __future__ import annotations

import pytest

from dataflow.artifacts import S3Object, S3ParquetArtifactStorage
from dataflow.observability import PrometheusMetrics
from dataflow.retention import RetentionPolicy


class _FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def list_objects(self, bucket: str, prefix: str) -> list[S3Object]:
        return [
            S3Object(key=key, size=len(body))
            for (item_bucket, key), body in self.objects.items()
            if item_bucket == bucket and key.startswith(prefix)
        ]

    def copy_object(self, **_kwargs) -> None:
        raise AssertionError("copy is not used by GC")

    def put_object(self, **_kwargs) -> None:
        raise AssertionError("put is not used by GC")

    def delete_objects(self, *, bucket: str, keys: list[str]) -> None:
        for key in keys:
            self.objects.pop((bucket, key), None)


def test_retention_policy_reads_explicit_environment() -> None:
    policy = RetentionPolicy.from_env(
        {
            "DATAFLOW_RETENTION_RUN_SECONDS": "10",
            "DATAFLOW_RETENTION_EVENT_SECONDS": "20",
            "DATAFLOW_RETENTION_STAGING_SECONDS": "30",
            "DATAFLOW_RETENTION_ARTIFACT_SECONDS": "40",
            "DATAFLOW_GC_SCAN_BATCH_SIZE": "5",
            "DATAFLOW_GC_WORK_BATCH_SIZE": "2",
        }
    )

    assert policy == RetentionPolicy(
        terminal_run_seconds=10,
        event_seconds=20,
        staging_seconds=30,
        committed_artifact_seconds=40,
        scan_batch_size=5,
        work_batch_size=2,
    )


def test_retention_windows_allow_zero_but_batches_must_be_positive() -> None:
    assert RetentionPolicy(
        terminal_run_seconds=0,
        event_seconds=0,
        staging_seconds=0,
        committed_artifact_seconds=0,
    ).staging_seconds == 0

    with pytest.raises(ValueError, match="scan_batch_size must be positive"):
        RetentionPolicy(scan_batch_size=0)
    with pytest.raises(ValueError, match="must be an integer"):
        RetentionPolicy.from_env({"DATAFLOW_RETENTION_RUN_SECONDS": "later"})


def test_s3_delete_prefix_is_idempotent_and_does_not_delete_siblings() -> None:
    client = _FakeS3Client()
    client.objects[("bucket", "runs/a/committed/part.parquet")] = b"data"
    client.objects[("bucket", "runs/a/committed/_dataflow_commit.json")] = b"marker"
    client.objects[("bucket", "runs/a/attempts/001/data/part.parquet")] = b"keep"
    storage = S3ParquetArtifactStorage(client)

    assert storage.delete_prefix("s3://bucket/runs/a/committed") == 2
    assert storage.delete_prefix("s3://bucket/runs/a/committed") == 0
    assert ("bucket", "runs/a/attempts/001/data/part.parquet") in client.objects


def test_prometheus_metrics_expose_gc_outcomes() -> None:
    metrics = PrometheusMetrics()
    metrics.observe_gc_target(target="committed", outcome="deleted", objects_deleted=3)
    metrics.observe_gc_event_prune(count=4)

    payload, _ = metrics.render()
    text = payload.decode()
    assert 'dataflow_gc_targets_total{outcome="deleted",target="committed"} 1.0' in text
    assert 'dataflow_gc_objects_deleted_total{target="committed"} 3.0' in text
    assert "dataflow_gc_events_pruned_total 4.0" in text
