from __future__ import annotations

from dataflow.artifacts import (
    ArtifactFormat,
    S3Object,
    S3ParquetArtifactStorage,
    committed_artifact_uri,
    staging_artifact_uri_template,
)


class FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], tuple[bytes, str | None]] = {}
        self.copy_calls: list[tuple[str, str, str, str]] = []
        self.put_calls: list[tuple[str, str]] = []

    def seed(self, bucket: str, key: str, body: bytes, etag: str) -> None:
        self.objects[(bucket, key)] = (body, etag)

    def list_objects(self, bucket: str, prefix: str) -> list[S3Object]:
        return [
            S3Object(key=key, size=len(body), etag=etag)
            for (item_bucket, key), (body, etag) in self.objects.items()
            if item_bucket == bucket and key.startswith(prefix)
        ]

    def copy_object(
        self,
        *,
        source_bucket: str,
        source_key: str,
        destination_bucket: str,
        destination_key: str,
    ) -> None:
        self.copy_calls.append(
            (source_bucket, source_key, destination_bucket, destination_key)
        )
        self.objects[(destination_bucket, destination_key)] = self.objects[
            (source_bucket, source_key)
        ]

    def put_object(
        self,
        *,
        bucket: str,
        key: str,
        body: bytes,
        content_type: str,
    ) -> None:
        assert content_type == "application/json"
        self.put_calls.append((bucket, key))
        self.objects[(bucket, key)] = (body, None)

    def delete_objects(self, *, bucket: str, keys: list[str]) -> None:
        for key in keys:
            self.objects.pop((bucket, key), None)


def test_artifact_paths_are_deterministic_by_run_node_and_attempt() -> None:
    committed = committed_artifact_uri("s3://bucket/base/", "run-1", "node/a")
    template = staging_artifact_uri_template("s3://bucket/base/", "run-1", "node/a")

    assert committed == "s3://bucket/base/runs/run-1/artifacts/node-a/committed"
    assert template == (
        "s3://bucket/base/runs/run-1/artifacts/node-a/attempts/{attempt_number}/data"
    )


def test_s3_commit_copies_data_then_writes_completeness_marker() -> None:
    client = FakeS3Client()
    client.seed("bucket", "stage/part-000.parquet", b"abc", "etag-a")
    client.seed("bucket", "stage/part-001.parquet", b"defgh", "etag-b")
    storage = S3ParquetArtifactStorage(
        client,
        schema_loader=lambda uri: {"uri": uri, "fields": ["value"]},
    )

    metadata = storage.commit(
        "s3://bucket/stage",
        "s3://bucket/committed",
        format=ArtifactFormat.PARQUET,
    )

    assert metadata.size_bytes == 8
    assert metadata.schema_json == {
        "uri": "s3://bucket/committed",
        "fields": ["value"],
    }
    assert metadata.content_hash is not None
    assert [call[3] for call in client.copy_calls] == [
        "committed/part-000.parquet",
        "committed/part-001.parquet",
    ]
    assert client.put_calls == [("bucket", "committed/_dataflow_commit.json")]


def test_abort_deletes_only_attempt_staging_prefix() -> None:
    client = FakeS3Client()
    client.seed("bucket", "stage/attempt-1/part.parquet", b"failed", "one")
    client.seed("bucket", "other/part.parquet", b"keep", "two")
    storage = S3ParquetArtifactStorage(client)

    storage.abort("s3://bucket/stage/attempt-1")

    assert ("bucket", "stage/attempt-1/part.parquet") not in client.objects
    assert ("bucket", "other/part.parquet") in client.objects
