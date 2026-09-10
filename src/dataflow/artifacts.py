"""Durable artifact contracts, path conventions, and S3-compatible storage."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Protocol
from urllib.parse import urlparse

from pydantic import BaseModel, ConfigDict, Field


class ArtifactFormat(StrEnum):
    PARQUET = "parquet"


class ArtifactDurability(StrEnum):
    """Whether data is process-local/transient or recoverable across clusters."""

    TRANSIENT = "transient"
    DURABLE = "durable"


class ArtifactState(StrEnum):
    STAGING = "STAGING"
    COMMITTED = "COMMITTED"
    ABORTED = "ABORTED"


class ArtifactRef(BaseModel):
    """Logical durable artifact reference safe to persist in orchestration state."""

    model_config = ConfigDict(extra="forbid")

    run_id: str = Field(min_length=1)
    node_id: str = Field(min_length=1)
    format: ArtifactFormat = ArtifactFormat.PARQUET
    uri: str = Field(min_length=1)
    schema_json: dict[str, Any] | None = None
    size_bytes: int | None = Field(default=None, ge=0)
    row_count: int | None = Field(default=None, ge=0)
    content_hash: str | None = None


class ArtifactOutputSpec(BaseModel):
    """Physical-plan contract for one durable logical node output."""

    model_config = ConfigDict(extra="forbid")

    run_id: str = Field(min_length=1)
    node_id: str = Field(min_length=1)
    operator_id: str = Field(min_length=1)
    format: ArtifactFormat = ArtifactFormat.PARQUET
    durability: ArtifactDurability = ArtifactDurability.DURABLE
    committed_uri: str = Field(min_length=1)
    staging_uri_template: str = Field(min_length=1)
    checkpoint: bool = False

    def staging_uri(self, attempt_number: int) -> str:
        if attempt_number < 1:
            raise ValueError("attempt_number must be at least 1")
        return self.staging_uri_template.replace(
            "{attempt_number}",
            f"{attempt_number:03d}",
        )

    def committed_ref(self) -> ArtifactRef:
        return ArtifactRef(
            run_id=self.run_id,
            node_id=self.node_id,
            format=self.format,
            uri=self.committed_uri,
        )


@dataclass(frozen=True, slots=True)
class ArtifactStorageMetadata:
    size_bytes: int
    schema_json: dict[str, Any] | None = None
    row_count: int | None = None
    content_hash: str | None = None


class ArtifactCommitError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        error_code: str = "ARTIFACT_COMMIT_FAILED",
        retryable: bool = True,
    ) -> None:
        super().__init__(message)
        self.error_code = error_code
        self.retryable = retryable


class ArtifactStorage(Protocol):
    def commit(
        self,
        staging_uri: str,
        committed_uri: str,
        *,
        format: ArtifactFormat,
    ) -> ArtifactStorageMetadata: ...

    def abort(self, staging_uri: str) -> None: ...


@dataclass(frozen=True, slots=True)
class S3Object:
    key: str
    size: int
    etag: str | None = None


class S3ObjectClient(Protocol):
    def list_objects(self, bucket: str, prefix: str) -> list[S3Object]: ...

    def copy_object(
        self,
        *,
        source_bucket: str,
        source_key: str,
        destination_bucket: str,
        destination_key: str,
    ) -> None: ...

    def put_object(
        self,
        *,
        bucket: str,
        key: str,
        body: bytes,
        content_type: str,
    ) -> None: ...

    def delete_objects(self, *, bucket: str, keys: list[str]) -> None: ...


class Boto3S3ObjectClient:
    """Thin boto3 adapter; boto3 remains optional until S3 artifacts are enabled."""

    def __init__(self, client: Any) -> None:
        self._client = client

    @classmethod
    def from_default_config(cls, **client_kwargs: Any) -> Boto3S3ObjectClient:
        try:
            import boto3
        except ImportError as error:
            raise RuntimeError(
                "boto3 is required for S3 artifact storage; install the 'artifacts' extra"
            ) from error
        return cls(boto3.client("s3", **client_kwargs))

    def list_objects(self, bucket: str, prefix: str) -> list[S3Object]:
        paginator = self._client.get_paginator("list_objects_v2")
        objects: list[S3Object] = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for item in page.get("Contents", []):
                objects.append(
                    S3Object(
                        key=item["Key"],
                        size=int(item.get("Size", 0)),
                        etag=str(item.get("ETag", "")).strip('"') or None,
                    )
                )
        return objects

    def copy_object(
        self,
        *,
        source_bucket: str,
        source_key: str,
        destination_bucket: str,
        destination_key: str,
    ) -> None:
        self._client.copy_object(
            Bucket=destination_bucket,
            Key=destination_key,
            CopySource={"Bucket": source_bucket, "Key": source_key},
        )

    def put_object(
        self,
        *,
        bucket: str,
        key: str,
        body: bytes,
        content_type: str,
    ) -> None:
        self._client.put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            ContentType=content_type,
        )

    def delete_objects(self, *, bucket: str, keys: list[str]) -> None:
        for offset in range(0, len(keys), 1000):
            batch = keys[offset : offset + 1000]
            if batch:
                self._client.delete_objects(
                    Bucket=bucket,
                    Delete={"Objects": [{"Key": key} for key in batch], "Quiet": True},
                )


class S3ParquetArtifactStorage:
    """Copy-on-commit Parquet publisher for S3-compatible object stores.

    Ray Data writes to an immutable attempt-specific staging prefix. Every publish
    first clears the not-yet-committed stable prefix, then copies the current
    attempt's objects and writes the commit marker last. PostgreSQL decides logical
    visibility; the marker provides an additional object-store completeness signal.
    """

    COMMIT_MARKER = "_dataflow_commit.json"

    def __init__(
        self,
        client: S3ObjectClient,
        *,
        schema_loader: Callable[[str], dict[str, Any] | None] | None = None,
    ) -> None:
        self._client = client
        self._schema_loader = schema_loader

    def commit(
        self,
        staging_uri: str,
        committed_uri: str,
        *,
        format: ArtifactFormat,
    ) -> ArtifactStorageMetadata:
        if format is not ArtifactFormat.PARQUET:
            raise ArtifactCommitError(
                f"unsupported artifact format: {format}",
                error_code="UNSUPPORTED_ARTIFACT_FORMAT",
                retryable=False,
            )

        source_bucket, source_prefix = _parse_s3_uri(staging_uri)
        destination_bucket, destination_prefix = _parse_s3_uri(committed_uri)
        source_list_prefix = _directory_prefix(source_prefix)
        destination_list_prefix = _directory_prefix(destination_prefix)
        source_objects = [
            item
            for item in self._client.list_objects(source_bucket, source_list_prefix)
            if item.key != _join_key(destination_list_prefix, self.COMMIT_MARKER)
        ]
        if not source_objects:
            raise ArtifactCommitError(f"staging artifact is empty: {staging_uri}")

        total_size = 0
        digest = hashlib.sha256()
        try:
            stale_objects = self._client.list_objects(
                destination_bucket,
                destination_list_prefix,
            )
            self._client.delete_objects(
                bucket=destination_bucket,
                keys=[item.key for item in stale_objects],
            )

            for item in sorted(source_objects, key=lambda value: value.key):
                relative = item.key[len(source_list_prefix) :]
                if not relative:
                    continue
                destination_key = _join_key(destination_prefix, relative)
                self._client.copy_object(
                    source_bucket=source_bucket,
                    source_key=item.key,
                    destination_bucket=destination_bucket,
                    destination_key=destination_key,
                )
                total_size += item.size
                digest.update(relative.encode("utf-8"))
                digest.update(str(item.size).encode("ascii"))
                if item.etag:
                    digest.update(item.etag.encode("utf-8"))

            schema_json = self._schema_loader(committed_uri) if self._schema_loader else None
            content_hash = digest.hexdigest()
            marker = json.dumps(
                {
                    "format": format.value,
                    "source": staging_uri,
                    "uri": committed_uri,
                    "size_bytes": total_size,
                    "schema": schema_json,
                    "content_hash": content_hash,
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
            self._client.put_object(
                bucket=destination_bucket,
                key=_join_key(destination_prefix, self.COMMIT_MARKER),
                body=marker,
                content_type="application/json",
            )
        except ArtifactCommitError:
            raise
        except Exception as error:
            raise ArtifactCommitError(str(error)) from error

        return ArtifactStorageMetadata(
            size_bytes=total_size,
            schema_json=schema_json,
            content_hash=content_hash,
        )

    def abort(self, staging_uri: str) -> None:
        self._delete_prefix(staging_uri, error_code="ARTIFACT_ABORT_FAILED")

    def delete_prefix(self, uri: str) -> int:
        """Delete every object under a non-root S3 URI; repeating an empty delete is safe."""
        return self._delete_prefix(uri, error_code="ARTIFACT_GC_DELETE_FAILED")

    def _delete_prefix(self, uri: str, *, error_code: str) -> int:
        bucket, prefix = _parse_s3_uri(uri)
        try:
            objects = self._client.list_objects(bucket, _directory_prefix(prefix))
            self._client.delete_objects(bucket=bucket, keys=[item.key for item in objects])
            return len(objects)
        except Exception as error:
            raise ArtifactCommitError(str(error), error_code=error_code) from error


def committed_artifact_uri(base_uri: str, run_id: str, node_id: str) -> str:
    return (
        f"{base_uri.rstrip('/')}/runs/{run_id}/artifacts/"
        f"{_path_segment(node_id)}/committed"
    )


def staging_artifact_uri_template(base_uri: str, run_id: str, node_id: str) -> str:
    return (
        f"{base_uri.rstrip('/')}/runs/{run_id}/artifacts/"
        f"{_path_segment(node_id)}/attempts/{{attempt_number}}/data"
    )


def _parse_s3_uri(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    prefix = parsed.path.lstrip("/").rstrip("/")
    if parsed.scheme != "s3" or not parsed.netloc or not prefix:
        raise ArtifactCommitError(
            f"expected s3://bucket/non-empty-prefix URI, got {uri!r}",
            error_code="INVALID_ARTIFACT_URI",
            retryable=False,
        )
    return parsed.netloc, prefix


def _directory_prefix(prefix: str) -> str:
    return prefix.rstrip("/") + "/"


def _join_key(prefix: str, suffix: str) -> str:
    if not prefix:
        return suffix.lstrip("/")
    return f"{prefix.rstrip('/')}/{suffix.lstrip('/')}"


def _path_segment(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "-" for ch in value)


__all__ = [
    "ArtifactCommitError",
    "ArtifactDurability",
    "ArtifactFormat",
    "ArtifactOutputSpec",
    "ArtifactRef",
    "ArtifactState",
    "ArtifactStorage",
    "ArtifactStorageMetadata",
    "Boto3S3ObjectClient",
    "S3Object",
    "S3ObjectClient",
    "S3ParquetArtifactStorage",
    "committed_artifact_uri",
    "staging_artifact_uri_template",
]
