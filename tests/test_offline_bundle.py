from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import tarfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    digest.update(path.read_bytes())
    return digest.hexdigest()


def _fake_input(tmp_path: Path, version: str = "0.1.0") -> Path:
    input_dir = tmp_path / "input"
    (input_dir / "charts").mkdir(parents=True)
    (input_dir / "images").mkdir()
    (input_dir / "charts" / f"dataflow-{version}.tgz").write_bytes(b"dataflow-chart")
    (input_dir / "charts" / "kuberay-operator-1.6.2.tgz").write_bytes(
        b"kuberay-chart"
    )
    for name in (
        "dataflow-runtime.tar",
        "dataflow-control-plane.tar",
        "kuberay-operator.tar",
        "postgres.tar",
        "minio.tar",
    ):
        (input_dir / "images" / name).write_bytes(f"fake:{name}".encode())
    return input_dir


def _build(tmp_path: Path, name: str) -> Path:
    input_dir = _fake_input(tmp_path / name)
    output_dir = tmp_path / name / "out"
    subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts/build_offline_bundle.py"),
            "--root",
            str(ROOT),
            "--input-dir",
            str(input_dir),
            "--output-dir",
            str(output_dir),
            "--version",
            "0.1.0",
            "--arch",
            "amd64",
        ],
        check=True,
    )
    return output_dir / "dataflow-offline-0.1.0-amd64.tar.gz"


def test_offline_bundle_is_reproducible_and_self_describing(tmp_path: Path) -> None:
    first = _build(tmp_path, "first")
    second = _build(tmp_path, "second")

    assert _sha256(first) == _sha256(second)
    checksum_file = first.with_suffix("").with_suffix(".sha256")
    expected_digest, expected_name = checksum_file.read_text().strip().split()
    assert expected_name == first.name
    assert expected_digest == _sha256(first)

    extract = tmp_path / "extract"
    extract.mkdir()
    with tarfile.open(first, "r:gz") as archive:
        archive.extractall(extract, filter="data")
    bundle = extract / "dataflow-offline-0.1.0-amd64"

    manifest = json.loads((bundle / "manifest.json").read_text())
    assert manifest["schema_version"] == 1
    assert manifest["architecture"] == "amd64"
    assert manifest["platform"] == "linux/amd64"
    assert manifest["kuberay_version"] == "1.6.2"
    assert [item["archive"] for item in manifest["images"]] == [
        "images/dataflow-runtime.tar",
        "images/dataflow-control-plane.tar",
        "images/kuberay-operator.tar",
        "images/postgres.tar",
        "images/minio.tar",
    ]

    checksums = {}
    for line in (bundle / "SHA256SUMS").read_text().splitlines():
        digest, relative = line.split("  ", 1)
        checksums[relative] = digest
    for relative, digest in checksums.items():
        assert _sha256(bundle / relative) == digest

    assert (bundle / "scripts/load-images.sh").stat().st_mode & 0o111
    assert (bundle / "scripts/install.sh").stat().st_mode & 0o111


def test_offline_install_scripts_do_not_fetch_public_dependencies() -> None:
    for relative in (
        "release/offline/install.sh",
        "release/offline/bootstrap-reference-deps.sh",
        "release/offline/kind-load-images.sh",
    ):
        text = (ROOT / relative).read_text()
        assert "helm repo add" not in text
        assert "helm repo update" not in text
        assert "docker pull" not in text
        assert "curl " not in text
        assert "wget " not in text


def test_release_workflow_targets_both_linux_architectures() -> None:
    text = (ROOT / ".github/workflows/release.yml").read_text()
    assert "arch: [amd64, arm64]" in text
    assert "rayproject/ray:2.58.0-py311-aarch64" in text
    assert "docker buildx imagetools create" in text
    assert "offline-bundle-amd64" in text
    assert "pull-policy Never" in text
