from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from dataflow.release_contract import read_release_versions, validate_release_contract


def _write_contract(
    root: Path,
    *,
    package: str = "1.2.3",
    chart: str = "1.2.3",
    app: str = "1.2.3",
) -> None:
    root.joinpath("pyproject.toml").write_text(
        "[project]\nname = \"dataflow-orchestrator\"\n" f'version = "{package}"\n',
        encoding="utf-8",
    )
    chart_dir = root / "charts" / "dataflow"
    chart_dir.mkdir(parents=True)
    chart_dir.joinpath("Chart.yaml").write_text(
        yaml.safe_dump({"version": chart, "appVersion": app}),
        encoding="utf-8",
    )


def test_reads_release_versions(tmp_path: Path) -> None:
    _write_contract(tmp_path)
    assert read_release_versions(tmp_path).values() == ("1.2.3", "1.2.3", "1.2.3")


def test_accepts_matching_release_tag(tmp_path: Path) -> None:
    _write_contract(tmp_path)
    assert validate_release_contract(tmp_path, "v1.2.3") == "1.2.3"


def test_rejects_internal_version_drift(tmp_path: Path) -> None:
    _write_contract(tmp_path, chart="1.2.4")
    with pytest.raises(ValueError, match="release versions must match"):
        validate_release_contract(tmp_path)


@pytest.mark.parametrize("tag", ["1.2.3", "v1.2", "v01.2.3", "release-v1.2.3"])
def test_rejects_noncanonical_tags(tmp_path: Path, tag: str) -> None:
    _write_contract(tmp_path)
    with pytest.raises(ValueError, match="release tag must match"):
        validate_release_contract(tmp_path, tag)


def test_rejects_tag_version_drift(tmp_path: Path) -> None:
    _write_contract(tmp_path)
    with pytest.raises(ValueError, match="does not match project version"):
        validate_release_contract(tmp_path, "v1.2.4")
