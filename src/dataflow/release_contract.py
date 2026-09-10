from __future__ import annotations

import argparse
import re
import tomllib
from dataclasses import dataclass
from pathlib import Path

import yaml

_TAG_PATTERN = re.compile(
    r"^v(?P<version>(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*))$"
)


@dataclass(frozen=True)
class ReleaseVersions:
    package: str
    chart: str
    app: str

    def values(self) -> tuple[str, str, str]:
        return (self.package, self.chart, self.app)


def read_release_versions(root: Path) -> ReleaseVersions:
    pyproject_path = root / "pyproject.toml"
    chart_path = root / "charts" / "dataflow" / "Chart.yaml"

    with pyproject_path.open("rb") as stream:
        pyproject = tomllib.load(stream)
    chart = yaml.safe_load(chart_path.read_text(encoding="utf-8"))

    return ReleaseVersions(
        package=str(pyproject["project"]["version"]),
        chart=str(chart["version"]),
        app=str(chart["appVersion"]),
    )


def validate_release_contract(root: Path, tag: str | None = None) -> str:
    versions = read_release_versions(root)
    if len(set(versions.values())) != 1:
        raise ValueError(
            "release versions must match: "
            f"package={versions.package}, chart={versions.chart}, appVersion={versions.app}"
        )

    version = versions.package
    if tag is not None:
        match = _TAG_PATTERN.fullmatch(tag)
        if match is None:
            raise ValueError(f"release tag must match vX.Y.Z, got {tag!r}")
        if match.group("version") != version:
            raise ValueError(f"release tag {tag!r} does not match project version {version!r}")
    return version


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Validate DataFlow package, Helm chart and optional Git tag versions."
    )
    parser.add_argument("tag", nargs="?", help="Optional release tag, for example v0.1.0")
    parser.add_argument("--root", type=Path, default=Path.cwd())
    args = parser.parse_args(argv)

    try:
        version = validate_release_contract(args.root, args.tag)
    except (KeyError, OSError, TypeError, ValueError, yaml.YAMLError) as exc:
        parser.exit(2, f"release version contract violation: {exc}\n")
    print(version)


if __name__ == "__main__":
    main()
