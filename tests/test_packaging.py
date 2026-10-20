from __future__ import annotations

import importlib
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_all_console_scripts_import_without_ray():
    project = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]
    assert set(project["scripts"]) == {"dataflow-run"}
    for target in project["scripts"].values():
        module, name = target.split(":")
        assert callable(getattr(importlib.import_module(module), name))
    assert project["dependencies"] == ["pydantic>=2.8,<3"]
    assert set(project["optional-dependencies"]) == {"runtime", "dev"}
