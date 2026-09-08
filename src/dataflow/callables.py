"""Small reference callables used by examples and smoke tests."""

from __future__ import annotations

from typing import Any


def identity_batch(batch: Any) -> Any:
    return batch


def keep_all(_: dict[str, Any]) -> bool:
    return True
