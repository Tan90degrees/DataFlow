"""Small reference callables used by examples and smoke tests."""

from __future__ import annotations

import time
from typing import Any


def identity_batch(batch: Any) -> Any:
    return batch


def slow_identity_batch(batch: Any, *, seconds: float = 5.0) -> Any:
    """Return a batch after a deterministic delay for lifecycle/E2E tests."""

    if seconds < 0:
        raise ValueError("seconds must be non-negative")
    time.sleep(seconds)
    return batch


def keep_all(_: dict[str, Any]) -> bool:
    return True
