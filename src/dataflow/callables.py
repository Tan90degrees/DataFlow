"""Reference operators used by runnable examples."""

from typing import Any


def identity_batch(batch: Any) -> Any:
    return batch


def keep_all(_: dict[str, Any]) -> bool:
    return True
