"""Reference operators used by runnable examples."""

from typing import Any


def identity_batch(batch: Any) -> Any:
    return batch


def keep_all(_: dict[str, Any]) -> bool:
    return True


def profiled_batch(batch: Any, *, delay_seconds: float = 0.0) -> Any:
    """Linux benchmark UDF: simulate IO and record this worker's RSS high-water mark."""
    import os
    import resource
    import time

    import numpy as np

    if delay_seconds < 0:
        raise ValueError("delay_seconds must be non-negative")
    time.sleep(delay_seconds)
    result = dict(batch)
    rows = len(next(iter(result.values())))
    result["_worker_peak_rss_bytes"] = np.full(
        rows, resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024, dtype=np.int64
    )
    result["_worker_pid"] = np.full(rows, os.getpid(), dtype=np.int64)
    return result
