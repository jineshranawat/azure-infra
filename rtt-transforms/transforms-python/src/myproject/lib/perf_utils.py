"""Pure helpers backing the performance demonstrations.

Problem 22 (salting for skew) and Problem 25 (repartition-vs-coalesce sizing).
Kept pure so the technique is unit-tested independently of Spark.
"""
from __future__ import annotations

import hashlib
import math
from typing import Optional


def salt_bucket(key: Optional[str], n: int) -> int:
    """Deterministic salt bucket in [0, n) for a key (skew mitigation)."""
    if n <= 0:
        raise ValueError("n must be positive")
    if key is None:
        return 0
    digest = hashlib.md5(str(key).encode("utf-8")).hexdigest()
    return int(digest, 16) % n


def target_partitions(row_count: int, rows_per_partition: int = 1_000_000) -> int:
    """Right-size output partition count (repartition vs coalesce)."""
    if rows_per_partition <= 0:
        raise ValueError("rows_per_partition must be positive")
    if row_count <= 0:
        return 1
    return max(1, math.ceil(row_count / rows_per_partition))
