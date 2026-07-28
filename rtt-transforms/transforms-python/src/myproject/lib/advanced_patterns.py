"""Advanced data-engineering patterns (beyond the core 50).

Pure, deterministic, engine-agnostic helpers so they unit-test cleanly and can be reused
from Polars or PySpark transforms. Each function documents its INPUT (Args), OUTPUT
(Returns), failure modes (Raises) and a REFERENCE to the Palantir surface / DE pattern.
Advanced problems are prefixed 'A': A1..A11.

Index
-----
A1  scd1_latest ................ Slowly Changing Dimension Type 1 (overwrite latest)
A2  classify_cdc_with_tombstones CDC including explicit hard-delete tombstones
A3  content_hash / dedupe_by_hash Content-based (not just PK) de-duplication
A4  profile_column ............. Column profiling / data observability statistics
A5  tokenize_preserving ........ Format-preserving pseudonymisation of identifiers (PII)
A6  within_allowed_lateness .... Late / out-of-order handling with an allowed-lateness window
A7  reconcile_by_partition ..... Per-partition source-to-target reconciliation
A8  merge_schemas .............. Schema-evolution union (fan-in of differing schemas)
A9  backoff_schedule ........... Connector resilience: exponential backoff delays
A10 as_of_join_pick ............ Point-in-time ("as-of") dimension version lookup
A11 idempotency_key ............ Deterministic key for exactly-once / replay-safe writes
"""
from __future__ import annotations

import datetime as _dt
import hashlib
from typing import Any, Dict, Hashable, List, Mapping, Optional, Sequence


def scd1_latest(records: Sequence[Mapping], key: str, order: str) -> List[dict]:
    """A1 - Slowly Changing Dimension Type 1: keep only the latest row per key (overwrite).

    SCD1 discards history and keeps the current value. Contrast with SCD2
    (lib.scd.build_scd2), which preserves effective-dated history. Use SCD1 when only the
    current state matters (e.g. a trust's current contact email).

    Args:
        records (Sequence[Mapping]): INPUT - rows, each containing `key` and `order`.
        key (str): INPUT - the natural-key field name.
        order (str): INPUT - recency field (higher = newer), e.g. a timestamp.
    Returns:
        List[dict]: OUTPUT - one row per key (the newest), in first-seen key order.
    Reference:
        Palantir: incremental transforms / dimension modelling. DE pattern: SCD Type 1.
    """
    latest: Dict[Hashable, dict] = {}
    seen: List[Hashable] = []
    for r in records:
        k = r[key]
        if k not in latest:
            seen.append(k)
        if k not in latest or r[order] > latest[k][order]:
            latest[k] = dict(r)
    return [latest[k] for k in seen]


def classify_cdc_with_tombstones(current: Mapping[Hashable, Any],
                                 previous: Mapping[Hashable, Any],
                                 tombstones: Optional[Sequence[Hashable]] = None) -> Dict[Hashable, str]:
    """A2 - CDC with tombstones: classify each key as INSERT/UPDATE/UNCHANGED/DELETE.

    Extends lib.scd.classify_cdc by honouring EXPLICIT delete tombstones from a source CDC
    feed (a key can be deleted at source even if still present in `previous`). Implicit
    deletes (in previous, absent in current) are also emitted.

    Args:
        current (Mapping): INPUT - key -> comparable value for the new state.
        previous (Mapping): INPUT - key -> comparable value for the prior state.
        tombstones (Sequence): INPUT (optional) - keys explicitly deleted at source.
    Returns:
        Dict[Hashable, str]: OUTPUT - key -> INSERT | UPDATE | UNCHANGED | DELETE.
    Reference:
        Palantir: @incremental change feeds / CDC syncs. DE pattern: CDC + tombstone deletes.
    """
    tomb = set(tombstones or [])
    ops: Dict[Hashable, str] = {}
    for k, v in current.items():
        if k in tomb:
            ops[k] = "DELETE"
        elif k not in previous:
            ops[k] = "INSERT"
        elif previous[k] != v:
            ops[k] = "UPDATE"
        else:
            ops[k] = "UNCHANGED"
    for k in previous:
        if k not in current:
            ops[k] = "DELETE"
    for k in tomb:
        ops[k] = "DELETE"
    return ops


def content_hash(row: Mapping, cols: Sequence[str]) -> str:
    """A3a - Stable content hash of selected columns (basis for content de-duplication).

    Args:
        row (Mapping): INPUT - a single record.
        cols (Sequence[str]): INPUT - columns whose combined value defines identity.
    Returns:
        str: OUTPUT - hex SHA-1 digest; equal iff the selected values are equal.
    Reference:
        Palantir: data quality / dedupe. DE pattern: content-addressable de-duplication.
    """
    parts = ["" if row.get(c) is None else str(row.get(c)) for c in cols]
    return hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()


def dedupe_by_hash(rows: Sequence[Mapping], cols: Sequence[str]) -> List[dict]:
    """A3b - Remove rows that are duplicates by CONTENT (not just PK), first wins.

    Catches "same record, different PK" duplicates a PK-only dedupe misses.

    Args:
        rows (Sequence[Mapping]): INPUT - candidate records.
        cols (Sequence[str]): INPUT - columns defining content identity.
    Returns:
        List[dict]: OUTPUT - de-duplicated rows in first-seen order.
    Reference:
        Palantir: Data Expectations + Silver cleanse. DE pattern: content de-duplication.
    """
    seen = set()
    out: List[dict] = []
    for r in rows:
        h = content_hash(r, cols)
        if h not in seen:
            seen.add(h)
            out.append(dict(r))
    return out


def profile_column(values: Sequence) -> Dict[str, Any]:
    """A4 - Column profile: count, nulls, null%, distinct, min, max (data observability).

    Powers automatic profiling / Data Health without a separate profiling tool.

    Args:
        values (Sequence): INPUT - all values of one column (Nones allowed).
    Returns:
        dict: OUTPUT - {count, nulls, null_pct (0..1 or None), distinct, min, max}.
    Reference:
        Palantir: Data Health / expectations. DE pattern: column profiling / observability.
    """
    n = len(values)
    non_null = [v for v in values if v is not None]
    nulls = n - len(non_null)
    return {
        "count": n,
        "nulls": nulls,
        "null_pct": round(nulls / n, 4) if n else None,
        "distinct": len(set(non_null)),
        "min": min(non_null) if non_null else None,
        "max": max(non_null) if non_null else None,
    }


def tokenize_preserving(value: Optional[str], key: str) -> Optional[str]:
    """A5 - Format-preserving pseudonymisation of an identifier (keeps shape, masks content).

    Unlike a plain SHA-256 (lib.pii.pseudonymise), this preserves STRUCTURE
    (digit->digit, upper->upper, lower->lower, punctuation kept) so downstream length/format
    checks still pass while the real value is hidden. Deterministic for a given `key`.

    Args:
        value (str|None): INPUT - identifier to tokenise (e.g. 'NHS1000580'); None -> None.
        key (str): INPUT - secret salt; same key => same token (stable joins).
    Returns:
        str|None: OUTPUT - token with identical character classes/length (e.g. 'NHS7413926').
    Reference:
        Palantir: property security / Markings + in-pipeline PII control. DE pattern: FPE tokenisation.
    """
    if value is None:
        return None
    out = []
    for i, ch in enumerate(str(value)):
        h = int(hashlib.sha256(f"{key}:{i}:{ch}".encode("utf-8")).hexdigest(), 16)
        if ch.isdigit():
            out.append(str((int(ch) + h) % 10))
        elif "A" <= ch <= "Z":
            out.append(chr((ord(ch) - 65 + h) % 26 + 65))
        elif "a" <= ch <= "z":
            out.append(chr((ord(ch) - 97 + h) % 26 + 97))
        else:
            out.append(ch)
    return "".join(out)


def within_allowed_lateness(event_ts: Optional[_dt.date], watermark: Optional[_dt.date],
                            allowed_days: int) -> Optional[bool]:
    """A6 - Late/out-of-order gate: is a late event still inside the allowed-lateness window?

    Args:
        event_ts (date|None): INPUT - the event's own date.
        watermark (date|None): INPUT - the current stream watermark (progress marker).
        allowed_days (int): INPUT - how many days late an event may be and still be accepted.
    Returns:
        bool|None: OUTPUT - True if on-time/future or within lateness; False if too late; None if unknown.
    Reference:
        Palantir: streaming / @incremental watermarking. DE pattern: allowed lateness window.
    """
    if event_ts is None or watermark is None:
        return None
    delay = (watermark - event_ts).days
    if delay <= 0:
        return True
    return delay <= allowed_days


def reconcile_by_partition(source_counts: Mapping[Hashable, int],
                           target_counts: Mapping[Hashable, int]) -> Dict[str, Any]:
    """A7 - Per-partition reconciliation: variance per partition + overall balanced flag.

    Stronger than a single total: catches a partition that gained rows while another lost them.

    Args:
        source_counts (Mapping): INPUT - partition-key -> row count on the source side.
        target_counts (Mapping): INPUT - partition-key -> row count on the target side.
    Returns:
        dict: OUTPUT - {'variances': {key: target-source}, 'balanced': bool}.
    Reference:
        Palantir: recon control totals / audit. DE pattern: partition-level reconciliation.
    """
    keys = set(source_counts) | set(target_counts)
    variances = {k: target_counts.get(k, 0) - source_counts.get(k, 0) for k in keys}
    return {"variances": variances, "balanced": all(v == 0 for v in variances.values())}


def merge_schemas(cols_a: Sequence[str], cols_b: Sequence[str]) -> List[str]:
    """A8 - Schema-evolution union: unified, order-preserving column list for a fan-in.

    When unioning frames with different columns, produce the superset (missing columns are
    then filled with null on each side). Handles additive schema drift safely.

    Args:
        cols_a (Sequence[str]): INPUT - columns of the first frame (order preserved).
        cols_b (Sequence[str]): INPUT - columns of the second frame.
    Returns:
        List[str]: OUTPUT - cols_a followed by any columns only in cols_b.
    Reference:
        Palantir: transforms fan-in / union; schema drift. DE pattern: schema evolution.
    """
    out = list(cols_a)
    for c in cols_b:
        if c not in out:
            out.append(c)
    return out


def backoff_schedule(max_retries: int, base: float = 1.0, factor: float = 2.0,
                     cap: float = 30.0) -> List[float]:
    """A9 - Connector resilience: exponential backoff delay schedule (seconds).

    Args:
        max_retries (int): INPUT - number of retry attempts (>= 0).
        base (float): INPUT - initial delay in seconds (> 0).
        factor (float): INPUT - multiplier per attempt (>= 1; 2.0 = double each time).
        cap (float): INPUT - maximum delay any single attempt may wait.
    Returns:
        List[float]: OUTPUT - delay before each attempt, each capped at `cap`.
    Raises:
        ValueError: if max_retries < 0 or base <= 0 or factor < 1.
    Reference:
        Palantir: Data Connection sync retry/throttle. DE pattern: exponential backoff.
    """
    if max_retries < 0 or base <= 0 or factor < 1:
        raise ValueError("require max_retries>=0, base>0, factor>=1")
    return [min(cap, base * (factor ** i)) for i in range(max_retries)]


def as_of_join_pick(fact_ts: _dt.date, dim_versions: Sequence) -> Optional[Any]:
    """A10 - Point-in-time ("as-of") join: the dimension value effective at fact_ts.

    Picks the dimension version whose valid_from is the latest one <= fact_ts. Prevents the
    classic bug of joining today's dimension onto a historical fact.

    Args:
        fact_ts (date): INPUT - the fact's event date.
        dim_versions (Sequence[tuple]): INPUT - list of (valid_from_date, value), any order.
    Returns:
        Any|None: OUTPUT - the value effective at fact_ts, or None if none applies yet.
    Reference:
        Palantir: SCD2 + link resolution. DE pattern: as-of / point-in-time join.
    """
    eligible = [v for v in dim_versions if v[0] <= fact_ts]
    if not eligible:
        return None
    return max(eligible, key=lambda v: v[0])[1]


def idempotency_key(parts: Sequence) -> str:
    """A11 - Deterministic idempotency key for exactly-once / replay-safe writes.

    Args:
        parts (Sequence): INPUT - components defining one logical write (e.g. [source, batch_id, pk]).
    Returns:
        str: OUTPUT - stable 24-char hex key; identical inputs => identical key.
    Reference:
        Palantir: immutable transactions + idempotent replay (Problem 19). DE pattern: idempotency key.
    """
    return hashlib.sha256("|".join(str(p) for p in parts).encode("utf-8")).hexdigest()[:24]
