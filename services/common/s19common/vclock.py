"""Vector clocks and sibling reconciliation (Dynamo-style versioning).

A *version* is a dict:
    {"clock": {"intake": 2, "dispatch": 1}, "value_json": "...", "ts": 1700000000000, "writer": "dispatch"}

Rules
-----
* ``a`` descends ``b``  <=>  for every node n: a[n] >= b[n]
* if neither descends the other they are *concurrent* (a conflict -> siblings)
* ``reconcile`` keeps only the maximal versions (drops every version that some
  other version descends) and removes duplicates (equal clocks).
"""
from __future__ import annotations

import hashlib
import json
from typing import Dict, Iterable, List

Clock = Dict[str, int]

BEFORE = "before"          # a happened-before b
AFTER = "after"            # a happened-after b
EQUAL = "equal"
CONCURRENT = "concurrent"


def normalize(clock: Clock | None) -> Clock:
    return {str(k): int(v) for k, v in (clock or {}).items() if int(v) > 0}


def increment(clock: Clock | None, node: str) -> Clock:
    c = normalize(clock)
    c[node] = c.get(node, 0) + 1
    return c


def merge(a: Clock | None, b: Clock | None) -> Clock:
    a, b = normalize(a), normalize(b)
    return {k: max(a.get(k, 0), b.get(k, 0)) for k in set(a) | set(b)}


def merge_all(clocks: Iterable[Clock]) -> Clock:
    out: Clock = {}
    for c in clocks:
        out = merge(out, c)
    return out


def descends(a: Clock | None, b: Clock | None) -> bool:
    """True if a >= b component-wise (a has seen everything b has seen)."""
    a, b = normalize(a), normalize(b)
    return all(a.get(k, 0) >= v for k, v in b.items())


def compare(a: Clock | None, b: Clock | None) -> str:
    ab, ba = descends(a, b), descends(b, a)
    if ab and ba:
        return EQUAL
    if ab:
        return AFTER
    if ba:
        return BEFORE
    return CONCURRENT


def clock_id(clock: Clock | None) -> str:
    canon = json.dumps(normalize(clock), sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(canon.encode()).hexdigest()


def reconcile(versions: Iterable[dict]) -> List[dict]:
    """Return the maximal (non-dominated) versions, de-duplicated by clock."""
    unique: Dict[str, dict] = {}
    for v in versions:
        cid = clock_id(v["clock"])
        # keep the newest wall-clock copy for identical clocks (identical content in practice)
        if cid not in unique or v.get("ts", 0) > unique[cid].get("ts", 0):
            unique[cid] = v
    items = list(unique.values())
    result = []
    for v in items:
        dominated = any(
            o is not v and compare(o["clock"], v["clock"]) == AFTER for o in items
        )
        if not dominated:
            result.append(v)
    result.sort(key=lambda x: (x.get("ts", 0), clock_id(x["clock"])))
    return result


def digest(versions: Iterable[dict]) -> str:
    """Stable digest of a sibling set (used by read repair and anti-entropy)."""
    ids = sorted(clock_id(v["clock"]) for v in versions)
    return hashlib.sha1("|".join(ids).encode()).hexdigest()
