"""Domain-specific conflict resolution for concurrent incident versions.

When two writers update the same incident without seeing each other's write
(e.g. Dispatch marks it DISPATCHED while a field officer escalates severity),
the replicas keep both versions as *siblings*. On read, the coordinator merges
them with rules that never lose safety-relevant information:

  status          -> most advanced lifecycle state wins
  severity        -> highest severity wins
  peopleAffected  -> maximum
  notes           -> union (deduplicated, ordered by time)
  assignedResources / assignedVolunteers -> set union
  every other field -> last-writer-wins by `updatedAt` (falls back to version timestamp)
"""
from __future__ import annotations

import json
from typing import List

STATUS_ORDER = ["REPORTED", "VERIFIED", "DISPATCHED", "RESOLVED"]
SEVERITY_ORDER = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
UNION_FIELDS = ("assignedResources", "assignedVolunteers")


def _rank(order: list, value) -> int:
    return order.index(value) if value in order else -1


def merge_incident_siblings(siblings: List[dict]) -> dict:
    """siblings: list of version dicts (with value_json). Returns merged value dict."""
    values = []
    for s in siblings:
        try:
            v = json.loads(s["value_json"])
        except (TypeError, ValueError):
            v = {"raw": s.get("value_json")}
        values.append((v.get("updatedAt", s.get("ts", 0)), s.get("ts", 0), v))
    # LWW base: newest version first, then overlay the commutative merges
    values.sort(key=lambda t: (t[0], t[1]))
    merged = dict(values[-1][2])
    docs = [t[2] for t in values]

    statuses = [d.get("status") for d in docs if d.get("status")]
    if statuses:
        merged["status"] = max(statuses, key=lambda s: _rank(STATUS_ORDER, s))
    severities = [d.get("severity") for d in docs if d.get("severity")]
    if severities:
        merged["severity"] = max(severities, key=lambda s: _rank(SEVERITY_ORDER, s))
    people = [d.get("peopleAffected") for d in docs if isinstance(d.get("peopleAffected"), int)]
    if people:
        merged["peopleAffected"] = max(people)

    notes, seen = [], set()
    for d in docs:
        for n in d.get("notes", []) or []:
            sig = json.dumps(n, sort_keys=True)
            if sig not in seen:
                seen.add(sig)
                notes.append(n)
    if notes:
        merged["notes"] = sorted(notes, key=lambda n: n.get("ts", 0) if isinstance(n, dict) else 0)

    for f in UNION_FIELDS:
        items = []
        for d in docs:
            for x in d.get(f, []) or []:
                if x not in items:
                    items.append(x)
        if items:
            merged[f] = items

    merged["updatedAt"] = max(t[0] for t in values)
    merged["mergedFromSiblings"] = len(siblings)
    return merged
