#!/usr/bin/env python3
"""Consistency test-suite for the Dynamo-style incident store (run against the PUBLIC URL).

Usage:
  pip install httpx
  BASE_URL=http://<ingress-ip> ADMIN_TOKEN=<token> python scripts/consistency_test.py
  (local docker compose: BASE_URL=http://localhost:8001 COORD_URL=http://localhost:8003)

Every scenario prints PASS/FAIL with evidence; the output is the "testing evidence" for requirement (f).
"""
import json
import os
import sys
import time

import httpx

BASE = os.getenv("BASE_URL", "http://localhost:8001").rstrip("/")
COORD = os.getenv("COORD_URL", BASE).rstrip("/")       # same host behind the Ingress
ADMIN = {"X-Admin-Token": os.getenv("ADMIN_TOKEN", "")}
R0, R1, R2 = "incident-store-0", "incident-store-1", "incident-store-2"
c = httpx.Client(timeout=10)
results = []


def check(name, ok, evidence):
    results.append((name, ok))
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       {json.dumps(evidence)[:400]}")


def fault(replica, mode, delay=1500):
    r = c.post(f"{COORD}/api/quorum/replicas/{replica}/fault", json={"mode": mode, "delayMs": delay}, headers=ADMIN)
    r.raise_for_status()


def heal_all():
    for r in (R0, R1, R2):
        fault(r, "none")


def debug(key):
    return c.get(f"{COORD}/api/quorum/debug/{key}").json()


def statuses(key):
    return {rid: [v["value"].get("status") for v in d.get("versions", [])] if "versions" in d else d.get("error")
            for rid, d in debug(key).items()}


def new_incident(sev="HIGH", region="north"):
    r = c.post(f"{BASE}/api/incidents", json={
        "reporterId": f"tester-{int(time.time()*1000) % 100000}", "type": "FLOOD", "severity": sev, "region": region,
        "description": "consistency test incident", "latitude": 28.6, "longitude": 77.2, "peopleAffected": 10})
    return r


def put(key, value, context, client, w=None):
    params = {"w": w} if w else {}
    return c.put(f"{COORD}/api/quorum/kv/{key}", params=params,
                 json={"value": value, "context": context, "clientId": client})


def main():
    heal_all()
    cfg = c.get(f"{COORD}/api/quorum/config").json()
    check("1. Configuration N=3, W=2, R=2 satisfies R+W>N", cfg["n"] == 3 and cfg["rPlusWGreaterThanN"], cfg)

    r = new_incident()
    inc = r.json()
    check("2. Quorum write (W=2) acknowledged", r.status_code == 201 and inc["quorum"]["acks"] >= 2, inc["quorum"])
    key = inc["incidentId"]
    g = c.get(f"{BASE}/api/incidents/{key}").json()
    check("2b. Quorum read (R=2) returns the write", g.get("value", {}).get("status") == "REPORTED",
          {"responded": g.get("responded"), "context": g.get("context")})

    # ---- 3. replica failure + recovery (anti-entropy)
    fault(R2, "down")
    r = new_incident(region="south")
    inc3 = r.json()
    k3 = inc3["incidentId"]
    time.sleep(1)
    before = statuses(k3)
    check("3a. Write succeeds with one replica DOWN (2/3 acks)", r.status_code == 201 and inc3["quorum"]["acks"] == 2,
          {"replicas": inc3["quorum"]["replicas"], "perReplica": before})
    fault(R2, "none")
    ae = c.post(f"{COORD}/api/quorum/anti-entropy", headers=ADMIN).json()
    after = statuses(k3)
    check("3b. Recovered replica is repaired by anti-entropy (Merkle-lite)",
          after.get(R2) == ["REPORTED"], {"antiEntropy": ae.get("repaired"), "perReplica": after})

    # ---- 4. silent data loss + read repair
    cur = c.get(f"{COORD}/api/quorum/kv/{key}").json()
    fault(R0, "drop_writes")
    doc = dict(cur["value"], status="VERIFIED", updatedAt=int(time.time() * 1000))
    put(key, doc, cur["context"], "field-officer")
    time.sleep(0.5)
    fault(R0, "none")
    diverged = statuses(key)
    rr = c.get(f"{COORD}/api/quorum/kv/{key}", params={"r": 3}).json()
    time.sleep(1.5)
    repaired = statuses(key)
    check("4. Read repair fixes a replica that silently dropped a write",
          diverged.get(R0) == ["REPORTED"] and rr["value"]["status"] == "VERIFIED" and repaired.get(R0) == ["VERIFIED"],
          {"beforeRead": diverged, "staleInQuorum": rr.get("staleReplicasInQuorum"), "afterRead": repaired})

    # ---- 5. concurrent writes -> siblings -> semantic merge
    cur = c.get(f"{COORD}/api/quorum/kv/{key}").json()
    ctx = cur["context"]
    a = dict(cur["value"], status="DISPATCHED", updatedAt=int(time.time() * 1000))
    b = dict(cur["value"], severity="CRITICAL", updatedAt=int(time.time() * 1000) + 1)
    put(key, a, ctx, "dispatch")
    put(key, b, ctx, "field-officer")          # same (stale) context => concurrent with the first write
    time.sleep(0.5)
    g = c.get(f"{COORD}/api/quorum/kv/{key}").json()
    merged = g.get("value", {})
    check("5a. Concurrent writes detected as siblings (vector clocks)", g.get("conflict") and len(g["siblings"]) == 2,
          {"siblingClocks": [s["clock"] for s in g.get("siblings", [])]})
    check("5b. Conflict resolved by semantic merge (DISPATCHED + CRITICAL kept)",
          merged.get("status") == "DISPATCHED" and merged.get("severity") == "CRITICAL",
          {"status": merged.get("status"), "severity": merged.get("severity"), "context": g.get("context")})
    time.sleep(1.5)
    g2 = c.get(f"{COORD}/api/quorum/kv/{key}", params={"r": 3}).json()
    check("5c. Merged version written back - siblings collapsed on all replicas", not g2.get("conflict"),
          {"perReplica": statuses(key)})

    # ---- 6. quorum loss and tunable consistency
    fault(R1, "down")
    fault(R2, "down")
    cur = c.get(f"{COORD}/api/quorum/kv/{key}", params={"r": 1}).json()
    w2 = put(key, dict(cur["value"], status="RESOLVED"), cur["context"], "dispatch")
    check("6a. W=2 write REJECTED when only 1/3 replicas is up (consistency over availability)",
          w2.status_code == 503, w2.json())
    w1 = put(key, dict(cur["value"], status="RESOLVED"), cur["context"], "dispatch", w=1)
    check("6b. Same write with W=1 is accepted (availability over consistency)", w1.status_code == 200,
          {"acks": w1.json().get("acks")})
    r2 = c.get(f"{COORD}/api/quorum/kv/{key}")
    check("6c. R=2 read fails while 2 replicas are down", r2.status_code == 503, r2.json())
    heal_all()
    c.post(f"{COORD}/api/quorum/anti-entropy", headers=ADMIN)
    final = statuses(key)
    check("6d. After recovery + anti-entropy all replicas converge", len({json.dumps(v) for v in final.values()}) == 1,
          final)

    # ---- 7. slow replica does not slow the quorum
    fault(R1, "slow", 2000)
    t = time.time()
    r = new_incident(region="east")
    took = time.time() - t
    fault(R1, "none")
    check("7. A slow replica (2 s) does not block W=2 writes", r.status_code == 201 and took < 1.5,
          {"seconds": round(took, 3), "replicas": r.json()["quorum"]["replicas"]})

    passed = sum(ok for _, ok in results)
    print(f"\n{passed}/{len(results)} checks passed")
    sys.exit(0 if passed == len(results) else 1)


if __name__ == "__main__":
    try:
        main()
    finally:
        try:
            heal_all()
        except Exception:  # noqa: BLE001
            pass
