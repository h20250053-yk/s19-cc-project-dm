"""End-to-end tests of the Dynamo layer: 3 real gRPC replicas + the quorum engine.

Run:  pip install -r services/requirements.txt pytest pytest-asyncio
      scripts/gen_proto.sh && pytest -q tests/
"""
import asyncio
import importlib.util
import json
import pathlib
import socket
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "services" / "common"))
sys.path.insert(0, str(ROOT / "services" / "quorum-coordinator"))

from s19common import vclock  # noqa: E402

spec = importlib.util.spec_from_file_location("replica_server", ROOT / "services/incident-store/app/server.py")
replica_server = importlib.util.module_from_spec(spec)
spec.loader.exec_module(replica_server)

from app.quorum import QuorumEngine, QuorumError  # noqa: E402
from app.merge import merge_incident_siblings  # noqa: E402


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


@pytest.fixture
def cluster(tmp_path):
    servers, servicers, targets = [], {}, {}
    for i in range(3):
        port = free_port()
        rid = f"incident-store-{i}"
        srv, svc = replica_server.serve(port=port, data_dir=str(tmp_path / rid), block=False, replica_id=rid)
        servers.append(srv)
        servicers[rid] = svc
        targets[rid] = f"127.0.0.1:{port}"
    yield targets, servicers
    for s in servers:
        s.stop(0)


def run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


async def _settle(engine):
    await asyncio.sleep(0.3)
    if engine._bg:
        await asyncio.wait(list(engine._bg), timeout=3)


# ---------------------------------------------------------------- vector clocks
def test_vclock_compare():
    a = {"intake": 1}
    b = vclock.increment(a, "dispatch")
    c = vclock.increment(a, "field")
    assert vclock.compare(b, a) == vclock.AFTER
    assert vclock.compare(a, b) == vclock.BEFORE
    assert vclock.compare(b, c) == vclock.CONCURRENT
    assert vclock.compare(b, dict(b)) == vclock.EQUAL
    vs = [{"clock": a, "value_json": "1", "ts": 1}, {"clock": b, "value_json": "2", "ts": 2},
          {"clock": c, "value_json": "3", "ts": 3}]
    assert {v["value_json"] for v in vclock.reconcile(vs)} == {"2", "3"}


def test_semantic_merge():
    s1 = {"value_json": json.dumps({"status": "DISPATCHED", "severity": "HIGH", "notes": [{"t": "a", "ts": 1}],
                                    "updatedAt": 10, "assignedResources": ["r1"]}), "ts": 10}
    s2 = {"value_json": json.dumps({"status": "VERIFIED", "severity": "CRITICAL", "notes": [{"t": "b", "ts": 2}],
                                    "updatedAt": 11, "assignedResources": ["r2"]}), "ts": 11}
    m = merge_incident_siblings([s1, s2])
    assert m["status"] == "DISPATCHED" and m["severity"] == "CRITICAL"
    assert len(m["notes"]) == 2 and set(m["assignedResources"]) == {"r1", "r2"}


# ---------------------------------------------------------------- quorum behaviour
def test_quorum_write_read(cluster):
    targets, _ = cluster

    async def go():
        e = QuorumEngine(targets, w=2, r=2, timeout_s=1.0)
        w = await e.put("inc-1", {"region": "north", "status": "REPORTED"}, None, "intake")
        assert w["acks"] >= 2
        g = await e.get("inc-1")
        assert g["found"] and g["value"]["status"] == "REPORTED" and not g["conflict"]
        await _settle(e)
        await e.close()
    run(go())


def test_write_survives_one_replica_down_and_recovery(cluster):
    targets, svcs = cluster

    async def go():
        e = QuorumEngine(targets, w=2, r=2, timeout_s=0.5)
        await e.set_fault("incident-store-2", "down", 0)
        w = await e.put("inc-2", {"region": "south", "status": "REPORTED"}, None, "intake")
        assert w["acks"] == 2 and w["replicas"]["incident-store-2"].startswith("error")
        await _settle(e)
        assert svcs["incident-store-2"].store.get("inc-2") == []          # replica missed the write
        await e.set_fault("incident-store-2", "none", 0)                    # replica recovers
        summary = await e.anti_entropy()
        assert summary["repaired"].get("incident-store-2") == 1
        assert len(svcs["incident-store-2"].store.get("inc-2")) == 1        # repaired by anti-entropy
        await e.close()
    run(go())


def test_quorum_failure_when_two_down(cluster):
    targets, _ = cluster

    async def go():
        e = QuorumEngine(targets, w=2, r=2, timeout_s=0.5)
        await e.set_fault("incident-store-1", "down", 0)
        await e.set_fault("incident-store-2", "down", 0)
        with pytest.raises(QuorumError):
            await e.put("inc-3", {"region": "east"}, None, "intake")
        # sloppy W=1 still succeeds (availability over consistency, demo of tunable quorum)
        w = await e.put("inc-3", {"region": "east"}, None, "intake", w=1)
        assert w["acks"] == 1
        await _settle(e)
        await e.close()
    run(go())


def test_read_repair_after_dropped_write(cluster):
    targets, svcs = cluster

    async def go():
        e = QuorumEngine(targets, w=2, r=2, timeout_s=1.0)
        first = await e.put("inc-4", {"region": "west", "status": "REPORTED"}, None, "intake")
        await _settle(e)
        await e.set_fault("incident-store-0", "drop_writes", 0)
        await e.put("inc-4", {"region": "west", "status": "VERIFIED"}, first["context"], "dispatch")
        await _settle(e)
        await e.set_fault("incident-store-0", "none", 0)
        stale = json.loads(svcs["incident-store-0"].store.get("inc-4")[0]["value_json"])
        assert stale["status"] == "REPORTED"
        g = await e.get("inc-4", r=3)
        assert g["value"]["status"] == "VERIFIED"
        assert "incident-store-0" in g["staleReplicasInQuorum"]
        await _settle(e)
        fixed = json.loads(svcs["incident-store-0"].store.get("inc-4")[0]["value_json"])
        assert fixed["status"] == "VERIFIED" and e.stats.read_repairs >= 1
        await e.close()
    run(go())


def test_concurrent_writes_create_siblings_then_merge(cluster):
    targets, svcs = cluster

    async def go():
        e = QuorumEngine(targets, w=2, r=2, timeout_s=1.0)
        base = await e.put("inc-5", {"region": "central", "status": "REPORTED", "severity": "MEDIUM",
                                     "updatedAt": 1}, None, "intake")
        await _settle(e)
        ctx = base["context"]
        # two writers update from the same context without seeing each other
        await e.put("inc-5", {"region": "central", "status": "DISPATCHED", "severity": "MEDIUM", "updatedAt": 2},
                    ctx, "dispatch")
        await e.put("inc-5", {"region": "central", "status": "REPORTED", "severity": "CRITICAL", "updatedAt": 3},
                    ctx, "field-officer")
        await _settle(e)
        g = await e.get("inc-5")
        assert g["conflict"] and len(g["siblings"]) == 2
        assert g["value"]["status"] == "DISPATCHED" and g["value"]["severity"] == "CRITICAL"
        await _settle(e)
        # the merged version was written back: siblings collapse to one on the next read
        g2 = await e.get("inc-5", r=3)
        assert not g2["conflict"] and g2["value"]["mergedFromSiblings"] == 2
        for svc in svcs.values():
            assert len(svc.store.get("inc-5")) == 1
        await e.close()
    run(go())


def test_slow_replica_does_not_block_quorum(cluster):
    targets, _ = cluster

    async def go():
        e = QuorumEngine(targets, w=2, r=2, timeout_s=2.0)
        await e.set_fault("incident-store-1", "slow", 1500)
        import time
        t = time.perf_counter()
        await e.put("inc-6", {"region": "north"}, None, "intake")
        assert time.perf_counter() - t < 1.0      # did not wait for the slow replica
        await e.set_fault("incident-store-1", "none", 0)
        await _settle(e)
        await e.close()
    run(go())
