"""Leaderless quorum engine (Dynamo-style) used by the Quorum Coordinator.

N replicas, write quorum W, read quorum R.  With N=3, W=2, R=2 we get R+W>N, so
every read quorum overlaps every write quorum in at least one replica and a
successful read always observes the latest successful write.

Implemented mechanisms
  * quorum writes   - send to all N in parallel, succeed after W acks (others finish in background)
  * quorum reads    - send to all N, answer after R responses
  * versioning      - vector clocks, clients pass back the `context` they read
  * conflicts       - concurrent versions are kept as siblings and merged on read
  * read repair     - after a read, stale/missing replicas get the reconciled version
  * anti-entropy    - background Merkle-lite comparison (256 buckets) repairs replicas
                      that were down or dropped writes (replica recovery)
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

import grpc
from prometheus_client import Counter, Gauge, Histogram

from s19common import vclock
from s19common.pb import incident_store_pb2 as pb
from s19common.pb import incident_store_pb2_grpc as pbg

from .merge import merge_incident_siblings

log = logging.getLogger("quorum")

Q_OPS = Counter("quorum_operations_total", "Quorum operations", ["op", "result"])
Q_LAT = Histogram("quorum_operation_duration_seconds", "Quorum op latency", ["op"],
                  buckets=(0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5))
Q_ACKS = Histogram("quorum_write_acks", "Replica acks observed per write", buckets=(0, 1, 2, 3))
REPLICA_ERRORS = Counter("quorum_replica_errors_total", "Replica RPC failures", ["replica", "op"])
READ_REPAIRS = Counter("quorum_read_repairs_total", "Read repairs pushed to a replica", ["replica"])
CONFLICTS = Counter("quorum_conflicts_total", "Reads that found concurrent siblings")
STALE = Counter("quorum_stale_responses_total", "Replica responses older than the reconciled value", ["replica"])
AE_RUNS = Counter("anti_entropy_runs_total", "Anti-entropy rounds", ["result"])
AE_REPAIRS = Counter("anti_entropy_keys_repaired_total", "Keys repaired by anti-entropy", ["replica"])
REPLICA_UP = Gauge("quorum_replica_up", "1 if replica answered the last health probe", ["replica"])


class QuorumError(Exception):
    def __init__(self, message: str, detail: dict):
        super().__init__(message)
        self.detail = detail


def to_pb(v: dict) -> pb.Version:
    return pb.Version(clock=v["clock"], value_json=v["value_json"], timestamp_ms=int(v.get("ts", 0)),
                      writer=v.get("writer", ""))


def from_pb(v: pb.Version) -> dict:
    return {"clock": dict(v.clock), "value_json": v.value_json, "ts": v.timestamp_ms, "writer": v.writer}


class ReplicaClient:
    def __init__(self, replica_id: str, target: str, timeout_s: float):
        self.id = replica_id
        self.target = target
        self.timeout = timeout_s
        self.channel = grpc.aio.insecure_channel(target, options=[
            ("grpc.keepalive_time_ms", 30000), ("grpc.enable_retries", 0)])
        self.stub = pbg.IncidentStoreStub(self.channel)

    async def put(self, key: str, versions: List[dict]) -> pb.PutResponse:
        return await self.stub.Put(pb.PutRequest(key=key, versions=[to_pb(v) for v in versions]),
                                   timeout=self.timeout)

    async def get(self, key: str) -> List[dict]:
        r = await self.stub.Get(pb.GetRequest(key=key), timeout=self.timeout)
        return [from_pb(v) for v in r.versions]

    async def scan(self, after: str, limit: int):
        r = await self.stub.Scan(pb.ScanRequest(after_key=after, limit=limit), timeout=self.timeout * 3)
        return [(i.key, [from_pb(v) for v in i.versions]) for i in r.items]

    async def bucket_hashes(self) -> List[str]:
        return list((await self.stub.GetBucketHashes(pb.Empty(), timeout=self.timeout * 3)).hashes)

    async def bucket_digests(self, b: int) -> Dict[str, str]:
        r = await self.stub.GetBucketDigests(pb.BucketRequest(bucket=b), timeout=self.timeout * 3)
        return {d.key: d.digest for d in r.digests}

    async def ping(self) -> dict:
        r = await self.stub.Ping(pb.Empty(), timeout=self.timeout)
        return {"replica": r.replica_id, "faultMode": r.fault_mode, "keys": r.key_count, "delayMs": r.delay_ms}

    async def set_fault(self, mode: str, delay_ms: int) -> dict:
        r = await self.stub.SetFault(pb.FaultRequest(mode=mode, delay_ms=delay_ms), timeout=self.timeout)
        return {"replica": r.replica_id, "faultMode": r.fault_mode, "keys": r.key_count, "delayMs": r.delay_ms}

    async def close(self):
        await self.channel.close()


@dataclass
class Stats:
    writes: int = 0
    write_failures: int = 0
    reads: int = 0
    read_failures: int = 0
    not_found: int = 0
    conflicts: int = 0
    read_repairs: int = 0
    stale_responses: int = 0
    anti_entropy_runs: int = 0
    anti_entropy_repairs: int = 0
    last_anti_entropy: Optional[dict] = None
    per_replica_errors: Dict[str, int] = field(default_factory=dict)


def parse_replicas(spec: str) -> Dict[str, str]:
    """"incident-store-0=host:50051,incident-store-1=host:50051" -> {id: target}"""
    out = {}
    for i, part in enumerate([p.strip() for p in spec.split(",") if p.strip()]):
        if "=" in part:
            rid, target = part.split("=", 1)
        else:
            rid, target = f"replica-{i}", part
        out[rid.strip()] = target.strip()
    return out


class QuorumEngine:
    def __init__(self, replicas: Dict[str, str], w: int, r: int, timeout_s: float = 0.8,
                 event_sink: Optional[Callable[[str, str, dict], None]] = None):
        if not replicas:
            raise ValueError("no replicas configured")
        self.clients = [ReplicaClient(rid, t, timeout_s) for rid, t in replicas.items()]
        self.n = len(self.clients)
        self.w = w
        self.r = r
        self.stats = Stats()
        self.event_sink = event_sink or (lambda *_: None)
        self._bg: set = set()

    # ------------------------------------------------------------------ helpers
    def _spawn(self, coro):
        t = asyncio.create_task(coro)
        self._bg.add(t)
        t.add_done_callback(self._bg.discard)
        return t

    def _err(self, client: ReplicaClient, op: str, exc: BaseException) -> str:
        REPLICA_ERRORS.labels(client.id, op).inc()
        self.stats.per_replica_errors[client.id] = self.stats.per_replica_errors.get(client.id, 0) + 1
        if isinstance(exc, grpc.aio.AioRpcError):
            return f"{exc.code().name}: {exc.details()}"
        return f"{type(exc).__name__}: {exc}"

    def _emit(self, event_type: str, key: str, region: str, extra: dict):
        evt = {"eventType": event_type, "incidentId": key, "region": region or "global",
               "ts": int(time.time() * 1000), **extra}
        try:
            self.event_sink("consistency-events", key, evt)
        except Exception:  # noqa: BLE001
            log.exception("event sink failed")

    @staticmethod
    def _region_of(versions: List[dict]) -> str:
        for v in versions:
            try:
                return json.loads(v["value_json"]).get("region", "global")
            except (ValueError, TypeError, AttributeError):
                continue
        return "global"

    def _clamp(self, q: Optional[int], default: int) -> int:
        q = default if q is None else int(q)
        return max(1, min(self.n, q))

    # ------------------------------------------------------------------ write
    async def put(self, key: str, value: dict, context: Optional[dict], client_id: str,
                  w: Optional[int] = None) -> dict:
        w = self._clamp(w, self.w)
        started = time.perf_counter()
        clock = vclock.increment(context or {}, client_id)
        version = {"clock": clock, "value_json": json.dumps(value, separators=(",", ":")),
                   "ts": int(time.time() * 1000), "writer": client_id}
        status: Dict[str, str] = {c.id: "pending" for c in self.clients}
        acked = asyncio.Event()
        acks = 0
        done = 0

        async def one(c: ReplicaClient):
            nonlocal acks, done
            try:
                await c.put(key, [version])
                status[c.id] = "ok"
                acks += 1
            except Exception as exc:  # noqa: BLE001
                status[c.id] = "error: " + self._err(c, "put", exc)
            finally:
                done += 1
                if acks >= w or done == self.n:
                    acked.set()

        for c in self.clients:
            self._spawn(one(c))
        await acked.wait()
        self.stats.writes += 1
        Q_ACKS.observe(acks)
        Q_LAT.labels("put").observe(time.perf_counter() - started)
        result = {"key": key, "context": clock, "w": w, "n": self.n, "acks": acks, "replicas": dict(status)}
        if acks < w:
            self.stats.write_failures += 1
            Q_OPS.labels("put", "quorum_failed").inc()
            self._emit("QUORUM_FAILURE", key, value.get("region", "global"), {"op": "put", "acks": acks, "w": w})
            raise QuorumError(f"write quorum not reached ({acks}/{w})", result)
        Q_OPS.labels("put", "ok").inc()
        return result

    # ------------------------------------------------------------------ read
    async def get(self, key: str, r: Optional[int] = None, resolve: bool = True) -> dict:
        r = self._clamp(r, self.r)
        started = time.perf_counter()
        responses: Dict[str, List[dict]] = {}
        errors: Dict[str, str] = {}
        enough = asyncio.Event()

        async def one(c: ReplicaClient):
            try:
                responses[c.id] = await c.get(key)
            except Exception as exc:  # noqa: BLE001
                errors[c.id] = self._err(c, "get", exc)
            finally:
                if len(responses) >= r or len(responses) + len(errors) == self.n:
                    enough.set()

        tasks = [self._spawn(one(c)) for c in self.clients]
        await enough.wait()
        self.stats.reads += 1
        first = dict(responses)                       # the R (or more) answers we decide on
        if len(first) < r:
            self.stats.read_failures += 1
            Q_OPS.labels("get", "quorum_failed").inc()
            self._emit("QUORUM_FAILURE", key, "global", {"op": "get", "responses": len(first), "r": r})
            raise QuorumError(f"read quorum not reached ({len(first)}/{r})",
                              {"key": key, "r": r, "responded": list(first), "errors": errors})

        siblings = vclock.reconcile(v for vs in first.values() for v in vs)
        Q_LAT.labels("get").observe(time.perf_counter() - started)
        if not siblings:
            self.stats.not_found += 1
            Q_OPS.labels("get", "not_found").inc()
            # still repair: a late replica may hold the key (e.g. W=1 write landed elsewhere)
            self._spawn(self._finish_read(key, tasks, responses, None))
            return {"key": key, "found": False, "r": r, "responded": list(first)}

        conflict = len(siblings) > 1
        stale_in_quorum = [rid for rid, vs in first.items() if vclock.digest(vs) != vclock.digest(siblings)]
        resolved_version = None
        if conflict:
            self.stats.conflicts += 1
            CONFLICTS.inc()
            region = self._region_of(siblings)
            self._emit("CONFLICT_DETECTED", key, region, {"siblings": len(siblings)})
            if resolve:
                merged = merge_incident_siblings(siblings)
                resolved_clock = vclock.increment(vclock.merge_all(s["clock"] for s in siblings), "coordinator")
                resolved_version = {"clock": resolved_clock, "value_json": json.dumps(merged, separators=(",", ":")),
                                    "ts": int(time.time() * 1000), "writer": "coordinator"}
        Q_OPS.labels("get", "conflict" if conflict else "ok").inc()
        self._spawn(self._finish_read(key, tasks, responses, resolved_version))

        if resolved_version is not None:
            value, context = json.loads(resolved_version["value_json"]), resolved_version["clock"]
        elif conflict:
            value, context = None, vclock.merge_all(s["clock"] for s in siblings)
        else:
            value, context = json.loads(siblings[0]["value_json"]), siblings[0]["clock"]
        return {
            "key": key, "found": True, "value": value, "context": context, "r": r, "n": self.n,
            "conflict": conflict, "resolvedBy": "semantic-merge" if resolved_version else None,
            "siblings": [{"clock": s["clock"], "writer": s["writer"], "ts": s["ts"],
                          "value": json.loads(s["value_json"])} for s in siblings] if conflict else [],
            "responded": list(first), "staleReplicasInQuorum": stale_in_quorum,
            "readRepair": "scheduled",
        }

    async def _finish_read(self, key: str, tasks, responses: Dict[str, List[dict]],
                           resolved_version: Optional[dict]):
        """Wait for the slower replicas, then push the reconciled state to every stale one."""
        await asyncio.wait(tasks, timeout=self.clients[0].timeout * 2)
        all_versions = [v for vs in responses.values() for v in vs]
        if resolved_version is not None:
            all_versions.append(resolved_version)
        final = vclock.reconcile(all_versions)
        if not final:
            return
        target = vclock.digest(final)
        region = self._region_of(final)
        for c in self.clients:
            if c.id not in responses:
                continue                       # replica did not answer - anti-entropy will handle it
            if vclock.digest(responses[c.id]) == target:
                continue
            if responses[c.id] and not resolved_version:
                STALE.labels(c.id).inc()
                self.stats.stale_responses += 1
            try:
                await c.put(key, final)
                READ_REPAIRS.labels(c.id).inc()
                self.stats.read_repairs += 1
                self._emit("READ_REPAIR", key, region, {"replica": c.id})
                log.info("read repair", extra={"key": key, "replica": c.id})
            except Exception as exc:  # noqa: BLE001
                self._err(c, "read_repair", exc)

    # ------------------------------------------------------------------ diagnostics
    async def debug_key(self, key: str) -> dict:
        async def one(c):
            try:
                vs = await c.get(key)
                return c.id, {"versions": [{"clock": v["clock"], "writer": v["writer"], "ts": v["ts"],
                                            "value": json.loads(v["value_json"])} for v in vs],
                              "digest": vclock.digest(vs) if vs else None}
            except Exception as exc:  # noqa: BLE001
                return c.id, {"error": self._err(c, "debug", exc)}
        return dict(await asyncio.gather(*(one(c) for c in self.clients)))

    async def scan(self, limit: int = 50, after: str = "") -> List[dict]:
        """Best-effort listing (not a quorum read): merges Scan results from all healthy replicas."""
        merged: Dict[str, List[dict]] = {}

        async def one(c):
            try:
                for k, vs in await c.scan(after, limit):
                    merged.setdefault(k, []).extend(vs)
            except Exception as exc:  # noqa: BLE001
                self._err(c, "scan", exc)
        await asyncio.gather(*(one(c) for c in self.clients))
        out = []
        for k in sorted(merged)[:limit]:
            sib = vclock.reconcile(merged[k])
            out.append({"key": k, "siblings": len(sib), "value": json.loads(sib[-1]["value_json"]),
                        "context": sib[-1]["clock"] if len(sib) == 1 else vclock.merge_all(s["clock"] for s in sib)})
        return out

    async def health(self) -> List[dict]:
        async def one(c):
            try:
                p = await c.ping()
                REPLICA_UP.labels(c.id).set(1 if p["faultMode"] != "down" else 0)
                return {"id": c.id, "target": c.target, "reachable": True, **p}
            except Exception as exc:  # noqa: BLE001
                REPLICA_UP.labels(c.id).set(0)
                return {"id": c.id, "target": c.target, "reachable": False, "error": self._err(c, "ping", exc)}
        return list(await asyncio.gather(*(one(c) for c in self.clients)))

    async def set_fault(self, replica_id: str, mode: str, delay_ms: int) -> dict:
        for c in self.clients:
            if c.id == replica_id:
                return await c.set_fault(mode, delay_ms)
        raise KeyError(replica_id)

    # ------------------------------------------------------------------ anti-entropy
    async def anti_entropy(self, max_keys: int = 500) -> dict:
        """Merkle-lite sync: compare 256 bucket hashes, then per-key digests in differing buckets."""
        started = time.perf_counter()
        hashes: Dict[str, List[str]] = {}
        for c in self.clients:
            try:
                hashes[c.id] = await c.bucket_hashes()
            except Exception as exc:  # noqa: BLE001
                self._err(c, "anti_entropy", exc)
        if len(hashes) < 2:
            AE_RUNS.labels("skipped").inc()
            return {"status": "skipped", "reason": "fewer than 2 reachable replicas", "reachable": list(hashes)}
        ids = list(hashes)
        diff_buckets = [b for b in range(256) if len({hashes[i][b] for i in ids}) > 1]
        by_id = {c.id: c for c in self.clients}
        repaired: Dict[str, int] = {}
        keys_checked = 0
        for b in diff_buckets:
            digests = {}
            for rid in ids:
                try:
                    digests[rid] = await by_id[rid].bucket_digests(b)
                except Exception as exc:  # noqa: BLE001
                    self._err(by_id[rid], "anti_entropy", exc)
            all_keys = set().union(*[set(d) for d in digests.values()]) if digests else set()
            for key in sorted(all_keys):
                if len({digests[rid].get(key) for rid in digests}) == 1:
                    continue
                keys_checked += 1
                if keys_checked > max_keys:
                    break
                got: Dict[str, List[dict]] = {}
                for rid in digests:
                    try:
                        got[rid] = await by_id[rid].get(key)
                    except Exception as exc:  # noqa: BLE001
                        self._err(by_id[rid], "anti_entropy", exc)
                final = vclock.reconcile(v for vs in got.values() for v in vs)
                if not final:
                    continue
                target = vclock.digest(final)
                for rid, vs in got.items():
                    if vclock.digest(vs) != target:
                        try:
                            await by_id[rid].put(key, final)
                            repaired[rid] = repaired.get(rid, 0) + 1
                            AE_REPAIRS.labels(rid).inc()
                        except Exception as exc:  # noqa: BLE001
                            self._err(by_id[rid], "anti_entropy", exc)
        total = sum(repaired.values())
        self.stats.anti_entropy_runs += 1
        self.stats.anti_entropy_repairs += total
        AE_RUNS.labels("ok").inc()
        summary = {"status": "ok", "reachable": ids, "differingBuckets": len(diff_buckets),
                   "keysChecked": keys_checked, "repaired": repaired,
                   "durationMs": int((time.perf_counter() - started) * 1000), "at": int(time.time() * 1000)}
        self.stats.last_anti_entropy = summary
        if total:
            log.info("anti-entropy repaired keys", extra=summary)
            self._emit("ANTI_ENTROPY_REPAIR", f"bucket-sync-{summary['at']}", "global", {"repaired": total})
        return summary

    async def close(self):
        for t in list(self._bg):
            t.cancel()
        for c in self.clients:
            await c.close()
