"""Replicated Incident Store - one independent Dynamo-style replica.

Three copies run as the StatefulSet `incident-store` (pods incident-store-0/1/2), each
with its own PersistentVolume holding a SQLite file. Replicas never talk to each
other: the stateless Quorum Coordinator fans reads/writes out to them over gRPC.

Ports
  50051  gRPC  (s19.store.v1.IncidentStore)
  8080   HTTP  Prometheus /metrics
"""
from __future__ import annotations

import json
import logging
import os
import signal
import sqlite3
import threading
import time
import hashlib
import zlib
from concurrent import futures

import grpc
from prometheus_client import Counter, Gauge, start_http_server

from s19common import vclock
from s19common.obs import setup_logging
from s19common.pb import incident_store_pb2 as pb
from s19common.pb import incident_store_pb2_grpc as pbg

SERVICE = "incident-store"
REPLICA_ID = os.getenv("REPLICA_ID") or os.getenv("HOSTNAME", "incident-store-local")
DATA_DIR = os.getenv("DATA_DIR", "/data")
GRPC_PORT = int(os.getenv("GRPC_PORT", "50051"))
METRICS_PORT = int(os.getenv("METRICS_PORT", "8080"))
NUM_BUCKETS = 256
FAULT_MODES = ("none", "down", "slow", "drop_writes")

log = setup_logging(SERVICE)

RPCS = Counter("replica_rpc_total", "Replica RPCs", ["replica", "method", "result"])
KEYS = Gauge("replica_keys", "Number of keys stored", ["replica"])
FAULT = Gauge("replica_fault_mode", "0=none 1=down 2=slow 3=drop_writes", ["replica"])


def bucket_of(key: str) -> int:
    return zlib.crc32(key.encode()) % NUM_BUCKETS


def to_pb(v: dict) -> pb.Version:
    return pb.Version(clock=v["clock"], value_json=v["value_json"], timestamp_ms=v.get("ts", 0),
                      writer=v.get("writer", ""))


def from_pb(v: pb.Version) -> dict:
    return {"clock": dict(v.clock), "value_json": v.value_json, "ts": v.timestamp_ms, "writer": v.writer}


class Store:
    """SQLite-backed sibling store. Every row is one version of one key."""

    def __init__(self, path: str):
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self.lock = threading.Lock()
        self.db = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=NORMAL")
        self.db.execute(
            """CREATE TABLE IF NOT EXISTS versions (
                   key       TEXT    NOT NULL,
                   clock_id  TEXT    NOT NULL,
                   clock     TEXT    NOT NULL,
                   value     TEXT    NOT NULL,
                   ts        INTEGER NOT NULL,
                   writer    TEXT    NOT NULL,
                   bucket    INTEGER NOT NULL,
                   PRIMARY KEY (key, clock_id))"""
        )
        self.db.execute("CREATE INDEX IF NOT EXISTS idx_versions_bucket ON versions(bucket, key)")

    def _rows_to_versions(self, rows) -> list[dict]:
        return [{"clock": json.loads(r[0]), "value_json": r[1], "ts": r[2], "writer": r[3]} for r in rows]

    def get(self, key: str) -> list[dict]:
        rows = self.db.execute(
            "SELECT clock, value, ts, writer FROM versions WHERE key=?", (key,)).fetchall()
        return self._rows_to_versions(rows)

    def put(self, key: str, incoming: list[dict]) -> int:
        """Merge incoming versions with stored siblings; keep only maximal versions."""
        with self.lock:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                merged = vclock.reconcile(self.get(key) + incoming)
                self.db.execute("DELETE FROM versions WHERE key=?", (key,))
                b = bucket_of(key)
                for v in merged:
                    self.db.execute(
                        "INSERT INTO versions(key, clock_id, clock, value, ts, writer, bucket) VALUES (?,?,?,?,?,?,?)",
                        (key, vclock.clock_id(v["clock"]), json.dumps(vclock.normalize(v["clock"]), sort_keys=True),
                         v["value_json"], int(v.get("ts", 0)), v.get("writer", ""), b),
                    )
                self.db.execute("COMMIT")
                return len(merged)
            except Exception:
                self.db.execute("ROLLBACK")
                raise

    def scan(self, after_key: str, limit: int) -> list[tuple[str, list[dict]]]:
        keys = [r[0] for r in self.db.execute(
            "SELECT DISTINCT key FROM versions WHERE key > ? ORDER BY key LIMIT ?", (after_key, limit))]
        return [(k, self.get(k)) for k in keys]

    def count(self) -> int:
        return self.db.execute("SELECT COUNT(DISTINCT key) FROM versions").fetchone()[0]

    def _key_clock_ids(self, bucket: int | None = None):
        if bucket is None:
            q = self.db.execute("SELECT bucket, key, clock_id FROM versions ORDER BY bucket, key, clock_id")
        else:
            q = self.db.execute(
                "SELECT bucket, key, clock_id FROM versions WHERE bucket=? ORDER BY key, clock_id", (bucket,))
        return q.fetchall()

    def bucket_hashes(self) -> list[str]:
        acc = [hashlib.sha1() for _ in range(NUM_BUCKETS)]
        per_key: dict[tuple[int, str], list[str]] = {}
        for b, k, cid in self._key_clock_ids():
            per_key.setdefault((b, k), []).append(cid)
        for (b, k), cids in sorted(per_key.items()):
            acc[b].update(f"{k}:{hashlib.sha1('|'.join(sorted(cids)).encode()).hexdigest()};".encode())
        return [h.hexdigest() for h in acc]

    def bucket_digests(self, bucket: int) -> list[tuple[str, str]]:
        per_key: dict[str, list[str]] = {}
        for _, k, cid in self._key_clock_ids(bucket):
            per_key.setdefault(k, []).append(cid)
        return [(k, hashlib.sha1("|".join(sorted(c)).encode()).hexdigest()) for k, c in sorted(per_key.items())]


class Fault:
    def __init__(self):
        self.mode = "none"
        self.delay_ms = 0


class IncidentStoreServicer(pbg.IncidentStoreServicer):
    def __init__(self, store: Store, replica_id: str = REPLICA_ID):
        self.rid = replica_id
        self.store = store
        self.fault = Fault()
        KEYS.labels(self.rid).set(store.count())
        FAULT.labels(self.rid).set(0)

    # ---- failure injection gate -------------------------------------------------
    def _gate(self, context, method: str) -> bool:
        """Returns True if the call should silently drop its effect (drop_writes)."""
        mode = self.fault.mode
        if mode == "down":
            RPCS.labels(self.rid, method, "injected_down").inc()
            context.abort(grpc.StatusCode.UNAVAILABLE, f"{self.rid} is down (fault injection)")
        if mode == "slow":
            time.sleep(self.fault.delay_ms / 1000.0)
        return mode == "drop_writes"

    # ---- data plane -------------------------------------------------------------
    def Put(self, request, context):
        drop = self._gate(context, "Put")
        if drop:
            RPCS.labels(self.rid, "Put", "dropped").inc()
            return pb.PutResponse(ok=True, replica_id=self.rid, stored_versions=0)
        n = self.store.put(request.key, [from_pb(v) for v in request.versions])
        KEYS.labels(self.rid).set(self.store.count())
        RPCS.labels(self.rid, "Put", "ok").inc()
        return pb.PutResponse(ok=True, replica_id=self.rid, stored_versions=n)

    def Get(self, request, context):
        self._gate(context, "Get")
        versions = self.store.get(request.key)
        RPCS.labels(self.rid, "Get", "ok").inc()
        return pb.GetResponse(found=bool(versions), replica_id=self.rid, versions=[to_pb(v) for v in versions])

    def Scan(self, request, context):
        self._gate(context, "Scan")
        limit = min(max(request.limit or 50, 1), 1000)
        items = [pb.KeyVersions(key=k, versions=[to_pb(v) for v in vs])
                 for k, vs in self.store.scan(request.after_key, limit)]
        RPCS.labels(self.rid, "Scan", "ok").inc()
        return pb.ScanResponse(replica_id=self.rid, items=items)

    def GetBucketHashes(self, request, context):
        self._gate(context, "GetBucketHashes")
        RPCS.labels(self.rid, "GetBucketHashes", "ok").inc()
        return pb.BucketHashes(replica_id=self.rid, hashes=self.store.bucket_hashes())

    def GetBucketDigests(self, request, context):
        self._gate(context, "GetBucketDigests")
        RPCS.labels(self.rid, "GetBucketDigests", "ok").inc()
        return pb.BucketDigests(replica_id=self.rid, digests=[
            pb.KeyDigest(key=k, digest=d) for k, d in self.store.bucket_digests(request.bucket)])

    # ---- control plane (never blocked by faults) --------------------------------
    def _ping(self) -> pb.PingResponse:
        return pb.PingResponse(replica_id=self.rid, fault_mode=self.fault.mode,
                               key_count=self.store.count(), delay_ms=self.fault.delay_ms)

    def Ping(self, request, context):
        return self._ping()

    def SetFault(self, request, context):
        mode = request.mode or "none"
        if mode not in FAULT_MODES:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"mode must be one of {FAULT_MODES}")
        self.fault.mode = mode
        self.fault.delay_ms = max(0, request.delay_ms or (1500 if mode == "slow" else 0))
        FAULT.labels(self.rid).set(FAULT_MODES.index(mode))
        log.warning("fault injection changed", extra={"replica": self.rid, "mode": mode,
                                                      "delay_ms": self.fault.delay_ms})
        return self._ping()


def serve(port: int = GRPC_PORT, data_dir: str = DATA_DIR, block: bool = True,
          replica_id: str = REPLICA_ID):
    store = Store(os.path.join(data_dir, f"{replica_id}.db"))
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=32))
    servicer = IncidentStoreServicer(store, replica_id)
    pbg.add_IncidentStoreServicer_to_server(servicer, server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    log.info("replica started", extra={"replica": replica_id, "port": port, "keys": store.count()})
    if block:
        start_http_server(METRICS_PORT)
        stop = threading.Event()
        signal.signal(signal.SIGTERM, lambda *_: stop.set())
        signal.signal(signal.SIGINT, lambda *_: stop.set())
        stop.wait()
        server.stop(grace=5).wait()
        log.info("replica stopped", extra={"replica": replica_id})
    return server, servicer


if __name__ == "__main__":
    serve()
