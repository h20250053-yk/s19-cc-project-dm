"""Quorum Coordinator - stateless REST front-end to the 3 incident-store replicas.

REST (port 8080, prefix /api/quorum)
  PUT  /api/quorum/kv/{key}?w=2            body {"value": {...}, "context": {...}|null, "clientId": "intake"}
  GET  /api/quorum/kv/{key}?r=2&resolve=true
  GET  /api/quorum/scan?limit=50&after=
  GET  /api/quorum/debug/{key}             raw per-replica state (demo of divergence / repair)
  GET  /api/quorum/replicas                health + fault mode of every replica
  POST /api/quorum/replicas/{id}/fault     (X-Admin-Token) body {"mode": "none|down|slow|drop_writes", "delayMs": 1500}
  POST /api/quorum/anti-entropy            (X-Admin-Token) run a Merkle-lite sync round now
  GET  /api/quorum/stats                   counters of this coordinator pod
  GET  /api/quorum/config
gRPC (client) -> incident-store-{0,1,2}:50051
Kafka (producer) -> topic consistency-events
"""
from __future__ import annotations

import asyncio
import contextlib
import os
from dataclasses import asdict
from typing import Optional

from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from s19common.kafka_util import KafkaPublisher
from s19common.obs import instrument, setup_logging

from .quorum import QuorumEngine, QuorumError, parse_replicas

SERVICE = "quorum-coordinator"
log = setup_logging(SERVICE)

REPLICAS = os.getenv(
    "REPLICAS",
    "incident-store-0=incident-store-0.incident-store-headless:50051,"
    "incident-store-1=incident-store-1.incident-store-headless:50051,"
    "incident-store-2=incident-store-2.incident-store-headless:50051",
)
WRITE_QUORUM = int(os.getenv("WRITE_QUORUM", "2"))
READ_QUORUM = int(os.getenv("READ_QUORUM", "2"))
RPC_TIMEOUT_MS = int(os.getenv("RPC_TIMEOUT_MS", "800"))
ANTI_ENTROPY_INTERVAL_S = int(os.getenv("ANTI_ENTROPY_INTERVAL_S", "30"))
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "")
# If set, fault injection and manual anti-entropy require header  X-Admin-Token: <value>
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "")


class PutBody(BaseModel):
    value: dict
    context: Optional[dict] = None
    clientId: str = Field(default="coordinator", pattern=r"^[a-z0-9-]{1,32}$")


class FaultBody(BaseModel):
    mode: str = Field(pattern=r"^(none|down|slow|drop_writes)$")
    delayMs: int = Field(default=1500, ge=0, le=10000)


state: dict = {}


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    publisher = KafkaPublisher(KAFKA_BOOTSTRAP, SERVICE)
    engine = QuorumEngine(parse_replicas(REPLICAS), WRITE_QUORUM, READ_QUORUM, RPC_TIMEOUT_MS / 1000.0,
                          event_sink=publisher.publish)
    state.update(engine=engine, publisher=publisher)
    task = asyncio.create_task(_anti_entropy_loop(engine)) if ANTI_ENTROPY_INTERVAL_S > 0 else None
    log.info("coordinator ready", extra={"n": engine.n, "w": WRITE_QUORUM, "r": READ_QUORUM})
    yield
    if task:
        task.cancel()
    await engine.close()
    publisher.flush()


async def _anti_entropy_loop(engine: QuorumEngine):
    while True:
        await asyncio.sleep(ANTI_ENTROPY_INTERVAL_S)
        try:
            await engine.health()
            await engine.anti_entropy()
        except Exception:  # noqa: BLE001
            log.exception("anti-entropy round failed")


app = FastAPI(title="S19 Quorum Coordinator", version="1.0.0", lifespan=lifespan)
instrument(app, SERVICE)


def _engine() -> QuorumEngine:
    return state["engine"]


def _require_admin(token: Optional[str]) -> None:
    if ADMIN_TOKEN and token != ADMIN_TOKEN:
        raise HTTPException(401, "missing or wrong X-Admin-Token")


@app.put("/api/quorum/kv/{key}")
async def put_key(key: str, body: PutBody, w: Optional[int] = Query(default=None, ge=1, le=5)):
    try:
        return await _engine().put(key, body.value, body.context, body.clientId, w)
    except QuorumError as e:
        return JSONResponse(status_code=503, content={"error": str(e), **e.detail})


@app.get("/api/quorum/kv/{key}")
async def get_key(key: str, r: Optional[int] = Query(default=None, ge=1, le=5), resolve: bool = True):
    try:
        res = await _engine().get(key, r, resolve)
    except QuorumError as e:
        return JSONResponse(status_code=503, content={"error": str(e), **e.detail})
    if not res["found"]:
        return JSONResponse(status_code=404, content=res)
    return res


@app.get("/api/quorum/scan")
async def scan(limit: int = Query(default=50, ge=1, le=500), after: str = ""):
    return {"items": await _engine().scan(limit, after)}


@app.get("/api/quorum/debug/{key}")
async def debug_key(key: str):
    return await _engine().debug_key(key)


@app.get("/api/quorum/replicas")
async def replicas():
    return {"replicas": await _engine().health()}


@app.post("/api/quorum/replicas/{replica_id}/fault")
async def set_fault(replica_id: str, body: FaultBody, x_admin_token: Optional[str] = Header(default=None)):
    _require_admin(x_admin_token)
    try:
        return await _engine().set_fault(replica_id, body.mode, body.delayMs)
    except KeyError:
        raise HTTPException(404, f"unknown replica {replica_id}")


@app.post("/api/quorum/anti-entropy")
async def run_anti_entropy(x_admin_token: Optional[str] = Header(default=None)):
    _require_admin(x_admin_token)
    return await _engine().anti_entropy()


@app.get("/api/quorum/stats")
async def stats():
    e = _engine()
    return {"pod": os.getenv("HOSTNAME", "local"), "n": e.n, "w": e.w, "r": e.r, **asdict(e.stats)}


@app.get("/api/quorum/config")
async def config():
    e = _engine()
    return {"n": e.n, "w": e.w, "r": e.r, "rPlusWGreaterThanN": e.r + e.w > e.n,
            "replicas": {c.id: c.target for c in e.clients}, "rpcTimeoutMs": RPC_TIMEOUT_MS,
            "antiEntropyIntervalS": ANTI_ENTROPY_INTERVAL_S}
