"""Dispatch Service - assigns resources and volunteers to incidents.

REST (port 8080, prefix /api/dispatch)
  POST /api/dispatch                     body {"incidentId", "quantity"?, "kind"?, "volunteers"?}
  GET  /api/dispatch?status=ACTIVE&limit=50
  GET  /api/dispatch/{id}
  POST /api/dispatch/{id}/complete       release resources/volunteers, mark incident RESOLVED
Kafka consumer  incident-events  -> auto-dispatch CRITICAL incidents (AUTO_DISPATCH=true)
Kafka producer  dispatch-events  -> DISPATCH_CREATED / DISPATCH_COMPLETED (input of the Flink job)
REST clients    quorum-coordinator (read incident + context, write DISPATCHED/RESOLVED)
                resource-registry  (reserve / release)
                volunteer-coordination (assign / release)
Pub/Sub         alert-requests (DISPATCH_ASSIGNED notifications -> serverless Alert Service)
Storage         Cloud SQL table dispatch_assignments
"""
from __future__ import annotations

import asyncio
import contextlib
import os
import pathlib
import uuid
from typing import Optional

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from psycopg.errors import UniqueViolation
from psycopg.rows import dict_row
from pydantic import BaseModel, Field
from starlette.concurrency import run_in_threadpool

from s19common.db import migrate, open_pool
from s19common.gcp import PROJECT_ID, PubSubPublisher
from s19common.kafka_util import (TOPIC_DISPATCH_EVENTS, TOPIC_INCIDENT_EVENTS, KafkaConsumerThread,
                                  KafkaPublisher, now_ms)
from s19common.obs import instrument, setup_logging

SERVICE = "dispatch"
log = setup_logging(SERVICE)

COORDINATOR_URL = os.getenv("COORDINATOR_URL", "http://quorum-coordinator:8080")
REGISTRY_URL = os.getenv("REGISTRY_URL", "http://resource-registry:8080")
VOLUNTEER_URL = os.getenv("VOLUNTEER_URL", "http://volunteer-coordination:8080")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "")
ALERT_TOPIC = os.getenv("ALERT_TOPIC", "alert-requests")
AUTO_DISPATCH = os.getenv("AUTO_DISPATCH", "true").lower() == "true"
AUTO_DISPATCH_SEVERITIES = set(os.getenv("AUTO_DISPATCH_SEVERITIES", "CRITICAL").split(","))
CLIENT_ID = "dispatch"

RESOURCES_BY_SEVERITY = {"CRITICAL": 3, "HIGH": 2, "MEDIUM": 1, "LOW": 1}
VOLUNTEERS_BY_SEVERITY = {"CRITICAL": 5, "HIGH": 3, "MEDIUM": 1, "LOW": 0}


class DispatchIn(BaseModel):
    incidentId: str = Field(min_length=3, max_length=64)
    quantity: Optional[int] = Field(default=None, ge=1, le=20)
    kind: Optional[str] = Field(default=None, pattern=r"^(VEHICLE|SUPPLY|TEAM)$")
    volunteers: Optional[int] = Field(default=None, ge=0, le=50)


state: dict = {}
COLS = ("id::text AS id, incident_id, region, severity, resource_ids, volunteer_ids, status, mode, "
        "shortfall, created_at, completed_at")


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    pool = await open_pool()
    await migrate(pool, pathlib.Path(__file__).parent / "migrations", "dispatch_schema_migrations")
    state.update(pool=pool, http=httpx.AsyncClient(timeout=5.0), loop=asyncio.get_running_loop(),
                 kafka=KafkaPublisher(KAFKA_BOOTSTRAP, SERVICE), pubsub=PubSubPublisher(PROJECT_ID, ALERT_TOPIC))
    consumer = None
    if AUTO_DISPATCH:
        consumer = KafkaConsumerThread(KAFKA_BOOTSTRAP, "dispatch-auto", [TOPIC_INCIDENT_EVENTS], _on_incident_event)
        consumer.start()
    state["consumer"] = consumer
    yield
    if consumer:
        consumer.stop()
    state["kafka"].flush()
    await state["http"].aclose()
    await pool.close()


app = FastAPI(title="S19 Dispatch Service", version="1.0.0", lifespan=lifespan)
instrument(app, SERVICE)


def _on_incident_event(evt: dict) -> None:
    """Runs on the Kafka consumer thread; hands work to the asyncio loop."""
    if evt.get("eventType") != "INCIDENT_REPORTED" or evt.get("severity") not in AUTO_DISPATCH_SEVERITIES:
        return
    fut = asyncio.run_coroutine_threadsafe(
        dispatch_incident(DispatchIn(incidentId=evt["incidentId"]), mode="AUTO"), state["loop"])
    try:
        fut.result(timeout=30)
    except HTTPException as e:
        log.info("auto-dispatch skipped", extra={"incidentId": evt["incidentId"], "reason": e.detail})
    except Exception:  # noqa: BLE001
        log.exception("auto-dispatch failed", extra={"incidentId": evt.get("incidentId")})


async def _call(method: str, url: str, **kw) -> httpx.Response:
    try:
        return await state["http"].request(method, url, **kw)
    except httpx.HTTPError as exc:
        raise HTTPException(503, f"dependency unreachable: {url} ({exc})")


async def _read_incident(incident_id: str) -> tuple[dict, dict]:
    r = await _call("GET", f"{COORDINATOR_URL}/api/quorum/kv/{incident_id}")
    if r.status_code == 404:
        raise HTTPException(404, "incident not found")
    if r.status_code != 200:
        raise HTTPException(503, f"quorum read failed: {r.text}")
    body = r.json()
    return body["value"], body["context"]


async def _write_incident(incident_id: str, doc: dict, context: dict) -> dict:
    doc["updatedAt"] = now_ms()
    r = await _call("PUT", f"{COORDINATOR_URL}/api/quorum/kv/{incident_id}",
                    json={"value": doc, "context": context, "clientId": CLIENT_ID})
    if r.status_code != 200:
        raise HTTPException(503, f"quorum write failed: {r.text}")
    return r.json()


async def _db(sql: str, params=(), one=False):
    async with state["pool"].connection() as conn:
        cur = conn.cursor(row_factory=dict_row)
        await cur.execute(sql, params)
        return await (cur.fetchone() if one else cur.fetchall())


@app.post("/api/dispatch", status_code=201)
async def create_dispatch(body: DispatchIn):
    return await dispatch_incident(body, mode="MANUAL")


async def dispatch_incident(body: DispatchIn, mode: str) -> dict:
    doc, context = await _read_incident(body.incidentId)
    if doc.get("status") in ("DISPATCHED", "RESOLVED") and mode == "AUTO":
        raise HTTPException(409, f"incident already {doc['status']}")
    severity, region = doc.get("severity", "LOW"), doc["region"]
    try:
        row = await _db(f"INSERT INTO dispatch_assignments(incident_id, region, severity, mode) "
                        f"VALUES (%s,%s,%s,%s) RETURNING {COLS}", (body.incidentId, region, severity, mode), one=True)
    except UniqueViolation:
        raise HTTPException(409, "incident already has an active dispatch")

    qty = body.quantity or RESOURCES_BY_SEVERITY.get(severity, 1)
    res = await _call("POST", f"{REGISTRY_URL}/api/resources/reserve",
                      json={"incidentId": body.incidentId, "region": region, "kind": body.kind, "quantity": qty})
    reserved = res.json().get("reserved", []) if res.status_code == 200 else []
    n_vol = body.volunteers if body.volunteers is not None else VOLUNTEERS_BY_SEVERITY.get(severity, 0)
    volunteers = []
    if n_vol:
        vr = await _call("POST", f"{VOLUNTEER_URL}/api/volunteers/assign",
                         json={"incidentId": body.incidentId, "region": region, "count": n_vol})
        volunteers = vr.json().get("assigned", []) if vr.status_code == 200 else []

    resource_ids = [r["id"] for r in reserved]
    volunteer_ids = [v["id"] for v in volunteers]
    status = "ACTIVE" if (resource_ids or volunteer_ids) else "FAILED"
    row = await _db(f"UPDATE dispatch_assignments SET resource_ids=%s, volunteer_ids=%s, status=%s, shortfall=%s "
                    f"WHERE id::text=%s RETURNING {COLS}",
                    (resource_ids, volunteer_ids, status, qty - len(resource_ids), row["id"]), one=True)
    if status == "FAILED":
        log.warning("dispatch failed: nothing available", extra={"incidentId": body.incidentId, "region": region})
        return JSONResponse(status_code=409, content={"error": "no resources or volunteers available",
                                                      "dispatch": _json(row)})

    doc["status"] = "DISPATCHED"
    doc["assignedResources"] = sorted(set(doc.get("assignedResources", [])) | set(resource_ids))
    doc["assignedVolunteers"] = sorted(set(doc.get("assignedVolunteers", [])) | set(volunteer_ids))
    doc.setdefault("notes", []).append({"text": f"{mode} dispatch {row['id']}: {len(resource_ids)} resources, "
                                                f"{len(volunteer_ids)} volunteers", "author": CLIENT_ID,
                                        "ts": now_ms()})
    quorum = await _write_incident(body.incidentId, doc, context)

    state["kafka"].publish(TOPIC_DISPATCH_EVENTS, body.incidentId, {
        "eventId": uuid.uuid4().hex, "eventType": "DISPATCH_CREATED", "incidentId": body.incidentId,
        "dispatchId": row["id"], "region": region, "severity": severity, "resourceCount": len(resource_ids),
        "volunteerCount": len(volunteer_ids), "mode": mode, "ts": now_ms()})
    await run_in_threadpool(state["pubsub"].publish, {
        "alertType": "DISPATCH_ASSIGNED", "incidentId": body.incidentId, "region": region, "severity": severity,
        "volunteerIds": volunteer_ids, "resourceIds": resource_ids,
        "message": f"You have been assigned to incident {body.incidentId} in {region} ({severity})"},
        source=SERVICE)
    log.info("dispatch created", extra={"incidentId": body.incidentId, "dispatchId": row["id"], "mode": mode,
                                        "resources": len(resource_ids), "volunteers": len(volunteer_ids)})
    return {"dispatch": _json(row), "incidentQuorum": {"acks": quorum["acks"], "context": quorum["context"]}}


def _json(row: dict) -> dict:
    return {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in row.items()}


@app.get("/api/dispatch")
async def list_dispatches(status: Optional[str] = Query(default=None, pattern=r"^(ACTIVE|COMPLETED|FAILED)$"),
                          limit: int = Query(default=50, ge=1, le=500)):
    if status:
        rows = await _db(f"SELECT {COLS} FROM dispatch_assignments WHERE status=%s ORDER BY created_at DESC LIMIT %s",
                         (status, limit))
    else:
        rows = await _db(f"SELECT {COLS} FROM dispatch_assignments ORDER BY created_at DESC LIMIT %s", (limit,))
    return {"items": [_json(r) for r in rows]}


@app.get("/api/dispatch/{dispatch_id}")
async def get_dispatch(dispatch_id: str):
    row = await _db(f"SELECT {COLS} FROM dispatch_assignments WHERE id::text=%s", (dispatch_id,), one=True)
    if not row:
        raise HTTPException(404, "dispatch not found")
    return _json(row)


@app.post("/api/dispatch/{dispatch_id}/complete")
async def complete_dispatch(dispatch_id: str):
    row = await _db(f"SELECT {COLS} FROM dispatch_assignments WHERE id::text=%s", (dispatch_id,), one=True)
    if not row:
        raise HTTPException(404, "dispatch not found")
    if row["status"] != "ACTIVE":
        raise HTTPException(409, f"dispatch is {row['status']}")
    inc = row["incident_id"]
    await _call("POST", f"{REGISTRY_URL}/api/resources/release", json={"incidentId": inc})
    await _call("POST", f"{VOLUNTEER_URL}/api/volunteers/release", json={"incidentId": inc})
    doc, context = await _read_incident(inc)
    doc["status"] = "RESOLVED"
    doc.setdefault("notes", []).append({"text": f"dispatch {dispatch_id} completed", "author": CLIENT_ID,
                                        "ts": now_ms()})
    await _write_incident(inc, doc, context)
    row = await _db(f"UPDATE dispatch_assignments SET status='COMPLETED', completed_at=now() WHERE id::text=%s "
                    f"RETURNING {COLS}", (dispatch_id,), one=True)
    state["kafka"].publish(TOPIC_DISPATCH_EVENTS, inc, {
        "eventId": uuid.uuid4().hex, "eventType": "DISPATCH_COMPLETED", "incidentId": inc, "dispatchId": dispatch_id,
        "region": row["region"], "severity": row["severity"], "resourceCount": 0, "ts": now_ms()})
    return _json(row)


@app.get("/api/dispatch/_meta/consumer")
async def consumer_status():
    c = state.get("consumer")
    return {"autoDispatch": AUTO_DISPATCH, "processed": c.processed if c else 0,
            "lastError": c.last_error if c else None}
