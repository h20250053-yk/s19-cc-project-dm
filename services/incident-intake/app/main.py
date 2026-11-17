"""Incident Intake Service - public entry point for incident reports.

REST (port 8080, public via GKE Ingress at /api/incidents)
  POST  /api/incidents                       report a new incident (validated)
  GET   /api/incidents?limit=20              recent incidents (best-effort scan)
  GET   /api/incidents/{id}?r=2              quorum read via the coordinator
  PATCH /api/incidents/{id}                  status / notes update (read-modify-write with vector-clock context)
  POST  /api/incidents/{id}/attachments      upload a photo/document -> GCS
Outbound
  REST  -> quorum-coordinator  (PUT/GET /api/quorum/kv/{id})
  Kafka -> incident-events     (INCIDENT_REPORTED / INCIDENT_UPDATED) consumed by Flink + Dispatch
  Pub/Sub -> alert-requests    (CRITICAL incidents trigger the serverless Alert Service)
  GCS   -> raw/ NDJSON batches of every raw report, attachments/ uploads
"""
from __future__ import annotations

import contextlib
import os
import time
import uuid
from enum import Enum
from typing import List, Optional

import httpx
from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool
from pydantic import BaseModel, Field

from s19common.gcp import PROJECT_ID, GcsBatchWriter, PubSubPublisher, upload_bytes
from s19common.kafka_util import TOPIC_INCIDENT_EVENTS, KafkaPublisher, now_ms
from s19common.obs import instrument, setup_logging

SERVICE = "incident-intake"
log = setup_logging(SERVICE)

COORDINATOR_URL = os.getenv("COORDINATOR_URL", "http://quorum-coordinator:8080")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "")
RAW_BUCKET = os.getenv("RAW_BUCKET", "")
ALERT_TOPIC = os.getenv("ALERT_TOPIC", "alert-requests")
MAX_ATTACHMENT_BYTES = int(os.getenv("MAX_ATTACHMENT_BYTES", str(5 * 1024 * 1024)))
CLIENT_ID = "intake"


class IncidentType(str, Enum):
    FLOOD = "FLOOD"
    FIRE = "FIRE"
    EARTHQUAKE = "EARTHQUAKE"
    CYCLONE = "CYCLONE"
    LANDSLIDE = "LANDSLIDE"
    MEDICAL = "MEDICAL"
    OTHER = "OTHER"


class Severity(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class Status(str, Enum):
    REPORTED = "REPORTED"
    VERIFIED = "VERIFIED"
    DISPATCHED = "DISPATCHED"
    RESOLVED = "RESOLVED"


class IncidentReport(BaseModel):
    reporterId: str = Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_.@-]+$")
    type: IncidentType
    severity: Severity
    region: str = Field(min_length=2, max_length=32, pattern=r"^[a-z0-9-]+$")
    description: str = Field(min_length=3, max_length=2000)
    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)
    peopleAffected: int = Field(default=0, ge=0, le=10_000_000)


class IncidentUpdate(BaseModel):
    status: Optional[Status] = None
    severity: Optional[Severity] = None
    note: Optional[str] = Field(default=None, max_length=1000)
    author: str = Field(default="field-officer", pattern=r"^[a-z0-9-]{1,32}$")
    context: Optional[dict] = None   # vector clock from a previous GET; omit to read-modify-write


state: dict = {}


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    state["http"] = httpx.AsyncClient(base_url=COORDINATOR_URL, timeout=5.0)
    state["kafka"] = KafkaPublisher(KAFKA_BOOTSTRAP, SERVICE)
    state["pubsub"] = PubSubPublisher(PROJECT_ID, ALERT_TOPIC)
    state["raw"] = GcsBatchWriter(RAW_BUCKET, "raw/incidents")
    yield
    state["raw"].flush()
    state["kafka"].flush()
    await state["http"].aclose()


app = FastAPI(title="S19 Incident Intake", version="1.0.0", lifespan=lifespan)
instrument(app, SERVICE)


def _event(event_type: str, doc: dict) -> dict:
    return {"eventId": uuid.uuid4().hex, "eventType": event_type, "incidentId": doc["incidentId"],
            "region": doc["region"], "severity": doc["severity"], "type": doc["type"],
            "reporterId": doc.get("reporterId", ""), "status": doc.get("status"), "ts": now_ms()}


async def _coordinator(method: str, path: str, **kw) -> httpx.Response:
    try:
        return await state["http"].request(method, path, **kw)
    except httpx.HTTPError as exc:
        log.error("coordinator unreachable", extra={"error": str(exc)})
        raise HTTPException(503, "quorum coordinator unreachable")


@app.post("/api/incidents", status_code=201)
async def report_incident(report: IncidentReport):
    incident_id = f"inc-{uuid.uuid4().hex[:12]}"
    ts = now_ms()
    doc = {"incidentId": incident_id, **report.model_dump(mode="json"), "status": Status.REPORTED.value,
           "createdAt": ts, "updatedAt": ts, "notes": [], "assignedResources": [], "assignedVolunteers": []}
    r = await _coordinator("PUT", f"/api/quorum/kv/{incident_id}", json={"value": doc, "clientId": CLIENT_ID})
    if r.status_code != 200:
        log.warning("quorum write failed", extra={"incidentId": incident_id, "status": r.status_code})
        return JSONResponse(status_code=503, content={"error": "incident not durably stored", "quorum": r.json()})
    q = r.json()
    state["raw"].add({"receivedAt": ts, "incidentId": incident_id, "report": report.model_dump(mode="json")})
    state["kafka"].publish(TOPIC_INCIDENT_EVENTS, incident_id, _event("INCIDENT_REPORTED", doc))
    alerted = False
    if report.severity == Severity.CRITICAL:
        alerted = await run_in_threadpool(
            state["pubsub"].publish,
            {"alertType": "CRITICAL_INCIDENT", "incidentId": incident_id, "region": report.region,
             "severity": report.severity.value, "incidentType": report.type.value,
             "message": f"CRITICAL {report.type.value} reported in {report.region}: {report.description[:140]}"},
            source=SERVICE) is not None
    log.info("incident reported", extra={"incidentId": incident_id, "region": report.region,
                                          "severity": report.severity.value, "acks": q["acks"]})
    return {"incidentId": incident_id, "status": doc["status"], "context": q["context"],
            "quorum": {"acks": q["acks"], "w": q["w"], "n": q["n"], "replicas": q["replicas"]},
            "alertPublished": alerted}


@app.get("/api/incidents")
async def list_incidents(limit: int = Query(default=20, ge=1, le=200)):
    r = await _coordinator("GET", "/api/quorum/scan", params={"limit": limit})
    return {"items": [i["value"] | {"siblings": i["siblings"]} for i in r.json()["items"]]}


@app.get("/api/incidents/{incident_id}")
async def get_incident(incident_id: str, r: Optional[int] = Query(default=None, ge=1, le=3)):
    params = {"r": r} if r else {}
    resp = await _coordinator("GET", f"/api/quorum/kv/{incident_id}", params=params)
    if resp.status_code == 404:
        raise HTTPException(404, "incident not found")
    return JSONResponse(status_code=resp.status_code, content=resp.json())


@app.patch("/api/incidents/{incident_id}")
async def update_incident(incident_id: str, upd: IncidentUpdate):
    cur = await _coordinator("GET", f"/api/quorum/kv/{incident_id}")
    if cur.status_code == 404:
        raise HTTPException(404, "incident not found")
    if cur.status_code != 200:
        return JSONResponse(status_code=cur.status_code, content=cur.json())
    body = cur.json()
    doc = dict(body["value"])
    context = upd.context if upd.context is not None else body["context"]
    if upd.status:
        doc["status"] = upd.status.value
    if upd.severity:
        doc["severity"] = upd.severity.value
    if upd.note:
        doc.setdefault("notes", []).append({"text": upd.note, "author": upd.author, "ts": now_ms()})
    doc["updatedAt"] = now_ms()
    w = await _coordinator("PUT", f"/api/quorum/kv/{incident_id}",
                           json={"value": doc, "context": context, "clientId": upd.author})
    if w.status_code != 200:
        return JSONResponse(status_code=w.status_code, content=w.json())
    state["kafka"].publish(TOPIC_INCIDENT_EVENTS, incident_id, _event("INCIDENT_UPDATED", doc))
    return {"incidentId": incident_id, "value": doc, "context": w.json()["context"], "quorum": w.json()}


@app.post("/api/incidents/{incident_id}/attachments", status_code=201)
async def upload_attachment(incident_id: str, file: UploadFile = File(...)):
    if not RAW_BUCKET:
        raise HTTPException(503, "RAW_BUCKET not configured")
    data = await file.read()
    if len(data) > MAX_ATTACHMENT_BYTES:
        raise HTTPException(413, "attachment too large")
    safe = "".join(ch for ch in (file.filename or "upload.bin") if ch.isalnum() or ch in "._-")[:100]
    uri = await run_in_threadpool(upload_bytes, RAW_BUCKET, f"attachments/{incident_id}/{int(time.time())}-{safe}", data,
                       file.content_type or "application/octet-stream")
    log.info("attachment stored", extra={"incidentId": incident_id, "uri": uri, "bytes": len(data)})
    return {"incidentId": incident_id, "uri": uri, "bytes": len(data)}


@app.get("/api/incidents/_meta/types")
async def types() -> dict:
    return {"types": [t.value for t in IncidentType], "severities": [s.value for s in Severity],
            "statuses": [s.value for s in Status]}
