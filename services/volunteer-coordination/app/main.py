"""Volunteer Coordination Service - volunteer registry, availability and assignments (Firestore).

REST (port 8080, prefix /api/volunteers)
  POST  /api/volunteers                       register a volunteer
  GET   /api/volunteers?region=&available=    list
  GET   /api/volunteers/stats                 counts per region (used by Situation Analytics)
  GET   /api/volunteers/{id}
  PATCH /api/volunteers/{id}/availability     body {"available": true|false}
  POST  /api/volunteers/{id}/heartbeat        app check-in (updates lastSeen + location)
  POST  /api/volunteers/assign                body {"incidentId","region","count","skills"?}   (called by Dispatch)
  POST  /api/volunteers/release               body {"incidentId"}
  POST  /api/volunteers/_seed?perRegion=6     demo data
Firestore collections: volunteers/{id}, volunteer_assignments/{incidentId}
Semi-structured, high-churn documents (availability flips, heartbeats) suit Firestore better than SQL.
"""
from __future__ import annotations

import contextlib
import random
import uuid
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query
from google.cloud import firestore
from pydantic import BaseModel, Field
from starlette.concurrency import run_in_threadpool

from s19common.gcp import firestore_client
from s19common.kafka_util import now_ms
from s19common.obs import instrument, setup_logging

SERVICE = "volunteer-coordination"
log = setup_logging(SERVICE)
REGIONS = ["north", "south", "east", "west", "central"]
SKILLS = ["first-aid", "swimming", "driving", "logistics", "medical", "search-rescue", "cooking", "translation"]


class VolunteerIn(BaseModel):
    name: str = Field(min_length=2, max_length=80)
    phone: str = Field(pattern=r"^\+?[0-9]{8,15}$")
    region: str = Field(pattern=r"^[a-z0-9-]{2,32}$")
    skills: List[str] = Field(default_factory=list, max_length=10)
    available: bool = True


class AvailabilityIn(BaseModel):
    available: bool


class HeartbeatIn(BaseModel):
    latitude: Optional[float] = Field(default=None, ge=-90, le=90)
    longitude: Optional[float] = Field(default=None, ge=-180, le=180)


class AssignIn(BaseModel):
    incidentId: str = Field(min_length=3, max_length=64)
    region: str = Field(pattern=r"^[a-z0-9-]{2,32}$")
    count: int = Field(ge=1, le=50)
    skills: List[str] = Field(default_factory=list)


class ReleaseIn(BaseModel):
    incidentId: str = Field(min_length=3, max_length=64)


state: dict = {}


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    state["db"] = firestore_client()
    yield


app = FastAPI(title="S19 Volunteer Coordination", version="1.0.0", lifespan=lifespan)
instrument(app, SERVICE)


def _vols():
    return state["db"].collection("volunteers")


def _public(doc) -> dict:
    d = doc.to_dict() or {}
    d["id"] = doc.id
    if "phone" in d:                         # never expose full phone numbers through the API
        d["phone"] = d["phone"][:3] + "*****" + d["phone"][-2:]
    return d


@app.post("/api/volunteers", status_code=201)
async def register(v: VolunteerIn):
    vid = f"vol-{uuid.uuid4().hex[:10]}"
    data = {**v.model_dump(), "currentIncident": None, "createdAt": now_ms(), "lastSeen": now_ms()}
    await run_in_threadpool(_vols().document(vid).set, data)
    return {"id": vid, **data}


@app.get("/api/volunteers")
async def list_volunteers(region: Optional[str] = None, available: Optional[bool] = None,
                          limit: int = Query(default=50, ge=1, le=500)):
    q = _vols()
    if region:
        q = q.where(filter=firestore.FieldFilter("region", "==", region))
    if available is not None:
        q = q.where(filter=firestore.FieldFilter("available", "==", available))
    docs = await run_in_threadpool(lambda: list(q.limit(limit).stream()))
    return {"items": [_public(d) for d in docs]}


@app.get("/api/volunteers/stats")
async def stats():
    def _count():
        out = {}
        for r in REGIONS:
            base = _vols().where(filter=firestore.FieldFilter("region", "==", r))
            total = base.count().get()[0][0].value
            avail = base.where(filter=firestore.FieldFilter("available", "==", True)).count().get()[0][0].value
            out[r] = {"total": total, "available": avail, "assigned": total - avail}
        return out
    return {"byRegion": await run_in_threadpool(_count)}


@app.get("/api/volunteers/{vid}")
async def get_volunteer(vid: str):
    doc = await run_in_threadpool(_vols().document(vid).get)
    if not doc.exists:
        raise HTTPException(404, "volunteer not found")
    return _public(doc)


@app.patch("/api/volunteers/{vid}/availability")
async def set_availability(vid: str, body: AvailabilityIn):
    ref = _vols().document(vid)
    snap = await run_in_threadpool(ref.get)
    if not snap.exists:
        raise HTTPException(404, "volunteer not found")
    if body.available and snap.get("currentIncident"):
        raise HTTPException(409, "volunteer is assigned to an incident; release first")
    await run_in_threadpool(ref.update, {"available": body.available, "lastSeen": now_ms()})
    return _public(await run_in_threadpool(ref.get))


@app.post("/api/volunteers/{vid}/heartbeat")
async def heartbeat(vid: str, body: HeartbeatIn):
    upd = {"lastSeen": now_ms()}
    if body.latitude is not None and body.longitude is not None:
        upd["location"] = firestore.GeoPoint(body.latitude, body.longitude)
    try:
        await run_in_threadpool(_vols().document(vid).update, upd)
    except Exception:  # google.api_core.exceptions.NotFound
        raise HTTPException(404, "volunteer not found")
    return {"id": vid, "lastSeen": upd["lastSeen"]}


@app.post("/api/volunteers/assign")
async def assign(body: AssignIn):
    """Transactionally pick `count` available volunteers in the region and mark them busy.
    The Firestore transaction retries automatically if another dispatcher grabs the same volunteer."""
    db = state["db"]

    @firestore.transactional
    def _tx(tx):
        q = (_vols().where(filter=firestore.FieldFilter("region", "==", body.region))
             .where(filter=firestore.FieldFilter("available", "==", True)).limit(body.count * 3))
        candidates = list(q.stream(transaction=tx))
        if body.skills:
            candidates.sort(key=lambda d: -len(set(d.get("skills") or []) & set(body.skills)))
        chosen = candidates[: body.count]
        for d in chosen:
            tx.update(d.reference, {"available": False, "currentIncident": body.incidentId, "lastSeen": now_ms()})
        tx.set(db.collection("volunteer_assignments").document(body.incidentId),
               {"incidentId": body.incidentId, "region": body.region, "volunteerIds": [d.id for d in chosen],
                "status": "ACTIVE", "createdAt": now_ms()}, merge=True)
        return [_public(d) for d in chosen]

    assigned = await run_in_threadpool(lambda: _tx(db.transaction()))
    log.info("volunteers assigned", extra={"incidentId": body.incidentId, "count": len(assigned)})
    return {"incidentId": body.incidentId, "assigned": assigned, "shortfall": body.count - len(assigned)}


@app.post("/api/volunteers/release")
async def release(body: ReleaseIn):
    db = state["db"]

    def _do():
        ref = db.collection("volunteer_assignments").document(body.incidentId)
        snap = ref.get()
        if not snap.exists:
            return 0
        ids = snap.get("volunteerIds") or []
        batch = db.batch()
        for vid in ids:
            batch.update(_vols().document(vid), {"available": True, "currentIncident": None, "lastSeen": now_ms()})
        batch.update(ref, {"status": "RELEASED", "releasedAt": now_ms()})
        batch.commit()
        return len(ids)
    return {"incidentId": body.incidentId, "released": await run_in_threadpool(_do)}


@app.post("/api/volunteers/_seed", status_code=201)
async def seed(perRegion: int = Query(default=6, ge=1, le=50)):
    def _do():
        batch, n = state["db"].batch(), 0
        for r in REGIONS:
            for i in range(perRegion):
                vid = f"vol-seed-{r}-{i}"
                batch.set(_vols().document(vid), {
                    "name": f"Volunteer {r.title()} {i}", "phone": f"+9198{random.randint(10000000, 99999999)}",
                    "region": r, "skills": random.sample(SKILLS, 3), "available": True, "currentIncident": None,
                    "createdAt": now_ms(), "lastSeen": now_ms()})
                n += 1
        batch.commit()
        return n
    return {"seeded": await run_in_threadpool(_do)}
