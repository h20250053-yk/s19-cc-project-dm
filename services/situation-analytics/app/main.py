"""Situation Analytics Service - serving side of the analytics pipeline.

Pipeline:  incident-events + dispatch-events + consistency-events (Kafka)
             -> Flink job on Dataproc (1-minute tumbling event-time windows, stateful surge detection)
             -> incident-aggregates (Kafka)
             -> this service (consumer) -> Firestore analytics_windows/{region}_{windowStart}
REST (port 8080, prefix /api/analytics)
  GET /api/analytics/windows?region=&limit=   latest window aggregates
  GET /api/analytics/summary                  incident trend + resource demand + volunteers + consistency
  GET /api/analytics/consistency              Dynamo outcomes: conflicts, read repairs, quorum failures
  GET /api/analytics/alerts?limit=            recent alerts written by the serverless Alert Service
"""
from __future__ import annotations

import contextlib
import os
import time
from typing import Optional

import httpx
from fastapi import FastAPI, Query
from google.cloud import firestore
from prometheus_client import Counter, Gauge
from starlette.concurrency import run_in_threadpool

from s19common.gcp import firestore_client
from s19common.kafka_util import TOPIC_AGGREGATES, KafkaConsumerThread
from s19common.obs import instrument, setup_logging

SERVICE = "situation-analytics"
log = setup_logging(SERVICE)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "")
COORDINATOR_URL = os.getenv("COORDINATOR_URL", "http://quorum-coordinator:8080")
REGISTRY_URL = os.getenv("REGISTRY_URL", "http://resource-registry:8080")
VOLUNTEER_URL = os.getenv("VOLUNTEER_URL", "http://volunteer-coordination:8080")

WINDOWS_STORED = Counter("analytics_windows_stored_total", "Flink window results persisted", ["region"])
WINDOW_LAG = Gauge("analytics_window_lag_seconds", "now - windowEnd of the newest Flink result")
SURGES = Counter("analytics_surges_total", "Windows flagged as surge by Flink", ["region"])
LAST_WINDOW_INCIDENTS = Gauge("analytics_last_window_incidents", "Incidents in latest window", ["region"])

state: dict = {"last_window_end": 0}


def _store_window(agg: dict) -> None:
    """Kafka consumer thread: persist one Flink window result to Firestore."""
    region = agg.get("region", "global")
    doc_id = f"{region}_{agg['windowStart']}"
    state["db"].collection("analytics_windows").document(doc_id).set({**agg, "storedAt": int(time.time() * 1000)})
    WINDOWS_STORED.labels(region).inc()
    LAST_WINDOW_INCIDENTS.labels(region).set(agg.get("incidents", 0))
    if agg.get("surge"):
        SURGES.labels(region).inc()
    state["last_window_end"] = max(state["last_window_end"], int(agg.get("windowEnd", 0)))
    WINDOW_LAG.set(max(0.0, time.time() - state["last_window_end"] / 1000.0))


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    state["db"] = firestore_client()
    state["http"] = httpx.AsyncClient(timeout=4.0)
    consumer = KafkaConsumerThread(KAFKA_BOOTSTRAP, "situation-analytics", [TOPIC_AGGREGATES], _store_window,
                                   offset_reset="earliest")
    consumer.start()
    state["consumer"] = consumer
    yield
    consumer.stop()
    await state["http"].aclose()


app = FastAPI(title="S19 Situation Analytics", version="1.0.0", lifespan=lifespan)
instrument(app, SERVICE)


def _windows(region: Optional[str], limit: int) -> list:
    q = state["db"].collection("analytics_windows")
    if region:
        q = q.where(filter=firestore.FieldFilter("region", "==", region))
    q = q.order_by("windowStart", direction=firestore.Query.DESCENDING).limit(limit)
    return [d.to_dict() for d in q.stream()]


async def _get_json(url: str) -> dict:
    try:
        r = await state["http"].get(url)
        return r.json() if r.status_code == 200 else {"error": r.status_code}
    except httpx.HTTPError as exc:
        return {"error": str(exc)}


@app.get("/api/analytics/windows")
async def windows(region: Optional[str] = Query(default=None, pattern=r"^[a-z0-9-]{2,32}$"),
                  limit: int = Query(default=20, ge=1, le=200)):
    if state["last_window_end"]:
        WINDOW_LAG.set(max(0.0, time.time() - state["last_window_end"] / 1000.0))
    return {"items": await run_in_threadpool(_windows, region, limit)}


@app.get("/api/analytics/consistency")
async def consistency():
    recent = await run_in_threadpool(_windows, None, 60)
    totals = {k: sum(int(w.get(k, 0)) for w in recent) for k in ("conflicts", "readRepairs", "quorumFailures")}
    return {"lastWindowsTotals": totals, "windowsConsidered": len(recent),
            "coordinatorPodStats": await _get_json(f"{COORDINATOR_URL}/api/quorum/stats"),
            "replicas": await _get_json(f"{COORDINATOR_URL}/api/quorum/replicas")}


@app.get("/api/analytics/alerts")
async def alerts(limit: int = Query(default=20, ge=1, le=200)):
    def _q():
        q = state["db"].collection("alerts").order_by("createdAt", direction=firestore.Query.DESCENDING).limit(limit)
        return [d.to_dict() for d in q.stream()]
    return {"items": await run_in_threadpool(_q)}


@app.get("/api/analytics/summary")
async def summary():
    recent = await run_in_threadpool(_windows, None, 50)
    latest: dict = {}
    for w in recent:                                   # newest first -> keep first per region
        latest.setdefault(w["region"], w)
    c = state.get("consumer")
    return {
        "generatedAt": int(time.time() * 1000),
        "latestWindowByRegion": latest,
        "surgingRegions": sorted(r for r, w in latest.items() if w.get("surge")),
        "resourceDemand": await _get_json(f"{REGISTRY_URL}/api/resources/stats"),
        "volunteers": await _get_json(f"{VOLUNTEER_URL}/api/volunteers/stats"),
        "consistency": await _get_json(f"{COORDINATOR_URL}/api/quorum/stats"),
        "pipeline": {"consumerProcessed": c.processed if c else 0, "consumerLastError": c.last_error if c else None,
                     "lastWindowEnd": state["last_window_end"]},
    }
