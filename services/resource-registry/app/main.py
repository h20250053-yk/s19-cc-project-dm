"""Resource Registry - emergency vehicles, supply kits and response teams (Cloud SQL).

REST (port 8080, prefix /api/resources)
  POST  /api/resources                         register a resource
  GET   /api/resources?region=&kind=&status=   list / filter
  GET   /api/resources/stats                   counts by region/kind/status (used by Situation Analytics)
  GET   /api/resources/{id}
  PATCH /api/resources/{id}/status             body {"status": "...", "version": n}  (optimistic locking)
  POST  /api/resources/reserve                 body {"incidentId","region","kind","quantity"}  (used by Dispatch)
  POST  /api/resources/release                 body {"incidentId"}
Storage: Cloud SQL PostgreSQL tables resources, resource_reservations
"""
from __future__ import annotations

import contextlib
import pathlib
from enum import Enum
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from psycopg.rows import dict_row
from pydantic import BaseModel, Field

from s19common.db import migrate, open_pool
from s19common.obs import instrument, setup_logging

SERVICE = "resource-registry"
log = setup_logging(SERVICE)


class Kind(str, Enum):
    VEHICLE = "VEHICLE"
    SUPPLY = "SUPPLY"
    TEAM = "TEAM"


class RStatus(str, Enum):
    AVAILABLE = "AVAILABLE"
    RESERVED = "RESERVED"
    DEPLOYED = "DEPLOYED"
    MAINTENANCE = "MAINTENANCE"


class ResourceIn(BaseModel):
    kind: Kind
    subtype: str = Field(min_length=2, max_length=40, pattern=r"^[a-z0-9-]+$")
    name: str = Field(min_length=2, max_length=80)
    region: str = Field(min_length=2, max_length=32, pattern=r"^[a-z0-9-]+$")
    capacity: int = Field(default=1, ge=0, le=100000)
    latitude: Optional[float] = Field(default=None, ge=-90, le=90)
    longitude: Optional[float] = Field(default=None, ge=-180, le=180)


class StatusIn(BaseModel):
    status: RStatus
    version: int = Field(ge=1)


class ReserveIn(BaseModel):
    incidentId: str = Field(min_length=3, max_length=64)
    region: str = Field(pattern=r"^[a-z0-9-]+$")
    kind: Optional[Kind] = None
    quantity: int = Field(default=1, ge=1, le=20)


class ReleaseIn(BaseModel):
    incidentId: str = Field(min_length=3, max_length=64)
    deployedToAvailable: bool = True


state: dict = {}
COLS = "id::text AS id, kind, subtype, name, region, status, capacity, latitude, longitude, version, " \
       "created_at, updated_at"


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    pool = await open_pool()
    applied = await migrate(pool, pathlib.Path(__file__).parent / "migrations", "registry_schema_migrations")
    log.info("registry ready", extra={"migrationsApplied": applied})
    state["pool"] = pool
    yield
    await pool.close()


app = FastAPI(title="S19 Resource Registry", version="1.0.0", lifespan=lifespan)
instrument(app, SERVICE)


async def fetch(sql: str, params=(), one=False):
    async with state["pool"].connection() as conn:
        cur = conn.cursor(row_factory=dict_row)
        await cur.execute(sql, params)
        return await (cur.fetchone() if one else cur.fetchall())


@app.post("/api/resources", status_code=201)
async def create_resource(r: ResourceIn):
    return await fetch(
        f"INSERT INTO resources(kind, subtype, name, region, capacity, latitude, longitude) "
        f"VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING {COLS}",
        (r.kind.value, r.subtype, r.name, r.region, r.capacity, r.latitude, r.longitude), one=True)


@app.get("/api/resources")
async def list_resources(region: Optional[str] = None, kind: Optional[Kind] = None,
                         status: Optional[RStatus] = None, limit: int = Query(default=100, ge=1, le=500)):
    where, params = [], []
    for col, val in (("region", region), ("kind", kind.value if kind else None),
                     ("status", status.value if status else None)):
        if val:
            where.append(f"{col} = %s")
            params.append(val)
    sql = f"SELECT {COLS} FROM resources {'WHERE ' + ' AND '.join(where) if where else ''} " \
          f"ORDER BY region, kind, name LIMIT %s"
    return {"items": await fetch(sql, (*params, limit))}


@app.get("/api/resources/stats")
async def stats():
    rows = await fetch("SELECT region, kind, status, count(*)::int AS count FROM resources "
                       "GROUP BY region, kind, status ORDER BY region, kind, status")
    active = await fetch("SELECT count(*)::int AS n FROM resource_reservations WHERE status='ACTIVE'", one=True)
    summary: dict = {}
    for r in rows:
        summary.setdefault(r["region"], {}).setdefault(r["kind"], {})[r["status"]] = r["count"]
    return {"byRegion": summary, "activeReservations": active["n"], "rows": rows}


@app.get("/api/resources/{resource_id}")
async def get_resource(resource_id: str):
    row = await fetch(f"SELECT {COLS} FROM resources WHERE id::text = %s", (resource_id,), one=True)
    if not row:
        raise HTTPException(404, "resource not found")
    return row


@app.patch("/api/resources/{resource_id}/status")
async def set_status(resource_id: str, body: StatusIn):
    row = await fetch(
        f"UPDATE resources SET status=%s, version=version+1, updated_at=now() "
        f"WHERE id::text=%s AND version=%s RETURNING {COLS}",
        (body.status.value, resource_id, body.version), one=True)
    if not row:
        raise HTTPException(409, "resource not found or version conflict (re-read and retry)")
    return row


@app.post("/api/resources/reserve")
async def reserve(body: ReserveIn):
    """Atomically reserve up to `quantity` AVAILABLE resources in the region.
    FOR UPDATE SKIP LOCKED lets concurrent dispatchers grab different rows without blocking."""
    kind_sql = "AND kind = %s" if body.kind else ""
    params = [body.region] + ([body.kind.value] if body.kind else []) + [body.quantity]
    async with state["pool"].connection() as conn:
        async with conn.transaction():
            cur = conn.cursor(row_factory=dict_row)
            await cur.execute(
                f"SELECT id FROM resources WHERE region=%s AND status='AVAILABLE' {kind_sql} "
                f"ORDER BY capacity DESC LIMIT %s FOR UPDATE SKIP LOCKED", params)
            ids = [r["id"] for r in await cur.fetchall()]
            if not ids:
                return {"incidentId": body.incidentId, "reserved": [], "shortfall": body.quantity}
            await cur.execute(
                f"UPDATE resources SET status='RESERVED', version=version+1, updated_at=now() "
                f"WHERE id = ANY(%s) RETURNING {COLS}", (ids,))
            reserved = await cur.fetchall()
            for rid in ids:
                await cur.execute("INSERT INTO resource_reservations(resource_id, incident_id) VALUES (%s,%s)",
                                  (rid, body.incidentId))
    log.info("resources reserved", extra={"incidentId": body.incidentId, "count": len(reserved)})
    return {"incidentId": body.incidentId, "reserved": reserved, "shortfall": body.quantity - len(reserved)}


@app.post("/api/resources/release")
async def release(body: ReleaseIn):
    async with state["pool"].connection() as conn:
        async with conn.transaction():
            cur = conn.cursor(row_factory=dict_row)
            await cur.execute(
                "UPDATE resource_reservations SET status='RELEASED', released_at=now() "
                "WHERE incident_id=%s AND status='ACTIVE' RETURNING resource_id", (body.incidentId,))
            ids = [r["resource_id"] for r in await cur.fetchall()]
            if ids:
                await cur.execute(
                    "UPDATE resources SET status='AVAILABLE', version=version+1, updated_at=now() "
                    "WHERE id = ANY(%s)", (ids,))
    return {"incidentId": body.incidentId, "released": len(ids)}
