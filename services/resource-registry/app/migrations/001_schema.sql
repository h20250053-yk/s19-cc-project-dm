-- Resource Registry schema (Cloud SQL for PostgreSQL 15; gen_random_uuid() is built in)

CREATE TABLE IF NOT EXISTS resources (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    kind        TEXT NOT NULL CHECK (kind IN ('VEHICLE', 'SUPPLY', 'TEAM')),
    subtype     TEXT NOT NULL,               -- ambulance, fire-truck, boat, water-kit, medical-team ...
    name        TEXT NOT NULL,
    region      TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'AVAILABLE'
                CHECK (status IN ('AVAILABLE', 'RESERVED', 'DEPLOYED', 'MAINTENANCE')),
    capacity    INTEGER NOT NULL DEFAULT 1 CHECK (capacity >= 0),
    latitude    DOUBLE PRECISION,
    longitude   DOUBLE PRECISION,
    version     INTEGER NOT NULL DEFAULT 1,  -- optimistic concurrency for PATCH
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_resources_region_kind_status ON resources(region, kind, status);

CREATE TABLE IF NOT EXISTS resource_reservations (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    resource_id  UUID NOT NULL REFERENCES resources(id),
    incident_id  TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'RELEASED')),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    released_at  TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_reservations_incident ON resource_reservations(incident_id, status);
CREATE UNIQUE INDEX IF NOT EXISTS uq_active_reservation
    ON resource_reservations(resource_id) WHERE status = 'ACTIVE';
