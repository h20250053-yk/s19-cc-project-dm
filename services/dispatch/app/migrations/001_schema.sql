-- Dispatch Service schema (same Cloud SQL instance/database, separate tables & migration table)
CREATE TABLE IF NOT EXISTS dispatch_assignments (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    incident_id    TEXT NOT NULL,
    region         TEXT NOT NULL,
    severity       TEXT NOT NULL,
    resource_ids   TEXT[] NOT NULL DEFAULT '{}',
    volunteer_ids  TEXT[] NOT NULL DEFAULT '{}',
    status         TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'COMPLETED', 'FAILED')),
    mode           TEXT NOT NULL DEFAULT 'MANUAL' CHECK (mode IN ('MANUAL', 'AUTO')),
    shortfall      INTEGER NOT NULL DEFAULT 0,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at   TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_dispatch_incident ON dispatch_assignments(incident_id);
CREATE INDEX IF NOT EXISTS idx_dispatch_status ON dispatch_assignments(status, created_at DESC);
-- one active dispatch per incident (prevents duplicate auto-dispatch on Kafka redelivery)
CREATE UNIQUE INDEX IF NOT EXISTS uq_dispatch_active_incident
    ON dispatch_assignments(incident_id) WHERE status = 'ACTIVE';
