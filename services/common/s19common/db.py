"""Cloud SQL (PostgreSQL) helpers: async connection pool + file-based migrations.

Connection: GKE pods reach the Cloud SQL instance on its *private IP* (Private
Service Access peering created by Terraform). Credentials come from the
Kubernetes Secret `cloudsql-credentials` (also created by Terraform).
"""
from __future__ import annotations

import logging
import os
import pathlib

from psycopg_pool import AsyncConnectionPool

log = logging.getLogger("db")


def dsn() -> str:
    return (f"host={os.getenv('DB_HOST', 'localhost')} port={os.getenv('DB_PORT', '5432')} "
            f"dbname={os.getenv('DB_NAME', 's19')} user={os.getenv('DB_USER', 's19app')} "
            f"password={os.getenv('DB_PASSWORD', 's19app')} connect_timeout=5 "
            f"application_name={os.getenv('HOSTNAME', 's19')}")


async def open_pool(min_size: int = 1, max_size: int = 5) -> AsyncConnectionPool:
    # db-f1-micro allows ~25 connections: 2 services x up to 3 pods x 5 = 30 worst case,
    # so keep max_size small (DB_POOL_MAX) and HPA off for these two services.
    pool = AsyncConnectionPool(dsn(), min_size=min_size,
                               max_size=int(os.getenv("DB_POOL_MAX", str(max_size))), open=False)
    await pool.open(wait=True, timeout=30)
    return pool


async def migrate(pool: AsyncConnectionPool, migrations_dir: pathlib.Path, table: str) -> list[str]:
    """Apply *.sql files in name order exactly once. A Postgres advisory lock
    serialises concurrent pods so two replicas never run the same migration."""
    applied = []
    async with pool.connection() as conn:
        await conn.execute("SELECT pg_advisory_lock(hashtext(%s))", (table,))
        try:
            await conn.execute(
                f"CREATE TABLE IF NOT EXISTS {table} (version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ DEFAULT now())")
            done = {r[0] for r in await (await conn.execute(f"SELECT version FROM {table}")).fetchall()}
            for f in sorted(migrations_dir.glob("*.sql")):
                if f.name in done:
                    continue
                async with conn.transaction():
                    await conn.execute(f.read_text())
                    await conn.execute(f"INSERT INTO {table}(version) VALUES (%s)", (f.name,))
                applied.append(f.name)
                log.info("migration applied", extra={"migration": f.name, "table": table})
        finally:
            await conn.execute("SELECT pg_advisory_unlock(hashtext(%s))", (table,))
    return applied
