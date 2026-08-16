"""PostgreSQL sink for window metrics.

The only module in the hot path that knows a database exists. Mirrors the
gateway producer's connection discipline: probe with exponential backoff before
declaring failure, so a consumer that boots ahead of PostgreSQL waits instead of
crash-looping.

Writes are **absolute upserts** (`SET`, not `+=`). That is safe only because
windows.py withholds offset commits until a window closes, so any redelivered
window is rebuilt in full before it is rewritten — see the offset-safety note
there. An additive upsert would double-count the same events on redelivery.
"""

from __future__ import annotations

import logging
import os
import sys
import time
from pathlib import Path
from typing import Sequence

import psycopg

from windows import MetricRow

logging.basicConfig(stream=sys.stderr, level=logging.INFO)
logger = logging.getLogger("hot_path.db")

_SCHEMA_PATH = Path(__file__).parent / "schema.sql"

# Absolute upsert keyed on the COALESCE expression index in schema.sql. The
# conflict target must repeat the index expression verbatim for PostgreSQL to
# match it.
_UPSERT = """
INSERT INTO trip_window_metrics (
    window_start, window_end, zone_id, trip_count,
    total_revenue, avg_fare, avg_tip, avg_distance_km, is_final, updated_at
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, now())
ON CONFLICT (window_start, COALESCE(zone_id, -1)) DO UPDATE SET
    window_end      = EXCLUDED.window_end,
    trip_count      = EXCLUDED.trip_count,
    total_revenue   = EXCLUDED.total_revenue,
    avg_fare        = EXCLUDED.avg_fare,
    avg_tip         = EXCLUDED.avg_tip,
    avg_distance_km = EXCLUDED.avg_distance_km,
    is_final        = EXCLUDED.is_final,
    updated_at      = now()
"""


def _dsn() -> str:
    """Build the connection string from the environment.

    Defaults match the Compose service so the container works with no explicit
    configuration; every value is overridable for other environments.
    """
    host = os.environ.get("POSTGRES_HOST", "postgres")
    port = os.environ.get("POSTGRES_PORT", "5432")
    user = os.environ.get("POSTGRES_USER", "northstar")
    password = os.environ.get("POSTGRES_PASSWORD", "northstar")
    database = os.environ.get("POSTGRES_DB", "northstar")
    return f"host={host} port={port} user={user} password={password} dbname={database}"


class MetricsSink:
    """A long-lived connection plus the one write this phase needs."""

    def __init__(self) -> None:
        self._conn: psycopg.Connection | None = None

    def connect(self, max_attempts: int = 8) -> None:
        """Open a connection, retrying with capped exponential backoff."""
        delay = 1.0
        for attempt in range(1, max_attempts + 1):
            try:
                # autocommit off: each flush is one explicit transaction, so a
                # partial batch never lands.
                self._conn = psycopg.connect(_dsn(), autocommit=False)
                logger.info("connected to PostgreSQL (attempt %d)", attempt)
                return
            except psycopg.Error as exc:
                logger.warning(
                    "PostgreSQL not ready (attempt %d/%d): %s — retrying in %.0fs",
                    attempt, max_attempts, exc, delay,
                )
                time.sleep(delay)
                delay = min(delay * 2, 30)  # cap so we don't back off forever
        raise RuntimeError(f"could not reach PostgreSQL after {max_attempts} attempts")

    def apply_schema(self) -> None:
        """Run schema.sql. Idempotent — every statement is IF NOT EXISTS."""
        if self._conn is None:
            raise RuntimeError("sink not connected; call connect() first")
        ddl = _SCHEMA_PATH.read_text(encoding="utf-8")
        with self._conn.cursor() as cur:
            cur.execute(ddl)
        self._conn.commit()
        logger.info("schema applied")

    def write(self, rows: Sequence[MetricRow]) -> int:
        """Upsert a batch of window rows in one transaction. Returns row count."""
        if self._conn is None:
            raise RuntimeError("sink not connected; call connect() first")
        if not rows:
            return 0

        params = [
            (
                r.window_start, r.window_end, r.zone_id, r.trip_count,
                r.total_revenue, r.avg_fare, r.avg_tip, r.avg_distance_km, r.is_final,
            )
            for r in rows
        ]
        try:
            with self._conn.cursor() as cur:
                cur.executemany(_UPSERT, params)
            self._conn.commit()
            return len(rows)
        except psycopg.Error:
            # Roll back so the connection is reusable, and let the caller decide
            # whether to retry — offsets must not advance past a failed write.
            self._conn.rollback()
            logger.exception("failed to write %d metric row(s)", len(rows))
            raise

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
