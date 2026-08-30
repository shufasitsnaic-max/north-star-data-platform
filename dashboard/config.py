"""Environment-driven configuration for the dashboard.

Read-only credentials by intent: the dashboard is a view, and nothing here
should ever be used to write.
"""

from __future__ import annotations

import os

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "northstar")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "northstar")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "northstar")


def postgres_dsn() -> str:
    return (
        f"host={POSTGRES_HOST} port={POSTGRES_PORT} dbname={POSTGRES_DB} "
        f"user={POSTGRES_USER} password={POSTGRES_PASSWORD}"
    )
