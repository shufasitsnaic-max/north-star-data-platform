"""Shared constants for the cold-path DAGs.

Kept in one place so the two DAGs cannot disagree about where the jobs live or
how they are launched — a divergence there would show up as one DAG silently
running in local mode while the other used the cluster.
"""

from __future__ import annotations

import pendulum

# Paths inside the scheduler container, established by the bind mounts in
# docker-compose.yml. The jobs are mounted read-only; only the lake is writable.
JOBS = "/opt/batch_jobs/jobs"
SQL = "/opt/batch_jobs/sql"

# The serving store, reached through an Airflow connection supplied as
# AIRFLOW_CONN_NORTHSTAR_PG in the environment rather than created by hand in
# the UI — a connection that only exists in the metadata database is invisible
# to anyone reading this repository, and vanishes with the volume.
POSTGRES_CONN_ID = "northstar_pg"

# Every DAG here is a recompute, so a start date only has to be in the past.
# Fixed rather than dynamic: a start_date that moves makes run history
# unreproducible.
START_DATE = pendulum.datetime(2026, 1, 1, tz="UTC")

# spark-submit is on PATH from the pyspark install in the image. The jobs read
# SPARK_MASTER from the environment (see the note below), so no --master flag
# appears anywhere in these DAGs.
SPARK_SUBMIT = "spark-submit"


def submit(job: str, args: str = "") -> str:
    """The shell command for one cold-path job.

    Note what is deliberately absent. `--master` is not passed: common/spark.py
    calls .master(config.SPARK_MASTER), and a builder's explicit .master()
    overrides whatever spark-submit was given, so the flag would be a silent
    no-op and the job would run local[*] inside the scheduler. The master URL
    reaches the job as the SPARK_MASTER environment variable instead, set on
    the scheduler service.

    `--packages` is not passed either, for the opposite reason: connector JARs
    are resolved before the JVM exists, so they cannot come from config at all.
    They are declared cluster-wide in batch_jobs/conf/spark-defaults.conf, which
    this container reaches through SPARK_CONF_DIR.

    Two settings that look alike and behave oppositely; both cost a debugging
    round in P4 and are recorded in docs/DECISIONS.md.
    """
    return f"{SPARK_SUBMIT} {JOBS}/{job}" + (f" {args}" if args else "")
