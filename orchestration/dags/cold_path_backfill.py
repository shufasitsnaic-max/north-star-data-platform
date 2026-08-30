"""One-shot historical backfill: raw monthly files -> the lake.

Run manually, once. This is the *third* thing in the cold path, and worth
naming precisely because the other two are easy to confuse it with: the hot
path serves fresh numbers, `cold_path_incremental` recomputes the same events
completely, and this DAG loads history that never transited the message bus at
all. It exists to supply the ML training corpus and the dashboard's long-run
trends.

Trigger it from the UI, or:

    docker compose exec airflow-scheduler airflow dags trigger cold_path_backfill
"""

from __future__ import annotations

from airflow import DAG
from airflow.operators.bash import BashOperator

from common import START_DATE, submit

# The months this backfill owns, all at or before the cutoff. Twelve rather
# than the full thirty-six by a deliberate scope decision (docs/DECISIONS.md,
# 2026-08-30): ~50M rows is already far more than scikit-learn will train on,
# and the remaining months can be added by extending this list and
# re-triggering — the DAG maps over it either way.
#
# Every month here must be <= the cutoff. bulk_load refuses a later one rather
# than writing nothing, so a mistake in this list fails the task loudly instead
# of silently producing an empty partition.
MONTHS = [f"2025-{month:02d}" for month in range(1, 13)]

with DAG(
    dag_id="cold_path_backfill",
    description="One-shot: load historical monthly files into the lake",
    start_date=START_DATE,
    # Manual only. This is not a recurring job — running it on a schedule would
    # re-read and rewrite the same immutable history forever.
    schedule=None,
    catchup=False,
    # Two overlapping runs would write the same partitions with dynamic
    # overwrite and race each other.
    max_active_runs=1,
    tags=["cold-path", "backfill"],
) as dag:
    # Dynamic task mapping: one task per month, so a month that fails retries
    # alone instead of redoing the other eleven. This is exactly why bulk_load
    # takes --month and processes one file per invocation.
    BashOperator.partial(
        task_id="bulk_load",
        # Cap concurrency: twelve simultaneous Spark applications would each ask
        # the cluster for executors on a machine with four cores, and spend more
        # time contending than working. Two keeps the worker busy without
        # thrashing.
        max_active_tis_per_dag=2,
        # append_env keeps the container's environment — SPARK_MASTER,
        # SPARK_CONF_DIR, JAVA_HOME and PATH all matter here, and replacing the
        # environment wholesale would strip them.
        append_env=True,
        retries=2,
    ).expand(
        bash_command=[submit("bulk_load.py", f"--month {month}") for month in MONTHS]
    )
