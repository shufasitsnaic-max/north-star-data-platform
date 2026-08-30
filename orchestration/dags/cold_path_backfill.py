"""One-shot historical backfill: raw monthly files -> the lake -> the serving store.

Run manually. This is the *third* thing in the cold path, and worth naming
precisely because the other two are easy to confuse it with: the hot path serves
fresh numbers, `cold_path_incremental` recomputes the same events completely,
and this DAG loads history that never transited the message bus at all. It
exists to supply the ML training corpus and the dashboard's long-run trends.

Shape:

    bulk_load[2023-01] ─┐
    bulk_load[2023-02] ─┤
    ...                 ├──> aggregate_history ──> merge_into_serving
    bulk_load[2025-12] ─┘                       ↑
    ensure_schema ──────────────────────────────┘

`aggregate_history` is the one full-lake aggregation. The recurring DAG
deliberately scopes itself to post-cutoff days — backfilled history never
changes, so re-reading it every three minutes would cost tens of millions of
row-reads to reproduce numbers already stored. That means loading months
without this DAG's final tasks would leave them in the lake and *absent* from
the serving store, so the two run together.

Trigger it from the UI, or:

    docker compose exec airflow-scheduler airflow dags trigger cold_path_backfill
"""

from __future__ import annotations

import os
from datetime import date

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from common import POSTGRES_CONN_ID, SQL, START_DATE, submit


def _months(start: str, end: str) -> list[str]:
    """Every 'YYYY-MM' from start to end inclusive."""
    start_year, start_month = (int(part) for part in start.split("-"))
    end_year, end_month = (int(part) for part in end.split("-"))
    months = []
    year, month = start_year, start_month
    while (year, month) <= (end_year, end_month):
        months.append(f"{year:04d}-{month:02d}")
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return months


# The range this backfill owns, as configuration rather than code, so the scope
# can be trimmed without editing a DAG — the lake runs ~200MB per month, and
# disk is the binding constraint on a codespace.
#
# Every month must be at or before the cutoff. bulk_load refuses a later one
# rather than writing nothing, so a mistake here fails the task loudly instead
# of producing an empty partition.
MONTHS = _months(
    os.environ.get("BACKFILL_START", "2023-01"),
    os.environ.get("BACKFILL_END", "2025-12"),
)

# Everything, for the one full aggregation. Any date before the source's first
# record does; this one is unambiguous about intent.
EPOCH = date(1970, 1, 1).isoformat()

with DAG(
    dag_id="cold_path_backfill",
    description="One-shot: load historical months into the lake and publish their metrics",
    start_date=START_DATE,
    # Manual only. Running this on a schedule would re-read and rewrite the same
    # immutable history forever.
    schedule=None,
    catchup=False,
    # Two overlapping runs would write the same partitions with dynamic
    # overwrite and race each other.
    max_active_runs=1,
    template_searchpath=[SQL],
    tags=["cold-path", "backfill"],
) as dag:
    # Dynamic task mapping: one task per month, so a month that fails retries
    # alone instead of redoing the rest. This is exactly why bulk_load takes
    # --month and processes one file per invocation.
    bulk_load = BashOperator.partial(
        task_id="bulk_load",
        # Cap concurrency: dozens of simultaneous Spark applications would each
        # ask a four-core machine for executors and spend more time contending
        # than working. Two keeps the worker busy without thrashing.
        max_active_tis_per_dag=2,
        # append_env keeps the container's environment — SPARK_MASTER,
        # SPARK_CONF_DIR, JAVA_HOME and PATH all matter, and replacing the
        # environment wholesale would strip them.
        append_env=True,
        retries=2,
    ).expand(
        bash_command=[submit("bulk_load.py", f"--month {month}") for month in MONTHS]
    )

    # The full-lake pass, run once here rather than every three minutes there.
    aggregate_history = BashOperator(
        task_id="aggregate_history",
        bash_command=submit("aggregate_daily.py", f"--since {EPOCH}"),
        append_env=True,
    )

    ensure_schema = SQLExecuteQueryOperator(
        task_id="ensure_schema",
        conn_id=POSTGRES_CONN_ID,
        sql="schema.sql",
    )

    merge_into_serving = SQLExecuteQueryOperator(
        task_id="merge_into_serving",
        conn_id=POSTGRES_CONN_ID,
        sql="merge_daily.sql",
    )

    bulk_load >> aggregate_history >> merge_into_serving
    ensure_schema >> merge_into_serving
