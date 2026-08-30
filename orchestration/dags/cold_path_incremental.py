"""The recurring cold path: bus -> lake -> daily rollups -> serving store.

This is the batch layer of the lambda architecture, processing **the same
events the hot path already saw** — minutes later instead of seconds,
completely instead of within a watermark, and rerunnably instead of once. When
the two layers disagree, this pipeline holds the number to trust.

Shape:

    stream_to_lake ──> aggregate_daily ──┐
                                         ├──> merge_into_serving
    ensure_schema ───────────────────────┘

`ensure_schema` has no data dependency on the Spark tasks, so it runs alongside
them rather than in front — the DDL only has to be in place before the merge.
"""

from __future__ import annotations

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from common import POSTGRES_CONN_ID, SQL, START_DATE, submit

with DAG(
    dag_id="cold_path_incremental",
    description="Recompute the post-cutoff lake and republish daily metrics",
    start_date=START_DATE,
    # Wall-clock, not @daily, and the distinction matters. The simulator
    # compresses event time by roughly 366x, so a @daily DAG would fire *zero*
    # times during a ten-minute demo while the replay burns through a year of
    # event time. "Daily batch" is the story the metrics tell; three minutes is
    # the schedule that tells it.
    schedule="*/3 * * * *",
    # Without this, Airflow backfills a run for every three-minute interval
    # since start_date — thousands of runs on first boot.
    catchup=False,
    # Two overlapping recomputes write the same partitions with dynamic
    # overwrite, and would race.
    max_active_runs=1,
    # Lets the merge task load merge_daily.sql by name instead of embedding a
    # second copy of SQL that psql already verified.
    template_searchpath=[SQL],
    tags=["cold-path", "incremental"],
) as dag:
    stream_to_lake = BashOperator(
        task_id="stream_to_lake",
        bash_command=submit("stream_to_lake.py"),
        append_env=True,
    )

    aggregate_daily = BashOperator(
        task_id="aggregate_daily",
        bash_command=submit("aggregate_daily.py"),
        append_env=True,
    )

    # Idempotent DDL, applied every run rather than once at init. A fresh
    # serving-store volume then heals itself on the next scheduled run instead
    # of failing every merge until someone notices.
    ensure_schema = SQLExecuteQueryOperator(
        task_id="ensure_schema",
        conn_id=POSTGRES_CONN_ID,
        sql="schema.sql",
    )

    # The staging -> serving merge. Spark's JDBC writer has no upsert, and
    # overwriting the serving table would either drop its indexes or leave the
    # dashboard reading an empty table mid-write.
    merge_into_serving = SQLExecuteQueryOperator(
        task_id="merge_into_serving",
        conn_id=POSTGRES_CONN_ID,
        sql="merge_daily.sql",
    )

    stream_to_lake >> aggregate_daily >> merge_into_serving
    ensure_schema >> merge_into_serving
