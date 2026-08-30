"""Score the model's quotes against what the trips actually cost.

The rubric's "compare earlier predictions with the actual outcome, and evaluate
every day". One row per event-time day per model version, recomputed in full on
every run.

Shape:

    ensure_ml_schema ──> evaluate_daily

Why a wall-clock schedule for a "daily" job, again
--------------------------------------------------
Same reasoning as `cold_path_incremental`, and worth repeating because it looks
wrong at first glance. The evaluation is *per event-time day* — each row scores
the trips that happened on one calendar day of 2026. But the replay compresses
event time by roughly 366x, so several event-time days pass in a few minutes of
wall clock. A `@daily` schedule would fire once while the simulator burned
through a year of trips. Five minutes of wall clock keeps the table current
without pretending the cadence and the grain are the same thing.
"""

from __future__ import annotations

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from common import ML_SQL, POSTGRES_CONN_ID, START_DATE

with DAG(
    dag_id="ml_daily_eval",
    description="Daily predicted-vs-actual error metrics for the fare model",
    start_date=START_DATE,
    schedule="*/5 * * * *",
    # Otherwise Airflow backfills a run for every five-minute interval since the
    # start date.
    catchup=False,
    # The evaluation rewrites every day's row, so two overlapping runs would
    # write the same rows concurrently.
    max_active_runs=1,
    template_searchpath=[ML_SQL],
    tags=["ml", "evaluation"],
) as dag:
    # Idempotent DDL every run, so a fresh serving-store volume heals itself
    # instead of failing every evaluation until someone notices. The predictor
    # applies the same file on start; whichever runs first wins and the other is
    # a no-op.
    ensure_ml_schema = SQLExecuteQueryOperator(
        task_id="ensure_ml_schema",
        conn_id=POSTGRES_CONN_ID,
        sql="schema.sql",
    )

    evaluate_daily = SQLExecuteQueryOperator(
        task_id="evaluate_daily",
        conn_id=POSTGRES_CONN_ID,
        sql="evaluate_daily.sql",
    )

    ensure_ml_schema >> evaluate_daily
