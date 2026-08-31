"""Every read the dashboard makes, in one place.

Strictly read-only. The dashboard is a *view* over the serving store — it owns
no tables, writes nothing, and can be stopped or restarted without any other
component noticing. That is what makes it safe to demo from.

It reads three tables written by three independent components, which is the
point worth making when presenting it: `trip_window_metrics` from the hot path,
`cold_daily_zone_metrics` from the cold path, and `fare_predictions` /
`ml_daily_eval` from the ML layer. None of them know this exists.

Caching
-------
Each query is cached with a TTL matched to how fast its source actually
changes: the hot path rewrites windows every couple of seconds, the cold path
every three minutes, the evaluation every five. Polling faster than the writer
would just add database load and show the same numbers.
"""

from __future__ import annotations

import pandas as pd
import psycopg
import streamlit as st

import config


def _fetch(sql: str, params: tuple = ()) -> pd.DataFrame:
    """Run one query and return a frame.

    Builds the frame from the cursor description rather than going through
    pandas' SQLAlchemy path, which would mean a dependency the dashboard has no
    other use for.
    """
    with psycopg.connect(config.postgres_dsn()) as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql, params)
            columns = [c.name for c in cursor.description]
            return pd.DataFrame(cursor.fetchall(), columns=columns)


# --------------------------------------------------------------------------
# Hot path — rolling windows, seconds behind the replay
# --------------------------------------------------------------------------

@st.cache_data(ttl=5)
def hot_windows(limit: int = 96) -> pd.DataFrame:
    """Recent citywide windows, oldest first for plotting.

    `zone_id IS NULL` is the citywide rollup, not a missing value — the hot path
    writes one such row per window alongside the per-zone rows.
    """
    return _fetch(
        """
        SELECT window_start, trip_count, total_revenue, avg_fare, avg_tip, is_final
        FROM trip_window_metrics
        WHERE zone_id IS NULL
        ORDER BY window_start DESC
        LIMIT %s
        """,
        (limit,),
    ).sort_values("window_start")


@st.cache_data(ttl=5)
def hot_top_zones(limit: int = 10) -> pd.DataFrame:
    """Busiest pickup zones in the most recent window."""
    return _fetch(
        """
        SELECT zone_id, trip_count, avg_fare
        FROM trip_window_metrics
        WHERE zone_id IS NOT NULL
          AND window_start = (SELECT max(window_start) FROM trip_window_metrics)
        ORDER BY trip_count DESC
        LIMIT %s
        """,
        (limit,),
    )


# --------------------------------------------------------------------------
# Cold path — the full history, recomputed from the lake
# --------------------------------------------------------------------------

@st.cache_data(ttl=60)
def cold_daily() -> pd.DataFrame:
    """Citywide daily totals across everything the lake holds."""
    return _fetch(
        """
        SELECT metric_date, trip_count, total_revenue, avg_fare, avg_tip, avg_distance_km
        FROM cold_daily_zone_metrics
        WHERE zone_id IS NULL
        ORDER BY metric_date
        """
    )


@st.cache_data(ttl=60)
def reconciliation() -> pd.DataFrame:
    """Hot vs cold trip counts for days both layers have seen.

    The platform's central claim, as a query: two independent code paths over
    the same events. Only post-cutoff days are comparable — backfilled days
    never transited the bus, so the hot path never saw them.
    """
    return _fetch(
        """
        SELECT c.metric_date,
               c.trip_count AS cold_trips,
               h.hot_trips
        FROM cold_daily_zone_metrics c
        JOIN (
            SELECT window_start::date AS d, sum(trip_count) AS hot_trips
            FROM trip_window_metrics
            WHERE zone_id IS NULL
            GROUP BY 1
        ) h ON h.d = c.metric_date
        WHERE c.zone_id IS NULL
        ORDER BY c.metric_date
        """
    )


# --------------------------------------------------------------------------
# ML layer — quotes and how wrong they were
# --------------------------------------------------------------------------

@st.cache_data(ttl=30)
def daily_eval() -> pd.DataFrame:
    """Daily predicted-vs-actual error, one row per event-time day."""
    return _fetch(
        """
        SELECT eval_date, model_version, predictions, mae, rmse, mape, r2,
               mean_actual, mean_predicted
        FROM ml_daily_eval
        ORDER BY eval_date
        """
    )


@st.cache_data(ttl=10)
def live_predictions(start, end, limit: int = 300) -> pd.DataFrame:
    """The scoring feed: individual quotes in the selected range, newest first.

    Predicted and actual sit side by side because they arrive together — a
    replayed event describes a finished trip, so it carries the fare with it.
    The error is computed in SQL rather than in pandas so the same definition
    serves the table and any future aggregate over it.

    `end=None` means no upper bound. That is what makes the feed *live*: the
    range widget's maximum is fixed when the page loads, so a bounded query can
    never show a day that started replaying since — the feed would re-run every
    ten seconds against a window nothing new can enter, and look frozen while
    working perfectly.
    """
    upper = "AND pickup_datetime < %s" if end is not None else ""
    params = (start, end, limit) if end is not None else (start, limit)
    return _fetch(
        f"""
        SELECT pickup_datetime,
               dropoff_datetime,
               pickup_zone_id,
               dropoff_zone_id,
               predicted_amount,
               actual_amount,
               predicted_amount - actual_amount AS error,
               CASE WHEN actual_amount <> 0
                    THEN 100 * (predicted_amount - actual_amount) / abs(actual_amount)
               END AS error_pct
        FROM fare_predictions
        WHERE pickup_datetime >= %s
          {upper}
        ORDER BY pickup_datetime DESC
        LIMIT %s
        """,
        params,
    )


@st.cache_data(ttl=10)
def prediction_windows(start, end) -> pd.DataFrame:
    """Quoted vs charged, averaged into 5-minute event-time windows.

    The same grain the hot path uses, so the two live panels are directly
    comparable. This is the chart that *grows* as a replay advances: each new
    window appends a point rather than redrawing history.

    `end=None` is open-ended, for the same reason as live_predictions().
    """
    upper = "AND pickup_datetime < %s" if end is not None else ""
    params = (start, end) if end is not None else (start,)
    return _fetch(
        f"""
        SELECT date_trunc('hour', pickup_datetime)
                 + (floor(date_part('minute', pickup_datetime) / 5) * interval '5 minutes')
                 AS window_start,
               count(*)              AS trips,
               avg(predicted_amount) AS predicted,
               avg(actual_amount)    AS actual
        FROM fare_predictions
        WHERE pickup_datetime >= %s
          {upper}
        GROUP BY 1
        ORDER BY 1
        """,
        params,
    )


@st.cache_data(ttl=30)
def prediction_range() -> pd.DataFrame:
    """Earliest and latest scored event time — the bounds of the date filter."""
    return _fetch(
        """
        SELECT min(pickup_datetime)::date AS first_day,
               max(pickup_datetime)::date AS last_day
        FROM fare_predictions
        """
    )


# Matched to the scoring feed's refresh interval: this drives the live/idle
# badge, so a 30s cache would report "idle" for twenty seconds after a replay
# started — the one moment the badge most needs to be right.
@st.cache_data(ttl=10)
def prediction_freshness() -> pd.DataFrame:
    """When the predictor last wrote anything. Drives the 'scoring idle' alert."""
    return _fetch(
        """
        SELECT max(predicted_at) AS last_written,
               count(*)          AS total,
               max(model_version) AS model_version
        FROM fare_predictions
        """
    )
