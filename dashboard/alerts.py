"""Anomaly detection over what the other three layers already computed.

The dashboard invents no analysis of its own. Each rule reads a number some
other component produced and says whether it is out of line — which keeps the
alerting honest: if an alert fires, the evidence for it is a row in a table
someone else wrote.

Every alert carries an icon, a title, and a sentence of *why*, not just a
colour. A red dot alone tells a reader who cannot see red nothing at all, and
tells everyone else nothing about what to do.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import pandas as pd


@dataclass
class Alert:
    status: str  # good | warning | serious | critical
    title: str
    detail: str


# A day's error this many times the median of the others is not noise.
_MAE_CRITICAL = 2.0
_MAE_SERIOUS = 1.5

# Below this, the model explains almost none of the variance in price.
_R2_SERIOUS = 0.30

# Hot and cold are expected to differ slightly; this much is worth explaining.
_DIVERGENCE_WARN = 0.05

# The predictor writes continuously while a replay runs.
_STALE_AFTER = timedelta(minutes=5)


def evaluate(
    daily_eval: pd.DataFrame,
    reconciliation: pd.DataFrame,
    freshness: pd.DataFrame,
) -> list[Alert]:
    """All alerts, most severe first."""
    alerts: list[Alert] = []
    alerts += _model_error(daily_eval)
    alerts += _explanatory_power(daily_eval)
    alerts += _layer_divergence(reconciliation)
    alerts += _scoring_stalled(freshness)

    if not alerts:
        alerts.append(
            Alert("good", "All checks clear", "No anomalies in the current window.")
        )

    order = {"critical": 0, "serious": 1, "warning": 2, "good": 3}
    return sorted(alerts, key=lambda a: order[a.status])


def _model_error(daily_eval: pd.DataFrame) -> list[Alert]:
    """Days whose error stands out against the rest.

    Compared against the median of the *other* days rather than a fixed
    threshold, so the rule keeps working if the model is retrained or the price
    level shifts — it asks "is this day unlike the others", which is the
    question, rather than "is this number big".
    """
    if len(daily_eval) < 2:
        return []

    found = []
    for _, row in daily_eval.iterrows():
        others = daily_eval[daily_eval["eval_date"] != row["eval_date"]]["mae"]
        if others.empty:
            continue
        baseline = float(others.median())
        if baseline <= 0:
            continue
        ratio = float(row["mae"]) / baseline

        if ratio >= _MAE_CRITICAL:
            status = "critical"
        elif ratio >= _MAE_SERIOUS:
            status = "serious"
        else:
            continue

        found.append(
            Alert(
                status,
                f"Fare estimates were {ratio:.1f}x worse on {row['eval_date']}",
                f"Mean error ${float(row['mae']):.2f} against ${baseline:.2f} on a typical "
                f"day, across {int(row['predictions']):,} quotes.",
            )
        )
    return found


def _explanatory_power(daily_eval: pd.DataFrame) -> list[Alert]:
    """Days where the model barely beat guessing the average.

    Reported separately from the error rule because it says something different.
    A high MAE on a day of expensive trips can still be a working model; an R2
    near zero means the model is not tracking *which* trips are expensive, which
    is a failure of the model rather than a hard day.
    """
    if daily_eval.empty or "r2" not in daily_eval:
        return []

    weak = daily_eval[daily_eval["r2"].notna() & (daily_eval["r2"] < _R2_SERIOUS)]
    return [
        Alert(
            "serious",
            f"Model explained almost nothing on {row['eval_date']}",
            f"R2 {float(row['r2']):.3f} — close to no better than quoting the average "
            f"price for every trip.",
        )
        for _, row in weak.iterrows()
    ]


def _layer_divergence(reconciliation: pd.DataFrame) -> list[Alert]:
    """Days where the hot and cold paths disagree about how many trips happened.

    A small gap is expected and explainable — the hot path drops events arriving
    past its watermark, and it does not deduplicate a replayed window. A large
    one means something is genuinely wrong in one of the two paths.
    """
    if reconciliation.empty:
        return []

    found = []
    for _, row in reconciliation.iterrows():
        cold = float(row["cold_trips"])
        hot = float(row["hot_trips"])
        if cold <= 0:
            continue
        gap = abs(cold - hot) / cold
        if gap < _DIVERGENCE_WARN:
            continue
        direction = "above" if hot > cold else "below"
        found.append(
            Alert(
                "warning",
                f"Hot and cold disagree by {gap:.0%} on {row['metric_date']}",
                f"Hot path counted {int(hot):,} trips, {direction} the cold path's "
                f"{int(cold):,}. The cold path is the number to trust — it recomputes "
                f"from the log and deduplicates.",
            )
        )
    return found


def _scoring_stalled(freshness: pd.DataFrame) -> list[Alert]:
    """Nothing has been scored recently.

    Wall clock, deliberately, unlike everything else here. This asks whether the
    *service* is alive, which is a question about now — not about when the trips
    it is scoring happened.
    """
    if freshness.empty or pd.isna(freshness.iloc[0]["last_written"]):
        return [Alert("warning", "No predictions yet", "The predictor has not scored anything.")]

    last = freshness.iloc[0]["last_written"]
    age = datetime.now(timezone.utc) - last
    if age < _STALE_AFTER:
        return []
    return [
        Alert(
            "warning",
            "Scoring is idle",
            f"Last quote written {int(age.total_seconds() // 60)} minutes ago. Expected "
            f"while no replay is running.",
        )
    ]
