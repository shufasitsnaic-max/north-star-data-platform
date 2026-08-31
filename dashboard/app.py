"""Streamlit dashboard: one view over four independent components.

Everything on this page was computed by something else. The hot path wrote the
rolling windows, the cold path wrote the daily history, the ML layer wrote the
quotes and their errors — and none of them know this page exists. The dashboard
opens a read-only connection and draws what it finds, which is why it can be
restarted mid-demo without consequence.

Chart conventions, applied throughout:

- **No dual-axis charts.** Trip counts and dollars are different scales, so they
  get separate charts. Two y-axes on one plot lets the author imply any
  correlation they like by choosing the scales.
- **Colour follows the entity.** Actual is always blue, predicted always orange.
- **A legend whenever there are two series**, so identity is never colour alone.
- **Selective labels.** Hover carries the numbers; the chart carries the shape.
"""

from __future__ import annotations

from datetime import date, datetime, time

import altair as alt
import pandas as pd
import streamlit as st

import alerts as alerting
import queries
import theme

st.set_page_config(page_title="North Star", page_icon="🚕", layout="wide")
alt.theme.register("northstar", enable=True)(theme.chart_theme)


def _line(
    frame: pd.DataFrame,
    x: str,
    y: str,
    x_title: str,
    y_title: str,
    color: str,
    y_format: str = ",.0f",
) -> alt.Chart:
    """A single-series line with a crosshair tooltip.

    No legend: with one series the title already names it, and a legend box for
    one entry is noise.
    """
    return (
        alt.Chart(frame)
        .mark_line(color=color, strokeWidth=2, point=False)
        .encode(
            x=alt.X(f"{x}:T", title=x_title),
            y=alt.Y(f"{y}:Q", title=y_title, scale=alt.Scale(zero=False)),
            tooltip=[
                alt.Tooltip(f"{x}:T", title=x_title),
                alt.Tooltip(f"{y}:Q", title=y_title, format=y_format),
            ],
        )
        .properties(height=240)
        .interactive()
    )


# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

st.title("🚕 North Star")
st.caption(
    "A lambda-architecture platform over NYC taxi trips — a real-time hot path, "
    "a recomputing cold path, and a fare model, all over the same event stream."
)

alert_slot = st.container()

# The scoring feed renders here, above the hot path: it is the panel most worth
# watching during a replay, so it gets the position that needs no scrolling.
# Reserved as a container because the fragment that fills it is defined below —
# Python needs the def before the call, the reader wants the panel before the
# charts, and a container is what reconciles the two.
feed_slot = st.container()

# ---------------------------------------------------------------------------
# Controls. The dashboard stays strictly read-only: it filters the *view* and
# hands you the command to drive the simulation, rather than launching
# containers itself. Starting a replay from a web page would mean mounting the
# Docker socket into it, trading a real architectural property — this thing
# cannot break anything — for a button.
# ---------------------------------------------------------------------------

with st.sidebar:
    st.header("View")

    bounds = queries.prediction_range()
    has_predictions = not bounds.empty and pd.notna(bounds.iloc[0]["first_day"])
    if has_predictions:
        first_day = bounds.iloc[0]["first_day"]
        last_day = bounds.iloc[0]["last_day"]
        picked = st.date_input(
            "Event-time range",
            value=(first_day, last_day),
            min_value=first_day,
            max_value=last_day,
            help="Filters the live feed and the quoted-vs-charged chart. Event time "
            "— when the trips happened, not when they were scored.",
        )
        # A partially-chosen range comes back as a 1-tuple mid-interaction.
        range_start = picked[0] if isinstance(picked, tuple) else picked
        range_end = picked[1] if isinstance(picked, tuple) and len(picked) > 1 else range_start
    else:
        range_start = range_end = date.today()

    # The range widget's maximum is whatever had been scored when the page
    # loaded. During a replay that goes stale within seconds, so a feed bounded
    # by it silently stops showing new trips — the panel looks frozen while the
    # pipeline is working. On by default because a live feed that needs a page
    # reload to show live data is not a live feed.
    follow_live = st.checkbox(
        "Follow live",
        value=True,
        help="Pin the quote feed to the newest trips as they are scored, "
        "ignoring the end date above. The history panels still respect it.",
    )

    st.divider()
    st.header("Replay")
    st.caption(
        "Predictions cover 2026 onward only — everything at or before the cutoff "
        "is what the model was trained on."
    )
    replay_from = st.date_input(
        "Simulate from",
        value=date(2026, 1, 1),
        min_value=date(2026, 1, 1),
        max_value=date(2026, 5, 31),
        key="replay_from",
    )
    replay_rows = st.select_slider(
        "Events", options=[20_000, 50_000, 150_000, 500_000, 1_000_000], value=150_000
    )
    st.caption("Run this in the repo root:")
    # Built by joining a list rather than with line continuations, so the
    # rendered command is one copy-pasteable line and the source has no
    # escaping to get wrong.
    st.code(
        " ".join([
            "docker compose run --rm",
            f"-e MAX_ROWS={replay_rows}",
            f"-e START_DATETIME={replay_from.isoformat()}T00:00:00",
            "simulator",
        ]),
        language="bash",
    )
    st.caption(
        f"~{replay_rows // 420 // 60 or 1} min at the measured ~420 events/sec. "
        "The predictor scores them as they arrive; the cold path folds them in on "
        "its next 3-minute tick."
    )

# ---------------------------------------------------------------------------
# Live hot path. Fragment-scoped so it refreshes on its own without redrawing
# the historical panels, which change every few minutes at most.
# ---------------------------------------------------------------------------


@st.fragment(run_every="10s")
def hot_panel() -> None:
    st.subheader("Live — hot path")
    st.caption(
        "Five-minute event-time windows, written by the Kafka consumer seconds after "
        "each trip replays. Refreshes every 10 seconds."
    )

    windows = queries.hot_windows()
    if windows.empty:
        st.info("No windows yet. Start a replay: `docker compose run --rm simulator`")
        return

    latest = windows.iloc[-1]
    left, middle, right, far = st.columns(4)
    # Hero numbers rather than charts: a single current value is a number, and
    # drawing it as a one-bar chart would say less in more space.
    left.metric("Trips in latest window", f"{int(latest['trip_count']):,}")
    middle.metric("Revenue", f"${float(latest['total_revenue']):,.0f}")
    right.metric("Average fare", f"${float(latest['avg_fare']):.2f}")
    far.metric(
        "Window",
        pd.to_datetime(latest["window_start"]).strftime("%b %d %H:%M"),
        help="Event time — when the trips happened, not wall clock.",
    )

    st.altair_chart(
        _line(
            windows, "window_start", "trip_count",
            "Event time", "Trips per 5-minute window", theme.BLUE,
        ),
        use_container_width=True,
    )

    zones = queries.hot_top_zones()
    if not zones.empty:
        st.caption("Busiest pickup zones in the latest window")
        st.altair_chart(
            alt.Chart(zones)
            .mark_bar(color=theme.BLUE, cornerRadiusEnd=4, size=18)
            .encode(
                # Zone is an identity, so it goes on the categorical axis and is
                # sorted by the value being compared, not by id.
                y=alt.Y("zone_id:N", title="Pickup zone", sort="-x"),
                x=alt.X("trip_count:Q", title="Trips"),
                tooltip=[
                    alt.Tooltip("zone_id:N", title="Zone"),
                    alt.Tooltip("trip_count:Q", title="Trips", format=","),
                    alt.Tooltip("avg_fare:Q", title="Avg fare", format="$.2f"),
                ],
            )
            .properties(height=260),
            use_container_width=True,
        )


hot_panel()
st.divider()

# ---------------------------------------------------------------------------
# Cold path
# ---------------------------------------------------------------------------

st.subheader("History — cold path")
st.caption(
    "Daily totals recomputed from the Parquet lake by Spark, orchestrated by Airflow. "
    "Complete rather than fast: every event, every run."
)

cold = queries.cold_daily()
if cold.empty:
    st.info("No daily metrics yet. Trigger the `cold_path_backfill` DAG.")
else:
    cold["metric_date"] = pd.to_datetime(cold["metric_date"])
    left, right = st.columns(2)
    # Two charts, not one with two y-axes. Counts and dollars share no scale,
    # and a dual axis would let the eye infer a relationship the data does not
    # support.
    with left:
        st.altair_chart(
            _line(cold, "metric_date", "trip_count", "Date", "Trips per day", theme.BLUE),
            use_container_width=True,
        )
    with right:
        st.altair_chart(
            _line(
                cold, "metric_date", "avg_fare", "Date", "Average fare ($)",
                theme.BLUE, y_format="$.2f",
            ),
            use_container_width=True,
        )

    recon = queries.reconciliation()
    if not recon.empty:
        st.caption(
            "**Cross-layer check.** Hot and cold are independent code paths over the same "
            "events. Where they disagree, the cold path is the number to trust — it "
            "recomputes from the log and deduplicates."
        )
        melted = recon.melt(
            id_vars="metric_date",
            value_vars=["cold_trips", "hot_trips"],
            var_name="layer",
            value_name="trips",
        ).replace({"cold_trips": "Cold path", "hot_trips": "Hot path"})
        st.altair_chart(
            alt.Chart(melted)
            .mark_bar(cornerRadiusEnd=4)
            .encode(
                x=alt.X("metric_date:T", title="Date"),
                y=alt.Y("trips:Q", title="Trips"),
                # Two series, so a legend is mandatory — identity is never
                # carried by colour alone.
                color=alt.Color(
                    "layer:N",
                    title=None,
                    scale=alt.Scale(
                        domain=["Cold path", "Hot path"],
                        range=[theme.BLUE, theme.ORANGE],
                    ),
                ),
                xOffset="layer:N",
                tooltip=[
                    alt.Tooltip("metric_date:T", title="Date"),
                    alt.Tooltip("layer:N", title="Layer"),
                    alt.Tooltip("trips:Q", title="Trips", format=","),
                ],
            )
            .properties(height=240),
            use_container_width=True,
        )

st.divider()

# ---------------------------------------------------------------------------
# ML layer
# ---------------------------------------------------------------------------

st.subheader("Fare estimates — predicted vs actual")
st.caption(
    "The model quotes a price from pickup information alone: the two zones and the clock. "
    "It never sees the distance driven or the meter. Errors are scored per event-time day "
    "by an Airflow DAG."
)

evaluation = queries.daily_eval()
if evaluation.empty:
    st.info("No evaluations yet. Train a model, start the predictor, run `ml_daily_eval`.")
else:
    evaluation["eval_date"] = pd.to_datetime(evaluation["eval_date"])
    numeric = ["mae", "rmse", "mape", "mean_actual", "mean_predicted"]
    evaluation[numeric] = evaluation[numeric].astype(float)

    latest = evaluation.iloc[-1]
    left, middle, right, far = st.columns(4)
    left.metric("Mean error, latest day", f"${latest['mae']:.2f}")
    middle.metric("Within", f"{latest['mape']:.1f}%", help="Mean absolute percentage error")
    right.metric(
        "R²",
        f"{latest['r2']:.3f}" if pd.notna(latest["r2"]) else "—",
        help="Share of price variance the model explains. Near zero means no better "
        "than quoting the average.",
    )
    far.metric("Quotes scored", f"{int(evaluation['predictions'].sum()):,}")

    left, right = st.columns(2)
    with left:
        melted = evaluation.melt(
            id_vars="eval_date",
            value_vars=["mean_actual", "mean_predicted"],
            var_name="series",
            value_name="amount",
        ).replace({"mean_actual": "Actual", "mean_predicted": "Predicted"})
        st.altair_chart(
            alt.Chart(melted)
            .mark_line(strokeWidth=2, point=alt.OverlayMarkDef(size=40, filled=True))
            .encode(
                x=alt.X("eval_date:T", title="Event-time day"),
                y=alt.Y("amount:Q", title="Average price ($)", scale=alt.Scale(zero=False)),
                color=alt.Color(
                    "series:N",
                    title=None,
                    # Fixed domain so actual stays blue even if a series is
                    # filtered away — colour follows the entity, not the rank.
                    scale=alt.Scale(
                        domain=["Actual", "Predicted"],
                        range=[theme.COLOR_ACTUAL, theme.COLOR_PREDICTED],
                    ),
                ),
                tooltip=[
                    alt.Tooltip("eval_date:T", title="Day"),
                    alt.Tooltip("series:N", title=None),
                    alt.Tooltip("amount:Q", title="Average", format="$.2f"),
                ],
            )
            .properties(height=240, title="Quoted vs charged")
            .interactive(),
            use_container_width=True,
        )
    with right:
        st.altair_chart(
            _line(
                evaluation, "eval_date", "mae", "Event-time day",
                "Mean absolute error ($)", theme.ORANGE, y_format="$.2f",
            ).properties(title="Daily error"),
            use_container_width=True,
        )

    with st.expander("Daily evaluation table"):
        # A table view is not a fallback; it is how a reader checks a chart, and
        # how anyone who cannot read the colours gets the same information.
        st.dataframe(evaluation, use_container_width=True)

st.divider()

# ---------------------------------------------------------------------------
# Live scoring feed. Its own fragment so it keeps up with a running replay
# without redrawing the three-year history above it.
# ---------------------------------------------------------------------------

# Error bands, as percentages of the actual price. Named so the caption, the
# colouring and the legend cannot drift apart.
_BANDS = [(10.0, "good", "within 10%"), (25.0, "warning", "10-25% off"), (None, "critical", "over 25% off")]


def _band(error_pct: float) -> str:
    """Which band an error falls in. Absolute: over- and under-quoting are both wrong."""
    if pd.isna(error_pct):
        return "good"
    magnitude = abs(float(error_pct))
    for threshold, status, _ in _BANDS:
        if threshold is None or magnitude < threshold:
            return status
    return "critical"


def _ago(seconds: float) -> str:
    """Humanised age. Seconds while it matters, then coarser — nobody reads 4,812s."""
    if seconds < 90:
        return f"{seconds:.0f}s"
    if seconds < 5400:
        return f"{seconds / 60:.0f} min"
    return f"{seconds / 3600:.1f} h"


@st.fragment(run_every="10s")
def scoring_feed(start, end) -> None:
    st.subheader("Live — fare quotes as they are scored")
    # Say which mode the panel is in. "Selected range" would be a lie while
    # following, and the difference is exactly what a reader needs to know when
    # the table is or isn't moving.
    scope = "as they are scored" if end is None else "in the selected range"
    st.caption(
        f"Every trip the model has priced {scope}, newest first. Quoted "
        "and charged sit side by side because they arrive together: a replayed event "
        "describes a finished trip, so it carries its own fare. The model never sees "
        "that fare, the distance, or the dropoff time."
    )

    # Whether anything is actually arriving. A panel that redraws identical rows
    # every ten seconds reads as broken rather than idle, and the honest fix is
    # not to redraw less often — it is to say which of the two is happening.
    fresh = queries.prediction_freshness()
    last_written = fresh.iloc[0]["last_written"] if not fresh.empty else None
    if pd.notna(last_written):
        written = pd.Timestamp(last_written)
        written = written.tz_localize("UTC") if written.tzinfo is None else written.tz_convert("UTC")
        age = (pd.Timestamp.now(tz="UTC") - written).total_seconds()
        if age < 30:
            st.success(
                f"Scoring now — newest quote {_ago(age)} old, refreshing every 10s.",
                icon="🟢",
            )
        else:
            st.info(
                f"Idle — newest quote is {_ago(age)} old. The panel still refreshes every "
                "10s, but nothing new appears until a replay is running.",
                icon="⏸️",
            )

    feed = queries.live_predictions(start, end)
    if feed.empty:
        st.info(
            "No quotes yet. Predictions cover 2026 onward — widen the range, or "
            "start a replay with the command in the sidebar. The feed only moves "
            "while a replay is running."
        )
        return

    money = ["predicted_amount", "actual_amount", "error", "error_pct"]
    feed[money] = feed[money].astype(float)
    feed["duration_min"] = (
        pd.to_datetime(feed["dropoff_datetime"]) - pd.to_datetime(feed["pickup_datetime"])
    ).dt.total_seconds() / 60

    within_10 = float((feed["error_pct"].abs() < 10).mean() * 100)
    left, middle, right = st.columns(3)
    left.metric("Quotes shown", f"{len(feed):,}")
    middle.metric("Within 10%", f"{within_10:.0f}%")
    right.metric("Median error", f"${feed['error'].abs().median():.2f}")

    st.altair_chart(
        alt.Chart(queries.prediction_windows(start, end).astype(
            {"predicted": float, "actual": float}
        ).melt(
            id_vars="window_start",
            value_vars=["actual", "predicted"],
            var_name="series",
            value_name="amount",
        ).replace({"actual": "Charged", "predicted": "Quoted"}))
        .mark_line(strokeWidth=2)
        .encode(
            x=alt.X("window_start:T", title="Event time"),
            y=alt.Y("amount:Q", title="Average price ($)", scale=alt.Scale(zero=False)),
            # Fixed domain: charged stays blue even if a series is filtered out.
            color=alt.Color(
                "series:N",
                title=None,
                scale=alt.Scale(
                    domain=["Charged", "Quoted"],
                    range=[theme.COLOR_ACTUAL, theme.COLOR_PREDICTED],
                ),
            ),
            tooltip=[
                alt.Tooltip("window_start:T", title="Window"),
                alt.Tooltip("series:N", title=None),
                alt.Tooltip("amount:Q", title="Average", format="$.2f"),
            ],
        )
        .properties(height=220, title="Quoted vs charged, per 5-minute window")
        .interactive(),
        use_container_width=True,
    )

    st.caption(
        f"Error band: {theme.STATUS_ICON['good']} within 10%  ·  "
        f"{theme.STATUS_ICON['warning']} 10-25% off  ·  "
        f"{theme.STATUS_ICON['critical']} over 25% off. "
        "A positive error means the model quoted more than the trip cost."
    )

    table = feed[[
        "pickup_datetime", "dropoff_datetime", "duration_min",
        "pickup_zone_id", "dropoff_zone_id",
        "predicted_amount", "actual_amount", "error", "error_pct",
    ]]

    # The colour reinforces the number; it never replaces it. A reader who
    # cannot distinguish the hues still has the percentage in the same cell,
    # which is the whole reason the band is not encoded as a bare dot.
    styled = table.style.map(
        lambda v: f"color: {theme.STATUS[_band(v)]}; font-weight: 600",
        subset=["error_pct"],
    ).format({
        "predicted_amount": "${:.2f}",
        "actual_amount": "${:.2f}",
        "error": "{:+.2f}",
        "error_pct": "{:+.1f}%",
        "duration_min": "{:.0f} min",
    })

    st.dataframe(
        styled,
        use_container_width=True,
        height=420,
        column_config={
            "pickup_datetime": st.column_config.DatetimeColumn("Pickup", format="MMM DD HH:mm"),
            "dropoff_datetime": st.column_config.DatetimeColumn("Dropoff", format="MMM DD HH:mm"),
            "duration_min": st.column_config.TextColumn("Duration"),
            "pickup_zone_id": st.column_config.NumberColumn("From zone"),
            "dropoff_zone_id": st.column_config.NumberColumn("To zone"),
            "predicted_amount": st.column_config.TextColumn("Quoted"),
            "actual_amount": st.column_config.TextColumn("Charged"),
            "error": st.column_config.TextColumn("Error"),
            "error_pct": st.column_config.TextColumn("Error %"),
        },
    )


with feed_slot:
    scoring_feed(
        datetime.combine(range_start, time.min),
        None if follow_live else datetime.combine(range_end, time.max),
    )
    st.divider()

# ---------------------------------------------------------------------------
# Alerts — rendered into the slot reserved at the top
# ---------------------------------------------------------------------------

with alert_slot:
    found = alerting.evaluate(
        queries.daily_eval(), queries.reconciliation(), queries.prediction_freshness()
    )
    for alert in found:
        # Icon plus title plus explanation. A colour alone would be unreadable
        # to a colourblind viewer and uninformative to everyone else.
        st.markdown(
            f"<div style='border-left:4px solid {theme.STATUS[alert.status]};"
            f"padding:.5rem .75rem;margin:.25rem 0;background:#f6f6f4;border-radius:4px'>"
            f"<strong>{theme.STATUS_ICON[alert.status]} {alert.title}</strong><br>"
            f"<span style='color:{theme.INK_SECONDARY};font-size:.9em'>{alert.detail}</span>"
            f"</div>",
            unsafe_allow_html=True,
        )
