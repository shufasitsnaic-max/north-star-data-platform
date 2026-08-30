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

    with st.expander("Per-trip quotes — the rows behind the averages"):
        st.dataframe(queries.recent_predictions(), use_container_width=True, height=320)

    with st.expander("Daily evaluation table"):
        # A table view is not a fallback; it is how a reader checks a chart, and
        # how anyone who cannot read the colours gets the same information.
        st.dataframe(evaluation, use_container_width=True)

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
