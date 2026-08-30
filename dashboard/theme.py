"""Chart palette and shared Altair styling.

Colour is assigned by the *job* it does, not by taste, and the categorical pair
below was validated rather than eyeballed: blue and orange clear the lightness
band, the chroma floor, the colour-vision-deficiency separation floor, the
normal-vision floor and 3:1 contrast against the chart surface. Worst all-pairs
CVD separation is dE 24.7, well clear of the 8 target.

Rules this file exists to enforce:

- **Colour follows the entity, never its rank.** `actual` is always blue and
  `predicted` is always orange, in every chart, so a filter that drops a series
  cannot repaint the survivor.
- **Status colours are reserved.** The four alert states never double as a
  series colour, and they always ship with an icon and a label — colour never
  carries the meaning alone, which is what makes the alerts readable to someone
  who cannot distinguish red from green.
- **Text wears text tokens.** Values and labels stay in ink colours; a coloured
  mark beside them carries the identity.
"""

from __future__ import annotations

# Categorical slots, in fixed order. A third series would take AQUA; a ninth
# would not exist — it would fold into "other" or become a second chart.
BLUE = "#2a78d6"
ORANGE = "#eb6834"
AQUA = "#1baf7a"

# Series roles. Named rather than positional so a chart cannot accidentally
# swap them.
COLOR_ACTUAL = BLUE
COLOR_PREDICTED = ORANGE

# Status palette — fixed, never themed, never reused as a series colour.
STATUS = {
    "good": "#0ca30c",
    "warning": "#fab219",
    "serious": "#ec835a",
    "critical": "#d03b3b",
}

# Icons pair with every status so the state is legible without colour.
STATUS_ICON = {
    "good": "✅",
    "warning": "⚠️",
    "serious": "🔶",
    "critical": "🔴",
}

# Chart surface and ink. Matches .streamlit/config.toml, because the contrast
# figures above are only meaningful against the surface the chart renders on.
SURFACE = "#fcfcfb"
INK_PRIMARY = "#0b0b0b"
INK_SECONDARY = "#52514e"
GRID = "#e5e4e0"


def chart_theme() -> dict:
    """Altair theme: recessive grid and axes, ink-coloured text.

    The data should be the most prominent thing on the chart. Grid lines are a
    reference, not content, so they sit well back.
    """
    return {
        "config": {
            "background": SURFACE,
            "view": {"stroke": "transparent"},
            "axis": {
                "labelColor": INK_SECONDARY,
                "titleColor": INK_SECONDARY,
                "labelFontSize": 11,
                "titleFontSize": 11,
                "titleFontWeight": "normal",
                "gridColor": GRID,
                "domainColor": GRID,
                "tickColor": GRID,
            },
            "legend": {
                "labelColor": INK_PRIMARY,
                "titleColor": INK_SECONDARY,
                "labelFontSize": 12,
                "titleFontSize": 11,
                "symbolType": "square",
            },
            "title": {"color": INK_PRIMARY, "fontSize": 13, "fontWeight": 600},
        }
    }
