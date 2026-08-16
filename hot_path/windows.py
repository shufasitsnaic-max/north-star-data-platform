"""Event-time tumbling windows, held in memory until they close.

Windowing model
---------------
Fixed-size **tumbling** windows over `pickup_datetime` (event time), not wall
clock. A replayed month must bucket by when trips happened, or the whole replay
collapses into a single wall-clock bucket. Rejected sliding windows: they
multiply row count by the overlap factor for a smoother line the dashboard can
compute itself from tumbling rows.

Each event updates two aggregates: its **pickup zone** and a **citywide rollup**
(`zone_id=None`). Storing the rollup rather than summing zones at query time
keeps the dashboard's headline metric a single indexed row read, and stays
correct for events whose zone is null.

Watermark and finalization
--------------------------
The watermark is the highest `pickup_datetime` seen. A window is *closed* once
`window_end + grace <= watermark`. The grace period absorbs mild out-of-order
arrival; the simulator replays in ascending pickup order, so in practice this is
belt-and-braces rather than load-bearing.

Offset safety (the non-obvious part)
------------------------------------
Aggregates live in memory, so a restart loses every partially-filled window. If
offsets were committed as events were consumed, a restart would resume mid-window
and recompute that window from only its *remaining* events — then overwrite the
correct stored row with an undercount.

So each window records the lowest Kafka offset that fed it, and the consumer only
ever commits `min(offset)` across windows still open. On restart, consumption
resumes at the first event of the oldest unfinished window and rebuilds it in
full. That is what makes the absolute (`SET`, not `+=`) upsert in db.py safe:
rewriting a window with a freshly recomputed total is always correct, whereas an
additive upsert would double-count on redelivery.
"""

from __future__ import annotations

import logging
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Iterable, Optional

from schemas import TripEventView

logging.basicConfig(stream=sys.stderr, level=logging.INFO)
logger = logging.getLogger("hot_path.windows")

_CENTS = Decimal("0.01")


def window_start_for(moment: datetime, size_minutes: int) -> datetime:
    """Floor an event time to the start of its tumbling window.

    Floors on the wall-clock grid (00:00, 00:05, ...) rather than relative to
    the first event seen, so window boundaries are reproducible across restarts
    and identical no matter where a replay begins.
    """
    floored_minute = (moment.minute // size_minutes) * size_minutes
    return moment.replace(minute=floored_minute, second=0, microsecond=0)


@dataclass
class Aggregate:
    """Running totals for one (window, zone) cell."""

    trip_count: int = 0
    fare_sum: Decimal = Decimal(0)
    tip_sum: Decimal = Decimal(0)
    revenue_sum: Decimal = Decimal(0)
    # Distance is nullable in the canonical contract, so it carries its own
    # counter — averaging over trip_count would silently treat a missing
    # distance as zero and drag the average down.
    distance_sum: float = 0.0
    distance_count: int = 0

    def add(self, event: TripEventView) -> None:
        self.trip_count += 1
        self.fare_sum += event.fare_amount
        self.tip_sum += event.tip_amount
        self.revenue_sum += event.total_amount
        if event.trip_distance_km is not None:
            self.distance_sum += event.trip_distance_km
            self.distance_count += 1

    def avg_fare(self) -> Decimal:
        return (self.fare_sum / self.trip_count).quantize(_CENTS, ROUND_HALF_UP)

    def avg_tip(self) -> Decimal:
        return (self.tip_sum / self.trip_count).quantize(_CENTS, ROUND_HALF_UP)

    def avg_distance_km(self) -> Optional[float]:
        if self.distance_count == 0:
            return None
        return self.distance_sum / self.distance_count


@dataclass
class Window:
    """All zone aggregates for one time bucket, plus its offset low-water mark."""

    start: datetime
    end: datetime
    # None key = the citywide rollup.
    zones: dict[Optional[int], Aggregate] = field(default_factory=dict)
    # Lowest offset per partition that contributed to this window.
    min_offsets: dict[int, int] = field(default_factory=dict)

    def add(self, event: TripEventView, partition: int, offset: int) -> None:
        zone = event.pickup_location.zone_id
        for key in (zone, None):  # per-zone and citywide, from the same event
            self.zones.setdefault(key, Aggregate()).add(event)

        current = self.min_offsets.get(partition)
        if current is None or offset < current:
            self.min_offsets[partition] = offset


@dataclass
class MetricRow:
    """One row as it will be written to PostgreSQL."""

    window_start: datetime
    window_end: datetime
    zone_id: Optional[int]
    trip_count: int
    total_revenue: Decimal
    avg_fare: Decimal
    avg_tip: Decimal
    avg_distance_km: Optional[float]
    is_final: bool


class WindowStore:
    """In-memory set of open windows, plus the watermark that closes them."""

    def __init__(self, size_minutes: int, grace_minutes: int) -> None:
        self._size = timedelta(minutes=size_minutes)
        self._size_minutes = size_minutes
        self._grace = timedelta(minutes=grace_minutes)
        self._windows: dict[datetime, Window] = {}
        self._watermark: Optional[datetime] = None
        # Highest offset consumed per partition — the commit point when nothing
        # is open. Counted, not silently dropped, per the no-silent-caps rule.
        self._last_offsets: dict[int, int] = {}
        self.late_events = 0

    def note_offset(self, partition: int, offset: int) -> None:
        """Record an offset that fed no window (unparseable or late message).

        Without this the commit point would stall behind a poison message
        forever, since only aggregated events advance the low-water mark.
        """
        self._last_offsets[partition] = offset

    def add(self, event: TripEventView, partition: int, offset: int) -> None:
        """Route one event into its window, advancing the watermark."""
        self._last_offsets[partition] = offset

        start = window_start_for(event.pickup_datetime, self._size_minutes)
        if start not in self._windows:
            # A window already finalized and evicted cannot be reopened: its
            # aggregates are gone, so a late event can only be reported.
            if self._watermark is not None and start + self._size + self._grace <= self._watermark:
                self.late_events += 1
                logger.warning(
                    "late event for closed window %s (pickup=%s); counted, not aggregated "
                    "— total late: %d",
                    start.isoformat(), event.pickup_datetime.isoformat(), self.late_events,
                )
                return
            self._windows[start] = Window(start=start, end=start + self._size)

        self._windows[start].add(event, partition, offset)

        if self._watermark is None or event.pickup_datetime > self._watermark:
            self._watermark = event.pickup_datetime

    def open_rows(self) -> list[MetricRow]:
        """Snapshot every open window as a non-final row (the liveness flush)."""
        return [row for window in self._windows.values() for row in _rows_for(window, False)]

    def take_closed_rows(self) -> list[MetricRow]:
        """Emit and evict every window the watermark has moved past."""
        if self._watermark is None:
            return []

        closed = [
            start for start, window in self._windows.items()
            if window.end + self._grace <= self._watermark
        ]
        rows: list[MetricRow] = []
        for start in sorted(closed):
            rows.extend(_rows_for(self._windows.pop(start), True))
        return rows

    def safe_commit_offsets(self) -> dict[int, int]:
        """Offsets it is safe to commit: the start of the oldest open window.

        Returns the *next* offset to read per partition, which is what Kafka's
        commit API expects. With no windows open, everything consumed so far is
        durable, so the commit point is one past the last message.
        """
        offsets: dict[int, int] = {}
        for partition, last in self._last_offsets.items():
            open_mins = [
                w.min_offsets[partition]
                for w in self._windows.values()
                if partition in w.min_offsets
            ]
            offsets[partition] = min(open_mins) if open_mins else last + 1
        return offsets

    @property
    def watermark(self) -> Optional[datetime]:
        return self._watermark

    @property
    def open_window_count(self) -> int:
        return len(self._windows)


def _rows_for(window: Window, is_final: bool) -> Iterable[MetricRow]:
    for zone_id, agg in window.zones.items():
        yield MetricRow(
            window_start=window.start,
            window_end=window.end,
            zone_id=zone_id,
            trip_count=agg.trip_count,
            total_revenue=agg.revenue_sum.quantize(_CENTS, ROUND_HALF_UP),
            avg_fare=agg.avg_fare(),
            avg_tip=agg.avg_tip(),
            avg_distance_km=agg.avg_distance_km(),
            is_final=is_final,
        )
