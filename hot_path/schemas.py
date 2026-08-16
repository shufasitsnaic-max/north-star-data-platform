"""Consumer-side view of the canonical event — a deliberately partial model.

This is a **tolerant reader**: it declares only the fields the hot path
aggregates and ignores everything else on the wire. That is the point. The
gateway can add fields to its canonical `TripEvent` without breaking this
consumer, and no code is shared between the two components (per the
per-component dependency rule in CLAUDE.md).

The tradeoff, stated plainly: the two definitions can drift. The protection is
that the fields below are the *contract-critical* ones — renaming or removing
any of them is a breaking schema change that must bump `schema_version`, and
this consumer fails loudly on the next event rather than silently reading nulls.

Nothing here references a specific data source: these are canonical field names.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


class TripLocationView(BaseModel):
    """Only the zone matters for windowing; lat/lon are ignored if present."""

    model_config = ConfigDict(extra="ignore")

    zone_id: Optional[int] = None


class TripEventView(BaseModel):
    """The subset of a canonical TripEvent the hot path actually aggregates."""

    model_config = ConfigDict(extra="ignore")

    # Canonical event time. Windows key off this, never off ingested_at or wall
    # clock — replayed history must bucket by when the trip happened.
    pickup_datetime: datetime
    pickup_location: TripLocationView

    # Money stays Decimal end to end: parsed from the JSON string the gateway
    # emits, summed as Decimal, written to numeric. No float rounding anywhere.
    fare_amount: Decimal = Field(ge=0)
    tip_amount: Decimal = Field(default=Decimal(0), ge=0)
    total_amount: Decimal = Field(ge=0)

    # Nullable in the canonical contract, so nullable here. Averaged over the
    # events that actually carry it rather than treated as zero.
    trip_distance_km: Optional[float] = Field(default=None, ge=0)
