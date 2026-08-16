"""Canonical internal event contract.

This model is source-independent: it must describe a taxi trip regardless of
which adapter produced it. Nothing in this file may reference TLC (or any
other source) by name. Source-specific mapping lives in gateway/adapters/.
"""

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Optional
from uuid import UUID

from pydantic import BaseModel, Field, model_validator

SCHEMA_VERSION = "1.0.0"


class PaymentType(str, Enum):
    CASH = "CASH"
    CARD = "CARD"
    OTHER = "OTHER"
    UNKNOWN = "UNKNOWN"


class TripLocation(BaseModel):
    """A pickup or dropoff point.

    Different sources ground truth location differently (zone lookup vs. raw
    GPS) — both fields are optional so no adapter has to fabricate the one
    it doesn't have, but at least one must be present.
    """

    zone_id: Optional[int] = Field(default=None, description="Source-defined taxi zone identifier.")
    lat: Optional[float] = Field(default=None, ge=-90, le=90)
    lon: Optional[float] = Field(default=None, ge=-180, le=180)

    @model_validator(mode="after")
    def _require_zone_or_coordinates(self) -> "TripLocation":
        has_zone = self.zone_id is not None
        has_coordinates = self.lat is not None and self.lon is not None
        if not has_zone and not has_coordinates:
            raise ValueError("location requires zone_id or both lat and lon")
        return self


class TripEvent(BaseModel):
    """The canonical taxi trip event, as published downstream of the gateway."""

    # --- envelope: assigned by the gateway, never sourced from input ---
    event_id: UUID
    source: str = Field(description="Adapter that produced this event, e.g. 'tlc_yellow'.")
    ingested_at: datetime = Field(description="When the gateway received the record (UTC).")
    schema_version: str = SCHEMA_VERSION

    # --- trip identity ---
    # pickup_datetime is the canonical event time for downstream windowing
    # (the hot-path consumer keys its rolling windows off this, not
    # ingested_at) — it must come from the source record and is never
    # defaulted to "now".
    pickup_datetime: datetime
    dropoff_datetime: datetime
    pickup_location: TripLocation
    dropoff_location: TripLocation
    passenger_count: Optional[int] = Field(default=None, ge=0)
    trip_distance_km: Optional[float] = Field(default=None, ge=0)
    provider_id: Optional[str] = None
    payment_type: PaymentType = PaymentType.UNKNOWN

    # --- fare breakdown ---
    # Collapsed to the concepts that generalize across sources. Anything
    # more granular (itemized regulatory fees, rate codes, vendor flags)
    # is preserved in source_extras rather than promoted here or dropped.
    fare_amount: Decimal = Field(ge=0)
    surcharges_amount: Decimal = Field(default=Decimal(0), ge=0)
    tip_amount: Decimal = Field(default=Decimal(0), ge=0)
    tolls_amount: Decimal = Field(default=Decimal(0), ge=0)
    total_amount: Decimal = Field(ge=0)

    # --- audit passthrough ---
    source_extras: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _dropoff_after_pickup(self) -> "TripEvent":
        if self.dropoff_datetime <= self.pickup_datetime:
            raise ValueError("dropoff_datetime must be after pickup_datetime")
        return self
