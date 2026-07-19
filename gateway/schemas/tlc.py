"""TLC-specific input model.

This mirrors the NYC TLC yellow-cab trip record field-for-field: names,
raw units (miles, not km), and raw codes (numeric payment_type, RatecodeID)
are kept as the source defines them. Nothing here is canonical — this model
exists purely so FastAPI can reject a malformed TLC payload (wrong types,
missing required fields) with a 422 before the adapter ever sees it.

Field-level type validation lives here. Cross-field semantics that are
source-independent (e.g. dropoff after pickup) live on the canonical
TripEvent model instead, since they'd apply to any source.
"""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class TLCTripInput(BaseModel):
    """Raw shape of one TLC yellow-cab trip record."""

    VendorID: int
    tpep_pickup_datetime: datetime
    tpep_dropoff_datetime: datetime

    # TLC ships these as floats in its parquet files purely so nulls are
    # representable (Parquet has no nullable int32) — they're conceptually
    # integer codes, so we type them as Optional[int] here rather than float.
    passenger_count: Optional[int] = None
    RatecodeID: Optional[int] = None
    payment_type: Optional[int] = None

    trip_distance: float = Field(ge=0, description="Miles, as TLC reports it.")
    store_and_fwd_flag: Optional[str] = None
    PULocationID: int
    DOLocationID: int

    fare_amount: float
    extra: float = 0
    mta_tax: float = 0
    tip_amount: float = 0
    tolls_amount: float = 0
    improvement_surcharge: float = 0
    total_amount: float
    # Both added to the TLC schema after it was first published — absent on
    # older records, so they must stay nullable.
    congestion_surcharge: Optional[float] = None
    airport_fee: Optional[float] = None
