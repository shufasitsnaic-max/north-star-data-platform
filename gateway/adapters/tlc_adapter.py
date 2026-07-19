"""Maps TLC-shaped input to the canonical TripEvent.

This is the only file that may know both the TLC schema and the canonical
schema. Swapping the data source means writing a new module like this one —
nothing else in the platform should need to change.

adapt_tlc() is a pure function: envelope metadata (event_id, ingested_at)
that isn't sourced from the TLC record itself is passed in by the caller
rather than generated here, so this function has no clock/randomness of
its own and is trivially testable.
"""

from datetime import datetime
from decimal import Decimal
from typing import Optional
from uuid import UUID

from schemas.canonical import PaymentType, TripEvent, TripLocation
from schemas.tlc import TLCTripInput

MILES_TO_KM = Decimal("1.609344")

# TLC payment_type codes: 0=Flex Fare, 1=Credit card, 2=Cash, 3=No charge,
# 4=Dispute, 5=Unknown, 6=Voided trip. Only codes with a clean canonical
# equivalent get their own mapping; everything else (including TLC's own
# "Unknown" code) is separated below.
_KNOWN_PAYMENT_CODES: dict[int, PaymentType] = {
    1: PaymentType.CARD,
    2: PaymentType.CASH,
    5: PaymentType.UNKNOWN,
}


def _map_payment_type(code: Optional[int]) -> PaymentType:
    if code is None:
        return PaymentType.UNKNOWN
    return _KNOWN_PAYMENT_CODES.get(code, PaymentType.OTHER)


def _to_decimal(value: float) -> Decimal:
    # str() first avoids inheriting float's binary-imprecision artifacts
    # (Decimal(0.1) != Decimal("0.1")).
    return Decimal(str(value))


def adapt_tlc(trip: TLCTripInput, *, event_id: UUID, ingested_at: datetime) -> TripEvent:
    surcharges_amount = (
        _to_decimal(trip.extra)
        + _to_decimal(trip.mta_tax)
        + _to_decimal(trip.improvement_surcharge)
        + _to_decimal(trip.congestion_surcharge or 0)
        + _to_decimal(trip.airport_fee or 0)
    )

    return TripEvent(
        event_id=event_id,
        source="tlc_yellow",
        ingested_at=ingested_at,
        pickup_datetime=trip.tpep_pickup_datetime,
        dropoff_datetime=trip.tpep_dropoff_datetime,
        pickup_location=TripLocation(zone_id=trip.PULocationID),
        dropoff_location=TripLocation(zone_id=trip.DOLocationID),
        passenger_count=trip.passenger_count,
        trip_distance_km=float(_to_decimal(trip.trip_distance) * MILES_TO_KM),
        provider_id=str(trip.VendorID),
        payment_type=_map_payment_type(trip.payment_type),
        fare_amount=_to_decimal(trip.fare_amount),
        surcharges_amount=surcharges_amount,
        tip_amount=_to_decimal(trip.tip_amount),
        tolls_amount=_to_decimal(trip.tolls_amount),
        total_amount=_to_decimal(trip.total_amount),
        source_extras={
            "ratecode_id": trip.RatecodeID,
            "store_and_fwd_flag": trip.store_and_fwd_flag,
            "extra": trip.extra,
            "mta_tax": trip.mta_tax,
            "improvement_surcharge": trip.improvement_surcharge,
            "congestion_surcharge": trip.congestion_surcharge,
            "airport_fee": trip.airport_fee,
        },
    )
