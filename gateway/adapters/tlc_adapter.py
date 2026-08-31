"""Maps TLC-shaped input to the canonical TripEvent.

This is the only file that may know both the TLC schema and the canonical
schema. Swapping the data source means writing a new module like this one —
nothing else in the platform should need to change.

adapt_tlc() is a pure function: envelope metadata (event_id, ingested_at)
that isn't sourced from the TLC record itself is passed in by the caller
rather than generated here, so this function has no clock/randomness of
its own and is trivially testable.

derive_event_id() lives here for the same reason the rest of this module
does: it reads TLC field names, so it is source-specific and must not leak
upstream into the gateway or downstream past the canonical contract.
"""

import hashlib
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


# The natural key the id is derived from — the same six fields the cold path
# collapses duplicates on (batch_jobs/jobs/stream_to_lake.py NATURAL_KEY) and
# the same ones the batch adapter hashes. Keeping the three in step is what
# makes a trip mean the same thing on every path.
_NATURAL_KEY_FORMAT = "%Y-%m-%d %H:%M:%S"


def derive_event_id(trip: TLCTripInput) -> UUID:
    """A deterministic id for a trip, so a re-replay rewrites rather than repeats.

    The gateway used to mint a uuid4 per request on the reasoning that it sees
    each record once. That is false here: the simulator replays the same
    historical records on every demo run, so a random id made the same trip a
    new trip each time. `fare_predictions` is keyed on event_id precisely to
    stop that (see ml/sql/schema.sql), and a random id silently defeated it —
    error metrics ended up weighted by how often a trip happened to be replayed.

    Deliberately NOT byte-identical to the batch adapter's hash of the same key.
    Spark renders numbers by its own rules ("132" vs "132.0", and float
    formatting differs from Python's), so matching across two runtimes is
    fragile and buys nothing: batch owns 2023-2025, this path owns 2026, the
    ranges never overlap, and nothing joins on event_id across them.

    The cost is the one the cold path already accepts and documents: two
    genuinely distinct trips sharing all six fields collapse into one. Far
    cheaper than double-counting an entire replay.
    """
    natural_key = "|".join(
        (
            trip.tpep_pickup_datetime.strftime(_NATURAL_KEY_FORMAT),
            trip.tpep_dropoff_datetime.strftime(_NATURAL_KEY_FORMAT),
            str(trip.PULocationID),
            str(trip.DOLocationID),
            # str() on the Decimal, not the float, so 0.1 does not become
            # 0.1000000000000000055511151231257827 and change the digest.
            str(_to_decimal(trip.total_amount)),
            str(trip.VendorID),
        )
    )
    digest = hashlib.sha256(natural_key.encode("utf-8")).hexdigest()
    # UUID() takes the first 32 hex chars and imposes the dashed shape itself,
    # so the id is a valid UUID for the canonical contract without pretending
    # to carry a version's semantics.
    return UUID(digest[:32])


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
