"""Maps TLC-shaped raw records to the canonical schema, vectorized for Spark.

This is the batch sibling of gateway/adapters/tlc_adapter.py, and — like it —
the only file in this component that may know both the TLC schema and the
canonical one. Swapping the data source means writing a module like this one.

Why a second implementation exists at all
-----------------------------------------
The gateway's adapt_tlc() is row-at-a-time Pydantic. Using it here would mean a
per-row Python UDF over ~110M rows (slow) and pulling gateway/ source into this
component's image (breaking the per-component dependency rule). So the same
mapping is re-expressed as Spark SQL column expressions.

That is a real cost — two implementations of one mapping, free to drift — and
the mitigation is tests/test_adapter_conformance.py, which asserts this module
and the running gateway produce identical canonical output for the same raw
rows. If they diverge, that test fails.

A useful side effect of the "column expressions only, no Python UDFs" rule: the
Spark executors need no Python dependencies beyond stock pyspark, so the worker
image stays trivial.

Arithmetic fidelity
-------------------
The gateway computes money as Decimal(str(float)) and distance as an exact
Decimal product cast to float. Doing the same arithmetic in double would drift
in the last bits, so every money and distance expression here casts to a wide
Decimal *first*, computes exactly, and narrows only at the end. That is what
lets the conformance test compare values rather than tolerances.
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime

from pyspark.sql import Column, DataFrame, functions as F
from pyspark.sql.types import (
    DecimalType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from schemas.canonical_spark import MONEY, SCHEMA_VERSION

logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger("batch_jobs.tlc_adapter")

# Adapter identity, written to every event's `source` field. Matches the value
# gateway/adapters/tlc_adapter.py emits — a mismatch here would split the lake
# into two apparent sources.
SOURCE = "tlc_yellow"

# Exact, per NIST. The gateway multiplies as Decimal; so do we.
MILES_TO_KM = "1.609344"

# Wide intermediate type for exact arithmetic before narrowing to storage types.
# 8 decimal places is far past TLC's 2, so nothing is lost on the way through.
EXACT = DecimalType(20, 8)

# TLC payment_type codes: 0=Flex Fare, 1=Credit card, 2=Cash, 3=No charge,
# 4=Dispute, 5=Unknown, 6=Voided trip. Only codes with a clean canonical
# equivalent are mapped; everything else falls through to OTHER, and a missing
# code becomes UNKNOWN. Mirrors _KNOWN_PAYMENT_CODES in the gateway's adapter.
KNOWN_PAYMENT_CODES = {1: "CARD", 2: "CASH", 5: "UNKNOWN"}

# The source-specific passthrough. The canonical contract types source_extras as
# an opaque dict, so its *shape* is the source's business and is injected into
# schemas.canonical_spark.lake_schema() from here — that module never names TLC.
# Kept rather than dropped for audit, and because ratecode_id flags airport and
# flat-fare trips, which is a live P5 feature candidate.
TLC_SOURCE_EXTRAS = StructType(
    [
        StructField("ratecode_id", IntegerType(), nullable=True),
        StructField("store_and_fwd_flag", StringType(), nullable=True),
        StructField("extra", DoubleType(), nullable=True),
        StructField("mta_tax", DoubleType(), nullable=True),
        StructField("improvement_surcharge", DoubleType(), nullable=True),
        StructField("congestion_surcharge", DoubleType(), nullable=True),
        StructField("airport_fee", DoubleType(), nullable=True),
    ]
)

# Raw TLC columns we consume, and the type each is coerced to on the way in.
# TLC ships the integer codes as floats purely so nulls are representable in
# Parquet (no nullable int32), which is why they need an explicit narrowing —
# without it, provider_id would serialize as "2.0" rather than "2".
_INT_COLUMNS = [
    "VendorID",
    "passenger_count",
    "RatecodeID",
    "payment_type",
    "PULocationID",
    "DOLocationID",
]
_DOUBLE_COLUMNS = [
    "trip_distance",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
]
_TIMESTAMP_COLUMNS = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]
_STRING_COLUMNS = ["store_and_fwd_flag"]

_ALL_RAW_COLUMNS = _INT_COLUMNS + _DOUBLE_COLUMNS + _TIMESTAMP_COLUMNS + _STRING_COLUMNS

# Fields the gateway defaults to 0 when absent, per TLCTripInput. Anything not
# listed here stays null when missing.
_DEFAULT_ZERO = ["extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge"]


def _resolve_raw_columns(raw: DataFrame) -> DataFrame:
    """Normalize a raw TLC frame: canonical column names, canonical types.

    TLC renamed `airport_fee` to `Airport_fee` partway through the dataset's
    life and has varied casing elsewhere, so columns are resolved through a
    lowercased lookup rather than by exact name — the same tactic the simulator
    uses. Columns absent from a given month (congestion_surcharge and
    airport_fee predate nothing, but early months lack them) are materialized as
    typed nulls so every month presents the same shape.
    """
    lower_to_actual = {name.lower(): name for name in raw.columns}
    selected: list[Column] = []

    for name in _ALL_RAW_COLUMNS:
        actual = lower_to_actual.get(name.lower())
        if actual is None:
            selected.append(F.lit(None).cast(_raw_type(name)).alias(name))
            continue
        column = F.col(f"`{actual}`").cast(_raw_type(name))
        if name in _DEFAULT_ZERO:
            # TLCTripInput gives these a default of 0, so a null here would be
            # rejected by the gateway's model but accepted as 0 — match it.
            column = F.coalesce(column, F.lit(0.0))
        selected.append(column.alias(name))

    return raw.select(*selected)


def _raw_type(name: str):
    if name in _INT_COLUMNS:
        return IntegerType()
    if name in _DOUBLE_COLUMNS:
        return DoubleType()
    if name in _TIMESTAMP_COLUMNS:
        return "timestamp"
    return StringType()


def _exact(name: str) -> Column:
    """One raw money/distance column as an exact Decimal, nulls treated as 0.

    Mirrors the gateway's `Decimal(str(value))` and its `value or 0` handling of
    the two late-added surcharge fields.
    """
    return F.coalesce(F.col(name).cast(EXACT), F.lit(0).cast(EXACT))


def _surcharges() -> Column:
    """extra + mta_tax + improvement_surcharge + congestion + airport, exactly."""
    return (
        _exact("extra")
        + _exact("mta_tax")
        + _exact("improvement_surcharge")
        + _exact("congestion_surcharge")
        + _exact("airport_fee")
    )


def _payment_type() -> Column:
    """Numeric TLC code -> canonical PaymentType enum value."""
    mapping = F.when(F.col("payment_type").isNull(), F.lit("UNKNOWN"))
    for code, canonical in KNOWN_PAYMENT_CODES.items():
        mapping = mapping.when(F.col("payment_type") == F.lit(code), F.lit(canonical))
    return mapping.otherwise(F.lit("OTHER"))


def _reject_reason(month: str | None) -> Column:
    """Why the gateway would refuse this record, or null if it would accept it.

    Every branch corresponds to a real 422 from the gateway — either a
    field-level constraint on TLCTripInput or a validator on the canonical
    TripEvent. Rows are counted by reason and logged rather than dropped
    silently. Branch order determines only which reason is *reported* for a row
    that violates several rules; the accept/reject verdict is unaffected.
    """
    required_missing = (
        F.col("VendorID").isNull()
        | F.col("PULocationID").isNull()
        | F.col("DOLocationID").isNull()
        | F.col("tpep_pickup_datetime").isNull()
        | F.col("tpep_dropoff_datetime").isNull()
        | F.col("fare_amount").isNull()
        | F.col("total_amount").isNull()
        | F.col("trip_distance").isNull()
    )

    # The canonical model constrains each of these with ge=0. Note surcharges is
    # checked as the *sum*: the gateway sums first and validates the total, so a
    # negative component offset by a larger positive one is accepted.
    negative_money = (
        (F.col("fare_amount") < 0)
        | (F.col("tip_amount") < 0)
        | (F.col("tolls_amount") < 0)
        | (F.col("total_amount") < 0)
        | (_surcharges() < 0)
    )

    reason = (
        F.when(required_missing, F.lit("missing_required"))
        # TLCTripInput: trip_distance = Field(ge=0)
        .when(F.col("trip_distance") < 0, F.lit("negative_distance"))
        # TripEvent validator: dropoff must be strictly after pickup
        .when(
            F.col("tpep_dropoff_datetime") <= F.col("tpep_pickup_datetime"),
            F.lit("dropoff_not_after_pickup"),
        )
        .when(negative_money, F.lit("negative_money"))
        .when(F.col("passenger_count") < 0, F.lit("negative_passenger_count"))
    )

    if month is not None:
        # Not a gateway rule: TLC's monthly files carry a handful of records
        # whose pickup falls outside the month they ship in (2023-01 contains
        # stray 2008 and 2022 rows). Trust the filename over the field, exactly
        # as the simulator does — otherwise the lake sprouts a year=2008
        # partition holding two rows. Source-packaging knowledge, so it lives
        # here in the source-specific adapter and nowhere downstream.
        start, end = _month_bounds(month)
        reason = reason.when(
            (F.col("tpep_pickup_datetime") < F.lit(start))
            | (F.col("tpep_pickup_datetime") >= F.lit(end)),
            F.lit("outside_source_month"),
        )

    return reason.otherwise(F.lit(None).cast(StringType()))


def _month_bounds(month: str) -> tuple[datetime, datetime]:
    """'2023-01' -> half-open [2023-01-01, 2023-02-01)."""
    year, month_number = (int(part) for part in month.split("-"))
    start = datetime(year, month_number, 1)
    end = datetime(year + 1, 1, 1) if month_number == 12 else datetime(year, month_number + 1, 1)
    return start, end


def _event_id() -> Column:
    """A deterministic, UUID-shaped id derived from the record's natural key.

    The gateway mints a random uuid4 per request, which it can afford because it
    sees each record once. A batch job cannot: re-running a month must produce
    the same rows, or an idempotency check comparing two runs would compare
    different ids forever. Hashing the natural key makes a rerun byte-identical.
    """
    natural_key = F.concat_ws(
        "|",
        F.date_format("tpep_pickup_datetime", "yyyy-MM-dd HH:mm:ss"),
        F.date_format("tpep_dropoff_datetime", "yyyy-MM-dd HH:mm:ss"),
        F.col("PULocationID"),
        F.col("DOLocationID"),
        F.col("total_amount"),
        F.col("VendorID"),
    )
    digest = F.sha2(natural_key, 256)
    return F.concat_ws(
        "-",
        F.substring(digest, 1, 8),
        F.substring(digest, 9, 4),
        F.substring(digest, 13, 4),
        F.substring(digest, 17, 4),
        F.substring(digest, 21, 12),
    )


def adapt_tlc_batch(
    raw: DataFrame,
    *,
    ingested_at: datetime,
    month: str | None = None,
) -> tuple[DataFrame, dict[str, int]]:
    """Adapt a raw TLC frame to the canonical schema.

    Args:
        raw: a frame read straight from a TLC monthly parquet file.
        ingested_at: envelope metadata. For backfilled records this is honestly
            "when the backfill ran", not when a gateway saw them — the gateway
            never saw them. Passed in rather than generated so this function has
            no clock of its own, matching adapt_tlc()'s design.
        month: 'YYYY-MM' of the file being read. When given, records whose
            pickup falls outside that month are rejected. Omit when the caller
            has no monthly packaging to appeal to.

    Returns:
        (canonical frame, counts keyed by rejection reason plus 'total' and
        'accepted'). The counts are logged here too — dropped rows are never
        silent.
    """
    normalized = _resolve_raw_columns(raw).withColumn("_reject_reason", _reject_reason(month))

    # One pass to attribute rejections, so the drop is auditable and comparable
    # against the ~1% the gateway rejects on the same data (P2/P3 measured 0.9%
    # and 1.04%). A number far off that is a bug in this adapter, not bad data.
    counts_by_reason = {
        row["_reject_reason"]: row["count"]
        for row in normalized.groupBy("_reject_reason").count().collect()
    }
    accepted_count = counts_by_reason.pop(None, 0)
    stats: dict[str, int] = {
        "total": accepted_count + sum(counts_by_reason.values()),
        "accepted": accepted_count,
        **counts_by_reason,
    }
    rejected = stats["total"] - accepted_count
    if rejected:
        logger.info(
            "rejected %d of %d record(s) (%.2f%%): %s",
            rejected, stats["total"], 100.0 * rejected / max(stats["total"], 1),
            ", ".join(f"{reason}={count}" for reason, count in sorted(counts_by_reason.items())),
        )

    valid = normalized.filter(F.col("_reject_reason").isNull())

    canonical = valid.select(
        # --- envelope ---
        _event_id().alias("event_id"),
        F.lit(SOURCE).alias("source"),
        F.lit(ingested_at).cast("timestamp").alias("ingested_at"),
        F.lit(SCHEMA_VERSION).alias("schema_version"),
        # --- trip identity ---
        F.col("tpep_pickup_datetime").alias("pickup_datetime"),
        F.col("tpep_dropoff_datetime").alias("dropoff_datetime"),
        # TLC grounds location in zone lookups and has no GPS at all, so lat/lon
        # stay null rather than being invented from zone centroids.
        F.struct(
            F.col("PULocationID").alias("zone_id"),
            F.lit(None).cast(DoubleType()).alias("lat"),
            F.lit(None).cast(DoubleType()).alias("lon"),
        ).alias("pickup_location"),
        F.struct(
            F.col("DOLocationID").alias("zone_id"),
            F.lit(None).cast(DoubleType()).alias("lat"),
            F.lit(None).cast(DoubleType()).alias("lon"),
        ).alias("dropoff_location"),
        F.col("passenger_count"),
        # Source-neutral units: TLC reports miles, the contract stores km.
        # Multiplied as exact Decimal, then narrowed, to match the gateway.
        (F.col("trip_distance").cast(EXACT) * F.lit(MILES_TO_KM).cast(EXACT))
        .cast(DoubleType())
        .alias("trip_distance_km"),
        F.col("VendorID").cast(StringType()).alias("provider_id"),
        _payment_type().alias("payment_type"),
        # --- fare breakdown ---
        F.col("fare_amount").cast(EXACT).cast(MONEY).alias("fare_amount"),
        _surcharges().cast(MONEY).alias("surcharges_amount"),
        F.col("tip_amount").cast(EXACT).cast(MONEY).alias("tip_amount"),
        F.col("tolls_amount").cast(EXACT).cast(MONEY).alias("tolls_amount"),
        F.col("total_amount").cast(EXACT).cast(MONEY).alias("total_amount"),
        # --- audit passthrough: itemized detail the canonical model collapses ---
        F.struct(
            F.col("RatecodeID").alias("ratecode_id"),
            F.col("store_and_fwd_flag"),
            F.col("extra"),
            F.col("mta_tax"),
            F.col("improvement_surcharge"),
            F.col("congestion_surcharge"),
            F.col("airport_fee"),
        ).alias("source_extras"),
        # --- partitions, derived from the canonical event time ---
        F.year("tpep_pickup_datetime").alias("year"),
        F.month("tpep_pickup_datetime").alias("month"),
        F.dayofmonth("tpep_pickup_datetime").alias("day"),
    )

    return canonical, stats
