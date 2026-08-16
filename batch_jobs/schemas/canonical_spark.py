"""Canonical event contract, expressed for Spark.

Source-independent, like gateway/schemas/canonical.py: nothing in this file may
name TLC or any other source. The one source-shaped thing a taxi event carries —
`source_extras`, typed `dict[str, Any]` in the Pydantic contract — is therefore
*injected* as a StructType by the caller rather than declared here. The adapter
that knows the source supplies its shape; this module never inspects it.

Two schemas, deliberately:

- WIRE_SCHEMA is what the gateway actually puts on Kafka. Pydantic's
  model_dump_json() renders Decimal as a JSON **string** and datetimes as ISO
  8601 **strings**, so every one of those fields is read as StringType and cast
  afterwards. Letting Spark's JSON reader coerce them directly is the trap: on
  any format it dislikes it emits NULL rather than failing, so a serialization
  change would silently empty the money columns instead of erroring.
  It also omits `source_extras` entirely — a source-agnostic wire schema cannot
  know that object's shape. Callers pull it out separately with
  get_json_object(value, '$.source_extras') and parse it with the injected
  schema.

- lake_schema() is the physical Parquet schema. Money is DecimalType(12,2),
  matching both the Pydantic Decimal and the Postgres numeric the hot path
  writes — no float anywhere in the money path, end to end.

Schemas are explicit, never inferred. Same tolerant-reader reasoning as
hot_path/schemas.py: the gateway may ADD a canonical field without silently
changing the lake's physical schema, and a REMOVED field fails loudly here
instead of quietly reading as null.
"""

from __future__ import annotations

from pyspark.sql.types import (
    DecimalType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Mirrors SCHEMA_VERSION in the gateway's canonical contract. Bump together.
SCHEMA_VERSION = "1.0.0"

# Money precision. 12 digits with 2 decimal places holds any plausible fare and
# matches the serving store's numeric columns exactly, so a value can round-trip
# lake -> Postgres -> dashboard without a representation change.
MONEY = DecimalType(12, 2)

# Partition columns, derived from pickup_datetime (the canonical event time).
# CLAUDE.md's P4 verification asserts this exact layout on disk.
PARTITION_COLUMNS = ["year", "month", "day"]


def _location(prefix: str) -> StructType:
    """A pickup or dropoff point.

    Both representations are optional and at least one must be present, exactly
    as the canonical TripLocation defines it — a zone-based source never has to
    fabricate coordinates, and a GPS-based one never has to invent a zone.
    """
    return StructType(
        [
            StructField(f"{prefix}zone_id", IntegerType(), nullable=True),
            StructField(f"{prefix}lat", DoubleType(), nullable=True),
            StructField(f"{prefix}lon", DoubleType(), nullable=True),
        ]
    )


# --------------------------------------------------------------------------
# Wire format: what from_json() parses off the Kafka topic.
# --------------------------------------------------------------------------

WIRE_SCHEMA = StructType(
    [
        # --- envelope: assigned by the gateway, never sourced from the record ---
        StructField("event_id", StringType(), nullable=True),
        StructField("source", StringType(), nullable=True),
        StructField("ingested_at", StringType(), nullable=True),
        StructField("schema_version", StringType(), nullable=True),
        # --- trip identity ---
        StructField("pickup_datetime", StringType(), nullable=True),
        StructField("dropoff_datetime", StringType(), nullable=True),
        StructField("pickup_location", _location(""), nullable=True),
        StructField("dropoff_location", _location(""), nullable=True),
        StructField("passenger_count", IntegerType(), nullable=True),
        StructField("trip_distance_km", DoubleType(), nullable=True),
        StructField("provider_id", StringType(), nullable=True),
        StructField("payment_type", StringType(), nullable=True),
        # --- fare breakdown: JSON strings on the wire, cast to Decimal on load ---
        StructField("fare_amount", StringType(), nullable=True),
        StructField("surcharges_amount", StringType(), nullable=True),
        StructField("tip_amount", StringType(), nullable=True),
        StructField("tolls_amount", StringType(), nullable=True),
        StructField("total_amount", StringType(), nullable=True),
        # source_extras is absent by design — see the module docstring.
    ]
)

# The fare fields that arrive as strings and are cast to MONEY on the way in.
MONEY_FIELDS = [
    "fare_amount",
    "surcharges_amount",
    "tip_amount",
    "tolls_amount",
    "total_amount",
]


# --------------------------------------------------------------------------
# Lake format: what actually lands in Parquet.
# --------------------------------------------------------------------------


def lake_schema(source_extras: StructType) -> StructType:
    """The physical canonical schema, given this source's `source_extras` shape.

    `source_extras` is passed in rather than declared because the canonical
    contract types it as an opaque passthrough dict. Injecting it keeps this
    module free of source knowledge while still buying columnar storage and
    typed access for the fields a model might later use — a JSON blob would
    cost roughly 30x the space and force a parse on every read.
    """
    return StructType(
        [
            # --- envelope ---
            StructField("event_id", StringType(), nullable=False),
            StructField("source", StringType(), nullable=False),
            StructField("ingested_at", TimestampType(), nullable=False),
            StructField("schema_version", StringType(), nullable=False),
            # --- trip identity ---
            # pickup_datetime is the canonical event time and is never nullable:
            # the hot path's windows and the lake's partitions both key off it.
            StructField("pickup_datetime", TimestampType(), nullable=False),
            StructField("dropoff_datetime", TimestampType(), nullable=False),
            StructField("pickup_location", _location(""), nullable=False),
            StructField("dropoff_location", _location(""), nullable=False),
            StructField("passenger_count", IntegerType(), nullable=True),
            StructField("trip_distance_km", DoubleType(), nullable=True),
            StructField("provider_id", StringType(), nullable=True),
            StructField("payment_type", StringType(), nullable=False),
            # --- fare breakdown ---
            StructField("fare_amount", MONEY, nullable=False),
            StructField("surcharges_amount", MONEY, nullable=False),
            StructField("tip_amount", MONEY, nullable=False),
            StructField("tolls_amount", MONEY, nullable=False),
            StructField("total_amount", MONEY, nullable=False),
            # --- audit passthrough, shape supplied by the source's adapter ---
            StructField("source_extras", source_extras, nullable=True),
            # --- partition columns, derived from pickup_datetime ---
            StructField("year", IntegerType(), nullable=False),
            StructField("month", IntegerType(), nullable=False),
            StructField("day", IntegerType(), nullable=False),
        ]
    )


def lake_columns(source_extras: StructType) -> list[str]:
    """Column order for the lake, so both writers emit an identical layout."""
    return [field.name for field in lake_schema(source_extras).fields]
