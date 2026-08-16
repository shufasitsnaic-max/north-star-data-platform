"""The drift guard: the batch adapter must agree with the gateway's adapter.

Two implementations of one mapping exist (see adapters/tlc_batch_adapter.py for
why), and nothing but this test stops them diverging. It compares against a
committed fixture of *recorded gateway output* rather than importing gateway
code, so the components stay decoupled at build time.

What is deliberately NOT compared: `event_id` and `ingested_at`. Both are
envelope metadata that the canonical contract defines as assigned by the
producer rather than derived from the record — adapt_tlc() takes them as
parameters for exactly this reason. They get their own weaker assertions below.

Runs in local Spark; no cluster, no Kafka, no database. It does need a JDK on
the machine running it.
"""

from __future__ import annotations

import json
from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

import pytest
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from adapters.tlc_batch_adapter import TLC_SOURCE_EXTRAS, adapt_tlc_batch
from schemas.canonical_spark import SCHEMA_VERSION, lake_schema

FIXTURE = Path(__file__).parent / "fixtures" / "conformance.json"

# Fixed so the run is reproducible; excluded from comparison regardless.
INGESTED_AT = datetime(2026, 1, 1, 0, 0, 0)

# A float field's last bits are representation noise, not a mapping difference:
# the gateway multiplies exact Decimals and calls float(), we cast a Decimal to
# double. Any *real* divergence here would be metres, not femtometres.
DISTANCE_TOLERANCE = 1e-12

CENTS = Decimal("0.01")


def _cents(value) -> Decimal:
    """Round to the lake's precision the way Spark does.

    Spark's decimal cast rounds HALF_UP; Python's quantize defaults to
    HALF_EVEN. TLC money is already 2dp so the two never actually disagree on
    this data, but matching the rounding mode means a future source with 3dp
    fares fails on a real mapping difference rather than on a tie-break rule.
    """
    return Decimal(str(value)).quantize(CENTS, rounding=ROUND_HALF_UP)


# TLC ships its integer codes as floats so Parquet can represent nulls, so the
# fixture holds them that way and the adapter's narrowing gets exercised.
_DOUBLE_FIELDS = [
    "VendorID", "passenger_count", "RatecodeID", "payment_type",
    "PULocationID", "DOLocationID", "trip_distance", "fare_amount", "extra",
    "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge",
    "total_amount", "congestion_surcharge", "airport_fee",
]

RAW_SCHEMA = StructType(
    [StructField(name, DoubleType(), nullable=True) for name in _DOUBLE_FIELDS]
    + [
        StructField("tpep_pickup_datetime", TimestampType(), nullable=True),
        StructField("tpep_dropoff_datetime", TimestampType(), nullable=True),
        StructField("store_and_fwd_flag", StringType(), nullable=True),
    ]
)


# --------------------------------------------------------------------------
# fixtures
# --------------------------------------------------------------------------


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.appName("adapter-conformance")
        .master("local[2]")
        # Must match common/spark.py, or the test would validate the adapter
        # under different timestamp semantics than the jobs actually run with.
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="session")
def fixture_data():
    if not FIXTURE.exists():
        pytest.fail(
            f"missing {FIXTURE}. Generate it against a running stack:\n"
            "  docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \\\n"
            "      --bootstrap-server localhost:9092 --topic tlc-raw-events \\\n"
            "      --from-beginning --max-messages 3000 > /tmp/canonical.jsonl\n"
            "  uv run python tests/generate_fixture.py --raw ../data/raw/"
            "yellow_tripdata_2023-01.parquet --canonical /tmp/canonical.jsonl"
        )
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def _to_rows(raw_records: list[dict]) -> list[tuple]:
    """Fixture dicts -> tuples matching RAW_SCHEMA's field order."""
    rows = []
    for record in raw_records:
        values = [
            None if record.get(name) is None else float(record[name])
            for name in _DOUBLE_FIELDS
        ]
        values.append(datetime.fromisoformat(record["tpep_pickup_datetime"]))
        values.append(datetime.fromisoformat(record["tpep_dropoff_datetime"]))
        values.append(record.get("store_and_fwd_flag"))
        rows.append(tuple(values))
    return rows


def _adapt(spark, raw_records: list[dict]):
    frame = spark.createDataFrame(_to_rows(raw_records), schema=RAW_SCHEMA)
    # month=None: the fixture's rows were already month-filtered upstream, and
    # the gateway itself has no month rule to conform to.
    return adapt_tlc_batch(frame, ingested_at=INGESTED_AT, month=None)


def _collect_by_key(canonical):
    """Collect the adapted frame, keyed by natural key, timestamps as strings.

    Formatting the timestamps inside Spark sidesteps the naive-datetime timezone
    round-trip that createDataFrame/collect would otherwise impose, so the test
    asserts what Spark stored rather than what Python inferred on the way back.
    """
    rows = canonical.select(
        "*",
        F.date_format("pickup_datetime", "yyyy-MM-dd HH:mm:ss").alias("_pickup_str"),
        F.date_format("dropoff_datetime", "yyyy-MM-dd HH:mm:ss").alias("_dropoff_str"),
    ).collect()
    return {
        (
            row["_pickup_str"],
            row["_dropoff_str"],
            row["pickup_location"]["zone_id"],
            row["dropoff_location"]["zone_id"],
            _cents(row["total_amount"]),
        ): row
        for row in rows
    }


def _expected_key(event: dict):
    return (
        datetime.fromisoformat(event["pickup_datetime"]).strftime("%Y-%m-%d %H:%M:%S"),
        datetime.fromisoformat(event["dropoff_datetime"]).strftime("%Y-%m-%d %H:%M:%S"),
        event["pickup_location"]["zone_id"],
        event["dropoff_location"]["zone_id"],
        _cents(event["total_amount"]),
    )


# --------------------------------------------------------------------------
# the accept/reject boundary
# --------------------------------------------------------------------------


def test_gateway_accepted_rows_are_all_accepted(spark, fixture_data):
    """Every row the gateway published must survive the batch adapter."""
    accepted = fixture_data["accepted"]
    _, stats = _adapt(spark, [pair["raw"] for pair in accepted])

    assert stats["accepted"] == len(accepted), (
        f"batch adapter rejected {len(accepted) - stats['accepted']} row(s) the gateway "
        f"accepted — the two disagree on validity. Reasons: "
        f"{ {k: v for k, v in stats.items() if k not in ('total', 'accepted')} }"
    )


def test_gateway_rejected_rows_are_all_rejected(spark, fixture_data):
    """And every row it refused must be refused here too.

    This is the half a values-only fixture would miss: an adapter that mapped
    every field perfectly but silently admitted negative-fare refunds would pass
    the comparison test and quietly poison the lake.
    """
    rejected = fixture_data["rejected"]
    if not rejected:
        pytest.skip("fixture captured no gateway rejections to compare against")

    _, stats = _adapt(spark, rejected)
    assert stats["accepted"] == 0, (
        f"batch adapter accepted {stats['accepted']} row(s) the gateway rejected with a 422"
    )


# --------------------------------------------------------------------------
# the value mapping
# --------------------------------------------------------------------------


def test_canonical_values_match_gateway_output(spark, fixture_data):
    accepted = fixture_data["accepted"]
    canonical, _ = _adapt(spark, [pair["raw"] for pair in accepted])
    produced = _collect_by_key(canonical)

    money_fields = [
        "fare_amount", "surcharges_amount", "tip_amount", "tolls_amount", "total_amount",
    ]

    for pair in accepted:
        expected = pair["canonical"]
        key = _expected_key(expected)
        assert key in produced, f"no adapted row for gateway event {expected['event_id']}"
        row = produced[key]

        assert row["source"] == expected["source"]
        assert row["schema_version"] == expected["schema_version"] == SCHEMA_VERSION
        assert row["payment_type"] == expected["payment_type"]
        assert row["provider_id"] == expected["provider_id"]
        assert row["passenger_count"] == expected["passenger_count"]

        for side in ("pickup_location", "dropoff_location"):
            assert row[side]["zone_id"] == expected[side]["zone_id"]
            # TLC has no GPS at all; inventing coordinates from zone centroids
            # would fabricate precision the source never had.
            assert row[side]["lat"] is None and row[side]["lon"] is None

        # Money is compared at the lake's own precision. The gateway emits an
        # unquantized Decimal string ("5.0"); the lake stores numeric(12,2)
        # ("5.00"). Same value, and 2dp is all the lake can represent anyway.
        for field in money_fields:
            actual, wanted = _cents(row[field]), _cents(expected[field])
            assert actual == wanted, f"{field}: batch {actual} != gateway {wanted}"

        if expected["trip_distance_km"] is None:
            assert row["trip_distance_km"] is None
        else:
            delta = abs(row["trip_distance_km"] - expected["trip_distance_km"])
            scale = max(abs(expected["trip_distance_km"]), 1.0)
            assert delta / scale < DISTANCE_TOLERANCE, (
                f"trip_distance_km: batch {row['trip_distance_km']} != "
                f"gateway {expected['trip_distance_km']}"
            )

        for field in TLC_SOURCE_EXTRAS.fieldNames():
            actual, wanted = row["source_extras"][field], expected["source_extras"][field]
            if isinstance(wanted, float) and actual is not None:
                assert abs(actual - wanted) < 1e-9, f"source_extras.{field}"
            else:
                assert actual == wanted, f"source_extras.{field}: {actual} != {wanted}"


# --------------------------------------------------------------------------
# envelope + physical schema
# --------------------------------------------------------------------------


def test_event_id_is_uuid_shaped_and_deterministic(spark, fixture_data):
    """Not compared against the gateway — it mints a random uuid4 per request.

    What must hold is that a rerun of the same month produces the same ids, so
    the P4 idempotency check compares rows rather than fresh UUIDs every time.
    """
    raw = [pair["raw"] for pair in fixture_data["accepted"]][:50]
    first, _ = _adapt(spark, raw)
    second, _ = _adapt(spark, raw)

    ids_first = [row["event_id"] for row in first.select("event_id").collect()]
    ids_second = [row["event_id"] for row in second.select("event_id").collect()]

    assert ids_first == ids_second, "event_id is not stable across runs"
    for event_id in ids_first:
        parts = event_id.split("-")
        assert [len(part) for part in parts] == [8, 4, 4, 4, 12], f"not UUID-shaped: {event_id}"


def test_partition_columns_follow_event_time(spark, fixture_data):
    """Partitions key off pickup_datetime, never ingested_at or wall clock."""
    raw = [pair["raw"] for pair in fixture_data["accepted"]][:50]
    canonical, _ = _adapt(spark, raw)

    mismatched = canonical.filter(
        (F.col("year") != F.year("pickup_datetime"))
        | (F.col("month") != F.month("pickup_datetime"))
        | (F.col("day") != F.dayofmonth("pickup_datetime"))
    ).count()
    assert mismatched == 0


def test_output_matches_the_declared_lake_schema(spark, fixture_data):
    """The adapter's frame must be exactly what lake_schema() promises.

    Both writers target this schema; if they disagree on column order or type,
    the second one to write a partition corrupts it rather than erroring.
    """
    raw = [pair["raw"] for pair in fixture_data["accepted"]][:10]
    canonical, _ = _adapt(spark, raw)
    expected = lake_schema(TLC_SOURCE_EXTRAS)

    assert canonical.schema.fieldNames() == expected.fieldNames()
    for produced_field, expected_field in zip(canonical.schema.fields, expected.fields):
        assert produced_field.dataType == expected_field.dataType, (
            f"{produced_field.name}: {produced_field.dataType} != {expected_field.dataType}"
        )
