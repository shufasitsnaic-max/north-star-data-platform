"""Recompute the lake's post-cutoff half from the message bus.

This is the cold path proper. `bulk_load.py` is a historical backfill of files
that never transited the bus; this job processes **the same events the hot path
already saw**, minutes later instead of seconds, completely instead of within a
watermark, and rerunnably instead of once. When the two layers disagree, this
is the number to trust — which is what makes the hot path's documented
compromises acceptable rather than defects.

Source independence
-------------------
Unlike `bulk_load.py`, this job is downstream of the bus and may not know the
data source. It never imports an adapter. The one unavoidable piece of source
knowledge — the shape of the `source_extras` struct, which has to be declared
to be stored as typed columns — arrives as a *name* from configuration and is
resolved through `adapters/registry.py`. Configuration may know the source;
code may not.

Running it
----------
The Kafka source is not bundled in the apache/spark image and is a launcher-time
dependency, so every invocation must supply it:

    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 jobs/stream_to_lake.py

The coordinate lives in config.KAFKA_SQL_PACKAGE so the DAG and this docstring
cannot drift apart. It cannot be enforced from inside the job — by the time
Python runs, the JVM classpath is already fixed — so a missing connector is
turned into an error message naming this command rather than Spark's bare
"Failed to find data source: kafka".

Full recompute, every run
-------------------------
`startingOffsets: earliest` -> `endingOffsets: latest` on every invocation, with
no offset bookkeeping and no checkpoint. Recomputing the world from an immutable
log is what the batch layer of a lambda architecture is *for*: there is no
watermark state to corrupt, a rerun repairs any past mistake, and a bad offset
cannot silently produce a permanently wrong lake. At this data size the whole
topic is seconds of work.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import DataFrame, functions as F  # noqa: E402
from pyspark.sql.types import TimestampType  # noqa: E402

from adapters.registry import source_extras_schema  # noqa: E402
from common import config  # noqa: E402
from common.spark import build_session  # noqa: E402
from schemas.canonical_spark import MONEY, MONEY_FIELDS, WIRE_SCHEMA, lake_columns  # noqa: E402

logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger("batch_jobs.stream_to_lake")

# The natural key duplicates are collapsed on. Deliberately NOT event_id: the
# gateway mints a fresh uuid4 per request, so a re-replayed trip arrives with a
# different id and would survive any id-based dedupe. See the module note below
# on what this costs.
NATURAL_KEY = [
    "pickup_datetime",
    "dropoff_datetime",
    "pickup_zone_id",
    "dropoff_zone_id",
    "total_amount",
    "provider_id",
]

# Substrings identifying "the topic does not exist yet", which is a waiting
# state rather than a failure — the topic is created by the gateway's first
# produce, so before any replay has run there is legitimately nothing to read.
# The hot path treats this the same way. Matched narrowly on purpose: anything
# unrecognized is re-raised rather than swallowed.
_MISSING_TOPIC_MARKERS = (
    "unknown_topic_or_partition",
    "unknowntopicorpartition",
    "do not exist",
    "does not exist",
)

# The connector is not bundled in the apache/spark image and cannot be added
# from inside a running JVM — see the note in common/spark.py. When it is
# absent Spark says only "Failed to find data source: kafka", which does not
# hint at the cause, so it is translated into the command that fixes it.
_MISSING_CONNECTOR_MARKER = "failed to find data source: kafka"


def _read_topic(spark) -> DataFrame:
    """Read the whole topic as a batch, earliest to latest."""
    return (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", config.KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        # A batch read of a retention-trimmed topic should not abort: the lake
        # is a recomputation of what the bus still holds, and a partial read is
        # a smaller lake, not a corrupt one.
        .option("failOnDataLoss", "false")
        .load()
    )


def _parse(raw: DataFrame, source: str) -> DataFrame:
    """Wire JSON -> canonical columns, typed as the lake stores them."""
    extras_schema = source_extras_schema(source)

    payload = raw.select(F.col("value").cast("string").alias("json"))

    parsed = payload.select(
        F.from_json("json", WIRE_SCHEMA).alias("e"),
        # source_extras is absent from WIRE_SCHEMA by design — a source-agnostic
        # wire schema cannot know that object's shape — so it is pulled out
        # separately and parsed with the shape configuration named.
        F.from_json(F.get_json_object("json", "$.source_extras"), extras_schema).alias(
            "source_extras"
        ),
    )

    # Pydantic's model_dump_json() renders Decimal and datetime as JSON strings,
    # so every one of these arrives as StringType and is cast here. Casting
    # rather than letting the JSON reader coerce is deliberate: on a format it
    # dislikes the reader yields NULL instead of failing, which would silently
    # empty the money columns after a serialization change.
    cast_to_money = [F.col(f"e.{name}").cast(MONEY).alias(name) for name in MONEY_FIELDS]
    cast_to_timestamp = [
        F.col(f"e.{name}").cast(TimestampType()).alias(name)
        for name in ("pickup_datetime", "dropoff_datetime", "ingested_at")
    ]

    # Everything the wire already carries in its final type. Structs pass
    # through whole — the wire and lake definitions of a location are identical.
    verbatim = [
        F.col(f"e.{name}").alias(name)
        for name in (
            "event_id",
            "source",
            "schema_version",
            "pickup_location",
            "dropoff_location",
            "passenger_count",
            "trip_distance_km",
            "provider_id",
            "payment_type",
        )
    ]

    return parsed.select(
        *verbatim, *cast_to_money, *cast_to_timestamp, F.col("source_extras")
    )


def _deduplicate(events: DataFrame) -> tuple[DataFrame, int, int]:
    """Collapse duplicates on the natural key. Returns (frame, before, after).

    Necessary because a re-run replay pushes every trip through the gateway
    again with a fresh `event_id`, so without this a second demo replay would
    double every number in the lake.

    The cost, stated plainly: two genuinely distinct trips that share all six
    key fields collapse into one, and that is a real if small loss. It is the
    right trade here and the opposite of the call made in `bulk_load.py`, where
    reruns are already idempotent through partition overwrite and dedupe would
    only ever destroy real rows. Double-counting an entire replay is far worse
    than losing a handful of coincidences.
    """
    keyed = events.withColumn("pickup_zone_id", F.col("pickup_location.zone_id")).withColumn(
        "dropoff_zone_id", F.col("dropoff_location.zone_id")
    )
    before = keyed.count()
    deduped = keyed.dropDuplicates(NATURAL_KEY).drop("pickup_zone_id", "dropoff_zone_id")
    after = deduped.count()
    return deduped, before, after


def stream_to_lake() -> int:
    """Rebuild the post-cutoff partitions from the topic. Returns rows written."""
    spark = build_session("stream_to_lake")

    try:
        raw = _read_topic(spark)
        events = _parse(raw, config.SOURCE_EXTRAS)
    except Exception as exc:  # noqa: BLE001 — narrowed immediately below
        message = str(exc).lower()
        if _MISSING_CONNECTOR_MARKER in message:
            spark.stop()
            raise RuntimeError(
                "the Kafka source is not on the classpath. It is a launcher-time "
                "dependency and cannot be added from inside a running JVM, so it must "
                f"be passed at submit time:\n"
                f"    spark-submit --packages {config.KAFKA_SQL_PACKAGE} "
                f"{Path(__file__).name}"
            ) from exc
        if any(marker in message for marker in _MISSING_TOPIC_MARKERS):
            logger.info(
                "topic %s does not exist yet — nothing to recompute. Run the simulator "
                "to populate it.",
                config.KAFKA_TOPIC,
            )
            spark.stop()
            return 0
        raise

    # The cutoff filter, and the reason this job cannot clobber the backfill:
    # the two writers own disjoint date ranges, and both use dynamic partition
    # overwrite, so an overlap would mean whichever ran last wins silently.
    post_cutoff = events.filter(F.col("pickup_datetime") > F.lit(config.CUTOFF))

    deduped, before, after = _deduplicate(post_cutoff)
    if before == 0:
        logger.info(
            "no events after the cutoff (%s) on topic %s — lake unchanged",
            config.CUTOFF.isoformat(), config.KAFKA_TOPIC,
        )
        spark.stop()
        return 0

    partitioned = (
        deduped.withColumn("year", F.year("pickup_datetime"))
        .withColumn("month", F.month("pickup_datetime"))
        .withColumn("day", F.dayofmonth("pickup_datetime"))
    )

    # Same column order and same file-per-day shuffle as bulk_load, through the
    # same helper. The two writers share one directory tree, so a layout
    # disagreement here would produce Parquet files that cannot be read as one
    # dataset.
    ordered = partitioned.select(*lake_columns(source_extras_schema(config.SOURCE_EXTRAS)))
    ordered = ordered.repartition("year", "month", "day")

    (
        ordered.write.mode("overwrite")
        .partitionBy("year", "month", "day")
        .parquet(config.LAKE_PATH)
    )

    logger.info(
        "topic %s -> %s: %d post-cutoff event(s), %d after dedupe (%d duplicate(s) collapsed)",
        config.KAFKA_TOPIC, config.LAKE_PATH, before, after, before - after,
    )
    spark.stop()
    return after


def main() -> int:
    # No arguments: this job has exactly one mode. The range it reads is the
    # whole topic and the range it owns is everything past the cutoff, both
    # fixed by configuration rather than by the caller.
    argparse.ArgumentParser(description=__doc__.splitlines()[0]).parse_args()

    try:
        stream_to_lake()
    except Exception:  # noqa: BLE001 — log the trace, then fail the task
        logger.exception("stream-to-lake failed")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
