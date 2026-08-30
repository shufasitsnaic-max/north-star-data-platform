"""Roll the lake up to daily x zone metrics and stage them for the serving store.

Reads the *whole* lake — both writers' halves — and recomputes every day it
finds. Consistent with the rest of the batch layer: there is no incremental
state to drift, and a rerun repairs any past mistake. The output is around
365 days x ~260 zones per year, so a full recompute is trivially cheap
compared to the bookkeeping that avoiding one would cost.

Source independence
-------------------
This job is downstream of the lake and names no source. It reads canonical
columns only and never touches `source_extras`, so unlike the two lake writers
it needs no registry lookup and no configuration naming a source.

Where this job stops
--------------------
It writes a **staging** table and nothing else. The merge into the table the
dashboard reads is `sql/merge_daily.sql`, run as a separate step, because
Spark's JDBC writer has no upsert and overwriting the serving table would
either drop its indexes or leave a reader looking at an empty table mid-write.

That split is also forced by the runtime: `spark-submit` runs jobs with the
container's Python, not this component's uv environment, so the only library
available here is `pyspark`. There is no database driver to run the merge with
even if it belonged in this file — which is why the merge is SQL executed by
whoever has a client, `psql` today and Airflow's Postgres operator in step 5.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import DataFrame, functions as F  # noqa: E402
from pyspark.sql.types import DecimalType  # noqa: E402

from common import config  # noqa: E402
from common.spark import build_session  # noqa: E402

logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger("batch_jobs.aggregate_daily")

# Serving-store column widths, matching sql/schema.sql exactly. Narrowing here
# rather than letting Postgres coerce means a value too wide to store fails in
# Spark with the column named, instead of as a JDBC batch error.
REVENUE = DecimalType(14, 2)
AVERAGE = DecimalType(10, 2)


def _metrics(trips: DataFrame, group_by: list) -> DataFrame:
    """Aggregate to one row per grouping. `group_by` fixes the grain."""
    return trips.groupBy(*group_by).agg(
        F.count(F.lit(1)).alias("trip_count"),
        F.sum("total_amount").cast(REVENUE).alias("total_revenue"),
        F.avg("fare_amount").cast(AVERAGE).alias("avg_fare"),
        F.avg("tip_amount").cast(AVERAGE).alias("avg_tip"),
        # avg() ignores nulls, so this averages over the trips that actually
        # carried a distance. A day where none did yields NULL, which is
        # different from an average of zero and is stored as such.
        F.avg("trip_distance_km").alias("avg_distance_km"),
    )


def aggregate_daily() -> int:
    """Recompute daily x zone rollups into the staging table. Returns row count."""
    spark = build_session("aggregate_daily")

    lake = spark.read.parquet(config.LAKE_PATH)

    trips = lake.select(
        # Event-time day. to_date on the canonical event time, never on
        # ingested_at — a backfilled row's ingest date says when the job ran.
        F.to_date("pickup_datetime").alias("metric_date"),
        F.col("pickup_location.zone_id").alias("zone_id"),
        "fare_amount",
        "tip_amount",
        "total_amount",
        "trip_distance_km",
    )

    per_zone = _metrics(trips, [F.col("metric_date"), F.col("zone_id")])

    # The citywide row is computed from the trips directly, NOT by averaging
    # the per-zone averages — those would weight a 3-trip zone the same as a
    # 30,000-trip one. Two independent aggregations unioned is both correct and
    # more obvious than a rollup/grouping-set, which hides which rows are which.
    citywide = _metrics(trips, [F.col("metric_date")]).select(
        "metric_date",
        F.lit(None).cast("int").alias("zone_id"),
        "trip_count",
        "total_revenue",
        "avg_fare",
        "avg_tip",
        "avg_distance_km",
    )

    rollups = per_zone.unionByName(citywide)

    # A few hundred thousand rows at most, so a handful of partitions is plenty
    # — and each partition opens its own JDBC connection, which is the resource
    # actually worth limiting here.
    #
    # Cached because it is consumed twice, by the write and by the row count
    # below. Without this the second pass re-reads and re-aggregates the entire
    # lake — tens of millions of rows — to produce one number.
    staged = rollups.coalesce(4).cache()

    (
        staged.write.format("jdbc")
        .option("url", config.jdbc_url())
        .option("dbtable", config.STAGING_TABLE)
        .option("user", config.POSTGRES_USER)
        .option("password", config.POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        # Staging is disposable and carries no indexes, so dropping and
        # recreating it costs nothing. This is exactly the write the serving
        # table must NOT receive.
        .mode("overwrite")
        .save()
    )

    written = staged.count()
    days = staged.select("metric_date").distinct().count()
    logger.info(
        "%s -> %s: %d daily x zone row(s) across %d day(s) staged. "
        "Run sql/merge_daily.sql to publish.",
        config.LAKE_PATH, config.STAGING_TABLE, written, days,
    )
    staged.unpersist()
    spark.stop()
    return written


def main() -> int:
    # No arguments: the grain is fixed and the range is "the whole lake".
    argparse.ArgumentParser(description=__doc__.splitlines()[0]).parse_args()

    try:
        aggregate_daily()
    except Exception:  # noqa: BLE001 — log the trace, then fail the task
        logger.exception("daily aggregation failed")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
