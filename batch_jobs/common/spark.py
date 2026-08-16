"""SparkSession construction, with the settings every cold-path job needs.

Kept in one place so the two lake writers cannot disagree about timezone or
partition-overwrite semantics — a divergence there would corrupt the lake
rather than raise an error.
"""

from __future__ import annotations

import logging
import sys
import time

from pyspark.sql import SparkSession

from common import config

logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger("batch_jobs.spark")


def build_session(app_name: str, max_attempts: int = 6) -> SparkSession:
    """Build a SparkSession, retrying with backoff until the master answers.

    Inter-service connections retry before failing structurally (CLAUDE.md), and
    a Spark master that is still electing itself when Airflow fires the first
    task is the normal case on a cold `docker compose up`, not an error.
    """
    delay = 2.0
    last_error: Exception | None = None

    for attempt in range(1, max_attempts + 1):
        try:
            builder = (
                SparkSession.builder.appName(app_name)
                .master(config.SPARK_MASTER)
                # Every timestamp in this platform is handled as written. TLC's
                # pickup/dropoff times are naive local wall-clock, and the hot
                # path already buckets them as-is; pinning the session to UTC
                # stops Spark from shifting them by the container's timezone and
                # keeps hot and cold agreeing on what "2023-01-15 08:00" means.
                .config("spark.sql.session.timeZone", "UTC")
                # Overwrite only the partitions this run actually produced,
                # leaving every other date on disk untouched. This is what makes
                # a full recompute safe to run repeatedly.
                .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
                # Snappy is the P4 requirement and the sane default: splittable,
                # cheap to decompress, good enough ratio for columnar data.
                .config("spark.sql.parquet.compression.codec", "snappy")
                # 200 shuffle partitions is Spark's default and is sized for a
                # real cluster. At one month per job it just produces hundreds of
                # tiny files, which is slower to write and slower to read back.
                .config("spark.sql.shuffle.partitions", "16")
            )

            if config.SPARK_DRIVER_HOST:
                # Executors dial the driver back on this address. Without it,
                # Spark advertises the container's internal hostname and the
                # executors hang until the task times out — the single most
                # common Spark-on-Compose failure.
                builder = builder.config("spark.driver.host", config.SPARK_DRIVER_HOST)
                builder = builder.config("spark.driver.bindAddress", "0.0.0.0")

            session = builder.getOrCreate()
            logger.info(
                "SparkSession '%s' ready on %s (attempt %d)",
                app_name, config.SPARK_MASTER, attempt,
            )
            return session
        except Exception as exc:  # noqa: BLE001 — any startup failure is retryable
            last_error = exc
            logger.warning(
                "Spark not ready (attempt %d/%d): %s — retrying in %.0fs",
                attempt, max_attempts, exc, delay,
            )
            time.sleep(delay)
            delay = min(delay * 2, 30)

    logger.exception("could not start Spark after %d attempts", max_attempts)
    raise RuntimeError(f"could not start Spark after {max_attempts} attempts") from last_error
