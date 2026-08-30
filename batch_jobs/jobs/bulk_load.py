"""Backfill one month of raw source files into the lake.

This is the *historical* writer: it supplies the training corpus and the
dashboard's long-run trends from files that never transited the message bus.
It is deliberately NOT what makes the cold path "cold" — the cold path's job is
recomputing the same events the hot path already saw, and that is
`stream_to_lake.py`'s half of the lake.

Why a job may know the source
-----------------------------
This module imports a source-specific adapter and knows the raw files' naming
convention, which looks like a violation of "nothing downstream of the adapter
may reference a specific source". It isn't: the bulk loader is a *sibling of
the gateway*, not something downstream of it. Both are adapter invocations —
the gateway adapts one record arriving over HTTP, this adapts a file's worth at
a time. Everything past the lake still sees canonical columns only.

One month per invocation
------------------------
So Airflow can map over months with `.expand()` and a failed month retries
alone instead of redoing thirty-six. Running the whole range is a shell loop
(or, later, a DAG), not a flag.

Rerunning is safe
-----------------
`partitionOverwriteMode=dynamic` (set in common/spark.py) means a rerun
replaces exactly the date partitions this month produces and leaves every other
date untouched. Combined with the adapter's deterministic `event_id`, a second
run of the same month is byte-identical to the first.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

# Put the component root on sys.path before importing anything out of it.
#
# Both entrypoints need this and neither supplies it: `python jobs/bulk_load.py`
# puts *jobs/* on sys.path, and spark-submit likewise uses the submitted
# script's own directory — so `import adapters...` resolves in neither.
# pyproject's `pythonpath = ["."]` looks like it should cover this but is a
# pytest setting and applies only during a test run.
#
# Doing it here rather than demanding a PYTHONPATH in the environment keeps the
# job runnable identically by hand, under spark-submit, and from whatever
# working directory Airflow picks in step 5 — one less thing for a DAG to get
# wrong. Only the driver ever needs it: the adapter is pure Spark SQL with no
# Python UDFs, so executors run no Python at all.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import functions as F  # noqa: E402

from adapters.tlc_batch_adapter import TLC_SOURCE_EXTRAS, adapt_tlc_batch  # noqa: E402
from common import config  # noqa: E402
from common.spark import build_session  # noqa: E402
from schemas.canonical_spark import lake_columns  # noqa: E402

logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger("batch_jobs.bulk_load")

# The source's file naming convention. Source knowledge, legitimately here for
# the reason in the module docstring — and the same shape data_fetcher writes.
RAW_FILE_TEMPLATE = "yellow_tripdata_{month}.parquet"


def _month_bounds(month: str) -> tuple[datetime, datetime]:
    """'2023-01' -> half-open [2023-01-01, 2023-02-01)."""
    year, month_number = (int(part) for part in month.split("-"))
    start = datetime(year, month_number, 1)
    end = datetime(year + 1, 1, 1) if month_number == 12 else datetime(year, month_number + 1, 1)
    return start, end


def _valid_month(value: str) -> str:
    """argparse type: accept only 'YYYY-MM', and only at or before the cutoff.

    Refusing a post-cutoff month is a guard, not pedantry. Both lake writers use
    dynamic partition overwrite, so if this job ever wrote a date the Kafka
    loader also owns, whichever ran last would silently clobber the other. The
    cutoff is the whole mechanism preventing that — a mistyped `--month
    2026-03` must be a loud error, never a run that quietly writes nothing.
    """
    try:
        start, _ = _month_bounds(value)
    except (ValueError, TypeError):
        raise argparse.ArgumentTypeError(f"expected YYYY-MM, got {value!r}") from None

    if start > config.CUTOFF:
        raise argparse.ArgumentTypeError(
            f"{value} starts after the cutoff ({config.CUTOFF.isoformat()}); "
            "months past the cutoff belong to the Kafka loader, not the backfill"
        )
    return value


def bulk_load(month: str) -> int:
    """Adapt one month of raw files into the lake. Returns rows written."""
    spark = build_session(f"bulk_load-{month}")

    source_path = f"{config.RAW_PATH}/{RAW_FILE_TEMPLATE.format(month=month)}"
    logger.info("reading %s", source_path)
    raw = spark.read.parquet(source_path)

    # `ingested_at` on a backfilled row is honestly "when the backfill ran". The
    # gateway never saw these records; claiming otherwise would be a lie in the
    # audit trail. Naive UTC, matching the session timezone.
    ingested_at = datetime.now(timezone.utc).replace(tzinfo=None)

    # Passing `month` lets the adapter reject records whose pickup falls outside
    # the month they ship in — TLC's files carry a handful of strays. Without it
    # the lake sprouts a year=2008 partition holding two rows.
    canonical, stats = adapt_tlc_batch(raw, ingested_at=ingested_at, month=month)

    # The cutoff filter. A no-op for every month except the boundary one, and
    # kept anyway: `_valid_month` guards whole months, this guards individual
    # records inside the month the cutoff falls in.
    filtered = canonical.filter(F.col("pickup_datetime") <= F.lit(config.CUTOFF))

    # Pin the column order explicitly rather than trusting the adapter's select
    # to stay in step. Both writers call lake_columns(), so the two halves of
    # the lake cannot end up with Parquet files that disagree on layout.
    ordered = filtered.select(*lake_columns(TLC_SOURCE_EXTRAS))

    # Shuffle to one task per date partition before writing. Left alone, the 16
    # shuffle partitions would each emit a file into each of ~31 day
    # directories — ~500 tiny files per month, ~18k across the full backfill,
    # which is slower to write and much slower to read back.
    ordered = ordered.repartition("year", "month", "day")

    # A deliberate extra pass, so the run reports a row count it actually
    # verified rather than one inferred from the adapter's pre-filter stats.
    # One month is ~45MB; the honesty is worth the scan.
    written = ordered.count()
    dropped_by_cutoff = stats["accepted"] - written

    (
        ordered.write.mode("overwrite")
        .partitionBy("year", "month", "day")
        .parquet(config.LAKE_PATH)
    )

    logger.info(
        "%s -> %s: read %d, accepted %d, written %d%s",
        month, config.LAKE_PATH, stats["total"], stats["accepted"], written,
        f", {dropped_by_cutoff} dropped past the cutoff" if dropped_by_cutoff else "",
    )
    spark.stop()
    return written


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--month",
        required=True,
        type=_valid_month,
        help="month to load, as YYYY-MM (one month per invocation)",
    )
    args = parser.parse_args()

    try:
        bulk_load(args.month)
    except Exception:  # noqa: BLE001 — log the trace, then fail the task
        logger.exception("bulk load failed for %s", args.month)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
