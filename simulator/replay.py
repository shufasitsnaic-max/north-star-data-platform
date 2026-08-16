"""TLC replay simulator.

Reads historical TLC yellow-taxi parquet files and replays their records
through the gateway in pickup_datetime order, at a configurable pace. This is
what makes a static dataset behave like a "live" stream (see DECISIONS.md:
"Streaming = replay, not live data").

Scope boundaries (deliberate):
- Speaks only the gateway's HTTP contract (POST /events/trips). It has no idea
  Kafka exists and never references the canonical schema — the gateway owns the
  validate -> adapt -> produce step.
- Sends TLC-shaped payloads (raw field names/units); the gateway's adapter is
  the single place that maps them to the canonical event.

Config comes from env (for the container) with CLI overrides (for local runs):
    DATA_DIR / --data-dir        where the parquet files live      (default /data/raw)
    GATEWAY_URL / --gateway-url  gateway base URL                  (default http://gateway:8000)
    --start-month / --end-month  restrict which monthly files load (YYYY-MM)
    --start-datetime             begin replay at this pickup time  (the ML cutoff)
    --max-rows                   cap number of records replayed
    --sleep                      seconds to wait between records    (default 0)

Memory: records are streamed one monthly file at a time, never concatenated.
With the full 2023-01..2026-05 scope on disk, loading every file to sort it
globally would mean ~120M rows in memory before the first record is sent. See
iter_records() for why a per-file sort still yields a globally ordered replay.
"""

from __future__ import annotations

import argparse
import logging
import math
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import httpx
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pyarrow import Table

logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger("simulator")

# Canonical TLC field names the gateway's TLCTripInput expects. We rename each
# source file's columns to these (case-insensitively) so payload keys line up.
FIELDS = [
    "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
    "RatecodeID", "store_and_fwd_flag", "PULocationID", "DOLocationID", "payment_type",
    "trip_distance", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
    "improvement_surcharge", "total_amount", "congestion_surcharge", "airport_fee",
]
PICKUP = "tpep_pickup_datetime"
DT_FIELDS = {"tpep_pickup_datetime", "tpep_dropoff_datetime"}
# Integer codes that TLC stores as floats (so nulls are representable in parquet).
INT_FIELDS = {"VendorID", "passenger_count", "RatecodeID", "payment_type", "PULocationID", "DOLocationID"}


def month_of(path: Path) -> str:
    """'…/yellow_tripdata_2023-01.parquet' -> '2023-01'."""
    return path.stem.replace("yellow_tripdata_", "")


def discover_files(data_dir: Path, start_month: str | None, end_month: str | None) -> list[Path]:
    """Find yellow_tripdata_YYYY-MM.parquet files, optionally within a range."""
    files = sorted(data_dir.glob("yellow_tripdata_*.parquet"))
    if not files:
        raise FileNotFoundError(f"no yellow_tripdata_*.parquet under {data_dir} — run data_fetcher first")

    if start_month:
        files = [f for f in files if month_of(f) >= start_month]
    if end_month:
        files = [f for f in files if month_of(f) <= end_month]
    if not files:
        raise FileNotFoundError(f"no files in range [{start_month}..{end_month}] under {data_dir}")
    return files


def load_normalized(path: Path) -> Table:
    """Read one file, keep only known fields, rename them to canonical names.

    TLC varies column casing across months (e.g. airport_fee vs Airport_fee),
    so we resolve each field case-insensitively rather than assuming exact names.
    """
    table = pq.read_table(path)
    lower_to_actual = {name.lower(): name for name in table.column_names}
    present = [(f, lower_to_actual[f.lower()]) for f in FIELDS if f.lower() in lower_to_actual]
    selected = table.select([actual for _, actual in present])
    return selected.rename_columns([canonical for canonical, _ in present])


def month_bounds(files: list[Path]) -> tuple[datetime, datetime]:
    """Half-open [start, end) datetime span covered by the loaded monthly files."""
    months = sorted(month_of(f) for f in files)
    first_year, first_month = (int(p) for p in months[0].split("-"))
    last_year, last_month = (int(p) for p in months[-1].split("-"))
    start = datetime(first_year, first_month, 1)
    # Exclusive upper bound = first instant of the month after the last file.
    end = datetime(last_year + 1, 1, 1) if last_month == 12 else datetime(last_year, last_month + 1, 1)
    return start, end


def prepare_month(path: Path, start_dt: datetime | None) -> Table:
    """Load one month, drop out-of-month records, sort ascending, trim to >= start_dt."""
    table = load_normalized(path)

    # TLC files contain a handful of records whose pickup_datetime falls far
    # outside the month they ship in (2023-01 carries stray 2008 and 2022 rows).
    # They're ~0.002% of records, but sorting ascending piles every one of them
    # at the front of the replay — so the stream would open on 2008 timestamps
    # and the hot path's event-time windows would span years of empty ground.
    # Trust the filename over the field: keep only rows inside this file's month.
    start, end = month_bounds([path])
    before = table.num_rows
    # `&` (not pc.and_) — Acero's expression engine has no `and_` function.
    table = table.filter(
        pc.greater_equal(pc.field(PICKUP), pc.scalar(start))
        & pc.less(pc.field(PICKUP), pc.scalar(end))
    )
    dropped = before - table.num_rows
    if dropped:
        # Never drop rows silently — say how many and over what span.
        logger.info(
            "%s: dropped %d record(s) with pickup outside [%s, %s)",
            path.name, dropped, start.date().isoformat(), end.date().isoformat(),
        )

    table = table.sort_by([(PICKUP, "ascending")])
    if start_dt is not None:
        table = table.filter(pc.greater_equal(pc.field(PICKUP), pc.scalar(start_dt)))
    return table


def iter_records(files: list[Path], start_dt: datetime | None, limit: int | None):
    """Yield cleaned records in ascending pickup order, one month at a time.

    Deliberately streams rather than building one table for the whole range.
    With the full 2023-01..2026-05 scope downloaded that would be ~120M rows
    concatenated and globally sorted in memory before a single record was sent —
    tens of gigabytes, on a 16GB machine, to replay the first five thousand.

    A per-file sort is sufficient for a *global* ordering here, and that is the
    non-obvious part: the out-of-month filter above guarantees every row in a
    file falls inside that file's own month, and the files are visited in
    chronological order. So months cannot interleave, and concatenating
    per-month sorts yields the same sequence a global sort would — without ever
    holding more than one month.

    Batching keeps the Python-side materialization bounded too: to_pylist() on a
    3M-row month builds 3M dicts at once.
    """
    emitted = 0
    for path in files:  # filename order is chronological for yellow_tripdata_YYYY-MM
        table = prepare_month(path, start_dt)
        if table.num_rows == 0:
            continue
        if limit is not None:
            table = table.slice(0, limit - emitted)

        logger.info("%s: replaying %d record(s)", path.name, table.num_rows)
        for batch in table.to_batches(max_chunksize=10_000):
            for row in batch.to_pylist():
                yield clean_row(row)
                emitted += 1
        if limit is not None and emitted >= limit:
            return


def clean_row(row: dict) -> dict:
    """Make one Arrow row JSON-serializable and typed the way the gateway wants."""
    out: dict = {}
    for key, value in row.items():
        if value is None:
            out[key] = None
        elif key in DT_FIELDS:
            out[key] = value.isoformat()  # datetime -> ISO 8601 string
        elif isinstance(value, float) and math.isnan(value):
            out[key] = None  # NaN is not valid JSON; treat as missing
        elif key in INT_FIELDS and isinstance(value, float):
            out[key] = int(value)  # 1.0 -> 1 for integer codes
        else:
            out[key] = value
    return out


def wait_for_gateway(client: httpx.Client, base_url: str, max_attempts: int = 12) -> None:
    """Poll the gateway's /health with backoff before starting the replay."""
    delay = 1.0
    for attempt in range(1, max_attempts + 1):
        try:
            resp = client.get(f"{base_url}/health", timeout=5.0)
            if resp.status_code == 200:
                logger.info("gateway healthy (attempt %d)", attempt)
                return
        except httpx.HTTPError as exc:
            logger.warning("gateway not ready (attempt %d/%d): %s", attempt, max_attempts, exc)
        time.sleep(delay)
        delay = min(delay * 2, 20)
    raise RuntimeError(f"gateway at {base_url} never became healthy")


def post_row(client: httpx.Client, url: str, payload: dict, max_attempts: int = 4) -> str:
    """POST one record. Returns 'sent' (202), 'rejected' (422), or 'error'.

    A 422 is expected occasionally — real TLC data contains records the gateway
    rightly refuses (e.g. dropoff before pickup). We log and skip, never crash.
    Connection errors retry with backoff, since the gateway may briefly blip.
    """
    delay = 1.0
    for attempt in range(1, max_attempts + 1):
        try:
            resp = client.post(url, json=payload, timeout=10.0)
            if resp.status_code == 202:
                return "sent"
            if resp.status_code == 422:
                logger.debug("gateway rejected a record (422): %s", resp.text[:200])
                return "rejected"
            logger.warning("unexpected status %d: %s", resp.status_code, resp.text[:200])
            return "error"
        except httpx.HTTPError as exc:
            logger.warning("POST failed (attempt %d/%d): %s", attempt, max_attempts, exc)
            time.sleep(delay)
            delay = min(delay * 2, 20)
    return "error"


def main() -> int:
    parser = argparse.ArgumentParser(description="Replay TLC records through the gateway.")
    parser.add_argument("--data-dir", default=os.environ.get("DATA_DIR", "/data/raw"))
    parser.add_argument("--gateway-url", default=os.environ.get("GATEWAY_URL", "http://gateway:8000"))
    parser.add_argument("--start-month", default=os.environ.get("START_MONTH"), help="YYYY-MM")
    parser.add_argument("--end-month", default=os.environ.get("END_MONTH"), help="YYYY-MM")
    parser.add_argument("--start-datetime", default=os.environ.get("START_DATETIME"),
                        help="ISO datetime; replay begins at this pickup time (the ML cutoff)")
    parser.add_argument("--max-rows", type=int, default=int(os.environ.get("MAX_ROWS", "0")) or None)
    parser.add_argument("--sleep", type=float, default=float(os.environ.get("SLEEP", "0")))
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    start_dt = datetime.fromisoformat(args.start_datetime) if args.start_datetime else None

    # A replay that begins at a cutoff has no use for the months before it, and
    # opening them costs a full read each. Derive the lower file bound from the
    # cutoff when one wasn't given explicitly, so `--start-datetime 2026-01-01`
    # doesn't quietly scan all of 2023-2025 to find nothing.
    start_month = args.start_month
    if start_month is None and start_dt is not None:
        start_month = start_dt.strftime("%Y-%m")
        logger.info("no --start-month given; derived %s from --start-datetime", start_month)

    try:
        files = discover_files(data_dir, start_month, args.end_month)
        logger.info("loading %d file(s): %s", len(files), ", ".join(f.name for f in files))
    except (FileNotFoundError, OSError) as exc:
        logger.error("could not discover files: %s", exc)
        return 2

    limit = args.max_rows or None
    logger.info(
        "replaying %s record(s)%s",
        limit if limit else "all",
        f" from {start_dt.isoformat()}" if start_dt else "",
    )

    endpoint = f"{args.gateway_url}/events/trips"
    counts = {"sent": 0, "rejected": 0, "error": 0}
    try:
        with httpx.Client() as client:
            wait_for_gateway(client, args.gateway_url)
            for i, row in enumerate(iter_records(files, start_dt, limit)):
                outcome = post_row(client, endpoint, row)
                counts[outcome] += 1
                if args.sleep:
                    time.sleep(args.sleep)
                if (i + 1) % 1000 == 0:
                    logger.info("progress: %d sent (%s)", i + 1, counts)
    except (FileNotFoundError, OSError) as exc:
        logger.exception("failed while reading the dataset")
        logger.error("replay aborted after %s: %s", counts, exc)
        return 2

    logger.info("replay complete: %s", counts)
    return 0 if counts["error"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())