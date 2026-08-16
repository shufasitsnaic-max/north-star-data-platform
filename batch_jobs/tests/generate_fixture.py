"""Builds the conformance fixture: raw TLC rows paired with the gateway's output.

Run this once, against a live stack, then commit the result. The test that
consumes it never imports gateway code — it compares against *recorded
behaviour*, so the two adapters stay coupled by observation rather than by a
build-time dependency (the same reasoning that made hot_path a tolerant reader
instead of sharing a contracts package).

The fixture records two kinds of example, and both matter:

  accepted — a raw row and the canonical event the gateway published for it.
             Proves the value mapping agrees.
  rejected — a raw row the gateway refused (it never reached the topic).
             Proves the *accept/reject boundary* agrees, which a
             values-only fixture would miss entirely.

Rejected rows are derived, not guessed: we sample the first N raw rows in the
simulator's replay order and consume rather more than N canonical events, so any
sampled row without a match was necessarily refused rather than merely not yet
sent.

--------------------------------------------------------------------------
Usage (inside the codespace, with kafka + gateway + a replay already done):

  # 1. capture what the gateway actually published
  docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
      --bootstrap-server localhost:9092 --topic tlc-raw-events \
      --from-beginning --max-messages 3000 > /tmp/canonical.jsonl

  # 2. pair it against the raw file that produced it
  cd batch_jobs
  uv run python tests/generate_fixture.py \
      --raw ../data/raw/yellow_tripdata_2023-01.parquet \
      --canonical /tmp/canonical.jsonl \
      --rows 500

Note the replay that filled the topic must have started at the beginning of the
same month, otherwise the sampled prefix and the captured events don't overlap
and the script will say so rather than emit a misleading fixture.
--------------------------------------------------------------------------
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import sys
from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

import pyarrow.compute as pc
import pyarrow.parquet as pq

logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger("generate_fixture")

# The raw TLC columns the adapter consumes, in the gateway's spelling.
FIELDS = [
    "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
    "RatecodeID", "store_and_fwd_flag", "PULocationID", "DOLocationID", "payment_type",
    "trip_distance", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
    "improvement_surcharge", "total_amount", "congestion_surcharge", "airport_fee",
]
PICKUP = "tpep_pickup_datetime"
DROPOFF = "tpep_dropoff_datetime"
DEFAULT_OUT = Path(__file__).resolve().parent / "fixtures" / "conformance.json"


def month_of(path: Path) -> str:
    """'…/yellow_tripdata_2023-01.parquet' -> '2023-01'."""
    return path.stem.replace("yellow_tripdata_", "")


def month_bounds(month: str) -> tuple[datetime, datetime]:
    year, month_number = (int(part) for part in month.split("-"))
    start = datetime(year, month_number, 1)
    end = datetime(year + 1, 1, 1) if month_number == 12 else datetime(year, month_number + 1, 1)
    return start, end


def load_raw_prefix(path: Path, rows: int) -> list[dict]:
    """The first `rows` records in replay order, cleaned only enough to be JSON.

    Deliberately does NOT coerce TLC's float-coded integers back to int: the
    batch adapter is responsible for that narrowing, so the fixture must present
    the values exactly as the parquet holds them or the test would skip the very
    path it exists to check.
    """
    table = pq.read_table(path)
    lower_to_actual = {name.lower(): name for name in table.column_names}
    present = [(f, lower_to_actual[f.lower()]) for f in FIELDS if f.lower() in lower_to_actual]
    table = table.select([actual for _, actual in present])
    table = table.rename_columns([canonical for canonical, _ in present])

    # Same out-of-month drop the simulator applies, so the ordering here matches
    # the ordering that produced the events on the topic.
    start, end = month_bounds(month_of(path))
    table = table.filter(
        pc.greater_equal(pc.field(PICKUP), pc.scalar(start))
        & pc.less(pc.field(PICKUP), pc.scalar(end))
    )
    table = table.sort_by([(PICKUP, "ascending")])

    cleaned: list[dict] = []
    for row in table.slice(0, rows).to_pylist():
        out = {}
        for key, value in row.items():
            if isinstance(value, datetime):
                out[key] = value.isoformat()
            elif isinstance(value, float) and math.isnan(value):
                out[key] = None  # NaN is not JSON; the gateway sees it as missing
            else:
                out[key] = value
        cleaned.append(out)
    return cleaned


def _money_key(value) -> str:
    """Normalize a fare to 2dp so a float and a Decimal string compare equal.

    HALF_UP to match Spark's decimal cast, so pairing here and comparison in the
    test agree on the same tie-break rule.
    """
    return str(Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def _time_key(value: str) -> str:
    """'2023-01-01T00:32:10' and '2023-01-01 00:32:10' are the same instant."""
    return datetime.fromisoformat(value).strftime("%Y-%m-%d %H:%M:%S")


def raw_key(row: dict) -> tuple:
    return (
        _time_key(row[PICKUP]),
        _time_key(row[DROPOFF]),
        int(row["PULocationID"]),
        int(row["DOLocationID"]),
        _money_key(row["total_amount"]),
        int(row["VendorID"]),
    )


def canonical_key(event: dict) -> tuple:
    return (
        _time_key(event["pickup_datetime"]),
        _time_key(event["dropoff_datetime"]),
        int(event["pickup_location"]["zone_id"]),
        int(event["dropoff_location"]["zone_id"]),
        _money_key(event["total_amount"]),
        int(event["provider_id"]),
    )


def load_canonical(path: Path) -> list[dict]:
    """Read the JSON-lines dump straight off kafka-console-consumer."""
    events = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("Processed a total of"):
            continue  # the consumer's own trailer line
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            logger.warning("skipping unparseable line: %.80s", line)
    return events


def main() -> int:
    parser = argparse.ArgumentParser(description="Build the adapter conformance fixture.")
    parser.add_argument("--raw", required=True, help="one TLC monthly parquet file")
    parser.add_argument("--canonical", required=True, help="JSONL dump of the topic")
    parser.add_argument("--rows", type=int, default=500, help="raw rows to sample")
    parser.add_argument("--out", default=str(DEFAULT_OUT))
    args = parser.parse_args()

    raw_path = Path(args.raw)
    raw_rows = load_raw_prefix(raw_path, args.rows)
    events = load_canonical(Path(args.canonical))
    logger.info("sampled %d raw row(s); captured %d canonical event(s)", len(raw_rows), len(events))

    if len(events) <= len(raw_rows):
        logger.error(
            "captured %d events for %d sampled rows — capture strictly more events than "
            "rows sampled, or an unmatched row can't be distinguished from an unsent one",
            len(events), len(raw_rows),
        )
        return 2

    # Ambiguous keys (genuinely identical trips) are dropped from both sides
    # rather than risk pairing a row with the wrong event.
    by_key: dict[tuple, dict] = {}
    ambiguous: set[tuple] = set()
    for event in events:
        key = canonical_key(event)
        if key in by_key:
            ambiguous.add(key)
        by_key[key] = event

    accepted, rejected, skipped = [], [], 0
    for row in raw_rows:
        key = raw_key(row)
        if key in ambiguous:
            skipped += 1
            continue
        event = by_key.get(key)
        if event is None:
            rejected.append(row)
        else:
            accepted.append({"raw": row, "canonical": event})

    if skipped:
        logger.info("skipped %d row(s) whose natural key was not unique", skipped)
    if not accepted:
        logger.error(
            "no raw row matched any captured event — the replay that filled the topic "
            "probably started somewhere other than the beginning of %s", month_of(raw_path)
        )
        return 2

    fixture = {
        "_comment": (
            "Golden fixture for the batch adapter. 'accepted' pairs a raw TLC row with the "
            "canonical event the gateway published for it; 'rejected' holds raw rows the "
            "gateway refused. Regenerate with tests/generate_fixture.py."
        ),
        "source_file": raw_path.name,
        "accepted": accepted,
        "rejected": rejected,
    }

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(fixture, indent=1), encoding="utf-8")
    logger.info(
        "wrote %s — %d accepted pair(s), %d rejected row(s) (%.1f%% rejected), %.0f KB",
        out_path, len(accepted), len(rejected),
        100.0 * len(rejected) / max(len(accepted) + len(rejected), 1),
        out_path.stat().st_size / 1024,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
