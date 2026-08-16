"""On-demand TLC data fetcher.

Downloads NYC TLC *yellow* taxi monthly parquet files into a local, gitignored
directory (default: repo-root/data/raw/). Run it once to populate the data the
simulator replays; it is intentionally NOT a running service.

Design choices:
- Idempotent: a month already present (non-empty, valid parquet magic bytes) is
  skipped, so re-running only fetches what's missing ("download once").
- Resilient: each download retries with exponential backoff.
- Scoped: defaults to a single month so a first run is fast and small; pass a
  range to grab the full 2023-2025 project scope (see DECISIONS.md for why that
  window — post-COVID, excludes the 2020-2022 regime shift).

Usage (from inside data_fetcher/):
    uv run python fetch.py                        # just 2023-01
    uv run python fetch.py --start 2023-01 --end 2025-12   # full project scope
    uv run python fetch.py --start 2024-06 --end 2024-08 --out /some/dir
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

import httpx

logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger("data_fetcher")

# Official TLC distribution (CloudFront). Files are one parquet per month:
#   yellow_tripdata_YYYY-MM.parquet
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
FILENAME = "yellow_tripdata_{month}.parquet"

# Default output = repo-root/data/raw, resolved relative to THIS file so it
# works regardless of the caller's working directory.
DEFAULT_OUT = Path(__file__).resolve().parent.parent / "data" / "raw"

PARQUET_MAGIC = b"PAR1"  # every valid parquet file starts and ends with this


def month_range(start: str, end: str) -> list[str]:
    """Inclusive list of 'YYYY-MM' strings from start to end."""
    sy, sm = (int(p) for p in start.split("-"))
    ey, em = (int(p) for p in end.split("-"))
    if (ey, em) < (sy, sm):
        raise ValueError(f"end {end} is before start {start}")
    months: list[str] = []
    y, m = sy, sm
    while (y, m) <= (ey, em):
        months.append(f"{y:04d}-{m:02d}")
        m += 1
        if m > 12:
            y, m = y + 1, 1
    return months


def _looks_like_parquet(path: Path) -> bool:
    """Cheap integrity check: valid parquet starts with the PAR1 magic bytes."""
    try:
        with path.open("rb") as fh:
            return fh.read(4) == PARQUET_MAGIC
    except OSError:
        return False


def download_month(month: str, out_dir: Path, max_attempts: int = 5) -> bool:
    """Download one month's parquet with backoff. Returns True if a file ended
    up present (freshly downloaded or already there); False on give-up."""
    dest = out_dir / FILENAME.format(month=month)

    # Idempotent skip: already have a non-empty, valid-looking file.
    if dest.exists() and dest.stat().st_size > 0 and _looks_like_parquet(dest):
        logger.info("%s already present (%.1f MB) — skipping", dest.name, dest.stat().st_size / 1e6)
        return True

    url = f"{BASE_URL}/{FILENAME.format(month=month)}"
    tmp = dest.with_suffix(".parquet.part")  # download to a temp path, rename on success
    delay = 1.0
    for attempt in range(1, max_attempts + 1):
        try:
            logger.info("downloading %s (attempt %d/%d)", url, attempt, max_attempts)
            with httpx.stream("GET", url, timeout=60.0, follow_redirects=True) as resp:
                resp.raise_for_status()
                with tmp.open("wb") as fh:
                    for chunk in resp.iter_bytes(chunk_size=1 << 20):  # 1 MiB chunks
                        fh.write(chunk)
            if not _looks_like_parquet(tmp):
                raise ValueError("downloaded file is not a valid parquet (bad magic bytes)")
            tmp.rename(dest)
            logger.info("saved %s (%.1f MB)", dest.name, dest.stat().st_size / 1e6)
            return True
        except (httpx.HTTPError, ValueError, OSError) as exc:
            logger.warning("failed %s (attempt %d/%d): %s", month, attempt, max_attempts, exc)
            tmp.unlink(missing_ok=True)
            if attempt < max_attempts:
                time.sleep(delay)
                delay = min(delay * 2, 30)
    logger.error("giving up on %s after %d attempts", month, max_attempts)
    return False


def main() -> int:
    parser = argparse.ArgumentParser(description="Download TLC yellow-taxi monthly parquet files.")
    parser.add_argument("--start", default="2023-01", help="first month, YYYY-MM (default 2023-01)")
    parser.add_argument("--end", default=None, help="last month, YYYY-MM (default = --start)")
    parser.add_argument("--out", default=str(DEFAULT_OUT), help=f"output dir (default {DEFAULT_OUT})")
    args = parser.parse_args()

    end = args.end or args.start
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    try:
        months = month_range(args.start, end)
    except ValueError as exc:
        logger.error("bad range: %s", exc)
        return 2

    logger.info("fetching %d month(s) [%s .. %s] into %s", len(months), args.start, end, out_dir)
    failures = [m for m in months if not download_month(m, out_dir)]
    if failures:
        logger.error("done with %d failure(s): %s", len(failures), ", ".join(failures))
        return 1
    logger.info("done — %d month(s) available in %s", len(months), out_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())