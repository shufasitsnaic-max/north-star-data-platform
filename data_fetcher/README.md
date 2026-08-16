# data_fetcher

On-demand utility that downloads NYC TLC **yellow** taxi monthly parquet files
into the gitignored `data/raw/` directory the simulator replays from. It is
**not** a running service — run it by hand, once, to populate data.

```bash
cd data_fetcher
uv sync

uv run python fetch.py                                  # just 2023-01 (small, fast first run)
uv run python fetch.py --start 2023-01 --end 2025-12    # full project scope (~1.5 GB, 36 files)
uv run python fetch.py --start 2024-06 --end 2024-08    # a specific window
```

Files land in `data/raw/yellow_tripdata_YYYY-MM.parquet`. Re-running skips
months already downloaded, so it's safe to resume an interrupted fetch.

**Scope:** the project uses 2023–2025 only (post-COVID). See `docs/DECISIONS.md`
for why 2020–2022 is excluded.