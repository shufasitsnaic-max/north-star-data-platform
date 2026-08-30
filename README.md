# NYC TLC Data Platform

A decoupled **lambda-architecture** data platform processing the NYC Taxi & Limousine
Commission (TLC) Trip Record dataset. A validating gateway feeds a Kafka message bus
that fans out to a real-time hot path and a batch cold path, with an ML layer and a
live dashboard on top.

> Personal capstone / learning project — built incrementally, one phase at a time.

## Architecture at a glance

Historical TLC records are **replayed by a simulator** through a validating **FastAPI**
gateway, buffered in **Kafka**, and split into two paths from one topic:

- **Hot path** — a **Python Kafka consumer** computes rolling window aggregates and
  writes live metrics to **PostgreSQL**.
- **Cold path** — **Airflow** orchestrates **Spark** batch jobs that write
  date-partitioned, Snappy-compressed **Parquet** to a data lake.
- **ML** — a **scikit-learn** model trains on data up to a cutoff, predicts as later
  records replay, and is evaluated daily (predicted vs actual) via Airflow.
- **Dashboard** — a **Streamlit** app over PostgreSQL shows live metrics, historical
  trends, predictions-vs-actuals, and anomaly alerts.

## Tech stack

| Layer         | Tech                             |
| ------------- | -------------------------------- |
| Ingestion sim | Python replay simulator          |
| Gateway       | FastAPI + Pydantic               |
| Message bus   | Apache Kafka (KRaft, no Zookeeper) |
| Hot path      | Python Kafka consumer            |
| Hot storage   | PostgreSQL                       |
| Orchestration | Apache Airflow                   |
| Batch compute | Apache Spark (PySpark)           |
| Cold storage  | Parquet (Snappy, partitioned)    |
| ML            | scikit-learn                     |
| Dashboard     | Streamlit                        |
| Runtime       | Docker Compose                   |

## Project structure

```
.
├── docker-compose.yml
├── data_fetcher/     # on-demand TLC parquet downloader (not a running service)
├── gateway/          # FastAPI ingestion + Pydantic schemas + source adapter
├── simulator/        # replays historical TLC records -> gateway
├── hot_path/         # Kafka consumer -> rolling windows -> PostgreSQL
├── orchestration/    # Airflow DAGs (batch + daily ML eval)
├── batch_jobs/       # Spark cold-path transforms
├── ml/               # scikit-learn train / predict / evaluate
└── dashboard/        # Streamlit app
```

## Roadmap

Each phase must pass its verification routine before the next begins.

- [x] **Phase 1** — Validation gateway (FastAPI + Pydantic, 422 on bad input)
- [x] **Phase 2** — Kafka cluster + gateway producer + replay simulator (`tlc-raw-events`)
- [x] **Phase 3** — Hot path: Kafka consumer -> rolling metrics -> PostgreSQL
- [x] **Phase 4** — Cold path: Airflow + Spark -> partitioned Parquet, daily aggregates -> PostgreSQL
- [x] **Phase 5** — ML: fare estimation at pickup, scored live, evaluated daily by Airflow
- [x] **Phase 6** — Streamlit dashboard: live hot metrics, cold trends, per-trip quotes, alerts

## Getting started

**Prerequisites:** a Linux host with a working Docker daemon, plus
[uv](https://docs.astral.sh/uv/).

The repo ships a [dev container](.devcontainer/devcontainer.json), which is the
supported path — it provisions Docker and uv for you:

- **GitHub Codespaces** — *Code > Codespaces > Create codespace on master*.
- **Locally** — VS Code, *Dev Containers: Reopen in Container*.

Running on the host directly also works on Linux or macOS. On **Windows 11 Home**
it does not: Docker Desktop there supports only the WSL 2 backend (the Hyper-V
backend is Pro/Enterprise-only). Use a codespace instead — see
`docs/DECISIONS.md` for the full reasoning.

All commands below are run **inside** the dev container.

### 1. Fetch the data (once)

The TLC dataset is not in git. Download at least one month into the gitignored
`data/raw/`:

```bash
cd data_fetcher
uv run python fetch.py                                  # just 2023-01 (~48 MB)
uv run python fetch.py --start 2023-01 --end 2026-05    # full project scope (~2 GB)
```

Re-running skips months already downloaded, so an interrupted fetch resumes safely.
The project scope is 2023-01 .. 2026-05 with the train/replay cutoff at 2025-12-31 —
see `docs/DECISIONS.md` for why 2020–2022 is deliberately excluded.

### 2. Start the stack

```bash
docker compose up -d kafka gateway postgres hot_path
```

### 3. Replay records through it

The simulator sits behind a Compose profile, so it runs on demand rather than at
startup:

```bash
docker compose run --rm simulator                       # 5,000-record smoke test
docker compose run --rm -e MAX_ROWS=0 simulator         # the whole month
```

### 4. Confirm events reached the topic

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic tlc-raw-events --from-beginning --max-messages 5
```

### 5. Confirm the hot path is aggregating

Rolling window metrics should appear — and keep updating — as the replay proceeds:

```bash
docker compose exec postgres psql -U northstar -d northstar -c \
  "SELECT window_start, trip_count, avg_fare, is_final FROM window_metrics
   WHERE zone_id IS NULL ORDER BY window_start DESC LIMIT 5;"
```

The gateway alone can still be run standalone (Phase 1 style), though it now needs a
reachable broker:

```bash
cd gateway && uv sync && uv run uvicorn main:app
```

## Status

> **Picking this up again?** `docs/DECISIONS.md` opens with a **Resuming** section:
> current state, the one job left mid-flight, and the traps already paid for.

**All six phases built.** Validation gateway, Kafka + producer + replay simulator, hot
path -> PostgreSQL, the full cold path, the ML layer, and a Streamlit dashboard over
all of it. Phases 1–5 are verified end-to-end; the dashboard renders against live data
with a browser pass on its newest panels outstanding.

The dashboard is **read-only** — it owns no tables and writes nothing, reading three
tables written by three components that do not know it exists. It filters the view and
hands you the replay command rather than starting a replay itself, which keeps that
property intact.

**Phase 5 (ML) estimates what a ride will cost, before it starts** — the upfront-pricing
question a rider actually asks. It uses only what is known at pickup: the two zones and
the clock, never the distance driven or the meter reading. On normal days it lands
within **$4.36-$4.94** of the real price against **$3.91** in training. On New Year's Day
its R2 collapses to **0.025** — the daily evaluation caught a real model weakness, with a
nameable fix, on its first run.

**Phase 4 (cold path) is done.** Airflow runs both DAGs unattended — a one-shot backfill
that mapped 12 months of history into the lake, and a recurring pipeline that recomputes
the post-cutoff half from Kafka, rolls the whole lake up to daily x zone metrics, and
merges them into the serving store. The lake holds 428 Snappy Parquet files partitioned
`year=/month=/day=`; the serving table holds ~102k daily x zone rows.

The result worth pointing at is the **cross-layer reconciliation**. Hot and cold are two
independent code paths over the same events, and on a single clean replay their trip
counts agree *exactly*. Where they diverge — a window replayed twice — the hot path
over-counts duplicates it cannot dedupe while the cold path holds the true figure, which
is precisely the property a lambda architecture exists to provide.

The adapter's drift guard still runs with no cluster, no Kafka and no database:

```bash
cd batch_jobs && uv run pytest -v
```

The full design — including four infrastructure bugs worth reading before touching the
Spark or Airflow config, and the launcher-versus-runtime distinction that caused two of
them — is in `docs/DECISIONS.md`.

**Data scope:** 2023-01 .. 2026-05, with the train/replay cutoff at 2025-12-31. Everything
at or before the cutoff is backfilled from the raw files; everything after it arrives over
Kafka as the simulator replays it.
