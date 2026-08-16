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
- [ ] **Phase 4** — Cold path: Airflow + Spark -> partitioned Parquet *(in progress — step 1
      of 6 built; full design and a resume checklist are in `docs/DECISIONS.md`)*
- [ ] **Phase 5** — ML: train / predict / daily evaluation (Airflow-scheduled)
- [ ] **Phase 6** — Streamlit dashboard (hot + cold + preds + alerts)

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

Phases 1–3 complete and verified end-to-end (validation gateway, Kafka + producer +
replay simulator, hot path -> PostgreSQL).

**Phase 4 (cold path) is in progress.** Step 1 of 6 is built: `batch_jobs/` holds the
canonical Spark schema, a vectorized TLC adapter, and a conformance test that pins that
adapter to the gateway's own output. It needs no cluster, no Kafka and no database:

```bash
cd batch_jobs && uv run pytest -v
```

Still to come: the bulk loader and Spark cluster, the Kafka -> lake job, daily
aggregates, and the Airflow DAGs. The full design — with rejected alternatives and a
resume checklist — is in `docs/DECISIONS.md`.

**Data scope:** 2023-01 .. 2026-05, with the train/replay cutoff at 2025-12-31. Everything
at or before the cutoff is backfilled from the raw files; everything after it arrives over
Kafka as the simulator replays it.
