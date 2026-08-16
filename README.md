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
- [ ] **Phase 2** — Kafka cluster + gateway producer + replay simulator (`tlc-raw-events`)
- [ ] **Phase 3** — Hot path: Kafka consumer -> rolling metrics -> PostgreSQL
- [ ] **Phase 4** — Cold path: Airflow + Spark -> partitioned Parquet
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
uv run python fetch.py --start 2023-01 --end 2025-12    # full project scope
```

Re-running skips months already downloaded.

### 2. Start the stack

```bash
docker compose up -d kafka gateway
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

The gateway alone can still be run standalone (Phase 1 style), though it now needs a
reachable broker:

```bash
cd gateway && uv sync && uv run uvicorn main:app
```

## Status

Phase 1 complete. Phase 2 (Kafka + producer + simulator) built; verification pending.
