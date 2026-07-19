# NYC TLC Data Platform

A decoupled, production-grade **lambda-architecture** data platform processing the
NYC Taxi & Limousine Commission (TLC) Trip Record dataset. Streaming (hot path) and
batch (cold path) pipelines run as independent, containerized services.

> Personal capstone / learning project — built incrementally, one phase at a time.

## Architecture at a glance

Ingestion enters through a validating **FastAPI** gateway, buffers in **Kafka**, and
splits into two paths:

- **Hot path** — **Flink** computes rolling window aggregates and sinks live metrics to **PostgreSQL**.
- **Cold path** — **Airflow** orchestrates **Spark** batch jobs that write date-partitioned,
  Snappy-compressed **Parquet** to a data lake, modeled into a star schema with **dbt**.
- **MLOps** — **cuML/Triton** serve predictions (with a Scikit-learn CPU fallback), mirrored
  as a shadow model from the gateway.

## Tech stack

| Layer        | Tech                          |
| ------------ | ----------------------------- |
| Gateway      | FastAPI + Pydantic            |
| Message bus  | Apache Kafka (+ Zookeeper)    |
| Stream       | Apache Flink (PyFlink)        |
| Hot storage  | PostgreSQL                    |
| Orchestration| Apache Airflow                |
| Batch compute| Apache Spark (PySpark)        |
| Cold storage | Parquet (Snappy, partitioned) |
| Modeling     | dbt Core                      |
| Serving      | cuML / Triton (+ scikit-learn)|
| Runtime      | Docker Compose                |

## Project structure

```
.
├── docker-compose.yml
├── gateway/          # FastAPI ingestion + Pydantic schemas
├── streaming/        # Flink hot-path job
├── orchestration/    # Airflow DAGs
├── batch_jobs/       # Spark cold-path transforms
├── transformations/  # dbt project + models
└── ml_ops/           # training + Triton config
```

## Roadmap

Each phase must pass its verification routine before the next begins.

- [ ] **Phase 1** — Validation gateway (FastAPI + Pydantic, 422 on bad input)
- [ ] **Phase 2** — Kafka cluster + gateway producer (`tlc-raw-events`)
- [ ] **Phase 3** — Flink → Postgres hot path (rolling window metrics)
- [ ] **Phase 4** — Airflow + Spark → partitioned Parquet cold path
- [ ] **Phase 5** — dbt models, tests, and lineage docs
- [ ] **Phase 6** — cuML/Triton shadow-model serving

## Getting started

_TODO: fill in once Phase 1 lands._

```bash
# docker compose up --build   # (coming soon)
```

## Status

🚧 Phase 1 in progress.
