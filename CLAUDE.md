# CLAUDE.md — north-star-data-platform

Operating manual for building this repo. This is **how we build**, not a restatement
of the spec. The full requirements live in the master spec (keep it at
`docs/capstone_blueprint.pdf`); refer to it, don't inline it.

## Project

A decoupled, production-grade **lambda-architecture** data platform over taxi trip
data. A validating gateway feeds a message bus that splits into a real-time hot path
and a batch cold path, plus an ML serving layer. Built **incrementally, one phase at a
time** — see the phase sequence in the spec (§3).

## Architecture (mental model)

```
ingest → FastAPI gateway (validate) → Kafka (tlc-raw-events)
                                        ├── HOT:  Flink windowed aggregates → PostgreSQL
                                        ├── COLD: Airflow → Spark → partitioned Snappy Parquet → dbt star schema
                                        └── ML:   cuML/Triton serving (scikit-learn CPU fallback), shadow-mirrored from gateway
```

## Core principle: source-independent core

The internal event contract — the Pydantic model **and** the Kafka event schema — is
**canonical and source-agnostic**. All source-specific parsing lives behind an
**adapter** that maps raw input → the canonical contract.

- Nothing downstream of Kafka may reference a specific data source.
- Swapping the source (e.g. TLC files → Singapore LTA taxi API + weather API) should
  mean writing a new adapter and adjusting the schema — never a downstream rewrite.

## Ground rules (non-negotiable)

- **Modular, not monolithic.** One concern per module. Never combine multi-tier logic
  in a single pass or a single container.
- **Explicit persistence.** Every persistence target (PostgreSQL, Airflow metadata,
  Parquet lake) gets an explicit Docker volume mount. Never rely on ephemeral container
  state for data.
- **Error handling.** Wrap risky logic in `try/except`; log stack traces to stderr.
- **Resilient connections.** Inter-service connections (Kafka, DB, etc.) use
  exponential-backoff retry before failing structurally.
- **Explicit dependencies.** Each component declares its own dependencies. Python deps
  are managed per-component with `uv` (`pyproject.toml` + `uv.lock`); no
  `requirements.txt`, no shared environment.
- **Commented config.** Configuration files carry descriptive comments.

## Directory structure

```
.
├── docker-compose.yml
├── gateway/          # FastAPI ingestion + Pydantic schemas + source adapter
├── streaming/        # Flink hot-path job
├── orchestration/    # Airflow DAGs
├── batch_jobs/       # Spark cold-path transforms
├── transformations/  # dbt project + models
└── ml_ops/           # training + Triton config
```

## Running & verifying

Each phase has a verification routine that **must pass before advancing**. Do not
scaffold future phases early.

- **P1 — Gateway:** `curl` a payload with corrupt types → expect HTTP **422**, no crash.
- **P2 — Kafka:** `kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic tlc-raw-events --from-beginning`.
- **P3 — Hot path:** `psql` row scans confirm rolling-window metrics update as events arrive.
- **P4 — Cold path:** `pyarrow.parquet.ParquetFile('/data/lake/...').metadata` asserts schema + `year=/month=/day=` partitions.
- **P5 — dbt:** `dbt test` → zero primary-key / null violations.
- **P6 — Serving:** inference telemetry; prediction errors must not propagate upstream or stall ingestion.

## How we work together

- **Explain before generating.** Describe the approach first; keep each change small
  enough to review in one sitting. I'm building this to understand every part.
- **Phase discipline.** Follow the sequence strictly; don't advance until the current
  phase's verification passes.
- **Ask first** before adding a new dependency, service, or container.
- Prefer clarity over cleverness.
- **Update decision log** At the end of each phase, before committing, append a dated entry to docs/DECISIONS.md recording decisions made and alternatives rejected —
  including implementation choices not discussed in chat.

## Don't

- Commit secrets or data (`.env`, `*.parquet`, raw datasets).
- Scaffold future phases ahead of time.
- Merge separate services into one container to save time.
- Reference a specific data source anywhere downstream of the adapter.
