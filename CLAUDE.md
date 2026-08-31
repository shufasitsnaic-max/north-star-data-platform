# CLAUDE.md — north-star-data-platform

Operating manual for building this repo. This is **how we build**, not a restatement
of the spec. The full requirements live in the master spec (keep it at
`docs/capstone_blueprint.pdf`); refer to it, don't inline it. Note: the stack was
streamlined from the original blueprint — see `docs/DECISIONS.md` (2026-08-16 scope
revision) for what changed and why.

## Project

A decoupled **lambda-architecture** data platform over NYC TLC taxi trip data. A
validating gateway feeds a message bus that fans out to a real-time hot path and a
batch cold path, plus an ML layer (train / predict / evaluate) and a Streamlit
dashboard. Built **incrementally, one phase at a time**.

## Architecture (mental model)

```
simulator (replays historical TLC in pickup_datetime order)
   -> FastAPI gateway (validate -> adapt) -> Kafka (tlc-raw-events)
                                              |-- HOT:  Python consumer -> rolling windows -> PostgreSQL
                                              \-- COLD: Airflow -> Spark -> partitioned Snappy Parquet

   ML:        scikit-learn trains on <=cutoff (batch); predicts as >cutoff replays;
              Airflow runs a daily evaluation comparing predicted vs actual -> PostgreSQL
   Dashboard: Streamlit over PostgreSQL — live hot metrics, cold trends, pred-vs-actual, alerts
```

## Core principle: source-independent core

The internal event contract — the Pydantic model **and** the Kafka event schema — is
**canonical and source-agnostic**. All source-specific parsing lives behind an
**adapter** that maps raw input -> the canonical contract.

- Nothing downstream of Kafka may reference a specific data source.
- Swapping the source (e.g. TLC -> another taxi/weather source) should mean writing a
  new adapter and adjusting the schema — never a downstream rewrite.

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
├── simulator/        # replays historical TLC records -> gateway (drives "real-time")
├── hot_path/         # Kafka consumer -> rolling windows -> PostgreSQL
├── orchestration/    # Airflow DAGs (batch schedule + daily ML eval)
├── batch_jobs/       # Spark cold-path transforms -> partitioned Parquet
├── ml/               # scikit-learn train / predict / evaluate
└── dashboard/        # Streamlit app (hot metrics + cold trends + preds + alerts)
```

## Running & verifying

Each phase has a verification routine that **must pass before advancing**. Do not
scaffold future phases early.

- **P1 — Gateway** (done): `curl` a payload with corrupt types -> HTTP **422**, no crash.
- **P2 — Kafka + producer + simulator** (done): run the simulator; confirm events land on the
  topic via `kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic tlc-raw-events --from-beginning`.
- **P3 — Hot path** (done): `psql` row scans confirm rolling-window metrics update as the
  consumer processes replayed events.
- **P4 — Cold path** (done): `pyarrow.parquet.ParquetFile('/data/lake/...').metadata` asserts
  schema + `year=/month=/day=` partitions. Also verified: Airflow runs both DAGs unattended,
  and hot vs cold trip counts reconcile exactly on a single replay.
- **P5 — ML** (done): fare estimation at pickup. Trains on <=cutoff, scores post-cutoff
  events off the bus, and an Airflow DAG records daily predicted-vs-actual error. Now on
  `fare-hgb-3`, retrained over all 36 pre-cutoff months: normal-day MAE $4.30-$4.85, R2
  0.72-0.74, better than v2 on all eight evaluated days. New Year's Day R2 is still 0.047 —
  a real limit the evaluation caught on its own.
  **Two ordering rules, both learned the hard way:** evaluate the outgoing model *before*
  re-scoring (the re-score upserts in place and destroys its per-trip rows), and pause
  `ml_daily_eval` for the duration (it is on a 5-minute schedule).
- **P6 — Dashboard** (done): Streamlit shows live hot metrics, cold historical trends, a
  per-trip quoted-vs-charged feed, and anomaly alerts — refreshing as the simulator replays.
  Read-only by design: it owns no tables and writes nothing, which is why it filters the view
  and hands you the replay command rather than starting one. Verified in the browser against
  a running replay: feed and hot panels advance on their own, error bands colour correctly.
  Every panel honours one event-time range; the live feed can outrun its upper bound
  ("Follow live"), because a feed needing a page reload to show live data is not live.

## How we work together

- **Explain before generating.** Describe the approach first; keep each change small
  enough to review in one sitting. I'm building this to understand every part.
- **Phase discipline.** Follow the sequence strictly; don't advance until the current
  phase's verification passes.
- **Ask first** before adding a new dependency, service, or container.
- Prefer clarity over cleverness.
- **Update decision log.** At the end of each phase, before committing, append a dated
  entry to `docs/DECISIONS.md` recording decisions made and alternatives rejected —
  including implementation choices not discussed in chat.

## Don't

- **`curl` test payloads into the running gateway.** The topic is append-only and every
  derived store rebuilds from it, so a throwaway test event is permanent. Test adapters
  directly instead. See DECISIONS 2026-08-31.
- **Assume `git pull` deployed anything.** Source is baked into images; use
  `docker compose up -d --build`. A correct schema migration once sat unexecuted for weeks
  because the image predated it.
- Commit secrets or data (`.env`, `*.parquet`, raw datasets).
- Scaffold future phases ahead of time.
- Merge separate services into one container to save time.
- Reference a specific data source anywhere downstream of the adapter.
- Reintroduce Flink or a GPU serving layer (Triton/cuML) — see DECISIONS.md for why
  they were cut.
