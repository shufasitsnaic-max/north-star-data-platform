# Decisions Log

A running record of design decisions, one section per phase, plus a standing list of
decisions deferred to future phases. Newest phase at the bottom.

**Purpose:** the Claude Code chat is ephemeral — this file is where the *reasoning*
survives between sessions. Git history says what changed; this says **why**, and what
we chose *not* to do (yet). At the end of each phase, record: what was decided, why,
and anything left open.

**How to keep it current:** at each stopping point, ask Claude Code to append the
implementation decisions it made that aren't captured here (library choices, validator
details, config defaults) — then review the entry before committing. This doc is only
useful if it's curated, not a dump.

---

## Phase 1 — Validation Gateway (FastAPI)  · 2026-07-19

**Status:** complete. All P1 verification cases pass (valid -> 200, corrupt types -> 422,
dropoff-before-pickup -> 422).

### Architecture
- **Two-model adapter pattern.** `TLCTripInput` (mirrors the raw TLC yellow-cab record)
  -> `adapt_tlc()` (pure transform, the only source-specific piece) -> `TripEvent`
  (canonical, source-agnostic contract). Everything downstream of the adapter sees only
  `TripEvent`. This is the seam that makes a future source swap contained.
- **Endpoints:** `POST /events/trips` (validate -> adapt -> return canonical event) and
  `GET /health`. For Phase 1 the endpoint returns the adapted `TripEvent` directly, since
  Kafka doesn't exist yet — lets us verify the full pipeline with curl.

### Schema decisions
- **Location — `zone_id` canonical, `lat`/`lon` optional ("at least one of").**
  Rejected lat/lon-only: modern TLC has no GPS, only 263 zone IDs, so forcing lat/lon
  would mean geocoding zone centroids — inventing precision the source never had and
  losing the honest `zone_id`.
- **Surcharges — collapse to `surcharges_amount`, but preserve detail in `source_extras`.**
  Canonical model exposes the lump sum; itemized NYC surcharges, `ratecode_id`, and
  `store_and_fwd_flag` are kept in a `source_extras` passthrough dict for audit and — 
  importantly — as candidate ML features later (`ratecode_id` flags airport/flat-fare
  trips that likely tip differently).
- **Units — `trip_distance_km`, converted from TLC's miles in the adapter.** Source-neutral
  unit chosen now to avoid a silent bug when a km-native source is added.
- **`payment_type`** — generalized enum (CASH | CARD | OTHER | UNKNOWN), defaults to
  UNKNOWN, from TLC's numeric codes.
- **Event time — `pickup_datetime` is non-nullable and never defaulted to `now()`.**
  This is the record's own event time, which becomes the basis for hot-path windowing.
  Deliberate: replaying historical data means event time != arrival time.
- Semantic checks (dropoff > pickup, non-negative amounts) live as Pydantic validators on
  `TLCTripInput`, so they surface as 422s rather than producing nonsense canonical events.

### Ingestion shape
- **Single-event POST.** One request = one `TripEvent` = one future Kafka message. Replay
  looping lives *outside* the gateway. Rejected a batch/array endpoint — it would blur the
  one-event-one-message invariant. If replay throughput bites later, batch on the Kafka
  *producer* side, not the HTTP contract.
- The gateway stays a pure validate->adapt boundary — no awareness of replay, batching, or
  historical-vs-live.

### Tooling
- **uv per component.** `gateway/` is its own uv project (`pyproject.toml` + `uv.lock`),
  `uv init --bare`. No `requirements.txt`, no shared mega-environment. Because gateway is
  its own project root, imports have no `gateway.` prefix; run from inside `gateway/`.
- **Run / verify locally:** `cd gateway && uv sync && uv run uvicorn main:app`.

---

## Scope revision — streamline for the course rubric  · 2026-08-16

**Context.** The original blueprint (Triton/cuML serving, Flink streaming) was shaped by
an industry-attachment angle (Grab, interested in GPU serving) that is no longer
happening due to a legal fallout between the school and the company. Retargeting the
project purely at the course rubric, while **keeping the TLC direction and the
hot+cold lambda demonstration** (the more interesting original goal). Priorities now:
ship fast, and maximize marks on a rubric that weights End-to-End Pipeline (30),
ML + Real-Time Output incl. a dashboard (30), Presentation (30), Robustness (10).

### Cut: Triton / cuML serving layer
A GPU inference server earns none of the marks the rubric asks for; "model training and
inference" is fully satisfied by a **scikit-learn** model. Triton was also the
hardest-to-operate piece. Removing it costs zero rubric marks and saves significant time.

### Swap: Flink -> plain Python Kafka consumer (hot path)
Rejected keeping Flink. Reasons: (1) it is the most painful component to containerize
(JVM/PyFlink friction); (2) "real-time processing" rubric credit is **identical** for a
consumer that reads the topic, keeps rolling windows, and upserts aggregates to Postgres;
(3) explainability matters — Presentation is 30 marks and a component we fully understand
beats an impressive black box in a 5-minute video. **Preserved from Flink thinking:**
event-time windowing on `pickup_datetime` (the simulator can replay faster than
real time, so windows must key off event time, not wall clock).

### Added: Streamlit dashboard
The rubric's 30-mark "ML and Real-Time Output" explicitly requires a live/regularly
refreshed dashboard; the original blueprint had none — this was the biggest gap. Streamlit
over Postgres is the fastest route and doubles as the visual proof of both layers in the
video (live hot metrics beside cold historical trends, plus predicted-vs-actual and
anomaly alerts).

### Made optional: dbt
Spark can do the transformations directly. dbt's real draw is `dbt test` for cheap
data-quality marks — so it's a time-permitting add (Phase 7), not a blocking phase.

### Kept (all named in the rubric)
Kafka + Docker Compose (message queue, "which services start together"), PostgreSQL (hot
store), Spark (distributed processing), Airflow (orchestration + daily ML eval). The
replay **simulator** stays a separate component outside the gateway — it *is* the
"real-time" input, and a controlled replay is a better demo than live data.

### Streamlined phase order (P1 done)
- **P2** — Kafka + gateway producer + replay simulator
- **P3** — hot-path consumer -> Postgres
- **P4** — Airflow + Spark -> partitioned Parquet
- **P5** — ML train / predict / daily evaluation (Airflow-scheduled)
- **P6** — Streamlit dashboard (hot + cold + preds + alerts)
- **P7** *(optional)* — dbt tests

---

## Cut: dbt (former P7) dropped entirely  · 2026-08-16

Superseding the "Made optional" call above: **dbt is cut, not deferred.** With an
expedited timeline, the optional data-quality marks `dbt test` would earn don't justify
standing up a dbt project, and Spark already does the cold-path transformations directly.
Removing it means the roadmap now ends at **P6 (dashboard)**; the `transformations/`
directory will not be created. Rejected keeping it as a stretch goal — a "maybe later"
phase invites half-finished scaffolding, which CLAUDE.md explicitly forbids. If
data-quality checks are wanted later, the cheaper route is a few assertions in the Spark
job or a Postgres constraint, not a new tool.

---

## Phase 2 — Kafka + producer + replay simulator  · 2026-08-16

**Status:** code complete, **verification pending** — the end-to-end run (Compose up ->
simulator -> console-consumer) has not been executed yet because Docker was not installed
on the dev machine. Everything below the Docker line *has* been verified locally.

### Kafka topology
- **Single-node Kafka in KRaft mode, no Zookeeper.** One container instead of two, and
  Zookeeper is deprecated upstream. Rejected the Confluent images in favour of the
  official `apache/kafka` — fewer vendor env-var conventions to learn.
- **Two listeners, deliberately.** `PLAINTEXT://kafka:29092` for in-network clients and
  `PLAINTEXT_HOST://localhost:9092` for tools run on the host. A broker can only
  advertise one address per listener, and `localhost` is wrong inside the network while
  `kafka` is unresolvable outside it — hence the split rather than a single port.
- **One partition (`KAFKA_NUM_PARTITIONS: 1`).** Kafka only guarantees ordering *within*
  a partition, and the simulator's whole purpose is to emit in `pickup_datetime` order.
  More partitions would shard that ordering away before the hot path ever sees it.
  Revisit only if throughput demands it — parallelism is not a Phase 3 problem.
- **Auto-create topics enabled.** `tlc-raw-events` springs into existence on first
  produce. Rejected a separate topic-creation init container as ceremony for one topic.

### Producer
- **`confluent-kafka` over `kafka-python`.** librdkafka-backed, materially faster, and
  the only one of the two with a maintained idempotent-producer path.
- **Idempotence + `acks=all`.** Retries cannot duplicate records. Cheap here because
  throughput is a replay, not a firehose.
- **Async produce, 202 response.** `publish()` enqueues and returns; a background thread
  ships batches (`linger.ms=50`). The endpoint answers **202 Accepted**, not 200 — the
  event is *accepted for publication*, not confirmed on disk. Rejected blocking on
  delivery per request: it would serialize the replay behind broker round-trips for no
  correctness gain, given idempotence plus a shutdown `flush()`.
- **Keyed by pickup `zone_id`.** Irrelevant with one partition, but it means adding
  partitions later keeps a zone's trips ordered and colocated — which is the grouping
  the hot path aggregates by anyway.

### Simulator
- **Speaks HTTP only.** It knows the gateway's endpoint and nothing else — not Kafka,
  not the canonical schema. Rejected producing to Kafka directly: that would route
  around validation, which is the gateway's entire reason to exist.
- **Sends TLC-shaped payloads.** Raw field names and units go over the wire; the
  adapter stays the single translation point.
- **Global sort before replay,** not per-file. With multiple months loaded, per-file
  ordering would emit February's early trips after January's late ones.
- **Case-insensitive column resolution.** TLC renamed `airport_fee` -> `Airport_fee`
  partway through the dataset's life; resolving via a lowercased lookup absorbs that
  instead of erroring on whichever month disagrees.
- **`promote_options="permissive"`** when concatenating months, so a month missing an
  optional column null-fills rather than failing the whole load.

### Out-of-month timestamp filter (found during verification prep)
`yellow_tripdata_2023-01.parquet` contains **48 records whose `pickup_datetime` falls
outside January 2023** — 2 from 2008, 36 from 2022, 10 from February. Only 0.0016% of
3.07M rows, but the ascending sort piles all 38 pre-2023 rows at the *front* of the
replay, so a capped smoke test replays mostly garbage and the stream opens on a 2008
timestamp.

- **Decision: the simulator drops rows whose pickup falls outside the span of the months
  it loaded**, trusting the filename over the field, and logs the drop count.
- **Why it matters beyond cosmetics:** Phase 3 windows on event time. Unfiltered, the
  consumer would open its first window in 2008 and traverse fourteen years of empty
  ground before reaching live data.
- **Why the simulator and not the gateway.** "A pickup must fall inside the month whose
  file shipped it" is source-specific knowledge — it depends on TLC's one-file-per-month
  packaging. The gateway is source-agnostic past the adapter and has no business
  knowing it. Rejected a plausible-date-range validator on the canonical model for the
  same reason: it would hardcode a policy every future source would inherit.
- **Rejected dropping them silently** — the count is logged, per the no-silent-caps rule.

### Verified locally (no Docker required)
Ran the real `replay.py` functions over the real parquet, feeding each cleaned row
through `TLCTripInput` -> `adapt_tlc`:
- 3,066,766 rows load; 48 dropped by the filter; remainder sorted ascending from
  `2023-01-01T00:00:00`.
- Of the first 5,000 records, **4,956 accepted (99.1%)**, 44 rejected — all negative
  `fare_amount`/`total_amount`, i.e. TLC refunds and voided trips that the canonical
  model's `ge=0` constraints correctly refuse. **A ~0.9% rejection rate on real data is
  expected, not a bug**; the simulator counts 422s and continues.
- No JSON serialization leaks (NaN -> null, datetimes -> ISO 8601, float-coded ints
  narrowed back to int).

### Implementation notes
- `pc.and_` does **not** exist in Arrow's Acero expression engine — compound filters use
  the `&` operator on `Expression` objects. Cost a failed run to discover.
- Both service images install with `uv sync --no-dev` off `pyproject.toml` + `uv.lock`,
  copying manifests before source so dependency layers cache across code edits.
- The gateway healthcheck uses `urllib` rather than `curl`, which `python:3.11-slim`
  does not ship.
- **Simulator is one-shot, behind a `replay` Compose profile** — `docker compose up`
  starts only the long-running services; a replay is triggered explicitly with
  `docker compose run --rm simulator`. Rejected making it a restarting service: a replay
  is an event you trigger, not a daemon.
- **`./data` is mounted read-only** into the simulator. It only ever reads.

### Open
- Verification run itself, once a working Docker daemon exists — see the
  dev-environment entry below, which is what unblocks it.
- Only 2023-01 is downloaded so far; the full 2023–2025 scope is a longer fetch.

---

## Dev environment — move to a Linux dev container  · 2026-08-16

**Context.** Phase 2 was code-complete but unverifiable: the dev machine could not run
Docker at all. Diagnosis, in order of what actually blocks:

1. **Windows 11 _Home_.** Docker Desktop on Home offers **only** the WSL 2 backend; the
   Hyper-V backend is Pro/Enterprise-only. So "Docker Desktop without WSL" is not a
   configuration that exists on this edition.
2. **WSL 2 never installed.** The MSIX app package was present (v2.7.11.0), but
   `WslService`, `LxssManager`, and `vmcompute` were all unregistered — i.e. the
   *Virtual Machine Platform* and *Windows Subsystem for Linux* optional features never
   got enabled. `wsl --status` returned `Wsl/ERROR_SERVICE_DOES_NOT_EXIST`.
3. **The install attempts were bugchecking the machine.** Five BSODs on record, three of
   them clustered inside the install window: `0x3B` ×2, `0x139`, `0x1`, and a `0x20001`
   HYPERVISOR_ERROR. Varied stop codes with generic access violations, plus the
   hypervisor itself faulting, points at faulty RAM or a bad driver — not a WSL bug.

**Decision: develop and run the stack in a Linux dev container (GitHub Codespaces),**
committed as `.devcontainer/devcontainer.json`. Local virtualization is removed from the
critical path entirely, and the blueprint survives intact — Kafka, Postgres, Spark, and
Airflow all run under Compose exactly as designed.

### Rejected alternatives
- **Repair WSL 2 in place** (disable Memory Integrity, enable the two features via DISM
  rather than `wsl --install`, reboot). Plausible — the features genuinely never enabled,
  so the install never got a fair attempt — but a HYPERVISOR_ERROR already in the log
  makes it roughly a coin flip, and each failed attempt costs a crash on the machine the
  work lives on. Not worth gating a capstone on. Still available as a fallback.
- **A local Linux VM (VirtualBox/VMware).** Leans on the same virtualization stack that is
  already crashing, and Windows' hypervisor is running, which forces VirtualBox into slow
  paravirtualized fallback. Strictly worse than a remote host.
- **Native Windows, no Docker.** The tempting one, and the reason it's recorded here:
  Kafka (KRaft, `.bat` scripts), PostgreSQL, FastAPI, and Streamlit *do* all run natively
  on Windows, so P2/P3/P6 were reachable. **Airflow is the blocker — it has no Windows
  support at all**, requiring POSIX. Taking this path meant swapping the orchestrator for
  Prefect or a hand-rolled scheduler, i.e. losing a component the rubric names outright,
  and abandoning CLAUDE.md's explicit-Docker-volume rule alongside it. Rejected: a
  hardware problem should not get to redesign the architecture.
- **Paid VPS (Hetzner ~€4–7/mo).** Fine, and better if the stack ever needs to stay up
  between sessions. Deferred purely on cost — Codespaces is free at this scale on the
  GitHub Student pack (180 core-hours/month), and the same Compose files move to a VPS
  unchanged if that stops being true.

### Container decisions
- **`docker-in-docker`, not `docker-outside-of-docker`.** The non-obvious one. Binding the
  host socket makes Compose bind mounts resolve against the *daemon's* filesystem, so the
  simulator's `./data:/data:ro` would silently mount a non-existent path and replay
  against an empty directory. Running the daemon inside the container keeps the workspace
  and the daemon on one filesystem.
- **Python 3.11 base**, matching the `python:3.11-slim` service images, so `data_fetcher`
  run in the container behaves identically to code inside a service image.
- **uv pinned to 0.5.11**, the version the service Dockerfiles copy in, so a single uv
  version reads every `uv.lock` in the repo.
- **Only ports 8000 and 9092 forwarded.** Postgres/Airflow/Streamlit ports arrive with
  their phases; pre-declaring them would be scaffolding ahead, which CLAUDE.md forbids.
- **No data fetch in `postCreateCommand`.** `data_fetcher` stays manual and on-demand per
  the earlier decision; auto-downloading ~45 MB on every codespace create would quietly
  make it part of the startup path.
- **Default 2-core / 8 GB machine.** Enough for Kafka + gateway. Revisit at P4 when Spark
  and Airflow land — a 4-core machine burns included core-hours twice as fast.

### Open
- The dev container is committed but **unexercised** — Phase 2 verification is still the
  next action, now runnable.
- Whether 8 GB holds up once Spark and Airflow join the stack in P4.

---

## Open / deferred decisions (cross-phase)

- **ML task.** Per-trip **tip prediction** is the core model: train on <=cutoff records,
  predict as later records replay, compare predicted vs actual, and run a daily Airflow
  evaluation (maps onto the rubric's "compare earlier predictions with actual" +
  "evaluate every day"). Aggregate demand/revenue **forecasting** remains a possible
  extension. Pick one target per model; don't build one model secretly trying to be both.
- **Streaming = replay, not live data.** TLC is historical; we simulate "live" by
  replaying post-cutoff records in `pickup_datetime` order. Batch vs stream is a
  freshness-vs-completeness tradeoff (lambda architecture) over the *same* events.
  <=cutoff trains the model (batch), >cutoff replays as the stream.
- **Data scope: 2023–2025 only (post-COVID).** Decided 2026-08-16. Deliberately exclude
  2020–2022: pandemic-era ridership is a regime shift (collapsed volumes, distorted
  zone/fare/tip patterns) the model would wrongly learn as signal, hurting predictions on
  normalized traffic. 2023–2025 is a consistent post-recovery regime. Cutoff for the
  train/replay split lands inside this range (exact month TBD in P5 — likely end-2023 or
  mid-2024, leaving enough post-cutoff months to make a visible replay stream).
- **Data acquisition = a separate on-demand fetcher module.** TLC publishes monthly
  yellow-trip parquet files. A dedicated `data_fetcher/` component downloads the chosen
  months once into gitignored `/data/`, idempotently (skip files already present). It is
  NOT part of the always-on Compose stack — run it manually before a first simulation.
  Keeps data acquisition as its own concern, off the runtime path.
- **Event-time windowing** is implemented in the **hot-path consumer** (P3), built on the
  non-nullable `pickup_datetime` from Phase 1 — not Flink.
- **Future source swap** (TLC -> another taxi/weather source) = new adapter + schema
  adjustment only; nothing downstream of the adapter should change. This is the whole
  point of the canonical contract, and it survives the streamlining unchanged.

---

<!-- Template for new entries:

## Phase N — <name>  · YYYY-MM-DD
**Status:**
### <area>
- Decision — why. Rejected <alternative> because <reason>.
### Open
- ...
-->
