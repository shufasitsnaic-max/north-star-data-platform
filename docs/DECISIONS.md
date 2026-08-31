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

## Resuming — read this first  · paused 2026-08-30 ~15:10 UTC

> **Superseded in part on 2026-08-31.** The backfill described below is **complete** —
> 36 continuous months, 1,100 days from 2023-01-01. Its recovery command was also
> wrong; see *Operational findings + fixes — 2026-08-31* at the foot of this file for
> the corrected form and what else changed.

All six phases are built. P1-P5 are verified end to end; P6 renders but its newest panels
have not had a browser pass. One long-running job was left mid-flight.

### State when work stopped

| | |
|---|---|
| Lake | 27 of 36 months loaded for 2023-01..2025-12, plus 2026-01..04 from replay |
| Serving table | ~103k rows / 430 days — **stale**, the backfill's merge has not run yet |
| Predictions | ~205k over 2026-01-01..04, model `fare-hgb-2` |
| `cold_path_backfill` | run `manual__2026-08-30T13:14:25+00:00` **still `running`**, DAG **paused** |
| `cold_path_incremental` | unpaused, healthy, ~121s per run on a 3-minute schedule |
| Disk | ~6.6GB free of 32GB, shared between `/workspaces` and `/var/lib/docker` |

### First action: finish the backfill

The DAG was **paused mid-run** — most likely toggled by accident in the UI while deleting a
duplicate run. A paused DAG leaves an in-flight run untouched: it stays `running` with its
remaining tasks stuck in `scheduled` indefinitely, which reads as a hang rather than a
setting. Worth knowing because nothing about the symptom points at the cause.

```bash
cd /workspaces/north-star-data-platform
docker compose up -d                     # everything; restart policies cover most of it
docker compose exec airflow-scheduler airflow dags unpause cold_path_backfill
```

It resumes at map index 27 within ~30s. Nine months remain at ~215s each, 2 concurrent, so
**~16 minutes**, then `aggregate_history` (one Spark pass over ~110M rows, 10-20 min) and the
merge.

Confirm it moved rather than assuming:

```bash
docker compose exec airflow-scheduler airflow tasks states-for-dag-run \
  cold_path_backfill "manual__2026-08-30T13:14:25+00:00"
```

Success count climbs to 38 (36 months + `ensure_schema` + `aggregate_history`). Then:

```bash
docker compose exec -T postgres psql -U northstar -d northstar -c "
SELECT count(*) AS rows, min(metric_date), max(metric_date),
       count(DISTINCT metric_date) AS days FROM cold_daily_zone_metrics;"
```

**~300k rows across ~1,100 days from 2023-01-01** is the target. Until that lands the
dashboard's history chart still shows three disconnected islands rather than three continuous
years.

If the run has been marked `failed` by a scheduler restart, do not re-trigger from scratch —
clear the unfinished tasks instead, since the 27 loaded months need no reloading:

```bash
docker compose exec airflow-scheduler airflow tasks clear cold_path_backfill \
  --start-date 2026-08-30 --end-date 2026-08-30 --only-failed --yes
```

### Then, in order

1. **Browser pass on the dashboard**, the only thing standing between P6 and verified. The
   palette was validated computationally; layout was never looked at. Check label collisions,
   column widths and overflow on the scoring feed. Port 8501 needs adding by hand in the
   PORTS panel until the next container rebuild picks up `devcontainer.json`.
2. **Longer 2026 replay.** Four event-time days is enough to separate the holiday from normal
   days, not enough for a weekly or seasonal picture. The sidebar renders the command; ~420
   events/sec.
3. **Retrain on the full corpus.** The current model saw 14 months. After the backfill the
   lake holds 36, and `TRAIN_SAMPLE_ROWS` stratifies across whatever is there. Remember the
   consumer-group reset afterwards or nothing re-scores — the command is in `predictor.py`.

### Known traps, all previously paid for

- `docker compose run` uses the **cached image**; add `--build` after changing source, or the
  old code runs and looks like a mystery.
- `spark-submit --master` is **silently ignored** — set `SPARK_MASTER` in the environment.
  `--packages` is the opposite: it *must* be on the submit line or in `spark-defaults.conf`,
  because connectors are resolved before the JVM exists.
- **Training a new model re-scores nothing** until the Kafka consumer group is reset.
- **Never evaluate while the predictor is re-scoring** — the evaluation is a full recompute
  over a table another service is rewriting, and produces rows that do not partition.
- `devcontainer.json`'s `forwardPorts` is read **only at container creation**; add ports by
  hand in the PORTS panel meanwhile. Codespaces never exposes ports on the laptop's
  `localhost` — use the forwarded `*.app.github.dev` URL.
- Watch **disk**. `/workspaces` and `/var/lib/docker` share one 32GB filesystem, and Spark's
  worker scratch grew to 8.49GB unnoticed before cleanup was enabled.

### Shutting down

Stopping the codespace is enough — **do not** `docker compose down -v`, which would destroy
the Kafka log, both databases and Airflow's history. Named volumes and everything under
`/workspaces` (the lake, the raw files, the model artifact) survive a stop; only container
writable layers are lost, and nothing of value lives there.

Stop it from github.com/codespaces, or let the idle timeout do it. **Worth doing
deliberately**: this is a 4-core machine, which burns Student Pack core-hours at twice the
rate of the default.

On restart the Docker daemon comes back and services carrying `restart: unless-stopped`
return by themselves; `docker compose up -d` covers the rest. Expect the first Spark submit
after a restart to re-download connector JARs, since the Ivy cache lives in the container's
`/tmp`.


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

**Status: VERIFIED end-to-end 2026-08-16** in codespace `northstar-p2` — see
"Verification run" below. (Superseded status: was "code complete, verification pending"
while the dev machine had no working Docker daemon.)

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

### Verification run · 2026-08-16
Full path exercised in codespace `northstar-p2`: `data_fetcher` -> Compose (`kafka` +
`gateway`) -> `docker compose run --rm simulator` -> `kafka-console-consumer.sh
--from-beginning --max-messages 5`. Five canonical events printed, then
`Processed a total of 5 messages`. **P2 verification passes.**

What the payload confirms beyond "messages exist":
- **Replay order is preserved.** `pickup_datetime` strictly ascending across the five
  (`00:00:00, :05, :06, :08, :09`) — the single-partition topic is holding global order
  as intended, which P3's event-time windowing depends on.
- **Arithmetic is internally consistent.** For all five, `fare + surcharges + tip + tolls
  == total`, and `surcharges_amount` equals the sum of the `source_extras` components
  (extra + mta_tax + improvement_surcharge + congestion + airport). The rollup is correct.
- **Canonical contract holds.** Money as strings (Decimal, no float drift), distance in km,
  `payment_type` mapped to the enum (`CASH`/`CARD`/`OTHER`), source-specific leftovers
  quarantined in `source_extras`, nulls where TLC genuinely has none.

### Open
- Only 2023-01 is downloaded so far; the full 2023–2025 scope is a longer fetch.
- **Cash trips report `tip_amount: 0.0` — a data artifact, not a fact.** Two of the five
  sampled events are `payment_type: CASH` with a zero tip. TLC only captures tips paid
  through the meter, so cash tips are systematically recorded as zero rather than as
  unknown. This is a **P5 modelling hazard**: training tip prediction on unfiltered data
  teaches the model "cash implies no tip," which is an artifact of collection, not
  behaviour. Decide in P5 whether to train on card trips only (cleanest), or to model tip
  *rate* conditional on payment type. Flagged now because it is invisible once the data is
  aggregated. Nothing to change in P2 — the adapter is correctly passing through what the
  source says.

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

### Build fix — the base image wrapped in a Dockerfile
**Observed directly:** `mcr.microsoft.com/devcontainers/python:1-3.11-bookworm` ships a
preconfigured Yarn apt source (`dl.yarnpkg.com`) whose signing key has since rotated, so
`apt-get update` in the image fails with `NO_PUBKEY 62D54FD4003F6525`. This is a real defect
and it matters here, because dev container *features* run `apt-get update` during their
install — so a broken apt source can take `docker-in-docker` down with it.

**Decision: wrap the base image in `.devcontainer/Dockerfile`** that deletes the Yarn apt
source, then runs `apt-get update` under `set -e` to prove apt is healthy. `devcontainer.json`
switches from `"image"` to `"build".dockerfile`. Matching on the URL rather than a filename
keeps it correct whether the image uses legacy `.list` or deb822 `.sources` format, and the
verification step means a still-broken repo fails loudly at image build instead of inside an
opaque feature-install step. Retained as a cheap guard against a defect we know is present.

- **Rejected: import the rotated Yarn key.** Chases an upstream key this project has no use
  for — nothing here touches Yarn — and would need chasing again on the next rotation.
- **Rejected: pin an older base image tag.** Leaves the codespace on a stale Python/tooling
  base to dodge one bad apt line, and the tag would drift out from under us anyway.
- **Rejected: `postCreateCommand` cleanup.** Runs *after* features install, i.e. too late to
  protect a feature install. Wrong lifecycle stage.

### Correction — the "codespace is in a recovery container" diagnosis was wrong
An earlier version of this entry (commit `c5aba52`) claimed the first codespace create failed
into an Alpine recovery container with no Docker daemon, and that the Yarn source caused it.
**That causal claim was not supported by evidence and is retracted.**

The diagnosis rested entirely on `gh codespace ssh` failing with *"Please check if an SSH
server is installed in the container."* That error is **not** evidence of a failed build.
`gh codespace ports` against the same codespace returns our own `portsAttributes` labels —
`gateway (FastAPI)` on 8000, `kafka (host listener)` on 9092 — which only exist in our
`devcontainer.json`. The container therefore built with our config and was healthy the whole
time; a recovery container would never have applied those labels.

**Lesson worth keeping: an unreachable container is not a broken container.** Diagnose
container health with a check that doesn't share a transport with the symptom. Two codespaces
were stopped/created chasing this before `ports` was tried.

- **Real open issue:** `gh codespace ssh` does not reach these codespaces, so the stack cannot
  be driven from a local terminal. P2 is therefore verified from the browser web editor.
  Adding `ghcr.io/devcontainers/features/sshd:1` is the candidate fix if CLI access is wanted
  later — deferred rather than adopted, since it is a new feature dependency and the browser
  path unblocks P2 today.

### Rebuilding the dev container destroys all Docker state  · 2026-08-16

**Observed:** after a Full Rebuild for the JDK, `kafka-console-consumer.sh` returned
`UNKNOWN_TOPIC_OR_PARTITION` — the topic, the Kafka log, the Postgres volume and every
built image were gone.

This follows directly from the `docker-in-docker` choice above, and it is worth stating
because the two decisions are usually discussed apart: **the Docker daemon lives inside the
dev container**, so its volumes live inside the dev container too. `/workspaces` is on the
codespace's persistent volume and survives a rebuild; `/var/lib/docker` is not and does not.

- **The rule to remember:** a dev container rebuild is equivalent to `docker compose down -v`
  plus `docker image prune -a`. Budget for a re-`up`, a re-replay, and image rebuilds after
  any rebuild. Nothing is corrupted and nothing needs diagnosing — this is the design working
  as chosen.
- **What is safe:** the downloaded dataset in `data/raw/`, because it is workspace state, not
  Docker state. Committed code likewise.
- **Not worth "fixing".** Binding the host socket would preserve volumes across rebuilds but
  reintroduces exactly the bind-mount resolution bug that `docker-in-docker` was chosen to
  avoid. Paying a re-replay occasionally is cheaper than replaying against an empty directory
  and not noticing.

### JDK: installed in the Dockerfile, not via the java feature  · 2026-08-16

PySpark needs a JVM to start even a `local[*]` session, so `batch_jobs`' adapter and its
conformance test cannot run in this container without one.

`ghcr.io/devcontainers/features/java:1` was tried first and left `java: command not found`.
**Why it failed was never established** — the diagnostic went unrun and the rebuild that
replaced it destroyed the evidence. The leading suspect is PATH: that feature installs into
SDKMAN and reaches PATH only through a *nested* `containerEnv` reference
(`${SDKMAN_DIR}/candidates/java/current/bin:${PATH}`), while `devcontainer.json` separately
overrides PATH in `remoteEnv` for uv. Plausible, unconfirmed, recorded as such rather than
asserted — this file already carries one retraction for a confident wrong diagnosis.

**Decision: `apt-get install openjdk-17-jdk-headless` in `.devcontainer/Dockerfile`,** with
`JAVA_HOME` set explicitly and a `java -version` in the build so a broken install fails at
image build rather than at the first `pytest`. apt's openjdk registers `/usr/bin/java`
through alternatives, so it is on PATH by construction with no cross-layer variable
resolution that can silently fail. Same reasoning that already put the Yarn fix in that
file. 17 because Spark 3.5.x targets it; it moves when the pyspark pin moves.
**Verified: JDK 17.0.20 resolves in the rebuilt container.**

### Open
- The dev container is committed but **unexercised** — Phase 2 verification is the next
  action, run from the browser terminal in codespace `northstar-p2`.
- Whether 8 GB holds up once Spark and Airflow join the stack in P4.
- Whether the Yarn apt breakage would actually have broken the `docker-in-docker` install
  here. The Dockerfile now prevents it, so this stays untested by design.

---

## Phase 3 — Hot path: Kafka -> event-time windows -> PostgreSQL  · 2026-08-16

**Status: VERIFIED end-to-end 2026-08-16** in codespace `northstar` — see "Verification run"
below.

New component `hot_path/` (`consumer.py`, `windows.py`, `db.py`, `schemas.py`,
`schema.sql`) and one new service, `postgres`. Nothing here names a data source.

### Approved additions
- **`postgres:16-alpine`** with an explicit `postgres-data` named volume and a `pg_isready`
  healthcheck. Alpine for image size; revisit if an extension ever needs the Debian base.
- **`psycopg[binary]==3.2.3`** — current generation, and maps Python `Decimal` to
  `numeric` without a float round trip.
- **`confluent-kafka==2.6.1`** — pinned to the gateway's exact version so producer and
  consumer share one librdkafka generation.

### Window shape
- **5-minute tumbling windows over `pickup_datetime`**, one row per `(window_start,
  zone_id)` plus a `zone_id IS NULL` citywide rollup row.
- **Event time, not wall clock.** A replayed month bucketed by wall clock collapses into a
  single bucket. `canonical.py` already committed to this by making `pickup_datetime`
  non-nullable and never defaulting it to "now".
- **Why 5 minutes:** the P2 run's `ingested_at` deltas measure replay at **~420 events/sec**
  at `SLEEP=0`, i.e. ~366x event-time compression, so 5-minute windows surface ~1.2 new
  windows/sec — live-feeling but readable. 1-minute windows would emit ~6/sec, which is
  noise. Rejected 1-min for that reason and 15-min as too coarse for short demand spikes.
- **Window size is data resolution, not display refresh.** Demo pacing is the simulator's
  `SLEEP`; changing it needs no schema change. Recorded because conflating the two is the
  obvious mistake.
- **Rejected sliding windows** — they multiply row count by the overlap factor to produce a
  smoothing the dashboard can compute from tumbling rows itself.
- **Citywide stored, not derived at query time.** Keeps the dashboard headline a single
  indexed row read, and stays correct for events whose zone is null.

### Contract sharing: tolerant reader
`hot_path/schemas.py` declares **only the fields it aggregates** and ignores the rest, rather
than importing or copying the gateway's `TripEvent`.
- Consumers reading a subset is standard event-driven practice: the gateway can add canonical
  fields without breaking the hot path, and no build-time coupling is introduced.
- **Rejected a shared `contracts/` package** — one source of truth, but it adds a directory
  outside the CLAUDE.md structure and couples every component at build time.
- **Rejected copying `canonical.py` wholesale** — two full copies drift with nothing enforcing
  sync, and the hot path would validate fields it never reads.
- Accepted risk, stated plainly: the definitions *can* drift. Mitigation is that the fields
  chosen are contract-critical, so a breaking change fails loudly here rather than reading nulls.

### Offset safety (the non-obvious decision)
Aggregates live in memory, so a restart loses partially-filled windows. Committing offsets as
events are consumed would resume mid-window, recompute that window from only its *remaining*
events, and overwrite a correct stored row with an undercount.

**Decision: each window records the lowest Kafka offset that fed it, and the consumer commits
only `min(offset)` across still-open windows.** A restart therefore resumes at the first event
of the oldest unfinished window and rebuilds it in full.
- This is what makes the **absolute** upsert (`SET`, not `+=`) correct: rewriting a fully
  recomputed window is always right.
- **Rejected an additive upsert** (`trip_count = trip_count + EXCLUDED.trip_count`): it removes
  the need to rebuild, but double-counts on any at-least-once redelivery. Absolute writes plus
  a withheld commit is the pairing that works; mixing the two halves would corrupt totals.
- **Rejected exactly-once (transactional Kafka -> Postgres)** — real complexity for a guarantee
  idempotent upserts already provide here.
- Two write cadences, deliberately split: a **liveness flush** every 2s rewrites open windows as
  `is_final=false` (no commit — still filling), while **finalization** on the watermark writes
  `is_final=true` and only then advances offsets. Liveness is a display concern, durability is
  an offset concern.

### Schema notes
- **Upsert key is a unique index on `(window_start, COALESCE(zone_id, -1))`, not a primary key.**
  PostgreSQL treats NULLs as distinct in a unique constraint, so a plain PK would let the
  citywide row insert repeatedly. `COALESCE` folds it for uniqueness only — the stored value
  stays NULL, so queries read as `WHERE zone_id IS NULL` rather than against a magic number.
- **Money as `numeric`, never float**, matching the canonical contract's `Decimal`.
- **`avg_distance_km` is nullable and averaged over events that carry a distance**, since
  `trip_distance_km` is optional — dividing by `trip_count` would silently treat missing as zero.
- **`is_final` is exposed to the dashboard** so P6 can style the in-progress bucket differently.
- Schema is applied idempotently on every consumer start; no migration tool at this scale.

### Verified locally (no Kafka or Postgres required)
Exercised `windows.py` directly over synthetic events built from the real P2 payload shape —
22 checks, all passing: 5-minute grid flooring at boundaries, zone + citywide double count from
one event, Decimal sums and averages, null distance excluded from the mean, watermark
finalization only once `window_end + grace` is passed, offset floor held at the oldest open
window then advancing on finalization, late events counted rather than dropped, and the commit
point stepping past a poison message.

### Implementation notes
- Late events for an evicted window **cannot** be reopened (their aggregates are gone), so they
  are logged and counted, never silently dropped — per the no-silent-caps rule. `GRACE_MINUTES`
  is the knob if they ever become non-trivial.
- Malformed messages are logged with a stack trace, counted, and stepped over, with their offset
  noted so the commit point cannot stall behind a poison message forever.
- `hot_path` sets `restart: unless-stopped` — a worker, not a server; a broker or database blip
  shouldn't silently stop the hot path.
- Devcontainer now forwards **5432**, per the "ports arrive with their phase" rule.
- Postgres credentials default to `northstar` and are overridable from the environment; nothing
  secret is committed.
- `confluent-kafka==2.6.1` has no installable Windows wheel (`Invalid Wheel-Version`), so
  `uv sync` on `hot_path/` fails on the Windows dev machine. Harmless — the component only ever
  runs in a Linux container — but it means local work on this component is container-only.

### Verification run · 2026-08-16
Replayed 50,000 rows (`49,482` accepted, `518` rejected = **1.04%**, matching the ~0.9%
negative-fare refund rate P2 measured on the same data). Covered event time
`2023-01-01 00:00` -> `15:45`. **P3 verification passes.**

Four independent cross-checks, not just "rows exist":
- **`open windows = 3`, stable for the whole replay.** Exactly what a 5-minute window plus a
  10-minute grace predicts — the filling window plus two awaiting grace. A wrong watermark
  would make this drift or grow without bound.
- **Throughput agrees with an independent estimate.** ~330 trips per 5-minute citywide window
  = ~66/min, against 3,066,766 rows / 44,640 minutes = **68.7/min** derived separately from
  the dataset. Two unrelated routes to the same number.
- **Rollup invariant holds.** For window `15:30`, citywide `trip_count` = 331 = the sum of
  that window's per-zone counts. The two are produced by different code paths over the same
  events, so agreement rules out both double-counting and rollup drift.
- **`is_final` tracks the watermark.** Newest window `f`, all older windows `t`.

Totals: 190 citywide windows (190 x 5 min = 950 min = the 15.8h of event time covered),
10,443 zone rows across **211 distinct zones** of TLC's 265 — plausible for half a day of
yellow-cab activity. Internal arithmetic also holds: `total_revenue/trip_count` ~30.60 against
`avg_fare` 22.36 leaves ~5.00 of surcharges beyond the 3.25 tip, matching congestion 2.50 +
extra 1.00 + mta 0.50 + improvement 1.00.

**The cash-tip artifact is now visible in aggregate:** `avg_tip / avg_fare` ~14.5% citywide,
where NYC card tipping runs 22-24%. That gap is the P5 hazard recorded under Phase 2 showing
up as a real number rather than a theoretical concern.

### Runtime defects found and fixed during verification
- **`831b558` — hot path started before the topic existed.** The topic is created by the
  gateway's first produce, so on a fresh Kafka volume the consumer logged
  `UNKNOWN_TOPIC_OR_PART` on every poll. Worse, librdkafka's default 5-minute metadata refresh
  would have left it idle for minutes after a replay finally created the topic. Dropped
  `topic.metadata.refresh.interval.ms` to 10s and demoted the message to a once-only
  informational waiting state. Rejected having the consumer create the topic itself: production
  is the gateway's concern, and a consumer that creates topics hides ordering bugs.
- **`3ff8dc1` — Docker created `./data` as root.** Compose bind-mounts `./data` into the
  simulator; when the directory is absent Docker creates the bind-mount source as root, after
  which `data_fetcher` cannot `mkdir data/raw` (Errno 13) and `git pull` cannot write into it
  either. Fixed by tracking an empty `data/.gitkeep` so `git clone` creates the directory as
  the developer. Ignore rule had to become `/data/*` rather than `/data/` — a trailing slash
  excludes the directory outright and no negation inside it can match.

### Open
- Windows are held in memory: a long replay with many open zones grows the working set. Bounded
  in practice by the watermark evicting closed windows (observed steady at 3 open windows over
  50k events), but untested at full-month scale.
- Verified against a 50k-row slice, not the full month. Sustained-throughput behaviour and the
  eventual table size at 2023-2025 scale are unmeasured.

### Operational lesson — one environment, not two
Verification cost far more rounds than the code warranted, because a second codespace was
created on a mistaken diagnosis and the two then diverged: one held the fetched data and Kafka
volume, the other held uncommitted devcontainer edits from an earlier session. Symptoms that
looked like Phase 3 bugs (missing topic, missing dataset, a blocked `git pull`) were all
environment drift. **Keep exactly one codespace, and commit from it rather than leaving work
uncommitted there** — an uncommitted change inside a remote environment is invisible from the
local clone, and `has_uncommitted_changes` in the API is a real signal worth heeding.

---

## Phase 4 — Cold path: Airflow + Spark -> partitioned Parquet  · 2026-08-16

**Status: VERIFIED end-to-end 2026-08-30.** All six steps built and verified in one
session. The plan below was recorded *before* writing any code so it would survive a lost
session; the "Step N" sections near the end of this entry describe what was actually built,
including where it departs from the plan and why.

Phase 4's headline result is the cross-layer reconciliation under Step 4: on a single clean
replay the hot and cold paths agree **exactly**, and where they diverge the cold path is
demonstrably the correct one. That is the lambda architecture's central claim, measured
rather than asserted.

**Next action: Phase 5 (ML).** Read the bug sections under Steps 2, 3 and 5 before adding
any new Spark task — the launcher-versus-runtime distinction below governs how anything new
must be invoked: the DAG must set `SPARK_MASTER` in
the task environment *and* pass `--packages` on the submit line, and confusing the two gives a
silent no-op in one direction and a hard failure in the other.

| # | Step | State |
|---|------|-------|
| 1 | canonical Spark schema + batch adapter + conformance test | **verified** 2026-08-30, `fba0137` |
| 2 | `bulk_load.py` + `spark-master` / `spark-worker` in Compose | **verified** 2026-08-30 |
| 3 | `stream_to_lake.py` (Kafka -> lake) | **verified** 2026-08-30 |
| 4 | `aggregate_daily.py` + staging/merge into Postgres | **verified** 2026-08-30 |
| 5 | `orchestration/` image + both DAGs + Airflow services | **verified** 2026-08-30 |
| 6 | backfill (12 months, scoped down) + verification run | **verified** 2026-08-30 |

### Resuming (read this first)

The stack lives in codespace `north-star-data-platform` at
`/workspaces/north-star-data-platform`; nothing runs on the Windows dev machine. A dev
container rebuild wipes all Docker state (see the dev-environment entry), so after any
rebuild expect to re-`up` and re-replay before anything works.

```bash
# 1. confirm the tree is clean and in sync in BOTH places before touching anything
git -C /workspaces/north-star-data-platform status -sb

# 2. bring the stack up (rebuilds images if they were wiped)
cd /workspaces/north-star-data-platform ; docker compose up -d kafka gateway postgres hot_path

# 3. the topic is only populated by an explicit replay
cd /workspaces/north-star-data-platform ; docker compose run --rm simulator

# 4. step 1's drift guard — needs no cluster, no Kafka, no database
cd /workspaces/north-star-data-platform/batch_jobs ; uv run pytest -v
```

**Step 1 is confirmed green — 7/7 on 2026-08-30**, in 73s of local Spark. The
null-vs-absent fix (`869df4b`) is real, not merely believed. Step 2 was unblocked by that
run; the machine was also confirmed already on `standardLinux32gb` (4 cores / 15GB / 22GB
free), so the sizing item under "Compose additions" needs no further action.

### The conceptual correction worth keeping (cold path != old data)

The tempting mental model — *hot path handles new data, cold path handles old data* — is
**wrong**, and it describes a data warehouse rather than a lambda architecture. Recording
the right framing because it drives the design and it's the presentation angle:

Both layers process **the same events**. Every replayed 2026 trip goes through the hot path
(~1s, 5-minute in-memory windows, drops late events past grace, loses open windows on
restart) *and* through the cold path (minutes later, full recompute from the log, every
event every time, rerunnable). The split is **latency vs. completeness**, not old vs. new.
The cold path's job is to be the number you trust when the two disagree — which is what
makes P3's documented compromises acceptable rather than defects.

The 2023–2025 bulk load is a **third thing**: a historical backfill supplying the training
corpus and the dashboard's long-run trends. It reuses the same Spark code and the same lake,
but it is *not* what makes the cold path "cold".

### Ingestion topology — hybrid: bulk load + bus

Two writers into one lake:

```
data/raw/*.parquet ──[backfill DAG, one-shot, <= cutoff]──┐
                                                          ├──> /data/lake/trips/year=/month=/day=
Kafka tlc-raw-events ─[incremental DAG, sched, > cutoff]──┘     (canonical schema, Snappy Parquet)
```

- **Rejected: Kafka-only (cold path reads solely from the bus).** Architecturally purest —
  the lake would contain only what actually transited the message bus. Killed by arithmetic:
  P2 measured replay at ~420 events/sec, so pushing the 36-month training corpus through the
  gateway would take **days**, and the lake would additionally be bounded by Kafka retention.
  A training corpus that can only be built by a multi-day replay is not a training corpus.
- **Rejected: Spark reads `data/raw` directly, Kafka feeds only the hot path.** Simplest and
  fastest to build, and rejected on principle: the cold path would then know raw TLC column
  names, directly violating the CLAUDE.md rule that nothing downstream of the adapter may
  reference a specific source. It also makes the lambda story *untrue* — hot and cold would
  no longer be two views of the same events.
- **Chosen: hybrid.** The bulk loader is a **sibling of the gateway, not something downstream
  of it** — it is an adapter invocation, which is exactly what the source-independence rule
  permits. Everything past the lake still sees canonical columns only.

### The cost of the hybrid: two adapter implementations

`gateway/adapters/tlc_adapter.py` is row-at-a-time Pydantic. Spark cannot use it at 110M-row
scale without a per-row Python UDF (slow, and it would force `gateway/` source into the
`batch_jobs/` image, breaking the per-component dependency rule). So `batch_jobs/adapters/
tlc_batch_adapter.py` reimplements the same mapping as **vectorized Spark SQL column
expressions**.

This is a real cost, stated plainly: two implementations of one mapping, free to drift, is
the exact failure the canonical contract exists to prevent. The mitigation is a
**conformance test against a golden file**:

1. Run ~200 real raw rows through the **actual running gateway**; capture the canonical JSON.
2. Commit both halves as `batch_jobs/tests/fixtures/conformance.json`.
3. The test runs those same raw rows through the Spark adapter and asserts field-by-field
   equality against the recorded gateway output.

- **Rejected: importing `adapt_tlc()` into the test.** One source of truth, but reintroduces
  precisely the build-time coupling P3 rejected when it chose a tolerant reader over a shared
  `contracts/` package. The golden file tests the gateway's *observed behaviour*, not its
  source, so coupling is zero.
- **Knowing exception to "don't commit data":** the fixture is ~150KB of derived test data,
  not a raw dataset. Committed deliberately, because a fixture generated on demand would make
  the test skippable, and a skippable drift guard guards nothing.
- **Consequence worth noting:** because the batch adapter is pure Spark SQL with no Python
  UDFs, Spark executors need no Python dependencies beyond stock pyspark. The worker image
  stays trivial. The constraint pays for itself twice.

### Recompute strategy — full rebuild, no offset tracking

The Kafka -> lake job reads `startingOffsets: earliest` -> `endingOffsets: latest` on **every
run**, and rewrites affected date partitions with `partitionOverwriteMode=dynamic`.

- No offset bookkeeping, no watermark state, trivially idempotent, and a rerun repairs any
  past mistake. This is not laziness — recomputing the world from the immutable log is what
  the batch layer of a lambda architecture is *for*, and it's the property that lets the cold
  path correct the hot path.
- **Rejected: incremental reads with offsets persisted in an Airflow Variable or a Postgres
  table.** Cheaper per run, but it re-adds exactly the state-management burden the batch layer
  exists to avoid, and a bad offset silently produces a permanently wrong lake. At this data
  size a full recompute is seconds.

### The cutoff is a P4 decision, not a P5 one

Because both writers use dynamic partition overwrite, overlapping date ranges would mean
whichever job ran last silently clobbers the other's partitions. So the cutoff is enforced
config, shared by both jobs: backfill owns `<= cutoff`, Kafka loader owns `> cutoff`. P5
inherits the same value for the train/replay split — the two were always one decision. Value
pinned in the cross-phase section: `2025-12-31T23:59:59`.

### Deduplication — natural key, not `event_id`

`event_id` is a fresh UUID minted per gateway request, so replaying the same month twice
writes every trip twice with different IDs, and a full recompute from `earliest` faithfully
preserves the duplication. Dedupe is therefore on the natural key
`(pickup_datetime, dropoff_datetime, pickup_zone, dropoff_zone, total_amount, provider_id)`.
Practical payoff: re-replaying during a demo, or restarting a botched replay, is harmless.

### Component layout

```
batch_jobs/
├── schemas/canonical_spark.py    # canonical StructType — SOURCE-AGNOSTIC
├── adapters/tlc_batch_adapter.py # the ONLY source-specific file in this component
├── jobs/bulk_load.py             # raw parquet (<= cutoff) -> lake, ONE MONTH per invocation
├── jobs/stream_to_lake.py        # Kafka (> cutoff) -> lake
├── jobs/aggregate_daily.py       # lake -> daily x zone -> Postgres staging
├── common/{spark,config}.py      # session builder w/ backoff; env-driven config
└── tests/                        # conformance golden file + test
orchestration/
├── Dockerfile                    # apache/airflow + JDK + spark-submit client
└── dags/{cold_path_backfill,cold_path_incremental}.py
```

- **`canonical_spark.py` declares an explicit `StructType`, never inferred.** Same
  tolerant-reader reasoning as `hot_path/schemas.py`: the gateway can *add* a canonical field
  without silently changing the lake's schema, and a *removed* field fails loudly instead of
  quietly reading nulls.
- Money as `DecimalType(12,2)`, matching the Pydantic `Decimal` and Postgres `numeric`. No
  float anywhere in the money path, end to end.
- `source_extras` stored as a **typed struct, not a JSON blob** — Phase 2 already flagged
  `ratecode_id` as a P5 feature candidate, and a struct keeps it queryable without parsing.
- **`bulk_load.py` processes one month per invocation** so Airflow can map over months and a
  failed month retries alone instead of redoing 36.

### Serving-table write: staging + merge

`aggregate_daily.py` produces daily x zone rollups (count, revenue, avg fare/tip/distance,
plus a `zone_id IS NULL` citywide row mirroring the hot path's shape) — roughly 1,250 days x
~260 zones ~= **325k rows**, trivially small. Spark writes to a **staging** table; a separate
SQL task merges with `INSERT ... ON CONFLICT DO UPDATE`.

- **Rejected: `mode("overwrite")` on the serving table.** Spark's JDBC writer has no upsert,
  and overwrite drops/recreates the table (losing indexes) or, with `truncate=true`, leaves
  the dashboard reading an empty table for several seconds mid-write. Staging + merge is
  atomic from the reader's side.
- Note this is the **same absolute-upsert pattern P3 chose** for window metrics — one
  consistent write discipline across both layers rather than two.

### DAG shapes

- **`cold_path_backfill`** — `schedule=None`, manual trigger, dynamic task mapping
  (`.expand()`) over the 36 months `<= cutoff`. Run once.
- **`cold_path_incremental`** — `stream_to_lake -> aggregate_daily -> merge_into_serving`.

Three settings, each preventing a specific failure:
- **`schedule="*/3 * * * *"` (wall clock), not `@daily`.** The replay compresses event time
  ~366x, so a `@daily` DAG would fire **zero times** during a 10-minute demo while the
  simulator burns through a year of event time. "Daily batch" is the story; a 3-minute cron
  is the schedule. Same event-time-vs-wall-clock distinction P3 hit, biting differently.
- **`catchup=False`.** Otherwise Airflow backfills a run for every 3-minute interval since the
  start date — thousands of runs on first boot.
- **`max_active_runs=1`.** Two overlapping full recomputes writing the same partitions race
  and corrupt them.

### Compose additions (approved 2026-08-16)

Six new service definitions — five long-running, one one-shot (`airflow-init`, like
`simulator`): `spark-master`, `spark-worker`, `airflow-postgres`, `airflow-init`,
`airflow-scheduler`, `airflow-webserver`. New volumes `airflow-db-data`, `airflow-logs`; new
bind mounts `./data/lake` (rw into scheduler + master + worker) and `./data/raw:ro`.

- **Spark master UI moved to 8081** — 8080 is Airflow's webserver.
- **Dedicated `airflow-postgres` rather than a second database inside the serving
  `postgres`.** One concern per container, and it means dropping the serving-store volume
  can't take Airflow's history with it. Rejected the shared-instance variant on that coupling
  alone; the extra container is cheap.
- **Airflow metadata, Spark cluster, and the lake each get an explicit mount**, per the
  no-ephemeral-state rule.
- **Estimated ~10.5GB resident** with the full stack up, so the codespace moves from
  2-core/8GB to **4-core/16GB** (`standardLinux32gb`). This resolves the "whether 8 GB holds
  up once Spark and Airflow join" item left open in the dev-environment entry: it does not.
  Cost: Student-pack core-hours burn at 2x.

Three gotchas to bake in from the start, each an hour lost if discovered live:
1. **`spark.driver.host=airflow-scheduler` + `spark.driver.bindAddress=0.0.0.0`.** In client
   mode executors connect *back* to the driver; without this Spark advertises an unreachable
   internal hostname and executors hang. The single most common Spark-on-Compose failure.
2. **Bake the Kafka and Postgres JDBC jars into the image; never `--packages` at runtime.**
   `--packages` resolves from Maven on every run and needs a warm Ivy cache — a classic
   container flake with no useful error message.
3. **Airflow deps via `pip --constraint`, not uv.** A deliberate, knowing exception to the
   per-component uv rule. Airflow publishes a curated constraint set for its ~90 transitive
   dependencies; resolving them independently is the standard route to an unbootable
   scheduler. Recorded here so a future reader sees an exception, not an oversight. Every
   other component keeps uv.

**Why client mode over cluster mode:** cluster mode requires the application file to be
present on the workers and loses the driver's stdout, whereas `SparkSubmitOperator` in client
mode streams driver logs straight into the Airflow task log — which is genuinely useful for
the demo (the Spark job's output is visible inside Airflow).

**Honest caveat to state out loud in the presentation:** `./data/lake` is a shared local bind
mount. On a real cluster this would be S3 or HDFS. It is the one place the "distributed"
story is simulated, and Spark itself is admittedly oversized for ~45MB/month — the argument
for it is that the code is unchanged at 45GB, not that this data needs a cluster.

### Verification plan (must pass before P5)

- **CLAUDE.md's stated check:** `pyarrow.parquet.ParquetFile('/data/lake/trips/...').metadata`
  asserts schema + `year=/month=/day=` partitions.
- **Conformance test green** — both adapters agree on 200 real rows.
- **Cross-layer reconciliation** (the one that actually proves something): for a date the
  simulator has replayed, sum `trip_count` from the hot path's `window_metrics` and compare
  against the cold path's `cold_daily_zone_metrics`. Two independent code paths over the same
  events. A small divergence is **expected and explainable** — it is the hot path's late-event
  drops — and explaining the gap is worth more than the numbers matching exactly.
- **Idempotency proof:** run `cold_path_incremental` twice back to back; row counts unchanged.
  Demonstrates dedupe + dynamic partition overwrite together.

### Build order

Front-loads the component most likely to be wrong (the adapter), defers the heaviest infra:

1. `canonical_spark.py` + `tlc_batch_adapter.py` + conformance test *(local Spark, no cluster)*
2. `bulk_load.py` + `spark-master`/`spark-worker` — verify on **3 months**, not 36
3. `stream_to_lake.py`
4. `aggregate_daily.py` + staging/merge
5. `orchestration/` image + both DAGs + Airflow services
6. Full 36-month backfill, verification run, this entry updated to `VERIFIED`

### Step 1 — canonical schema, batch adapter, drift guard  · built 2026-08-16

`batch_jobs/` now contains `schemas/canonical_spark.py`, `adapters/tlc_batch_adapter.py`,
`common/{config,spark}.py`, and `tests/` with the generator, the conformance test and the
committed fixture. Runs in local Spark — no cluster, no Kafka, no database.

Decisions made *during* implementation that the plan above did not anticipate:

- **Two schemas, wire and lake.** Pydantic's `model_dump_json()` emits `Decimal` and
  `datetime` as JSON **strings**, so `WIRE_SCHEMA` reads them as `StringType` and casts
  afterwards. Rejected letting Spark's JSON reader coerce them directly: on any format it
  dislikes it yields NULL rather than an error, so a serialization change would silently
  empty the money columns instead of failing.
- **`WIRE_SCHEMA` omits `source_extras` entirely.** A source-agnostic wire schema cannot know
  that object's shape. `stream_to_lake` will extract it with
  `get_json_object(value, '$.source_extras')` and parse it with the injected schema.
- **`source_extras`' shape is injected, not declared.** `lake_schema()` takes the `StructType`
  as a parameter; the TLC adapter supplies it. Rejected a JSON-string column — source-agnostic
  but ~30x the bytes and a parse on every read — and rejected hardcoding TLC's field names in
  the schema module, which is the one file forbidden from naming a source. The precedent that
  makes this legitimate: `KAFKA_TOPIC: tlc-raw-events` is already source-specific
  *configuration* handed to the source-agnostic hot path. Configuration may know the source;
  code may not.
- **Exact-Decimal arithmetic, not doubles.** Money and distance cast to `Decimal(20,8)` first,
  compute, and narrow only at the end, mirroring the gateway's `Decimal(str(x))`. This is what
  let the conformance test compare **values** rather than tolerances — and it held: 200 real
  records matched field for field, including the `Decimal x 1.609344 -> double` conversion.
- **`event_id` is sha256 of the natural key, not `uuid4`.** The gateway can afford randomness
  because it sees each record once; a batch job that re-runs a month must produce identical
  rows, or the idempotency check in step 6 compares fresh UUIDs forever. Excluded from the
  conformance comparison alongside `ingested_at` — both are envelope metadata the contract
  defines as producer-assigned, which is exactly why `adapt_tlc()` takes them as parameters.
- **`ingested_at` on backfilled rows is when the backfill ran.** The gateway never saw them;
  claiming otherwise would be a lie in the audit trail.
- **Nullability is deliberately excluded from the schema comparison.** Spark reads Parquet
  back with every field nullable regardless of what was written, so two writers differing only
  on that axis cannot corrupt each other — while differing names, order or types can. Worth
  recording because the naive version was also *inconsistent*: a top-level field's nullability
  lives outside its `dataType` but a struct field's lives inside its parent's, so it policed
  nested fields and ignored the rest.

**The bug the test caught, and why it mattered.** The adapter coalesced the surcharge fields
to zero. But `TLCTripInput` declares them `float = 0`, **not** `Optional[float]` — so the zero
defaults an *absent key*, while an explicit null is a type error the gateway answers with a
422. The simulator converts NaN to `None`, so a NaN surcharge genuinely arrives as JSON null
and is genuinely refused. The batch adapter would therefore have written rows into the lake
that never reached the bus — a silent divergence between the two paths, in the one component
whose entire job is to not diverge. Absent and null are now distinguished, with a direct test.
Note the fixture's 22 rejections were all negative-fare, so the boundary test passed *despite*
the bug; it surfaced only through the schema assertion.

**Fixture:** 2,000 rows scanned from 2023-01, 200 accepted pairs kept, all 22 rejections kept,
302 KB. Rejection rate **1.10%**, against P2's 0.9% and P3's 1.04% on the same data — three
independent measurements converging on the same refund/void rate.

### Step 2 — bulk_load + a real Spark cluster  · verified 2026-08-30

`batch_jobs/jobs/bulk_load.py` plus `spark-master` / `spark-worker` in Compose. Verified on
three deliberately non-consecutive months — 2023-01 (local mode), 2024-06 and 2025-12 (on the
cluster) — rather than three adjacent ones, so the run exercised the adapter's case-insensitive
column resolution (TLC renamed `airport_fee` to `Airport_fee` partway through the dataset) and
the cutoff boundary month instead of three near-identical files.

| month | read | accepted | written | rejected |
|-------|------|----------|---------|----------|
| 2023-01 | 3,066,766 | 3,040,385 | 3,040,385 | 0.86% |
| 2024-06 | 3,539,193 | 3,476,089 | 3,476,089 | 1.78% |
| 2025-12 | 4,305,006 | 4,198,089 | 4,198,089 | 2.48% |

Design decisions made during implementation:

- **No dedupe in the bulk loader**, departing from this entry's dedupe section. That reasoning
  is about the *Kafka* path, where re-replay genuinely duplicates. A rerun of this job
  overwrites its own partitions, so it is already idempotent — and `event_id` hashes a
  six-field natural key that two distinct short trips can plausibly collide on, so deduping
  here would silently delete real trips from the training corpus. Dedupe belongs in step 3.
- **A month starting past the cutoff is refused, not silently empty.** Both writers use
  dynamic partition overwrite, so a mistyped `--month` must be loud. Rejected letting the
  row-level filter handle it: that yields a successful run that wrote nothing, which reads as
  "this month has no data".
- **The row-level cutoff filter is defensive, not load-bearing — worth knowing.** 2025-12
  wrote `written == accepted`, zero dropped, and that is guaranteed rather than lucky: the
  cutoff is `23:59:59` and TLC timestamps are second-resolution, so the only excluded instant
  is sub-second past it and cannot exist in the data. The thing actually keeping the two
  writers off each other's partitions is the whole-month guard. Kept anyway, because a
  sub-second source would change that and the filter costs nothing.
- **Repartition by `(year, month, day)` before writing.** Left alone, 16 shuffle partitions
  each emit a file into each of ~31 day directories — ~500 tiny files per month, ~18k across a
  36-month backfill. Confirmed: 2023-01 produced exactly 31 files, one per day.
- **One extra `count()` pass before the write**, so the run reports a row count it verified
  rather than one inferred from the adapter's pre-filter stats. ~45MB a month; the honesty is
  cheap.

### The four bugs step 2 found, and what they have in common

All four were infrastructure; none were the Spark logic. `bulk_load.py` ran correctly the
first time it was able to start, and every failure was in how it was launched or hosted. The
common root cause is worth naming: this config was written on the Windows machine, which has
no Docker daemon, so none of it could be executed before being pushed. The Python was locally
testable and worked; the container config was pure assertion and did not.

1. **`ModuleNotFoundError: no module named 'adapters'`.** Both `python jobs/bulk_load.py` and
   `spark-submit` put the *script's* directory on `sys.path`, not the component root.
   `pyproject`'s `pythonpath = ["."]` reads like it covers this but is a **pytest** setting and
   applies only under a test run — which is exactly why the conformance suite never caught it.
   Fixed in the script rather than by requiring `PYTHONPATH` in the environment, so the job
   behaves identically by hand, under spark-submit, and under whatever working directory
   Airflow gives it in step 5.
2. **`KerberosAuthException: failure to login` — `NullPointerException: invalid null input:
   name`.** uid 1000 has no entry in `apache/spark`'s `/etc/passwd`, so Hadoop's
   `UnixLoginModule` handed `UnixPrincipal` a null name and died in `SecurityManager`'s
   constructor before Spark started. **Identity resolution, not permissions** — no `chmod`
   would have helped. Fixed by mounting the host's passwd file read-only, keeping the uid
   override rather than reverting to the image's `spark` user (185): everything the cluster
   writes lands in a host bind mount, and the P4 verification reads the lake back from the dev
   container. Confirmed — lake files are owned by `vscode`.
3. **A healthcheck the image could not run.** It shelled out to `curl`, which `apache/spark`
   does not ship. It could never have passed, and the worker gates on `service_healthy`, so
   the worker would have waited forever — a symptom that reads as a registration or networking
   bug and sends you looking in the wrong place. The second version was also wrong: the
   master's RPC endpoint binds to the address its hostname resolves to, so `localhost:7077` is
   refused while the container hostname answers (measured directly), *and* a raw TCP connect
   to 7077 makes the master log a netty stack trace plus "got disassociated, removing it" on
   every check. Now probes the web UI on 8080 with bash's `/dev/tcp`, falling back to the RPC
   port on the container's own address.
4. **`spark-submit --master` is silently ignored.** `build_session()` calls
   `.master(config.SPARK_MASTER)` unconditionally, and a builder's explicit `.master()`
   overrides whatever spark-submit was given. The first "cluster" run was really `local[*]`
   inside the master container — visible only as `(executor driver)` in the task lines. The
   env var is the intended mechanism and `config.py` already says so; the submit command was
   simply wrong. **This is a live hazard for step 5:** an Airflow `SparkSubmitOperator` sets
   `--master` from its connection, and this code would ignore that just as quietly. The DAG
   must set `SPARK_MASTER` in the task environment. Left as-is rather than made to defer to
   spark-submit, because detecting "am I under spark-submit" needs a fragile env-var sniff and
   the config-driven route is already the recorded design.

### Data findings worth carrying into P5

- **Rejection rate is not stable across months:** 0.86% / 1.78% / 2.48%. Earlier figures
  (P2's 0.9%, P3's 1.04%, the fixture's 1.10%) were all *samples* of 2023-01; 0.86% is the
  first full-population measurement of that month and supersedes them rather than
  contradicting them.
- **2024-06's higher rate is genuine data, not the casing path.** The structural rejection
  categories barely moved between the two months — `dropoff_not_after_pickup` 1121 to 1140,
  `outside_source_month` 48 to 50 — while the entire increase was `negative_money`
  (25,212 to 61,914). Structural checks flat and the data-dependent check moving is what rules
  out an adapter fault.
- **December 2025 carries 58,019 zero-duration trips** (`dropoff == pickup`), against ~1,130 in
  the other months, and only 2 genuinely negative. A meter/logging artifact, refused
  identically by the gateway's `TripEvent` validator, so hot and cold do not diverge.
  Consequence for P5: the last month before the cutoff runs ~1.3% thinner than its neighbours.
- **Row counts are larger than planned on:** ~3.5–4.3M per month, not ~3M. A 36-month backfill
  is therefore nearer 130M rows than 110M.

### Step 3 — stream_to_lake: the cold path proper  · verified 2026-08-30

`batch_jobs/jobs/stream_to_lake.py` plus `adapters/registry.py` and a mounted
`batch_jobs/conf/spark-defaults.conf`. Reads the whole topic as a batch
(`earliest` -> `latest`), filters to `> cutoff`, dedupes on the natural key, and
rewrites the affected partitions. This is the half of the lake that recomputes
events the hot path already saw; `bulk_load` remains a backfill of files that never
transited the bus.

Design decisions made during implementation:

- **A registry, so a downstream job never names a source.** `source_extras` is stored
  as a typed struct, so whoever writes the lake must know that struct's fields — but
  this job sits downstream of the bus, where the core principle forbids source
  knowledge. `adapters/registry.py` maps a source *name* to its `StructType`, and the
  name arrives as `SOURCE_EXTRAS` configuration. Same arrangement already recorded for
  `KAFKA_TOPIC`: configuration may know the source, code may not. Rejected importing
  `TLC_SOURCE_EXTRAS` directly — two lines shorter, and it turns the swap story from
  "new adapter plus a config value" into "new adapter plus edit every job". The
  registry raises on an unknown name rather than defaulting to an empty struct, which
  would write a lake whose physical schema disagrees with the other writer's and only
  surface much later as a read error.
- **Dedupe here, and deliberately the opposite call from `bulk_load`.** The gateway
  mints a fresh `uuid4` per request, so a re-run replay arrives with new `event_id`s
  and would double every number in the lake. `bulk_load` needs no equivalent because
  partition overwrite already makes its reruns idempotent, and deduping there could
  only destroy real rows. Two jobs, opposite choices, one reason: what a rerun means
  differs between them.
- **The dedupe's feared cost turned out to be zero, measured.** The concern was that
  two genuinely distinct trips sharing all six key fields would collapse into one.
  Measured against 2024-06 in the lake — 3,476,089 rows written by `bulk_load`, which
  does not dedupe, so every row is a distinct raw record — **0 rows were collapsible,
  0.0000%**. Six fields at second resolution make the natural key effectively unique.
  Recorded because the tradeoff was accepted on judgement and is now backed by a
  number.
- **A missing topic is a waiting state, not a failure.** The topic is created by the
  gateway's first produce, so before any replay there is legitimately nothing to read.
  Matches how the hot path treats the same condition. Matched narrowly on known
  markers; anything unrecognized re-raises rather than being swallowed.

### The two bugs, and the distinction they turn on

Both were launcher-versus-runtime confusions, and the second was self-inflicted by the
fix for step 2's bug 2.

1. **`Failed to find data source: kafka`.** The connector was declared through
   `spark.jars.packages` on the session builder, reasoning by analogy with `spark.master`
   after step 2's bug 4 showed submit-line flags being silently overridden here. **The
   analogy is wrong.** `spark.master` is read when the session is built, so config can
   own it. `spark.jars.packages` is read by SparkSubmit *before the JVM exists* — it
   resolves through Ivy and builds the classpath — so setting it from inside a running
   JVM silently does nothing. Same-looking `spark.*` key, different lifecycle. The
   `packages` argument was removed rather than left as a knob that appears to work, and
   the job now translates Spark's bare message into the submit command that fixes it.
   **Consequence for step 5:** the DAG must pass `--packages` on the submit line *and*
   set `SPARK_MASTER` in the task environment. Confusing the two produces a silent
   no-op in one direction and a hard failure in the other.
2. **`FileNotFoundException: /home/vscode/.ivy2/cache/resolved-...xml`.** A direct
   consequence of mounting `/etc/passwd` in `159979a`. Java derives `user.home` from the
   passwd entry rather than from `$HOME`, so making uid 1000 resolvable also gave it a
   home directory that exists on the host and not in the `apache/spark` image — and the
   `HOME=/tmp` already set on both services does not reach Ivy for that reason. Fixed by
   pinning `spark.jars.ivy` in a mounted `spark-defaults.conf`, scoped explicitly to
   settings that must exist before a JVM starts. Rejected adding another flag to every
   submit command: step 5's DAG would have to remember it, and forgetting it fails at a
   distance.

`spark-defaults.conf` is now the third place Spark configuration lives, so the split is
worth stating: `common/spark.py` owns anything a running session can set, this file owns
anything needed before the JVM starts, and the submit line owns per-job connectors.

### Verification run

- **Dedupe, end to end.** Replayed 2026-01 twice (20,000 rows each, 19,625 accepted per
  pass, 1.88% rejected). The job reported **39,250 post-cutoff events, 19,625 after
  dedupe, 19,625 collapsed** — two replays producing a lake identical to one. A repeated
  or botched demo replay is harmless.
- **No false collapse in the single-replay case:** the first pass's 19,625 events deduped
  to exactly 19,625, losing nothing.
- **Two writers, one tree.** `year=2023`, `2024`, `2025` (bulk_load) and `year=2026`
  (stream_to_lake) coexist under `data/lake/trips/` with neither clobbering the other —
  the cutoff split working as designed.
- Conformance suite still green after the `common/spark.py` and `common/config.py` edits.

### Step 4 — aggregate_daily + the serving-table merge  · verified 2026-08-30

`batch_jobs/jobs/aggregate_daily.py`, `sql/schema.sql` and `sql/merge_daily.sql`. Spark
rolls the whole lake up to daily x zone metrics and writes a staging table; the merge
publishes it into `cold_daily_zone_metrics` with `INSERT ... ON CONFLICT DO UPDATE`.

Design decisions made during implementation:

- **The merge is SQL, not Python, and that is forced as well as chosen.** `spark-submit`
  runs jobs with the *container's* Python, not this component's uv environment, so
  `pyspark` is the only library available inside the Spark containers. There is no
  database driver there and no way to add one without a custom image. So the merge is a
  `.sql` file executed by whoever has a client — `psql` during verification, Airflow's
  Postgres operator in step 5. Worth recording as a general constraint: **nothing in
  `batch_jobs/pyproject.toml` reaches the cluster.** It governs what any future cold-path
  job may import.
- **Citywide rows are aggregated from the trips, not from the per-zone averages.**
  Averaging averages would weight a 3-trip zone the same as a 30,000-trip one. Implemented
  as two aggregations unioned rather than a rollup or grouping set, because the union makes
  it obvious which rows are which grain.
- **The serving table deliberately mirrors `trip_window_metrics`** — same
  NULL-means-citywide convention, same money-as-numeric rule, same `COALESCE(zone_id, -1)`
  unique index. PostgreSQL treats NULLs as distinct in a unique constraint, so without that
  trick the citywide row would be re-inserted on every merge, forever. The dashboard reads
  both tables and should not have to learn two vocabularies for one idea.
- **The merge skips rows whose values did not change** (`IS DISTINCT FROM` on every
  metric). Saves dead tuples, and keeps `updated_at` meaning "this number last changed"
  rather than "a job last ran" — which made it the tool that answered a question during
  verification.
- **Both connector coordinates moved into `spark-defaults.conf`.** Forgetting `--packages`
  already caused one failure in step 3, and step 5's DAG would have to remember it per
  task. Declared cluster-wide, no operator can omit them. `bulk_load` pays a cached Ivy
  lookup it does not need — a fair price for deleting a class of mistake. Note this makes
  `spark-defaults.conf` authoritative for *launcher-time* settings, `common/spark.py` for
  session-time ones, and the submit line for nothing at all.

### Verification run

- **Idempotency:** first merge `INSERT 0 21644`, second merge `INSERT 0 0`. The serving
  table is never accumulated into, and a rerun is free.
- **Shape:** 21,644 rows over 93 days, 93 of them citywide — exactly the 31 + 30 + 31 + 1
  days the lake held.
- **Cross-layer reconciliation**, the check this phase's plan called "the one that actually
  proves something". Two independent code paths over the same events:

| date | replays | cold | hot | gap |
|------|---------|------|-----|-----|
| 2026-01-01 | 2 | 19,625 | 21,673 | **-2,048** |
| 2026-01-02 | 1 | 19,561 | 19,561 | **0** |

  On a single clean replay the two layers agree **exactly** — not within a tolerance. On
  the day that was replayed twice they diverge, and the divergence is the hot path
  **over-counting**: it has no dedupe, so it counted the first replay in full plus the part
  of the second that arrived before its watermark had moved past those event times, and
  dropped the rest as late. The cold path deduped on the natural key and holds the true
  figure.

  That is the lambda architecture's thesis demonstrated rather than claimed: the batch
  layer is the number to trust when the two disagree. It is a *better* presentation result
  than two matching numbers would have been.

- **The prediction recorded in the plan was wrong in an instructive way.** It expected a
  small positive gap from the hot path's late-event drops. On a replay driven at `SLEEP=0`
  in strict `pickup_datetime` order, events arrive in event-time order, so the watermark
  never has anything to drop — hence a gap of exactly zero. Late drops need *out-of-order*
  arrival, which this replay does not produce. The mechanism is real; the conditions to
  observe it are not present.

- **Backfilled dates cannot be reconciled and should never be compared.** 2023-01-01 shows
  a gap of +69,819 purely because the cold path read the whole month's file while the hot
  path only ever saw the few thousand events P2/P3 replayed. Different event sets, not a
  discrepancy. Reconciliation is meaningful **only on the post-cutoff half**, where both
  layers genuinely processed the same events.

### The instability the merge's own bookkeeping exposed

A merge reported `INSERT 0 320` while the table grew by only 229 rows, so 91 pre-existing
rows had been rewritten. Grouping by `updated_at` showed the second write batch spanning
2026-01-01 to 2026-01-02 — meaning 91 rows of **2026-01-01 recomputed differently**, on a
day whose trips had not changed at all.

Re-running the aggregation over an unchanged lake produced `INSERT 0 0`, so the job is
**reproducible**. The churn appeared only when the input *grew*. Best explanation, strongly
suspected but not provable now that the old values are overwritten: `avg_distance_km` is
the only metric stored at full precision, floating-point addition is not associative, and
adding a day changed how rows distribute across shuffle partitions and therefore the order
the distances were summed. Counts and `Decimal` sums are exact, and the fare averages are
narrowed to 2 decimals, which rounds any equivalent noise away.

Fixed by rounding `avg_distance_km` to 3 decimals — metre precision on a kilometre figure,
far beyond what a source reporting miles to 2 decimals justifies. The property being
defended is worth stating: **a day's numbers must not depend on which other days exist in
the lake.** Without it every scheduled run would rewrite rows that had not changed, and
`updated_at` would degrade from "this number last changed" into "a job last ran".

Worth noting *how* this was caught: only because the merge skips unchanged rows. A plain
`DO UPDATE` would have rewritten all 21,644 rows every run and hidden the instability
completely. The optimisation paid for itself as instrumentation before it ever paid for
itself as performance.

### Steps 5 and 6 — Airflow, and the backfill  · verified 2026-08-30

`orchestration/` (image + three DAG files) and four Compose services. Both DAGs ran to
success under the scheduler, not just under `airflow tasks test`.

- **BashOperator, not SparkSubmitOperator** — a deliberate deviation from the plan. That
  operator's main contribution is building `--master` and `--packages`, and this project
  resolves both elsewhere: the master URL must arrive as `SPARK_MASTER` because
  `common/spark.py`'s explicit `.master()` would override the flag, and connectors must be
  declared before the JVM exists. The operator would have added an Airflow-connection
  indirection plus `env_vars` semantics never verified here, in exchange for two flags this
  code ignores. `BashOperator` runs the command already proven by hand, so its failure modes
  were known before it ran.
- **`SPARK_CONF_DIR=/opt/spark-conf`** on the scheduler, pointing at the same
  `batch_jobs/conf` the Spark services mount. Without it the driver in this container gets
  neither the Ivy cache path nor the connector packages — reproducing both step-3 bugs
  inside a new image. It worked first try, which is the only reason step 5 was not a third
  round of the same two failures.
- **Client mode puts the driver in the scheduler**, so that service carries the JDK, the
  lake read-write, the raw drop read-only, and `SPARK_DRIVER_HOST`. Confirmed from the logs:
  tasks ran on `172.19.0.2 (executor 0)` with the UI at `airflow-scheduler:4040` — the
  worker executing, the driver local, which is exactly the intended topology.
- **`airflow db migrate` and `users create` run in a one-shot `airflow-init`**, like
  `simulator`. Both idempotent, so a repeated `up` is harmless.
- **The connection is supplied as `AIRFLOW_CONN_NORTHSTAR_PG` in the environment**, not
  created in the UI. A connection living only in the metadata database is invisible in this
  repository and vanishes with the volume.

**The bug: `could not translate host name "postgres"`.** Both SQL tasks failed on DNS.
`airflow-scheduler` declared `depends_on` for what it needs to *boot* — `airflow-init` and
`spark-master` — but not for the services its *tasks* talk to. Docker DNS only resolves
running containers, so bringing up the scheduler alone produced one whose every task failed
on name resolution, which reads as a network fault rather than a container that was never
started. Fixed by depending on `postgres` and `kafka` as well. Worth generalising: a
service's dependencies are what its work touches, not just what its process needs to start.

**A smaller trap, recorded because it will recur.** `devcontainer.json`'s `forwardPorts` is
read only when the container is **created**, so adding 8080 and 8081 there does nothing for
an already-running codespace — the ports must be added by hand in the PORTS panel until the
next rebuild. Codespaces also does not expose ports on the laptop's `localhost` at all; the
forwarded `*.app.github.dev` URL is the only route.

### Step 6 — the backfill, and Phase 4's verification

Twelve months (2025-01..2025-12) via `cold_path_backfill`'s dynamic task mapping, two
concurrent, **17 minutes end to end**, `success`. Twelve rather than thirty-six by the scope
decision taken today; the remaining months need only an extended list and a re-trigger.

| check | result |
|-------|--------|
| CLAUDE.md's P4 assertion | 428 Parquet files, SNAPPY, `year=/month=/day=` |
| serving table | 102,783 rows, 2023-01-01 .. 2026-01-02, written by Airflow |
| `cold_path_backfill` | success, 12/12 mapped tasks |
| `cold_path_incremental` | four consecutive scheduled runs green |
| cross-layer reconciliation | exact agreement on a single replay (step 4) |

- **The 3-minute cadence holds, with a caveat.** Each incremental run takes ~1:45-2:05
  against a 3-minute schedule, so it keeps up — but there is a stable ~3-minute lag left over
  from the initial queue, and `aggregate_daily` re-reads the entire lake every run. At 45M
  rows that is fine; it is the first thing that will stop being fine if the backfill is
  extended to 36 months. The fix then is scoping the aggregation to recent dates, not
  lengthening the schedule.
- **The memory estimate was badly pessimistic.** The plan projected ~10.5GB resident with the
  full stack and moved the codespace to 4-core/16GB on that basis. Measured with all ten
  containers up: **4.7GB** — webserver 1.59, spark-worker 1.22, scheduler 0.79, kafka 0.68,
  and everything else under 300MB. The upgrade was probably unnecessary for memory; the extra
  cores still earn their keep on Spark. Recorded because the estimate drove a decision that
  doubles core-hour burn.
- **Rejection rates keep climbing with the source's age:** 0.86% (2023-01), 1.78% (2024-06),
  2.48% (2025-12), **4.22%** (2025-01). Not investigated per-month; 2025-12's spike was
  traced to zero-duration trips, and the same cause is plausible here. Worth a look in P5
  since it is the training corpus that thins.

### Open

- **Resolved 2026-08-30:** step 1 is fully green, 7/7. The concern behind this item — that
  the null-vs-absent fix might have broken one of the five previously-passing tests — did not
  materialize.
- **Fixed since this entry was written:** the simulator footgun. `iter_records()` now streams
  one month at a time instead of concatenating all 41 (`ce43357`) — a per-file sort still
  yields a globally ordered replay because the out-of-month filter keeps every row inside its
  own file's month. Also derives `--start-month` from `--start-datetime`.
- **Resolved:** TLC has published through **2026-05**; the full range is downloaded.
- 36 months is ~110M rows — more than scikit-learn will want to train on directly. Sampling
  strategy is a P5 problem, flagged here because it's a consequence of this scope choice.
- Disk: ~2GB of parquet plus ~3.5GB of Spark/Airflow images against the codespace's 32GB.
  Should fit; unmeasured.
- The 302 KB fixture is committed as a knowing exception to the no-data rule. If it becomes a
  nuisance, the cheaper route is fewer accepted pairs, not regenerating it on demand — a
  skippable drift guard guards nothing.

- **`bulk_load.py` has no test.** The conformance suite covers the adapter thoroughly and the
  entrypoint not at all, which is precisely why bug 1 reached the codespace. Not obviously
  worth a Spark-session test at this size, but the gap is real and should be a conscious
  choice rather than an oversight.
- **The full backfill is a scope lever — RESOLVED 2026-08-30, see "Operational findings".**
  Settled at 12 months, then reversed to the full 36 once 8.49GB of Spark worker scratch was
  reclaimed. Original text: at ~4M rows/month, 36 months is ~130M rows and ~2GB of Parquet.
  Backfilling 12 months instead would cut step 6's runtime and disk while leaving a corpus far
  larger than scikit-learn will use, and nothing architectural changes — the DAG maps over a
  list either way.
- **Step 5 is the remaining risk concentration.** Three new containers, an Airflow metadata DB
  and a spark-submit client image — the same blind-config surface that produced all four bugs
  above, roughly tripled. Check the base image's toolset *before* writing healthchecks.

- **The Ivy cache lives only as long as the container.** Repeated submits reuse it;
  recreating the container re-downloads the connector. A few seconds, accepted rather
  than managing a bind mount's ownership. Revisit if step 5 recreates containers often.
- **`stream_to_lake` reads the whole topic every run,** which is correct now and is
  bounded by Kafka retention rather than by the lake. Worth re-measuring in step 6 once
  a long replay has built up real topic volume.


## Phase 5 — ML: fare estimation at pickup  · 2026-08-30

**Status: VERIFIED end-to-end 2026-08-30.** Trained, scoring the replayed stream, and
evaluated daily by Airflow.

### The task changed, and why

Recorded earlier as per-trip **tip prediction**. Re-examined at the start of P5 and
replaced with **fare estimation at pickup**. Tip prediction was the weakest thing in the
plan, for three reasons worth keeping because they generalise:

- **It was mostly a data artifact.** TLC records tips only for card payments; cash tips are
  logged as zero. A third of the labels would have been structurally wrong, and a model
  trained on all of them learns "cash implies no tip", which is a recording convention, not
  behaviour.
- **It was nearly determined by another column in the same row.** Card tips cluster tightly
  around a percentage of fare, so `tip ~= 0.2 x fare_amount` is most of the model. The
  metrics would have looked excellent while demonstrating almost nothing.
- **Nobody acts on a predicted tip.** There is no decision it informs.

**Chosen instead: predict what the ride will cost, before it starts.** The rider sees an
estimate and decides whether to book — the upfront-pricing model. The same predictions
support dispatch, and the daily error metric doubles as a drift detector, which is what
makes P6's alerts panel a real feature rather than a decoration.

**Target: `total_amount - tip_amount`** — everything the rider is charged except the part
they choose. Rejected `total_amount`, which folds the tip back in and drags the cash
artifact along with it. Rejected `fare_amount` alone, which omits tolls and surcharges that
a rider genuinely pays and would make the estimate systematically low on tunnel and
congestion-zone trips.

### Features: only what is known before the wheels turn

Pickup zone, dropoff zone, the encoded zone pair, hour, day of week, month, passenger count.

**`trip_distance_km` and `dropoff_datetime` are deliberately excluded.** Both are realised
quantities. Including either would produce an excellent model and a fake prediction — you
cannot quote a price using the distance a taxi has not driven yet. The zone pair carries
distance implicitly, and learning that mapping is the entire job.

Every feature is canonical. Nothing in `ml/` reads `source_extras`, so the layer names no
source — a stronger position than the tip model would have held, since that one wanted
`fare_amount`. `ratecode_id` was dropped for the same reason despite P2 flagging it as a
candidate: it lives in `source_extras`, and reaching in would make the ML layer
source-aware.

### The negative result that justified the baselines

`train.py` reports three models: a global median floor, a **zone-pair median lookup** a
business could build in an afternoon, and the gradient booster. The first version:

| model | MAE | RMSE | R2 |
|-------|-----|------|-----|
| global median | $11.13 | $19.72 | -0.11 |
| zone-pair median | $4.24 | $8.66 | 0.786 |
| gradient boosting (v1) | $6.06 | $10.05 | 0.712 |

**The model lost to its own baseline by 43%.** Without the lookup we would have shipped
"MAE $6.06, R2 0.71" and called it a success.

Diagnosis: the two zones were target-encoded **independently**, which destroys the
interaction that sets the price. "Midtown" averages ~$18 and "JFK" averages ~$60, but
neither marginal says that *this pair* is a long airport run. Two marginals cannot
reconstruct 69k pair-specific prices; the lookup keeps exactly that information.

Fix: encode the zone pair as its own feature, so the model starts from what the lookup
knows and adds what it cannot express.

| model | MAE | RMSE | R2 |
|-------|-----|------|-----|
| zone-pair median | $4.24 | $8.66 | 0.786 |
| gradient boosting (v2) | **$3.91** | $7.91 | **0.822** |

**Beats the lookup by 7.8%.** Modest, and that modesty is the finding: the route determines
most of what a ride costs, and hour, weekday and season add about eight percent on top.
That is a more informative claim than a large number would have been, and it is only
available because the baseline exists.

### Design decisions

- **`features.py` is shared by training and serving.** Training/serving skew raises no
  error; it silently returns wrong numbers. One definition of a feature row, one column
  order, imported by both sides.
- **Zones are target-encoded, not categorical.** TLC has 265 zones and
  HistGradientBoosting caps native categorical cardinality at `max_bins` (255), so zone ids
  as categories fail at fit time. Target encoding is also the better representation — an
  encoded zone *is* "what trips from here typically cost" — and scikit-learn's
  `TargetEncoder` cross-fits, so a row's encoding never comes from its own target.
- **`loss="absolute_error"`**, because the quoted metric is "typically within $X". Squared
  error would trade many small errors for a few large ones, which is the wrong bargain for
  a price estimate.
- **The holdout is chronological, not random.** Fit on records at or before the cutoff;
  score only records after it. The predictor skips pre-cutoff events still on the topic —
  quoting a price for a trip the model trained on would flatter it.
- **Evaluation is pure SQL**, because the predictor records the outcome beside the quote.
  The orchestration image therefore needs no scikit-learn and never touches the artifact.
  Rejected a Spark job joining predictions against the lake: more independent, but it needs
  a whole job, an `ml/` mount into the Spark containers, and a second definition of the
  target.
- **One image for `ml_train` and `ml_predictor`.** A model pickled by one scikit-learn
  version and unpickled by another may load and answer differently rather than failing.
  Sharing an image makes the versions impossible to diverge.

### The holiday finding — the one to talk about

The model systematically under-quoted its very first day of live scoring: predicted $21.88
against $33.58 actual on 2026-01-01.

The obvious explanation was wrong. The first hypothesis was that 2026 fares had risen — a
regime shift. The cold path's own daily aggregates refuted it: **2026-01-02 averages $30.59
per trip, squarely inside late December's $27.71-$32.69 range.** No shift.

What actually happened is narrower and more interesting. **2026-01-01 averages $36.11, far
above every surrounding day** — New Year's Day has about a fifth of the usual trip volume
(19,625 against ~100,000) and the trips that do happen are longer and pricier.

**The model cannot know this.** Its calendar features are hour, day-of-week and month.
1 January 2026 is a Thursday, so it confidently priced a normal January Thursday. There is
no holiday feature.

Why this is worth presenting rather than hiding:

- The daily evaluation caught a real, explicable model weakness on its first run. That is
  precisely what a daily evaluation is *for*, and it demonstrates the ML layer working as a
  system rather than as a metric.
- The fix is obvious and nameable — a holiday/calendar flag — without having to be built.
  Knowing what is missing is a result.
- It makes P6's alerts panel meaningful: a spike in daily MAE against a stable baseline is
  exactly the alert worth showing.

### The caveat on every forward number so far

Three compounding limits, all of which resolve with more replay:

1. **The prediction set spans ~1.5 event-time days** — all of 2026-01-01 and 2026-01-02
   until 07:53. That is the whole of what the simulator has replayed past the cutoff.
2. **One of those two days is an extreme holiday**, so half the forward evidence is an
   outlier.
3. **That holiday is double-weighted.** 2026-01-01 was deliberately replayed twice in P4
   step 3 to test the cold path's dedupe. The cold path collapsed the duplicates; the
   predictor did not, because the gateway mints a fresh `event_id` per request — so
   `fare_predictions` holds 39,250 Jan-1 rows against Jan-2's 19,561. **Roughly two thirds
   of the prediction table is New Year's Day.**

Consequence: the pooled `avg_predicted` / `avg_actual` figures are not a fair summary of
the model. The per-day `ml_daily_eval` rows are unaffected, since they group by event-time
date. Do not quote a headline forward MAE until a longer replay has run.

### Verification run

204,944 predictions over four event-time days, evaluated by `ml_daily_eval`:

| day | predictions | MAE | RMSE | MAPE | R2 |
|-----|-------------|-----|------|------|-----|
| 2026-01-01 *(New Year's Day)* | 39,250 | **$13.73** | $22.38 | 31.7% | **0.025** |
| 2026-01-02 | 19,561 | $4.94 | $11.51 | 17.5% | 0.713 |
| 2026-01-03 | 105,737 | $4.50 | $10.58 | 16.8% | 0.732 |
| 2026-01-04 | 40,396 | $4.36 | $10.19 | 16.0% | 0.737 |

**The model generalises forward.** Normal days sit at $4.36-$4.94 against a training MAE of
$3.91 — the mild degradation expected on genuinely unseen data, on a chronological holdout
rather than a random one.

**The holiday failure is sharper than MAE alone suggests.** New Year's Day is 3.1x the error,
but the number to quote is **R2 = 0.025**: on that day the model explains essentially none of
the variance in price. It is no better than guessing the average. Every other day sits at
0.71-0.74.

Note `predictions` for 2026-01-01 is double its true trip count, because that day was
replayed twice during P4 step 3's dedupe test and the predictor does not dedupe (the gateway
mints a fresh `event_id` per request). The error metrics are unaffected — duplicates are
identical trips, so they do not move an average — but the count is not a trip count.

### Two bugs found by running it

- **The predictor could not keep up with its own input.** Measured ~120 events/sec against a
  replay producing ~420/sec, so it fell steadily behind and a 205k re-score was heading for
  half an hour. `model.predict()` was being called on a **one-row DataFrame per event**,
  paying the whole pipeline's fixed cost — encoder transform plus booster setup — for a
  single number. The database writes were already batched; the model call, the expensive
  half, was not. Now one `predict()` per batch, through the same `build_features` path, so a
  batch of N and a batch of 1 remain identical code. Worth noting *why* it surfaced: only a
  re-score made throughput visible. In normal operation the service would have drifted
  further behind the replay with nothing reporting it.
- **Evaluating during a re-score produces incoherent rows.** The first evaluation ran while
  the predictor was rewriting `fare_predictions` with v2, and produced per-version rows that
  did not partition — 16,650 v1 rows and 39,250 v2 rows for a day holding 39,250 predictions
  in total. The evaluation is a full recompute over a table another service is actively
  rewriting and has no way to know it. **Operational rule: evaluate after a re-score
  finishes, never during.** Those rows were deleted; retired model versions are kept
  deliberately (`model_version` is in the primary key precisely so two models' error series
  stay separable), but only when they describe predictions that actually existed.

### Open

- **No holiday feature.** The named fix for the finding above. A calendar flag — public
  holidays, and probably day-before/day-after — is the obvious next iteration, and the
  clean way to show it would be a `fare-hgb-3` evaluated against v2 on the same days.
- **Changing the model does not re-score anything.** The consumer group has already read
  past those events, so rows keep their old prediction and old `model_version` until the
  group is reset. The command is recorded in `predictor.py`; it is an operational step, not
  something the code can infer.
- **Four days of forward data.** Enough to separate the holiday from normal days, not enough
  to characterise weekly or seasonal behaviour. The replay is capped by `MAX_ROWS`, not by
  anything structural.


## Phase 6 — Streamlit dashboard  · 2026-08-30

**Status: BUILT, partially verified.** The base page renders against live data. The live
scoring feed, the event-time range filter and the replay-command panel were added after that
first render and have not been checked in a browser yet.

### What it is, and what it deliberately is not

A **read-only view** over the serving store. It owns no tables, writes nothing, and reads
three tables written by three components that do not know it exists — `trip_window_metrics`
from the hot path, `cold_daily_zone_metrics` from the cold path, and `fare_predictions` /
`ml_daily_eval` from the ML layer. That is why it can be restarted mid-demo without
consequence, and why a bug in it cannot corrupt anything.

**The request that was turned down, and why.** The original ask was a date picker that
*starts* a simulation from the chosen date. Doing that means launching a container from a web
page, which means mounting the Docker socket into it — trading a real architectural property
for a button. Instead the sidebar renders the exact `docker compose run` command for the
chosen date and event count, so the demo is still driven from one screen and the read-only
property survives. If a real button is wanted later, the clean route is an Airflow DAG
triggered through its API: orchestration is the component whose job is starting jobs.

### A conceptual correction worth keeping

The proposed feed had quoted and charged appearing separately — the quote first, the true
fare "arriving a few minutes later" and updating the row. That is how it works in production
and **not** how it works here: a replayed event describes a trip that already finished, so it
carries its own fare, and the predictor writes both columns milliseconds apart. There is no
later moment when truth shows up.

What is real, and what the dashboard shows instead, is the **cold path correcting the hot
path**. The hot path publishes a count within seconds from in-memory windows that drop late
events and do not deduplicate; the cold path republishes the same day minutes later,
recomputed from the whole log. That is a genuine value that changes, from a genuine second
pass, and it is the lambda architecture's actual claim rather than a simulation of it.

### Chart decisions

- **No dual-axis charts, anywhere.** Trip counts and dollars are different scales, so they
  get separate charts. Two y-axes on one plot let an author imply any correlation they like
  by choosing the scales; it is the most common way a chart lies.
- **The categorical palette was validated, not chosen by eye.** Blue `#2a78d6` and orange
  `#eb6834` clear the lightness band, the chroma floor, colour-vision-deficiency separation
  (worst all-pairs dE 24.7 against an 8 target), the normal-vision floor and 3:1 contrast
  against the `#fcfcfb` surface the theme pins. The theme is pinned to light deliberately:
  those contrast figures are only meaningful against the surface the chart actually renders
  on, so a dark mode needs its own validated steps rather than an automatic inversion.
- **Colour follows the entity, never its rank.** Charged is always blue, quoted always
  orange, with fixed scale domains so filtering a series cannot repaint the survivor.
- **Two series always carry a legend**, and the error band in the feed is a coloured
  *number* with a spelled-out caption — never a bare coloured dot. Colour reinforces the
  value; it never carries the meaning alone.

### Alerts read other components' numbers

The dashboard invents no analysis. Each rule reads a figure some other component computed, so
any alert's evidence is a row someone else wrote:

- **Model error** compares each day's MAE against the *median of the other days* rather than
  a fixed threshold. It asks "is this day unlike the others", which keeps working across
  retrains and price shifts; a fixed threshold would need re-tuning after both.
- **Explanatory power** is a separate rule from error, because they say different things. A
  high MAE on a day of expensive trips can still be a working model; an R2 near zero means
  the model is not tracking *which* trips are expensive, which is a failure of the model
  rather than a hard day. New Year's Day trips this one and not the other way round.
- **Layer divergence** flags hot-vs-cold gaps over 5%, and says in the alert text that the
  cold path is the number to trust.
- **Scoring idle** is the only wall-clock rule, deliberately: it asks whether the *service*
  is alive, which is a question about now, not about when the trips happened.

Every alert ships with an icon, a title and a sentence of why. A red dot alone tells a
colourblind reader nothing and everyone else nothing actionable.

### Open

- **Not yet eyeballed.** The palette validator checks colour, not layout. Label collisions,
  overflow and column widths need a browser pass.
- **`dropoff_datetime` is NULL on rows scored before it existed.** The column was added to a
  running system, and ~205k rows predate it. Re-scoring after a consumer-group reset would
  fill them; leaving it means the feed shows blank durations for early trips.
- **No dark mode.** Deliberate rather than missing — see the palette note above.

---

## Operational findings — 2026-08-30

Two things found while preparing to extend the backfill, neither of which any test would have
caught, and both worth keeping because they are about *running* the system rather than
building it.

### Spark's worker was quietly eating the data lake's disk

`/workspaces` and `/var/lib/docker` are **the same 32GB filesystem** on this machine. So
Docker's footprint competes directly with the Parquet lake, which is a bind mount into the
repository.

`docker ps -s` showed **spark-worker holding an 8.49GB writable layer**, against tens of
megabytes for every other container. The cause: `SPARK_WORKER_DIR=/tmp/spark-work` holds every
executor's scratch, stdout and stderr, and **Spark's worker never cleans it up by default**.
Roughly 40 spark-submits had accumulated that.

Recreating the container reclaimed all of it — 4.0GB free became 12GB — and
`SPARK_WORKER_OPTS` now enables cleanup (every 15 minutes, keeping 30 minutes of history so a
recent failure can still be inspected).

**Why it matters beyond the fix:** nothing reported this. It surfaced only because we asked
whether there was room for more months. Left alone it would have failed mid-backfill as a
confusing write error rather than an obvious "out of space", with a half-loaded lake.

### The backfill scope decision, reversed on evidence

Recorded earlier the same day as **12 months**, chosen because 36 looked like it would not
fit. After reclaiming the 8.49GB, 36 months fits with ~4.5GB to spare, so the range is now
**2023-01 .. 2025-12** — three continuous years rather than three disconnected islands, which
is what the dashboard's trend chart actually wants.

The range moved from a hardcoded list into `BACKFILL_START` / `BACKFILL_END` configuration at
the same time, because the binding constraint is disk and scope should be trimmable without
editing a DAG. Measured consumption is **~330MB of lake per month**, not the ~200MB estimated.

### `aggregate_daily` no longer re-reads the whole lake

It recomputed every day in the lake on every 3-minute run. At 45M rows that took ~2 minutes;
at 110M it would have exceeded its own schedule and the DAG would never have kept up.

The waste is structural rather than incidental: **backfilled history never changes**, so
re-aggregating 2023 every three minutes reproduces numbers already in the serving table. So
`--since` now defaults to the cutoff — the only range the Kafka writer moves — and
`cold_path_backfill` runs one full pass (`--since 1970-01-01`) plus the merge after its months
land, so loading months and publishing their metrics happen together.

Rejected simply lengthening the schedule: it trades a slower dashboard for a problem that
returns the next time the backfill grows.

---

## Open / deferred decisions (cross-phase)

- **ML task — SUPERSEDED 2026-08-30, see Phase 5.** Replaced by fare estimation at pickup;
  tip prediction was mostly a recording artifact (cash tips are logged as zero) and nearly
  determined by `fare_amount`. Original text kept for the record:
  Per-trip **tip prediction** is the core model: train on <=cutoff records,
  predict as later records replay, compare predicted vs actual, and run a daily Airflow
  evaluation (maps onto the rubric's "compare earlier predictions with actual" +
  "evaluate every day"). Aggregate demand/revenue **forecasting** remains a possible
  extension. Pick one target per model; don't build one model secretly trying to be both.
- **Streaming = replay, not live data.** TLC is historical; we simulate "live" by
  replaying post-cutoff records in `pickup_datetime` order. Batch vs stream is a
  freshness-vs-completeness tradeoff (lambda architecture) over the *same* events.
  <=cutoff trains the model (batch), >cutoff replays as the stream.
- **Data scope: 2023-01 .. 2026-05 (post-COVID).** Originally recorded as "2023–2025 only";
  **extended forward to 2026-05 on 2026-08-16**, which is the last month TLC has published.
  The exclusion that matters is unchanged and still deliberate: **2020–2022 stays out**,
  because pandemic-era ridership is a regime shift (collapsed volumes, distorted
  zone/fare/tip patterns) the model would wrongly learn as signal, hurting predictions on
  normalized traffic. Extending the *upper* bound doesn't touch that reasoning — it only
  buys more post-cutoff months to replay.
- **Cutoff pinned: `2025-12-31T23:59:59`.** Decided 2026-08-16, in P4 rather than P5,
  because the cold path needs it first (see Phase 4 — it partitions which writer owns which
  dates). 41 months total splits into **36 months ≤ cutoff** (~110M rows, the training
  corpus) and **5 months > cutoff** (2026-01..05, ~15M rows ≈ 10h of replay at `SLEEP=0`) —
  enough runway that a demo never runs dry. It also lands on a calendar-year boundary, which
  makes the story easy to tell: *"the model has seen through 2025; 2026 is arriving live."*
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

## Operational findings + fixes — 2026-08-31

The backfill finished. Three things went wrong on the way, and two of them were the
platform telling the truth about a real defect rather than misbehaving.

### The backfill's last two tasks: a DAG race, not a hang

All 36 `bulk_load` months succeeded, then `aggregate_history` **failed after 94 seconds**
against an expected 10-20 minutes — the giveaway that it died on startup rather than
part-way:

```
SparkFileNotFoundException: File file:/data/lake/trips/year=2026/month=1/day=3/
part-00001-....snappy.parquet does not exist
```

**Cause:** `cold_path_incremental` was unpaused and firing every three minutes.
`stream_to_lake` reads the topic `earliest` -> `latest` on *every* run and rewrites all
post-cutoff partitions with dynamic overwrite — new events or not. `aggregate_history`
needs an uninterrupted multi-minute scan of the whole lake, including those same 2026
partitions. It cannot win that race; the rewrite deleted a file mid-scan.

Not a fluke — **near-100% reproducible**. The backfill is designed as a one-shot to run
*before* the recurring DAG is live, and nothing enforces that.

**Fix applied:** pause `cold_path_incremental`, wait for `stream_to_lake` to finish, clear
the two dead tasks, let them rerun, unpause. Worked first time: `aggregate_history` took
2m34s.

**Deferred, not done:** an Airflow pool with one slot shared by both DAGs' Spark tasks, so
they serialize instead of racing. Rejected doing it inline because the backfill was the
priority and a pool changes scheduling behaviour for every future run — it deserves its own
change. The tradeoff to weigh then: a long backfill would *block* incremental runs rather
than corrupt them, which is the right failure but a visible one.

### Pausing a DAG strands its in-flight run — twice in one session

Already recorded on 2026-08-30 for `cold_path_backfill`; hit again immediately for
`cold_path_incremental` while applying the fix above. A paused DAG keeps its
currently-running task but parks every remaining task at `scheduled` indefinitely, so the
run never leaves `running`.

**Consequence for any "wait for it to finish" check:** waiting on the *run* state hangs
forever. Wait on the specific **task** instead — here only `stream_to_lake` writes the lake,
so that task reaching `success` is the real safety condition, whatever the run says.

### The documented `tasks clear` command was wrong

The resume checklist's recovery command silently did nothing, reporting `Nothing to clear`:

```bash
airflow tasks clear cold_path_backfill --start-date 2026-08-30 --end-date 2026-08-30
```

Both dates parse to **midnight**, making a zero-width window that excludes a run whose
execution date is `13:14:25`. Corrected form — exact timestamps, or a bracketing range:

```bash
airflow tasks clear cold_path_backfill -t "aggregate_history|merge_into_serving" \
  --start-date "2026-08-30T13:14:25+00:00" --end-date "2026-08-30T13:14:25+00:00"
```

Run it **without** `--yes` first: the confirmation prompt lists what it will clear, which is
the only cheap guard against a regex that matches more than intended.

### The "corrupted zone" that wasn't — and an ad-hoc query that was

A day appearing ~5x its neighbours in `cold_daily_zone_metrics` looked like double-counting.
It was not. Two separate things were confusing the picture:

1. **A bad diagnostic query.** `SELECT sum(trip_count) ... GROUP BY metric_date` counts the
   `zone_id IS NULL` **citywide rollup row on top of the per-zone rows it summarises** —
   every figure doubled. Confirmed exactly: a day showing 4,810 had precisely 2,405 scored
   trips. Any ad-hoc query over this table must filter `zone_id IS NOT NULL` or read the
   rollup alone; the two must never be summed together.
2. **Uneven replay coverage, which is real but harmless.** Against source counts:

   | date | in source | in lake | coverage |
   |---|---|---|---|
   | 2026-01-01 | 114,466 | 19,625 | 17% |
   | 2026-01-02 | 100,054 | 19,561 | 20% |
   | 2026-01-03 | 108,632 | 105,737 | 97% |
   | 2026-01-04 | 93,622 | 40,396 | 43% |

   Every day sits at or *below* source, so nothing multiplied — earlier sessions simply
   replayed different spans. Jan 3 is the only near-complete day.

**The check that settles duplication-vs-coverage:** compare against source, and verify
`sum(trip_count) FILTER (WHERE zone_id IS NOT NULL)` equals the citywide row per day. It did,
on every day — the aggregation is internally consistent.

### Gateway: `event_id` is now derived, not random

**The real defect, found while investigating the above.** `ml/sql/schema.sql` documents
`fare_predictions.event_id` as *"the canonical event id, which the batch adapter derives from
the natural key. Primary key, so re-consuming an event rewrites its quote rather than
recording a second one."* The batch adapter does exactly that. **The gateway did not** — it
minted a `uuid4()` per request, reasoning it "sees each record once."

That premise is false in a replay-driven system: the simulator replays the same historical
trips on every run, so the same trip arrived with a different id each time and the primary
key stopped deduplicating anything. Error metrics were silently weighted by how often a trip
happened to be replayed.

**Fix:** `derive_event_id()` in `gateway/adapters/tlc_adapter.py` — SHA-256 over the same
six-field natural key the cold path dedupes on, UUID-shaped. It lives in the adapter because
it reads TLC field names; putting it in `main.py` or the canonical schema would breach the
source-independence rule. `adapt_tlc()`'s signature is unchanged, so it stays pure and
nothing downstream moved.

- **Rejected: migrating `fare_predictions` to a natural-key primary key.** The schema's
  guarantee was already correct; only the gateway was violating it. A migration would have
  changed the table to accommodate a bug rather than fixing the bug.
- **Rejected: byte-identical ids with the batch adapter.** Spark renders numbers by its own
  rules (`132` vs `132.0`, and float formatting differs from Python's), so matching two
  runtimes is fragile and buys nothing — batch owns 2023-2025, the gateway owns 2026, the
  ranges never overlap, and nothing joins on `event_id` across them. Determinism *within*
  the gateway path is the whole requirement.
- **Cost accepted:** two genuinely distinct trips sharing all six fields collapse into one
  prediction. Identical to the tradeoff `_deduplicate` already documents in the cold path,
  and far cheaper than double-counting whole replays.

**Important limit:** `event_id` is stamped into the Kafka payload at produce time, so the
~207k messages already in the topic keep their random ids permanently. The fix applies only
to newly produced messages — **do not reset the predictor's consumer group** expecting a
re-score to rebuild them with derived ids. It won't.

### Dashboard: the scoring feed was filtered out of its own live data

The hot-path panels visibly refreshed while the per-trip quote feed sat still. Not a fragment
problem — both are `@st.fragment(run_every="10s")`. The hot panels have **no date filter**;
the feed is bounded by the sidebar range, whose maximum comes from `prediction_range()` **at
page load**. Trips scored after that fall outside the window, so the feed re-queried every ten
seconds against a range nothing new could enter, and looked frozen while working perfectly.

A live feed that needs a page reload to show live data is not a live feed.

- `live_predictions` / `prediction_windows` accept `end=None` for an open upper bound.
- Sidebar **"Follow live"** checkbox, **on by default**, drops the feed's upper bound. The
  history panels still respect the range.
- The caption states which mode it is in — "as they are scored" vs "in the selected range".
  Rejected leaving the wording static: it would actively mislead exactly when a reader is
  trying to work out why the table is or isn't moving.
- **Feed moved above the hot path**, directly under the alerts — it is the panel most worth
  watching during a replay, so it gets the position that needs no scrolling. Rendered into a
  `st.container()` reserved at the top, because the fragment is defined further down the file:
  Python needs the `def` before the call, the reader wants the panel before the charts.

### Still open

- Browser pass on the reordered dashboard. Specifically unverified: whether a self-refreshing
  fragment rendered into an earlier-declared container keeps auto-updating. If it doesn't, move
  the whole `scoring_feed` definition above `hot_panel()` and drop the container.
- The Airflow pool described above.
- Evening out 2026 coverage. Now safe to re-replay any range, since ids are derived — but the
  ~207k pre-fix prediction rows carry random ids and can never be overwritten, so a genuinely
  clean 2026 needs them deleted first.
- Retrain on the full 36-month corpus (the model still reflects 14).

---

<!-- Template for new entries:

## Phase N — <name>  · YYYY-MM-DD
**Status:**
### <area>
- Decision — why. Rejected <alternative> because <reason>.
### Open
- ...
-->
