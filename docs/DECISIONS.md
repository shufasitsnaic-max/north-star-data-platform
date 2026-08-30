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
- **The full backfill is a scope lever.** At ~4M rows/month, 36 months is ~130M rows and ~2GB
  of Parquet. Backfilling 12 months instead would cut step 6's runtime and disk while leaving
  a corpus far larger than scikit-learn will use, and nothing architectural changes — the DAG
  maps over a list either way. Undecided; re-triggerable later.
- **Step 5 is the remaining risk concentration.** Three new containers, an Airflow metadata DB
  and a spark-submit client image — the same blind-config surface that produced all four bugs
  above, roughly tripled. Check the base image's toolset *before* writing healthchecks.

- **The Ivy cache lives only as long as the container.** Repeated submits reuse it;
  recreating the container re-downloads the connector. A few seconds, accepted rather
  than managing a bind mount's ownership. Revisit if step 5 recreates containers often.
- **`stream_to_lake` reads the whole topic every run,** which is correct now and is
  bounded by Kafka retention rather than by the lake. Worth re-measuring in step 6 once
  a long replay has built up real topic volume.

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

<!-- Template for new entries:

## Phase N — <name>  · YYYY-MM-DD
**Status:**
### <area>
- Decision — why. Rejected <alternative> because <reason>.
### Open
- ...
-->
