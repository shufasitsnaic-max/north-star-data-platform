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

**Status: PLANNED — nothing in this section is implemented yet.** Recorded before writing
code so the design survives a lost chat session. Anything here may still change during
implementation; the entry gets a `VERIFIED` status and a verification run appended when P4
actually passes. Read it as intent, not as a description of the repo.

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

### Open

- **Simulator footgun:** setting `START_DATETIME` without `START_MONTH` makes `build_dataset`
  load and globally sort all 41 months (~120M rows) and OOM. `discover_files()` already
  supports the range flags, so the fix is a ~3-line default deriving `start_month` from
  `start_datetime`. In scope for P4 since P4 is what makes 41 months present.
- Whether TLC has actually published through 2026-05 at fetch time. If the last month or two
  are missing, the cutoff still holds; the replay just has less runway.
- 36 months is ~110M rows — more than scikit-learn will want to train on directly. Sampling
  strategy is a P5 problem, flagged here because it's a consequence of this scope choice.
- Disk: ~2GB of parquet plus ~3.5GB of Spark/Airflow images against the codespace's 32GB.
  Should fit; unmeasured.

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
