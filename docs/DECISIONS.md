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

**Status:** complete. All P1 verification cases pass (valid → 200, corrupt types → 422,
dropoff-before-pickup → 422).

### Architecture
- **Two-model adapter pattern.** `TLCTripInput` (mirrors the raw TLC yellow-cab record)
  → `adapt_tlc()` (pure transform, the only source-specific piece) → `TripEvent`
  (canonical, source-agnostic contract). Everything downstream of the adapter sees only
  `TripEvent`. This is the seam that makes a future source swap contained.
- **Endpoints:** `POST /events/trips` (validate → adapt → return canonical event) and
  `GET /health`. For Phase 1 the endpoint returns the adapted `TripEvent` directly, since
  Kafka doesn't exist yet — lets us verify the full pipeline with curl.

### Schema decisions
- **Location — `zone_id` canonical, `lat`/`lon` optional ("at least one of").**
  Rejected lat/lon-only: modern TLC has no GPS, only 263 zone IDs, so forcing lat/lon
  would mean geocoding zone centroids — inventing precision the source never had and
  losing the honest `zone_id`. TLC populates `zone_id`; a future LTA source populates
  `lat`/`lon`; neither fakes the other.
- **Surcharges — collapse to `surcharges_amount`, but preserve detail in `source_extras`.**
  Canonical model exposes the lump sum ("add-ons beyond the meter" is universal);
  itemized NYC surcharges, `ratecode_id`, and `store_and_fwd_flag` are kept in a
  `source_extras` passthrough dict for audit and — importantly — as candidate ML
  features later (`ratecode_id` flags airport/flat-fare trips that likely tip differently).
- **Units — `trip_distance_km`, converted from TLC's miles in the adapter.** Source-neutral
  unit chosen now to avoid a silent bug when a km-native source is added.
- **`payment_type`** — generalized enum (CASH | CARD | OTHER | UNKNOWN), defaults to
  UNKNOWN, from TLC's numeric codes.
- **Event time — `pickup_datetime` is non-nullable and never defaulted to `now()`.**
  This is the record's own event time, which becomes the basis for Flink watermarking in
  Phase 3. Deliberate: replaying historical data means event time ≠ arrival time.
- Semantic checks (dropoff > pickup, non-negative amounts) live as Pydantic validators on
  `TLCTripInput`, so they surface as 422s rather than producing nonsense canonical events.

### Ingestion shape
- **Single-event POST.** One request = one `TripEvent` = one future Kafka message. Replay
  looping lives *outside* the gateway (a future simulator reads historical files and POSTs
  in `pickup_datetime` order). Rejected a batch/array endpoint — it would blur the
  one-event-one-message invariant. If replay throughput bites later, batch on the Kafka
  *producer* side, not the HTTP contract.
- The gateway stays a pure validate→adapt boundary — no awareness of replay, batching, or
  historical-vs-live.

### Tooling
- **uv per component.** `gateway/` is its own uv project (`pyproject.toml` + `uv.lock`),
  `uv init --bare`. No `requirements.txt`, no shared mega-environment. Because gateway is
  its own project root, imports have no `gateway.` prefix; run from inside `gateway/`.
- **Run / verify locally:** `cd gateway && uv sync && uv run uvicorn main:app`.

---

## Open / deferred decisions (cross-phase)

- **ML task split.** Per-trip **tip prediction** = Phase 6 core (fits the spec's
  predict-on-arrival / compare-on-completion shadow-model loop). Aggregate demand/revenue
  **forecasting** = first *extension* (time-series off the batch warehouse). Pick one as
  the target per model; don't build one model secretly trying to be both.
- **Streaming = replay, not live data.** TLC is historical; we simulate "live" by replaying
  post-cutoff records in `pickup_datetime` order. Batch vs stream is a freshness-vs-
  completeness tradeoff (lambda architecture) over the *same* events — not past-vs-future
  data. Suggested cutoff ~Jan 2024: ≤cutoff trains the model (batch), >cutoff replays as
  the stream.
- **Event-time / watermarking** to be implemented properly at Flink (Phase 3), built on the
  non-nullable `pickup_datetime` decided in Phase 1.
- **Future source swap** (TLC → Singapore LTA taxi API + weather API) = new adapter + schema
  adjustment only; nothing downstream of the adapter should change. This is the whole point
  of the canonical contract.

---

<!-- Template for new entries:

## Phase N — <name>  · YYYY-MM-DD
**Status:**
### <area>
- Decision — why. Rejected <alternative> because <reason>.
### Open
- ...
-->
