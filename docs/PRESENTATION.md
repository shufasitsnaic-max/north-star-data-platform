# North Star — presentation source

Slide-by-slide source material for a deck about this project. Each `##` is one slide:
a **Point** (the single idea it must land), **Content** (what goes on it), **Visual**
(what to draw), and **Say** (speaker notes, not slide text).

The through-line: *the same events, processed twice on purpose — and what it takes to
make two answers agree.*

---

## 1 — Title

**Point:** Set the frame in one sentence.

**Content:**
- **North Star**
- A lambda-architecture data platform over NYC taxi trips
- Real-time and batch over one event stream, with a live fare model on top

**Visual:** Full-bleed title. A single faint pipeline line running left to right, with
one node splitting into two — the shape the whole talk explains.

**Say:** Personal capstone, built incrementally over six phases. Every phase had to pass
a verification routine before the next began.

---

## 2 — The question the project answers

**Point:** Frame the real engineering tension, not the tech list.

**Content:**
- Fast answers and correct answers are different problems
- Streaming gives you *seconds*, but only within a watermark, and only once
- Batch gives you *completeness*, and can be re-run when logic changes
- Most systems pick one and regret it

**Visual:** Two columns — "Fast / approximate / once" vs "Complete / correct / rerunnable"
— with a question mark between them.

**Say:** Lambda architecture's answer is to refuse the choice: run both over the same
event stream, and let the batch layer hold the number you trust.

---

## 3 — The data

**Point:** Real, large, public, and messy enough to be interesting.

**Content:**
- NYC Taxi & Limousine Commission yellow-taxi trip records
- Project scope **2023-01 → 2026-05** — 41 monthly Parquet files
- **~110M rows** at or before the cutoff; **~15M** after it
- ~2GB compressed

**Visual:** A timeline bar 2023 → 2026 with a vertical cut near the end labelled
**cutoff: 2025-12-31**. Left side "history / training". Right side "arriving live".

**Say:** The cutoff is the single most important design decision in the project. Everything
else follows from it.

---

## 4 — The trick: replay as "real time"

**Point:** A historical dataset can drive a genuinely real-time system.

**Content:**
- A simulator replays records **in `pickup_datetime` order** through the live gateway
- ~420 events/sec measured → roughly **366× event-time compression**
- A year of event time passes in about a day of wall clock
- Everything downstream is genuinely streaming; only the *clock* is compressed

**Visual:** Two parallel time axes — wall clock (short) and event time (long) — with
arrows mapping a compressed segment onto a stretched one.

**Say:** This is why "daily batch" runs on a three-minute schedule here. A `@daily` DAG
would fire zero times during a ten-minute demo while the replay burned through a year.

**Note the engineering detail worth one line:** sorting 110M rows globally would mean
holding them all in memory. Because the files are monthly and non-overlapping, sorting
*within* each file and reading files in order produces the same global sequence — for free.

---

## 5 — Architecture

**Point:** One stream in, two paths out, two consumers of the result.

**Content:**

```
simulator ──▶ FastAPI gateway ──▶ Kafka (tlc-raw-events)
              validate ─▶ adapt          │
                                         ├──▶ HOT:  Python consumer ─▶ rolling windows ─▶ PostgreSQL
                                         ├──▶ COLD: Airflow ─▶ Spark ─▶ partitioned Parquet lake
                                         └──▶ ML:   predictor scores each trip ─▶ PostgreSQL
                                                     │
                                         Streamlit dashboard ◀── PostgreSQL
```

**Visual:** The hero diagram. Kafka as the hinge. Hot path in one colour, cold path in
another, ML in a third. The dashboard hanging off Postgres, reading only.

**Say:** Nine services in Docker Compose. Each concern is its own container — never merged
to save time, because the whole point is that the tiers are independent.

---

## 6 — The rule that shaped everything: a source-independent core

**Point:** The most valuable constraint in the project, and it's an architectural one.

**Content:**
- The canonical event contract — a Pydantic model plus the Kafka schema — is **source-agnostic**
- All TLC-specific parsing lives behind **one adapter**
- **Nothing downstream of Kafka may name a data source**
- Swapping TLC for another source = write a new adapter, adjust the schema. No downstream rewrite.

**Visual:** A funnel: many possible sources → one adapter → a single clean contract →
everything downstream drawn in one uniform colour to show it doesn't care.

**Say:** The contract keeps `zone_id` as "a source-defined zone identifier". It never
learns what a TLC zone is. That discipline is why the platform is a *platform* and not a
TLC script.

---

## 7 — Step 1: the gateway validates before anything else

**Point:** Bad data is rejected at the door, loudly.

**Content:**
- FastAPI + Pydantic, `POST /events/trips`
- Two distinct failure modes, both → **HTTP 422**, never a crash:
  - type-malformed body, rejected before any code runs
  - type-valid but semantically impossible — e.g. **dropoff before pickup**
- Only validated, adapted, canonical events reach the bus

**Visual:** A gate. Three arrows approaching: one green passes, two red bounce off, each
labelled with its failure mode.

**Say:** Phase 1's verification was literally: curl a payload with corrupt types, get a
422, and confirm the process is still alive.

---

## 8 — Step 1b: identity, and a bug worth a slide

**Point:** A subtle identity bug silently corrupted a correctness guarantee.

**Content:**
- Every event needs an id. The gateway originally minted a random `uuid4` per request
- Reasoning: *"the gateway sees each record once"*
- **In a replay-driven system that is false.** The same trip arrives on every replay
- `fare_predictions` is keyed on `event_id` *specifically* to make re-consumption rewrite
  rather than duplicate — and a random id silently defeated it
- Result: error metrics weighted by how often a trip happened to be replayed
- **Fix:** derive the id by hashing the trip's natural key — the same six fields the batch
  layer already dedupes on

**Visual:** Same trip replayed three times. Before: three different ids → three rows.
After: one id → one row, rewritten.

**Say:** The schema comment already *said* the id was derived from the natural key. The
batch adapter did it. The gateway didn't. The documentation was right and the code drifted
— which is a more common failure than either being wrong alone.

---

## 9 — Step 2: Kafka as the fan-out point

**Point:** One topic, many independent readers, no coupling.

**Content:**
- Apache Kafka **3.8.1**, KRaft mode — no Zookeeper
- Single topic `tlc-raw-events`
- Three independent consumers, each with its own group and its own offsets
- A consumer can be down, slow, or rewound without any other noticing
- The log is the system's source of truth — **append-only, and replayable**

**Visual:** One horizontal log of ordered records; three consumers reading at three
different positions, each with its own marker.

**Say:** Append-only cuts both ways. Later in the talk: two test events I sent by hand are
now permanent, and every derived store rebuilds from them.

---

## 10 — Step 3a: the hot path

**Point:** Seconds behind the replay, with honest event-time semantics.

**Content:**
- Python Kafka consumer → in-memory rolling windows → PostgreSQL
- **5-minute event-time windows**, **10-minute grace period**, flushed every **2 seconds**
- Two distinct write paths:
  - **Liveness flush** — rewrite open windows so the dashboard moves. *No offset commit:
    the window is still filling*
  - **Finalization** — a window the watermark has passed is written `is_final=true`,
    evicted, and **only then** are offsets committed
- Absolute upsert: the incoming value **replaces** the stored one, never accumulates

**Visual:** A timeline of 5-minute buckets. A watermark line advancing left to right.
Buckets behind it locked and shaded; buckets ahead of it still open and being rewritten.

**Say:** "Only then are offsets committed" is the crash-safety property: a crash re-delivers
events rather than losing them, and the absolute upsert makes re-delivery harmless.

---

## 11 — Step 3b: the cold path

**Point:** The same events again — completely, and re-runnably.

**Content:**
- Airflow orchestrates Spark 3.5.3; Parquet lake partitioned `year=/month=/day=`, Snappy
- **`cold_path_incremental`** — every 3 minutes: bus → lake → daily rollups → serving store
- **`cold_path_backfill`** — one-shot, 36 months, one task per month so a failure retries alone
- Reads the topic `earliest → latest` **every run**: recompute the world from an immutable
  log rather than track what's been seen
- Dedupes on a six-field **natural key**, deliberately *not* `event_id`

**Visual:** Directory tree of the lake showing the partition layout, beside a small Airflow
DAG graph with the fan-in from 36 mapped tasks to one aggregation.

**Say:** Why natural key and not event id? Because a re-replayed trip legitimately arrives
with a new id. The cost is stated in the code: two genuinely distinct trips sharing all six
fields collapse into one. Losing a handful of coincidences beats double-counting a replay.

---

## 12 — Why the batch layer must never accumulate

**Point:** One-line rule, load-bearing across three components.

**Content:**
- Every merge is an **absolute upsert**: `value = EXCLUDED.value`, never `value + EXCLUDED.value`
- A layer that recomputes from scratch and *adds* would double every number it touched on
  each rerun
- Applied identically in the hot path's windows, the cold path's daily merge, and the ML
  predictions

**Visual:** Two counters after three runs — "replace: 100, 100, 100" vs "accumulate: 100,
200, 300" — with the second crossed out.

**Say:** Spark's JDBC writer has no upsert, which is why the cold path writes to a staging
table and merges with SQL. Overwriting the serving table directly would either drop its
indexes or leave the dashboard reading an empty table mid-write.

---

## 13 — The lambda payoff

**Point:** The architecture's central claim, expressed as a query you can run.

**Content:**
- Two independent code paths process the same events
- Hot: minutes, within a watermark, once
- Cold: completely, rerunnably
- **When they disagree, the cold number is the one to trust**
- Verified: hot and cold trip counts reconcile **exactly** on a single replay

**Visual:** Two pipelines converging on a single equals sign. Below it, a small table of
per-day hot vs cold counts matching.

**Say:** This is the slide that justifies the whole design. If the two paths didn't agree,
the extra complexity would buy nothing.

---

## 14 — Step 4: the ML layer asks a real question

**Point:** Not "predict a number" — predict the thing a rider actually wants to know.

**Content:**
- **What will this ride cost, before it starts?**
- Uses only what exists at pickup: **two zones and the clock**
- Never the distance driven, the dropoff time, or the meter — none of it exists yet
- Target is `total_amount − tip_amount`: everything the rider is charged **except the part
  they choose**

**Visual:** Split panel — "known at pickup" (zones, hour, day, month, passengers) vs
"exists only afterwards" (distance, duration, meter, tip), the right side greyed out.

**Say:** Why exclude the tip? TLC only records it for card payments. Including it would
make about a third of the training targets structurally wrong — a recording artifact, not
behaviour.

---

## 15 — The model

**Point:** Deliberately modest, and honest about its inputs.

**Content:**
- `HistGradientBoostingRegressor` (scikit-learn)
- Features: `pickup_zone_id`, `dropoff_zone_id`, `zone_pair`, `hour`, `day_of_week`,
  `month`, `passenger_count`
- Zone columns via `TargetEncoder`; the rest passed through as plain numbers
- Trains on **≤ cutoff** only; scores **> cutoff** events live off the bus
- 1.5M rows sampled **stratified by month**, so seasonality survives the reduction

**Visual:** Feature list on the left, a small pipeline diagram (encode → boost) on the
right, and the train/serve cut marked on a timeline.

**Say:** `zone_pair` matters more than either zone alone — a fare is a property of the
route, not the origin. v1 without it lost to a plain zone-pair median lookup by 43%.

---

## 16 — Results

**Point:** Good on normal days, and transparently bad on one.

**Content:**
- Current model **`fare-hgb-3`**
- Normal days: **MAE $4.30–$4.85**, **R² 0.72–0.74**
- New Year's Day: **MAE $13.50**, **R² 0.047**
- ~232k trips scored across 2026-01-01 → 01-08

**Visual:** Bar chart of per-day MAE, with Jan 1 towering over the rest in a warning
colour. R² as a second series or small multiples.

**Say:** The New Year's Day collapse is the most valuable number in the project. The daily
evaluation found it unprompted, on its first run — a real model limit, surfaced by the
platform rather than by someone eyeballing output.

---

## 17 — Measuring a retrain honestly

**Point:** How to tell a real improvement from noise.

**Content:**
- The backfill grew the training corpus from **14 months to 36**
- Same features, same estimator, same 1.5M row budget — so the gain is **seasonal
  coverage**, not volume: three full winters instead of one
- `fare-hgb-3` beat `fare-hgb-2` on **all 8 evaluated days**, by **1.3–2.5%**
- Small margins — but 8 for 8 in one direction is not noise

**Visual:** Slope chart, v2 → v3, eight lines all tilting down. Understated, not triumphal.

**Say:** Two rules learned the hard way. Evaluate the outgoing model *before* re-scoring,
because the re-score upserts in place and destroys its per-trip rows. And pause the
evaluation DAG for the duration — it's on a five-minute schedule, so intending not to
evaluate mid-flight isn't enough.

---

## 18 — Step 5: the dashboard, and one property worth defending

**Point:** Read-only is an architectural guarantee, not a limitation.

**Content:**
- Streamlit over PostgreSQL: live hot metrics, three-year cold trends, per-trip
  quoted-vs-charged feed, anomaly alerts
- Reads **three tables written by three components that don't know it exists**
- It **owns no tables and writes nothing** — a bug here cannot corrupt data
- So it *filters the view* and **hands you the replay command** rather than starting one
- Starting a replay from a web page would mean mounting the Docker socket into it —
  trading a real architectural property for a button

**Visual:** Dashboard screenshot or wireframe, with an arrow labelled "reads" and a
crossed-out arrow labelled "writes".

**Say:** Error bands are colour *and* icon *and* the number in the same cell — green under
10%, amber 10–25%, red beyond. Colour reinforces; it never carries the meaning alone.

---

## 19 — What "live" actually means

**Point:** A refreshing panel and a live panel are not the same thing.

**Content:**
- The quote feed refreshed every 10 seconds and looked **frozen**
- Cause: its upper bound came from the data **at page load**, so trips scored afterwards
  fell outside the window
- It re-queried, correctly, against a range nothing new could enter
- Fix: a **"Follow live"** mode that drops the upper bound, plus a badge that says whether
  it's scoring or idle
- *A feed that needs a page reload to show live data is not a live feed*

**Visual:** Before/after. Left: a window bounded behind the incoming data, new rows falling
outside. Right: an open-ended bound catching them.

**Say:** The deeper lesson: an auto-refreshing panel that redraws identical rows reads as
broken. The fix wasn't to redraw less — it was to say which of the two was happening.

---

## 20 — Six phases, each gated by verification

**Point:** Incremental delivery with a real gate, not a plan on paper.

**Content:**
| Phase | Verification that had to pass |
|---|---|
| 1 — Gateway | Corrupt payload → 422, no crash |
| 2 — Kafka + simulator | Events visible on the topic from the beginning |
| 3 — Hot path | `psql` shows window metrics updating during a replay |
| 4 — Cold path | Parquet metadata asserts schema + date partitions; hot/cold reconcile |
| 5 — ML | Daily predicted-vs-actual error recorded by Airflow |
| 6 — Dashboard | Panels advance on their own against a running replay |

**Visual:** Six checkpoints along a line, each with a tick and its test.

**Say:** No phase was scaffolded early. The rule was: don't advance until the current
phase's verification passes.

---

## 21 — What went wrong (the honest slide)

**Point:** The failures taught more than the successes, and they're worth showing.

**Content:**
- **A DAG race.** The full-lake aggregation died in 94 seconds against an expected 20
  minutes — the recurring DAG rewrites the same partitions every 3 minutes and deleted a
  file mid-scan. Not a fluke: ~100% reproducible.
- **A migration that was right and never ran.** A correct `ALTER TABLE` sat in an image
  built before it was written. *Source is baked into images — `git pull` deploys nothing.*
- **Two test events, permanently.** Payloads sent by hand into the live gateway hijacked the
  hot path's "latest window", couldn't be deleted, and still regenerate a row downstream.
- **A paused DAG strands its in-flight run** — remaining tasks park at `scheduled` forever
  and the run never leaves `running`. Hit three times in one day.

**Visual:** Four cards, each with the symptom and the one-line root cause. Neutral tone, not
alarmist.

**Say:** Every one of these is written into a decision log with the reasoning, so the next
person doesn't re-derive it. The failures that cost the most were the ones where the symptom
pointed nowhere near the cause.

---

## 22 — The habit that made it survivable

**Point:** The decision log is infrastructure.

**Content:**
- `docs/DECISIONS.md` — ~2,000 lines, one section per phase
- Records what was decided, **why**, and what was **rejected**
- Opens with a **Resuming** section: current state, anything mid-flight, traps already paid for
- Git history says *what changed*; this says *why*
- Known cosmetic artifacts are listed explicitly, so nobody re-investigates them as bugs

**Visual:** A document spine with dated entries; callouts for "Decided", "Rejected",
"Still open".

**Say:** The chat that produced this project is ephemeral. The reasoning had to live
somewhere durable or it would have been rebuilt from scratch every session.

---

## 23 — What was deliberately left out

**Point:** Scope discipline is a design decision too.

**Content:**
- **Flink** — cut. The hot path's windowing is ~200 lines of Python and fully understood.
- **GPU serving (Triton / cuML)** — cut. The model is a boosted tree over 7 features.
- **dbt** — cut. The transformations belong to Spark and SQL that already exist.
- **Zone-id → place-name lookup** — proposed, declined. Ids are sufficient for the demo.
- **A clean 2026 rebuild** — declined. Would have fixed three cosmetic artifacts and
  destroyed reproducible data to do it.

**Visual:** A list with strikethroughs, each with its one-line reason. Reasons are the point.

**Say:** Every one of these is in the decision log with its rejection reasoning, so a future
session doesn't re-litigate it — or worse, quietly add it back.

---

## 24 — Where it stands

**Point:** Finished, verified, and honest about the edges.

**Content:**
- All six phases **built and verified**
- Cold store: **1,100 continuous days** from 2023-01-01 — 36 months from raw files, plus
  replayed 2026 days
- **~232k** trips scored; both model versions retained for comparison
- Nine services, Docker Compose, every persistence target on an explicit volume
- Known open items recorded: an Airflow pool to prevent the DAG race, three cosmetic data
  artifacts, and thin test coverage

**Visual:** Scoreboard of the headline numbers, with a small "known open" panel that doesn't
hide.

**Say:** The last slide includes what's *not* done, because a status report that only lists
wins isn't a status report.

---

## 25 — Closing

**Point:** One sentence someone repeats afterwards.

**Content:**
- **The same events, processed twice on purpose**
- Fast where speed matters, complete where correctness does
- The hard part was never either path — it was making them agree, and knowing which to
  trust when they don't

**Visual:** The hero diagram again, reduced to its essential shape: one stream, two paths,
one answer.

---

## Appendix — numbers, for reference

| | |
|---|---|
| Dataset scope | 2023-01 → 2026-05, 41 monthly Parquet files, ~2GB compressed |
| Train/serve cutoff | 2025-12-31 23:59:59 |
| Training corpus | 36 months, ~110M rows; 1.5M sampled stratified by month |
| Replay throughput | ~420 events/sec (~366× event-time compression) |
| Hot path windows | 5-minute event-time, 10-minute grace, 2-second flush |
| Cold path cadence | every 3 minutes (wall clock, not `@daily` — see slide 4) |
| Lake layout | Parquet, Snappy, partitioned `year=/month=/day=` |
| Serving store | 1,100 days from 2023-01-01, ~256k zone-day rows |
| Predictions | ~232k, model `fare-hgb-3` |
| Normal-day accuracy | MAE $4.30–$4.85, R² 0.72–0.74 |
| New Year's Day | MAE $13.50, R² 0.047 |
| Stack | Kafka 3.8.1 (KRaft), PostgreSQL 16, Spark 3.5.3, Airflow, FastAPI, scikit-learn, Streamlit |

## Appendix — deck guidance

- **Arc:** tension (slide 2) → mechanism (5–13) → payoff (13) → application (14–19) →
  process and honesty (20–23) → close.
- **If time is short**, the minimum coherent deck is 1, 2, 4, 5, 6, 10, 11, 13, 16, 21, 25.
- **The three slides that carry the talk:** 6 (source independence), 13 (the reconciliation),
  21 (what went wrong). The first two are the architecture; the third is the credibility.
- Prefer one diagram per slide over bullet density. Slides 5, 10 and 13 should be almost
  entirely visual.
- Keep the failure slide (21) neutral in tone. It reads as confidence, not apology.
