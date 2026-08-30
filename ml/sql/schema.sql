-- ML layer output: per-trip price quotes, and the daily accuracy of those quotes.
--
-- Applied idempotently by predictor.py on start and by the eval DAG on every
-- run, so a fresh volume heals itself rather than failing until someone
-- notices. Same discipline as hot_path/schema.sql.
--
-- No column here names a data source: these are canonical fields plus the
-- model's own output.

-- One row per scored trip. The prediction and the outcome sit side by side so
-- the daily evaluation is a pure aggregation with nothing to join against.
CREATE TABLE IF NOT EXISTS fare_predictions (
    -- The canonical event id, which the batch adapter derives from the natural
    -- key. Primary key, so re-consuming an event rewrites its quote rather than
    -- recording a second one — otherwise the error metrics would be weighted by
    -- how often a trip happened to be redelivered.
    event_id         text        PRIMARY KEY,

    -- Event time, not wall clock. The daily evaluation buckets on this, so a
    -- day's accuracy is the accuracy for trips that *happened* that day.
    pickup_datetime  timestamp   NOT NULL,

    pickup_zone_id   integer,
    dropoff_zone_id  integer,

    -- What the model quoted, from pickup information alone.
    predicted_amount numeric(10, 2) NOT NULL,

    -- What the trip actually cost, excluding the tip the rider chose:
    -- total_amount - tip_amount. Recorded for comparison only; the model never
    -- receives it.
    actual_amount    numeric(10, 2) NOT NULL,

    -- Which model produced the quote. Without this, swapping models silently
    -- merges two error series into one meaningless average.
    model_version    text        NOT NULL,

    predicted_at     timestamptz NOT NULL DEFAULT now()
);

-- The evaluation's access pattern: one event-time day at a time, per model.
CREATE INDEX IF NOT EXISTS fare_predictions_by_day
    ON fare_predictions (model_version, pickup_datetime);

-- Daily accuracy, one row per event-time day per model. This is the "compare
-- earlier predictions with what actually happened, every day" record.
CREATE TABLE IF NOT EXISTS ml_daily_eval (
    eval_date       date        NOT NULL,
    model_version   text        NOT NULL,

    predictions     bigint      NOT NULL,

    -- Mean absolute error, in dollars: "our quote is typically off by this
    -- much". The metric a rider would care about, and the one the model was
    -- trained to minimise.
    mae             numeric(10, 4) NOT NULL,

    -- Root mean squared error, which punishes large misses harder. Reported
    -- alongside MAE because a gap between the two is itself the signal: it
    -- means the errors are concentrated in a few bad quotes rather than spread.
    rmse            numeric(10, 4) NOT NULL,

    -- Mean absolute percentage error. MAE alone flatters a day of cheap trips;
    -- this is comparable across days with different price mixes.
    mape            numeric(10, 4),

    -- Share of variance explained. NULL on a day with fewer than two distinct
    -- actuals, where it is undefined rather than zero.
    r2              double precision,

    mean_actual     numeric(10, 2) NOT NULL,
    mean_predicted  numeric(10, 2) NOT NULL,

    computed_at     timestamptz NOT NULL DEFAULT now(),

    PRIMARY KEY (eval_date, model_version)
);

-- The dashboard's read pattern: the recent error trend, newest first.
CREATE INDEX IF NOT EXISTS ml_daily_eval_recent
    ON ml_daily_eval (eval_date DESC);
