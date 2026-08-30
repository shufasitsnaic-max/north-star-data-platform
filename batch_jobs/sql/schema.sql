-- Cold-path output: daily x zone rollups over the whole lake.
--
-- Sibling of hot_path/schema.sql, deliberately shaped the same way: same zone
-- semantics, same NULL-means-citywide convention, same money-as-numeric rule.
-- The dashboard reads both tables and should not have to reconcile two
-- different vocabularies for the same ideas.
--
-- Applied idempotently before every merge, so a fresh volume and a running
-- system converge to this shape without a migration tool. Phase 4 owns this
-- table alone; P5 adds its own rather than widening it.
--
-- No column here names a data source: these are canonical metrics.

CREATE TABLE IF NOT EXISTS cold_daily_zone_metrics (
    -- Event-time day, derived from pickup_datetime. `date` rather than a
    -- timestamp because the grain IS the day: storing midnight would invite
    -- someone to read it as an instant and apply a timezone shift to it.
    metric_date      date        NOT NULL,

    -- Pickup zone, or NULL for the citywide rollup row. NULL is meaningful
    -- ("all zones"), not missing, so it is not a sentinel integer — the same
    -- choice trip_window_metrics makes, for the same reason.
    zone_id          integer,

    trip_count       bigint      NOT NULL,

    -- Money as numeric, never float. These are summed Decimals read back from
    -- Parquet DecimalType(12,2), so the money path stays exact end to end:
    -- source -> canonical -> lake -> serving store.
    total_revenue    numeric(14, 2) NOT NULL,
    avg_fare         numeric(10, 2) NOT NULL,
    avg_tip          numeric(10, 2) NOT NULL,

    -- NULL when no trip that day carried a distance, which is distinct from an
    -- average of zero. Matches the hot path's treatment of the same field.
    avg_distance_km  double precision,

    -- When this row was last recomputed. The cold path rewrites history freely
    -- — that is what a batch layer is for — so "the data is from day X, the
    -- number was computed at time Y" are two different facts worth keeping.
    updated_at       timestamptz NOT NULL DEFAULT now()
);

-- Upsert key, and the index ON CONFLICT infers against. A plain PRIMARY KEY
-- cannot be used: PostgreSQL treats NULLs as distinct in a unique constraint,
-- which would let the citywide row be inserted once per merge, forever.
-- COALESCE folds it to a sentinel for uniqueness only — the stored value stays
-- NULL. Same trick as trip_window_metrics_key.
CREATE UNIQUE INDEX IF NOT EXISTS cold_daily_zone_metrics_key
    ON cold_daily_zone_metrics (metric_date, COALESCE(zone_id, -1));

-- The dashboard's two read patterns, mirroring the hot path's: the citywide
-- trend line, and one zone's history. Both time-ordered descending.
CREATE INDEX IF NOT EXISTS cold_daily_zone_metrics_citywide
    ON cold_daily_zone_metrics (metric_date DESC)
    WHERE zone_id IS NULL;

CREATE INDEX IF NOT EXISTS cold_daily_zone_metrics_by_zone
    ON cold_daily_zone_metrics (zone_id, metric_date DESC);
