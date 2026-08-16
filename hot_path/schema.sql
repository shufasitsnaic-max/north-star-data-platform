-- Hot-path output: rolling window metrics over canonical trip events.
--
-- Applied idempotently by db.py on every consumer start, so a fresh volume or a
-- restart both converge to this shape without a migration tool. Phase 3 owns
-- this table alone; later phases add their own rather than widening it.
--
-- No column here names a data source: these are canonical metrics.

CREATE TABLE IF NOT EXISTS trip_window_metrics (
    -- Event-time bucket boundaries (from pickup_datetime, not wall clock).
    -- `timestamp` without time zone matches the canonical contract, whose
    -- pickup_datetime is a naive local timestamp as published by the source.
    window_start     timestamp   NOT NULL,
    window_end       timestamp   NOT NULL,

    -- Pickup zone, or NULL for the citywide rollup row. NULL is meaningful
    -- here ("all zones"), which is why it is not a sentinel integer — queries
    -- read naturally as `WHERE zone_id IS NULL`.
    zone_id          integer,

    trip_count       bigint      NOT NULL,

    -- Money as numeric, never float: these are summed Decimals from the wire.
    total_revenue    numeric(14, 2) NOT NULL,
    avg_fare         numeric(10, 2) NOT NULL,
    avg_tip          numeric(10, 2) NOT NULL,

    -- NULL when no event in the window carried a distance, which is distinct
    -- from an average of zero.
    avg_distance_km  double precision,

    -- false while the window is still filling, true once the watermark has
    -- passed it and the row will not change again. The dashboard can use this
    -- to style the in-progress bucket differently from settled history.
    is_final         boolean     NOT NULL DEFAULT false,

    updated_at       timestamptz NOT NULL DEFAULT now()
);

-- Upsert key. A plain PRIMARY KEY cannot be used because PostgreSQL treats
-- NULLs as distinct in a unique constraint, which would let the citywide row
-- be inserted repeatedly. COALESCE folds it to a sentinel for uniqueness only
-- — the stored value stays NULL.
CREATE UNIQUE INDEX IF NOT EXISTS trip_window_metrics_key
    ON trip_window_metrics (window_start, COALESCE(zone_id, -1));

-- The dashboard's two read patterns: newest citywide points, and one zone's
-- history. Both are time-ordered descending.
CREATE INDEX IF NOT EXISTS trip_window_metrics_citywide
    ON trip_window_metrics (window_start DESC)
    WHERE zone_id IS NULL;

CREATE INDEX IF NOT EXISTS trip_window_metrics_by_zone
    ON trip_window_metrics (zone_id, window_start DESC);
