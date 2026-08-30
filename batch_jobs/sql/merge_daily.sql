-- Merge the staging table Spark just wrote into the table the dashboard reads.
--
-- Why this exists at all: Spark's JDBC writer has no upsert. Writing the
-- serving table directly would mean either mode("overwrite"), which drops and
-- recreates the table and loses its indexes, or truncate=true, which leaves
-- the dashboard reading an empty table for several seconds mid-write. Staging
-- plus this merge is atomic from a reader's point of view — the serving table
-- is never empty and never half-written.
--
-- Run after aggregate_daily.py, and after schema.sql. Idempotent: running it
-- twice against the same staging data produces the same rows, because every
-- conflicting row is overwritten rather than accumulated. That matters because
-- the cold path recomputes the world on every run by design.
--
-- Same absolute-upsert discipline the hot path uses for window metrics: the
-- incoming value REPLACES the stored one rather than adding to it. A batch
-- layer that recomputes from scratch must never accumulate, or a rerun would
-- double every number it touched.

INSERT INTO cold_daily_zone_metrics AS target (
    metric_date,
    zone_id,
    trip_count,
    total_revenue,
    avg_fare,
    avg_tip,
    avg_distance_km,
    updated_at
)
SELECT
    metric_date,
    zone_id,
    trip_count,
    total_revenue,
    avg_fare,
    avg_tip,
    avg_distance_km,
    now()
FROM cold_daily_zone_metrics_staging

-- Infers the expression index from schema.sql. The COALESCE must be written
-- exactly as the index declares it or PostgreSQL cannot match them and raises
-- "no unique or exclusion constraint matching the ON CONFLICT specification".
ON CONFLICT (metric_date, COALESCE(zone_id, -1)) DO UPDATE SET
    trip_count      = EXCLUDED.trip_count,
    total_revenue   = EXCLUDED.total_revenue,
    avg_fare        = EXCLUDED.avg_fare,
    avg_tip         = EXCLUDED.avg_tip,
    avg_distance_km = EXCLUDED.avg_distance_km,
    updated_at      = EXCLUDED.updated_at
-- Skip the write entirely when nothing changed. Saves dead tuples and, more
-- usefully, keeps updated_at honest: it then means "this number last changed",
-- not "a job last ran".
WHERE target.trip_count      IS DISTINCT FROM EXCLUDED.trip_count
   OR target.total_revenue   IS DISTINCT FROM EXCLUDED.total_revenue
   OR target.avg_fare        IS DISTINCT FROM EXCLUDED.avg_fare
   OR target.avg_tip         IS DISTINCT FROM EXCLUDED.avg_tip
   OR target.avg_distance_km IS DISTINCT FROM EXCLUDED.avg_distance_km;
