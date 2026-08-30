-- Score every event-time day the predictor has quoted, and republish the result.
--
-- This is the rubric's "compare earlier predictions with what actually
-- happened, and evaluate every day". Recomputed in full on each run rather than
-- appended to, for the same reason the cold path recomputes the lake: the
-- inputs are immutable, a rerun repairs any past mistake, and there is no
-- incremental state to drift.
--
-- Pure SQL because the predictor stores the outcome next to the quote, so
-- nothing needs joining and no Python is involved. The alternative — a Spark
-- job joining predictions against the lake — is more independent but needs a
-- whole job, an ml/ mount into the Spark containers, and a second definition of
-- the target. Rejected on cost; worth revisiting if the two ever disagree.
--
-- R2 is computed the long way (1 - SSE/SST) because PostgreSQL has no built-in
-- for it. It is NULL where undefined: a day with fewer than two predictions, or
-- one where every trip cost the same, has no variance to explain, and reporting
-- 0 there would read as "the model explains nothing" rather than "the question
-- does not apply".

INSERT INTO ml_daily_eval AS target (
    eval_date, model_version, predictions,
    mae, rmse, mape, r2, mean_actual, mean_predicted, computed_at
)
WITH per_day AS (
    SELECT
        pickup_datetime::date            AS eval_date,
        model_version,
        count(*)                         AS predictions,
        avg(abs(predicted_amount - actual_amount))   AS mae,
        sqrt(avg(power(predicted_amount - actual_amount, 2))) AS rmse,
        -- Guarded against division by zero: a legitimately free trip would
        -- otherwise abort the whole evaluation.
        avg(
            CASE WHEN actual_amount <> 0
                 THEN abs(predicted_amount - actual_amount) / abs(actual_amount)
            END
        ) * 100                          AS mape,
        sum(power(predicted_amount - actual_amount, 2)) AS sse,
        avg(actual_amount)               AS mean_actual,
        avg(predicted_amount)            AS mean_predicted,
        var_samp(actual_amount)          AS actual_variance
    FROM fare_predictions
    GROUP BY 1, 2
)
SELECT
    eval_date,
    model_version,
    predictions,
    mae,
    rmse,
    mape,
    -- SST = variance x (n - 1). NULL when the day has no variance to explain.
    CASE
        WHEN predictions > 1 AND actual_variance > 0
        THEN 1 - (sse / (actual_variance * (predictions - 1)))
    END AS r2,
    mean_actual,
    mean_predicted,
    now()
FROM per_day

ON CONFLICT (eval_date, model_version) DO UPDATE SET
    predictions    = EXCLUDED.predictions,
    mae            = EXCLUDED.mae,
    rmse           = EXCLUDED.rmse,
    mape           = EXCLUDED.mape,
    r2             = EXCLUDED.r2,
    mean_actual    = EXCLUDED.mean_actual,
    mean_predicted = EXCLUDED.mean_predicted,
    computed_at    = EXCLUDED.computed_at
-- Skip days whose numbers have not moved, so computed_at keeps meaning "this
-- score changed" rather than "a job ran". Same discipline as the cold path's
-- merge, and the same diagnostic value: unnecessary rewrites become visible.
WHERE target.predictions IS DISTINCT FROM EXCLUDED.predictions
   OR target.mae         IS DISTINCT FROM EXCLUDED.mae
   OR target.rmse        IS DISTINCT FROM EXCLUDED.rmse
   OR target.mape        IS DISTINCT FROM EXCLUDED.mape
   OR target.r2          IS DISTINCT FROM EXCLUDED.r2;
