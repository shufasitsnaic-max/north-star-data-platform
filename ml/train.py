"""Fit the fare estimator on everything at or before the cutoff.

Run once, by hand, before the predictor starts:

    docker compose run --rm ml_train

Reports three models rather than one, because a single error figure means
nothing without something to compare it against:

  1. **Global median** — the "do nothing" floor. Quote the same price for every
     trip. Any model that cannot beat this has learned nothing at all.
  2. **Zone-pair median** — a lookup table: the historical median price for this
     origin/destination pair. Strong, interpretable, and the honest bar, because
     a business could build it in an afternoon without any ML.
  3. **Gradient boosting** — the real model, which additionally learns how time
     of day, day of week and season move the price.

The claim worth making at the end is "the model beats a lookup table by X%",
not "MAE is $Y".

Temporal honesty
----------------
Training reads only records at or before the cutoff, and the predictor only
scores records after it. The holdout is therefore chronological, not random: the
model is judged on a period it has never seen, which is the only split that
means anything for a system that will run forwards in time.
"""

from __future__ import annotations

import json
import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import pyarrow.compute as pc
import pyarrow.dataset as ds
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import TargetEncoder

import config
from features import (
    FEATURE_COLUMNS,
    PASSTHROUGH_COLUMNS,
    ZONE_COLUMNS,
    build_features,
    quoted_amount,
)

logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger("ml.train")

# Bumped whenever the feature contract or the estimator changes. v2 added the
# encoded zone pair after v1 lost to the zone-pair median lookup by 43%. Written into
# every prediction row so the daily evaluation can tell one model's errors from
# another's — without it, swapping models silently mixes two error series into
# one meaningless average.
MODEL_VERSION = "fare-hgb-2"

_COLUMNS = [
    "pickup_datetime",
    "pickup_location",
    "dropoff_location",
    "passenger_count",
    "total_amount",
    "tip_amount",
]


def _load_sample() -> pd.DataFrame:
    """Sample the lake, stratified by month, from records at or before the cutoff.

    Stratified rather than a single random draw over everything: a plain sample
    would still be representative in expectation, but month-by-month reading
    keeps peak memory to one month instead of the whole corpus, and guarantees
    every month is present rather than probably present.
    """
    dataset = ds.dataset(config.LAKE_PATH, format="parquet", partitioning="hive")

    # Months are read from the directory names, not from the data. Scanning the
    # year/month columns of 45M rows to learn twelve values would read hundreds
    # of megabytes to answer a question the paths already answer.
    found = set()
    for path in dataset.files:
        match = re.search(r"year=(\d+)[/\\]month=(\d+)", path)
        if match:
            found.add((int(match.group(1)), int(match.group(2))))

    # Only months wholly at or before the cutoff. The cutoff falls on a month
    # boundary (31 December), so no partial month arises — but comparing the
    # month's *start* keeps that an assumption the code states rather than one
    # it relies on silently.
    months = sorted(
        (year, month)
        for year, month in found
        if datetime(year, month, 1) <= config.CUTOFF
    )
    if not months:
        raise RuntimeError(
            f"no lake partitions at or before the cutoff ({config.CUTOFF.isoformat()}). "
            "Run the cold_path_backfill DAG first."
        )

    per_month = max(config.TRAIN_SAMPLE_ROWS // len(months), 1)
    logger.info("sampling %d row(s) from each of %d month(s)", per_month, len(months))

    rng = np.random.default_rng(config.RANDOM_SEED)
    frames = []
    for year, month in months:
        table = dataset.to_table(
            columns=_COLUMNS,
            filter=(pc.field("year") == year) & (pc.field("month") == month),
        )
        frame = table.to_pandas()
        if len(frame) > per_month:
            frame = frame.iloc[rng.choice(len(frame), per_month, replace=False)]
        frames.append(frame)

    sample = pd.concat(frames, ignore_index=True)
    logger.info("sampled %d row(s) across %d month(s)", len(sample), len(months))
    return sample


def _prepare(sample: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    """Flatten the canonical structs, derive the target, drop unusable rows."""
    flat = pd.DataFrame(
        {
            "pickup_datetime": sample["pickup_datetime"],
            "pickup_zone_id": [loc["zone_id"] if loc else None for loc in sample["pickup_location"]],
            "dropoff_zone_id": [
                loc["zone_id"] if loc else None for loc in sample["dropoff_location"]
            ],
            "passenger_count": sample["passenger_count"],
            "total_amount": sample["total_amount"].astype("float64"),
            "tip_amount": sample["tip_amount"].astype("float64"),
        }
    )
    target = quoted_amount(flat)

    # Meter faults and disputed rides produce targets that are real records but
    # not real prices. Squared error chases them hard, so they are excluded from
    # fitting rather than allowed to bend the model toward outliers.
    usable = target.between(config.MIN_QUOTED_AMOUNT, config.MAX_QUOTED_AMOUNT)
    dropped = int((~usable).sum())
    if dropped:
        logger.info(
            "dropped %d row(s) (%.2f%%) outside $%.2f-$%.2f",
            dropped, 100.0 * dropped / len(flat),
            config.MIN_QUOTED_AMOUNT, config.MAX_QUOTED_AMOUNT,
        )

    flat = flat[usable].reset_index(drop=True)
    return build_features(flat), target[usable].reset_index(drop=True)


def _report(name: str, actual: np.ndarray, predicted: np.ndarray) -> dict:
    metrics = {
        "mae": float(mean_absolute_error(actual, predicted)),
        "rmse": float(np.sqrt(mean_squared_error(actual, predicted))),
        "r2": float(r2_score(actual, predicted)),
    }
    logger.info(
        "%-18s MAE $%.3f  RMSE $%.3f  R2 %.4f",
        name, metrics["mae"], metrics["rmse"], metrics["r2"],
    )
    return metrics


def train() -> dict:
    features, target = _prepare(_load_sample())

    # Random split *within* the training period. This measures fit quality, not
    # forward performance — the real forward test is the daily evaluation on
    # post-cutoff events, which is the number that actually matters.
    x_train, x_test, y_train, y_test = train_test_split(
        features, target, test_size=0.2, random_state=config.RANDOM_SEED
    )

    results = {}

    # Baseline 1: one price for everyone.
    results["global_median"] = _report(
        "global median", y_test, np.full(len(y_test), float(y_train.median()))
    )

    # Baseline 2: the lookup table a business could build without any ML.
    # Falls back to the global median for pairs never seen in training, which is
    # exactly the weakness the model should improve on.
    pair_median = y_train.groupby(
        [x_train["pickup_zone_id"], x_train["dropoff_zone_id"]]
    ).median()
    lookup = pd.MultiIndex.from_arrays(
        [x_test["pickup_zone_id"], x_test["dropoff_zone_id"]]
    ).map(pair_median)
    results["zone_pair_median"] = _report(
        "zone-pair median", y_test, pd.Series(lookup).fillna(y_train.median()).to_numpy()
    )

    # The model. TargetEncoder cross-fits internally, so a row's encoding is
    # never computed from its own target.
    model = Pipeline(
        [
            (
                "encode",
                ColumnTransformer(
                    [
                        ("zones", TargetEncoder(random_state=config.RANDOM_SEED), ZONE_COLUMNS),
                        ("rest", "passthrough", PASSTHROUGH_COLUMNS),
                    ]
                ),
            ),
            (
                "model",
                HistGradientBoostingRegressor(
                    # Absolute error rather than squared: the metric quoted to a
                    # rider is "typically within $X", and optimising squared
                    # error would trade many small errors for a few huge ones.
                    loss="absolute_error",
                    max_iter=300,
                    learning_rate=0.1,
                    early_stopping=True,
                    n_iter_no_change=20,
                    random_state=config.RANDOM_SEED,
                ),
            ),
        ]
    )
    model.fit(x_train, y_train)
    results["gradient_boosting"] = _report("gradient boosting", y_test, model.predict(x_test))

    improvement = (
        100.0
        * (results["zone_pair_median"]["mae"] - results["gradient_boosting"]["mae"])
        / results["zone_pair_median"]["mae"]
    )
    logger.info("model beats the zone-pair lookup by %.1f%% on MAE", improvement)

    model_dir = Path(config.MODEL_DIR)
    model_dir.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, model_dir / "fare_model.joblib")

    metadata = {
        "model_version": MODEL_VERSION,
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "cutoff": config.CUTOFF.isoformat(),
        "rows_trained": int(len(x_train)),
        "features": FEATURE_COLUMNS,
        "metrics": results,
        "improvement_over_lookup_pct": round(improvement, 2),
    }
    (model_dir / "fare_model.json").write_text(json.dumps(metadata, indent=2))
    logger.info("wrote %s (version %s)", model_dir / "fare_model.joblib", MODEL_VERSION)
    return metadata


def main() -> int:
    try:
        train()
    except Exception:  # noqa: BLE001 — log the trace, then fail
        logger.exception("training failed")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
