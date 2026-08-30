"""The feature contract, shared by training and prediction.

This module exists to make one class of bug impossible. Training/serving skew —
where the model is fitted on columns built one way and served columns built
another — raises no error at all, it just returns quietly wrong numbers. Both
`train.py` and `predictor.py` import from here, so there is exactly one
definition of what a feature row is and what order its columns come in.

What may be a feature
---------------------
Only what is known **before the trip happens**: where the rider is, where they
asked to go, and the clock. Nothing measured during or after the ride.

Deliberately excluded, and this is the entire integrity of the model:

- `trip_distance_km` and `dropoff_datetime` are realised quantities. Including
  either would make the model excellent and the prediction fake — you cannot
  quote a price using the distance a taxi has not yet driven.
- Every money column, since one of them is the target.
- `payment_type`, which is only settled at the end of the ride.

The zone pair carries distance implicitly: "Midtown to JFK" already implies a
long trip. Learning that mapping from history is precisely the job.

Why the zones are target-encoded rather than one-hot or categorical
-------------------------------------------------------------------
TLC has 265 zones. scikit-learn's HistGradientBoosting handles categorical
features natively, but only up to `max_bins` (255), so zone ids as categories
fail at fit time — a limit worth knowing before discovering it. One-hot would
mean 530 sparse columns that a histogram booster splits badly.

Target encoding replaces a zone id with the mean target observed for it in
training, which is both compact and directly meaningful: the encoded value of a
pickup zone *is* "what trips from here typically cost". scikit-learn's
TargetEncoder cross-fits internally, so the encoding a row receives during
training is not computed from that row's own target — the leak this technique
is usually criticised for.

Source independence
-------------------
Every field here is canonical. Nothing in the ML layer reads `source_extras`,
so the layer is portable across sources by construction rather than by care.
"""

from __future__ import annotations

from datetime import datetime

import pandas as pd

# Column order is part of the contract: scikit-learn matches features by
# position, so a reordering between fit and predict is silently wrong.
FEATURE_COLUMNS = [
    "pickup_zone_id",
    "dropoff_zone_id",
    "zone_pair",
    "hour",
    "day_of_week",
    "month",
    "passenger_count",
]

# Encoded against the target; see the module docstring.
#
# `zone_pair` is here because the first model without it LOST to a zone-pair
# median lookup by 43% on MAE. Encoding the two zones independently destroys the
# thing that sets the price: "Midtown" averages ~$18 and "JFK" averages ~$60,
# but neither number says that *this pair* is a long airport run. The lookup
# keeps that interaction; six loosely-related numerics cannot reconstruct 69k
# pair-specific prices from two marginals.
#
# With the pair encoded, the model starts from the same information the lookup
# has and adds what the lookup cannot express — how the hour, the weekday and
# the season move that pair's price.
ZONE_COLUMNS = ["pickup_zone_id", "dropoff_zone_id", "zone_pair"]

# Sentinel for a pair where either zone is missing. TargetEncoder treats it as
# one more category and learns its mean, which is more honest than dropping the
# row or inventing a zone.
_MISSING_ZONE = -1

# Passed to the booster as plain numbers. All are low-cardinality and either
# ordered or close enough that tree splits capture them: the model can isolate
# "hours 22-04" with two splits without being told the clock wraps.
PASSTHROUGH_COLUMNS = ["hour", "day_of_week", "month", "passenger_count"]

# What the model estimates: everything the rider is charged except the part they
# choose themselves. `total_amount` would fold in the tip, which TLC records
# only for card payments — a recording artifact, not behaviour, and one that
# would make a third of the targets structurally wrong.
TARGET = "quoted_amount"


def quoted_amount(frame: pd.DataFrame) -> pd.Series:
    """The target: fare + surcharges + tolls, i.e. total minus the tip.

    Derived rather than stored, because the canonical contract has no column for
    "what the rider is charged before tipping" — it has the components. Computed
    as a subtraction so it stays exact against `total_amount` rather than
    drifting from it by re-adding parts.
    """
    return (frame["total_amount"] - frame["tip_amount"]).astype("float64")


def build_features(frame: pd.DataFrame, pickup_column: str = "pickup_datetime") -> pd.DataFrame:
    """Derive the feature frame from canonical trip columns.

    Takes a frame carrying `pickup_datetime`, `pickup_zone_id`,
    `dropoff_zone_id` and `passenger_count`; returns exactly FEATURE_COLUMNS in
    order, as plain numerics. Encoding is the pipeline's job, not this
    function's — keeping the split means the same frame shape is produced during
    training and serving no matter how the encoding later changes.
    """
    pickup = pd.to_datetime(frame[pickup_column])

    pickup_zone = frame["pickup_zone_id"].fillna(_MISSING_ZONE).astype("int64")
    dropoff_zone = frame["dropoff_zone_id"].fillna(_MISSING_ZONE).astype("int64")

    features = pd.DataFrame(
        {
            "pickup_zone_id": pickup_zone,
            "dropoff_zone_id": dropoff_zone,
            # One id per origin/destination pair. 265 zones fit in three digits,
            # so this is lossless and reverses as (pair // 1000, pair % 1000).
            "zone_pair": pickup_zone * 1000 + dropoff_zone,
            "hour": pickup.dt.hour,
            "day_of_week": pickup.dt.dayofweek,
            "month": pickup.dt.month,
            # Nulls stay null rather than being imputed. The booster learns
            # which side of a split missing values belong on, which carries more
            # information than substituting a 1 that cannot then be told apart
            # from a genuine single rider.
            "passenger_count": frame["passenger_count"],
        }
    )

    return features[FEATURE_COLUMNS].astype("float64")


def single_row(
    pickup_datetime: datetime,
    pickup_zone_id: int | None,
    dropoff_zone_id: int | None,
    passenger_count: int | None,
) -> pd.DataFrame:
    """One event's features, built through the same path as the training set.

    The predictor could assemble a row by hand more cheaply. It does not, on
    purpose: the moment there are two ways to build a feature row they can
    disagree, and nothing would report it.
    """
    return build_features(
        pd.DataFrame(
            {
                "pickup_datetime": [pickup_datetime],
                "pickup_zone_id": [pickup_zone_id],
                "dropoff_zone_id": [dropoff_zone_id],
                "passenger_count": [passenger_count],
            }
        )
    )
