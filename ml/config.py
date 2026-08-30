"""Environment-driven configuration for the ML layer.

Every value has a default that works inside the Compose network, overridable
from the environment. Nothing secret is defaulted here.
"""

from __future__ import annotations

import os
from datetime import datetime

# --------------------------------------------------------------------------
# The cutoff. Same value the cold path enforces, and for a related reason: it
# splits training data from prediction data. Everything at or before it is
# history the model learns; everything after it is the future arriving over the
# bus. Keeping one value means the model can never be evaluated on a day it was
# trained on.
# --------------------------------------------------------------------------
CUTOFF = datetime.fromisoformat(os.environ.get("CUTOFF_DATETIME", "2025-12-31T23:59:59"))

# --------------------------------------------------------------------------
# Paths
# --------------------------------------------------------------------------
# Read-only: training samples the lake the cold path built.
LAKE_PATH = os.environ.get("LAKE_PATH", "/data/lake/trips")
# Read-write for training, read-only for serving. An explicit mount, per the
# no-ephemeral-state rule — a model that vanishes with the container is not a
# model, it is a cache.
MODEL_DIR = os.environ.get("MODEL_DIR", "/models")

# --------------------------------------------------------------------------
# Training
# --------------------------------------------------------------------------
# Rows to sample from the lake. ~40M rows sit at or before the cutoff, which is
# far more than a gradient booster needs and more than fits comfortably in
# memory. Sampled stratified by month so seasonality survives the reduction.
TRAIN_SAMPLE_ROWS = int(os.environ.get("TRAIN_SAMPLE_ROWS", "1500000"))

# Fixed so a rerun reproduces the same split and the same model. A model that
# cannot be rebuilt is not reproducible science, it is a lucky artifact.
RANDOM_SEED = int(os.environ.get("RANDOM_SEED", "42"))

# Guards against absurd targets poisoning the fit. TLC carries occasional
# five-figure fares from meter faults; they are real records but not real
# prices, and squared error chases them hard.
MIN_QUOTED_AMOUNT = float(os.environ.get("MIN_QUOTED_AMOUNT", "2.5"))
MAX_QUOTED_AMOUNT = float(os.environ.get("MAX_QUOTED_AMOUNT", "250"))

# --------------------------------------------------------------------------
# Message bus. Topic name is source-specific *configuration*, the same
# arrangement the hot path and the cold path already use.
# --------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "tlc-raw-events")
# Its own consumer group, so the predictor and the hot path each receive every
# event rather than splitting the topic between them.
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "ml-predictor")

# --------------------------------------------------------------------------
# Serving store — the same PostgreSQL the hot and cold paths write to.
# --------------------------------------------------------------------------
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "northstar")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "northstar")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "northstar")

# How many events to buffer before scoring and writing them together.
#
# Governs both the model call and the database round trip. The model call is
# what makes it matter: scoring one row at a time paid the full scikit-learn
# pipeline overhead per event and measured ~120 events/sec against a replay
# producing ~420/sec, so the service fell steadily behind its own input. One
# predict() over a batch pays that cost once.
#
# 500 rather than 200 because the fixed cost is now amortised over the batch and
# a larger batch is strictly better for it, while still bounding how many events
# a crash can force a re-read of.
WRITE_BATCH_SIZE = int(os.environ.get("WRITE_BATCH_SIZE", "500"))


def postgres_dsn() -> str:
    return (
        f"host={POSTGRES_HOST} port={POSTGRES_PORT} dbname={POSTGRES_DB} "
        f"user={POSTGRES_USER} password={POSTGRES_PASSWORD}"
    )
