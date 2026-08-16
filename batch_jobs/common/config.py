"""Environment-driven configuration for the cold-path jobs.

Every value has a default that works for a local run, and is overridable from
the environment so Compose (and later Airflow) can set it per deployment.
Nothing secret is defaulted here.
"""

from __future__ import annotations

import os
from datetime import datetime

# --------------------------------------------------------------------------
# The cutoff: the single boundary that divides the two lake writers.
# --------------------------------------------------------------------------
# Records at or before it are backfilled from the raw files; records after it
# arrive over the message bus as the simulator replays them. Because both
# writers use dynamic partition overwrite, an overlap would mean whichever ran
# last silently clobbers the other's partitions — so this must be one value,
# read from one place, by both jobs. P5 inherits it for the train/replay split.
CUTOFF = datetime.fromisoformat(os.environ.get("CUTOFF_DATETIME", "2025-12-31T23:59:59"))

# --------------------------------------------------------------------------
# Paths. Both are bind-mounted into the Spark containers; the lake is
# read-write, the raw drop is read-only (the jobs only ever read it).
# --------------------------------------------------------------------------
RAW_PATH = os.environ.get("RAW_PATH", "/data/raw")
LAKE_PATH = os.environ.get("LAKE_PATH", "/data/lake/trips")

# --------------------------------------------------------------------------
# Spark cluster. Defaults to local mode so the adapter and its conformance test
# run without any cluster at all; Airflow overrides this with the standalone
# master URL.
# --------------------------------------------------------------------------
SPARK_MASTER = os.environ.get("SPARK_MASTER", "local[*]")

# In client mode the executors connect *back* to the driver, so the driver has
# to advertise a hostname reachable on the Compose network — the container name,
# not the auto-detected internal one. Unset in local mode, where it's moot.
SPARK_DRIVER_HOST = os.environ.get("SPARK_DRIVER_HOST")

# --------------------------------------------------------------------------
# Message bus. The topic name is source-specific, but it is *configuration*,
# not knowledge baked into a job — the same arrangement hot_path already uses.
# --------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "tlc-raw-events")
