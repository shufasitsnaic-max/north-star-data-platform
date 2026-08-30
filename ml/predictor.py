"""Quote a price for every post-cutoff trip as it arrives on the bus.

A sibling of the hot-path consumer, on its own consumer group so both services
receive every event rather than splitting the topic between them.

What makes this a prediction and not a lookup
---------------------------------------------
The model is given only what is known before the wheels turn: the two zones and
the clock. It never sees the distance actually driven, the dropoff time, or any
money column. The actual price is recorded alongside the prediction purely so
the daily evaluation has something to compare against — it is an outcome, not an
input.

The temporal separation is real too: the model was fitted on records at or
before the cutoff, and this service scores only records after it. Pre-cutoff
events on the topic (the P2/P3 replays are still there) are skipped, because
quoting a price for a trip the model trained on would flatter it.

The honest caveat, worth saying out loud rather than hiding: because a replayed
event describes a trip that already finished, the prediction and the outcome
arrive together. In production the quote would be issued minutes earlier. What
that costs us is nothing in terms of leakage — feature selection handles that —
but it does mean this measures accuracy, not latency-to-outcome.
"""

from __future__ import annotations

import json
import logging
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import joblib
import pandas as pd
import psycopg
from confluent_kafka import Consumer, KafkaError

import config
from features import build_features

logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger("ml.predictor")

_SCHEMA_PATH = Path(__file__).parent / "sql" / "schema.sql"

# Absolute upsert on event_id. Re-consuming an event — a rebalance, a rewind,
# a re-replay — must rewrite its prediction, never add a second one, or the
# daily error metrics would be weighted by how often a trip happened to be
# redelivered.
_UPSERT = """
INSERT INTO fare_predictions (
    event_id, pickup_datetime, pickup_zone_id, dropoff_zone_id,
    predicted_amount, actual_amount, model_version, predicted_at
) VALUES (%s, %s, %s, %s, %s, %s, %s, now())
ON CONFLICT (event_id) DO UPDATE SET
    predicted_amount = EXCLUDED.predicted_amount,
    actual_amount    = EXCLUDED.actual_amount,
    model_version    = EXCLUDED.model_version,
    predicted_at     = EXCLUDED.predicted_at
"""

_running = True


def _stop(signum, _frame):
    global _running
    logger.info("signal %s received — finishing the current batch and exiting", signum)
    _running = False


def _connect_postgres(max_attempts: int = 10) -> psycopg.Connection:
    """Connect with exponential backoff, per the resilient-connections rule."""
    delay = 1.0
    last: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            connection = psycopg.connect(config.postgres_dsn(), autocommit=True)
            with connection.cursor() as cursor:
                cursor.execute(_SCHEMA_PATH.read_text())
            logger.info("connected to PostgreSQL (attempt %d)", attempt)
            return connection
        except Exception as exc:  # noqa: BLE001 — any startup failure is retryable
            last = exc
            logger.warning("PostgreSQL not ready (%d/%d): %s", attempt, max_attempts, exc)
            time.sleep(delay)
            delay = min(delay * 2, 20)
    raise RuntimeError("could not reach PostgreSQL") from last


def _load_model():
    model_dir = Path(config.MODEL_DIR)
    artifact = model_dir / "fare_model.joblib"
    if not artifact.exists():
        raise RuntimeError(
            f"no model at {artifact}. Train one first: docker compose run --rm ml_train"
        )
    metadata = json.loads((model_dir / "fare_model.json").read_text())
    logger.info(
        "loaded model %s, trained %s on %d rows",
        metadata["model_version"], metadata["trained_at"], metadata["rows_trained"],
    )
    return joblib.load(artifact), metadata["model_version"]


def _prepare(event: dict) -> dict | None:
    """One event reduced to what scoring needs, or None if it is not ours.

    Deliberately does NOT predict. Scoring one row at a time costs the full
    scikit-learn pipeline overhead per event — the encoder transform and the
    booster's own setup — for a single number, which measured at ~120 events/sec
    against a replay producing ~420/sec. The service could not keep up with its
    own input. Predictions are made per batch instead, in _flush.
    """
    pickup = datetime.fromisoformat(event["pickup_datetime"])
    if pickup <= config.CUTOFF:
        return None

    return {
        "event_id": event["event_id"],
        "pickup_datetime": pickup,
        "pickup_zone_id": (event.get("pickup_location") or {}).get("zone_id"),
        "dropoff_zone_id": (event.get("dropoff_location") or {}).get("zone_id"),
        "passenger_count": event.get("passenger_count"),
        # The outcome, carried for comparison only. Same definition the model
        # was trained against, computed the same way.
        "actual": float(event["total_amount"]) - float(event["tip_amount"]),
    }


def run() -> None:
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    model, model_version = _load_model()
    connection = _connect_postgres()

    consumer = Consumer(
        {
            "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
            "group.id": config.KAFKA_GROUP_ID,
            # Applies only to a group with no committed offsets, i.e. the
            # first ever start. A restart afterwards resumes where it left off,
            # which is what you want in normal operation.
            #
            # The consequence is an operational step worth knowing: **changing
            # the model does not re-score anything.** Existing rows keep their
            # old prediction and old model_version forever, because the group
            # has already read past them. To re-score after training a new
            # version, stop this service and reset the group:
            #
            #   docker compose stop ml_predictor
            #   docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group ml-predictor --topic tlc-raw-events --reset-offsets --to-earliest --execute
            #   docker compose start ml_predictor
            #
            # The upsert on event_id then rewrites each row in place rather than
            # duplicating it.
            "auto.offset.reset": "earliest",
            # Offsets are committed only after a batch is written, so a crash
            # re-delivers rather than loses. The upsert makes that harmless.
            "enable.auto.commit": False,
            # The topic is created by the gateway's first produce and may not
            # exist yet; refresh often enough to pick it up promptly.
            "topic.metadata.refresh.interval.ms": 10_000,
        }
    )
    consumer.subscribe([config.KAFKA_TOPIC])
    logger.info("consuming %s as group %s", config.KAFKA_TOPIC, config.KAFKA_GROUP_ID)

    batch: list[dict] = []
    scored = skipped = 0
    announced_waiting = False

    while _running:
        message = consumer.poll(1.0)

        if message is None:
            if batch:
                _flush(connection, consumer, batch, model, model_version)
                batch = []
            continue

        if message.error():
            if message.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                if not announced_waiting:
                    logger.info(
                        "topic %s does not exist yet — waiting for the first replay",
                        config.KAFKA_TOPIC,
                    )
                    announced_waiting = True
            else:
                logger.error("consumer error: %s", message.error())
            continue

        announced_waiting = False
        try:
            row = _prepare(json.loads(message.value()))
        except Exception:  # noqa: BLE001 — one bad event must not stop the service
            logger.exception("could not score an event; skipping it")
            continue

        if row is None:
            skipped += 1
            continue

        batch.append(row)
        scored += 1
        if len(batch) >= config.WRITE_BATCH_SIZE:
            _flush(connection, consumer, batch, model, model_version)
            batch = []
            logger.info("scored %d event(s), skipped %d at or before the cutoff", scored, skipped)

    if batch:
        _flush(connection, consumer, batch, model, model_version)
    consumer.close()
    connection.close()
    logger.info("stopped after scoring %d event(s)", scored)


def _flush(
    connection: psycopg.Connection,
    consumer: Consumer,
    batch: list[dict],
    model,
    model_version: str,
) -> None:
    """Score a whole batch in one call, write it, then commit offsets.

    One predict() over N rows rather than N calls over one row each: the
    pipeline's fixed cost is paid once per batch instead of once per event.

    Features are built through features.build_features, the same function the
    training set went through — a batch of N and a batch of 1 take an identical
    code path, so there is still exactly one definition of a feature row.

    Order matters at the end. Committing offsets before the write would mean a
    crash between the two loses predictions the consumer believes it has
    already handled.
    """
    frame = pd.DataFrame(batch)
    predictions = model.predict(build_features(frame))

    rows = [
        (
            record["event_id"],
            record["pickup_datetime"],
            record["pickup_zone_id"],
            record["dropoff_zone_id"],
            round(float(predicted), 2),
            round(record["actual"], 2),
            model_version,
        )
        for record, predicted in zip(batch, predictions)
    ]

    with connection.cursor() as cursor:
        cursor.executemany(_UPSERT, rows)
    consumer.commit(asynchronous=False)


def main() -> int:
    try:
        run()
    except Exception:  # noqa: BLE001 — log the trace, then fail
        logger.exception("predictor failed")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
