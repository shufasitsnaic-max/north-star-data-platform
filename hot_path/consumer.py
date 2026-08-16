"""Hot path entrypoint: Kafka -> event-time windows -> PostgreSQL.

Reads canonical trip events off the bus, aggregates them into event-time
tumbling windows (windows.py), and upserts those windows into PostgreSQL (db.py).
Knows nothing about any specific data source — only the canonical contract.

Two write cadences, deliberately different:

* **Liveness flush** (wall clock, FLUSH_INTERVAL_SECONDS): rewrite every open
  window as `is_final=false` so the dashboard sees the in-progress bucket while
  it fills. Offsets are *not* committed here — the window is still incomplete.
* **Finalization** (event time, driven by the watermark): a window the watermark
  has passed is written as `is_final=true`, evicted from memory, and only then
  may offsets advance.

That split is the whole design: liveness is a display concern, durability is an
offset concern, and conflating them is what would corrupt window totals across a
restart.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time
from types import FrameType
from typing import Optional

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from pydantic import ValidationError

from db import MetricsSink
from schemas import TripEventView
from windows import WindowStore

logging.basicConfig(stream=sys.stderr, level=logging.INFO)
logger = logging.getLogger("hot_path.consumer")

_TOPIC = os.environ.get("KAFKA_TOPIC", "tlc-raw-events")
_WINDOW_MINUTES = int(os.environ.get("WINDOW_MINUTES", "5"))
_GRACE_MINUTES = int(os.environ.get("GRACE_MINUTES", "10"))
_FLUSH_SECONDS = float(os.environ.get("FLUSH_INTERVAL_SECONDS", "2"))

_shutdown = False


def _request_shutdown(signum: int, _frame: Optional[FrameType]) -> None:
    """Flip the loop flag so the current iteration can finish cleanly."""
    global _shutdown
    logger.info("signal %d received — finishing current batch then exiting", signum)
    _shutdown = True


def _config() -> dict:
    # "Commented config" ground rule: every knob gets a reason.
    return {
        "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
        # Static group id: restarts rejoin the same group and resume from the
        # committed offset rather than replaying the topic.
        "group.id": os.environ.get("KAFKA_GROUP_ID", "hot-path-windows"),
        "client.id": "hot-path",
        # Offsets are committed by hand, only when a window closes. Auto-commit
        # would advance them mid-window and break restart correctness.
        "enable.auto.commit": False,
        # First run has no committed offset: start at the beginning so a replay
        # that already happened is still aggregated.
        "auto.offset.reset": "earliest",
    }


def _wait_for_broker(consumer: Consumer, max_attempts: int = 8) -> None:
    """Block until the broker answers metadata, with capped backoff."""
    delay = 1.0
    for attempt in range(1, max_attempts + 1):
        try:
            consumer.list_topics(timeout=5)
            logger.info("connected to Kafka (attempt %d)", attempt)
            return
        except KafkaException as exc:
            logger.warning(
                "Kafka not ready (attempt %d/%d): %s — retrying in %.0fs",
                attempt, max_attempts, exc, delay,
            )
            time.sleep(delay)
            delay = min(delay * 2, 30)
    raise RuntimeError(f"could not reach Kafka after {max_attempts} attempts")


def _commit(consumer: Consumer, store: WindowStore) -> None:
    """Commit the low-water offset of the oldest still-open window."""
    offsets = store.safe_commit_offsets()
    if not offsets:
        return
    partitions = [TopicPartition(_TOPIC, p, o) for p, o in offsets.items()]
    try:
        consumer.commit(offsets=partitions, asynchronous=False)
    except KafkaException:
        # Not fatal: the same offsets are recomputed and retried next cycle.
        # Worst case is redelivery, which the absolute upsert absorbs.
        logger.exception("offset commit failed — will retry on next finalization")


def main() -> int:
    signal.signal(signal.SIGINT, _request_shutdown)
    signal.signal(signal.SIGTERM, _request_shutdown)

    store = WindowStore(size_minutes=_WINDOW_MINUTES, grace_minutes=_GRACE_MINUTES)
    sink = MetricsSink()
    sink.connect()
    sink.apply_schema()

    consumer = Consumer(_config())
    _wait_for_broker(consumer)
    consumer.subscribe([_TOPIC])
    logger.info(
        "consuming %s: %d-minute windows, %d-minute grace, flushing every %.1fs",
        _TOPIC, _WINDOW_MINUTES, _GRACE_MINUTES, _FLUSH_SECONDS,
    )

    malformed = 0
    last_flush = time.monotonic()

    try:
        while not _shutdown:
            message = consumer.poll(1.0)

            if message is not None:
                if message.error():
                    # EOF is informational on some builds, not a failure.
                    if message.error().code() != KafkaError._PARTITION_EOF:
                        logger.error("consume error: %s", message.error())
                else:
                    try:
                        event = TripEventView.model_validate_json(message.value())
                        store.add(event, message.partition(), message.offset())
                    except (ValidationError, ValueError, json.JSONDecodeError):
                        # Poison message: log with a stack trace, count it, and
                        # step over it. Its offset is still noted so the commit
                        # point cannot stall behind it forever.
                        malformed += 1
                        logger.exception(
                            "unparseable message at %d:%d — skipped (total malformed: %d)",
                            message.partition(), message.offset(), malformed,
                        )
                        store.note_offset(message.partition(), message.offset())

            # Finalize whatever the watermark has moved past, then commit.
            closed = store.take_closed_rows()
            if closed:
                sink.write(closed)
                _commit(consumer, store)
                logger.info(
                    "finalized %d row(s); watermark=%s, open windows=%d",
                    len(closed),
                    store.watermark.isoformat() if store.watermark else "-",
                    store.open_window_count,
                )

            # Liveness flush of in-progress windows. No commit: still filling.
            now = time.monotonic()
            if now - last_flush >= _FLUSH_SECONDS:
                sink.write(store.open_rows())
                last_flush = now

    except Exception:
        logger.exception("hot path failed")
        return 1
    finally:
        # Persist partial state so the dashboard keeps the last view, then let
        # the group rebalance cleanly.
        try:
            sink.write(store.open_rows())
            _commit(consumer, store)
        except Exception:
            logger.exception("error during shutdown flush")
        consumer.close()
        sink.close()
        logger.info(
            "stopped — %d malformed, %d late event(s)", malformed, store.late_events
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
