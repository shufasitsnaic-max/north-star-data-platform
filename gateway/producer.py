"""Kafka producer for the gateway.

The gateway's job ends at the canonical contract: it validates, adapts, and
hands the resulting TripEvent to Kafka. This module is the only place in the
gateway that knows a message bus exists. It deliberately knows nothing about
TLC or any source — it publishes canonical TripEvents, serialized as JSON.

Design notes:
- One long-lived Producer per process (confluent_kafka's Producer is
  thread-safe and internally batches + retries).
- Resilient startup: wait_for_broker() retries with exponential backoff so a
  gateway container that boots before Kafka is ready doesn't crash-loop.
- Async delivery: produce() enqueues and a background thread ships batches. We
  poll(0) after each produce to serve delivery callbacks without blocking the
  request, and flush() on shutdown so nothing is lost. Delivery failures are
  logged to stderr per the error-handling ground rule.
"""

from __future__ import annotations

import logging
import os
import sys
import time

from confluent_kafka import KafkaException, Producer

from schemas.canonical import TripEvent

logging.basicConfig(stream=sys.stderr, level=logging.INFO)
logger = logging.getLogger("gateway.producer")

# Topic is fixed by the platform contract; only the broker address varies by env.
_TOPIC = os.environ.get("KAFKA_TOPIC", "tlc-raw-events")
_producer: Producer | None = None


def _config() -> dict:
    # "Commented config" ground rule: every knob gets a reason.
    return {
        # Internal Compose listener by default; overridden per environment.
        "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "client.id": "gateway",
        # Idempotent producer: retries can't create duplicate records.
        "enable.idempotence": True,
        "acks": "all",
        # Small batching window: trade a little latency for far fewer requests.
        "linger.ms": 50,
        "retries": 5,
    }


def _on_delivery(err, msg) -> None:
    # Called from the producer's background thread once a message is acked.
    if err is not None:
        logger.error("delivery failed (key=%s): %s", msg.key(), err)


def wait_for_broker(max_attempts: int = 8) -> None:
    """Block until the broker answers a metadata request, with backoff.

    Creating a Producer never fails (it connects lazily), so we actively probe
    with list_topics() to distinguish "broker up" from "broker still booting".
    """
    global _producer
    _producer = Producer(_config())
    delay = 1.0
    for attempt in range(1, max_attempts + 1):
        try:
            _producer.list_topics(timeout=5)
            logger.info("connected to Kafka (attempt %d)", attempt)
            return
        except KafkaException as exc:
            logger.warning(
                "Kafka not ready (attempt %d/%d): %s — retrying in %.0fs",
                attempt, max_attempts, exc, delay,
            )
            time.sleep(delay)
            delay = min(delay * 2, 30)  # cap the backoff so we don't wait forever
    raise RuntimeError(f"could not reach Kafka after {max_attempts} attempts")


def publish(event: TripEvent) -> None:
    """Serialize a canonical TripEvent to JSON and enqueue it for the topic."""
    if _producer is None:
        raise RuntimeError("producer not started; call wait_for_broker() first")

    # Key by pickup zone so a zone's trips stay ordered and colocated once we
    # add partitions later. With today's single partition, the simulator's
    # global pickup_datetime order is preserved regardless of key.
    zone = event.pickup_location.zone_id
    key = str(zone).encode("utf-8") if zone is not None else None
    value = event.model_dump_json().encode("utf-8")

    try:
        _producer.produce(_TOPIC, key=key, value=value, on_delivery=_on_delivery)
    except BufferError:
        # Local queue is full: let the client ship in-flight batches, retry once.
        logger.warning("producer queue full — flushing then retrying")
        _producer.poll(1)
        _producer.produce(_TOPIC, key=key, value=value, on_delivery=_on_delivery)

    # Serve delivery callbacks without blocking; actual send happens in bg thread.
    _producer.poll(0)


def flush(timeout: float = 10.0) -> None:
    """Block until queued messages are delivered (called on shutdown)."""
    if _producer is not None:
        remaining = _producer.flush(timeout)
        if remaining:
            logger.error("%d message(s) still undelivered at flush timeout", remaining)
