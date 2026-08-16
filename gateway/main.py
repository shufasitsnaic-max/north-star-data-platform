"""FastAPI validation gateway.

Phase 2 scope: accept a TLC-shaped trip record, validate it, adapt it to the
canonical TripEvent, and PUBLISH it to Kafka (topic tlc-raw-events). The gateway
is the validate -> adapt -> produce boundary and nothing more: it has no notion
of replay, batching, or live-vs-historical — those live in the simulator.

Downstream of the produce() call, nothing may reference TLC; only the canonical
TripEvent crosses the bus. See CLAUDE.md for the source-independence rule.
"""

import logging
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from uuid import uuid4

from fastapi import FastAPI, HTTPException
from pydantic import ValidationError

import producer
from adapters.tlc_adapter import adapt_tlc
from schemas.tlc import TLCTripInput

logging.basicConfig(stream=sys.stderr, level=logging.INFO)
logger = logging.getLogger("gateway")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Connect to Kafka with backoff before serving traffic; flush on shutdown so
    # no accepted event is silently dropped when the container stops.
    producer.wait_for_broker()
    yield
    producer.flush()


app = FastAPI(title="North Star Gateway", version="0.2.0", lifespan=lifespan)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/events/trips", status_code=202)
def ingest_trip(trip: TLCTripInput) -> dict[str, str]:
    # FastAPI already rejects type-malformed bodies against TLCTripInput with a
    # 422 before this runs. This try/except catches the second failure mode: a
    # record that's type-valid but semantically broken (e.g. dropoff before
    # pickup), which only surfaces once adapt_tlc builds the canonical TripEvent
    # and its validators run.
    try:
        event = adapt_tlc(trip, event_id=uuid4(), ingested_at=datetime.now(timezone.utc))
    except ValidationError as exc:
        logger.exception("rejected trip record during adaptation")
        # include_*=False: the default error dicts embed the attempted TripEvent
        # (UUID/Decimal/enum instances) and the raw ValueError, none of which are
        # JSON-serializable — including them would crash the response encoder
        # (a 500) instead of returning the intended 422.
        errors = exc.errors(include_url=False, include_context=False, include_input=False)
        raise HTTPException(status_code=422, detail=errors) from exc

    # Validated + adapted: hand the canonical event to the bus. 202 = accepted
    # for asynchronous publishing, which is honest for an async producer.
    producer.publish(event)
    return {"status": "accepted", "event_id": str(event.event_id)}