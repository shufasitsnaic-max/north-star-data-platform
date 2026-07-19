"""FastAPI validation gateway.

Phase 1 scope only: accept a TLC-shaped trip record, validate it, adapt it
to the canonical TripEvent, and return the result. No Kafka, no notion of
replay, batching, or live-vs-historical — those are deliberately out of
scope here; see CLAUDE.md for the phase boundary.
"""

import logging
import sys
from datetime import datetime, timezone
from uuid import uuid4

from fastapi import FastAPI, HTTPException
from pydantic import ValidationError

from adapters.tlc_adapter import adapt_tlc
from schemas.canonical import TripEvent
from schemas.tlc import TLCTripInput

logging.basicConfig(stream=sys.stderr, level=logging.INFO)
logger = logging.getLogger("gateway")

app = FastAPI(title="North Star Gateway", version="0.1.0")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/events/trips", response_model=TripEvent)
def ingest_trip(trip: TLCTripInput) -> TripEvent:
    # FastAPI already rejects type-malformed bodies against TLCTripInput
    # with a 422 before this runs. This try/except catches the second,
    # separate failure mode: a record that's type-valid but semantically
    # broken (e.g. dropoff before pickup), which only surfaces once
    # adapt_tlc constructs the canonical TripEvent and its validators run.
    try:
        return adapt_tlc(trip, event_id=uuid4(), ingested_at=datetime.now(timezone.utc))
    except ValidationError as exc:
        logger.exception("rejected trip record during adaptation")
        # include_input/include_context/include_url=False: the default
        # error dicts embed the full attempted TripEvent (UUID, Decimal,
        # enum instances) and the raw ValueError object, none of which
        # are JSON-serializable — including them crashes the response
        # encoder itself (a 500) instead of returning the intended 422.
        errors = exc.errors(include_url=False, include_context=False, include_input=False)
        raise HTTPException(status_code=422, detail=errors) from exc
