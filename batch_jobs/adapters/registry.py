"""Source lookup for the one thing a lake writer cannot avoid knowing.

`source_extras` is stored as a typed struct rather than a JSON blob, which
means whoever writes the lake has to know that struct's field names. For
`bulk_load` that is free — it already imports a source-specific adapter,
because it *is* an adapter invocation. For `stream_to_lake` it is a problem:
that job sits downstream of the message bus, where the core principle says
nothing may name a data source.

This registry is the seam. The job asks for a shape by name and never learns
which source it belongs to; the name arrives as configuration. That is the same
arrangement already recorded for `KAFKA_TOPIC: tlc-raw-events` — **configuration
may know the source, code may not** — and it keeps the swap story intact:
adding a source means writing an adapter and adding a line here, not editing
any job.

Rejected storing `source_extras` as a JSON string, which would need no registry
at all: roughly 30x the bytes and a parse on every read, and it would give up
the columnar access that made `ratecode_id` a candidate ML feature in the first
place.
"""

from __future__ import annotations

from pyspark.sql.types import StructType

from adapters.tlc_batch_adapter import TLC_SOURCE_EXTRAS

# Keyed by the value each adapter writes into a canonical event's `source`
# field, so a lake row can always be traced back to the shape it was written
# with. Note the key matches `tlc_batch_adapter.SOURCE`, not the topic name.
_SOURCE_EXTRAS: dict[str, StructType] = {
    "tlc_yellow": TLC_SOURCE_EXTRAS,
}


def source_extras_schema(source: str) -> StructType:
    """The `source_extras` StructType for a named source.

    Raises loudly on an unknown name rather than defaulting to an empty struct.
    A silent fallback would write a lake whose physical schema disagrees with
    the other writer's — the exact corruption the canonical contract exists to
    prevent, and one that would only surface much later as a read error.
    """
    try:
        return _SOURCE_EXTRAS[source]
    except KeyError:
        known = ", ".join(sorted(_SOURCE_EXTRAS)) or "(none registered)"
        raise KeyError(
            f"no source_extras schema registered for {source!r}; known sources: {known}"
        ) from None
