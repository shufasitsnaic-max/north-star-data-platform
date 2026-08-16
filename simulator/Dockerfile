# Simulator image: reads parquet (pyarrow) and POSTs records to the gateway.
FROM python:3.11-slim

COPY --from=ghcr.io/astral-sh/uv:0.5.11 /uv /bin/uv

WORKDIR /app

# Dependency layer first for caching.
COPY pyproject.toml uv.lock* ./
RUN uv sync --no-dev

COPY . .

ENV PATH="/app/.venv/bin:$PATH"

# One-shot job (not a server): run the replay and exit. Behaviour is driven by
# env vars (DATA_DIR, GATEWAY_URL, START_DATETIME, MAX_ROWS, SLEEP).
CMD ["python", "replay.py"]