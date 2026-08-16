# Hot-path image: Kafka consumer -> event-time windows -> PostgreSQL.
FROM python:3.11-slim

# uv gives fast, reproducible installs straight from pyproject.toml / uv.lock.
COPY --from=ghcr.io/astral-sh/uv:0.5.11 /uv /bin/uv

WORKDIR /app

# Copy dependency manifests first so `uv sync` is cached across code-only edits.
COPY pyproject.toml uv.lock* ./
RUN uv sync --no-dev

# Then the application source (including schema.sql, applied on startup).
COPY . .

# Put the project venv on PATH so python resolves the synced environment.
ENV PATH="/app/.venv/bin:$PATH"

# Unbuffered so log lines reach `docker compose logs` as they happen rather
# than sitting in a pipe buffer.
ENV PYTHONUNBUFFERED=1

# No port: this is a worker, not a server.
CMD ["python", "consumer.py"]
