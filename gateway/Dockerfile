# Gateway image: FastAPI validation + confluent-kafka producer.
FROM python:3.11-slim

# uv gives fast, reproducible installs straight from pyproject.toml / uv.lock.
COPY --from=ghcr.io/astral-sh/uv:0.5.11 /uv /bin/uv

WORKDIR /app

# Copy dependency manifests first so `uv sync` is cached across code-only edits.
# uv.lock is optional here (uv.lock* tolerates its absence) — sync resolves it.
COPY pyproject.toml uv.lock* ./
RUN uv sync --no-dev

# Then the application source.
COPY . .

# Put the project venv on PATH so `uvicorn` resolves without `uv run`.
ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 8000
# 0.0.0.0 so the port is reachable from other Compose services / the host.
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]