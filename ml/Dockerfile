# ML layer image, shared by `ml_train` and `ml_predictor`.
#
# One image for both on purpose. A model pickled by one scikit-learn version and
# unpickled by another is a real and quiet failure mode — it may load and give
# subtly different answers rather than erroring. Sharing an image makes the two
# versions impossible to diverge.
FROM python:3.11-slim

# uv gives fast, reproducible installs straight from pyproject.toml / uv.lock.
COPY --from=ghcr.io/astral-sh/uv:0.5.11 /uv /bin/uv

WORKDIR /app

# Dependency manifests first, so `uv sync` is cached across code-only edits.
COPY pyproject.toml uv.lock* ./
RUN uv sync --no-dev

# Then the source, including sql/ which predictor.py applies on start.
COPY . .

# Put the project venv on PATH so python resolves the synced environment.
ENV PATH="/app/.venv/bin:$PATH"

# Unbuffered so log lines reach `docker compose logs` as they happen rather
# than sitting in a pipe buffer.
ENV PYTHONUNBUFFERED=1

# No port: both roles are workers, not servers. The command is set per service
# in docker-compose.yml — train.py for the one-shot, predictor.py for the
# long-running consumer.
CMD ["python", "predictor.py"]
