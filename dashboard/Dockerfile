# Dashboard image: Streamlit over the serving store.
FROM python:3.11-slim

# uv gives fast, reproducible installs straight from pyproject.toml / uv.lock.
COPY --from=ghcr.io/astral-sh/uv:0.5.11 /uv /bin/uv

WORKDIR /app

# Dependency manifests first, so `uv sync` is cached across code-only edits.
COPY pyproject.toml uv.lock* ./
RUN uv sync --no-dev

# Then the application source, including .streamlit/config.toml which pins the
# theme the chart palette was validated against.
COPY . .

ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1

EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
