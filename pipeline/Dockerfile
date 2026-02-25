# Start with slim Python 3.13 image for smaller size
FROM python:3.13.11-slim

# Copy uv binary from official uv image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Set working directory inside container
WORKDIR /code

# Make uv create the venv inside /app and make it active
# ENV UV_PROJECT_ENVIRONMENT=/app/.venv
ENV PATH="/code/.venv/bin:$PATH"
# ENV PYTHONUNBUFFERED=1

# Copy dependency files first (better caching)
COPY pyproject.toml uv.lock .python-version ./

# Install dependencies exactly as locked (no project install needed unless you want it)
RUN uv sync --locked --no-dev

# Copy app code (include both scripts)
COPY ingest_data.py zones.py ./

# Default: run ingest_data.py (compose will override for zones_loader)
ENTRYPOINT ["python", "ingest_data.py"]