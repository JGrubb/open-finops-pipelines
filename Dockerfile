# Use official uv image with Python 3.13
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

# Set environment variables for Cloud Run
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

WORKDIR /app

# Copy dependency files first for better layer caching
COPY pyproject.toml ./

# Install dependencies in system Python (no venv needed in container)
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --system -r pyproject.toml

# Copy application code
COPY finops/ ./finops/

# Install the package itself
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --system --no-deps .

# Verify installation
RUN finops --version

# Create data directory structure (will be overridden by GCS mount in Cloud Run)
RUN mkdir -p /app/data/staging /app/data/exports

# Default command (can be overridden by Cloud Scheduler/Cloud Build)
CMD ["finops", "--config", "/config/config.toml", "aws", "run-pipeline"]
