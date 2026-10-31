# =============================================================================
# Market Data Pipeline - Dockerfile
# =============================================================================
# Build: docker build -t market-data-pipeline .
# Run: docker run --env-file .env -v ./logs:/app/logs -v ./cache:/app/cache market-data-pipeline
# =============================================================================

FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Install uv for fast package management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy dependency files
COPY pyproject.toml uv.lock* ./

# Install Python dependencies
RUN uv sync --frozen --no-dev

# Copy application code
COPY src/ ./src/
COPY .env.example ./

# Create necessary directories
RUN mkdir -p logs cache access_token

# Set Python path
ENV PYTHONPATH=/app/src

# Default command (can be overridden)
CMD ["uv", "run", "python", "-m", "src.api.main"]