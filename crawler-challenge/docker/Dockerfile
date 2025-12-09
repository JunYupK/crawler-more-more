# Multi-stage build for smaller image size
FROM python:3.11-slim as builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Create virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy requirements and install Python dependencies
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Final stage
FROM python:3.11-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    libpq5 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy virtual environment from builder
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Create app directory
WORKDIR /app

# Copy application code
COPY *.py ./
COPY config/ ./config/
COPY monitoring/ ./monitoring/
COPY data/ ./data/

# Create non-root user for security
RUN useradd -m -u 1000 crawler && \
    chown -R crawler:crawler /app

USER crawler

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD python3 -c "import redis; r = redis.Redis(host='${REDIS_HOST:-redis-service}', port=int('${REDIS_PORT:-6379}')); r.ping()" || exit 1

# Default command
CMD ["python3", "multithreaded_crawler.py", "--count", "10000", "--workers", "16", "--batch-size", "100"]
