# ============================================
# Stage 1: Builder
# ============================================
FROM python:3.12-slim as builder

# Metadata
LABEL maintainer="jeanmichel.liev1@gmail.com"
LABEL description="GDPR-compliant dbt anonymization pipeline"
LABEL version="1.0.0"

# Python environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install uv (modern Python package manager)
RUN pip install uv

# Working directory
WORKDIR /app

# Copy dependency file
COPY pyproject.toml ./

# Install Python dependencies using uv
RUN uv pip install --system -r pyproject.toml

# ============================================
# Stage 2: Runtime
# ============================================
FROM python:3.12-slim

# Runtime environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/app/.venv/bin:$PATH" \
    DBT_PROFILES_DIR=/app/dbt_project

# Install minimal system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user for security
RUN groupadd -r dbtuser && useradd -r -g dbtuser dbtuser

# Working directory
WORKDIR /app

# Copy Python dependencies from builder stage
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy application code
COPY --chown=dbtuser:dbtuser . .

# Create required directories
RUN mkdir -p \
    /app/dbt_project/logs \
    /app/dbt_project/target \
    /app/data \
    && chown -R dbtuser:dbtuser /app

# Switch to non-root user
USER dbtuser

# Healthcheck
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import duckdb; print('OK')" || exit 1

# Expose port for `dbt docs serve`
EXPOSE 8080

# Default entrypoint
CMD ["dbt", "--version"]
