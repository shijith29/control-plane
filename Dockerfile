# One image, two targets:
#   runtime — control plane, worker and migrations (no dev tools, non-root)
#   dev     — + test/lint/type/security tooling; used by `make test` and `make check`
#
# Only the locked third-party dependencies are installed; our own code runs from /app/src via
# PYTHONPATH. That means the build never has to package the project, so it needs no packages
# beyond those pinned in uv.lock (fewer downloads, nothing unpinned).
FROM python:3.12-slim AS base
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_HTTP_TIMEOUT=300 \
    UV_HTTP_RETRIES=8 \
    PATH=/opt/venv/bin:$PATH
RUN pip install --no-cache-dir --retries 8 --timeout 120 "uv==0.11.32"
WORKDIR /app

# ------------------------------------------------------------------------------------------
FROM base AS runtime
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project
COPY src ./src
COPY alembic.ini ./
COPY migrations ./migrations
COPY scripts ./scripts
RUN useradd --system --uid 10001 --no-create-home app
USER app
EXPOSE 8080
CMD ["uvicorn", "control_plane.main:create_app", "--factory", "--host", "0.0.0.0", "--port", "8080"]

# ------------------------------------------------------------------------------------------
FROM base AS dev
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project
COPY . .
CMD ["pytest", "--cov"]
