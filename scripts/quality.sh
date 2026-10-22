#!/usr/bin/env sh
# Static quality gates (run by `make quality` / `make check` inside the dev image).
set -eu
cd "$(dirname "$0")/.."

echo "==> ruff format --check";  ruff format --check .
echo "==> ruff check";           ruff check .
echo "==> mypy --strict";        mypy
echo "==> bandit";               bandit -q -c pyproject.toml -r src
echo "==> pip-audit";            scripts/audit-deps.sh
echo "all quality gates passed"
