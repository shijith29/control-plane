#!/usr/bin/env sh
# Audit the locked third-party dependencies (our own package is not on PyPI, so it is excluded).
set -eu
cd "$(dirname "$0")/.."
REQ="$(mktemp)"
trap 'rm -f "$REQ"' EXIT
uv export --frozen --no-emit-project --no-hashes -q > "$REQ"
pip-audit -r "$REQ" --disable-pip --no-deps --progress-spinner off
