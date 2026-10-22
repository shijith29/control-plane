#!/usr/bin/env bash
# Block until the control plane reports ready (DB + broker), or fail with diagnostics.
set -euo pipefail
cd "$(dirname "$0")/.."
[ -f .env ] && set -a && . ./.env && set +a

PORT="${HTTP_PORT:-8080}"
URL="http://localhost:${PORT}/readyz"
DEADLINE=$((SECONDS + ${WAIT_TIMEOUT:-180}))

printf "waiting for %s " "$URL"
until curl -fsS "$URL" >/dev/null 2>&1; do
  if [ "$SECONDS" -ge "$DEADLINE" ]; then
    echo " timed out"
    docker compose ps
    docker compose logs --tail=40 migrate control-plane
    exit 1
  fi
  printf "."
  sleep 1
done
echo " ready"
cat <<EOF

  API            http://localhost:${PORT}
  OpenAPI docs   http://localhost:${PORT}/docs
  RabbitMQ UI    http://localhost:${RABBITMQ_MGMT_PORT:-15672}   (user/password from .env)

  try:  make demo
EOF
