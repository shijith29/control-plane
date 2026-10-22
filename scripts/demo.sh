#!/usr/bin/env bash
# Happy path: create a tenant and print every state it passes through until it is active.
set -euo pipefail
cd "$(dirname "$0")/.."
[ -f .env ] && set -a && . ./.env && set +a
API="http://localhost:${HTTP_PORT:-8080}"

field() { grep -o "\"$1\":\"[^\"]*\"" | head -1 | cut -d'"' -f4; }
num()   { grep -o "\"$1\":[0-9]*" | head -1 | cut -d: -f2; }

SLUG="demo-$(date +%s)"
echo "POST /v1/tenants  slug=${SLUG}"
CREATED=$(curl -fsS -X POST "$API/v1/tenants" -H 'content-type: application/json' \
  -d "{\"slug\":\"${SLUG}\",\"name\":\"Demo ${SLUG}\"}")
ID=$(printf '%s' "$CREATED" | field id)
echo "  -> tenant ${ID}"

LAST=""
for _ in $(seq 1 60); do
  BODY=$(curl -fsS "$API/v1/tenants/$ID")
  STATUS=$(printf '%s' "$BODY" | field status)
  TASK=$(printf '%s' "$BODY" | grep -o '"current_task":{[^}]*}' | grep -o '"status":"[^"]*"' | cut -d'"' -f4)
  VERSION=$(printf '%s' "$BODY" | num version)
  LINE="tenant=${STATUS} version=${VERSION} deploy-task=${TASK}"
  [ "$LINE" != "$LAST" ] && echo "  $LINE" && LAST="$LINE"
  [ "$STATUS" = "active" ] || [ "$STATUS" = "failed" ] && break
  sleep 0.5
done

echo
echo "GET /v1/tenants/${ID}"
curl -fsS "$API/v1/tenants/$ID"; echo
echo "GET /v1/tenants/${ID}/tasks"
curl -fsS "$API/v1/tenants/$ID/tasks"; echo
