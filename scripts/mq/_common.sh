# Shared by the scripts in scripts/mq: talks to RabbitMQ's own management HTTP API.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
[ -f "$ROOT/.env" ] && set -a && . "$ROOT/.env" && set +a
MGMT="http://localhost:${RABBITMQ_MGMT_PORT:-15672}/api"
AUTH="${RABBITMQ_USER:?set in .env}:${RABBITMQ_PASSWORD:?set in .env}"

new_uuid() {
  if command -v uuidgen >/dev/null 2>&1; then uuidgen | tr 'A-Z' 'a-z'; else cat /proc/sys/kernel/random/uuid; fi
}

# json_string <text>: escape text as a JSON string literal (backslashes, quotes, control chars)
json_string() {
  printf '"%s"' "$(printf '%s' "$1" | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g' | tr '\n\t\r' '   ')"
}

# publish_raw <payload-text>: publish to exchange `provisioning` with routing key `task.update`
publish_raw() {
  local body
  body="{\"properties\":{\"delivery_mode\":2,\"content_type\":\"application/json\"},\"routing_key\":\"task.update\",\"payload_encoding\":\"string\",\"payload\":$(json_string "$1")}"
  curl -fsS -u "$AUTH" -H 'content-type: application/json' \
    -X POST "$MGMT/exchanges/%2F/provisioning/publish" -d "$body"
  echo
}
