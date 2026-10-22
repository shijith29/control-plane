#!/usr/bin/env bash
# Publish a TaskUpdate straight onto the broker (bypassing the worker).
#   scripts/mq/publish-update.sh <task_id> <in_progress|done|failed> [update_id]
# Reuse an update_id to simulate a duplicate delivery.
. "$(dirname "$0")/_common.sh"

TASK_ID="${1:?usage: $0 <task_id> <status> [update_id]}"
STATUS="${2:?usage: $0 <task_id> <status> [update_id]}"
UPDATE_ID="${3:-$(new_uuid)}"
case "$STATUS" in in_progress|done|failed) ;; *) echo "status must be in_progress|done|failed" >&2; exit 2;; esac

PAYLOAD="{\"schema_version\":1,\"update_id\":\"${UPDATE_ID}\",\"task_id\":\"${TASK_ID}\",\"status\":\"${STATUS}\"}"
echo "publishing: $PAYLOAD"
publish_raw "$PAYLOAD"
