#!/usr/bin/env bash
# Message count of a queue:  scripts/mq/queue-depth.sh task_updates.dlq
. "$(dirname "$0")/_common.sh"
QUEUE="${1:?usage: $0 <queue>}"
curl -fsS -u "$AUTH" "$MGMT/queues/%2F/${QUEUE}" | grep -o '"messages":[0-9]*' | head -1 | cut -d: -f2
