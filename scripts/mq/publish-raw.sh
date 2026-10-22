#!/usr/bin/env bash
# Publish arbitrary text as a task update — e.g. a poison message.
#   scripts/mq/publish-raw.sh 'not json at all'
. "$(dirname "$0")/_common.sh"
publish_raw "${1:?usage: $0 '<any text>'}"
