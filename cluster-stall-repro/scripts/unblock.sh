#!/usr/bin/env bash
set -euo pipefail

PORT=${1:?usage: $0 PORT}
N=$((PORT - 7000))
CONTAINER="cluster-stall-valkey-${N}"

docker unpause "$CONTAINER" >/dev/null
echo "unpaused $CONTAINER"
