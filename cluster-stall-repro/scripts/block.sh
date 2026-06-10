#!/usr/bin/env bash
set -euo pipefail

if [[ $EUID -ne 0 ]]; then
  exec sudo -E "$0" "$@"
fi

PORT=${1:?usage: $0 PORT}
N=$((PORT - 7000))
CONTAINER="cluster-stall-valkey-${N}"

docker pause "$CONTAINER" >/dev/null
ss -K -t "( sport = :$PORT or dport = :$PORT )" >/dev/null 2>&1 || true

echo "paused $CONTAINER, RST'd sockets to/from tcp/$PORT"
