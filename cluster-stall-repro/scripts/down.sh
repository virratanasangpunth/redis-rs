#!/usr/bin/env bash
set -euo pipefail

NAME_PREFIX=${NAME_PREFIX:-cluster-stall-valkey}
PORTS=(7001 7002 7003 7004 7005 7006)

for port in "${PORTS[@]}"; do
  idx=$((port - 7000))
  name="${NAME_PREFIX}-${idx}"
  if docker ps -a --format '{{.Names}}' | grep -qx "$name"; then
    docker rm -f "$name" >/dev/null
    echo "  removed $name"
  fi
done

if [[ "${1:-}" == "--volumes" ]]; then
  for port in "${PORTS[@]}"; do
    idx=$((port - 7000))
    vol="${NAME_PREFIX}-${idx}-data"
    if docker volume rm "$vol" >/dev/null 2>&1; then
      echo "  removed volume $vol"
    fi
  done
fi
