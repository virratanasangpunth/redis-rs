#!/usr/bin/env bash
#
# Tear down the cluster started by cluster-up.sh.
#
# Env overrides (must match cluster-up.sh):
#   NAME_PREFIX  container name prefix     (default: cluster-node)
#   BASE_PORT    first node port           (default: 7001)
#   PRIMARIES    number of primary shards  (default: 3)
#   REPLICAS     replicas per primary      (default: 1)
set -euo pipefail

NAME_PREFIX="${NAME_PREFIX:-cluster-node}"
BASE_PORT="${BASE_PORT:-7001}"
PRIMARIES="${PRIMARIES:-3}"
REPLICAS="${REPLICAS:-1}"

total=$(( PRIMARIES + PRIMARIES * REPLICAS ))
last_port=$(( BASE_PORT + total - 1 ))

echo "Removing ${NAME_PREFIX} containers on ports ${BASE_PORT}..${last_port} ..."
for (( port = BASE_PORT; port <= last_port; port++ )); do
  if docker rm -f "${NAME_PREFIX}-${port}" >/dev/null 2>&1; then
    echo "  removed ${NAME_PREFIX}-${port}"
  fi
done
echo "Done."
