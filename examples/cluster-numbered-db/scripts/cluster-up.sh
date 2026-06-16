#!/usr/bin/env bash
#
# Spin up a Redis/Valkey cluster using Docker.
#
# Designed for Amazon Linux 2023 on EC2, where `--network host` works cleanly
# (unlike Docker Desktop on macOS, where host networking and the cluster bus
# ports are flaky). Each node listens on 127.0.0.1:<port> and uses bus port
# <port>+10000.
#
# Works with both Valkey and Redis images. The server/cli binaries and whether
# `--cluster-databases` is passed are derived from the image name, and can be
# overridden explicitly.
#
#   # Valkey 9, multi-database cluster (success path):
#   ./scripts/cluster-up.sh
#
#   # Stock Redis, db0-only cluster (failure path for database_id != 0):
#   IMAGE=redis:7.4 ./scripts/cluster-up.sh
#
# Env overrides:
#   IMAGE         container image            (default: valkey/valkey:9.0)
#   SERVER_BIN    server binary             (default: redis-server for redis* images,
#                                            otherwise valkey-server)
#   CLI_BIN       cli binary                (default: redis-cli / valkey-cli, as above)
#   NAME_PREFIX   container name prefix      (default: cluster-node)
#   BASE_PORT     first node port            (default: 7001)
#   PRIMARIES     number of primary shards   (default: 3)
#   REPLICAS      replicas per primary       (default: 1)
#   DATABASES     value for `databases`      (default: 16)
#   CLUSTER_DATABASES  value for `cluster-databases` (Valkey 9+; what actually
#                 enables >1 database in cluster mode). Defaults to DATABASES for
#                 valkey images and 0 (flag omitted) for redis images. Set to 0
#                 to omit the flag entirely.
#   NODE_TIMEOUT  cluster-node-timeout ms    (default: 5000)
set -euo pipefail

IMAGE="${IMAGE:-${VALKEY_IMAGE:-valkey/valkey:9.0}}"
NAME_PREFIX="${NAME_PREFIX:-cluster-node}"
BASE_PORT="${BASE_PORT:-7001}"
PRIMARIES="${PRIMARIES:-3}"
REPLICAS="${REPLICAS:-1}"
DATABASES="${DATABASES:-16}"
NODE_TIMEOUT="${NODE_TIMEOUT:-5000}"

# Stock Redis ships redis-server/redis-cli and has no `cluster-databases` config;
# Valkey ships valkey-server/valkey-cli and (9+) supports `cluster-databases`.
if [[ "${IMAGE}" == *redis* && "${IMAGE}" != *valkey* ]]; then
  SERVER_BIN="${SERVER_BIN:-redis-server}"
  CLI_BIN="${CLI_BIN:-redis-cli}"
  CLUSTER_DATABASES="${CLUSTER_DATABASES:-0}"
else
  SERVER_BIN="${SERVER_BIN:-valkey-server}"
  CLI_BIN="${CLI_BIN:-valkey-cli}"
  CLUSTER_DATABASES="${CLUSTER_DATABASES:-${DATABASES}}"
fi

total=$(( PRIMARIES + PRIMARIES * REPLICAS ))
last_port=$(( BASE_PORT + total - 1 ))

ports=()
for (( port = BASE_PORT; port <= last_port; port++ )); do
  ports+=( "$port" )
done

echo "Pulling ${IMAGE} ..."
docker pull "${IMAGE}" >/dev/null

echo "Starting ${total} nodes (${PRIMARIES} primaries, ${REPLICAS} replica(s) each) using ${SERVER_BIN} ..."
for port in "${ports[@]}"; do
  name="${NAME_PREFIX}-${port}"
  docker rm -f "${name}" >/dev/null 2>&1 || true

  server_args=(
    "${SERVER_BIN}"
    --port "${port}"
    --cluster-enabled yes
    --cluster-config-file "nodes-${port}.conf"
    --cluster-node-timeout "${NODE_TIMEOUT}"
    --databases "${DATABASES}"
    --appendonly no
    --save ""
    --protected-mode no
    --bind 0.0.0.0
  )
  if [[ "${CLUSTER_DATABASES}" -gt 0 ]]; then
    server_args+=( --cluster-databases "${CLUSTER_DATABASES}" )
  fi

  docker run -d --name "${name}" --network host "${IMAGE}" "${server_args[@]}" >/dev/null
  echo "  started ${name}"
done

echo "Waiting for nodes to answer PING ..."
for port in "${ports[@]}"; do
  ready=false
  for _ in $(seq 1 30); do
    if docker run --rm --network host "${IMAGE}" \
        "${CLI_BIN}" -h 127.0.0.1 -p "${port}" ping 2>/dev/null | grep -q PONG; then
      ready=true
      break
    fi
    sleep 0.5
  done
  if [[ "${ready}" != true ]]; then
    echo "ERROR: node on port ${port} did not become ready" >&2
    exit 1
  fi
done

create_list=()
for port in "${ports[@]}"; do
  create_list+=( "127.0.0.1:${port}" )
done

echo "Creating cluster ..."
docker run --rm --network host "${IMAGE}" \
  "${CLI_BIN}" --cluster create "${create_list[@]}" \
    --cluster-replicas "${REPLICAS}" \
    --cluster-yes

seeds="127.0.0.1:${BASE_PORT},127.0.0.1:$((BASE_PORT + 1)),127.0.0.1:$((BASE_PORT + 2))"
echo
echo "Cluster is up. Run the example with:"
echo
echo "  CLUSTER_NODES=${seeds} \\"
echo "      cargo run -p cluster-numbered-db-example"
echo
