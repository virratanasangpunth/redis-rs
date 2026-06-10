#!/usr/bin/env bash
set -euo pipefail

IMAGE=${IMAGE:-valkey/valkey:8}
NAME_PREFIX=${NAME_PREFIX:-cluster-stall-valkey}
ANNOUNCE_IP=${ANNOUNCE_IP:-127.0.0.1}
PORTS=(7001 7002 7003 7004 7005 7006)
REPLICAS=1

docker pull -q "$IMAGE" >/dev/null

for port in "${PORTS[@]}"; do
  idx=$((port - 7000))
  name="${NAME_PREFIX}-${idx}"
  vol="${name}-data"
  bus_port=$((10000 + port))

  if docker ps --format '{{.Names}}' | grep -qx "$name"; then
    echo "  $name already running (port $port)"
    continue
  fi
  if docker ps -a --format '{{.Names}}' | grep -qx "$name"; then
    docker rm "$name" >/dev/null
  fi

  docker volume create "$vol" >/dev/null

  docker run -d \
    --name "$name" \
    --network host \
    --restart no \
    -v "${vol}:/data" \
    "$IMAGE" \
    valkey-server \
      --port "$port" \
      --bind 0.0.0.0 \
      --protected-mode no \
      --cluster-enabled yes \
      --cluster-config-file "/data/nodes-${port}.conf" \
      --cluster-node-timeout 5000 \
      --cluster-announce-ip "$ANNOUNCE_IP" \
      --cluster-announce-port "$port" \
      --cluster-announce-bus-port "$bus_port" \
      --save "" \
      --appendonly no \
    >/dev/null
  echo "  started $name on $ANNOUNCE_IP:$port (bus $bus_port)"
done

echo "waiting for nodes to bind..."
for port in "${PORTS[@]}"; do
  for _ in $(seq 1 60); do
    if (echo > "/dev/tcp/${ANNOUNCE_IP}/${port}") 2>/dev/null; then
      echo "  ${ANNOUNCE_IP}:${port} ready"
      break
    fi
    sleep 0.5
  done
done

if docker exec "${NAME_PREFIX}-1" valkey-cli -p "${PORTS[0]}" cluster info 2>/dev/null \
   | grep -q "cluster_state:ok"; then
  echo "cluster already formed"
  exit 0
fi

echo "wiping stale cluster state on all nodes..."
for port in "${PORTS[@]}"; do
  idx=$((port - 7000))
  docker exec "${NAME_PREFIX}-${idx}" valkey-cli -p "$port" flushall >/dev/null
  docker exec "${NAME_PREFIX}-${idx}" valkey-cli -p "$port" cluster reset hard >/dev/null
done

NODES=()
for port in "${PORTS[@]}"; do
  NODES+=("${ANNOUNCE_IP}:${port}")
done

echo "creating cluster across ${NODES[*]} ..."
docker exec -i "${NAME_PREFIX}-1" \
  valkey-cli --cluster create "${NODES[@]}" \
  --cluster-replicas "${REPLICAS}" \
  --cluster-yes

docker exec "${NAME_PREFIX}-1" valkey-cli -p "${PORTS[0]}" cluster info
