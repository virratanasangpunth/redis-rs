# cluster-numbered-db-example

A runnable example of using a **numbered logical database with a Valkey cluster
client** via `ClusterClientBuilder::database_id`.

Classic Redis Cluster only supports database `0`. **Valkey 9** added support for
multiple logical databases in cluster mode. This example connects two clients to
the same cluster — one on db `0`, one on db `1` — and shows the databases are
isolated.

## Why EC2 / Amazon Linux 2023?

The helper scripts use `docker run --network host` so the six cluster nodes and
their cluster-bus ports (`port + 10000`) are reachable on `127.0.0.1`. Host
networking is reliable on Linux but flaky under Docker Desktop on macOS, so this
is meant to run on an **AL2023 EC2 instance**.

## Quick start (on an AL2023 EC2 instance)

```sh
# 1. One-time provisioning (docker + git + rust). Then re-login for the docker group.
./scripts/setup-ec2-al2023.sh
newgrp docker   # or log out / back in

# 2. Start a 6-node Valkey 9 cluster (3 primaries + 3 replicas), 16 databases each.
./scripts/cluster-up.sh

# 3. Run the example against the cluster.
CLUSTER_NODES=127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003 \
    cargo run -p cluster-numbered-db-example

# 4. Tear the cluster down when finished.
./scripts/cluster-down.sh
```

Expected output ends with:

```
=== Isolation ===
db0 client: CLIENT INFO reports db=0
db1 client: CLIENT INFO reports db=1
...
=== Reconnect ===
After reconnect:
  CLIENT INFO reports db=1
  db1 GET numbered-db:example:shared-key => "value-in-db-1"

✅ SUCCESS: numbered databases are isolated AND reapplied on reconnect.
```

## What the example does

**Isolation**

1. Builds two cluster clients with `.database_id(0)` and `.database_id(1)`.
2. Prints each connection's `db=` field from `CLIENT INFO`.
3. Writes the same key with a different value on each database and reads both
   back to show they don't collide.
4. Writes a key only on db1 and confirms it is invisible from db0.

**Reconnect** (proves the database survives a dropped connection)

5. Forcibly drops the cluster client's connection to every node by issuing
   `CLIENT KILL TYPE normal` from a *direct* (non-cluster) connection to each
   seed node.
6. Issues another command on the db1 client, which transparently reconnects, and
   asserts the reconnected connection still reports `db=1` and still sees the db1
   data — confirming `database_id` is reapplied during the reconnect handshake,
   not just the initial connect.

See [`src/main.rs`](src/main.rs).

## Configuration

`cluster-up.sh` / `cluster-down.sh` honor these env vars (defaults shown):

| Var            | Default             | Meaning                          |
| -------------- | ------------------- | -------------------------------- |
| `VALKEY_IMAGE` | `valkey/valkey:9.0` | container image                  |
| `BASE_PORT`    | `7001`              | first node port                  |
| `PRIMARIES`    | `3`                 | number of primary shards         |
| `REPLICAS`     | `1`                 | replicas per primary             |
| `DATABASES`    | `16`                | logical databases per node (sets both `databases` and Valkey 9's `cluster-databases`, which defaults to 1) |
| `NODE_TIMEOUT` | `5000`              | `cluster-node-timeout` (ms)      |

The example honors `CLUSTER_NODES` (comma-separated `host:port`, default
`127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003`).

## Note: server version matters

`database_id` makes the client issue `SELECT <db>` during each node's handshake.
Against a server that does **not** support multiple databases in cluster mode
(Redis OSS cluster, or Valkey < 9), that `SELECT` is rejected and connecting with
a non-zero `database_id` fails with a clear error
(`"Redis server refused to switch database"`). `database_id = 0` (the default)
skips `SELECT` entirely and behaves exactly as before.
