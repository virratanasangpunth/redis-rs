//! Example: using a numbered logical database with a Valkey cluster client.
//!
//! Classic Redis Cluster only supports database `0`. Valkey 9 added support for
//! multiple logical databases in cluster mode, and `ClusterClientBuilder` now
//! exposes [`database_id`] so every node connection issues `SELECT <db>` during
//! its handshake (and re-applies it on every reconnect).
//!
//! This binary connects two clients to the *same* cluster — one on database 0
//! and one on database 1 — and demonstrates three things:
//!
//! 1. **Isolation:** the same key holds different values on each database, and a
//!    key written only to db1 is not visible from db0.
//! 2. **Reconnect:** after forcibly dropping the cluster client's connections
//!    (via `CLIENT KILL`), the client transparently reconnects and the new
//!    connection is *still* on db1 — proving `database_id` is reapplied during
//!    the reconnect handshake, not just the initial one.
//! 3. **`SELECT` is rejected:** because the database is fixed for the client's
//!    lifetime via `database_id`, issuing `SELECT` at runtime is unsupported and
//!    is rejected client-side (`ErrorKind::Client`) *before* anything reaches a
//!    node — for single commands, pipelines, and transactions alike.
//!
//! Run a local cluster first (see `scripts/cluster-up.sh`), then:
//!
//! ```sh
//! CLUSTER_NODES=127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003 \
//!     cargo run -p cluster-numbered-db-example
//! ```
//!
//! Errors are surfaced with `.expect(...)`, so a failure panics with a
//! descriptive message rather than bubbling up as a `Result`.
//!
//! [`database_id`]: redis::cluster::ClusterClientBuilder::database_id

use redis::cluster::ClusterClientBuilder;
use redis::cluster_async::ClusterConnection;
use redis::{cmd, pipe, ErrorKind, RedisResult, Value};

const SHARED_KEY: &str = "numbered-db:example:shared-key";

/// Connect to the cluster, selecting `database_id` on every node connection.
async fn connect(nodes: &[String], database_id: i64) -> ClusterConnection {
    let client = ClusterClientBuilder::new(nodes.to_vec())
        .database_id(database_id)
        .build()
        .expect("failed to build cluster client from the seed nodes");
    client.get_async_connection().await.expect(
        "failed to connect to the Valkey cluster \
         (is it running, and does it support the requested database via cluster-databases?)",
    )
}

/// Returns the `db=<n>` field the server reports for this connection via `CLIENT INFO`.
async fn reported_db(conn: &mut ClusterConnection) -> String {
    let info: String = cmd("CLIENT")
        .arg("INFO")
        .query_async(conn)
        .await
        .expect("CLIENT INFO command failed");
    info.split_whitespace()
        .find(|field| field.starts_with("db="))
        .unwrap_or("db=?")
        .to_string()
}

/// Severs the cluster client's connection to a single node by opening a *direct*
/// (non-cluster) connection to it and running `CLIENT KILL TYPE normal`, which
/// drops every other normal client connection on that node. Returns the number
/// of connections killed.
async fn kill_normal_connections(node: &str) -> i64 {
    let client = redis::Client::open(node).expect("failed to open a direct client to the node");
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .expect("failed to open a direct connection to the node");
    cmd("CLIENT")
        .arg("KILL")
        .arg("TYPE")
        .arg("normal")
        .query_async::<i64>(&mut conn)
        .await
        .expect("CLIENT KILL failed")
}

#[tokio::main]
async fn main() {
    let nodes = cluster_nodes();
    println!("Seed nodes: {nodes:?}\n");

    // Two clients against the same cluster, each pinned to a different database.
    let mut db0 = connect(&nodes, 0).await;
    let mut db1 = connect(&nodes, 1).await;

    isolation_demo(&mut db0, &mut db1).await;
    select_rejected_demo(&mut db0).await;
    reconnect_demo(&nodes, &mut db1).await;

    println!(
        "\n✅ SUCCESS: numbered databases are isolated, reapplied on reconnect, \
         and runtime SELECT is rejected client-side."
    );
}

/// Asserts that a command was rejected by the cluster client itself — i.e. it
/// failed with [`ErrorKind::Client`] (the routing layer refusing to send it),
/// not with a server error. Panics if the command unexpectedly succeeded or
/// failed for a different reason.
fn assert_rejected(label: &str, result: RedisResult<Value>) {
    match result {
        Ok(value) => {
            panic!("{label}: expected the cluster client to reject SELECT, but it returned {value:?}")
        }
        Err(err) => {
            assert_eq!(
                err.kind(),
                ErrorKind::Client,
                "{label}: expected a client-side (ErrorKind::Client) rejection, got {err:?}",
            );
            println!("  {label}: rejected client-side -> {err}");
        }
    }
}

/// Demonstrates that `SELECT` is rejected on a cluster connection across every
/// way a command can be issued. None of these commands ever reach a node: the
/// routing layer rejects the request up front, so the rejection is independent
/// of whether the server itself would have accepted the database number.
async fn select_rejected_demo(conn: &mut ClusterConnection) {
    println!("\n=== SELECT is rejected on a cluster connection ===");

    // A single non-zero SELECT.
    assert_rejected(
        "SELECT 1 (single command)",
        cmd("SELECT").arg(1).query_async::<Value>(conn).await,
    );

    // `SELECT 0` is special: a standalone cluster node *accepts* it (db 0 is the
    // only valid database in cluster mode), yet the client still rejects it,
    // because the database is fixed via `database_id` and cannot change at runtime.
    assert_rejected(
        "SELECT 0 (single command; a node would accept this)",
        cmd("SELECT").arg(0).query_async::<Value>(conn).await,
    );

    // Command-name matching is case-insensitive.
    assert_rejected(
        "select 1 (lowercase)",
        cmd("select").arg(1).query_async::<Value>(conn).await,
    );

    // A pipeline whose only command is SELECT.
    assert_rejected(
        "pipeline [SELECT 1]",
        pipe().cmd("SELECT").arg(1).query_async::<Value>(conn).await,
    );

    // A pipeline mixing SELECT with ordinary commands: the *entire* pipeline is
    // rejected at routing time, so the SET and GET never run either.
    let pipe_key = "numbered-db:example:pipe-key";
    assert_rejected(
        "pipeline [SET, SELECT 1, GET]",
        pipe()
            .cmd("SET")
            .arg(pipe_key)
            .arg("never-written")
            .ignore()
            .cmd("SELECT")
            .arg(1)
            .cmd("GET")
            .arg(pipe_key)
            .query_async::<Value>(conn)
            .await,
    );
    // Confirm the SET above was indeed never applied.
    let leaked: Option<String> = cmd("GET")
        .arg(pipe_key)
        .query_async(conn)
        .await
        .expect("GET of the pipeline key failed");
    assert!(
        leaked.is_none(),
        "the rejected pipeline must not have written {pipe_key}, found {leaked:?}",
    );

    // An atomic pipeline (MULTI/EXEC transaction) containing SELECT.
    assert_rejected(
        "atomic pipeline [SELECT 1]",
        pipe()
            .atomic()
            .cmd("SELECT")
            .arg(1)
            .query_async::<Value>(conn)
            .await,
    );

    println!("All SELECT variants were rejected client-side (ErrorKind::Client).");
}

/// Shows that db0 and db1 are independent keyspaces on the same cluster.
async fn isolation_demo(db0: &mut ClusterConnection, db1: &mut ClusterConnection) {
    println!("=== Isolation ===");
    println!("db0 client: CLIENT INFO reports {}", reported_db(db0).await);
    println!("db1 client: CLIENT INFO reports {}", reported_db(db1).await);

    // Same key, different value on each database.
    cmd("SET")
        .arg(SHARED_KEY)
        .arg("value-in-db-0")
        .query_async::<()>(db0)
        .await
        .expect("failed to SET the shared key on db0");
    cmd("SET")
        .arg(SHARED_KEY)
        .arg("value-in-db-1")
        .query_async::<()>(db1)
        .await
        .expect("failed to SET the shared key on db1");

    let from_db0: String = cmd("GET")
        .arg(SHARED_KEY)
        .query_async(db0)
        .await
        .expect("failed to GET the shared key from db0");
    let from_db1: String = cmd("GET")
        .arg(SHARED_KEY)
        .query_async(db1)
        .await
        .expect("failed to GET the shared key from db1");
    println!("Wrote the same key on both databases:");
    println!("  db0 GET {SHARED_KEY} => {from_db0:?}");
    println!("  db1 GET {SHARED_KEY} => {from_db1:?}");
    assert_eq!(from_db0, "value-in-db-0");
    assert_eq!(from_db1, "value-in-db-1");

    // A key written only to db1 must not be visible from db0.
    let db1_only = "numbered-db:example:db1-only";
    cmd("SET")
        .arg(db1_only)
        .arg("only here")
        .query_async::<()>(db1)
        .await
        .expect("failed to SET the db1-only key on db1");
    let seen_from_db0: Option<String> = cmd("GET")
        .arg(db1_only)
        .query_async(db0)
        .await
        .expect("failed to GET the db1-only key from db0");
    println!("Key written only to db1 is invisible from db0:");
    println!("  db0 GET {db1_only} => {seen_from_db0:?} (expected None)");
    assert!(seen_from_db0.is_none());
}

/// Forces the db1 client to reconnect and verifies it is still on db1 afterwards.
async fn reconnect_demo(nodes: &[String], db1: &mut ClusterConnection) {
    println!("\n=== Reconnect ===");

    // Precondition: we are on db1, and the value written during the isolation
    // demo is present.
    assert_eq!(reported_db(db1).await, "db=1");

    // Drop the cluster client's connection to every seed node from the server
    // side. The next command issued on `db1` must therefore reconnect.
    println!("Killing the cluster client's node connections ...");
    let mut total_killed = 0;
    for node in nodes {
        let killed = kill_normal_connections(node).await;
        println!("  {node}: killed {killed} connection(s)");
        total_killed += killed;
    }
    assert!(
        total_killed > 0,
        "expected to kill at least the cluster client's own connection"
    );

    // This command transparently reconnects (re-running the handshake, which
    // re-issues SELECT 1). If the database were *not* reapplied, the reconnected
    // connection would silently fall back to db0.
    let after: String = cmd("GET")
        .arg(SHARED_KEY)
        .query_async(db1)
        .await
        .expect("GET after reconnect failed");
    let db_after = reported_db(db1).await;

    println!("After reconnect:");
    println!("  CLIENT INFO reports {db_after}");
    println!("  db1 GET {SHARED_KEY} => {after:?}");
    assert_eq!(
        db_after, "db=1",
        "reconnected connection must be back on db1, not db0"
    );
    assert_eq!(
        after, "value-in-db-1",
        "db1 data must still be visible after reconnect"
    );
}

/// Reads seed nodes from `CLUSTER_NODES` (comma-separated `host:port`), defaulting
/// to the three primaries created by `scripts/cluster-up.sh`. Bare `host:port`
/// entries are turned into `redis://host:port` URLs.
fn cluster_nodes() -> Vec<String> {
    let raw = std::env::var("CLUSTER_NODES")
        .unwrap_or_else(|_| "127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003".to_string());
    raw.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| {
            if s.contains("://") {
                s.to_string()
            } else {
                format!("redis://{s}")
            }
        })
        .collect()
}
