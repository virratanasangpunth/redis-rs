// ElastiCache TCP Deadlock Reproducer
//
// The reproducer spawns N workers doing concurrent GET/SET of configurable-size
// objects across a keyspace of 20 keys. On ElastiCache, with large items (>20mib), 
// these will eventually deadlock. 
// Press Ctrl+C to stop.
//
// Usage:
//
//   REDIS_URL=rediss://your-elasticache:6379#insecure \
//   PAYLOAD_SIZE_BYTES=20971520 \
//     cargo run --example large-items-elasticache-deadlock \
//     --features tokio-comp,tokio-rustls-comp,tls-rustls-insecure

use redis::{AsyncCommands, AsyncConnectionConfig};
use std::time::Instant;

const NUM_KEYS: usize = 20;
const NUM_TASKS: usize = 1;

fn key_name(i: usize) -> String {
    format!("item_no_{i}")
}

async fn redis_set_task(id: usize, mut con: redis::aio::MultiplexedConnection, payload_size: usize) {
    let payload = vec![0x42u8; payload_size];
    let mut i: u64 = 0;
    loop {
        let key = key_name(i as usize % NUM_KEYS);
        println!("[redis-set-task-{id}] SET {key}");
        let start = Instant::now();
        match con.set::<_, _, ()>(&key, &payload).await {
            Ok(()) => {
                i += 1;
                println!("[redis-set-task-{id}] SET {key} OK ({:.1?})", start.elapsed());
            }
            Err(e) => {
                println!("[redis-set-task-{id}] SET {key} failed ({:.1?}): {e}", start.elapsed());
            }
        }
    }
}

async fn redis_get_task(id: usize, mut con: redis::aio::MultiplexedConnection) {
    let mut i: u64 = 0;
    loop {
        let key = key_name(i as usize % NUM_KEYS);
        println!("[redis-get-task-{id}] GET {key}");
        let start = Instant::now();
        let result: Result<Option<Vec<u8>>, _> = con.get(&key).await;
        match result {
            Ok(_) => {
                i += 1;
                println!("[redis-get-task-{id}] GET {key} OK ({:.1?})", start.elapsed());
            }
            Err(e) => {
                println!("[redis-get-task-{id}] GET {key} failed ({:.1?}): {e}", start.elapsed());
            }
        }
    }
}

#[tokio::main]
async fn main() {
    rustls::crypto::ring::default_provider().install_default().expect("tls configuration to work");

    let url = std::env::var("REDIS_URL").expect("REDIS_URL env var is required");
    let payload_size: usize = std::env::var("PAYLOAD_SIZE_BYTES")
        .expect("PAYLOAD_SIZE_BYTES env var is required (in bytes)")
        .parse()
        .expect("PAYLOAD_SIZE_BYTES must be a valid integer");

    println!("Connecting to {url}...");
    let client = redis::Client::open(url.clone()).expect("Failed to create redis client");
    let config = AsyncConnectionConfig::default().set_response_timeout(None);
    let con = client
        .get_multiplexed_async_connection_with_config(&config)
        .await
        .expect("Failed to connect");

    println!("Connected to {url}. Payload size: {payload_size} bytes");

    let mut handles = Vec::new();
    for id in 0..NUM_TASKS {
        handles.push(tokio::spawn(redis_set_task(id, con.clone(), payload_size)));
    }
    for id in 0..NUM_TASKS {
        handles.push(tokio::spawn(redis_get_task(id, con.clone())));
    }

    println!("Press Ctrl+C to stop.");
    for h in handles {
        let _ = h.await;
    }
}
