use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use clap::Parser;
use hdrhistogram::Histogram;
use redis::cluster::ClusterClientBuilder;
use redis::cluster_read_routing::RandomReplicaStrategy;
use redis::{Client, Value};

const NUM_SHARDS: usize = 3;
const SHARD_KEYS: [&str; NUM_SHARDS] = [
    "7MbPlVDATO594N5QvhjpXiPVIBl5Zr8e",
    "xODNvJbu7FFd9poyKU9kjDNYag6cV5xs",
    "qyVguF6NgumWgiuh6qmgCQauBGyFHqQI",
];
const SHARD_SLOTS: [u16; NUM_SHARDS] = [2898, 7914, 12392];

#[derive(Parser, Debug)]
#[command(name = "cluster-stall-repro")]
struct Args {
    #[arg(
        long,
        default_value = "redis://127.0.0.1:7001,redis://127.0.0.1:7002,redis://127.0.0.1:7003,redis://127.0.0.1:7004,redis://127.0.0.1:7005,redis://127.0.0.1:7006"
    )]
    seeds: String,

    #[arg(long, default_value_t = 2_000)]
    response_timeout_ms: u64,

    #[arg(long, default_value_t = 10_000)]
    connection_timeout_ms: u64,
}

struct OpStats {
    period: Mutex<Histogram<u64>>,
    total: Mutex<Histogram<u64>>,
    success: AtomicU64,
    errors: AtomicU64,
}

impl OpStats {
    fn new() -> Self {
        Self {
            period: Mutex::new(Histogram::new(3).expect("hdr period")),
            total: Mutex::new(Histogram::new(3).expect("hdr total")),
            success: AtomicU64::new(0),
            errors: AtomicU64::new(0),
        }
    }

    fn record(&self, us: u64, ok: bool) {
        self.period.lock().unwrap().saturating_record(us);
        self.total.lock().unwrap().saturating_record(us);
        if ok {
            self.success.fetch_add(1, Ordering::Relaxed);
        } else {
            self.errors.fetch_add(1, Ordering::Relaxed);
        }
    }
}

async fn print_topology(seeds: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    let mut direct = None;
    let mut last_err: Option<Box<dyn std::error::Error>> = None;
    for seed in seeds {
        match Client::open(seed.as_str()) {
            Ok(c) => match c.get_multiplexed_async_connection().await {
                Ok(conn) => {
                    direct = Some(conn);
                    break;
                }
                Err(e) => last_err = Some(Box::new(e)),
            },
            Err(e) => last_err = Some(Box::new(e)),
        }
    }
    let mut direct = direct.ok_or_else(|| {
        last_err
            .map(|e| -> Box<dyn std::error::Error> { e })
            .unwrap_or_else(|| "no seeds configured".into())
    })?;

    let raw: Value = redis::cmd("CLUSTER")
        .arg("SLOTS")
        .query_async(&mut direct)
        .await?;

    struct ShardInfo {
        start: u16,
        end: u16,
        primary: (String, u16),
        replicas: Vec<(String, u16)>,
    }

    fn parse_node(v: &Value) -> Option<(String, u16)> {
        let arr = if let Value::Array(a) = v { a } else { return None };
        let ip = match arr.first()? {
            Value::BulkString(b) => String::from_utf8_lossy(b).into_owned(),
            Value::SimpleString(s) => s.clone(),
            _ => return None,
        };
        let port = match arr.get(1)? {
            Value::Int(i) => *i as u16,
            _ => return None,
        };
        Some((ip, port))
    }

    let slots_arr = match raw {
        Value::Array(a) => a,
        other => return Err(format!("unexpected CLUSTER SLOTS shape: {other:?}").into()),
    };
    let mut shards: Vec<ShardInfo> = Vec::new();
    for slot in &slots_arr {
        let arr = if let Value::Array(a) = slot {
            a
        } else {
            continue;
        };
        let start = if let Some(Value::Int(i)) = arr.first() {
            *i as u16
        } else {
            continue;
        };
        let end = if let Some(Value::Int(i)) = arr.get(1) {
            *i as u16
        } else {
            continue;
        };
        let primary = match arr.get(2).and_then(parse_node) {
            Some(p) => p,
            None => continue,
        };
        let replicas: Vec<_> = arr.iter().skip(3).filter_map(parse_node).collect();
        shards.push(ShardInfo {
            start,
            end,
            primary,
            replicas,
        });
    }
    shards.sort_by_key(|s| s.start);

    fn container_name(port: u16) -> String {
        if (7001..=7006).contains(&port) {
            format!("cluster-stall-valkey-{}", port - 7000)
        } else {
            "?".to_string()
        }
    }
    fn fmt_node(node: &(String, u16)) -> String {
        format!("{} ({}:{})", container_name(node.1), node.0, node.1)
    }

    println!("\ncluster topology (sorted by slot range):");
    println!(
        "  {:>3}  {:<12}  {:<40}  {:<40}  {}",
        "sh", "slots", "primary", "replica(s)", "test key (slot)"
    );
    for (idx, sh) in shards.iter().enumerate() {
        let primary = fmt_node(&sh.primary);
        let replicas = if sh.replicas.is_empty() {
            "(none)".to_string()
        } else {
            sh.replicas
                .iter()
                .map(fmt_node)
                .collect::<Vec<_>>()
                .join(", ")
        };
        let test_key = (0..NUM_SHARDS)
            .find(|&i| SHARD_SLOTS[i] >= sh.start && SHARD_SLOTS[i] <= sh.end)
            .map(|i| format!("{} (slot {})", SHARD_KEYS[i], SHARD_SLOTS[i]))
            .unwrap_or_else(|| "(no test key in this range)".to_string());
        println!(
            "  {:>3}  {:>5}-{:<6}  {:<40}  {:<40}  {}",
            idx, sh.start, sh.end, primary, replicas, test_key
        );
    }
    println!();
    Ok(())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let args = Args::parse();

    let seeds: Vec<String> = args.seeds.split(',').map(str::to_owned).collect();
    let client = ClusterClientBuilder::new(seeds.clone())
        .connection_timeout(Duration::from_millis(args.connection_timeout_ms))
        .response_timeout(Duration::from_millis(args.response_timeout_ms))
        .read_routing_strategy(RandomReplicaStrategy)
        .build()?;
    let conn = client.get_async_connection().await?;

    println!(
        "connected. seeds={:?} connection_timeout_ms={} response_timeout_ms={}",
        seeds, args.connection_timeout_ms, args.response_timeout_ms,
    );
    if let Err(e) = print_topology(&seeds).await {
        eprintln!("topology print failed: {e} (continuing)");
    }

    let stats: Vec<Vec<Arc<OpStats>>> = (0..NUM_SHARDS)
        .map(|_| vec![Arc::new(OpStats::new()), Arc::new(OpStats::new())])
        .collect();

    let start = Instant::now();

    fn snap_row(
        label: &str,
        shard: usize,
        t_s: f64,
        op: &OpStats,
        last_s: u64,
        last_e: u64,
    ) -> (u64, u64) {
        let s = op.success.load(Ordering::Relaxed);
        let e = op.errors.load(Ordering::Relaxed);
        let snap = {
            let mut h = op.period.lock().unwrap();
            let snap = h.clone();
            h.reset();
            snap
        };
        println!(
            "{:>4} {:>3} {:>6.1} {:>8} {:>8} {:>8} {:>9} {:>9} {:>9} {:>10}",
            label,
            shard,
            t_s,
            s - last_s,
            e - last_e,
            snap.len(),
            snap.value_at_quantile(0.5),
            snap.value_at_quantile(0.99),
            snap.value_at_quantile(0.999),
            snap.max(),
        );
        (s, e)
    }

    let reporter = {
        let stats = stats.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let _ = tick.tick().await; // immediate first tick
            let mut last: Vec<[(u64, u64); 2]> = vec![[(0, 0); 2]; NUM_SHARDS];
            println!(
                "{:>4} {:>3} {:>6} {:>8} {:>8} {:>8} {:>9} {:>9} {:>9} {:>10}",
                "op", "sh", "t_s", "ok", "err", "count", "p50_us", "p99_us", "p999_us", "max_us"
            );
            loop {
                tick.tick().await;
                let t_s = start.elapsed().as_secs_f64();
                for sh in 0..NUM_SHARDS {
                    let (s, e) = last[sh][0];
                    last[sh][0] = snap_row("SET", sh, t_s, &stats[sh][0], s, e);
                }
                for sh in 0..NUM_SHARDS {
                    let (s, e) = last[sh][1];
                    last[sh][1] = snap_row("GET", sh, t_s, &stats[sh][1], s, e);
                }
            }
        })
    };

    let mut tasks = Vec::with_capacity(NUM_SHARDS * 2);
    for shard in 0..NUM_SHARDS {
        for op_idx in 0..2 {
            let is_setter = op_idx == 0;
            let mut c = conn.clone();
            let op_stats = stats[shard][op_idx].clone();
            let key = SHARD_KEYS[shard];
            tasks.push(tokio::spawn(async move {
                let mut i: u64 = 0;
                loop {
                    let t = Instant::now();
                    let r: redis::RedisResult<redis::Value> = if is_setter {
                        redis::cmd("SET").arg(key).arg(i).query_async(&mut c).await
                    } else {
                        redis::cmd("GET").arg(key).query_async(&mut c).await
                    };
                    let us = t.elapsed().as_micros() as u64;
                    let ok = r.is_ok();
                    op_stats.record(us, ok);
                    if let Err(err) = r
                        && i.is_multiple_of(2000)
                    {
                        eprintln!(
                            "shard={shard} op={} i={i} err={err}",
                            if is_setter { "SET" } else { "GET" }
                        );
                    }
                    i += 1;
                }
            }));
        }
    }

    for t in tasks {
        let _ = t.await;
    }
    let _ = reporter.await;
    Ok(())
}
