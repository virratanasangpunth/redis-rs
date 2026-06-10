use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use clap::Parser;
use hdrhistogram::Histogram;
use redis::cluster::ClusterClientBuilder;
use redis::cluster_read_routing::RandomReplicaStrategy;
use redis::{Client, Value};

#[derive(Parser, Debug)]
#[command(name = "cluster-stall-repro")]
struct Args {
    /// Comma-separated seed URLs. Use rediss:// for TLS (e.g. ElastiCache with
    /// in-transit encryption; append #insecure to skip cert verification).
    #[arg(
        long,
        default_value = "redis://127.0.0.1:7001,redis://127.0.0.1:7002,redis://127.0.0.1:7003,redis://127.0.0.1:7004,redis://127.0.0.1:7005,redis://127.0.0.1:7006"
    )]
    seeds: String,

    /// Comma-separated keys to hammer (one SET+GET worker pair per key).
    /// Default: auto-derive one key per shard from CLUSTER SLOTS.
    #[arg(long)]
    keys: Option<String>,

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

fn crc16_xmodem(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &b in data {
        crc ^= (b as u16) << 8;
        for _ in 0..8 {
            crc = if crc & 0x8000 != 0 {
                (crc << 1) ^ 0x1021
            } else {
                crc << 1
            };
        }
    }
    crc
}

fn key_slot(key: &[u8]) -> u16 {
    let hashed = match key.iter().position(|&b| b == b'{') {
        Some(open) => match key[open + 1..].iter().position(|&b| b == b'}') {
            Some(close) if close > 0 => &key[open + 1..open + 1 + close],
            _ => key,
        },
        None => key,
    };
    crc16_xmodem(hashed) % 16384
}

struct ShardInfo {
    start: u16,
    end: u16,
    primary: (String, u16),
    replicas: Vec<(String, u16)>,
}

fn derive_keys(shards: &[ShardInfo]) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut keys: Vec<Option<String>> = (0..shards.len()).map(|_| None).collect();
    let mut filled = 0;
    for i in 0u64.. {
        if i > 1_000_000 {
            return Err("could not derive a key for every shard within 1M candidates".into());
        }
        let cand = format!("repro:{i}");
        let slot = key_slot(cand.as_bytes());
        if let Some(idx) = shards.iter().position(|s| slot >= s.start && slot <= s.end)
            && keys[idx].is_none()
        {
            keys[idx] = Some(cand);
            filled += 1;
            if filled == shards.len() {
                break;
            }
        }
    }
    Ok(keys.into_iter().map(|k| k.unwrap()).collect())
}

async fn fetch_shards(seeds: &[String]) -> Result<Vec<ShardInfo>, Box<dyn std::error::Error>> {
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

    fn parse_node(v: &Value) -> Option<(String, u16)> {
        let arr = if let Value::Array(a) = v {
            a
        } else {
            return None;
        };
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
    Ok(shards)
}

fn print_topology(shards: &[ShardInfo], keys: &[String]) {
    fn fmt_node(node: &(String, u16)) -> String {
        // local docker cluster from scripts/up.sh — show the container name too
        if (7001..=7006).contains(&node.1) {
            format!(
                "cluster-stall-valkey-{} ({}:{})",
                node.1 - 7000,
                node.0,
                node.1
            )
        } else {
            format!("{}:{}", node.0, node.1)
        }
    }

    println!("\ncluster topology (sorted by slot range):");
    println!(
        "  {:>3}  {:<12}  {:<40}  {:<40}  test key (slot)",
        "sh", "slots", "primary", "replica(s)"
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
        let test_key = keys
            .iter()
            .map(|k| (k, key_slot(k.as_bytes())))
            .find(|&(_, slot)| slot >= sh.start && slot <= sh.end)
            .map(|(k, slot)| format!("{k} (slot {slot})"))
            .unwrap_or_else(|| "(no test key in this range)".to_string());
        println!(
            "  {:>3}  {:>5}-{:<6}  {:<40}  {:<40}  {}",
            idx, sh.start, sh.end, primary, replicas, test_key
        );
    }
    println!();
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

    let shards = match fetch_shards(&seeds).await {
        Ok(s) => Some(s),
        Err(e) => {
            eprintln!("CLUSTER SLOTS fetch failed: {e}");
            None
        }
    };
    let keys: Vec<String> = match &args.keys {
        Some(s) => s.split(',').map(str::to_owned).collect(),
        None => derive_keys(
            shards
                .as_deref()
                .ok_or("cannot auto-derive keys without topology; pass --keys")?,
        )?,
    };
    if let Some(shards) = &shards {
        print_topology(shards, &keys);
    }
    let num_keys = keys.len();

    let stats: Vec<Vec<Arc<OpStats>>> = (0..num_keys)
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
            let mut last: Vec<[(u64, u64); 2]> = vec![[(0, 0); 2]; num_keys];
            println!(
                "{:>4} {:>3} {:>6} {:>8} {:>8} {:>8} {:>9} {:>9} {:>9} {:>10}",
                "op", "sh", "t_s", "ok", "err", "count", "p50_us", "p99_us", "p999_us", "max_us"
            );
            loop {
                tick.tick().await;
                let t_s = start.elapsed().as_secs_f64();
                for sh in 0..num_keys {
                    let (s, e) = last[sh][0];
                    last[sh][0] = snap_row("SET", sh, t_s, &stats[sh][0], s, e);
                }
                for sh in 0..num_keys {
                    let (s, e) = last[sh][1];
                    last[sh][1] = snap_row("GET", sh, t_s, &stats[sh][1], s, e);
                }
            }
        })
    };

    let mut tasks = Vec::with_capacity(num_keys * 2);
    for shard in 0..num_keys {
        for (op_idx, op_stats) in stats[shard].iter().enumerate() {
            let is_setter = op_idx == 0;
            let mut c = conn.clone();
            let op_stats = op_stats.clone();
            let key = keys[shard].clone();
            tasks.push(tokio::spawn(async move {
                let mut i: u64 = 0;
                loop {
                    let t = Instant::now();
                    let r: redis::RedisResult<redis::Value> = if is_setter {
                        redis::cmd("SET").arg(&key).arg(i).query_async(&mut c).await
                    } else {
                        redis::cmd("GET").arg(&key).query_async(&mut c).await
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
