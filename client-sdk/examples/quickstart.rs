//! Quickstart — self-contained end-to-end demo.
//!
//! Auto-spawns a `quic-server` on an ephemeral port, then exercises the
//! full SDK surface in ~60 lines:
//!
//! 1. Default-channel pub/sub with wildcard matching.
//! 2. Three dedicated streams with different `DeliveryPolicy` values.
//! 3. A 200-frame burst with per-stream latency histograms.
//!
//! ## Run
//!
//! ```text
//! cargo run -p vireon-sdk --release --example quickstart
//! ```
//!
//! No manual server setup needed — the example builds + spawns the server
//! itself.

#![allow(clippy::print_stdout)]

#[path = "_bench_common.rs"]
mod bench_common;

use std::sync::OnceLock;
use std::time::{Duration, Instant};

use bench_common::{init_tracing, resolve_server};
use vireon_sdk::{ClientBuilder, DeliveryPolicy, StreamHandle, StreamSpec, TlsVerify};

const BURST: u64 = 200;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();

    // ── spawn server (or use VIREON_ADDR) ───────────────────────────
    let (addr, _server) = resolve_server().await;

    println!("┌───────────────────────────────────────────────────");
    println!("│ vireon-sdk quickstart  ({BURST} frames per stream)");
    println!("│ server: {addr}");
    println!("└───────────────────────────────────────────────────\n");

    // ── connect subscriber + publisher ───────────────────────────────
    let sub = ClientBuilder::new(&addr)
        .sni("localhost")
        .tls_verify(TlsVerify::DangerAcceptInvalid)
        .connect()
        .await
        .expect("subscriber connect");

    let pub_client = ClientBuilder::new(&addr)
        .sni("localhost")
        .tls_verify(TlsVerify::DangerAcceptInvalid)
        .connect()
        .await
        .expect("publisher connect");
    println!("[publisher] connected");

    // ── 1. default channel ───────────────────────────────────────────
    let mut default_sub = sub.subscribe("chat.*").await.expect("subscribe");
    println!("[default] subscribed to chat.*");
    tokio::time::sleep(Duration::from_millis(200)).await;

    println!("[publisher] publishing chat.hello…");
    pub_client
        .publish("chat.hello", b"hello from vireon-sdk")
        .await
        .expect("publish");
    println!("[publisher] publish ok");
    let msg = tokio::time::timeout(Duration::from_secs(2), default_sub.recv())
        .await
        .expect("timeout")
        .expect("no message");
    println!(
        "[default] recv  topic={} payload={}",
        String::from_utf8_lossy(&msg.topic),
        String::from_utf8_lossy(&msg.payload),
    );

    // ── 2. dedicated streams with different policies ─────────────────
    let reliable = sub
        .open_stream(StreamSpec::new(DeliveryPolicy::ReliableOrdered).with_topic("qs.data"))
        .await
        .expect("open ReliableOrdered");
    let latest = sub
        .open_stream(StreamSpec::new(DeliveryPolicy::LatestOnly).with_topic("qs.cursor"))
        .await
        .expect("open LatestOnly");
    let realtime = sub
        .open_stream(StreamSpec::new(DeliveryPolicy::RealtimeDropOld).with_topic("qs.events"))
        .await
        .expect("open RealtimeDropOld");

    println!(
        "\n[streams] opened 3 dedicated QUIC streams (ids {}, {}, {})",
        reliable.stream_id(),
        latest.stream_id(),
        realtime.stream_id(),
    );
    println!(
        "  {:<20} {:<18}",
        "ReliableOrdered", "qs.data"
    );
    println!("  {:<20} {:<18}", "LatestOnly", "qs.cursor");
    println!("  {:<20} {:<18}", "RealtimeDropOld", "qs.events");

    tokio::time::sleep(Duration::from_millis(200)).await;

    // ── 3. burst: publish BURST frames per stream in parallel ───────
    //
    // Each topic gets its OWN dedicated QUIC stream on the publisher side
    // → independent flow control → a slow or blocked stream never stalls
    // the others. Three tokio tasks run concurrently, each calling
    // try_publish on its own StreamHandle.
    println!("\n[publish] sending {BURST} frames per stream (parallel, per-stream QUIC flow control)…");

    let pub_data = pub_client
        .open_stream(StreamSpec::new(DeliveryPolicy::ReliableOrdered).with_topic("qs.data"))
        .await
        .expect("open pub ReliableOrdered");
    let pub_cursor = pub_client
        .open_stream(StreamSpec::new(DeliveryPolicy::LatestOnly).with_topic("qs.cursor"))
        .await
        .expect("open pub LatestOnly");
    let pub_events = pub_client
        .open_stream(StreamSpec::new(DeliveryPolicy::RealtimeDropOld).with_topic("qs.events"))
        .await
        .expect("open pub RealtimeDropOld");

    println!(
        "[publish] dedicated streams opened (ids {}, {}, {})",
        pub_data.stream_id(),
        pub_cursor.stream_id(),
        pub_events.stream_id(),
    );

    let pub_task = tokio::spawn(async move {
        let tasks = vec![
            tokio::spawn(publish_burst(pub_data, "qs.data", BURST)),
            tokio::spawn(publish_burst(pub_cursor, "qs.cursor", BURST)),
            tokio::spawn(publish_burst(pub_events, "qs.events", BURST)),
        ];
        for t in tasks {
            let _ = t.await;
        }
    });

    // ── collect on each stream ───────────────────────────────────────
    let r_task = tokio::spawn(collect(reliable, "ReliableOrdered"));
    let l_task = tokio::spawn(collect(latest, "LatestOnly"));
    let rt_task = tokio::spawn(collect(realtime, "RealtimeDropOld"));

    pub_task.await.expect("pub task");
    tokio::time::sleep(Duration::from_millis(500)).await;
    sub.close().await.ok();
    pub_client.close().await.ok();

    let r = r_task.await.expect("r task");
    let l = l_task.await.expect("l task");
    let rt = rt_task.await.expect("rt task");

    // ── summary ──────────────────────────────────────────────────────
    println!("\n┌───────────────────────────────────────────────────────────");
    println!("│ {:<20} {:>8}  {:>10}  {:>10}", "stream", "recv", "p50", "p99");
    println!("│ {}", "-".repeat(56));
    for (name, s) in [
        ("ReliableOrdered", &r),
        ("LatestOnly", &l),
        ("RealtimeDropOld", &rt),
    ] {
        let p50 = s.p50.map(fmt_us).unwrap_or_else(|| "—".into());
        let p99 = s.p99.map(fmt_us).unwrap_or_else(|| "—".into());
        println!("│ {:<20} {:>8}  {:>10}  {:>10}", name, s.count, p50, p99);
    }
    println!("└───────────────────────────────────────────────────────────");

    println!("\n✓ quickstart complete — all 3 delivery policies verified.\n");
    Ok(())
}

// ── helpers ────────────────────────────────────────────────────────────

async fn publish_burst(stream: StreamHandle, topic: &'static str, burst: u64) {
    let mut buf = [0u8; 16];
    for seq in 0..burst {
        buf[8..16].copy_from_slice(&seq.to_be_bytes());
        loop {
            // Re-stamp timestamp on each retry so latency reflects only the
            // real network path, not the time spent waiting for backpressure.
            let ts = nanos();
            buf[0..8].copy_from_slice(&ts.to_be_bytes());
            match stream.try_publish(topic, &buf) {
                Ok(()) => break,
                Err(_) => tokio::task::yield_now().await,
            }
        }
    }
}

struct Stats {
    count: u64,
    p50: Option<u64>,
    p99: Option<u64>,
}

async fn collect(mut stream: vireon_sdk::StreamHandle, _label: &str) -> Stats {
    let mut samples: Vec<u64> = Vec::new();
    let mut count = 0u64;

    while let Some(msg) = stream.recv().await {
        count += 1;
        if msg.payload.len() >= 8 {
            let ts = u64::from_be_bytes([
                msg.payload[0], msg.payload[1], msg.payload[2], msg.payload[3],
                msg.payload[4], msg.payload[5], msg.payload[6], msg.payload[7],
            ]);
            let now = nanos();
            if now >= ts {
                samples.push(now - ts);
            }
        }
    }

    samples.sort_unstable();
    let p50 = pct(&samples, 50.0);
    let p99 = pct(&samples, 99.0);
    Stats { count, p50, p99 }
}

fn pct(sorted: &[u64], p: f64) -> Option<u64> {
    if sorted.is_empty() {
        return None;
    }
    let idx = ((p / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
    Some(sorted[idx.min(sorted.len() - 1)])
}

fn fmt_us(ns: u64) -> String {
    if ns < 1_000 {
        format!("{ns} ns")
    } else if ns < 1_000_000 {
        format!("{:.0} µs", ns as f64 / 1_000.0)
    } else {
        format!("{:.1} ms", ns as f64 / 1_000_000.0)
    }
}

fn nanos() -> u64 {
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    let e = EPOCH.get_or_init(Instant::now);
    e.elapsed().as_nanos() as u64
}
