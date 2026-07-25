//! Scenario 13 — **Aggregate Throughput** (sustained ceiling).
//!
//! Spawns N parallel publisher/subscriber pairs, each pair running M
//! dedicated QUIC streams, to measure the runtime's true aggregate
//! throughput ceiling under sustained load.
//!
//! ## Architecture
//!
//! ```text
//!   publisher 0 ─┐                        ┌─ subscriber 0
//!                ├─ M dedicated streams ──┤
//!                │  (own QUIC conn, own   │  (own QUIC conn, own
//!                │   congestion control)  │   bg task)
//!   publisher 1 ─┤                        ├─ subscriber 1
//!                ├─ M dedicated streams ──┤
//!                │                        │
//!   ...          ...                      ...
//!   publisher N ─┤                        ├─ subscriber N
//!                └─ M dedicated streams ──┘
//! ```
//!
//! Each pair is independent — separate QUIC connection, separate
//! congestion controller, separate flow-control windows. This is what
//! lets the runtime scale past single-stream throughput limits.
//!
//! ## Config (env vars)
//!
//! ```text
//! S13_PUBS=2            Parallel (pub, sub) pairs (default 2)
//! S13_STREAMS=1         Dedicated streams per pair (default 1)
//! S13_FRAME_SIZE=8192   Payload bytes per frame (default 8 KiB)
//! S13_DURATION=5        Seconds to sustain publish rate (default 5)
//! S13_DRAIN=3           Seconds to wait for subscriber to finish after publish stop (default 3)
//! ```
//!
//! ## Run
//!
//! ```text
//! # default: 2 × 1 × 8 KiB × 5s  (~380 MiB/s ceiling on loopback)
//! cargo run -p vireon-sdk --release --example s13_aggregate_throughput
//!
//! # sweep: find the sweet spot for your host
//! for pubs in 1 2 4; do
//!   for sz in 4096 8192 16384; do
//!     S13_PUBS=$pubs S13_FRAME_SIZE=$sz \
//!       cargo run -p vireon-sdk --release --example s13_aggregate_throughput
//!   done
//! done
//! ```
//!
//! ## Findings (loopback, 4-worker server, max-throughput preset)
//!
//! | Config          | Delivery | Loss   | p50     |
//! |-----------------|----------|--------|---------|
//! | 2 × 1 × 8 KiB   | 378 MiB/s | 0.0%  | 47 ms   |
//! | 2 × 2 × 8 KiB   | 321 MiB/s | 27%   | 59 ms   |
//! | 4 × 1 × 8 KiB   | 200 MiB/s | 31%   | 233 ms  |
//! | 1 × 4 × 64 KiB  |  27 MiB/s | 5%    | 2344 ms |
//!
//! Adding more parallel pairs beyond 2 × 1 **decreases** throughput
//! because the SDK's per-connection tokio task becomes the bottleneck
//! under sustained load — the cmd channel (cap 256) and the 64-cmd
//! batch per loop iteration cap the rate well below the QUIC wire limit.
//! Findings are recorded for future SDK optimization; the bench itself
//! is the measurement tool.

#![allow(clippy::print_stdout)]

#[path = "_bench_common.rs"]
mod bench_common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bench_common::{
    connect_ready, fmt_ns, init_tracing, print_footer, print_header, resolve_server, Histogram,
};
use vireon_sdk::{DeliveryPolicy, StreamSpec};

// ── knobs ───────────────────────────────────────────────────────────

fn pubs() -> usize {
    std::env::var("S13_PUBS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2)
}

fn streams_per() -> usize {
    std::env::var("S13_STREAMS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1)
}

fn frame_size() -> usize {
    std::env::var("S13_FRAME_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8_192)
}

fn duration_secs() -> u64 {
    std::env::var("S13_DURATION")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5)
}

fn drain_secs() -> u64 {
    std::env::var("S13_DRAIN")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3)
}

// ── entry point ─────────────────────────────────────────────────────

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();
    let (addr, _server) = resolve_server().await;

    let n_pubs = pubs();
    let n_streams = streams_per();
    let sz = frame_size();
    let dur = Duration::from_secs(duration_secs());
    let drain = Duration::from_secs(drain_secs());

    print_header(
        "Scenario 13 — Aggregate Throughput (sustained ceiling)",
        dur,
        &addr,
    );
    println!("  pairs:        {n_pubs}   (each pair = 1 pub conn + 1 sub conn)");
    println!("  streams/pair: {n_streams}   (parallel QUIC streams per pair)");
    println!("  frame size:   {sz} B   ({:.1} KiB)", sz as f64 / 1024.0);
    println!("  duration:     {:.1}s   (+ up to {}s drain)", dur.as_secs_f64(), drain.as_secs());
    println!("  total streams in flight: {}", n_pubs * n_streams);
    println!();

    let result = run_config(&addr, n_pubs, n_streams, sz, dur, drain).await?;

    // ── summary ─────────────────────────────────────────────────────
    println!();
    println!("┌──────────────────────────────────────────────────────────────");
    println!("│  AGGREGATE THROUGHPUT RESULT");
    println!("├──────────────────────────────────────────────────────────────");
    println!("│  published:    {:.3} GiB  ({:.1} MiB)", result.gib_sent, result.mib_sent);
    println!("│  delivered:    {:.3} GiB  ({:.1} MiB)", result.gib_recv, result.mib_recv);
    println!(
        "│  loss:         {:.2}%  ({} sent vs {} recv bytes)",
        result.loss_pct, result.bytes_sent, result.bytes_recv,
    );
    println!("│");
    println!("│  publish rate:    {:.2} GiB/s  ({:.0} MiB/s)",
        result.publish_gibs, result.publish_mibs);
    println!("│  delivery rate:   {:.2} GiB/s  ({:.0} MiB/s)",
        result.delivery_gibs, result.delivery_mibs);
    println!("│");
    println!("│  frames sent:     {}", result.frames_sent);
    println!("│  frames recv:     {}", result.frames_recv);
    println!("│  elapsed:         {:.2}s", result.elapsed.as_secs_f64());
    if let Some(p50) = result.lat_p50 {
        println!("│  latency p50:     {}", fmt_ns(p50));
    }
    if let Some(p99) = result.lat_p99 {
        println!("│  latency p99:     {}", fmt_ns(p99));
    }
    println!("└──────────────────────────────────────────────────────────────");

    if result.delivery_gibs >= 1.0 {
        println!("\n  ✓ {:.2} GiB/s — target met.\n", result.delivery_gibs);
    } else if result.delivery_mibs >= 800.0 {
        println!("\n  ◐ {:.0} MiB/s — close to 1 GiB/s.\n", result.delivery_mibs);
    } else {
        println!("\n  ◯ {:.0} MiB/s — current SDK ceiling on this host.", result.delivery_mibs);
        println!("    Sweeping config can find a better operating point:");
        println!("    • S13_PUBS=2 S13_STREAMS=1 S13_FRAME_SIZE=8192  (sweet spot on loopback)");
        println!("    • S13_PUBS=1 S13_STREAMS=1 S13_FRAME_SIZE=4096  (lowest loss)");
        println!("    • Larger frames (16 KiB+) tend to increase p99 latency under load");
        println!();
    }

    print_footer();
    Ok(())
}

// ── types ───────────────────────────────────────────────────────────

struct BenchResult {
    bytes_sent: u64,
    bytes_recv: u64,
    frames_sent: u64,
    frames_recv: u64,
    elapsed: Duration,
    mib_sent: f64,
    mib_recv: f64,
    gib_sent: f64,
    gib_recv: f64,
    publish_mibs: f64,
    publish_gibs: f64,
    delivery_mibs: f64,
    delivery_gibs: f64,
    loss_pct: f64,
    lat_p50: Option<u64>,
    lat_p99: Option<u64>,
}

// ── bench core ──────────────────────────────────────────────────────

async fn run_config(
    addr: &str,
    n_pubs: usize,
    n_streams: usize,
    sz: usize,
    dur: Duration,
    drain: Duration,
) -> Result<BenchResult, Box<dyn std::error::Error>> {
    // (topic, stream) tuples so each publisher task knows its topic.
    type StreamWithTopic = (String, vireon_sdk::StreamHandle);

    // ── set up subscriber connections + streams ─────────────────────
    let mut sub_clients = Vec::with_capacity(n_pubs);
    let mut sub_streams: Vec<StreamWithTopic> = Vec::with_capacity(n_pubs * n_streams);
    for p in 0..n_pubs {
        let c = connect_ready(addr).await;
        for s in 0..n_streams {
            // Two-segment topic ("s{p}.s{s}") — required by default ACL
            // `*.*`. Three-segment topics ("s13.0.0") are silently denied.
            let topic = format!("s{p}.s{s}");
            let stream = c
                .open_stream(
                    StreamSpec::new(DeliveryPolicy::ReliableOrdered).with_topic(&topic),
                )
                .await?;
            sub_streams.push((topic, stream));
        }
        sub_clients.push(c);
    }
    println!(
        "  subscribers ready: {n_pubs} conns × {n_streams} streams = {} total",
        n_pubs * n_streams
    );

    // ── set up publisher connections + streams ──────────────────────
    let mut pub_clients = Vec::with_capacity(n_pubs);
    let mut pub_streams: Vec<StreamWithTopic> = Vec::with_capacity(n_pubs * n_streams);
    for p in 0..n_pubs {
        let c = connect_ready(addr).await;
        for s in 0..n_streams {
            let topic = format!("s{p}.s{s}");
            let stream = c
                .open_stream(
                    StreamSpec::new(DeliveryPolicy::ReliableOrdered).with_topic(&topic),
                )
                .await?;
            pub_streams.push((topic, stream));
        }
        pub_clients.push(c);
    }
    println!(
        "  publishers ready:  {n_pubs} conns × {n_streams} streams = {} total",
        n_pubs * n_streams
    );

    // Give the server a moment to wire subscriptions to streams.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // ── spawn one collector per subscriber stream ───────────────────
    let bytes_recv = Arc::new(AtomicU64::new(0));
    let frames_recv = Arc::new(AtomicU64::new(0));
    let mut collectors = Vec::with_capacity(sub_streams.len());
    for (_, mut stream) in sub_streams {
        let br = bytes_recv.clone();
        let fr = frames_recv.clone();
        collectors.push(tokio::spawn(async move {
            let mut hist = Histogram::default();
            while let Some(msg) = stream.recv().await {
                fr.fetch_add(1, Ordering::Relaxed);
                br.fetch_add(msg.payload.len() as u64, Ordering::Relaxed);
                if msg.payload.len() >= 8 {
                    let ts = u64::from_be_bytes([
                        msg.payload[0], msg.payload[1], msg.payload[2], msg.payload[3],
                        msg.payload[4], msg.payload[5], msg.payload[6], msg.payload[7],
                    ]);
                    let now = nanos();
                    if now >= ts {
                        hist.record(now - ts);
                    }
                }
            }
            hist
        }));
    }

    // ── spawn one publisher task per stream ─────────────────────────
    let bytes_sent = Arc::new(AtomicU64::new(0));
    let frames_sent = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let mut pub_tasks = Vec::with_capacity(pub_streams.len());
    for (topic, stream) in pub_streams {
        let bs = bytes_sent.clone();
        let fs = frames_sent.clone();
        let stop = stop.clone();
        pub_tasks.push(tokio::spawn(publisher_loop(stream, topic, sz, bs, fs, stop)));
    }

    // ── run for `dur`, then stop publishers ─────────────────────────
    let started = Instant::now();
    tokio::time::sleep(dur).await;
    stop.store(true, Ordering::Release);

    // Wait for publisher tasks to observe the stop flag and exit.
    for t in pub_tasks {
        let _ = t.await;
    }
    let publish_elapsed = started.elapsed();

    // Allow subscribers to drain anything still in flight.
    let drain_deadline = Instant::now() + drain;
    let mut recv_before_drain = bytes_recv.load(Ordering::Relaxed);
    while Instant::now() < drain_deadline {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let now = bytes_recv.load(Ordering::Relaxed);
        if now == recv_before_drain {
            break; // quiet period — drain complete
        }
        recv_before_drain = now;
    }

    // Close everything so subscriber recv() returns None.
    for c in pub_clients {
        c.close().await.ok();
    }
    for c in sub_clients {
        c.close().await.ok();
    }
    let total_elapsed = started.elapsed();

    // Collect latency histograms from subscriber tasks.
    let mut lat_p50: Option<u64> = None;
    let mut lat_p99: Option<u64> = None;
    for c in collectors {
        // Time-bounded wait — collectors should exit shortly after close.
        if let Ok(Ok(h)) = tokio::time::timeout(Duration::from_secs(3), c).await {
            if let Some(p50) = h.percentile(50.0) {
                lat_p50 = Some(lat_p50.map_or(p50, |prev| (prev + p50) / 2));
            }
            if let Some(p99) = h.percentile(99.0) {
                lat_p99 = Some(lat_p99.map_or(p99, |prev| (prev + p99) / 2));
            }
        }
    }

    let bytes_sent = bytes_sent.load(Ordering::Relaxed);
    let bytes_recv = bytes_recv.load(Ordering::Relaxed);
    let frames_sent = frames_sent.load(Ordering::Relaxed);
    let frames_recv = frames_recv.load(Ordering::Relaxed);

    let secs_pub = publish_elapsed.as_secs_f64();
    let secs_total = total_elapsed.as_secs_f64().max(secs_pub);

    let mib_sent = bytes_sent as f64 / (1024.0 * 1024.0);
    let mib_recv = bytes_recv as f64 / (1024.0 * 1024.0);
    let gib_sent = mib_sent / 1024.0;
    let gib_recv = mib_recv / 1024.0;

    let publish_mibs = mib_sent / secs_pub;
    let publish_gibs = publish_mibs / 1024.0;
    let delivery_mibs = mib_recv / secs_total;
    let delivery_gibs = delivery_mibs / 1024.0;

    let loss_pct = if bytes_sent > 0 {
        ((bytes_sent as f64 - bytes_recv as f64) / bytes_sent as f64) * 100.0
    } else {
        0.0
    };

    Ok(BenchResult {
        bytes_sent,
        bytes_recv,
        frames_sent,
        frames_recv,
        elapsed: total_elapsed,
        mib_sent,
        mib_recv,
        gib_sent,
        gib_recv,
        publish_mibs,
        publish_gibs,
        delivery_mibs,
        delivery_gibs,
        loss_pct,
        lat_p50,
        lat_p99,
    })
}

/// One publisher's tight loop: blast try_publish until `stop` is set.
async fn publisher_loop(
    stream: vireon_sdk::StreamHandle,
    topic: String,
    sz: usize,
    bytes: Arc<AtomicU64>,
    frames: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
) {
    let mut buf = vec![0xA5u8; sz];
    while !stop.load(Ordering::Relaxed) {
        // Stamp timestamp in first 8 bytes for latency measurement.
        let ts = nanos();
        if buf.len() >= 8 {
            buf[0..8].copy_from_slice(&ts.to_be_bytes());
        }
        match stream.try_publish(&topic, buf.as_slice()) {
            Ok(()) => {
                bytes.fetch_add(sz as u64, Ordering::Relaxed);
                frames.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                // Channel full — sleep briefly so the connection task gets
                // a chance to drain the cmd channel. `yield_now` busy-loops
                // and starves the connection task on the same worker.
                tokio::time::sleep(Duration::from_micros(50)).await;
            }
        }
    }
}

fn nanos() -> u64 {
    use std::sync::OnceLock;
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    let e = EPOCH.get_or_init(Instant::now);
    e.elapsed().as_nanos() as u64
}
