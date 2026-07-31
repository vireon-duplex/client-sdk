//! Scenario 12 — **Full-Limit Stress** (exhaustive SDK surface).
//!
//! Exercises every observable axis of the runtime in one run:
//!
//! 1. **All 4 delivery policies** (ReliableOrdered, ReliableUnordered,
//!    RealtimeDropOld, LatestOnly) on dedicated streams in parallel.
//! 2. **Both QoS levels** (AtMostOnce, AtLeastOnce) on the default channel.
//! 3. **Four payload sizes** (64 B, 1 KiB, 64 KiB, 1 MiB) to span the
//!    tiny-frame RTT regime and the throughput-bound regime.
//! 4. **Fan-out** — N subscribers per topic, measuring aggregate delivery.
//! 5. **Fire-and-forget path** (`try_publish`) for raw throughput.
//! 6. **Default-channel wildcard matching** (`"stress.*"`).
//!
//! ## Run
//!
//! ```text
//! cargo run -p vireon-sdk --release --example s12_full_limit
//! ```
//!
//! Override frame count / duration via env (optional):
//! ```text
//! S12_FRAMES=2000 S12_FANOUT=4 cargo run -p vireon-sdk --release --example s12_full_limit
//! ```

#![allow(clippy::print_stdout)]

#[path = "_bench_common.rs"]
mod bench_common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bench_common::{
    Histogram, connect_ready, fmt_ns, init_tracing, print_footer, print_header, resolve_server,
};
use vireon_sdk::{DeliveryPolicy, Qos, StreamHandle, StreamSpec};

// ── knobs (env-overridable) ─────────────────────────────────────────

/// Frames per (stream × payload-size) cell. Override with `S12_FRAMES`.
fn frames_per_cell() -> u64 {
    std::env::var("S12_FRAMES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(500)
}

/// Fan-out subscribers per topic. Override with `S12_FANOUT`.
fn fanout() -> usize {
    std::env::var("S12_FANOUT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3)
}

// Payload sizes used for the size-sweep section.
const SIZES: &[(usize, &str)] = &[
    (64, "64 B"),
    (1024, "1 KiB"),
    (65_536, "64 KiB"),
    (1_048_576, "1 MiB"),
];

/// Pick the tokio worker thread count — auto-tunes to
/// `available_parallelism / 2` clamped to `[2, 6]`, leaving room for the
/// server's per-core worker threads. Override with `S12_WORKERS=N`.
fn worker_threads() -> usize {
    if let Some(n) = std::env::var("S12_WORKERS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
    {
        return n;
    }
    let phys = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8);
    (phys / 2).clamp(2, 6)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads())
        .enable_all()
        .build()?
        .block_on(async move { run().await })
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();
    let (addr, _server) = resolve_server().await;
    let frames = frames_per_cell();
    let fanout_n = fanout();

    print_header(
        "Scenario 12 — Full-Limit Stress (exhaustive SDK surface)",
        Duration::from_secs(0),
        &addr,
    );
    println!("  frames/cell:   {frames}");
    println!("  fan-out:       {fanout_n} subscribers per topic");
    println!("  payload sizes: 64 B / 1 KiB / 64 KiB / 1 MiB");
    println!("  policies:      ReliableOrdered, ReliableUnordered, RealtimeDropOld, LatestOnly");
    println!("  QoS:           AtMostOnce, AtLeastOnce");
    println!();

    // Brief settle between sections so the previous Clients' close-drain
    // fully completes before the next wave of handshakes. Without this,
    // connect_ready occasionally times out on rapid reconnect.
    async fn settle() {
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // ── Section 1: 4 policies × default-channel publish ──────────────
    section_policies(&addr, frames).await?;
    settle().await;

    // ── Section 2: payload size sweep (throughput + p99) ──────────────
    section_size_sweep(&addr, frames).await?;
    settle().await;

    // ── Section 3: fan-out (1 publisher → N subscribers) ──────────────
    section_fanout(&addr, frames, fanout_n).await?;
    settle().await;

    // ── Section 4: QoS levels (AtMostOnce vs AtLeastOnce) ────────────
    section_qos(&addr).await?;
    settle().await;

    // ── Section 5: fire-and-forget throughput (try_publish) ───────────
    section_try_publish_throughput(&addr, frames).await?;

    print_footer();
    println!("\n  \u{2713} full-limit stress complete.\n");
    Ok(())
}

// ════════════════════════════════════════════════════════════════════
// Section 1 — All 4 delivery policies on dedicated streams
// ════════════════════════════════════════════════════════════════════

async fn section_policies(addr: &str, frames: u64) -> Result<(), Box<dyn std::error::Error>> {
    println!("─── Section 1: 4 delivery policies (dedicated streams) ────");
    let sub = connect_ready(addr).await;
    let pub_c = connect_ready(addr).await;

    // Each policy gets its own dedicated stream + topic.
    let policies: &[(&str, DeliveryPolicy)] = &[
        ("ReliableOrdered", DeliveryPolicy::ReliableOrdered),
        ("ReliableUnordered", DeliveryPolicy::ReliableUnordered),
        ("RealtimeDropOld", DeliveryPolicy::RealtimeDropOld),
        ("LatestOnly", DeliveryPolicy::LatestOnly),
    ];

    let mut sub_streams = Vec::with_capacity(policies.len());
    let mut pub_streams = Vec::with_capacity(policies.len());
    for (name, policy) in policies {
        let topic = format!("pol.{name}");
        let s = sub
            .open_stream(StreamSpec::new(*policy).with_topic(topic.clone()))
            .await
            .expect("open sub stream");
        let p = pub_c
            .open_stream(StreamSpec::new(*policy).with_topic(topic))
            .await
            .expect("open pub stream");
        println!(
            "  opened {name:<20} sub_id={} pub_id={}",
            s.stream_id(),
            p.stream_id()
        );
        sub_streams.push((name, s));
        pub_streams.push(p);
    }
    // Give the server a moment to register all subscriptions.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Spawn one collector per subscriber stream.
    let mut collectors = Vec::new();
    for (name, s) in sub_streams {
        collectors.push(tokio::spawn(async move {
            let h = collect(s).await;
            (*name, h)
        }));
    }

    // Publish concurrently across all 4 publisher streams.
    let mut pub_tasks = Vec::new();
    for (i, p) in pub_streams.into_iter().enumerate() {
        let topic = format!("pol.{}", policies[i].0);
        pub_tasks.push(tokio::spawn(publish_burst(p, topic, frames, 1024)));
    }
    for t in pub_tasks {
        let _ = t.await;
    }

    // Close to make recv() return None on subscribers.
    tokio::time::sleep(Duration::from_millis(500)).await;
    pub_c.close().await.ok();
    sub.close().await.ok();

    println!();
    println!(
        "  {:<22} {:>8}  {:>10}  {:>10}  {:>10}",
        "policy", "recv", "p50", "p99", "max"
    );
    println!("  {}", "-".repeat(64));
    for c in collectors {
        let (name, h) = c.await.expect("collector panic");
        // LatestOnly intentionally collapses a burst to the newest frame
        // still queued at dequeue time — the low count is correct, not loss.
        let note = if name == "LatestOnly" {
            "  (intentional: queue collapses to newest)"
        } else {
            ""
        };
        println!(
            "  {:<22} {:>8}  {:>10}  {:>10}  {:>10}{note}",
            name,
            h.count,
            fmt_ns(h.p50.unwrap_or(0)),
            fmt_ns(h.p99.unwrap_or(0)),
            fmt_ns(h.max.unwrap_or(0)),
        );
    }
    println!();
    Ok(())
}

// ════════════════════════════════════════════════════════════════════
// Section 2 — Payload size sweep (throughput dominates)
// ════════════════════════════════════════════════════════════════════

async fn section_size_sweep(addr: &str, frames: u64) -> Result<(), Box<dyn std::error::Error>> {
    println!("─── Section 2: payload size sweep (ReliableOrdered dedicated) ────");
    println!("  (1 MiB path stresses QUIC flow-control + the 2s graceful-drain");
    println!("   cap; partial delivery at this size is the known limit.)");
    // Cap large sizes: 1 MiB × 500 = 524 MB is far beyond the 2s drain budget.
    let per_size = |sz: usize| -> u64 {
        if sz >= 1_048_576 {
            (frames / 10).clamp(5, 20)
        } else if sz >= 65_536 {
            (frames / 2).max(50)
        } else {
            frames
        }
    };

    let mut rows = Vec::new();
    for &(sz, label) in SIZES {
        let n = per_size(sz);
        let sub = connect_ready(addr).await;
        let pub_c = connect_ready(addr).await;
        let topic = format!("size.{label}");
        let s = sub
            .open_stream(StreamSpec::new(DeliveryPolicy::ReliableOrdered).with_topic(topic.clone()))
            .await
            .expect("open sub");
        let p = pub_c
            .open_stream(StreamSpec::new(DeliveryPolicy::ReliableOrdered).with_topic(topic))
            .await
            .expect("open pub");
        tokio::time::sleep(Duration::from_millis(150)).await;

        let collector = tokio::spawn(collect_with_throughput(s, n, sz));

        let pub_start = Instant::now();
        publish_await(p, format!("size.{label}"), n, sz).await;
        let pub_elapsed = pub_start.elapsed();

        // Wait for collector to finish (recv all `n` frames OR stream end)
        // BEFORE closing — closing the publisher mid-flight drops pending
        // writes that haven't reached quiche stream_send yet.
        let res = tokio::time::timeout(Duration::from_secs(60), collector)
            .await
            .expect("collector timeout")
            .expect("collector panic");

        pub_c.close().await.ok();
        sub.close().await.ok();

        let total_bytes = (res.count as u64) * (sz as u64);
        let mib_s = (total_bytes as f64) / pub_elapsed.as_secs_f64() / (1024.0 * 1024.0);
        rows.push((label, res.count, n, mib_s, res.p50, res.p99));
    }

    println!();
    println!(
        "  {:<8} {:>8}/{:<8}  {:>12}  {:>10}  {:>10}",
        "size", "recv", "sent", "throughput", "p50", "p99",
    );
    println!("  {}", "-".repeat(64));
    for (label, count, sent, mib_s, p50, p99) in &rows {
        println!(
            "  {:<8} {:>8}/{:<8}  {:>12}  {:>10}  {:>10}",
            label,
            count,
            sent,
            format!("{mib_s:.2} MiB/s"),
            fmt_ns(p50.unwrap_or(0)),
            fmt_ns(p99.unwrap_or(0)),
        );
    }
    println!();
    Ok(())
}

// ════════════════════════════════════════════════════════════════════
// Section 3 — Fan-out (1 publisher → N subscribers)
// ════════════════════════════════════════════════════════════════════

async fn section_fanout(
    addr: &str,
    frames: u64,
    fanout_n: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("─── Section 3: fan-out (1 publisher → {fanout_n} subscribers) ────");
    let publisher = connect_ready(addr).await;

    // Keep subscriber Clients alive in a Vec so they don't drop prematurely.
    let mut sub_clients = Vec::with_capacity(fanout_n);
    let mut subs = Vec::with_capacity(fanout_n);
    for i in 0..fanout_n {
        let s = connect_ready(addr).await;
        let mut sub = s.subscribe("fanout.*").await.expect("subscribe");
        subs.push(tokio::spawn(async move {
            let mut count = 0u64;
            // Bound the wait so we don't hang if a delivery is lost.
            loop {
                match tokio::time::timeout(Duration::from_secs(2), sub.recv()).await {
                    Ok(Some(_m)) => count += 1,
                    _ => break,
                }
            }
            (i, count)
        }));
        sub_clients.push(s);
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    let started = Instant::now();
    for i in 0..frames {
        publisher
            .publish("fanout.topic", format!("payload-{i}").as_bytes())
            .await
            .expect("publish");
    }
    // Allow all deliveries to land.
    tokio::time::sleep(Duration::from_millis(500)).await;
    // Close publisher + all subscribers so recv() returns None.
    publisher.close().await.ok();
    for c in sub_clients.drain(..) {
        c.close().await.ok();
    }
    let elapsed = started.elapsed();

    let mut total = 0u64;
    println!();
    for s in subs {
        let (i, c) = s.await.expect("sub panic");
        println!("  subscriber {i}: {c} frames");
        total += c;
    }
    let expected = frames * (fanout_n as u64);
    println!();
    println!(
        "  fan-out delivery: {total}/{expected} ({:.1}%) in {:.2}s — {}/s aggregate",
        (total as f64 / expected as f64) * 100.0,
        elapsed.as_secs_f64(),
        (total as f64 / elapsed.as_secs_f64()).round() as u64,
    );
    println!();
    Ok(())
}

// ════════════════════════════════════════════════════════════════════
// Section 4 — QoS levels on default channel
// ════════════════════════════════════════════════════════════════════

async fn section_qos(addr: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!("─── Section 4: QoS levels (AtMostOnce vs AtLeastOnce) ────");
    let qos_list = [
        (Qos::AtMostOnce, "AtMostOnce"),
        (Qos::AtLeastOnce, "AtLeastOnce"),
    ];

    println!();
    println!("  {:<14} {:>8}  {:>8}", "qos", "sent", "recv");
    println!("  {}", "-".repeat(36));

    let n = 100u64;
    for (qos, name) in qos_list {
        let sub_client = connect_ready(addr).await;
        let pub_c = connect_ready(addr).await;
        let topic = format!("qos.{name}");
        let mut s = sub_client
            .subscribe_with_qos(&topic, qos)
            .await
            .expect("subscribe qos");
        tokio::time::sleep(Duration::from_millis(150)).await;

        for i in 0..n {
            pub_c
                .publish(&topic, format!("q-{i}").as_bytes())
                .await
                .expect("publish");
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
        pub_c.close().await.ok();
        sub_client.close().await.ok();

        let mut got = 0u64;
        while let Ok(Some(_m)) = tokio::time::timeout(Duration::from_millis(200), s.recv()).await {
            got += 1;
        }
        println!("  {:<14} {:>8}  {:>8}", name, n, got);
    }
    println!();
    Ok(())
}

// ════════════════════════════════════════════════════════════════════
// Section 5 — Fire-and-forget throughput (try_publish)
// ════════════════════════════════════════════════════════════════════

async fn section_try_publish_throughput(
    addr: &str,
    frames: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("─── Section 5: fire-and-forget throughput (try_publish, 1 KiB) ────");

    let sub_client = connect_ready(addr).await;
    let pub_c = Arc::new(connect_ready(addr).await);

    let mut s = sub_client.subscribe("ff.*").await.expect("subscribe");
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Publisher spins up 4 concurrent tasks blasting try_publish.
    let per_task = frames / 4;
    let total_target = per_task * 4;
    let counter = Arc::new(AtomicU64::new(0));

    let started = Instant::now();
    let mut tasks = Vec::new();
    for t in 0..4 {
        let pc = pub_c.clone();
        let c = counter.clone();
        tasks.push(tokio::spawn(async move {
            let payload = vec![0xA5u8; 1024];
            let topic = format!("ff.task{t}");
            for _ in 0..per_task {
                loop {
                    match pc.try_publish(&topic, payload.as_slice()) {
                        Ok(()) => {
                            c.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                        Err(_) => tokio::time::sleep(Duration::from_micros(100)).await,
                    }
                }
            }
        }));
    }
    for t in tasks {
        let _ = t.await;
    }
    let pub_elapsed = started.elapsed();
    let accepted = counter.load(Ordering::Relaxed);

    // Drain receiver side for a bit; the rest will be flushed by close().
    let mut got = 0u64;
    let drain_deadline = Instant::now() + Duration::from_secs(3);
    while got < total_target && Instant::now() < drain_deadline {
        match tokio::time::timeout(Duration::from_millis(200), s.recv()).await {
            Ok(Some(_)) => got += 1,
            _ => break,
        }
    }
    let total_elapsed = started.elapsed();

    pub_c.close().await.ok();
    sub_client.close().await.ok();

    println!();
    println!(
        "  accepted by try_publish: {accepted}/{total_target} in {:.3}s",
        pub_elapsed.as_secs_f64()
    );
    let rate_per_sec = (accepted as f64) / pub_elapsed.as_secs_f64();
    let mib_s = rate_per_sec * 1024.0 / (1024.0 * 1024.0);
    println!("  publish rate:            {rate_per_sec:.0} frames/s  ({mib_s:.1} MiB/s)",);
    println!(
        "  subscriber drained:      {got} frames in {:.2}s",
        total_elapsed.as_secs_f64()
    );
    println!();
    Ok(())
}

// ════════════════════════════════════════════════════════════════════
// Helpers
// ════════════════════════════════════════════════════════════════════

struct LatStats {
    count: u64,
    p50: Option<u64>,
    p99: Option<u64>,
    max: Option<u64>,
}

impl From<Histogram> for LatStats {
    fn from(h: Histogram) -> Self {
        Self {
            count: h.count() as u64,
            p50: h.percentile(50.0),
            p99: h.percentile(99.0),
            max: h.max(),
        }
    }
}

async fn collect(mut stream: StreamHandle) -> LatStats {
    let mut hist = Histogram::default();
    while let Some(msg) = stream.recv().await {
        record_latency(&msg, &mut hist);
    }
    hist.into()
}

async fn collect_with_throughput(mut stream: StreamHandle, expected: u64, size: usize) -> LatStats {
    let mut hist = Histogram::default();
    let mut count = 0u64;
    // Quiet period scales with payload size: a 1 MiB frame can take
    // hundreds of ms to assemble from QUIC packets under flow control.
    let quiet = if size >= 1_048_576 {
        Duration::from_secs(5)
    } else if size >= 65_536 {
        Duration::from_secs(3)
    } else {
        Duration::from_secs(1)
    };
    loop {
        match tokio::time::timeout(quiet, stream.recv()).await {
            Ok(Some(msg)) => {
                count += 1;
                record_latency(&msg, &mut hist);
                if count >= expected {
                    break;
                }
            }
            Ok(None) => break, // stream closed
            Err(_) => break,   // quiet period — assume all delivered
        }
    }
    let mut s: LatStats = hist.into();
    s.count = count;
    s
}

fn record_latency(msg: &vireon_sdk::Message, hist: &mut Histogram) {
    if msg.payload.len() >= 8 {
        let ts = u64::from_be_bytes([
            msg.payload[0],
            msg.payload[1],
            msg.payload[2],
            msg.payload[3],
            msg.payload[4],
            msg.payload[5],
            msg.payload[6],
            msg.payload[7],
        ]);
        let now = nanos();
        if now >= ts {
            hist.record(now - ts);
        }
    }
}

/// Publish `frames` frames of `size` bytes via `publish().await` (natural
/// backpressure) — used by the size sweep, where throughput matters more
/// than peak frame rate.
async fn publish_await(stream: StreamHandle, topic: String, frames: u64, size: usize) {
    let mut buf = vec![0u8; size];
    for seq in 0..frames {
        if buf.len() >= 16 {
            buf[8..16].copy_from_slice(&seq.to_be_bytes());
        }
        let ts = nanos();
        if buf.len() >= 8 {
            buf[0..8].copy_from_slice(&ts.to_be_bytes());
        }
        // publish().await naturally paces to the connection task's intake.
        if stream.publish(&topic, buf.as_slice()).await.is_err() {
            break;
        }
    }
}

/// Fire-and-forget variant using `try_publish` — used by Section 1
/// (policies comparison) and Section 5 (raw throughput).
async fn publish_burst(stream: StreamHandle, topic: String, frames: u64, size: usize) {
    let mut buf = vec![0u8; size];
    for seq in 0..frames {
        // First 8 bytes = timestamp (for latency). Next 8 = sequence.
        let len = buf.len().min(16);
        if len >= 16 {
            buf[8..16].copy_from_slice(&seq.to_be_bytes());
        }
        loop {
            let ts = nanos();
            if buf.len() >= 8 {
                buf[0..8].copy_from_slice(&ts.to_be_bytes());
            }
            match stream.try_publish(&topic, buf.as_slice()) {
                Ok(()) => break,
                Err(_) => tokio::time::sleep(Duration::from_micros(100)).await,
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
