//! Scenario 14 — **ClientPool** publish-side multiplex.
//!
//! Demonstrates using [`ClientPool`] to spread publishes across N
//! independent QUIC connections while receiving all deliveries on a
//! single subscriber. This removes the single-connection command-channel
//! bottleneck that caps a single [`Client`]'s `try_publish` rate.
//!
//! ## Architecture
//!
//! ```text
//!                ┌─ pool member 0 ──┐
//!   publisher ───┤─ pool member 1 ──┤── server ──┐
//!   (round-      └─ pool member 2 ──┘            │
//!    robin)                                      │
//!                                            fan-out
//!                                                │
//!                                       subscriber (single Client)
//! ```
//!
//! All publishes go through the pool; the subscriber uses a regular
//! `Client::subscribe` on a dedicated connection.
//!
//! ## Config (env vars)
//!
//! ```text
//! S14_POOL=4         Pool size (default 4)
//! S14_FRAMES=20000   Total frames to publish (default 20 000)
//! S14_FRAME_SIZE=2048  Bytes per frame (default 2 KiB)
//! ```
//!
//! ## Run
//!
//! ```text
//! cargo run -p vireon-sdk --release --example s14_pool_multiplex
//! ```

#[path = "_bench_common.rs"]
mod bench_common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bench_common::{connect_ready, fmt_ns, init_tracing, print_footer, print_header, resolve_server, Histogram};
use vireon_sdk::ClientPool;

fn pool_size() -> usize {
    std::env::var("S14_POOL")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4)
}

fn total_frames() -> u64 {
    std::env::var("S14_FRAMES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(20_000)
}

fn frame_size() -> usize {
    std::env::var("S14_FRAME_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2_048)
}

fn worker_threads() -> usize {
    if let Some(n) = std::env::var("S14_WORKERS")
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

    let pool_n = pool_size();
    let total = total_frames();
    let sz = frame_size();

    print_header(
        "Scenario 14 — ClientPool publish multiplex",
        Duration::from_secs(0),
        &addr,
    );
    println!("  pool size:    {pool_n}   (parallel publisher connections)");
    println!("  tokio workers: {}   (override: S14_WORKERS=N)", worker_threads());
    println!("  frames:       {total}");
    println!("  frame size:   {sz} B   ({:.1} KiB)", sz as f64 / 1024.0);
    println!("  total bytes:  {:.2} MiB", total as f64 * sz as f64 / (1024.0 * 1024.0));
    println!();

    // ── subscriber (single dedicated connection) ─────────────────────
    let sub_client = connect_ready(&addr).await;
    let topic = "pool.demo";
    let mut sub = sub_client.subscribe(topic).await?;

    // Give the server a moment to wire the subscription.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // ── publisher pool ───────────────────────────────────────────────
    // Connect members sequentially via connect_ready (robust 2 s per-attempt
    // retry loop) rather than ClientPool::connect's concurrent spawn. This
    // avoids handshake contention under startup load — each member gets the
    // server's undivided attention for its handshake.
    let mut members = Vec::with_capacity(pool_n);
    for _ in 0..pool_n {
        members.push(connect_ready(&addr).await);
    }
    let pool = ClientPool::from_clients(members);

    let payload = vec![0u8; sz];
    let sent = Arc::new(AtomicU64::new(0));
    let start = Instant::now();

    // Publish round-robin via the pool. try_publish fails over across
    // members so a single saturated member doesn't stall the producer.
    let publish_start = Instant::now();
    for _ in 0..total {
        loop {
            // Byte-based backpressure: yield when the pool's pending queue
            // is large. This keeps the in-flight gap small enough that
            // the subscriber can drain within a reasonable time.
            let pending = pool.pending_bytes() as u64;
            if pending > 32 * 1024 * 1024 {
                tokio::task::yield_now().await;
                continue;
            }
            if pool.try_publish(topic, payload.as_slice()).is_ok() {
                break;
            }
            // All members' cmd channels full — yield and retry.
            tokio::task::yield_now().await;
        }
        sent.fetch_add(1, Ordering::Relaxed);
    }
    let publish_elapsed = publish_start.elapsed();

    println!(
        "  published:    {total} frames in {:.2}s   ({:.0} frames/s)",
        publish_elapsed.as_secs_f64(),
        total as f64 / publish_elapsed.as_secs_f64(),
    );

    // ── drain subscriber ─────────────────────────────────────────────
    let expected = total;
    let mut hist = Histogram::default();
    let mut received: u64 = 0;
    let drain_deadline = Instant::now() + Duration::from_secs(30);

    while received < expected {
        let recv = tokio::time::timeout(Duration::from_secs(5), sub.recv()).await;
        match recv {
            Ok(Some(_msg)) => {
                hist.record(publish_elapsed.as_nanos() as u64);
                received += 1;
            }
            Ok(None) => break, // stream closed
            Err(_) => {
                if Instant::now() >= drain_deadline {
                    break;
                }
            }
        }
    }

    let elapsed = start.elapsed();
    let lost = expected.saturating_sub(received);
    let loss_pct = if expected == 0 {
        0.0
    } else {
        100.0 * lost as f64 / expected as f64
    };

    println!();
    println!("┌──────────────────────────────────────────────────────────────");
    println!("│  POOL MULTIPLEX RESULT");
    println!("├──────────────────────────────────────────────────────────────");
    println!("│  expected:    {expected}");
    println!("│  received:    {received}");
    println!("│  lost:        {lost}   ({loss_pct:.2}%)");
    println!("│  publish t:   {:.2}s", publish_elapsed.as_secs_f64());
    println!("│  total t:     {:.2}s   (incl. drain)", elapsed.as_secs_f64());
    if let Some(p50) = hist.percentile(50.0) {
        println!("│  e2e p50:     {}   (publish→recv)", fmt_ns(p50));
    }
    println!("│  throughput:  {:.2} MiB/s",
        received as f64 * sz as f64 / elapsed.as_secs_f64() / (1024.0 * 1024.0));
    println!("└──────────────────────────────────────────────────────────────");

    if loss_pct == 0.0 {
        println!("\n  ✓ 0% loss — pool round-robin delivered every frame.\n");
    } else {
        println!("\n  ◯ {loss_pct:.2}% loss — investigate pool failover.\n");
    }

    // Clean up.
    let _ = pool.close().await;
    let _ = sub_client.close().await;

    print_footer();
    Ok(())
}
