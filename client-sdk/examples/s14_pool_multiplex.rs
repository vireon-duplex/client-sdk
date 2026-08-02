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
//! S14_FRAMES=5000    Total frames to publish (default 5 000)
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

use bench_common::{
    connect_ready, init_tracing, print_footer, print_header, resolve_server,
};
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
        .unwrap_or(5_000)
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
    println!(
        "  tokio workers: {}   (override: S14_WORKERS=N)",
        worker_threads()
    );
    println!("  frames:       {total}");
    println!("  frame size:   {sz} B   ({:.1} KiB)", sz as f64 / 1024.0);
    println!(
        "  total bytes:  {:.2} MiB",
        total as f64 * sz as f64 / (1024.0 * 1024.0)
    );
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
    let received_arc = Arc::new(AtomicU64::new(0));
    let start = Instant::now();

    // ── concurrent drain task ────────────────────────────────────────
    // Spawn the recv loop BEFORE publishing so the subscriber drains
    // concurrently with the publish burst. Without this, a 50 K-frame
    // burst fills the subscriber channel (>65 K depth) and the background
    // task drops messages via try_send — causing 60%+ apparent "loss".
    // Concurrent drain lets the subscriber keep pace with fan-out.
    let recv_counter = received_arc.clone();
    let drain_start = Instant::now();
    let drain_task = tokio::spawn(async move {
        let mut local_received: u64 = 0;
        let mut last_recv = drain_start;
        loop {
            // 10 s idle timeout: once the publisher stops and the server
            // drains its queues, no more frames arrive.
            match tokio::time::timeout(Duration::from_secs(10), sub.recv()).await {
                Ok(Some(_msg)) => {
                    local_received += 1;
                    last_recv = Instant::now();
                    recv_counter.store(local_received, Ordering::Relaxed);
                }
                Ok(None) => break, // stream closed
                Err(_) => break,   // idle timeout → done
            }
        }
        (local_received, last_recv)
    });

    // Publish round-robin via the pool. try_publish fails over across
    // members so a single saturated member doesn't stall the producer.
    //
    // Backpressure: when the pool's pending-write queue exceeds the
    // threshold OR all cmd channels are full, we sleep briefly instead
    // of yield_now(). Under heavy load yield_now() barely yields to the
    // background I/O task — a 100 µs sleep gives it real CPU time to
    // drain command channels and flush quiche's send buffer.
    let publish_start = Instant::now();
    for _ in 0..total {
        loop {
            let pending = pool.pending_bytes() as u64;
            if pending > 8 * 1024 * 1024 {
                tokio::time::sleep(Duration::from_micros(100)).await;
                continue;
            }
            if pool.try_publish(topic, payload.as_slice()).is_ok() {
                break;
            }
            // All members' cmd channels full — sleep and retry.
            tokio::time::sleep(Duration::from_micros(100)).await;
        }
        sent.fetch_add(1, Ordering::Relaxed);
    }
    let publish_elapsed = publish_start.elapsed();

    println!(
        "  published:    {total} frames in {:.2}s   ({:.0} frames/s)",
        publish_elapsed.as_secs_f64(),
        total as f64 / publish_elapsed.as_secs_f64(),
    );

    // ── wait for drain task to finish ────────────────────────────────
    // The drain task exits after a 10 s idle period (no new frames).
    // This gives the server time to flush its fan-out queues after the
    // publisher stops.
    let (received, last_recv) = drain_task.await.unwrap_or((0, drain_start));
    let effective_elapsed = last_recv.saturating_duration_since(drain_start);

    let elapsed = start.elapsed();
    let expected = total;
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
    println!(
        "│  e2e t:       {:.2}s   (publish→last-recv, excl. idle wait)",
        effective_elapsed.as_secs_f64()
    );
    println!(
        "│  throughput:  {:.2} MiB/s   (e2e)",
        received as f64 * sz as f64 / effective_elapsed.as_secs_f64().max(0.001) / (1024.0 * 1024.0)
    );
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
