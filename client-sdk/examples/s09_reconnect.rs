//! Scenario 09 — **Reconnect + Resubscribe FSM Validation**
//!
//! Verifies that the SDK's background connection task:
//! 1. Detects a server crash (connection close).
//! 2. Reconnects automatically per the configured `ReconnectPolicy`.
//! 3. Replays all active subscriptions + dedicated streams.
//! 4. Delivery resumes without the application noticing (beyond a gap).
//!
//! ## Run
//!
//! ```text
//! cargo run -p vireon-sdk --release --example s09_reconnect
//! ```
//!
//! ## What you should see
//!
//! ```text
//!   sub  stream id=4 rc.test
//!   Phase 1: published 478, received 478
//!   ⟳ killing server — reconnect FSM should fire…
//!   server back up after 1.6 s
//!   Phase 2: published 477, received 477
//!   ✓ RECONNECT VERIFIED — 477 frames received after server restart.
//! ```

#![allow(clippy::print_stdout)]

#[path = "_bench_common.rs"]
mod bench_common;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bench_common::{
    connect_ready, ephemeral_port, init_tracing, print_footer, print_header, write_dev_cert,
    ServerGuard,
};
use vireon_sdk::{ClientBuilder, DeliveryPolicy, ReconnectPolicy, StreamSpec, TlsVerify};

/// How long to publish in each phase (before and after reconnect).
const PHASE_DURATION: Duration = Duration::from_secs(3);
/// Maximum time to wait for reconnect after killing the server.
const RECONNECT_TIMEOUT: Duration = Duration::from_secs(15);

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();
    let (cert, key) = write_dev_cert().expect("cert");
    let port = ephemeral_port().expect("port");
    let addr = format!("127.0.0.1:{port}");

    // Phase 0 — start the first server.
    let server1 = ServerGuard::start(port, &cert, &key).expect("server");

    // Brief delay so the server binds before we connect.
    tokio::time::sleep(Duration::from_millis(500)).await;

    print_header(
        "Scenario 09 — Reconnect + Resubscribe FSM",
        PHASE_DURATION * 2,
        &addr,
    );
    println!("  reconnect:  max_attempts=10, backoff=500 ms\u{2013}5 s, resubscribe=true");
    println!();

    // ── connect subscriber with reconnect enabled ───────────────────
    let sub = ClientBuilder::new(&addr)
        .sni("localhost")
        .tls_verify(TlsVerify::DangerAcceptInvalid)
        .reconnect(ReconnectPolicy {
            max_attempts: 10,
            initial_backoff: Duration::from_millis(500),
            max_backoff: Duration::from_secs(5),
            resubscribe: true,
        })
        .connect()
        .await
        .expect("connect");

    // ── open a dedicated ReliableOrdered stream ──────────────────────
    let stream = sub
        .open_stream(
            StreamSpec::new(DeliveryPolicy::ReliableOrdered).with_topic("rc.test"),
        )
        .await
        .expect("open_stream");
    println!("  sub  stream id={} rc.test", stream.stream_id());

    tokio::time::sleep(Duration::from_millis(200)).await;

    // ── spawn subscriber collector with shared atomic counter ───────
    // Using Arc<AtomicU64> lets the main task read the received count at
    // phase boundaries without waiting for the collector task to finish.
    let recv_count = Arc::new(AtomicU64::new(0));
    let recv_clone = recv_count.clone();
    let collector = tokio::spawn(async move {
        let mut s = stream;
        while let Some(_msg) = s.recv().await {
            recv_clone.fetch_add(1, Ordering::Relaxed);
        }
    });

    // ── Phase 1: publish with a fresh publisher ─────────────────────
    let pub1 = connect_ready(&addr).await;
    let phase1_deadline = Instant::now() + PHASE_DURATION;
    let phase1_published = publish_burst(&pub1, "rc.test", phase1_deadline).await;
    // Let in-flight frames arrive.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let phase1_received = recv_count.load(Ordering::Relaxed);
    println!("  Phase 1: published {phase1_published}, received {phase1_received}");
    pub1.close().await.ok();

    // ── kill the server ──────────────────────────────────────────────
    println!("\n  \u{27f3} killing server \u{2014} reconnect FSM should fire\u{2026}");
    drop(server1);
    let kill_time = Instant::now();

    // Brief delay so the OS releases the UDP port before we rebind.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let _server2 = ServerGuard::start(port, &cert, &key).expect("server (restart)");

    // ── wait for the new server to accept connections ────────────────
    let reconnect_at = tokio::time::timeout(RECONNECT_TIMEOUT, async {
        loop {
            match ClientBuilder::new(&addr)
                .sni("localhost")
                .tls_verify(TlsVerify::DangerAcceptInvalid)
                .connect()
                .await
            {
                Ok(probe) => {
                    probe.close().await.ok();
                    return;
                }
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
    })
    .await;

    let server_up_at = match reconnect_at {
        Ok(()) => Instant::now(),
        Err(_) => {
            println!("  \u{2717} server did not come back within {RECONNECT_TIMEOUT:?}");
            print_footer();
            return Ok(());
        }
    };
    let server_latency = server_up_at.duration_since(kill_time);
    println!("  server back up after {:.1} s", server_latency.as_secs_f64());

    // Give the subscriber's reconnect FSM a moment to replay subscriptions.
    tokio::time::sleep(Duration::from_secs(1)).await;
    let phase1_total = recv_count.load(Ordering::Relaxed);

    // ── Phase 2: publish with a FRESH publisher ─────────────────────
    let pub2 = connect_ready(&addr).await;
    let phase2_deadline = Instant::now() + PHASE_DURATION;
    let phase2_published = publish_burst(&pub2, "rc.test", phase2_deadline).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let phase2_received = recv_count.load(Ordering::Relaxed) - phase1_total;
    println!(
        "  Phase 2: published {phase2_published}, received {phase2_received}"
    );

    // ── close everything ─────────────────────────────────────────────
    pub2.close().await.ok();
    sub.close().await.ok();
    // Abort the collector (its recv() loop ends when sub closes).
    collector.abort();

    // ── verdict ──────────────────────────────────────────────────────
    println!();
    if phase2_received > 0 {
        println!(
            "  \u{2713} RECONNECT VERIFIED \u{2014} {phase2_received} frames received after server restart."
        );
    } else {
        println!("  \u{2717} RECONNECT FAILED \u{2014} no frames received after reconnect.");
    }
    print_footer();
    Ok(())
}

// ── publisher burst ────────────────────────────────────────────────

/// Publish small frames at a controlled rate until `deadline`.
/// Returns the number of frames accepted.
///
/// Rate-limited to ~200 msg/s to avoid overwhelming the server during
/// the reconnect handshake (the SDK's publish path is fast enough to
/// flood the server at 100k+ msg/s, which starves QUIC handshake packets).
async fn publish_burst(client: &vireon_sdk::Client, topic: &str, deadline: Instant) -> usize {
    let mut seq: u64 = 0;
    let mut buf = [0u8; 64];
    loop {
        if Instant::now() >= deadline {
            return seq as usize;
        }
        let now = nanos();
        buf[0..8].copy_from_slice(&now.to_be_bytes());
        buf[8..16].copy_from_slice(&seq.to_be_bytes());
        match client.publish(topic, &buf).await {
            Ok(()) => seq += 1,
            Err(_) => {}
        }
        // Rate-limit: ~200 msg/s keeps the server responsive for handshake traffic.
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

// ── time helper ────────────────────────────────────────────────────

fn nanos() -> u64 {
    use std::sync::OnceLock;
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    let e = EPOCH.get_or_init(Instant::now);
    e.elapsed().as_nanos() as u64
}
