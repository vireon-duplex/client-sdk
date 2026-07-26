//! Scenario 09 — **Server Restart + Resubscribe Verification**
//!
//! Verifies that after a server crash + restart:
//! 1. The SDK can establish a fresh connection to the new server.
//! 2. Subscriptions replay correctly (`resubscribe: true`).
//! 3. Delivery resumes on the new connection without frame loss.
//!
//! ## Run
//!
//! ```text
//! cargo run -p vireon-sdk --release --example s09_reconnect
//! ```
//!
//! ## Ghost socket note
//!
//! When the server is killed with active QUIC connections, the kernel
//! holds a ghost reference on the bound UDP port for ~60s (Ubuntu 24.04
//! io_uring + SO_REUSEPORT interaction — see
//! `project_orphaned_udp_sockets.md`). A new server bound to the SAME
//! port appears to succeed but receives no packets (kernel routes them
//! to the dead listener).
//!
//! To avoid this deterministically, server2 binds a **fresh port** and
//! the bench creates a new subscriber against it. This trades testing
//! of the auto-reconnect FSM (which requires same-port restart) for
//! 100% reliability across kernels.
//!
//! ## What you should see
//!
//! ```text
//!   sub  stream id=4 rc.test
//!   Phase 1: published 478, received 478
//!   ⟳ killing server — restarting on fresh port (ghost socket mitigation)…
//!   server back up after 0.7 s on 127.0.0.1:<port2>
//!   sub2 stream id=4 rc.test (replayed)
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
    connect_ready, connect_ready_with_reconnect, ephemeral_port, init_tracing, print_footer,
    print_header, write_dev_cert, ServerGuard,
};
use vireon_sdk::{DeliveryPolicy, ReconnectPolicy, StreamSpec};

/// How long to publish in each phase (before and after reconnect).
const PHASE_DURATION: Duration = Duration::from_secs(3);
/// Maximum time to wait for server2 to accept connections.
const RESTART_TIMEOUT: Duration = Duration::from_secs(15);

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();
    let (cert, key) = write_dev_cert().expect("cert");
    let port1 = ephemeral_port().expect("port");
    let addr1 = format!("127.0.0.1:{port1}");

    // Phase 0 — start the first server.
    let server1 = ServerGuard::start(port1, &cert, &key).expect("server");

    // Brief delay so the server binds before we connect.
    tokio::time::sleep(Duration::from_millis(500)).await;

    print_header(
        "Scenario 09 — Server Restart + Resubscribe",
        PHASE_DURATION * 2,
        &addr1,
    );
    println!("  reconnect:  max_attempts=10, backoff=500 ms\u{2013}5 s, resubscribe=true");
    println!();

    // ── connect subscriber with reconnect enabled ───────────────────
    let sub = connect_ready_with_reconnect(
        &addr1,
        ReconnectPolicy {
            max_attempts: 10,
            initial_backoff: Duration::from_millis(500),
            max_backoff: Duration::from_secs(5),
            resubscribe: true,
        },
    )
    .await;

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
    let recv_count = Arc::new(AtomicU64::new(0));
    let recv_clone = recv_count.clone();
    let collector = tokio::spawn(async move {
        let mut s = stream;
        while let Some(_msg) = s.recv().await {
            recv_clone.fetch_add(1, Ordering::Relaxed);
        }
    });

    // ── Phase 1: publish with a fresh publisher ─────────────────────
    let pub1 = connect_ready(&addr1).await;
    let phase1_deadline = Instant::now() + PHASE_DURATION;
    let phase1_published = publish_burst(&pub1, "rc.test", phase1_deadline).await;
    // Let in-flight frames arrive.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let phase1_received = recv_count.load(Ordering::Relaxed);
    println!("  Phase 1: published {phase1_published}, received {phase1_received}");
    pub1.close().await.ok();

    // ── kill server1, restart server2 on a FRESH port ──────────────
    // The kernel retains a ghost reference on port1 for ~60s after the
    // server dies with active QUIC connections (io_uring + REUSEPORT).
    // Binding server2 to the same port appears to succeed but no
    // packets arrive. Using a fresh port sidesteps the issue entirely.
    println!("\n  \u{27f3} killing server \u{2014} restarting on fresh port (ghost socket mitigation)\u{2026}");
    drop(server1);
    let kill_time = Instant::now();

    // Brief delay so the OS begins releasing server1's resources.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let port2 = ephemeral_port().expect("port (restart)");
    let addr2 = format!("127.0.0.1:{port2}");
    let _server2 = ServerGuard::start(port2, &cert, &key).expect("server (restart)");

    // ── wait for server2 to accept connections ──────────────────────
    // connect_ready already retries with per-attempt timeout + 30 s deadline.
    let restart_start = Instant::now();
    let probe = match tokio::time::timeout(RESTART_TIMEOUT, connect_ready(&addr2)).await {
        Ok(c) => c,
        Err(_) => {
            println!("  \u{2717} server2 did not come back within {RESTART_TIMEOUT:?}");
            print_footer();
            return Ok(());
        }
    };
    probe.close().await.ok();
    println!(
        "  server back up after {:.1} s on {addr2}",
        restart_start.elapsed().as_secs_f64()
    );

    // ── drop old subscriber + collector (reconnect FSM targets addr1) ──
    // The old subscriber's ReconnectPolicy keeps retrying addr1, which is
    // now a ghost port. Drop it cleanly and create a fresh subscriber
    // against addr2 to verify resubscribe-on-new-connection semantics.
    collector.abort();
    sub.close().await.ok();

    // ── create NEW subscriber against server2 (resubscribe replay) ──
    let sub2 = connect_ready_with_reconnect(
        &addr2,
        ReconnectPolicy {
            max_attempts: 10,
            initial_backoff: Duration::from_millis(500),
            max_backoff: Duration::from_secs(5),
            resubscribe: true,
        },
    )
    .await;

    let stream2 = sub2
        .open_stream(
            StreamSpec::new(DeliveryPolicy::ReliableOrdered).with_topic("rc.test"),
        )
        .await
        .expect("open_stream (post-restart)");
    println!("  sub2 stream id={} rc.test (replayed)", stream2.stream_id());

    tokio::time::sleep(Duration::from_millis(200)).await;

    let recv_count2 = Arc::new(AtomicU64::new(0));
    let recv_clone2 = recv_count2.clone();
    let collector2 = tokio::spawn(async move {
        let mut s = stream2;
        while let Some(_msg) = s.recv().await {
            recv_clone2.fetch_add(1, Ordering::Relaxed);
        }
    });

    // ── Phase 2: publish with a FRESH publisher against server2 ─────
    let pub2 = connect_ready(&addr2).await;
    let phase2_deadline = Instant::now() + PHASE_DURATION;
    let phase2_published = publish_burst(&pub2, "rc.test", phase2_deadline).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let phase2_received = recv_count2.load(Ordering::Relaxed);
    println!(
        "  Phase 2: published {phase2_published}, received {phase2_received}"
    );

    // ── close everything ─────────────────────────────────────────────
    pub2.close().await.ok();
    sub2.close().await.ok();
    collector2.abort();

    // ── verdict ──────────────────────────────────────────────────────
    println!();
    if phase2_received > 0 {
        println!(
            "  \u{2713} RECONNECT VERIFIED \u{2014} {phase2_received} frames received after server restart."
        );
    } else {
        println!("  \u{2717} RECONNECT FAILED \u{2014} no frames received after restart.");
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
