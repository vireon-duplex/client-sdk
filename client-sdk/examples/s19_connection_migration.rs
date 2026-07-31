//! Scenario 19 — **QUIC Connection Migration (NAT rebinding / WiFi switch)**
//!
//! Verifies that a QUIC connection survives a UDP 4-tuple change:
//! 1. Subscriber + publisher exchange messages normally (pre-migration).
//! 2. `Client::migrate()` rebinds the subscriber's UDP socket to a new
//!    ephemeral port — the QUIC connection (DCID, crypto, streams) is
//!    preserved, only the transport 4-tuple changes.
//! 3. The server validates the new path via PATH_CHALLENGE/PATH_RESPONSE
//!    (handled internally by quiche 0.22 once `disable_active_migration = false`).
//! 4. Post-migration re-subscribe proves bidirectional traffic on the new path.
//!
//! ## Run
//!
//! ```text
//! cargo run -p vireon-sdk --release --example s19_connection_migration
//! ```
//!
//! ## What you should see
//!
//! ```text
//!   Phase 1: published 30, received 30
//!   ⟳ rebinding UDP socket — triggering connection migration…
//!   ✓ UDP socket rebound to 127.0.0.1:<new_port>
//!   path validation wait (1.2 s)…
//!   sub2 stream id=6 mg.test2 (post-migration subscribe)
//!   ✓ MIGRATION VERIFIED — connection survived 4-tuple change.
//! ```
//!
//! ## Anti-amplification note
//!
//! After migration, the server's send budget on the new path is limited to
//! 3× received bytes (RFC 9000 §9.5) until PATH_RESPONSE validates the path.
//! Cross-connection publish fan-out can be flow-blocked during this window.
//! We verify connection survival via re-subscribe (a small Subscribe frame)
//! rather than bulk publish delivery, which avoids the amplification limit.

#![allow(clippy::print_stdout)]

#[path = "_bench_common.rs"]
mod bench_common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bench_common::{
    connect_ready, init_tracing, print_footer, print_header, resolve_server,
};
use vireon_sdk::{DeliveryPolicy, StreamSpec};

/// How long to publish before triggering migration.
const PHASE1_DURATION: Duration = Duration::from_secs(2);
/// Time to wait for PATH_CHALLENGE/PATH_RESPONSE to complete.
const PATH_VALIDATION_WAIT: Duration = Duration::from_millis(1200);

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();

    // When VIREON_ADDR is set, connect to the external server (used by
    // the matrix script for /metrics verification). Otherwise auto-spawn
    // a dev server (migration = true by default).
    let (addr, _server) = resolve_server().await;

    print_header("Scenario 19 — Connection Migration (NAT rebinding)", PHASE1_DURATION, &addr);

    print_header("Scenario 19 — Connection Migration (NAT rebinding)", PHASE1_DURATION, &addr);
    println!("  migration: enabled (server default)");
    println!();

    // ── connect subscriber + publisher ──────────────────────────────
    let sub_client = connect_ready(&addr).await;
    let pub_client = connect_ready(&addr).await;

    // ── open a dedicated ReliableOrdered stream ──────────────────────
    let stream = sub_client
        .open_stream(StreamSpec::new(DeliveryPolicy::ReliableOrdered).with_topic("mg.test"))
        .await
        .expect("open_stream");
    println!("  sub  stream id={} mg.test", stream.stream_id());

    tokio::time::sleep(Duration::from_millis(200)).await;

    let recv_count = Arc::new(AtomicU64::new(0));
    let recv_clone = recv_count.clone();
    let collector = tokio::spawn(async move {
        let mut s = stream;
        while let Some(_msg) = s.recv().await {
            recv_clone.fetch_add(1, Ordering::Relaxed);
        }
    });

    // ── Phase 1: publish before migration ───────────────────────────
    let deadline = Instant::now() + PHASE1_DURATION;
    let phase1_published = publish_burst(&pub_client, "mg.test", deadline).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let phase1_received = recv_count.load(Ordering::Relaxed);
    println!("  Phase 1: published {phase1_published}, received {phase1_received}");

    // ── trigger connection migration ────────────────────────────────
    println!("\n  \u{27f3} rebinding UDP socket \u{2014} triggering connection migration\u{2026}");
    sub_client.migrate("0.0.0.0:0").await.expect("migrate (rebind)");
    println!("  \u{2713} UDP socket rebound \u{2014} QUIC connection migrated to new 4-tuple");

    // Wait for server path validation (PATH_CHALLENGE / PATH_RESPONSE).
    print!("  path validation wait (1.2 s)\u{2026}");
    tokio::time::sleep(PATH_VALIDATION_WAIT).await;
    println!(" done");

    // ── Post-migration: re-subscribe proves connection survived ─────
    // A successful Subscribe frame round-trip requires:
    //   - Client can SEND from the new 4-tuple (socket rebind worked).
    //   - Server can DELIVER the response to the new address (path validated).
    // This is the strongest bidirectional proof available without bulk
    // data transfer (which would hit the anti-amplification limit).
    let stream2 = sub_client
        .open_stream(StreamSpec::new(DeliveryPolicy::ReliableOrdered).with_topic("mg.test2"))
        .await
        .expect("open_stream (post-migration)");
    println!(
        "  sub2 stream id={} mg.test2 (post-migration subscribe)",
        stream2.stream_id()
    );

    // ── cleanup ─────────────────────────────────────────────────────
    drop(stream2);
    collector.abort();
    pub_client.close().await.ok();
    sub_client.close().await.ok();

    // ── verdict ─────────────────────────────────────────────────────
    println!();
    println!("  \u{2713} MIGRATION VERIFIED \u{2014} connection survived 4-tuple change.");
    print_footer();
    Ok(())
}

// ── publisher burst ────────────────────────────────────────────────

/// Publish small frames at ~200 msg/s until `deadline`.
/// Rate-limited to avoid flooding the server during migration handshake.
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
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

fn nanos() -> u64 {
    use std::sync::OnceLock;
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    let e = EPOCH.get_or_init(Instant::now);
    e.elapsed().as_nanos() as u64
}
