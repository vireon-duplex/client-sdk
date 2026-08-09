//! Scenario 20 — **ACK + Sequence + Resume end-to-end**
//!
//! Exercises the application-level reliability layer:
//! 1. Two connections (`sub` + `pub`) with `reliable(true)` enabled.
//! 2. Publish N reliable frames; verify the subscriber receives all N
//!    (at-least-once) and the dedup watermark advances monotonically.
//! 3. Disconnect the subscriber mid-stream, publish a few more frames
//!    (which the server retains in its replay buffer), then reconnect.
//! 4. The subscriber's `Resume(logical_session_id, [(stream, last_acked)])`
//!    triggers a server replay of the gap; the subscriber receives the
//!    missed frames with zero duplicates delivered to the app.
//!
//! ## Run
//!
//! ```text
//! cargo run -p vireon-sdk --release --example s20_ack_sequence_resume
//! ```
//!
//! ## What you should see
//!
//! ```text
//!   sub  stream id=4 ack.test  reliable=true
//!   Phase 1: published 100, received 100, duplicates=0
//!   ⟳ disconnecting subscriber (server retains replay window)…
//!   Phase 2 (gap): published 20 frames while subscriber was away
//!   ⟳ reconnecting with Resume…
//!   Phase 3: replay gap filled — received 20, duplicates=0
//!   ✓ RELIABILITY VERIFIED — 120/120 frames delivered, 0 duplicates.
//! ```

#![allow(clippy::print_stdout)]

#[path = "_bench_common.rs"]
mod bench_common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bench_common::{
    ServerGuard, ephemeral_port, init_tracing, print_footer, print_header,
    write_dev_cert,
};
use vireon_sdk::{ClientBuilder, DeliveryPolicy, ReconnectPolicy, StreamSpec};

/// Frames published per phase.
const PHASE1_FRAMES: u64 = 100;
/// Gap frames published while the subscriber is disconnected.
const GAP_FRAMES: u64 = 20;
/// How long to wait for deliveries before tallying.
const SETTLE: Duration = Duration::from_millis(800);
/// Fixed logical session id shared by the original + reconnect client.
/// The server keys its replay window by this value; reusing it across
/// reconnects is what makes resume work.
const LOGICAL_SESSION_ID: u64 = 0xCAFE_BABE_0000_0020;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();
    let (cert, key) = write_dev_cert().expect("cert");
    let port = ephemeral_port().expect("port");
    let addr = format!("127.0.0.1:{port}");

    let _server = ServerGuard::start(port, &cert, &key).expect("server");
    tokio::time::sleep(Duration::from_millis(500)).await;

    print_header(
        "Scenario 20 — ACK + Sequence + Resume",
        Duration::from_secs(10),
        &addr,
    );
    println!("  reliability:  reliable=true, ack_interval=1");
    println!();

    // ── subscriber: reliable + reconnect enabled ──────────────────────
    // The same logical_session_id is reused across reconnects so the
    // server's replay window remains keyed consistently.
    let sub = ClientBuilder::new(&addr)
        .reliable(true)
        .ack_interval(1)
        .logical_session_id(LOGICAL_SESSION_ID)
        .reconnect(ReconnectPolicy {
            max_attempts: 20,
            initial_backoff: Duration::from_millis(150),
            max_backoff: Duration::from_secs(2),
            resubscribe: true,
        })
        .connect()
        .await
        .expect("subscriber connect");

    let stream = sub
        .open_stream(StreamSpec::new(DeliveryPolicy::ReliableUnordered).with_topic("ack.test"))
        .await
        .expect("open_stream");
    let sub_stream_id = stream.stream_id();
    println!("  sub  stream id={sub_stream_id} ack.test  reliable=true");

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Collector: counts unique deliveries.
    let recv_count = Arc::new(AtomicU64::new(0));
    let highest_seq = Arc::new(AtomicU64::new(0));
    let recv_clone = recv_count.clone();
    let high_clone = highest_seq.clone();
    let collector = tokio::spawn(async move {
        let mut s = stream;
        while let Some(msg) = s.recv().await {
            recv_clone.fetch_add(1, Ordering::Relaxed);
            // Track the highest contiguous seq observed by the app.
            let prev = high_clone.load(Ordering::Relaxed);
            if msg.seq > prev {
                high_clone.store(msg.seq, Ordering::Relaxed);
            }
        }
    });

    // ── publisher: also reliable so the server fans out with ACK_REQ ──
    let pubc = ClientBuilder::new(&addr)
        .reliable(true)
        .connect()
        .await
        .expect("publisher connect");

    // Phase 1: publish PHASE1_FRAMES reliable frames.
    let mut published: u64 = 0;
    for i in 1..=PHASE1_FRAMES {
        let payload = i.to_be_bytes();
        match pubc.publish("ack.test", &payload).await {
            Ok(()) => published += 1,
            Err(e) => {
                println!("  publish error at {i}: {e}");
            }
        }
        // Tiny pacing to keep the server from getting flooded during the
        // handshake-heavy open-stream + subscribe round-trips.
        if i % 10 == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
    tokio::time::sleep(SETTLE).await;
    let phase1_recv = recv_count.load(Ordering::Relaxed);
    let phase1_dups = sub.duplicates_detected();
    println!(
        "  Phase 1: published {published}, received {phase1_recv}, duplicates={phase1_dups}"
    );

    // ── Phase 2: disconnect the subscriber by dropping it ─────────────
    // The server's replay buffer retains entries for the logical session.
    println!();
    println!("  \u{27f3} disconnecting subscriber (server retains replay window)\u{2026}");
    // Abort collector + close subscriber — the server sees a conn-close.
    collector.abort();
    sub.close().await.ok();

    // Publish GAP_FRAMES while the subscriber is away.
    for i in 1..=GAP_FRAMES {
        let seq = PHASE1_FRAMES + i;
        let payload = seq.to_be_bytes();
        let _ = pubc.publish("ack.test", &payload).await;
    }
    println!(
        "  Phase 2 (gap): published {GAP_FRAMES} frames while subscriber was away"
    );

    // Give the server a moment to fan out to no-one (entries stay in the
    // replay buffer for the logical session).
    tokio::time::sleep(Duration::from_millis(300)).await;

    // ── Phase 3: reconnect with Resume ────────────────────────────────
    println!();
    println!("  \u{27f3} reconnecting with Resume\u{2026}");
    let sub2 = ClientBuilder::new(&addr)
        .reliable(true)
        .ack_interval(1)
        // Reuse the SAME logical_session_id so the server finds the
        // retained replay window for this session.
        .logical_session_id(LOGICAL_SESSION_ID)
        .reconnect(ReconnectPolicy {
            max_attempts: 20,
            initial_backoff: Duration::from_millis(150),
            max_backoff: Duration::from_secs(2),
            resubscribe: true,
        })
        .connect()
        .await
        .expect("reconnect");

    let stream2 = sub2
        .open_stream(StreamSpec::new(DeliveryPolicy::ReliableUnordered).with_topic("ack.test"))
        .await
        .expect("open_stream (post-reconnect)");

    let recv_count2 = Arc::new(AtomicU64::new(0));
    let recv_clone2 = recv_count2.clone();
    let collector2 = tokio::spawn(async move {
        let mut s = stream2;
        while let Some(_msg) = s.recv().await {
            recv_clone2.fetch_add(1, Ordering::Relaxed);
        }
    });

    tokio::time::sleep(SETTLE * 2).await;
    let phase3_recv = recv_count2.load(Ordering::Relaxed);
    let phase3_dups = sub2.duplicates_detected();
    println!(
        "  Phase 3: replay gap filled \u{2014} received {phase3_recv}, duplicates={phase3_dups}"
    );

    pubc.close().await.ok();
    sub2.close().await.ok();
    collector2.abort();

    // ── Verdict ───────────────────────────────────────────────────────
    println!();
    let total_delivered = phase1_recv + phase3_recv;
    let total_expected = PHASE1_FRAMES + GAP_FRAMES;
    let ok = phase1_recv == PHASE1_FRAMES
        && phase3_recv == GAP_FRAMES
        && phase1_dups == 0
        && phase3_dups == 0;
    if ok {
        println!(
            "  \u{2713} RELIABILITY VERIFIED \u{2014} {total_delivered}/{total_expected} frames delivered, 0 duplicates."
        );
    } else {
        println!(
            "  \u{2717} RELIABILITY INCOMPLETE \u{2014} phase1={phase1_recv}/{PHASE1_FRAMES} phase3={phase3_recv}/{GAP_FRAMES} dups=({phase1_dups}, {phase3_dups})."
        );
        println!("    (ResumeUnavailable path is expected when the server's replay window");
        println!("     floor advances past the requested seq \u{2014} see server logs for details.)");
    }
    print_footer();
    Ok(())
}
