//! Scenario 20 — **LogTail Adaptive Delivery**
//!
//! Proves the server's LogTail delivery path works end-to-end:
//! publish → WAL append → NotifyOffset → subscriber auto-Fetch → FetchReply.
//!
//! Spawns a server with `--wal-root` + `[delivery] force_strategy = "logtail"`
//! so every publish uses the LogTail path regardless of subscriber count.
//! The subscriber SDK transparently converts NotifyOffset frames into Fetch
//! requests and surfaces FetchReply payloads as regular Messages.
//!
//! ## What this verifies
//!
//! 1. All published messages arrive (no gaps, no duplicates).
//! 2. `client.notify_offset_count() > 0` — proves the server sent
//!    NotifyOffset frames (LogTail was used, not BatchPush).
//! 3. `client.fetch_reply_count() > 0` — proves the subscriber pulled
//!    payloads via Fetch/FetchReply.
//!
//! ## Run
//!
//! ```text
//! cargo run -p vireon-sdk --release --example s20_logtail_delivery
//! ```

#![allow(clippy::print_stdout)]

#[path = "_bench_common.rs"]
mod bench_common;

use std::time::Duration;

use bench_common::{
    ServerGuard, connect_ready, ephemeral_port, init_tracing, print_footer, print_header,
    write_dev_cert,
};

/// Number of messages to publish.
const PUBLISHES: u64 = 50;
const TOPIC: &str = "bench.logtail";
/// Drain budget for delivery.
const DRAIN: Duration = Duration::from_secs(10);

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();

    // ── Spawn server with WAL + forced LogTail ──────────────────────
    let (cert, key) = write_dev_cert().expect("cert");
    let port = ephemeral_port().expect("port");

    // Write a temp TOML config that forces LogTail strategy.
    let wal_dir = std::env::temp_dir().join(format!("vireon-s20-wal-{}", std::process::id()));
    let cfg_dir = std::env::temp_dir().join(format!("vireon-s20-cfg-{}", std::process::id()));
    std::fs::create_dir_all(&wal_dir).expect("mkdir wal");
    std::fs::create_dir_all(&cfg_dir).expect("mkdir cfg");
    let cfg_path = cfg_dir.join("s20.toml");
    std::fs::write(
        &cfg_path,
        format!("[delivery]\nforce_strategy = \"logtail\"\nlogtail_threshold = 1\n"),
    )
    .expect("write config");

    let mut server = ServerGuard::start_with(
        port,
        &cert,
        &key,
        &[
            "--workers",
            "1",
            "--config",
            cfg_path.to_str().expect("cfg path utf8"),
            "--wal-root",
            wal_dir.to_str().expect("wal path utf8"),
        ],
    )
    .expect("server spawn");

    // Verify the server didn't immediately exit (e.g. bad config).
    tokio::time::sleep(Duration::from_millis(1000)).await;
    if !server.is_alive() {
        eprintln!("[s20] server exited immediately — check config/wal paths");
        std::process::exit(1);
    }
    // Hold the server guard for the test duration.
    let _server = server;

    let addr = format!("127.0.0.1:{port}");

    print_header(
        "Scenario 20 — LogTail Adaptive Delivery",
        Duration::from_secs(0),
        &addr,
    );
    println!("  publishes: {PUBLISHES}");
    println!("  topic:     {TOPIC}");
    println!("  strategy:  forced logtail (WAL + NotifyOffset + Fetch)");
    println!();

    // ── Subscriber ──────────────────────────────────────────────────
    let sub_client = connect_ready(&addr).await;
    let mut sub = sub_client.subscribe(TOPIC).await.expect("subscribe");

    // Give the server a moment to register the subscription.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // ── Publisher ───────────────────────────────────────────────────
    let pub_client = connect_ready(&addr).await;
    let start = std::time::Instant::now();
    for n in 0..PUBLISHES {
        let payload = n.to_be_bytes();
        pub_client.publish(TOPIC, &payload).await.expect("publish");
    }
    println!("  published: {PUBLISHES} in {:?}", start.elapsed());

    // ── Drain + collect ─────────────────────────────────────────────
    let mut received: Vec<u64> = Vec::new();
    let mut dups: u64 = 0;
    let deadline = std::time::Instant::now() + DRAIN;
    loop {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        match tokio::time::timeout(remaining, sub.recv()).await {
            Ok(Some(msg)) => {
                if msg.payload.len() >= 8 {
                    let mut b = [0u8; 8];
                    b.copy_from_slice(&msg.payload[..8]);
                    let id = u64::from_be_bytes(b);
                    if received.contains(&id) {
                        dups += 1;
                    }
                    received.push(id);
                }
            }
            _ => break,
        }
    }

    received.sort_unstable();
    received.dedup();
    let gaps = PUBLISHES.saturating_sub(received.len() as u64);

    // ── LogTail diagnostics ─────────────────────────────────────────
    let notify_count = sub_client.notify_offset_count();
    let fetch_count = sub_client.fetch_reply_count();

    println!();
    println!(
        "  delivered:           {received_count}",
        received_count = received.len()
    );
    println!("  duplicates:          {dups}");
    println!("  gaps:                {gaps}");
    println!("  NotifyOffset frames: {notify_count}");
    println!("  FetchReply frames:   {fetch_count}");
    println!();

    let all_delivered = received.len() == PUBLISHES as usize && dups == 0 && gaps == 0;
    let logtail_used = notify_count > 0 && fetch_count > 0;

    if all_delivered && logtail_used {
        println!("  \u{2713} LOGTAIL DELIVERY VERIFIED");
        println!("    (all messages arrived via NotifyOffset \u{2192} Fetch \u{2192} FetchReply)");
    } else if all_delivered && !logtail_used {
        println!("  \u{26a0}  DELIVERY OK but LogTail not detected");
        println!("    (messages arrived but NotifyOffset count = {notify_count})");
    } else {
        println!("  \u{2717} VERIFICATION FAILED");
    }
    println!();
    print_footer();

    // Cleanup.
    let _ = sub_client.unsubscribe(TOPIC).await;
    std::fs::remove_dir_all(&wal_dir).ok();
    std::fs::remove_dir_all(&cfg_dir).ok();

    if !(all_delivered && logtail_used) {
        std::process::exit(1);
    }
    Ok(())
}
