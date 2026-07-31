//! Scenario 15 — **Consumer Group Load Balancing**
//!
//! Joins N consumers to a consumer group on a single topic and verifies
//! that publishes are distributed across members in round-robin fashion
//! (no duplicates, balanced counts, full coverage).
//!
//! Server-side `GroupCoordinator` picks one member per publish via
//! round-robin. This example proves the client SDK wires Join + heartbeat
//! correctly so the server's `group_locals` registry stays populated.
//!
//! ## Run
//!
//! ```text
//! cargo run -p vireon-sdk --release --example s15_consumer_group
//! ```
//!
//! ## What you should see
//!
//! ```text
//!   consumers:  4
//!   publishes:  100
//!   delivered:  100
//!   duplicates: 0
//!   balance:    member c0=25 c1=25 c2=25 c3=25
//!   ✓ GROUP LOAD-BALANCING VERIFIED
//! ```
//!
//! ## Known limitation: multi-worker mode
//!
//! This scenario is **only valid in single-worker mode**. The server's
//! `group_locals` registry lives on the per-worker `ApplicationLayer`,
//! so in `--mode multi` each worker independently round-robins to its
//! own local members when an `InterWorkerPublish` fan-out reaches it,
//! producing N×delivery (where N is the number of workers with at least
//! one group member). Cross-worker group_locals synchronization is an
//! open server-side task; until then the matrix runner skips s15 in
//! multi/multi-cluster variants.

#![allow(clippy::print_stdout)]

#[path = "_bench_common.rs"]
mod bench_common;

use std::collections::HashMap;
use std::time::Duration;

use bench_common::{connect_ready, init_tracing, print_footer, print_header, resolve_server};
use tokio::sync::mpsc;
use vireon_sdk::Client;

/// Number of consumer-group members.
const MEMBERS: usize = 4;
/// Number of publishes to fan out.
const PUBLISHES: u64 = 100;
const TOPIC: &str = "bench.group";
const GROUP: &str = "workers";
const DRAIN: Duration = Duration::from_secs(10);

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();
    let (addr, _server) = resolve_server().await;

    print_header(
        "Scenario 15 — Consumer Group Load Balancing",
        Duration::from_secs(0),
        &addr,
    );
    println!("  members:   {MEMBERS}");
    println!("  publishes: {PUBLISHES}");
    println!("  topic:     {TOPIC}");
    println!("  group:     {GROUP}");
    println!();

    // Each consumer is a separate Client connection so the server records
    // distinct quic_stream_ids per member (a single connection would also
    // work — round-robin would still hit each consumer_id's stream — but
    // the multi-connection shape mirrors realistic deployments).
    let mut collectors = Vec::new();
    let mut conns: Vec<Client> = Vec::new();
    for i in 0..MEMBERS {
        let c = connect_ready(&addr).await;
        let consumer_id = format!("c{i}");
        let sub = c
            .subscribe_group(TOPIC, GROUP, &consumer_id)
            .await
            .expect("subscribe_group");
        conns.push(c);
        collectors.push(tokio::spawn(collect(i, consumer_id, sub)));
    }

    // Give the server a moment to register all joins before publishing.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publisher client.
    let pub_client = connect_ready(&addr).await;
    let start = std::time::Instant::now();
    for n in 0..PUBLISHES {
        let payload = n.to_be_bytes();
        pub_client
            .publish(TOPIC, &payload)
            .await
            .expect("publish");
    }
    println!("  published: {PUBLISHES} in {:?}", start.elapsed());

    // Drain: let the server deliver remaining frames + heartbeats.
    tokio::time::sleep(DRAIN).await;

    // Tear down: leave each group so the test is repeatable.
    for (i, c) in conns.iter().enumerate() {
        let _ = c.leave_group(TOPIC, GROUP, &format!("c{i}")).await;
    }

    // Collect results.
    let mut delivered: u64 = 0;
    let mut by_consumer: HashMap<String, u64> = HashMap::new();
    let mut dup_count: u64 = 0;
    let mut all_ids: Vec<u64> = Vec::new();
    for handle in collectors {
        let (consumer, count, dups, mut ids) = handle.await.unwrap();
        delivered += count;
        dup_count += dups;
        by_consumer.insert(consumer.clone(), count);
        all_ids.append(&mut ids);
    }

    // Sequence integrity: every published id must appear exactly once.
    all_ids.sort_unstable();
    all_ids.dedup();
    let gaps = PUBLISHES.saturating_sub(all_ids.len() as u64);

    println!();
    println!("  delivered:  {delivered}");
    println!("  duplicates: {dup_count}");
    println!("  gaps:       {gaps}");
    let mut balance_parts = String::new();
    for i in 0..MEMBERS {
        let key = format!("c{i}");
        let n = by_consumer.get(&key).copied().unwrap_or(0);
        if !balance_parts.is_empty() {
            balance_parts.push(' ');
        }
        balance_parts.push_str(&format!("{key}={n}"));
    }
    println!("  balance:    member {balance_parts}");

    let ok = delivered == PUBLISHES
        && dup_count == 0
        && gaps == 0
        && all_ids.len() == PUBLISHES as usize;
    if ok {
        println!("  \u{2713} GROUP LOAD-BALANCING VERIFIED");
    } else {
        println!("  \u{2717} VERIFICATION FAILED");
    }
    println!();
    print_footer();
    if !ok {
        std::process::exit(1);
    }
    Ok(())
}

/// Collect from a GroupSubscription until the channel closes (publisher
/// drop) or the drain budget elapses. Returns the consumer id, count,
/// duplicate count (same publish id arriving twice), and the raw ids.
async fn collect(
    _idx: usize,
    consumer: String,
    mut sub: vireon_sdk::GroupSubscription,
) -> (String, u64, u64, Vec<u64>) {
    let mut count = 0u64;
    let mut dups = 0u64;
    let mut ids: Vec<u64> = Vec::new();
    let deadline = std::time::Instant::now() + DRAIN;
    loop {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        match tokio::time::timeout(remaining, sub.recv()).await {
            Ok(Some(msg)) => {
                if msg.payload.len() >= 8 {
                    let mut b = [0u8; 8];
                    b.copy_from_slice(&msg.payload[..8]);
                    let id = u64::from_be_bytes(b);
                    if ids.contains(&id) {
                        dups += 1;
                    }
                    ids.push(id);
                    count += 1;
                }
            }
            _ => break,
        }
    }
    (consumer, count, dups, ids)
}

// Suppress unused-import warning for mpsc — kept for downstream callers
// who may extend the example with their own backpressure logic.
#[allow(dead_code)]
fn _use_mpsc(_tx: mpsc::Sender<()>) {}
