//! Scenario 18 — **Cross-node Fan-out: every node receives every message**
//!
//! Spawns a 3-node cluster on loopback. Subscribers on **all three**
//! nodes subscribe to the same topic. A publisher on **node 1**
//! publishes N messages. Each subscriber must receive **ALL N**
//! messages — no gaps, no duplicates.
//!
//! This is the definitive end-to-end test for cluster pub/sub fan-out
//! correctness: it proves that when a topic has subscribers spread
//! across multiple nodes, the owner node routes deliveries to every
//! subscriber regardless of which node they're on.
//!
//! ## Run
//!
//! ```text
//! # Default: single-worker nodes, replication=2
//! cargo run -p vireon-sdk --release --example s18_cross_node_fanout
//!
//! # Multi-core nodes:
//! VIREON_CLUSTER_MODE=multi VIREON_CLUSTER_WORKERS=2 \
//!   cargo run -p vireon-sdk --release --example s18_cross_node_fanout
//! ```

#![allow(clippy::print_stdout)]

#[path = "_bench_common.rs"]
mod bench_common;

use std::time::Duration;

use bench_common::{
    connect_ready, ephemeral_port, init_tracing, print_footer, print_header, write_dev_cert,
    ServerGuard,
};
use vireon_sdk::Subscription;

/// Number of publishes to fan out across the cluster.
const PUBLISHES: u64 = 200;
const TOPIC: &str = "cluster.fanout";
/// Wait for Subscribe frames to propagate to all nodes via the
/// inter-node UDP mesh before publishing.
const SUB_PROPAGATION: Duration = Duration::from_millis(500);
/// Drain window for subscribers to receive the tail of the burst.
const DRAIN: Duration = Duration::from_secs(10);

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();

    // ── Resolve env-var knobs ───────────────────────────────────────
    let mode = std::env::var("VIREON_CLUSTER_MODE").unwrap_or_else(|_| "single".into());
    let mode = if mode.trim().eq_ignore_ascii_case("multi") {
        "multi"
    } else {
        "single"
    };
    let workers: u32 = std::env::var("VIREON_CLUSTER_WORKERS")
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(1)
        .max(1);
    let replication: u8 = std::env::var("VIREON_CLUSTER_REPLICATION")
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(2)
        .clamp(1, 3);

    // ── Topology: 3 nodes on loopback ───────────────────────────────
    let (cert, key) = write_dev_cert().expect("cert");
    let q1 = ephemeral_port().expect("quic port 1");
    let q2 = ephemeral_port().expect("quic port 2");
    let q3 = ephemeral_port().expect("quic port 3");
    let c1 = ephemeral_port().expect("cluster udp 1");
    let c2 = ephemeral_port().expect("cluster udp 2");
    let c3 = ephemeral_port().expect("cluster udp 3");
    let peers = format!("1=127.0.0.1:{c1},2=127.0.0.1:{c2},3=127.0.0.1:{c3}");

    print_header(
        "Scenario 18 — Cross-node Fan-out (all nodes receive)",
        Duration::from_secs(0),
        &format!("node1=127.0.0.1:{q1}  node2=127.0.0.1:{q2}  node3=127.0.0.1:{q3}"),
    );
    println!("  nodes:          3");
    println!("  mode:           {mode} (workers={workers})");
    println!("  replication:    {replication}");
    println!("  publishes:      {PUBLISHES}");
    println!("  topic:          {TOPIC}");
    println!("  subscribers:    3 (one per node)");
    println!();

    // ── Build extra CLI args per node ───────────────────────────────
    let mut common: Vec<&str> = vec![
        "--cluster-peers",
        Box::leak(peers.clone().into_boxed_str()),
        "--cluster-replication-factor",
        Box::leak(replication.to_string().into_boxed_str()),
        "--workers",
        Box::leak(workers.to_string().into_boxed_str()),
    ];
    if mode == "multi" {
        common.push("--mode");
        common.push("multi");
    }

    let node_id_flag = "--cluster-node-id";
    let mut node_args: Vec<Vec<&str>> = Vec::new();
    for n in 1u32..=3 {
        let mut v: Vec<&str> = vec![node_id_flag, Box::leak(n.to_string().into_boxed_str())];
        v.extend(common.iter().copied());
        node_args.push(v);
    }

    // ── Spawn 3 server processes ────────────────────────────────────
    let g1 = ServerGuard::start_with(q1, &cert, &key, &node_args[0]).expect("spawn node 1");
    let g2 = ServerGuard::start_with(q2, &cert, &key, &node_args[1]).expect("spawn node 2");
    let g3 = ServerGuard::start_with(q3, &cert, &key, &node_args[2]).expect("spawn node 3");
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // ── Subscribers on all 3 nodes ──────────────────────────────────
    let sub1_client = connect_ready(&format!("127.0.0.1:{q1}")).await;
    let mut sub1 = sub1_client.subscribe(TOPIC).await.expect("subscribe node 1");
    let sub2_client = connect_ready(&format!("127.0.0.1:{q2}")).await;
    let mut sub2 = sub2_client.subscribe(TOPIC).await.expect("subscribe node 2");
    let sub3_client = connect_ready(&format!("127.0.0.1:{q3}")).await;
    let mut sub3 = sub3_client.subscribe(TOPIC).await.expect("subscribe node 3");
    println!("[subscribers] connected to nodes 1, 2, 3");

    // Wait for Subscribe frames to propagate to all nodes via the
    // inter-node UDP mesh before publishing.
    tokio::time::sleep(SUB_PROPAGATION).await;

    // ── Publisher on node 1 ─────────────────────────────────────────
    let pub_client = connect_ready(&format!("127.0.0.1:{q1}")).await;
    println!("[publisher] connected to node 1");

    let start = std::time::Instant::now();
    for n in 0u64..PUBLISHES {
        let payload = n.to_be_bytes();
        pub_client
            .publish(TOPIC, &payload)
            .await
            .expect("publish");
    }
    println!("[publisher] published {PUBLISHES} frames in {:?}", start.elapsed());

    // ── Drain + verify each subscriber ──────────────────────────────
    let (d1, g1c, dup1) = drain_verify(&mut sub1, "node 1").await;
    let (d2, g2c, dup2) = drain_verify(&mut sub2, "node 2").await;
    let (d3, g3c, dup3) = drain_verify(&mut sub3, "node 3").await;

    println!();
    println!("  node 1:  delivered {d1}  gaps {g1c}  dup {dup1}");
    println!("  node 2:  delivered {d2}  gaps {g2c}  dup {dup2}");
    println!("  node 3:  delivered {d3}  gaps {g3c}  dup {dup3}");

    let ok = d1 == PUBLISHES && g1c == 0 && dup1 == 0
        && d2 == PUBLISHES && g2c == 0 && dup2 == 0
        && d3 == PUBLISHES && g3c == 0 && dup3 == 0;

    println!();
    if ok {
        println!("  \u{2713} CROSS-NODE FAN-OUT VERIFIED — all 3 nodes received all {PUBLISHES} messages");
    } else {
        println!("  \u{2717} VERIFICATION FAILED");
    }
    println!();
    print_footer();

    // Explicit drop for deterministic log ordering.
    drop(sub1);
    drop(sub2);
    drop(sub3);
    drop(pub_client);
    drop(sub1_client);
    drop(sub2_client);
    drop(sub3_client);
    drop(g1);
    drop(g2);
    drop(g3);

    if !ok {
        std::process::exit(1);
    }
    Ok(())
}

/// Drain a subscriber and return `(delivered, gaps, duplicates)`.
async fn drain_verify(
    sub: &mut Subscription,
    _label: &str,
) -> (u64, u64, u64) {
    let mut ids: Vec<u64> = Vec::with_capacity(PUBLISHES as usize);
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
                    if ids.contains(&id) {
                        dups += 1;
                    }
                    ids.push(id);
                }
            }
            _ => break,
        }
    }
    ids.sort_unstable();
    ids.dedup();
    let gaps = PUBLISHES.saturating_sub(ids.len() as u64);
    let delivered = ids.len() as u64;
    (delivered, gaps, dups)
}
