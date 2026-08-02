//! Scenario 16 — **Cluster Replication & Cross-node Routing**
//!
//! Spawns a 3-node Vireon cluster on loopback, then proves end-to-end
//! cluster routing: a subscriber on **node 1** receives messages that a
//! publisher sent to **node 2**, with the cluster's consistent-hash ring
//! deciding per-topic ownership and `--cluster-replication-factor`
//! wiring replicas.
//!
//! This is the client-side demonstration of Task #116 (cluster
//! replication) — the same `--cluster-peers` string is handed to all
//! three server processes; each one filters it to find its own cluster
//! UDP bind address via `build_cluster_transport_config`.
//!
//! ## Run
//!
//! ```text
//! # Default: single-worker nodes, replication=2
//! cargo run -p vireon-sdk --release --example s16_cluster_replication
//!
//! # Multi-core nodes:
//! VIREON_CLUSTER_MODE=multi VIREON_CLUSTER_WORKERS=2 \
//!   cargo run -p vireon-sdk --release --example s16_cluster_replication
//!
//! # RF=3 (all nodes hold a replica copy of every topic):
//! VIREON_CLUSTER_REPLICATION=3 \
//!   cargo run -p vireon-sdk --release --example s16_cluster_replication
//! ```
//!
//! ## What you should see
//!
//! ```text
//!   nodes:          3
//!   mode:           multi (workers=2)
//!   replication:    2
//!   publishes:      100
//!   delivered:      100
//!   gaps:           0
//!   duplicates:     0
//!   ✓ CLUSTER ROUTING VERIFIED
//! ```

#![allow(clippy::print_stdout)]

#[path = "_bench_common.rs"]
mod bench_common;

use std::time::Duration;

use bench_common::{
    ServerGuard, connect_ready, ephemeral_port, init_tracing, print_footer, print_header,
    write_dev_cert,
};

/// Number of publishes to fan out across the cluster.
const PUBLISHES: u64 = 100;
const TOPIC: &str = "cluster.test";
/// How long to wait for the Subscribe frame to propagate to all nodes
/// via `InterNodeMessage::Subscribe` before publishing starts. 500 ms is
/// comfortable even on a loaded dev machine — the inter-node UDP mesh
/// propagates in <10 ms.
const SUB_PROPAGATION: Duration = Duration::from_millis(500);
/// Drain window for the subscriber to receive the tail of the burst.
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
    // Each node gets its own QUIC port (clients) and its own cluster UDP
    // port (inter-node). All three share the same `--cluster-peers`
    // string; each node's transport filters by its own node-id to find
    // its bind address.
    let (cert, key) = write_dev_cert().expect("cert");
    let q1 = ephemeral_port().expect("quic port 1");
    let q2 = ephemeral_port().expect("quic port 2");
    let q3 = ephemeral_port().expect("quic port 3");
    let c1 = ephemeral_port().expect("cluster udp 1");
    let c2 = ephemeral_port().expect("cluster udp 2");
    let c3 = ephemeral_port().expect("cluster udp 3");
    let peers = format!("1=127.0.0.1:{c1},2=127.0.0.1:{c2},3=127.0.0.1:{c3}");

    print_header(
        "Scenario 16 — Cluster Replication & Cross-node Routing",
        Duration::from_secs(0),
        &format!("node1=127.0.0.1:{q1}  node2=127.0.0.1:{q2}  node3=127.0.0.1:{q3}"),
    );
    println!("  nodes:          3");
    println!("  mode:           {mode} (workers={workers})");
    println!("  replication:    {replication}");
    println!("  publishes:      {PUBLISHES}");
    println!("  topic:          {TOPIC}");
    println!();

    // ── Build extra CLI args per node ───────────────────────────────
    // `ServerGuard::start_with` does NOT add `--workers` on its own, so
    // we pass it explicitly here for every node.
    let mut common: Vec<&str> = vec![
        "--cluster-peers",
        // Leak-extend with an owned String — lives for program duration.
        // SAFETY: `peers` is used as a CLI arg, never freed.
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
    // Build per-node arg slices (node-id differs).
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

    // Give all three nodes time to bind both their QUIC listener and
    // their cluster UDP socket before any client connects.
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // ── Subscriber on node 1 ────────────────────────────────────────
    let sub_client = connect_ready(&format!("127.0.0.1:{q1}")).await;
    let mut sub = sub_client.subscribe(TOPIC).await.expect("subscribe");
    println!("[subscriber] connected to node 1 (127.0.0.1:{q1})");

    // Wait for the Subscribe frame to propagate to other nodes via the
    // inter-node UDP mesh before publishing.
    tokio::time::sleep(SUB_PROPAGATION).await;

    // ── Publisher on node 2 ─────────────────────────────────────────
    let pub_client = connect_ready(&format!("127.0.0.1:{q2}")).await;
    println!("[publisher] connected to node 2 (127.0.0.1:{q2})");

    let start = std::time::Instant::now();
    for n in 0u64..PUBLISHES {
        let payload = n.to_be_bytes();
        pub_client.publish(TOPIC, &payload).await.expect("publish");
    }
    println!(
        "[publisher] published {PUBLISHES} frames in {:?}",
        start.elapsed()
    );

    // ── Drain + verify ──────────────────────────────────────────────
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

    println!();
    println!("  delivered:  {delivered}");
    println!("  gaps:       {gaps}");
    println!("  duplicates: {dups}");

    let ok = delivered == PUBLISHES && gaps == 0 && dups == 0;
    if ok {
        println!("  \u{2713} CLUSTER ROUTING VERIFIED");
    } else {
        println!("  \u{2717} VERIFICATION FAILED");
    }
    println!();
    print_footer();

    // Explicit drop before letting guards fall out of scope — makes the
    // log output deterministic for `cargo run` users.
    drop(sub);
    drop(pub_client);
    drop(sub_client);
    drop(g1);
    drop(g2);
    drop(g3);

    if !ok {
        std::process::exit(1);
    }
    Ok(())
}
