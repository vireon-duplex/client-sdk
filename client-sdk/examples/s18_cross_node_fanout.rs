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
const PUBLISHES: u64 = 2000;
const TOPIC: &str = "cluster.fanout";
/// Payload size per frame (bytes).  First 8 bytes carry the sequence
/// ID for gap/duplicate detection; the rest is filler.
const PAYLOAD_LEN: usize = 8_192;
/// Wait for Subscribe frames to propagate to all nodes via the
/// inter-node UDP mesh before publishing. Must be long enough for:
///   1. Subscribe frame → server → local registry
///   2. Cluster hash ring to form (consistent-hash owner resolution)
///   3. Server → InterNodeMessage::Subscribe → owner node (UDP)
///   4. Owner node processes → RemoteSubscriberRegistry updated
/// Under powersave CPU governor (~3x slower), this needs more time.
/// The server also re-broadcasts subscriptions on each heartbeat tick
/// (1 s), so 2 s guarantees at least one re-broadcast fires.
const SUB_PROPAGATION: Duration = Duration::from_millis(2000);
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

    print_header(
        "Scenario 18 — Cross-node Fan-out (all nodes receive)",
        Duration::from_secs(0),
        "(ports assigned below)",
    );
    println!("  nodes:          3");
    println!("  mode:           {mode} (workers={workers})");
    println!("  replication:    {replication}");
    println!("  publishes:      {PUBLISHES}");
    println!("  payload:        {PAYLOAD_LEN} B/frame");
    println!("  topic:          {TOPIC}");
    println!("  subscribers:    3 (one per node)");
    println!();

    // ── Spawn 3-node cluster with retry ────────────────────────────
    // Ghost UDP sockets from prior tests (kernel 6.8 io_uring RECV)
    // can cause a node to fail binding its port. We detect this by
    // checking process liveness after a brief startup window and
    // retry with fresh ports if any node died.
    const MAX_START_RETRIES: u32 = 4;
    let mut g1: Option<ServerGuard> = None;
    let mut g2: Option<ServerGuard> = None;
    let mut g3: Option<ServerGuard> = None;
    let mut q1: u16 = 0;
    let mut q2: u16 = 0;
    let mut q3: u16 = 0;

    for attempt in 0..MAX_START_RETRIES {
        // Pick fresh ports each attempt to avoid ghost-socket conflicts.
        q1 = ephemeral_port().expect("quic port 1");
        q2 = ephemeral_port().expect("quic port 2");
        q3 = ephemeral_port().expect("quic port 3");
        let c1 = ephemeral_port().expect("cluster udp 1");
        let c2 = ephemeral_port().expect("cluster udp 2");
        let c3 = ephemeral_port().expect("cluster udp 3");
        let peers = format!("1=127.0.0.1:{c1},2=127.0.0.1:{c2},3=127.0.0.1:{c3}");

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

        let mut ng1 =
            ServerGuard::start_with(q1, &cert, &key, &node_args[0]).expect("spawn node 1");
        let mut ng2 =
            ServerGuard::start_with(q2, &cert, &key, &node_args[1]).expect("spawn node 2");
        let mut ng3 =
            ServerGuard::start_with(q3, &cert, &key, &node_args[2]).expect("spawn node 3");

        // Wait for servers to bind (or detect early exit from ghost sockets).
        tokio::time::sleep(Duration::from_millis(800)).await;

        let alive = ng1.is_alive() && ng2.is_alive() && ng3.is_alive();
        if alive {
            // Extra warmup for cluster mesh formation + first heartbeat.
            tokio::time::sleep(Duration::from_millis(1000)).await;
            println!(
                "[cluster] all 3 nodes started (attempt {}) — node1=127.0.0.1:{q1}  node2=127.0.0.1:{q2}  node3=127.0.0.1:{q3}",
                attempt + 1
            );
            g1 = Some(ng1);
            g2 = Some(ng2);
            g3 = Some(ng3);
            break;
        }

        // At least one died — kill all and retry with fresh ports.
        eprintln!(
            "[cluster] attempt {}: a node exited early (likely ghost-socket bind failure), retrying...",
            attempt + 1
        );
        drop(ng1);
        drop(ng2);
        drop(ng3);

        if attempt + 1 < MAX_START_RETRIES {
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }

    let mut g1 = g1.expect("cluster node 1 failed to start after retries");
    let mut g2 = g2.expect("cluster node 2 failed to start after retries");
    let mut g3 = g3.expect("cluster node 3 failed to start after retries");

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
    let mut buf = vec![0u8; PAYLOAD_LEN];
    for n in 0u64..PUBLISHES {
        buf[..8].copy_from_slice(&n.to_be_bytes());
        // Fill remainder with a pattern for debugging.
        if PAYLOAD_LEN > 8 {
            let pattern = (n as u8).wrapping_mul(0x5B);
            for b in &mut buf[8..] {
                *b = pattern;
            }
        }
        pub_client
            .publish(TOPIC, &buf[..])
            .await
            .expect("publish");
    }
    let pub_elapsed = start.elapsed();
    let pub_fps = PUBLISHES as f64 / pub_elapsed.as_secs_f64();
    let pub_mibs =
        (PUBLISHES as usize * PAYLOAD_LEN) as f64 / pub_elapsed.as_secs_f64() / (1024.0 * 1024.0);
    println!(
        "[publisher] published {PUBLISHES} frames in {pub_elapsed:?} — {pub_fps:.0} frames/s ({pub_mibs:.1} MiB/s)"
    );

    // ── Drain + verify each subscriber (concurrently for timing) ────
    let drain_start = std::time::Instant::now();
    let ((d1, g1c, dup1), (d2, g2c, dup2), (d3, g3c, dup3)) = tokio::join!(
        drain_verify(&mut sub1, "node 1"),
        drain_verify(&mut sub2, "node 2"),
        drain_verify(&mut sub3, "node 3"),
    );
    let delivery_elapsed = drain_start.elapsed();
    let total_delivered = d1 + d2 + d3;
    let del_fps = total_delivered as f64 / delivery_elapsed.as_secs_f64();
    let del_mibs = (total_delivered as usize * PAYLOAD_LEN) as f64
        / delivery_elapsed.as_secs_f64()
        / (1024.0 * 1024.0);
    println!(
        "[delivery]   {total_delivered} frames in {delivery_elapsed:?} — {del_fps:.0} frames/s ({del_mibs:.1} MiB/s aggregate across 3 nodes)"
    );

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
/// Exits early once `PUBLISHES` unique messages have been received
/// (avoids inflating delivery time with unnecessary drain-wait).
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
                    } else {
                        ids.push(id);
                    }
                }
                // Early-exit once we've received everything expected.
                if ids.len() as u64 >= PUBLISHES {
                    break;
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
