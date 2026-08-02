//! Scenario 17 — **Multi-core Mode Comparison**
//!
//! Runs the same publish/subscribe workload twice — once against a
//! single-worker server (`--mode single --workers 1`), then against a
//! multi-worker server (`--mode multi --workers N`) — and reports
//! correctness + throughput for each. Acts as a regression check that
//! the SDK behaves correctly when the server is in multi-core mode
//! (cross-worker fan-out mesh, InterWorkerPublish broadcast, per-core
//! socket options are all dark under single-worker).
//!
//! ## Run
//!
//! ```text
//! # Default: run both trials, workers=min(num_cpus, 8)
//! cargo run -p vireon-sdk --release --example s17_multicore_modes
//!
//! # Skip the single-worker baseline (faster iteration on multi-core bugs):
//! VIREON_MULTICORE_SKIP_SINGLE=1 \
//!   cargo run -p vireon-sdk --release --example s17_multicore_modes
//!
//! # Force exactly 4 workers in the multi-core trial:
//! VIREON_MULTICORE_WORKERS=4 \
//!   cargo run -p vireon-sdk --release --example s17_multicore_modes
//! ```
//!
//! ## What you should see
//!
//! Both modes should report `received: 500/500 gaps: 0 dups: 0` —
//! verifying the SDK works correctly under both single-worker and
//! multi-worker server configurations.

#![allow(clippy::print_stdout)]

#[path = "_bench_common.rs"]
mod bench_common;

use std::time::{Duration, Instant};

use bench_common::{
    connect_ready, ephemeral_port, init_tracing, print_footer, print_header, write_dev_cert,
    ServerGuard,
};

/// Frames published per trial. 500 is enough to surface cross-worker
/// routing bugs (a fan-out miss shows up as a gap) without making the
/// example take >5 s on a loaded dev machine.
const PUBLISHES: u64 = 500;
/// Payload size in bytes — includes the 8-byte sequence id prefix; the
/// rest is zero-fill. 1 KiB is a representative pub/sub payload size.
const PAYLOAD: usize = 1024;
/// Drain budget per trial.
const DRAIN: Duration = Duration::from_secs(10);
const TOPIC: &str = "bench.multicore";

/// Trial outcome reported by [`run_trial`].
struct Trial {
    label: String,
    received: u64,
    gaps: u64,
    dups: u64,
    elapsed: Duration,
    ok: bool,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();

    let skip_single = std::env::var("VIREON_MULTICORE_SKIP_SINGLE").is_ok();
    let skip_multi = std::env::var("VIREON_MULTICORE_SKIP_MULTI").is_ok();
    let workers_override: Option<u32> = std::env::var("VIREON_MULTICORE_WORKERS")
        .ok()
        .and_then(|s| s.trim().parse().ok());

    // Default worker count: min(num_cpus, 8). Cap at 8 so dev machines
    // with 32 cores don't spawn 32 io_uring rings needlessly.
    let default_workers = std::thread::available_parallelism()
        .map(|n| n.get() as u32)
        .unwrap_or(4)
        .min(8);
    let multi_workers = workers_override.unwrap_or(default_workers).max(1);

    print_header(
        "Scenario 17 — Multi-core Mode Comparison",
        Duration::from_secs(0),
        "127.0.0.1:<ephemeral>",
    );
    println!("  payload:   {PAYLOAD}B   publishes: {PUBLISHES} per trial");
    println!();

    let mut trials: Vec<Trial> = Vec::new();

    if !skip_single {
        trials.push(
            run_trial("single", 1, /*mode_single=*/ true).await,
        );
    }
    if !skip_multi {
        trials.push(
            run_trial("multi", multi_workers, /*mode_single=*/ false).await,
        );
    }

    // ── Report ──────────────────────────────────────────────────────
    println!();
    for t in &trials {
        let msgs_per_s = (t.received as f64 / t.elapsed.as_secs_f64()).round() as u64;
        let mib_per_s =
            (t.received as f64 * PAYLOAD as f64) / t.elapsed.as_secs_f64() / 1024.0 / 1024.0;
        let status = if t.ok { "OK" } else { "FAIL" };
        println!(
            "  [{:<24}] received: {}/{PUBLISHES} gaps: {} dups: {}   {msgs_per_s} msgs/s  {mib_per_s:.1} MiB/s   [{status}]",
            t.label, t.received, t.gaps, t.dups,
        );
    }
    println!();

    let all_ok = trials.iter().all(|t| t.ok);
    if all_ok && !trials.is_empty() {
        if !skip_single && !skip_multi {
            println!("  \u{2713} BOTH MODES VERIFIED — SDK works in single and multi-core");
        } else {
            println!("  \u{2713} VERIFIED");
        }
    } else {
        println!("  \u{2717} VERIFICATION FAILED");
    }
    println!();
    print_footer();

    if !all_ok {
        std::process::exit(1);
    }
    Ok(())
}

/// Run one trial: spawn a server with the requested mode/workers, run
/// the fixed workload, verify integrity, return a [`Trial`].
async fn run_trial(mode: &str, workers: u32, mode_single: bool) -> Trial {
    let (cert, key) = write_dev_cert().expect("cert");
    let port = ephemeral_port().expect("port");

    // `ServerGuard::start_with` no longer hardcodes `--workers`; we
    // always pass `--mode` + `--workers` explicitly so both trials use
    // the same arg shape.
    let mode_str = if mode_single { "single" } else { "multi" };
    let workers_str = Box::leak(workers.to_string().into_boxed_str());
    let mode_str_leaked: &'static str = Box::leak(mode_str.to_string().into_boxed_str());
    let extra: Vec<&str> = vec![
        "--mode",
        mode_str_leaked,
        "--workers",
        workers_str,
    ];

    let guard = ServerGuard::start_with(port, &cert, &key, &extra)
        .unwrap_or_else(|e| panic!("spawn server ({mode}): {e}"));

    // Wait for the server to bind its QUIC listener + init workers.
    tokio::time::sleep(Duration::from_millis(1500)).await;

    let addr = format!("127.0.0.1:{port}");
    let sub_client = connect_ready(&addr).await;
    let mut sub = sub_client.subscribe(TOPIC).await.expect("subscribe");

    // Settle period: subscribe frame must be processed before publishing.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let pub_client = connect_ready(&addr).await;

    let label = format!("{mode}, workers={workers}");
    let start = Instant::now();

    // Build payload once per publish — the first 8 bytes carry the
    // sequence id; the rest is zero-fill.
    let mut buf = vec![0u8; PAYLOAD];
    for n in 0u64..PUBLISHES {
        buf[..8].copy_from_slice(&n.to_be_bytes());
        // Clone the buffer so the SDK owns its own copy. (publish takes
        // `impl Payload`; a &[u8] borrow would also work but we want to
        // re-use `buf` next iteration.)
        pub_client
            .publish(TOPIC, &buf[..])
            .await
            .expect("publish");
    }
    let elapsed = start.elapsed();

    // Drain and verify.
    let mut ids: Vec<u64> = Vec::with_capacity(PUBLISHES as usize);
    let mut dups: u64 = 0;
    let deadline = Instant::now() + DRAIN;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
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
    let received = ids.len() as u64;
    let ok = received == PUBLISHES && gaps == 0 && dups == 0;

    // Drop clients before tearing down the server so shutdown logs stay
    // deterministic.
    drop(sub);
    drop(pub_client);
    drop(sub_client);
    drop(guard);

    Trial {
        label,
        received,
        gaps,
        dups,
        elapsed,
        ok,
    }
}
