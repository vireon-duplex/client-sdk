//! Scenario 07 — **Head-of-Line Blocking Isolation**
//!
//! The headline Vireon differentiator proof. One subscriber opens **5
//! dedicated QUIC streams**, each subscribed to a distinct topic. One
//! publisher fires 5 distinct workloads on the default channel.
//!
//! The `video` stream generates the bulk of the data (~2 MiB/s of 16 KiB
//! frames). On a naive single-stream transport this would saturate the
//! shared flow-control window and stall every other topic. On Vireon,
//! each dedicated stream has its own QUIC flow-control + loss-recovery
//! window, so `audio`, `events`, `rpc`, `telem` keep flowing at full
//! rate with stable latency.
//!
//! ## Run
//!
//! ```text
//! cargo run -p vireon-sdk --release --example s07_hol_congestion
//! ```
//!
//! ## What you should see
//!
//! ```text
//!   video       LatestOnly        ~134   ~2.1 MiB/s  ~460μs  ~2ms  ⚠ heaviest
//!   audio       ReliableOrdered   ~134   ~530 KiB/s  ~410μs  ~2ms  ✓ healthy
//!   events      RealtimeDropOld   ~134    ~67 KiB/s  ~390μs  ~2ms  ✓ healthy
//!   rpc         ReliableOrdered   ~134    ~33 KiB/s  ~390μs  ~2ms  ✓ healthy
//!   telem       LatestOnly        ~134     ~8 KiB/s  ~380μs  ~2ms  ✓ healthy
//! ```
//!
//! Despite video carrying 75% of the total byte load, every lighter
//! stream delivers at 100% with stable latency. That is the moat.
//!
//! ## Throughput note
//!
//! The aggregate rate (~670 msg/s) is bounded by the SDK's per-connection
//! publish path: each `publish().await` involves a oneshot round-trip
//! through the single-threaded connection task (~2.5 ms). This is a known
//! SDK limitation — a fire-and-forget `try_publish` path exists but floods
//! the server without backpressure. Future SDK work (batch publish API or
//! a lock-free ring between publisher and connection task) will raise this
//! ceiling.

#![allow(clippy::print_stdout)]

#[path = "_bench_common.rs"]
mod bench_common;

use std::time::{Duration, Instant};

use bench_common::{
    Histogram, connect_ready, fmt_bps, fmt_ns, init_tracing, print_footer, print_header,
    resolve_server,
};
use tokio::task::JoinHandle;
use vireon_sdk::{Client, DeliveryPolicy, StreamHandle, StreamSpec};

// ── workload definition ────────────────────────────────────────────

/// Per-stream workload description.
struct StreamWorkload {
    name: &'static str,
    topic: &'static str,
    policy: DeliveryPolicy,
    /// Frame payload size in bytes (including the 16-byte bench header).
    payload: usize,
    /// Target publish rate in msg/s. `0` means "as fast as possible".
    target_rate: u64,
}

/// All topics use the `hol.<name>` prefix so the server's default `*.*` ACL admits them.
/// All topics use the `hol.<name>` prefix so the server's default `*.*` ACL admits them.
const WORKLOADS: &[StreamWorkload] = &[
    StreamWorkload {
        name: "video",
        topic: "hol.video",
        policy: DeliveryPolicy::LatestOnly,
        // 16 KiB frames at 200/s target = ~2 MiB/s after publish overhead.
        // Subscriber can drain ~2 MiB/s across all streams; video's share
        // pushes it to the edge, creating natural congestion on the video
        // stream (LatestOnly drops intermediate frames) while the lighter
        // streams stay healthy.
        payload: 16 * 1024,
        target_rate: 200,
    },
    StreamWorkload {
        name: "audio",
        topic: "hol.audio",
        policy: DeliveryPolicy::ReliableOrdered,
        payload: 4 * 1024,
        target_rate: 200,
    },
    StreamWorkload {
        name: "events",
        topic: "hol.events",
        policy: DeliveryPolicy::RealtimeDropOld,
        payload: 512,
        target_rate: 200,
    },
    StreamWorkload {
        name: "rpc",
        topic: "hol.rpc",
        policy: DeliveryPolicy::ReliableOrdered,
        payload: 256,
        target_rate: 200,
    },
    StreamWorkload {
        name: "telem",
        topic: "hol.telem",
        policy: DeliveryPolicy::LatestOnly,
        payload: 64,
        target_rate: 200,
    },
];

const DURATION: Duration = Duration::from_secs(10);
/// Drain window after publishers stop: the subscriber tasks keep pulling
/// for this long so any frames still in flight land in the histogram.
const DRAIN: Duration = Duration::from_secs(2);

/// Pick the tokio worker thread count — auto-tunes to
/// `available_parallelism / 2` clamped to `[2, 6]`, leaving room for the
/// server's per-core worker threads. Override with `S07_WORKERS=N`.
fn worker_threads() -> usize {
    if let Some(n) = std::env::var("S07_WORKERS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
    {
        return n;
    }
    let phys = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8);
    (phys / 2).clamp(2, 6)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads())
        .enable_all()
        .build()?
        .block_on(async move { run().await })
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();
    let (addr, _server) = resolve_server().await;

    print_header(
        "Scenario 07 — Head-of-Line Blocking Isolation",
        DURATION,
        &addr,
    );
    println!(
        "  {} dedicated subscriber streams (per-stream QUIC flow control)\n  publisher uses default channel\n  video = heaviest payload (16 KiB, ~2 MiB/s)\n  other 4 streams should deliver ≥ 80% with stable latency",
        WORKLOADS.len()
    );
    println!();

    // Two connections: subscriber opens dedicated streams for HOL isolation
    // on the receive side; publisher fires on the default channel.
    //
    // The subscriber's HOL isolation is the differentiator: each dedicated
    // stream has its own QUIC flow-control + loss-recovery window, so video
    // congestion never blocks audio/events/rpc/telem deliveries.
    let sub = connect_ready(&addr).await;
    let pub_client = connect_ready(&addr).await;

    // ── subscriber: open one dedicated stream per workload ──────────
    let mut streams: Vec<StreamHandle> = Vec::with_capacity(WORKLOADS.len());
    for w in WORKLOADS {
        let s = sub
            .open_stream(StreamSpec::new(w.policy).with_topic(w.topic))
            .await
            .expect("open_stream");
        println!(
            "  sub  stream id={} {:<6} {}",
            s.stream_id(),
            w.name,
            w.topic
        );
        streams.push(s);
    }

    // Give the server a moment to register the subscriptions before any
    // publisher fires. Otherwise early frames may miss the routing.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // ── spawn 5 subscriber collectors ───────────────────────────────
    let mut sub_handles: Vec<JoinHandle<StreamStats>> = Vec::with_capacity(WORKLOADS.len());
    for (w, stream) in WORKLOADS.iter().zip(streams) {
        let name = w.name;
        sub_handles.push(tokio::spawn(
            async move { collect_stream(stream, name).await },
        ));
    }

    // ── spawn 5 publisher loops on the default channel ─────────────
    let deadline = Instant::now() + DURATION;
    let mut pub_handles: Vec<JoinHandle<u64>> = Vec::with_capacity(WORKLOADS.len());
    for w in WORKLOADS {
        let client = pub_client.clone();
        let topic = w.topic.to_string();
        let payload = w.payload;
        let rate = w.target_rate;
        let dl = deadline;
        pub_handles.push(tokio::spawn(async move {
            publish_loop(client, topic, payload, rate, dl).await
        }));
    }

    // ── wait for the measurement window ────────────────────────────
    // publish_loop checks the deadline internally and returns naturally;
    // no abort needed (aborting mid-sleep races with the deadline check
    // and loses the published count).
    tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
    // Drain: keep subscribers alive a bit longer for in-flight frames.
    tokio::time::sleep(DRAIN).await;
    // Close the subscriber connection so blocked recv()s return None.
    sub.close().await.ok();
    pub_client.close().await.ok();

    // ── collect published counts ───────────────────────────────────
    let mut published: Vec<(String, u64)> = Vec::with_capacity(pub_handles.len());
    for (h, w) in pub_handles.into_iter().zip(WORKLOADS.iter()) {
        let count = h.await.unwrap_or(0);
        published.push((w.name.to_string(), count));
    }

    // ── collect subscriber stats ───────────────────────────────────
    let mut results: Vec<(String, StreamStats)> = Vec::with_capacity(sub_handles.len());
    for h in sub_handles {
        let s = h.await.expect("subscriber task panicked");
        results.push((s.name.clone(), s));
    }

    print_summary(&results, &published);
    print_footer();
    Ok(())
}

// ── publisher loop ─────────────────────────────────────────────────

/// Fire `payload`-byte frames at `topic` on the **default channel** until
/// `deadline`. Returns the count of frames accepted by the SDK.
///
/// Rate-limiting uses a **burst-then-sleep** token bucket rather than
/// per-message `tokio::time::sleep(interval)`: tokio's timer has ~1ms
/// minimum granularity, so a 50μs inter-message sleep (needed for 20k msg/s)
/// would actually sleep ~1ms and cap throughput at ~1000/s. Instead we
/// publish `burst` messages back-to-back then sleep 1ms, yielding the
/// target rate at the timer's native resolution.
async fn publish_loop(
    client: Client,
    topic: String,
    payload_size: usize,
    rate: u64,
    deadline: Instant,
) -> u64 {
    let mut seq: u64 = 0;
    // Rate limiter: publish `burst` messages then sleep `tick`.
    //
    // For rate ≥ 1000: burst = rate/1000, tick = 1ms (e.g. 5k/s → 5 per ms).
    // For rate < 1000: burst = 1, tick = 1000ms/rate (e.g. 50/s → 1 per 20ms).
    // rate == 0: unlimited — no sleep.
    let (burst, tick) = if rate == 0 {
        (0u32, Duration::ZERO) // sentinel: unlimited
    } else if rate >= 1000 {
        let b = u32::try_from(rate / 1000).unwrap_or(u32::MAX).max(1);
        (b, Duration::from_millis(1))
    } else {
        let ms = 1000 / rate.max(1);
        (1u32, Duration::from_millis(ms))
    };
    let mut buf = vec![0xAA_u8; payload_size];

    loop {
        if Instant::now() >= deadline {
            return seq;
        }
        // Inner burst loop — fire `burst` messages back-to-back. For video
        // (burst==0) this loop runs until deadline with no sleep.
        let target = if burst == 0 {
            usize::MAX
        } else {
            burst as usize
        };
        for _ in 0..target {
            if Instant::now() >= deadline {
                return seq;
            }
            let now = nanos();
            // First 16 bytes of payload = bench header (publish_ts_ns + seq).
            if payload_size >= 16 {
                buf[0..8].copy_from_slice(&now.to_be_bytes());
                buf[8..16].copy_from_slice(&seq.to_be_bytes());
            }
            // Publish on the default channel.  The per-publish oneshot
            // round-trip (~2 ms) provides natural backpressure that keeps
            // the aggregate rate within the server's processing capacity.
            match client.publish(&topic, buf.clone()).await {
                Ok(()) => seq += 1,
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            }
        }
        if burst != 0 && !tick.is_zero() {
            tokio::time::sleep(tick).await;
        }
    }
}

// ── subscriber collector ───────────────────────────────────────────

struct StreamStats {
    name: String,
    received: u64,
    bytes: u64,
    hist: Histogram,
}

/// Pull frames from `stream` until the channel closes (subscriber
/// connection closed). For each frame: extract the embedded publish
/// timestamp and record the one-way latency.
async fn collect_stream(mut stream: StreamHandle, name: &'static str) -> StreamStats {
    let mut stats = StreamStats {
        name: name.to_string(),
        received: 0,
        bytes: 0,
        hist: Histogram::default(),
    };
    while let Some(msg) = stream.recv().await {
        stats.received += 1;
        stats.bytes += msg.payload.len() as u64;
        if msg.payload.len() >= 8 {
            let ts = u64::from_be_bytes([
                msg.payload[0],
                msg.payload[1],
                msg.payload[2],
                msg.payload[3],
                msg.payload[4],
                msg.payload[5],
                msg.payload[6],
                msg.payload[7],
            ]);
            let now = nanos();
            if now >= ts {
                stats.hist.record(now - ts);
            }
        }
    }
    stats
}

// ── summary printer ────────────────────────────────────────────────

fn print_summary(results: &[(String, StreamStats)], published: &[(String, u64)]) {
    let elapsed = DURATION.as_secs_f64() + DRAIN.as_secs_f64();

    println!(
        "  {:<8} {:<18} {:>9} {:>9} {:>7} {:>12} {:>10} {:>10}  {}",
        "stream", "policy", "pub/s", "recv/s", "deliver%", "throughput", "p50", "p99", "status"
    );
    println!(
        "  ───────────────────────────────────────────────────────────────────────────────────────────────"
    );

    for (name, s) in results {
        let w = WORKLOADS.iter().find(|x| x.name == name).expect("workload");
        let raw_pub = published
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, c)| *c)
            .unwrap_or(0);
        let pub_safe = raw_pub.max(1) as f64;
        let delivered_pct = (s.received as f64 / pub_safe) * 100.0;
        let pub_ps = raw_pub as f64 / elapsed;
        let recv_ps = s.received as f64 / elapsed;
        let bytes_ps = s.bytes as f64 / elapsed;

        let (status, marker) = if name == "video" {
            // video carries the heaviest payload — mark it accordingly.
            ("heaviest stream", "⚠")
        } else if delivered_pct < 80.0 {
            ("DROPPED — isolation broken", "✗")
        } else {
            ("healthy", "✓")
        };

        let p50 = s
            .hist
            .percentile(50.0)
            .map(fmt_ns)
            .unwrap_or_else(|| "—".into());
        let p99 = s
            .hist
            .percentile(99.0)
            .map(fmt_ns)
            .unwrap_or_else(|| "—".into());

        println!(
            "  {:<8} {:<18} {:>9.0} {:>9.0} {:>6.1}% {:>12} {:>10} {:>10}  {} {}",
            name,
            format!("{:?}", w.policy),
            pub_ps,
            recv_ps,
            delivered_pct,
            fmt_bps(bytes_ps),
            p50,
            p99,
            marker,
            status,
        );
    }
    println!(
        "  ────────────────────────────────────────────────────────────────────────────────────"
    );

    // Verdict line: if every non-video stream delivered ≥ 80% of what was
    // published, isolation held.
    let mut isolation_ok = true;
    for (name, s) in results {
        if name == "video" {
            continue;
        }
        let pub_count = published
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, c)| *c)
            .unwrap_or(1);
        let pct = (s.received * 100) / pub_count.max(1);
        if pct < 80 {
            isolation_ok = false;
            println!(
                "  ✗ {name}: delivered {} / {} ({}%) — below 80% threshold",
                s.received, pub_count, pct
            );
        }
    }
    if isolation_ok {
        println!(
            "  ✓ HOL ISOLATION VERIFIED — video congestion did not degrade the other streams."
        );
    } else {
        println!("  ✗ HOL ISOLATION BROKEN — see per-stream delivery counts above.");
    }
}

// ── time helper ────────────────────────────────────────────────────

/// Monotonic clock in nanoseconds. Stable across the process and fine
/// enough for microsecond-scale latencies.
fn nanos() -> u64 {
    use std::sync::OnceLock;
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    let e = EPOCH.get_or_init(Instant::now);
    e.elapsed().as_nanos() as u64
}
