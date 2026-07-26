//! Shared helpers for the `sNN_*` benchmark scenarios under `examples/`.
//!
//! This file is NOT a standalone example binary — it is `#[path]`-included
//! by each scenario. Keeping it out of `[[example]]` in `Cargo.toml` means
//! cargo never tries to link it as a binary.
//!
//! Provides:
//!  - [`write_dev_cert`] — self-signed cert + key to temp files (idempotent).
//!  - [`ServerGuard`] — spawn the `quic-server` binary, kill on drop.
//!  - [`server_binary`] — locate + incrementally build the server binary.
//!  - [`ephemeral_port`] — pick a free UDP-ish port.
//!  - [`connect_ready`] — retry-connect until the server is up.
//!  - [`Histogram`] — tiny p50/p90/p99/p99.9/max latency aggregator.

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic, clippy::print_stdout)]

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use vireon_sdk::{ClientBuilder, ReconnectPolicy, TlsVerify};

// ── test cert generation ────────────────────────────────────────────

/// Write a self-signed cert + key to temp files; return their paths.
/// Idempotent per process: reuses the same files for all callers in this run.
pub fn write_dev_cert() -> std::io::Result<(PathBuf, PathBuf)> {
    let mut params =
        rcgen::CertificateParams::new(vec!["localhost".to_string()]).expect("rcgen params");
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "localhost");
    let key = rcgen::KeyPair::generate().expect("rcgen key");
    let cert = params.self_signed(&key).expect("rcgen self-signed");
    let cert_pem = cert.pem();
    let key_pem = key.serialize_pem();

    use std::sync::OnceLock;
    static CERT_PATH: OnceLock<PathBuf> = OnceLock::new();
    static KEY_PATH: OnceLock<PathBuf> = OnceLock::new();
    let id = std::process::id();
    let dir = std::env::temp_dir();
    let cert_path =
        CERT_PATH.get_or_init(|| dir.join(format!("vireon-bench-{id}-cert.pem")));
    let key_path =
        KEY_PATH.get_or_init(|| dir.join(format!("vireon-bench-{id}-key.pem")));
    std::fs::write(cert_path, cert_pem.as_bytes())?;
    std::fs::write(key_path, key_pem.as_bytes())?;
    Ok((cert_path.clone(), key_path.clone()))
}

// ── server lifecycle ────────────────────────────────────────────────

/// Locate the `quic-server` binary (incremental build, then search
/// target/<triple>/debug + target/debug).
pub fn server_binary() -> PathBuf {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let target_root = manifest.join("../target");
    // Build (incremental) so the binary is current.
    let _ = Command::new(env!("CARGO"))
        .args(["build", "-p", "quic-server", "--bin", "quic-server"])
        .status();

    let direct = target_root.join("debug/quic-server");
    if direct.exists() {
        return direct;
    }
    if let Ok(entries) = std::fs::read_dir(&target_root) {
        for e in entries.flatten() {
            let p = e.path().join("debug/quic-server");
            if p.exists() {
                return p;
            }
        }
    }
    panic!(
        "quic-server binary not found under {}; run `cargo build -p quic-server`",
        target_root.display()
    );
}

/// RAII guard: spawn a server, kill on drop.
pub struct ServerGuard {
    child: Option<Child>,
}

impl ServerGuard {
    /// Spawn with optional `--echo` / `--wal-root` flags. Piped stdout/stderr
    /// to keep test output clean.
    pub fn start(port: u16, cert: &Path, key: &Path) -> std::io::Result<Self> {
        Self::start_with(port, cert, key, &[])
    }

    /// Spawn with extra CLI args (e.g. `["--echo"]` or `["--wal-root", "/tmp/wal"]`).
    pub fn start_with(
        port: u16,
        cert: &Path,
        key: &Path,
        extra_args: &[&str],
    ) -> std::io::Result<Self> {
        let bin = server_binary();
        let mut cmd = Command::new(&bin);
        cmd.args([
            "--port",
            &port.to_string(),
            "--bind",
            "127.0.0.1",
            "--workers",
            "1",
            "--cert",
            cert.to_str().expect("cert path utf8"),
            "--key",
            key.to_str().expect("key path utf8"),
        ])
        .args(extra_args.iter().copied())
        .env("RUST_LOG", "warn")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
        let child = cmd.spawn()?;
        Ok(Self { child: Some(child) })
    }
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        if let Some(mut c) = self.child.take() {
            // Send SIGINT first so the server runs its graceful shutdown
            // path (drain connections → Reactor::drop → AsyncCancel2
            // cancels the multishot RECV → fds close cleanly). Without
            // this, SIGKILL bypasses all cleanup and leaves ghost UDP
            // sockets that cause handshake timeouts on the next run.
            let pid = c.id() as i32;
            // SAFETY: kill(2) on a child PID we own; signal number is
            // validated by the libc constants.
            unsafe { libc::kill(pid, libc::SIGINT); }

            // Poll for graceful exit (up to 3s).
            let deadline = std::time::Instant::now() + Duration::from_secs(3);
            loop {
                match c.try_wait() {
                    Ok(Some(_)) => return, // exited cleanly
                    Ok(None) if std::time::Instant::now() < deadline => {
                        std::thread::sleep(Duration::from_millis(50));
                    }
                    _ => break, // timed out or error → fall through to kill
                }
            }
            // Force kill as last resort.
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

/// Pick a free ephemeral port by briefly binding a TCP socket (OS hands
/// out free UDP/TCP ports from the same range).
pub fn ephemeral_port() -> std::io::Result<u16> {
    let l = std::net::TcpListener::bind("127.0.0.1:0")?;
    Ok(l.local_addr()?.port())
}

/// Resolve the server address for this benchmark run.
///
/// If `VIREON_ADDR` is set (e.g. `VIREON_ADDR=127.0.0.1:4433`), the example
/// connects to that external server and skips auto-spawning. Otherwise it
/// generates a dev cert, picks an ephemeral port, spawns a `quic-server`,
/// and waits for it to bind.
///
/// Returns `(addr, guard)` where `guard` is `None` when using an external
/// server.
pub async fn resolve_server() -> (String, Option<ServerGuard>) {
    if let Ok(addr) = std::env::var("VIREON_ADDR") {
        eprintln!("[bench] external server: {addr}");
        return (addr, None);
    }
    let (cert, key) = write_dev_cert().expect("cert");
    let port = ephemeral_port().expect("port");
    let guard = ServerGuard::start(port, &cert, &key).expect("server");
    // Give the server a moment to bind before we start connecting.
    // 1s covers the io_uring + buffer-pool init path even under load.
    tokio::time::sleep(Duration::from_millis(1000)).await;
    (format!("127.0.0.1:{port}"), Some(guard))
}

/// Retry-connect with `DangerAcceptInvalid` TLS until the server answers
/// (server takes a moment to start). SNI is fixed to `localhost`.
///
/// Each attempt is capped at 2 s so a server that hasn't bound yet
/// doesn't eat the full 10 s handshake timeout — retries happen fast.
pub async fn connect_ready(addr: &str) -> vireon_sdk::Client {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let attempt = tokio::time::timeout(
            Duration::from_secs(2),
            ClientBuilder::new(addr)
                .sni("localhost")
                .tls_verify(TlsVerify::DangerAcceptInvalid)
                .connect(),
        )
        .await;

        match attempt {
            Ok(Ok(c)) => return c,
            Ok(Err(e)) => {
                if tokio::time::Instant::now() >= deadline {
                    panic!("could not connect to test server: {e}");
                }
                tokio::time::sleep(Duration::from_millis(150)).await;
            }
            Err(_) => {
                // Per-attempt timeout — server not ready yet.
                if tokio::time::Instant::now() >= deadline {
                    panic!("could not connect to test server: connect_ready deadline exceeded");
                }
                tokio::time::sleep(Duration::from_millis(150)).await;
            }
        }
    }
}

/// Same retry-loop as [`connect_ready`], but configures a [`ReconnectPolicy`]
/// on the client. Used by scenarios that need auto-reconnect + resubscribe
/// semantics on the subscriber (e.g. s09_reconnect).
pub async fn connect_ready_with_reconnect(
    addr: &str,
    policy: ReconnectPolicy,
) -> vireon_sdk::Client {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let attempt = tokio::time::timeout(
            Duration::from_secs(2),
            ClientBuilder::new(addr)
                .sni("localhost")
                .tls_verify(TlsVerify::DangerAcceptInvalid)
                .reconnect(policy.clone())
                .connect(),
        )
        .await;

        match attempt {
            Ok(Ok(c)) => return c,
            Ok(Err(e)) => {
                if tokio::time::Instant::now() >= deadline {
                    panic!("could not connect to test server: {e}");
                }
                tokio::time::sleep(Duration::from_millis(150)).await;
            }
            Err(_) => {
                if tokio::time::Instant::now() >= deadline {
                    panic!("could not connect to test server: connect_ready_with_reconnect deadline exceeded");
                }
                tokio::time::sleep(Duration::from_millis(150)).await;
            }
        }
    }
}

// ── latency histogram ───────────────────────────────────────────────

/// Tiny latency histogram. Stores all samples in a Vec<u64> (nanoseconds)
/// and computes percentiles at the end. Sufficient for ~10⁵-sample
/// benchmarks; switch to HdrHistogram if sampling > 10⁷.
#[derive(Default)]
pub struct Histogram {
    samples: Vec<u64>,
}

impl Histogram {
    /// Record a latency in nanoseconds.
    pub fn record(&mut self, ns: u64) {
        self.samples.push(ns);
    }

    /// Number of recorded samples.
    #[allow(dead_code)]
    pub fn count(&self) -> usize {
        self.samples.len()
    }

    /// Compute the requested percentile (0..=100). Returns `None` when
    /// the histogram is empty.
    pub fn percentile(&self, pct: f64) -> Option<u64> {
        if self.samples.is_empty() {
            return None;
        }
        let mut sorted = self.samples.clone();
        sorted.sort_unstable();
        let idx = ((pct / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
        Some(sorted[idx.min(sorted.len() - 1)])
    }

    /// Min sample, or `None` when empty.
    #[allow(dead_code)]
    pub fn min(&self) -> Option<u64> {
        self.samples.iter().copied().min()
    }

    /// Max sample, or `None` when empty.
    #[allow(dead_code)]
    pub fn max(&self) -> Option<u64> {
        self.samples.iter().copied().max()
    }

    /// Arithmetic mean in ns, or `None` when empty.
    #[allow(dead_code)]
    pub fn mean(&self) -> Option<u64> {
        if self.samples.is_empty() {
            return None;
        }
        Some(self.samples.iter().sum::<u64>() / self.samples.len() as u64)
    }
}

/// Pretty-print a duration in a human-friendly unit (ns/μs/ms).
pub fn fmt_ns(ns: u64) -> String {
    if ns < 1_000 {
        format!("{ns} ns")
    } else if ns < 1_000_000 {
        format!("{:.1} μs", ns as f64 / 1_000.0)
    } else {
        format!("{:.2} ms", ns as f64 / 1_000_000.0)
    }
}

/// Pretty-print a byte rate per second (B/s → KiB/s → MiB/s).
pub fn fmt_bps(bytes_per_sec: f64) -> String {
    let kib = bytes_per_sec / 1024.0;
    if kib < 1024.0 {
        format!("{kib:.1} KiB/s")
    } else {
        format!("{:.2} MiB/s", kib / 1024.0)
    }
}

/// Header line used by every scenario's print block for visual consistency.
pub fn print_header(title: &str, duration: Duration, addr: &str) {
    println!();
    println!("═══════════════════════════════════════════════════════════════════════");
    println!("  {title}");
    println!("  duration: {:.1}s   target: {addr}", duration.as_secs_f64());
    println!("═══════════════════════════════════════════════════════════════════════");
}

/// Footer separator matching [`print_header`].
pub fn print_footer() {
    println!("═══════════════════════════════════════════════════════════════════════");
}

/// Initialise tracing for the process (idempotent). Helps when debugging
/// a scenario — set `RUST_LOG=vireon_sdk=debug` to see SDK internals.
pub fn init_tracing() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_target(true)
            .try_init();
    });
}
