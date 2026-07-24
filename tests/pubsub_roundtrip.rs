//! End-to-end integration test: generates a self-signed cert, spawns the real
//! `quic-server` binary on an ephemeral port, and exercises the SDK against it.
//!
//! Run with:
//! ```text
//! cargo test -p vireon-sdk --test pubsub_roundtrip -- --nocapture --test-threads=1
//! ```
//!
//! The test auto-builds the `quic-server` binary (incremental, fast once built).

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic, clippy::print_stdout)]

use std::path::{Path, PathBuf};

/// Install a tracing subscriber if none is active. Idempotent — safe to call
/// from every test. Without this, the SDK's `tracing::info!` / `warn!` calls
/// are invisible during debugging.
fn init_tracing() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_target(true)
            .try_init();
    });
}
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use bytes::Bytes;
use rcgen::{CertificateParams, KeyPair};
use vireon_sdk::{ClientBuilder, DeliveryPolicy, ReconnectPolicy, StreamSpec, TlsVerify};

// ── test cert generation ────────────────────────────────────────────

/// Write a self-signed cert + key to temp files; return their paths.
fn write_dev_cert() -> std::io::Result<(PathBuf, PathBuf)> {
    let mut params =
        CertificateParams::new(vec!["localhost".to_string()]).expect("rcgen params");
    params.distinguished_name.push(rcgen::DnType::CommonName, "localhost");
    let key = KeyPair::generate().expect("rcgen key");
    let cert = params.self_signed(&key).expect("rcgen self-signed");
    let cert_pem = cert.pem();
    let key_pem = key.serialize_pem();

    // Unique per call: pid + a monotonic counter. Two tests in the same
    // process would otherwise race on the pid-only filename.
    use std::sync::atomic::{AtomicU64, Ordering};
    static CALL: AtomicU64 = AtomicU64::new(0);
    let n = CALL.fetch_add(1, Ordering::Relaxed);
    let id = std::process::id();
    let dir = std::env::temp_dir();
    let cert_path = dir.join(format!("vireon-sdk-test-{id}-{n}-cert.pem"));
    let key_path = dir.join(format!("vireon-sdk-test-{id}-{n}-key.pem"));
    std::fs::write(&cert_path, cert_pem.as_bytes())?;
    std::fs::write(&key_path, key_pem.as_bytes())?;
    Ok((cert_path, key_path))
}

// ── server lifecycle ────────────────────────────────────────────────

/// Ensure the `quic-server` binary is built and return its path.
///
/// The workspace uses a `target/<host-triple>/` layout (explicit `--target`),
/// so we search the target root rather than hard-coding the path.
fn server_binary() -> PathBuf {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let target_root = manifest.join("../target");
    // Build (incremental) so the binary always exists and is current.
    let _ = Command::new(env!("CARGO"))
        .args(["build", "-p", "quic-server", "--bin", "quic-server"])
        .status();

    // Candidate layouts: target/debug/quic-server OR target/<triple>/debug/quic-server.
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
        "quic-server binary not found under {}; \
         run `cargo build -p quic-server` manually",
        target_root.display()
    );
}

/// RAII guard that kills the spawned server on drop.
struct ServerGuard {
    child: Option<Child>,
}

impl ServerGuard {
    fn start(port: u16, cert: &Path, key: &Path) -> std::io::Result<Self> {
        let bin = server_binary();
        let child = Command::new(&bin)
            .args([
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
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        Ok(Self { child: Some(child) })
    }
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

/// Allocate an ephemeral port by briefly binding a TCP socket (the QUIC server
/// uses UDP, but the OS hands out free ports from the same range).
fn ephemeral_port() -> std::io::Result<u16> {
    let l = std::net::TcpListener::bind("127.0.0.1:0")?;
    Ok(l.local_addr()?.port())
}

/// Connect with retries until the server is ready (it takes a moment to start).
async fn connect_ready(addr: &str) -> vireon_sdk::Client {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        match ClientBuilder::new(addr)
            .sni("localhost")
            .tls_verify(TlsVerify::DangerAcceptInvalid)
            .connect()
            .await
        {
            Ok(c) => return c,
            Err(e) => {
                if tokio::time::Instant::now() >= deadline {
                    panic!("could not connect to test server: {e}");
                }
                tokio::time::sleep(Duration::from_millis(150)).await;
            }
        }
    }
}

#[tokio::test]
async fn pubsub_roundtrip_e2e() {
    init_tracing();
    let (cert, key) = write_dev_cert().expect("write cert");
    let port = ephemeral_port().expect("ephemeral port");
    let addr = format!("127.0.0.1:{port}");
    let _server = ServerGuard::start(port, &cert, &key).expect("start server");

    // Subscriber + publisher connections (server skips origin on fan-out).
    let sub_client = connect_ready(&addr).await;
    let pub_client = connect_ready(&addr).await;

    // ── 1. default-channel wildcard subscribe + publish ─────────────
    let mut sub = sub_client
        .subscribe("test.*")
        .await
        .expect("subscribe");
    // Let the server register the subscription before publishing.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let payload = b"hello-e2e".to_vec();
    pub_client
        .publish("test.hello", payload.clone())
        .await
        .expect("publish");

    let msg = tokio::time::timeout(Duration::from_secs(3), sub.recv())
        .await
        .expect("timed out waiting for message")
        .expect("subscription closed");
    assert_eq!(msg.topic, Bytes::from_static(b"test.hello"));
    assert_eq!(msg.payload.as_ref(), payload.as_slice());
    println!("[e2e] default-channel roundtrip OK");

    // ── 2. wildcard non-match is NOT delivered ──────────────────────
    pub_client.publish("other.topic", b"should-not-arrive").await.ok();
    if let Ok(Some(m)) = tokio::time::timeout(Duration::from_millis(400), sub.recv()).await {
        panic!("unexpected message on non-matching topic: {:?}", m.topic);
    }
    println!("[e2e] wildcard non-match correctly skipped");

    // ── 3. dedicated stream with LatestOnly policy ──────────────────
    // Topic must match the server's default ACL `*.*` (two segments); a
    // single-segment name like "cursor" is denied and silently dropped.
    let mut stream = sub_client
        .open_stream(StreamSpec::new(DeliveryPolicy::LatestOnly).with_topic("cursor.move"))
        .await
        .expect("open stream");
    assert!(stream.stream_id() > 0, "dedicated stream id must be non-zero");
    tokio::time::sleep(Duration::from_millis(100)).await;
    pub_client.publish("cursor.move", b"move(10,20)").await.expect("publish to cursor");

    let sm = tokio::time::timeout(Duration::from_secs(3), stream.recv())
        .await
        .expect("timed out waiting for stream message")
        .expect("stream closed");
    assert_eq!(sm.topic, Bytes::from_static(b"cursor.move"));
    assert_eq!(sm.payload.as_ref(), b"move(10,20)");
    println!("[e2e] dedicated-stream (LatestOnly) roundtrip OK, stream_id={}", stream.stream_id());

    sub_client.close().await.ok();
    pub_client.close().await.ok();
}

#[tokio::test]
async fn reconnect_resumes_subscriptions() {
    init_tracing();
    let (cert, key) = write_dev_cert().expect("write cert");

    // Start the first server.
    let port1 = ephemeral_port().expect("ephemeral port 1");
    let addr1 = format!("127.0.0.1:{port1}");
    let server1 = ServerGuard::start(port1, &cert, &key).expect("start server 1");

    // Connect with a fast reconnect policy.
    let policy = ReconnectPolicy {
        max_attempts: 20,
        initial_backoff: Duration::from_millis(150),
        max_backoff: Duration::from_millis(500),
        resubscribe: true,
    };
    let sub_client = ClientBuilder::new(&addr1)
        .sni("localhost")
        .tls_verify(TlsVerify::DangerAcceptInvalid)
        .reconnect(policy)
        // Short idle timeout so the SDK detects the killed server within a
        // couple of seconds rather than waiting the full negotiated 30 s.
        .max_idle_timeout(Duration::from_secs(5))
        .connect()
        .await
        .expect("connect with reconnect");
    let pub_client = connect_ready(&addr1).await;

    // Subscribe on the default channel.
    let mut sub = sub_client.subscribe("chat.*").await.expect("subscribe");
    tokio::time::sleep(Duration::from_millis(100)).await;
    pub_client.publish("chat.hello", b"first").await.expect("publish 1");
    let m = tokio::time::timeout(Duration::from_secs(3), sub.recv())
        .await
        .expect("timeout waiting for first message")
        .expect("subscription closed");
    assert_eq!(m.payload.as_ref(), b"first");
    println!("[e2e-reconnect] initial subscribe + publish OK");

    // Kill the server, then immediately restart on the same port. The
    // subscriber's pinned peer address still resolves; what we're testing
    // is that the SDK detects the connection drop (via the idle timer) and
    // re-establishes + replays subscriptions on the fresh server.
    drop(server1);
    println!("[e2e-reconnect] killed server 1");
    // Brief pause so the OS releases the UDP port before server2 binds it.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let server2 = ServerGuard::start(port1, &cert, &key).expect("start server 2");
    println!("[e2e-reconnect] restarted server on same port");

    // Wait for the subscriber's idle timer (5 s) to fire + reconnect handshake
    // + replay. Add a margin so the publish lands inside the subscriber's
    // next alive window.
    tokio::time::sleep(Duration::from_secs(7)).await;

    // New publisher connection against the restarted server, then publish.
    let pub2 = connect_ready(&addr1).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    pub2.publish("chat.after", b"second").await.expect("publish 2");

    let m = tokio::time::timeout(Duration::from_secs(5), sub.recv())
        .await
        .expect("timeout: subscription was not resumed after reconnect")
        .expect("subscription closed");
    assert_eq!(m.topic, Bytes::from_static(b"chat.after"));
    assert_eq!(m.payload.as_ref(), b"second");
    println!("[e2e-reconnect] subscription resumed — message delivered post-reconnect");

    // Silence unused warnings on guards.
    drop(server2);
    sub_client.close().await.ok();
    pub_client.close().await.ok();
    pub2.close().await.ok();
}
