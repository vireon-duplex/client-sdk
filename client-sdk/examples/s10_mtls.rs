//! Scenario 10 — **Mutual TLS (mTLS) Verification**
//!
//! Verifies that:
//! 1. The server accepts a client that presents a certificate signed
//!    by the configured `--client-ca` CA.
//! 2. Publish + subscribe work end-to-end under mTLS.
//! 3. (Negative) The server rejects a client without a valid cert.
//!
//! Certificates are generated in-process via `rcgen`:
//!   - CA: self-signed, `is_ca = Ca`, used as `--client-ca`
//!   - Server cert: signed by CA, SAN=`localhost`, EKU=`ServerAuth`
//!   - Client cert: signed by CA, EKU=`ClientAuth`
//!
//! ## Run
//!
//! ```text
//! cargo run -p vireon-sdk --release --example s10_mtls
//! ```

#![allow(clippy::print_stdout, clippy::expect_used, clippy::unwrap_used)]

#[path = "_bench_common.rs"]
mod bench_common;

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bench_common::{ephemeral_port, init_tracing, print_footer, print_header, ServerGuard};
use vireon_sdk::{
    ClientBuilder, ClientIdentity, DeliveryPolicy, StreamSpec, TlsVerify,
};

/// How long to publish before checking delivery.
const PHASE_DURATION: Duration = Duration::from_secs(2);

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();

    // ── generate CA + server + client certs ─────────────────────────
    let dir = std::env::temp_dir().join(format!("vireon-mtls-{}", std::process::id()));
    std::fs::create_dir_all(&dir)?;
    let (ca_pem, server_pem, server_key, client_pem, client_key) =
        generate_mtls_certs(&dir)?;

    let port = ephemeral_port()?;
    let addr = format!("127.0.0.1:{port}");

    // ── start server with --client-ca ───────────────────────────────
    let server = ServerGuard::start_with(
        port,
        &server_pem,
        &server_key,
        &["--client-ca", ca_pem.to_str().unwrap()],
    )
    .expect("server");

    // Give the server a moment to bind.
    tokio::time::sleep(Duration::from_millis(500)).await;

    print_header("Scenario 10 — Mutual TLS (mTLS)", PHASE_DURATION, &addr);
    println!("  CA:     {}", ca_pem.display());
    println!("  Server: {} / {}", server_pem.display(), server_key.display());
    println!("  Client: {} / {}", client_pem.display(), client_key.display());
    println!();

    // ── Phase 1: valid mTLS client connects + publishes ─────────────
    println!("  Phase 1: connecting WITH client certificate…");
    let sub = ClientBuilder::new(&addr)
        .sni("localhost")
        .tls_verify(TlsVerify::Strict { ca: ca_pem.clone() })
        .client_identity(ClientIdentity {
            cert: client_pem.clone(),
            key: client_key.clone(),
        })
        .connect()
        .await
        .expect("mTLS connect should succeed with valid client cert");

    let stream = sub
        .open_stream(
            StreamSpec::new(DeliveryPolicy::ReliableOrdered).with_topic("mtls.test"),
        )
        .await
        .expect("open_stream");

    let recv_count = Arc::new(AtomicU64::new(0));
    let recv_clone = recv_count.clone();
    let collector = tokio::spawn(async move {
        let mut s = stream;
        while let Some(_msg) = s.recv().await {
            recv_clone.fetch_add(1, Ordering::Relaxed);
        }
    });

    // Publisher also needs client identity.
    let pub_client = ClientBuilder::new(&addr)
        .sni("localhost")
        .tls_verify(TlsVerify::Strict { ca: ca_pem.clone() })
        .client_identity(ClientIdentity {
            cert: client_pem.clone(),
            key: client_key.clone(),
        })
        .connect()
        .await
        .expect("publisher connect");

    let deadline = Instant::now() + PHASE_DURATION;
    let published = publish_burst(&pub_client, "mtls.test", deadline).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let received = recv_count.load(Ordering::Relaxed);
    println!("  Phase 1: published {published}, received {received}");

    pub_client.close().await.ok();
    sub.close().await.ok();
    collector.abort();
    drop(server);

    // ── Phase 2: client WITHOUT cert should be rejected ─────────────
    // Use a fresh port for server2 to avoid ghost-socket interference
    // (kernel holds a ghost ref on port1 for ~60s after killing server1
    // with active QUIC connections — see project_orphaned_udp_sockets.md).
    //
    // NOTE on QUIC TLS 1.3 handshake asymmetry: the client sees
    // is_established() BEFORE the server does (client Finished is sent
    // after processing server Finished). So the client's connect()
    // returns Ok before the server's post-handshake peer_cert() check
    // fires. The rejection surfaces as a failed app-layer operation
    // (open_stream/publish) shortly after connect.
    println!("\n  Phase 2: connecting WITHOUT client certificate (should be rejected)…");
    let port2 = ephemeral_port()?;
    let addr2 = format!("127.0.0.1:{port2}");
    let server2 = ServerGuard::start_with(
        port2,
        &server_pem,
        &server_key,
        &["--client-ca", ca_pem.to_str().unwrap()],
    )
    .expect("server restart");
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Connect (handshake completes because quiche 0.22's verify_peer(true)
    // validates certs IF presented but doesn't require them).
    let anon_client = ClientBuilder::new(&addr2)
        .sni("localhost")
        .tls_verify(TlsVerify::Strict { ca: ca_pem.clone() })
        // NOTE: no client_identity — server should reject post-handshake
        .reconnect(vireon_sdk::ReconnectPolicy::disabled())
        .connect()
        .await
        .expect("handshake completes (server rejection arrives after)");

    // Wait for the server's CONNECTION_CLOSE to propagate. The server
    // fires its peer_cert() check when is_established() becomes true,
    // then sends CONNECTION_CLOSE on the next flush cycle.
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Attempt a publish — should fail because the server has closed
    // the connection. open_stream would succeed locally (the stream is
    // created on the client side before the server rejects it), so we
    // test via publish which goes through the connection task and will
    // observe the closed state.
    let mut phase2_ok = false;
    for attempt in 0..5 {
        match anon_client.publish("mtls.test", b"rejected").await {
            Ok(()) => {
                // Publish succeeded — connection might still be alive.
                // Wait and retry.
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(_) => {
                phase2_ok = true;
                println!(
                    "  Phase 2: \u{2713} publish rejected as expected (no client cert, attempt {attempt})"
                );
                break;
            }
        }
    }
    if !phase2_ok {
        println!("  Phase 2: \u{2717} FAIL — publishes succeeded without client cert!");
    }
    anon_client.close().await.ok();
    drop(server2);

    // ── verdict ─────────────────────────────────────────────────────
    println!();
    let phase1_ok = received > 0 && published > 0 && received as usize >= published;
    if phase1_ok && phase2_ok {
        println!(
            "  \u{2713} mTLS VERIFIED \u{2014} valid cert accepted, missing cert rejected, {received} frames delivered."
        );
    } else {
        println!("  \u{2717} mTLS FAILED \u{2014} phase1_ok={phase1_ok}, phase2_ok={phase2_ok}");
    }
    print_footer();

    // Best-effort cleanup.
    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}

// ── publisher burst ────────────────────────────────────────────────

async fn publish_burst(client: &vireon_sdk::Client, topic: &str, deadline: Instant) -> usize {
    let mut seq: u64 = 0;
    let mut buf = [0u8; 32];
    loop {
        if Instant::now() >= deadline {
            return seq as usize;
        }
        buf[0..8].copy_from_slice(&seq.to_be_bytes());
        if client.publish(topic, &buf).await.is_ok() {
            seq += 1;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

// ── mTLS cert generation ───────────────────────────────────────────

/// Generate a CA, a server cert (signed by the CA, SAN=localhost,
/// EKU=ServerAuth), and a client cert (signed by the CA,
/// EKU=ClientAuth). Returns `(ca, server_cert, server_key,
/// client_cert, client_key)` as PEM file paths under `dir`.
fn generate_mtls_certs(
    dir: &std::path::Path,
) -> std::io::Result<(PathBuf, PathBuf, PathBuf, PathBuf, PathBuf)> {
    use rcgen::{CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose};

    // ── CA: self-signed, allowed to sign other certs ───────────────
    let mut ca_params = CertificateParams::new(Vec::<String>::new())
        .expect("ca params");
    ca_params.distinguished_name.push(DnType::CommonName, "vireon-bench-ca");
    ca_params.is_ca = IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
    ];
    let ca_key = KeyPair::generate().expect("ca key");
    let ca_cert = ca_params.self_signed(&ca_key).expect("ca self-signed");

    // ── Server cert: signed by CA, SAN=localhost, EKU=ServerAuth ───
    let mut server_params =
        CertificateParams::new(vec!["localhost".to_string()]).expect("server params");
    server_params.distinguished_name.push(DnType::CommonName, "vireon-bench-server");
    server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let server_key = KeyPair::generate().expect("server key");
    let server_cert = server_params
        .signed_by(&server_key, &ca_cert, &ca_key)
        .expect("server signed by CA");

    // ── Client cert: signed by CA, EKU=ClientAuth ──────────────────
    let mut client_params =
        CertificateParams::new(Vec::<String>::new()).expect("client params");
    client_params.distinguished_name.push(DnType::CommonName, "vireon-bench-client");
    client_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    let client_key = KeyPair::generate().expect("client key");
    let client_cert = client_params
        .signed_by(&client_key, &ca_cert, &ca_key)
        .expect("client signed by CA");

    // ── write all to PEM files ─────────────────────────────────────
    let ca_path = dir.join("ca.pem");
    let server_cert_path = dir.join("server.pem");
    let server_key_path = dir.join("server.key");
    let client_cert_path = dir.join("client.pem");
    let client_key_path = dir.join("client.key");

    std::fs::write(&ca_path, ca_cert.pem().as_bytes())?;
    std::fs::write(&server_cert_path, server_cert.pem().as_bytes())?;
    std::fs::write(&server_key_path, server_key.serialize_pem().as_bytes())?;
    std::fs::write(&client_cert_path, client_cert.pem().as_bytes())?;
    std::fs::write(&client_key_path, client_key.serialize_pem().as_bytes())?;

    Ok((
        ca_path,
        server_cert_path,
        server_key_path,
        client_cert_path,
        client_key_path,
    ))
}
