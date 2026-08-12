//! `vireon` — CLI for the Vireon QUIC pub/sub runtime.
//!
//! A redis-cli-style tool for manual testing and operations against a Vireon
//! server. Built on `vireon_sdk` (it does NOT reimplement the wire protocol).
//!
//! ```text
//! vireon ping
//! vireon pub sensor.temp "23.5C"
//! vireon sub "sensor.*"
//! vireon stream pub video.frame data.bin --policy latest_only
//! vireon group sub jobs.tasks workers worker-1
//! vireon mux sub --stream video=video.frame:latest_only \
//!                --stream chat=chat.msg:reliable_ordered
//! ```

#![allow(clippy::print_stdout)]
#![allow(clippy::print_stderr)]

use std::collections::HashMap;
use std::io::{Read as _, Write as _};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use clap::{Parser, Subcommand};

use vireon_sdk::{
    Client, ClientBuilder, ClientIdentity, DeliveryPolicy, TlsVerify,
};

/// Redis-cli-style CLI for the Vireon QUIC pub/sub runtime.
#[derive(Debug, Parser)]
#[command(
    name = "vireon",
    version,
    about = "CLI for the Vireon QUIC pub/sub runtime",
    long_about = "A redis-cli-style tool for manual testing and operations.\n\n\
                  Built on vireon-sdk — the same Rust library all language bindings wrap."
    )]
struct Cli {
    /// Server address (`host:port` or bare `host` → port 4433).
    #[arg(short, long, env = "VIREON_ADDR", default_value = "127.0.0.1:4433", global = true)]
    addr: String,

    /// TLS verification mode.
    ///
    /// `tofu` (default): trust the first certificate the server presents.
    /// `danger_accept_invalid`: skip all validation (dev only).
    /// `strict:<ca.pem>`: validate against a PEM CA bundle.
    /// `pinned:<cert.der>`: require the exact DER certificate.
    #[arg(long, env = "VIREON_TLS_VERIFY", default_value = "danger_accept_invalid", global = true)]
    tls_verify: String,

    /// Override the TLS SNI hostname (defaults to the host part of --addr).
    #[arg(long, env = "VIREON_SNI", global = true)]
    sni: Option<String>,

    /// mTLS client cert (PEM). Use together with --client-key.
    #[arg(long, env = "VIREON_CLIENT_CERT", global = true)]
    client_cert: Option<PathBuf>,

    /// mTLS client key (PEM).
    #[arg(long, env = "VIREON_CLIENT_KEY", global = true)]
    client_key: Option<PathBuf>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Health check: connect and report RTT.
    Ping,
    /// Publish a single message (exits after ack).
    Pub {
        /// Destination topic (server default ACL requires two segments, e.g. `sensor.temp`).
        topic: String,
        /// Inline payload. Mutually exclusive with --file / --stdin.
        /// Omit and pass --file or --stdin for binary input.
        payload: Option<String>,
        /// Read payload from file.
        #[arg(long)]
        file: Option<PathBuf>,
        /// Read payload from stdin.
        #[arg(long)]
        stdin: bool,
    },
    /// Subscribe to a topic pattern and print messages until Ctrl+C / --count.
    Sub {
        /// Topic pattern. `*` matches a single segment (`sensor.*`).
        pattern: String,
        /// Output format: `text` (default) or `json`.
        #[arg(long, default_value = "text")]
        format: String,
        /// Exit after this many messages.
        #[arg(long)]
        count: Option<u64>,
    },
    /// Dedicated-stream operations.
    Stream {
        #[command(subcommand)]
        op: StreamOp,
    },
    /// Consumer-group operations.
    Group {
        #[command(subcommand)]
        op: GroupOp,
    },
    /// Multiplex many dedicated streams (each with its own delivery policy)
    /// over a SINGLE QUIC connection — Vireon's headline feature.
    Mux {
        #[command(subcommand)]
        op: MuxOp,
    },
}

#[derive(Debug, Subcommand)]
enum StreamOp {
    /// Publish on a dedicated stream.
    Pub {
        /// Topic to publish on this stream.
        topic: String,
        /// Inline payload (mutually exclusive with --file / --stdin).
        payload: Option<String>,
        #[arg(long)]
        file: Option<PathBuf>,
        #[arg(long)]
        stdin: bool,
        /// Per-stream delivery policy.
        #[arg(long, default_value = "reliable_ordered")]
        policy: String,
    },
    /// Receive on a dedicated stream.
    Sub {
        /// Topic to subscribe to on this stream.
        topic: String,
        /// Per-stream delivery policy.
        #[arg(long, default_value = "reliable_ordered")]
        policy: String,
        /// Output format.
        #[arg(long, default_value = "text")]
        format: String,
        #[arg(long)]
        count: Option<u64>,
    },
}

#[derive(Debug, Subcommand)]
enum GroupOp {
    /// Join a consumer group and print messages.
    Sub {
        /// Topic the group consumes.
        topic: String,
        /// Group name.
        group: String,
        /// This consumer's name (unique within the group).
        consumer: String,
        /// Output format.
        #[arg(long, default_value = "text")]
        format: String,
        #[arg(long)]
        count: Option<u64>,
    },
}

#[derive(Debug, Subcommand)]
enum MuxOp {
    /// Open N dedicated streams on ONE connection and print interleaved
    /// messages tagged with each stream's label.
    Sub {
        /// Stream declarations, each `LABEL=TOPIC:POLICY`.
        /// Policy: reliable_ordered | reliable_unordered | realtime_drop_old | latest_only.
        /// Repeat the flag to declare multiple streams.
        #[arg(long = "stream", required = true)]
        streams: Vec<String>,
        /// Output format: `text` (default) or `json`.
        #[arg(long, default_value = "text")]
        format: String,
        /// Exit after this many TOTAL messages across all streams.
        #[arg(long)]
        count: Option<u64>,
    },
    /// Open N dedicated streams on ONE connection, then publish each
    /// `--send` item to its labelled stream. All sends share one QUIC
    /// connection; each leaves on its own dedicated stream id.
    Pub {
        /// Stream declarations, each `LABEL=TOPIC:POLICY` (same format as `mux sub`).
        #[arg(long = "stream", required = true)]
        streams: Vec<String>,
        /// Send items, each `LABEL=PAYLOAD` (payload may contain `=`).
        /// Processed in argument order; routed to the matching labelled stream.
        #[arg(long = "send", required = true)]
        sends: Vec<String>,
        /// Milliseconds to sleep between sends (default 0).
        #[arg(long, default_value_t = 0)]
        delay: u64,
    },
}

// ── Entry point ──────────────────────────────────────────────────────────

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let cli = Cli::parse();

    if let Err(e) = run(cli).await {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<(), String> {
    let tls = parse_tls_verify(&cli.tls_verify)?;
    let mut builder = ClientBuilder::new(&cli.addr);
    if let Some(sni) = &cli.sni {
        builder = builder.sni(sni);
    }
    builder = builder.tls_verify(tls);
    if let (Some(cert), Some(key)) = (&cli.client_cert, &cli.client_key) {
        builder = builder.client_identity(ClientIdentity {
            cert: cert.clone(),
            key: key.clone(),
        });
    }

    match cli.command {
        Command::Ping => {
            let t = Instant::now();
            let client = builder.connect().await.map_err(err_str)?;
            let rtt = t.elapsed();
            // Also exercise the control plane with a trivial subscribe/unsubscribe.
            let _ = client.subscribe("__vireon_cli_ping__").await;
            println!(
                "pong (connect RTT: {:.2} ms, addr: {})",
                rtt.as_secs_f64() * 1000.0,
                cli.addr
            );
            let _ = client.close().await;
            Ok(())
        }
        Command::Pub {
            topic,
            payload,
            file,
            stdin,
        } => {
            let bytes = read_payload(payload, file, stdin)?;
            let client = builder.connect().await.map_err(err_str)?;
            client
                .publish(&topic, bytes)
                .await
                .map_err(|e| format!("publish failed: {e}"))?;
            let _ = client.close().await;
            println!("ok");
            Ok(())
        }
        Command::Sub {
            pattern,
            format,
            count,
        } => {
            let client = builder.connect().await.map_err(err_str)?;
            let mut sub = client
                .subscribe(&pattern)
                .await
                .map_err(|e| format!("subscribe failed: {e}"))?;
            println!(
                "subscribed to {pattern} on {} — Ctrl+C to exit",
                cli.addr
            );
            recv_loop(&mut sub, &format, count).await;
            let _ = client.close().await;
            Ok(())
        }
        Command::Stream { op } => run_stream_op(op, builder).await,
        Command::Group { op } => run_group_op(op, builder).await,
        Command::Mux { op } => run_mux_op(op, builder).await,
    }
}

async fn run_stream_op(op: StreamOp, builder: ClientBuilder) -> Result<(), String> {
    match op {
        StreamOp::Pub {
            topic,
            payload,
            file,
            stdin,
            policy,
        } => {
            let policy = parse_policy(&policy)?;
            let bytes = read_payload(payload, file, stdin)?;
            let client = builder.connect().await.map_err(err_str)?;
            let spec = vireon_sdk::StreamSpec::new(policy).with_topic(topic.clone());
            let stream = client
                .open_stream(spec)
                .await
                .map_err(|e| format!("stream open failed: {e}"))?;
            stream
                .publish(&topic, bytes)
                .await
                .map_err(|e| format!("stream publish failed: {e}"))?;
            let _ = stream.close().await;
            let _ = client.close().await;
            println!("ok (stream {})", policy_name(policy));
            Ok(())
        }
        StreamOp::Sub {
            topic,
            policy,
            format,
            count,
        } => {
            let policy = parse_policy(&policy)?;
            let client = builder.connect().await.map_err(err_str)?;
            let spec = vireon_sdk::StreamSpec::new(policy).with_topic(topic.clone());
            let mut stream = client
                .open_stream(spec)
                .await
                .map_err(|e| format!("stream open failed: {e}"))?;
            println!(
                "streaming {topic} on stream id {} ({}) — Ctrl+C to exit",
                stream.stream_id(),
                policy_name(policy),
            );
            stream_recv_loop(&mut stream, &format, count).await;
            let _ = client.close().await;
            Ok(())
        }
    }
}

async fn run_group_op(op: GroupOp, builder: ClientBuilder) -> Result<(), String> {
    match op {
        GroupOp::Sub {
            topic,
            group,
            consumer,
            format,
            count,
        } => {
            let client = builder.connect().await.map_err(err_str)?;
            let mut g = client
                .subscribe_group(&topic, &group, &consumer)
                .await
                .map_err(|e| format!("group join failed: {e}"))?;
            println!(
                "consumer {consumer} joined group {group} on {topic} — Ctrl+C to exit"
            );
            group_recv_loop(&mut g, &format, count).await;
            let _ = client.close().await;
            Ok(())
        }
    }
}

async fn run_mux_op(op: MuxOp, builder: ClientBuilder) -> Result<(), String> {
    match op {
        MuxOp::Sub {
            streams,
            format,
            count,
        } => {
            // Parse all stream specs up-front so we fail fast on a bad spec.
            let specs: Vec<(String, String, DeliveryPolicy)> = streams
                .iter()
                .map(|s| parse_stream_spec(s))
                .collect::<Result<_, _>>()?;
            if specs.is_empty() {
                return Err("mux sub requires at least one --stream".into());
            }

            let client = builder.connect().await.map_err(err_str)?;

            // Open N dedicated streams on this single connection.
            let mut handles: Vec<vireon_sdk::StreamHandle> = Vec::with_capacity(specs.len());
            let mut labels: Vec<String> = Vec::with_capacity(specs.len());
            for (label, topic, policy) in &specs {
                let spec = vireon_sdk::StreamSpec::new(*policy).with_topic(topic.clone());
                let stream = client
                    .open_stream(spec)
                    .await
                    .map_err(|e| format!("open_stream({label}) failed: {e}"))?;
                println!(
                    "[{label:<8}] opened stream {:<3} ({}) → {topic}",
                    stream.stream_id(),
                    policy_name(*policy),
                );
                handles.push(stream);
                labels.push(label.clone());
            }
            println!(
                "listening on {} streams over 1 connection — Ctrl+C to exit",
                handles.len()
            );
            let _ = std::io::stdout().flush();

            // Spawn one task per stream; each prints tagged messages.
            let total = Arc::new(AtomicU64::new(0));
            let done = Arc::new(tokio::sync::Notify::new());
            let mut join = Vec::with_capacity(handles.len());
            for (mut stream, label) in handles.into_iter().zip(labels.into_iter()) {
                let total = total.clone();
                let done = done.clone();
                let fmt = format.clone();
                let limit = count;
                join.push(tokio::spawn(async move {
                    loop {
                        tokio::select! {
                            msg = stream.recv() => match msg {
                                Some(m) => {
                                    print_tagged_msg(&label, &m, &fmt);
                                    let _ = std::io::stdout().flush();
                                    if let Some(c) = limit {
                                        let prev = total.fetch_add(1, Ordering::Relaxed);
                                        if prev + 1 >= c {
                                            // Signal main task to tear everything down.
                                            done.notify_waiters();
                                            return;
                                        }
                                    }
                                }
                                None => {
                                    eprintln!("[{label}] (stream closed)");
                                    return;
                                }
                            },
                            _ = tokio::signal::ctrl_c() => return,
                        }
                    }
                }));
            }

            // Wait for Ctrl+C OR a worker hitting --count.
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    eprintln!("(interrupted — closing all streams)");
                }
                _ = done.notified() => {
                    eprintln!("(count reached — closing all streams)");
                }
            }
            for h in join {
                h.abort();
            }
            let _ = client.close().await;
            Ok(())
        }
        MuxOp::Pub {
            streams,
            sends,
            delay,
        } => {
            let specs: Vec<(String, String, DeliveryPolicy)> = streams
                .iter()
                .map(|s| parse_stream_spec(s))
                .collect::<Result<_, _>>()?;
            let send_items: Vec<(String, Bytes)> = sends
                .iter()
                .map(|s| parse_send_spec(s))
                .collect::<Result<_, _>>()?;

            // Validate that every --send label has a matching --stream declaration.
            let declared: std::collections::HashSet<&str> =
                specs.iter().map(|(l, _, _)| l.as_str()).collect();
            for (label, _) in &send_items {
                if !declared.contains(label.as_str()) {
                    return Err(format!(
                        "send label '{label}' has no matching --stream declaration"
                    ));
                }
            }

            let client = builder.connect().await.map_err(err_str)?;

            // Open one stream per declared spec, keyed by label. Reuses the
            // same connection for every stream — that's the whole point.
            let mut by_label: HashMap<String, (String, vireon_sdk::StreamHandle)> =
                HashMap::with_capacity(specs.len());
            for (label, topic, policy) in &specs {
                let spec = vireon_sdk::StreamSpec::new(*policy).with_topic(topic.clone());
                let stream = client
                    .open_stream(spec)
                    .await
                    .map_err(|e| format!("open_stream({label}) failed: {e}"))?;
                println!(
                    "[{label:<8}] opened stream {:<3} ({}) → {topic}",
                    stream.stream_id(),
                    policy_name(*policy),
                );
                by_label.insert(label.clone(), (topic.clone(), stream));
            }
            let _ = std::io::stdout().flush();

            let delay = if delay == 0 {
                None
            } else {
                Some(Duration::from_millis(delay))
            };

            let mut sent = 0u64;
            for (label, payload) in send_items {
                // Label-to-stream mapping was validated above, but avoid
                // panicking on a logic bug — surface a clean error instead.
                let (topic, stream) = match by_label.get(&label) {
                    Some(entry) => entry,
                    None => return Err(format!("internal: label '{label}' missing")),
                };
                stream
                    .publish(topic, payload.clone())
                    .await
                    .map_err(|e| format!("publish({label}) failed: {e}"))?;
                let payload_str = String::from_utf8_lossy(&payload);
                println!(
                    "[{label:<8}] {payload_str} → stream {}",
                    stream.stream_id(),
                );
                let _ = std::io::stdout().flush();
                sent += 1;
                if let Some(d) = delay {
                    tokio::time::sleep(d).await;
                }
            }

            // Close streams + connection.
            for (_, (_, stream)) in by_label {
                let _ = stream.close().await;
            }
            let _ = client.close().await;
            println!(
                "ok ({sent} sends across {} streams on 1 connection)",
                specs.len()
            );
            Ok(())
        }
    }
}

// ── Receive loops ────────────────────────────────────────────────────────

trait MsgRecv {
    async fn recv(&mut self) -> Option<vireon_sdk::Message>;
}

impl MsgRecv for vireon_sdk::Subscription {
    async fn recv(&mut self) -> Option<vireon_sdk::Message> {
        vireon_sdk::Subscription::recv(self).await
    }
}

impl MsgRecv for vireon_sdk::StreamHandle {
    async fn recv(&mut self) -> Option<vireon_sdk::Message> {
        vireon_sdk::StreamHandle::recv(self).await
    }
}

impl MsgRecv for vireon_sdk::GroupSubscription {
    async fn recv(&mut self) -> Option<vireon_sdk::Message> {
        vireon_sdk::GroupSubscription::recv(self).await
    }
}

async fn recv_loop<R: MsgRecv + Unpin>(rx: &mut R, format: &str, count: Option<u64>) {
    let mut n = 0u64;
    loop {
        let msg = tokio::select! {
            m = rx.recv() => match m {
                Some(m) => m,
                None => {
                    eprintln!("(channel closed)");
                    return;
                }
            },
            _ = tokio::signal::ctrl_c() => {
                eprintln!("(interrupted)");
                return;
            }
        };
        print_msg(&msg, format);
        n += 1;
        let _ = std::io::stdout().flush();
        if let Some(c) = count {
            if n >= c {
                return;
            }
        }
    }
}

async fn stream_recv_loop<R: MsgRecv + Unpin>(rx: &mut R, format: &str, count: Option<u64>) {
    recv_loop(rx, format, count).await;
}

async fn group_recv_loop<R: MsgRecv + Unpin>(rx: &mut R, format: &str, count: Option<u64>) {
    recv_loop(rx, format, count).await;
}

// ── Helpers ──────────────────────────────────────────────────────────────

fn print_msg(msg: &vireon_sdk::Message, format: &str) {
    let topic = String::from_utf8_lossy(&msg.topic);
    match format {
        "json" => {
            // Compact JSON line. Payload printed as UTF-8 lossy; escape
            // control characters so the output stays a valid JSON string.
            let payload = String::from_utf8_lossy(&msg.payload);
            let payload_escaped: String = payload
                .chars()
                .map(|c| match c {
                    '"' => "\\\"".to_string(),
                    '\\' => "\\\\".to_string(),
                    '\n' => "\\n".to_string(),
                    '\r' => "\\r".to_string(),
                    '\t' => "\\t".to_string(),
                    c if (c as u32) < 0x20 => format!("\\u{:04x}", c as u32),
                    c => c.to_string(),
                })
                .collect();
            println!(
                "{{\"topic\":\"{topic}\",\"payload\":\"{payload_escaped}\",\"seq\":{},\"stream_id\":{}}}",
                msg.seq, msg.stream_id
            );
        }
        _ => {
            let payload = String::from_utf8_lossy(&msg.payload);
            println!("{topic} = {payload}");
        }
    }
}

fn read_payload(
    inline: Option<String>,
    file: Option<PathBuf>,
    stdin: bool,
) -> Result<Bytes, String> {
    if stdin {
        let mut buf = Vec::new();
        std::io::stdin()
            .read_to_end(&mut buf)
            .map_err(|e| format!("stdin read failed: {e}"))?;
        return Ok(Bytes::from(buf));
    }
    if let Some(path) = file {
        let buf = std::fs::read(&path).map_err(|e| format!("read {}: {e}", path.display()))?;
        return Ok(Bytes::from(buf));
    }
    if let Some(s) = inline {
        return Ok(Bytes::from(s.into_bytes()));
    }
    Err("no payload provided (pass an inline arg, --file, or --stdin)".into())
}

fn parse_tls_verify(s: &str) -> Result<TlsVerify, String> {
    if let Some(ca) = s.strip_prefix("strict:") {
        let p = PathBuf::from(ca);
        if !p.exists() {
            return Err(format!("CA bundle not found: {}", p.display()));
        }
        return Ok(TlsVerify::Strict { ca: p });
    }
    if let Some(der) = s.strip_prefix("pinned:") {
        let p = PathBuf::from(der);
        if !p.exists() {
            return Err(format!("pinned cert not found: {}", p.display()));
        }
        let bytes = std::fs::read(&p).map_err(|e| format!("read {}: {e}", p.display()))?;
        return Ok(TlsVerify::Pinned { cert_der: bytes });
    }
    match s {
        "tofu" => Ok(TlsVerify::Tofu),
        "danger_accept_invalid" => Ok(TlsVerify::DangerAcceptInvalid),
        other => Err(format!(
            "unknown tls_verify mode: {other} (expected tofu, danger_accept_invalid, strict:<path>, pinned:<path>)"
        )),
    }
}

fn parse_policy(s: &str) -> Result<DeliveryPolicy, String> {
    match s {
        "reliable_ordered" | "ordered" => Ok(DeliveryPolicy::ReliableOrdered),
        "reliable_unordered" | "unordered" => Ok(DeliveryPolicy::ReliableUnordered),
        "realtime_drop_old" | "realtime" => Ok(DeliveryPolicy::RealtimeDropOld),
        "latest_only" | "latest" => Ok(DeliveryPolicy::LatestOnly),
        other => Err(format!(
            "unknown policy: {other} (expected reliable_ordered, reliable_unordered, realtime_drop_old, latest_only)"
        )),
    }
}

/// Parse `--stream LABEL=TOPIC:POLICY`.
/// Splits on the first `=` to get (label, "topic:policy"), then on the
/// LAST `:` of the remainder so topic segments may themselves contain `:`.
fn parse_stream_spec(s: &str) -> Result<(String, String, DeliveryPolicy), String> {
    let eq = s.find('=').ok_or_else(|| {
        format!("invalid --stream '{s}': expected LABEL=TOPIC:POLICY")
    })?;
    let label = s[..eq].to_string();
    if label.is_empty() {
        return Err(format!("invalid --stream '{s}': empty label"));
    }
    let rest = &s[eq + 1..];
    let colon = rest.rfind(':').ok_or_else(|| {
        format!("invalid --stream '{s}': expected LABEL=TOPIC:POLICY (missing ':policy')")
    })?;
    let topic = rest[..colon].to_string();
    let policy_str = &rest[colon + 1..];
    if topic.is_empty() {
        return Err(format!("invalid --stream '{s}': empty topic"));
    }
    let policy = parse_policy(policy_str)?;
    Ok((label, topic, policy))
}

/// Parse `--send LABEL=PAYLOAD`. Splits on the FIRST `=` only — the payload
/// may itself contain `=`, `:`, or any other byte.
fn parse_send_spec(s: &str) -> Result<(String, Bytes), String> {
    let eq = s.find('=').ok_or_else(|| {
        format!("invalid --send '{s}': expected LABEL=PAYLOAD")
    })?;
    let label = s[..eq].to_string();
    if label.is_empty() {
        return Err(format!("invalid --send '{s}': empty label"));
    }
    let payload = Bytes::copy_from_slice(&s.as_bytes()[eq + 1..]);
    Ok((label, payload))
}

/// Tagged print for `mux sub` — each message is prefixed with `[label]`.
fn print_tagged_msg(label: &str, msg: &vireon_sdk::Message, format: &str) {
    let topic = String::from_utf8_lossy(&msg.topic);
    match format {
        "json" => {
            let payload = String::from_utf8_lossy(&msg.payload);
            let payload_escaped: String = payload
                .chars()
                .map(|c| match c {
                    '"' => "\\\"".to_string(),
                    '\\' => "\\\\".to_string(),
                    '\n' => "\\n".to_string(),
                    '\r' => "\\r".to_string(),
                    '\t' => "\\t".to_string(),
                    c if (c as u32) < 0x20 => format!("\\u{:04x}", c as u32),
                    c => c.to_string(),
                })
                .collect();
            println!(
                "{{\"stream\":\"{label}\",\"topic\":\"{topic}\",\"payload\":\"{payload_escaped}\",\"seq\":{},\"stream_id\":{}}}",
                msg.seq, msg.stream_id
            );
        }
        _ => {
            let payload = String::from_utf8_lossy(&msg.payload);
            println!("[{label:<8}] {topic} = {payload}");
        }
    }
}

fn policy_name(p: DeliveryPolicy) -> &'static str {
    match p {
        DeliveryPolicy::ReliableOrdered => "reliable_ordered",
        DeliveryPolicy::ReliableUnordered => "reliable_unordered",
        DeliveryPolicy::RealtimeDropOld => "realtime_drop_old",
        DeliveryPolicy::LatestOnly => "latest_only",
    }
}

fn err_str<E: std::fmt::Display>(e: E) -> String {
    format!("{e}")
}

// Keep `Client` import path stable even if future refactors move it.
#[allow(dead_code)]
fn _client_anchor(_c: Client) {}
