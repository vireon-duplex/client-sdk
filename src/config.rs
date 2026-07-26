//! Configuration, builder, and TLS verification policy.
//!
//! [`ClientBuilder`] is the only construction path for a [`Client`]:
//!
//! ```no_run
//! # async fn demo() -> Result<(), vireon_sdk::ConnectError> {
//! use vireon_sdk::{ClientBuilder, TlsVerify};
//!
//! let client = ClientBuilder::new("127.0.0.1:4433")
//!     .sni("localhost")
//!     .tls_verify(TlsVerify::DangerAcceptInvalid) // dev only
//!     .connect()
//!     .await?;
//! # Ok(()) }
//! ```
//!
//! [`Client`]: crate::Client

use std::path::PathBuf;
use std::time::Duration;

use crate::connection::Client;
use crate::error::ConnectError;

/// How the client validates the server's TLS certificate.
///
/// The server ships with a self-signed certificate in development, so the
/// default is [`TlsVerify::Tofu`] (trust-on-first-use). Production deployments
/// should pin a certificate or supply a CA via [`TlsVerify::Strict`].
#[derive(Clone, Debug)]
pub enum TlsVerify {
    /// Validate against a CA bundle on disk (the only mode that should be used
    /// in production with a publicly-trusted certificate).
    Strict {
        /// Path to a PEM-encoded CA bundle.
        ca: PathBuf,
    },
    /// Accept the connection only if the server presents exactly this DER
    /// certificate. Defeats MITM while avoiding a public CA dependency.
    Pinned {
        /// DER bytes of the pinned leaf certificate.
        cert_der: Vec<u8>,
    },
    /// Trust-on-first-use: accept whatever certificate the server presents on
    /// the first connection. Suitable for development / trusted LANs.
    Tofu,
    /// Disable certificate validation entirely. **Development only.**
    DangerAcceptInvalid,
}

impl Default for TlsVerify {
    fn default() -> Self {
        Self::Tofu
    }
}

/// Client identity for mutual TLS (mTLS).
///
/// When set on a [`ClientBuilder`], the client presents this
/// certificate chain + key during the TLS handshake so the server can
/// authenticate the client. Independent of [`TlsVerify`] (which
/// controls how the *client* validates the *server*): production
/// mTLS deployments typically combine
/// [`TlsVerify::Strict`] with a [`ClientIdentity`].
///
/// ```
/// # use vireon_sdk::{ClientBuilder, ClientIdentity, TlsVerify};
/// # fn demo() -> Result<(), vireon_sdk::ConnectError> {
/// # async {
/// let _ = ClientBuilder::new("127.0.0.1:4433")
///     .sni("localhost")
///     .tls_verify(TlsVerify::Strict { ca: "/etc/vireon/ca.pem".into() })
///     .client_identity(ClientIdentity {
///         cert: "/etc/vireon/client.pem".into(),
///         key:  "/etc/vireon/client.key".into(),
///     })
///     .connect()
///     .await?;
/// # Ok::<_, vireon_sdk::ConnectError>(())
/// # });
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct ClientIdentity {
    /// PEM-encoded certificate chain path.
    pub cert: PathBuf,
    /// PEM-encoded private key path.
    pub key: PathBuf,
}

/// Reconnect behaviour after the connection drops.
///
/// `max_attempts == 0` (the default) disables automatic reconnection: the
/// client surfaces the disconnect and drops active subscriptions.
#[derive(Clone, Debug)]
pub struct ReconnectPolicy {
    /// Maximum reconnect attempts before giving up. `0` disables reconnect.
    pub max_attempts: u32,
    /// Backoff before the first retry.
    pub initial_backoff: Duration,
    /// Cap on the backoff between retries (exponential growth stops here).
    pub max_backoff: Duration,
    /// When `true`, re-establish every active subscription and reopen every
    /// dedicated stream after a successful reconnect.
    pub resubscribe: bool,
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 0,
            initial_backoff: Duration::from_millis(500),
            max_backoff: Duration::from_secs(10),
            resubscribe: true,
        }
    }
}

impl ReconnectPolicy {
    /// Fully disable automatic reconnection (explicit, readable call site).
    #[must_use]
    pub fn disabled() -> Self {
        Self {
            max_attempts: 0,
            ..Self::default()
        }
    }

    /// Backoff for the `attempt`-th retry (0-indexed), exponential up to the cap.
    #[must_use]
    pub fn backoff_for(&self, attempt: u32) -> Duration {
        // Exponential: initial * 2^attempt, clamped to max.
        let shift = attempt.min(31);
        let initial_ms = self.initial_backoff.as_millis() as u64;
        let base = initial_ms.saturating_mul(1u64 << shift);
        let max_ms = self.max_backoff.as_millis().max(1) as u64;
        Duration::from_millis(base.min(max_ms))
    }
}

/// Resolved configuration handed to the connection task.
#[derive(Clone, Debug)]
pub(crate) struct ClientConfig {
    /// Server hostname (no port).
    pub host: String,
    /// Server UDP port.
    pub port: u16,
    /// TLS SNI value.
    pub sni: String,
    /// Certificate validation policy.
    pub tls: TlsVerify,
    /// Optional client certificate for mutual TLS. When `Some`, the
    /// client presents this cert + key during the TLS handshake so the
    /// server can authenticate the client.
    pub client_identity: Option<ClientIdentity>,
    /// Reconnect policy.
    pub reconnect: ReconnectPolicy,
    /// Maximum accepted payload size for a single message (defensive cap).
    pub max_message_size: usize,
    /// Depth of each subscriber's bounded channel.
    pub subscriber_buffer: usize,
    /// Depth of the command channel between [`Client`] handles and the
    /// background I/O task. Larger values absorb `try_publish` bursts;
    /// smaller values apply backpressure sooner. Each entry is ~80 B +
    /// payload, so 4096 ≈ 330 KiB + payload bytes per connection.
    pub cmd_channel_cap: usize,
    /// QUIC idle timeout. The effective connection idle timeout is
    /// `min(client, server)`. Lower values detect dead peers faster at the
    /// cost of tearing down quiet connections sooner.
    pub idle_timeout: Duration,
}

/// Builder for a [`Client`].
///
/// Construct with [`ClientBuilder::new`], chain setters, then [`connect`].
///
/// [`connect`]: ClientBuilder::connect
/// [`Client`]: crate::Client
#[derive(Debug)]
pub struct ClientBuilder {
    cfg: ClientConfig,
}

impl ClientBuilder {
    /// Create a builder targeting `addr`.
    ///
    /// `addr` accepts `"host:port"` or a bare `"host"` (port defaults to
    /// `4433`, matching the server default). The SNI defaults to the host.
    #[must_use]
    pub fn new(addr: impl Into<String>) -> Self {
        let raw = addr.into();
        let (host, port) = parse_addr(&raw);
        let sni = host.clone();
        Self {
            cfg: ClientConfig {
                host,
                port,
                sni,
                tls: TlsVerify::default(),
                client_identity: None,
                reconnect: ReconnectPolicy::default(),
                max_message_size: 1024 * 1024,
                subscriber_buffer: 8192,
                cmd_channel_cap: 1024,
                idle_timeout: Duration::from_secs(60),
            },
        }
    }

    /// Override the TLS SNI hostname.
    #[must_use]
    pub fn sni(mut self, sni: impl Into<String>) -> Self {
        self.cfg.sni = sni.into();
        self
    }

    /// Set the certificate validation policy (default [`TlsVerify::Tofu`]).
    #[must_use]
    pub fn tls_verify(mut self, v: TlsVerify) -> Self {
        self.cfg.tls = v;
        self
    }

    /// Set the client identity for mutual TLS (default: none). When set,
    /// the client presents this certificate during the handshake so the
    /// server can authenticate it. Combine with [`TlsVerify::Strict`] for
    /// production mTLS.
    #[must_use]
    pub fn client_identity(mut self, id: ClientIdentity) -> Self {
        self.cfg.client_identity = Some(id);
        self
    }

    /// Set the reconnect policy (default: reconnect disabled).
    #[must_use]
    pub fn reconnect(mut self, p: ReconnectPolicy) -> Self {
        self.cfg.reconnect = p;
        self
    }

    /// Cap the payload size the SDK will accept for a single publish.
    #[must_use]
    pub fn max_message_size(mut self, n: usize) -> Self {
        self.cfg.max_message_size = n;
        self
    }

    /// Depth of each subscriber's bounded channel. Larger values absorb bursts
    /// at the cost of memory; smaller values apply backpressure sooner.
    #[must_use]
    pub fn subscriber_buffer(mut self, n: usize) -> Self {
        self.cfg.subscriber_buffer = n;
        self
    }

    /// Depth of the command channel between [`Client`] handles and the
    /// background I/O task (default 4096). `try_publish` returns
    /// [`PublishError::NotConnected`](crate::PublishError::NotConnected)
    /// when full; `publish().await` yields (natural backpressure).
    /// Raise for bursty high-throughput workloads; lower to apply
    /// backpressure sooner.
    #[must_use]
    pub fn cmd_channel_cap(mut self, n: usize) -> Self {
        self.cfg.cmd_channel_cap = n;
        self
    }

    /// QUIC idle timeout. The effective connection idle timeout is negotiated
    /// as `min(client, server)`. Lowering this makes dead-peer detection
    /// faster at the cost of closing quiet connections sooner; the SDK's
    /// reconnect FSM (if enabled) will re-establish the connection when it
    /// drops.
    ///
    /// Defaults to 60 s. The server default is 30 s, so the effective default
    /// is 30 s.
    #[must_use]
    pub fn max_idle_timeout(mut self, d: Duration) -> Self {
        self.cfg.idle_timeout = d;
        self
    }

    /// Establish the QUIC connection and spawn the background I/O task.
    ///
    /// Returns once the TLS handshake completes and the ALPN protocol is
    /// negotiated, or an error describing why connection failed.
    ///
    /// # Errors
    /// See [`ConnectError`].
    pub async fn connect(self) -> Result<Client, ConnectError> {
        let cfg = self.cfg;
        let cap = cfg.cmd_channel_cap.max(1);
        let (tx, rx) = tokio::sync::mpsc::channel(cap);
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        // Clone the sender into the task so it can embed it in StreamHandles.
        // The spawned task is detached: it exits when the last Client handle
        // drops (closing the command channel) or a Close command is received.
        tokio::spawn(crate::connection::run(cfg, rx, tx.clone(), ready_tx));
        match ready_rx.await {
            Ok(Ok(())) => Ok(Client::new(tx)),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(ConnectError::Closed(
                "connect task exited before the handshake completed".into(),
            )),
        }
    }
}

/// Split `"host:port"` (or `"host"`) into `(host, port)`.
///
/// IPv6 literals in brackets (`[::1]:4433`) are handled. A bare host without a
/// port defaults to `4433`.
fn parse_addr(raw: &str) -> (String, u16) {
    const DEFAULT_PORT: u16 = 4433;

    if raw.is_empty() {
        return (String::from("127.0.0.1"), DEFAULT_PORT);
    }

    // Bracketed IPv6: [::1]:port
    if let Some(rest) = raw.strip_prefix('[') {
        if let Some(end) = rest.find(']') {
            let host = format!("[{}]", &rest[..end]);
            let port_part = &rest[end + 1..];
            let port = port_part
                .strip_prefix(':')
                .and_then(|p| p.parse().ok())
                .unwrap_or(DEFAULT_PORT);
            return (host, port);
        }
    }

    match raw.rsplit_once(':') {
        // A bare host with no colon → default port.
        None => (raw.to_string(), DEFAULT_PORT),
        // host:port
        Some((h, p)) => match p.parse::<u16>() {
            Ok(port) => (h.to_string(), port),
            Err(_) => (raw.to_string(), DEFAULT_PORT),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_host_port() {
        assert_eq!(parse_addr("1.2.3.4:4433"), ("1.2.3.4".into(), 4433));
        assert_eq!(parse_addr("example.com:8443"), ("example.com".into(), 8443));
    }

    #[test]
    fn bare_host_defaults_port() {
        assert_eq!(parse_addr("example.com"), ("example.com".into(), 4433));
        assert_eq!(parse_addr(""), ("127.0.0.1".into(), 4433));
    }

    #[test]
    fn parses_ipv6_bracketed() {
        assert_eq!(parse_addr("[::1]:4433"), ("[::1]".into(), 4433));
        // Bracketed without port → default port.
        assert_eq!(parse_addr("[fe80::1]"), ("[fe80::1]".into(), 4433));
    }

    #[test]
    fn backoff_clamps() {
        let p = ReconnectPolicy::default();
        assert_eq!(p.backoff_for(0), Duration::from_millis(500));
        // Exponential growth then clamp at max_backoff.
        assert!(p.backoff_for(20) <= p.max_backoff);
    }
}
