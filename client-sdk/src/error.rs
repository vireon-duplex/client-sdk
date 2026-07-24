//! Typed errors for the public SDK API.
//!
//! No `anyhow` is used in the public surface: every error is actionable so a
//! caller can decide whether to retry, reconnect, or surface a specific
//! diagnostic. All errors implement [`std::error::Error`] via `thiserror`.

use std::io;

/// Failure to establish or maintain the QUIC connection.
#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    /// TLS handshake or certificate validation failed.
    #[error("TLS error: {0}")]
    Tls(String),

    /// QUIC handshake did not complete within the deadline.
    #[error("QUIC handshake timed out after {0:?}")]
    HandshakeTimeout(std::time::Duration),

    /// The server did not select an ALPN protocol the client offers.
    #[error("ALPN negotiation failed; server did not select h3-29")]
    AlpnNegotiation,

    /// The peer closed the connection (optionally with a reason phrase).
    #[error("connection closed by peer: {0}")]
    Closed(String),

    /// The [`Client`] handle was already closed via [`Client::close`].
    ///
    /// [`Client`]: crate::Client
    /// [`Client::close`]: crate::Client::close
    #[error("the client has been closed")]
    ClientClosed,

    /// A configuration value was invalid.
    #[error("configuration error: {0}")]
    Config(String),

    /// The underlying QUIC library returned an error.
    #[error("quiche error: {0}")]
    Quiche(#[from] quiche::Error),

    /// An I/O error on the UDP socket.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
}

/// Failure to publish a message.
#[derive(Debug, thiserror::Error)]
pub enum PublishError {
    /// The connection is no longer usable (closed or reconnecting).
    #[error("not connected")]
    NotConnected,

    /// The payload could not be encoded for the wire.
    #[error("encoding failed: {0}")]
    EncodingFailed(String),

    /// The payload exceeded the configured maximum message size.
    #[error("payload too large: {0} bytes")]
    TooLarge(usize),

    /// QUIC flow control refused the write; retry after the peer credits window.
    #[error("flow-control blocked")]
    FlowControlBlocked,
}

/// Failure to (un)subscribe.
#[derive(Debug, thiserror::Error)]
pub enum SubscribeError {
    /// The subscription pattern was rejected (malformed or denied by ACL).
    #[error("invalid pattern: {0}")]
    InvalidPattern(String),

    /// The connection is no longer usable.
    #[error("not connected")]
    NotConnected,
}

/// Failure to open or use a dedicated logical stream.
#[derive(Debug, thiserror::Error)]
pub enum StreamError {
    /// The declared delivery policy was rejected by the server.
    #[error("policy rejected: {0}")]
    PolicyRejected(String),

    /// No stream id capacity remains (the connection's stream limit is reached).
    #[error("no stream capacity remaining")]
    NoCapacity,

    /// The connection is no longer usable.
    #[error("not connected")]
    NotConnected,
}

/// Wraps the wire-level [`FrameError`] so callers get a single decode type.
///
/// [`FrameError`]: frame::error::FrameError
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct DecodeError(#[from] frame::error::FrameError);

// NOTE: we intentionally do NOT auto-convert DecodeError into the connection
// error — a single malformed frame on one stream must not tear down the whole
// connection. The task logs and skips instead.

/// A [`DecodeError`] convenience constructor from any error displaying as the
/// inner frame error.
impl DecodeError {
    /// Wrap the given [`FrameError`].
    ///
    /// [`FrameError`]: frame::error::FrameError
    #[inline]
    #[must_use]
    pub fn from_frame(e: frame::error::FrameError) -> Self {
        Self(e)
    }
}
