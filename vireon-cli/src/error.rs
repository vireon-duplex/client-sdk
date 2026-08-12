//! Typed errors for the CLI surface.
//!
//! Mirrors the client-sdk contract: every failure is an actionable enum
//! variant so callers (and future tests) can distinguish a bad argument
//! from a connect failure from a publish failure. No `anyhow` — all
//! errors implement [`std::error::Error`] via `thiserror`.

/// The single error type returned by every CLI command.
#[derive(Debug, thiserror::Error)]
pub enum CliError {
    /// A user-supplied argument was invalid (bad `--tls-verify`, unknown
    /// `--policy`, malformed `--stream`/`--send` spec, missing payload, …).
    #[error("{0}")]
    BadArg(String),

    /// The QUIC connection could not be established.
    #[error("connect failed: {0}")]
    Connect(#[from] vireon_sdk::ConnectError),

    /// A publish call failed.
    #[error("publish failed: {0}")]
    Publish(#[from] vireon_sdk::PublishError),

    /// A subscribe call failed.
    #[error("subscribe failed: {0}")]
    Subscribe(#[from] vireon_sdk::SubscribeError),

    /// Opening a dedicated stream failed.
    #[error("stream open failed: {0}")]
    StreamOpen(#[from] vireon_sdk::StreamError),

    /// Joining a consumer group failed.
    #[error("group join failed: {0}")]
    GroupJoin(#[from] vireon_sdk::GroupError),

    /// Local I/O (stdin, file read) failed.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}
