//! # vireon-sdk
//!
//! Production-grade async client SDK for the **Vireon** QUIC-native pub/sub
//! runtime.
//!
//! Vireon multiplexes many independent logical streams over a single TLS 1.3
//! QUIC connection, each with its own delivery semantics. The SDK turns that
//! into a small, ergonomic async API:
//!
//! ```text
//!   ClientBuilder::new("host:port").connect().await?
//!       .subscribe("sensor.*").await?      // wildcard subscription
//!       .publish("sensor.temp", bytes).await?
//!       .open_stream(StreamSpec::new(DeliveryPolicy::LatestOnly)).await?
//! ```
//!
//! ## Why multiple QUIC streams
//!
//! `publish(topic, bytes)` uses a shared default stream for the 5-minute
//! experience. `open_stream(StreamSpec)` opens a **dedicated** QUIC bidi
//! stream per delivery-policy group, which is what delivers real head-of-line
//! blocking isolation — Vireon's core differentiator. Congestion, loss, or
//! retransmission on one stream never blocks the others.
//!
//! ## Runtime
//!
//! The SDK is tokio-based. A single background task owns the `!Sync`
//! [`quiche::Connection`]; every public method is `async` and dispatches
//! commands to that task through a channel.
//!
//! ## Wire format
//!
//! Frames use the `frame::codec` 22-byte header + payload + 4-byte CRC32C
//! trailer (NOT the 40-byte conceptual header in `frame::header`). The SDK
//! reuses the frame crate's encoders/decoders directly so wire behaviour
//! always matches the server.
//!
//! [`quiche::Connection`]: quiche::Connection

#![warn(missing_docs)]

pub mod config;
pub mod connection;
pub mod error;
pub mod message;
pub mod pubsub;
pub mod stream;
pub mod transport;

// ── Primary public surface ──────────────────────────────────────────
//
// Re-exported here so users write `use vireon_sdk::{Client, ClientBuilder,
// DeliveryPolicy, ...}` without descending into submodules.

pub use config::{ClientBuilder, ClientIdentity, ReconnectPolicy, TlsVerify};
pub use connection::Client;
pub use error::{ConnectError, DecodeError, PublishError, StreamError, SubscribeError};
pub use message::{Message, Payload, Qos};
pub use pubsub::Subscription;
pub use stream::{StreamHandle, StreamSpec};

/// Per-stream delivery semantics — re-exported from `send_policy` so the wire
/// byte stays defined in exactly one place (the same byte the server decodes
/// on `StreamOpen`).
pub use send_policy::DeliveryPolicy;
