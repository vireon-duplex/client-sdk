//! clap definitions for the `vireon` CLI.
//!
//! All structs are [`Clone`] + [`Debug`] via derive. Fields are `pub` so
//! the dispatch layer in [`crate::main`] and the per-command modules in
//! [`crate::commands`] can read them directly.

use std::path::PathBuf;

use clap::{Parser, Subcommand};

/// Redis-cli-style CLI for the Vireon QUIC pub/sub runtime.
#[derive(Debug, Parser)]
#[command(
    name = "vireon",
    version,
    about = "CLI for the Vireon QUIC pub/sub runtime",
    long_about = "A redis-cli-style tool for manual testing and operations.\n\n\
                  Built on vireon-sdk — the same Rust library all language bindings wrap."
)]
pub struct Cli {
    /// Server address (`host:port` or bare `host` → port 4433).
    #[arg(short, long, env = "VIREON_ADDR", default_value = "127.0.0.1:4433", global = true)]
    pub addr: String,

    /// TLS verification mode.
    ///
    /// `tofu` (default): trust the first certificate the server presents.
    /// `danger_accept_invalid`: skip all validation (dev only).
    /// `strict:<ca.pem>`: validate against a PEM CA bundle.
    /// `pinned:<cert.der>`: require the exact DER certificate.
    #[arg(long, env = "VIREON_TLS_VERIFY", default_value = "danger_accept_invalid", global = true)]
    pub tls_verify: String,

    /// Override the TLS SNI hostname (defaults to the host part of --addr).
    #[arg(long, env = "VIREON_SNI", global = true)]
    pub sni: Option<String>,

    /// mTLS client cert (PEM). Use together with --client-key.
    #[arg(long, env = "VIREON_CLIENT_CERT", global = true)]
    pub client_cert: Option<PathBuf>,

    /// mTLS client key (PEM).
    #[arg(long, env = "VIREON_CLIENT_KEY", global = true)]
    pub client_key: Option<PathBuf>,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
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
pub enum StreamOp {
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
pub enum GroupOp {
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
pub enum MuxOp {
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
