# vireon-sdk

Production-grade **async Rust client SDK** for the [Vireon](https://github.com/vireon-duplex/vireon)
QUIC-native pub/sub runtime — the primary surface external developers use
to talk to a Vireon server.

```toml
# Cargo.toml
[dependencies]
vireon-sdk = { path = "../client-sdk" }   # or git / crates.io once published
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```rust
use vireon_sdk::{ClientBuilder, DeliveryPolicy, StreamSpec, TlsVerify};
use bytes::Bytes;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Subscriber + publisher on the same server (server skips origin on
    // fan-out, so a single connection cannot echo back to itself).
    let sub = ClientBuilder::new("127.0.0.1:4433")
        .sni("localhost")
        .tls_verify(TlsVerify::DangerAcceptInvalid) // dev only
        .connect().await?;

    let pub_ = ClientBuilder::new("127.0.0.1:4433")
        .sni("localhost")
        .tls_verify(TlsVerify::DangerAcceptInvalid)
        .connect().await?;

    let mut rx = sub.subscribe("chat.*").await?;
    tokio::time::sleep(Duration::from_millis(50)).await;
    pub_.publish("chat.hello", b"hello from vireon-sdk").await?;

    let msg = tokio::time::timeout(Duration::from_secs(2), rx.recv()).await?;
    assert_eq!(msg.unwrap().payload.as_ref(), b"hello from vireon-sdk");
    Ok(())
}
```

## Why this SDK exists

Vireon's differentiator is **per-stream delivery semantics with real
head-of-line blocking isolation**. QUIC multiplexes many independent
streams over a single TLS-1.3 connection, and Vireon exploits that so
congestion, loss, or retransmission on one stream never blocks another.

The SDK exposes this via two distinct surfaces:

| Surface | When to use | API |
|---------|-------------|-----|
| **Default channel** | Quick start, low-volume pub/sub, fan-out to many subscribers | `client.subscribe(pattern)` / `client.publish(topic, bytes)` |
| **Dedicated stream** | Real-time cursors, AI token streams, anything needing HOL isolation or a specific policy | `client.open_stream(StreamSpec::new(policy))` |

A dedicated stream gets its own QUIC bidi stream (ids `4, 8, 12, …`),
declares its [`DeliveryPolicy`] to the server via a `StreamOpen` frame,
and receives deliveries **on that stream id** — so frames on the default
channel or any other dedicated stream can never block it.

```rust
use vireon_sdk::{DeliveryPolicy, StreamSpec};

// Open a dedicated LatestOnly stream for low-latency cursor updates.
// The server drops stale buffered frames on this stream — a slow consumer
// never holds back a faster one.
let mut cursor = sub.open_stream(
    StreamSpec::new(DeliveryPolicy::LatestOnly).with_topic("cursor.move"),
).await?;

pub_.publish("cursor.move", b"move(10,20)").await?;
let m = cursor.recv().await.unwrap();
```

## Delivery policies

Defined in the [`send-policy`](https://github.com/vireon-duplex/vireon) crate so the
wire byte stays the same on both sides:

| Policy | Behaviour |
|--------|-----------|
| `ReliableOrdered` | In-order delivery with a bounded reorder window. Never drops. |
| `ReliableUnordered` | Deliver immediately; dedup by `Seq`. Drops only duplicates. |
| `RealtimeDropOld` | Deliver newest-first; drop buffered entries that fall behind. |
| `LatestOnly` | Keep only the most recent frame. Old buffered frames are replaced. |

## TLS verification

The server ships with a self-signed cert in development. Choose a policy
that matches the deployment:

```rust
use vireon_sdk::TlsVerify;
use std::path::PathBuf;

// Production with a publicly-trusted cert:
TlsVerify::Strict { ca: PathBuf::from("/etc/ssl/certs/ca-bundle.crt") }

// Pin a specific leaf cert (DER bytes). Defeats MITM without a public CA.
// NOTE: v1 falls back to Tofu; a custom verifier is tracked as follow-up.
TlsVerify::Pinned { cert_der: vec![...] }

// Trust-on-first-use — accept whatever the server presents. Good for
// trusted LANs / dev boxes. This is the default.
TlsVerify::Tofu

// Disable validation entirely. Dev only, never ship this.
TlsVerify::DangerAcceptInvalid
```

## Reconnect & subscription resumption

A drop in the underlying QUIC connection (server crash, network blip)
does not have to tear down the application's subscription state. Enable
the reconnect FSM on the builder:

```rust
use vireon_sdk::ReconnectPolicy;
use std::time::Duration;

let policy = ReconnectPolicy {
    max_attempts: 20,                           // 0 disables reconnect (default)
    initial_backoff: Duration::from_millis(500),
    max_backoff: Duration::from_secs(10),
    resubscribe: true,
};

let client = ClientBuilder::new("vireon.example.com:4433")
    .reconnect(policy)
    // Lower the idle timeout so a dead peer is detected faster. Default
    // is 60 s; the negotiated effective value is min(client, server).
    .max_idle_timeout(Duration::from_secs(15))
    .connect().await?;
```

On reconnect the SDK:

1. Re-establishes the QUIC connection with exponential backoff.
2. Re-sends every active `Subscribe` on the default channel.
3. Re-opens every dedicated stream (`StreamOpen` + `Subscribe`) on the
   same stream ids the application is already holding.

Commands arriving during the backoff window are drained and responded
to with `NotConnected` so callers do not hang waiting on a transport
that is down.

## Topic patterns

Dot-delimited segments with `*` as a single-segment wildcard (same
semantics as the server's matcher — see
[`pubsub-engine`](https://github.com/vireon-duplex/vireon)):

| Pattern         | Topic              | Match |
|-----------------|--------------------|-------|
| `sensor.temp`   | `sensor.temp`      | yes   |
| `sensor.*`      | `sensor.temp`      | yes   |
| `sensor.*`      | `sensor.temp.high` | no    |
| `*.*.*`         | `a.b.c`            | yes   |
| `sensor.*.high` | `sensor.temp.high` | yes   |

> **Gotcha:** the server's default ACL is `allow("*.*", None, ALL)` —
> topics must have **two or more dot-separated segments**. A single
> segment like `"cursor"` is silently denied at both Subscribe and
> Publish. Use `"cursor.move"` instead, or override the ACL via
> `quic-server --acl-rules FILE`.

## Public API surface

The crate re-exports everything through the crate root — no need to
descend into submodules.

```rust
use vireon_sdk::{
    // Construction
    ClientBuilder, ReconnectPolicy, TlsVerify,
    // Handle + operations
    Client,
    // Pub/sub
    Subscription, Message, Payload, Qos,
    // Dedicated streams
    StreamSpec, StreamHandle,
    // Per-stream delivery semantics (re-exported from send-policy)
    DeliveryPolicy,
    // Errors
    ConnectError, PublishError, SubscribeError, StreamError, DecodeError,
};
```

`Client` is cheap to [`Clone`](https://doc.rust-lang.org/std/clone/trait.Clone.html)
(it holds only an `mpsc::Sender`); the heavy state (the `quiche::Connection`,
UDP socket, per-stream decoders) lives on a single background tokio task.
Every public method is `async` and dispatches through that task.

## Error handling

No `anyhow` in the public surface — every error is a typed enum so callers
can match on the specific case that matters:

```rust
use vireon_sdk::{ConnectError, PublishError};

match client.publish("topic", b"hello").await {
    Ok(()) => { /* delivered */ }
    Err(PublishError::NotConnected) => {
        // transport is down — either reconnect is in progress
        // (max_attempts > 0) or the connection was explicitly closed.
    }
    Err(PublishError::TooLarge(n)) => {
        // payload exceeded ClientBuilder::max_message_size
    }
    Err(e) => eprintln!("publish failed: {e}"),
}
```

A malformed frame on one stream **does not** tear down the connection —
the SDK logs and resets that stream's decoder only.

## Examples

The crate ships a quickstart demo and a growing suite of benchmark
scenarios that prove specific data-plane guarantees against a real
`quic-server`. Every scenario auto-spawns its own server on an ephemeral
port — just `cargo run` and watch.

### Quickstart

```text
cargo run -p vireon-sdk --release --example quickstart
```

Two clients (subscriber + publisher), default channel + a dedicated
`LatestOnly` stream, ~25 lines each. Requires a server running on
`127.0.0.1:4433`.

### Benchmark scenarios

| Scenario | Run | Proves |
|----------|-----|--------|
| **s07** HOL isolation | `cargo run -p vireon-sdk --release --example s07_hol_congestion` | 5 dedicated streams (video 16 KiB @ ~2 MiB/s + audio/events/rpc/telem) — video congestion never degrades the lighter streams |
| **s09** Reconnect + resubscribe | `cargo run -p vireon-sdk --release --example s09_reconnect` | Server is killed mid-session; the SDK detects peer death, reconnects with backoff, replays all subscriptions, and delivery resumes in <5 s |
| **s11** Sequence integrity | `cargo run -p vireon-sdk --release --example s11_ordering` | 500 frames on a `ReliableOrdered` dedicated stream — every frame received exactly once, in ascending order, zero gaps |

All scenarios use the shared helper module
[`examples/_bench_common.rs`](examples/_bench_common.rs) (self-signed
cert generation, `ServerGuard` RAII, latency histogram, formatted
output). The helper is `#[path]`-included by each scenario and is **not**
a standalone binary — `autoexamples = false` in `Cargo.toml` keeps cargo
from trying to compile it.

#### s07 — Head-of-line blocking isolation

The headline Vireon differentiator proof. One subscriber opens 5
dedicated QUIC streams; one publisher fires 5 workloads on the default
channel. The `video` stream carries 75 % of the byte load (16 KiB
frames at ~2 MiB/s); the other four stay at 100 % delivery with stable
p99 latency (~2 ms):

```text
video    LatestOnly        134/s  100.0%  2.09 MiB/s  ⚠ heaviest
audio    ReliableOrdered   134/s  100.0%  536 KiB/s   ✓ healthy
events   RealtimeDropOld   134/s  100.0%  67 KiB/s    ✓ healthy
rpc      ReliableOrdered   134/s  100.0%  34 KiB/s    ✓ healthy
telem    LatestOnly        134/s  100.0%  8 KiB/s     ✓ healthy
✓ HOL ISOLATION VERIFIED
```

#### s09 — Reconnect + resubscribe FSM

Publishes on a `ReliableOrdered` dedicated stream, kills the server
process, starts a fresh server on the same port, and verifies the SDK's
background task reconnects + replays subscriptions automatically:

```text
Phase 1: published 478, received 478
⟳ killing server — reconnect FSM should fire…
server back up after 1.6 s
Phase 2: published 477, received 477
✓ RECONNECT VERIFIED
```

Dead-peer detection uses a 1-second heartbeat probe (quiche 0.22 has no
built-in keepalive) plus a 3-second idle timeout — a killed server is
detected in ~3 s instead of waiting for QUIC's 30 s idle timer.

#### s11 — Sequence integrity

Publishes 500 numbered frames on a `ReliableOrdered` dedicated stream
and verifies the subscriber receives every frame exactly once, in
ascending order, with no gaps:

```text
published:   500
received:    500
gaps:        0
duplicates:  0
out-of-order: 0
✓ SEQUENCE INTEGRITY VERIFIED
```

## Tests

The integration suite spawns a real `quic-server` binary on an ephemeral
port and exercises the full stack (TLS handshake, frame codec, fan-out,
dedicated streams, reconnect, subscription replay):

```text
cargo test -p vireon-sdk -- --nocapture --test-threads=1
```

Unit tests (codec round-trips, pattern matching, payload encoders,
backoff math, address parsing) run as part of the same invocation.

## Crate layout

```text
client-sdk/src/
├── lib.rs          Public re-exports + crate-level docs
├── config.rs       ClientBuilder, ClientConfig, TlsVerify, ReconnectPolicy
├── connection.rs   Client handle, ConnCmd channel, run() task, reconnect FSM
├── transport.rs    quiche + tokio UDP loop, ALPN/TLS, loss-recovery timers
├── message.rs      Message (received), Payload trait, Qos
├── pubsub.rs       Subscription receiver
├── stream.rs       StreamSpec, StreamHandle (dedicated QUIC stream)
└── error.rs        Typed errors (ConnectError, PublishError, …)
```

## Wire format

The SDK speaks the same `frame::codec` wire format the server uses:
**22-byte header + payload + 4-byte CRC32C trailer**. It reuses the
`frame` crate's encoders/decoders directly so wire behaviour always
matches the server — no duplicated byte layout to drift.

ALPN: the client offers `h3-29` (one of the three the server advertises).
No mandatory `Handshake` frame — `principal_id=1` is the server default,
so `subscribe` / `publish` work immediately after the QUIC handshake.

## License

GNU AFFERO GENERAL PUBLIC LICENSE v3.0 — see
[`LICENSE`](https://github.com/vireon-duplex/vireon) at the repo root.
