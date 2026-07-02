# Vireon Client SDK

Production-grade **async Rust client SDK** and **CLI** for the
[Vireon](https://github.com/vireon-duplex/vireon) QUIC-native pub/sub
runtime — the primary surface for talking to a Vireon server.

Vireon's differentiator is **per-stream delivery semantics with real
head-of-line blocking isolation**. QUIC multiplexes many independent
streams over a single TLS-1.3 connection, and Vireon exploits that so
congestion, loss, or retransmission on one stream never blocks another.

## Workspace members

| Crate | Description |
|-------|-------------|
| [`client-sdk/`](client-sdk/) | Async Rust SDK (`vireon-sdk`) — the programmatic API |
| [`vireon-cli/`](vireon-cli/) | Redis-cli-style command-line tool (`vireon`) for manual testing and operations |

---

## Install

### Option 1 — Systemd (production)

```text
cargo build --release
sudo ./scripts/install.sh
```

Installs the `vireon` binary to `/usr/local/bin/` and a systemd template
(`vireon-sub@.service`) for long-running subscribers:

```text
# Start a named subscriber instance:
sudo cp /etc/vireon/subscribers/example.conf /etc/vireon/subscribers/logging.conf
sudo systemctl enable --now vireon-sub@logging
journalctl -u vireon-sub@logging -f
```

### Option 2 — Docker

```text
docker build -t vireon-cli .

# One-shot commands:
docker run --rm vireon-cli ping
docker run --rm -e VIREON_ADDR=server.example.com:4433 vireon-cli sub "sensor.*"
```

Or via docker-compose (profile-gated — does NOT start with `docker compose up`):

```text
docker compose run --rm cli ping
docker compose run --rm cli sub "sensor.*" --count 10
```

Set `VIREON_ADDR` to point at your Vireon server:

```yaml
# docker-compose.yml override
services:
  cli:
    environment:
      VIREON_ADDR: "vireon.example.com:4433"
```

### Option 3 — Build from source

```text
cargo build --release
./target/release/vireon ping
```

---

## CLI quick reference

```text
vireon ping                          # health check (connect RTT)
vireon pub sensor.temp "23.5C"       # one-shot publish
vireon sub "sensor.*" --format json  # subscribe + tail
vireon stream pub video.frame f.bin --policy latest_only
vireon group sub jobs.tasks workers worker-1
```

Vireon's headline feature — many streams with independent delivery
policies multiplexed on ONE connection:

```text
vireon mux sub \
  --stream video=video.frame:latest_only \
  --stream audio=audio.frame:realtime_drop_old \
  --stream chat=chat.msg:reliable_ordered
```

See [`vireon-cli/README.md`](vireon-cli/README.md) for the full CLI surface.

---

## SDK quickstart

```rust
use vireon_sdk::{ClientBuilder, TlsVerify};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Subscriber + publisher need separate connections (server skips
    // origin on fan-out — a client never receives its own publishes).
    let sub = ClientBuilder::new("vireon.example.com:4433")
        .tls_verify(TlsVerify::DangerAcceptInvalid) // dev only
        .connect().await?;

    let pub_ = ClientBuilder::new("vireon.example.com:4433")
        .tls_verify(TlsVerify::DangerAcceptInvalid)
        .connect().await?;

    let mut rx = sub.subscribe("chat.*").await?;
    pub_.publish("chat.hello", b"hello from vireon-sdk").await?;

    let msg = tokio::time::timeout(Duration::from_secs(2), rx.recv()).await?;
    println!("{}", String::from_utf8_lossy(&msg.unwrap().payload));
    Ok(())
}
```

Dedicated streams get their own QUIC stream with a specific delivery policy:

```rust
use vireon_sdk::{DeliveryPolicy, StreamSpec};

let mut cursor = sub.open_stream(
    StreamSpec::new(DeliveryPolicy::LatestOnly).with_topic("cursor.move"),
).await?;
```

| Policy | Behaviour |
|--------|-----------|
| `ReliableOrdered` | In-order delivery, bounded reorder window. Never drops. |
| `ReleliableUnordered` | Deliver immediately; dedup by `Seq`. Drops only duplicates. |
| `RealtimeDropOld` | Deliver newest-first; drop buffered entries that fall behind. |
| `LatestOnly` | Keep only the most recent frame. |

See [`client-sdk/README.md`](client-sdk/README.md) for the full SDK API
(reconnect, mTLS, consumer groups, cluster, benchmark scenarios).

---

## Notes

- The server's default ACL requires **two or more dot-separated segments**
  in topics — `sensor.temp` works, `sensor` is silently denied.
- A client never receives its own publishes (server filters origin on
  fan-out). Use two connections to test round-trips.
- `ping` measures the QUIC handshake RTT (TLS + ALPN + first flight),
  not an application-level echo.

## License

GNU AFFERO GENERAL PUBLIC LICENSE v3.0
