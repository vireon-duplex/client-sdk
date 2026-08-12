# vireon-cli

A redis-cli-style command-line tool for the
[Vireon](https://github.com/vireon-duplex/vireon) QUIC pub/sub runtime — for manual testing,
operations, and ad-hoc inspection. Built on
[`vireon-sdk`](https://github.com/vireon-duplex/vireon) (it does not reimplement the wire protocol).

## Install

### Option 1 — System install (systemd)

Build and install alongside the server. The install script installs the
server binary, the CLI binary, and a systemd template for long-running
subscribers:

```text
cargo build --release -p quic-server -p vireon-cli
sudo ./scripts/install.sh
```

After install:

```text
vireon ping                                    # CLI on PATH (/usr/local/bin/)
sudo systemctl enable --now vireon             # start server
```

**Long-running subscribers** — the template unit `vireon-sub@.service`
runs `vireon sub` as a daemon, driven by an env-file:

```text
sudo cp /etc/vireon/subscribers/example.conf /etc/vireon/subscribers/logging.conf
sudoedit /etc/vireon/subscribers/logging.conf   # set PATTERN, FORMAT
sudo systemctl enable --now vireon-sub@logging  # start named instance
journalctl -u vireon-sub@logging -f             # tail output
```

### Option 2 — Docker

The server image bundles the CLI at `/usr/local/bin/vireon`. Use `docker
exec` against a running server, or `docker compose run` for one-shot
commands:

```text
# If the server is already running via docker compose:
docker compose up -d vireon
docker compose exec vireon vireon ping

# Or run the CLI as a one-shot container:
docker compose run --rm cli ping
docker compose run --rm cli sub "sensor.*" --count 10
docker compose run --rm cli pub sensor.temp "23.5C"
docker compose run --rm cli mux sub --stream video=video.frame:latest_only
```

The `cli` service is profile-gated — it does **not** start with
`docker compose up`, only when explicitly invoked. It auto-connects to
the server service via the compose network
(`VIREON_ADDR=vireon:4433`).

### Option 3 — Build from source (no install)

```text
cargo build --release -p vireon-cli
./target/release/vireon ping
```

### Prebuilt binary

Release builds are attached to every GitHub Release as
`vireon-linux-amd64` — see the [Releases
page](https://github.com/vireon-duplex/vireon/releases).

## Usage

```text
vireon [GLOBAL OPTIONS] <COMMAND>

Commands:
  ping    Health check: connect and report RTT
  pub     Publish a single message (exits after ack)
  sub     Subscribe to a pattern and print messages until Ctrl+C / --count
  stream  Dedicated-stream operations (pub / sub)
  group   Consumer-group operations (sub)
  mux     Multiplex many streams on ONE connection (Vireon's headline feature)
```

### Global options

| Flag | Env | Default | Meaning |
|------|-----|---------|---------|
| `-a, --addr <ADDR>` | `VIREON_ADDR` | `127.0.0.1:4433` | Server address (`host:port` or bare `host` → port 4433) |
| `--tls-verify <MODE>` | `VIREON_TLS_VERIFY` | `danger_accept_invalid` | `tofu`, `danger_accept_invalid`, `strict:<ca.pem>`, `pinned:<cert.der>` |
| `--sni <HOST>` | `VIREON_SNI` | host part of `--addr` | Override TLS SNI |
| `--client-cert <PATH>` | `VIREON_CLIENT_CERT` | — | mTLS client cert (PEM) |
| `--client-key <PATH>` | `VIREON_CLIENT_KEY` | — | mTLS client key (PEM) |

### ping

Connect, measure the TLS+QUIC handshake RTT, and exit.

```text
vireon ping
# pong (connect RTT: 1.72 ms, addr: 127.0.0.1:4433)
```

### pub — one-shot publish

```text
vireon pub sensor.temp "23.5C"               # inline string → bytes
vireon pub image.frame --file photo.jpg       # file contents → bytes
echo "hello" | vireon pub chat.msg --stdin    # stdin → bytes
```

### sub — subscribe and tail

Blocks until `Ctrl+C` or `--count N` is reached.

```text
vireon sub "sensor.*"                        # text format (default):  sensor.temp = 23.5C
vireon sub "sensor.*" --format json          # {"topic":"...","payload":"...","seq":N,"stream_id":M}
vireon sub "sensor.*" --count 10             # exit after 10 messages
```

### stream — dedicated-stream operations

```text
vireon stream pub video.frame data.bin --policy latest_only
vireon stream sub video.frame --policy realtime_drop_old

# policies: reliable_ordered | reliable_unordered | realtime_drop_old | latest_only
```

### group — consumer group

```text
vireon group sub jobs.tasks workers worker-1  # blocks, prints jobs
```

### mux — multiplex many streams on ONE connection

Vireon's headline feature: **one QUIC connection carries many independent
dedicated streams, each with its own `DeliveryPolicy`**. The `mux` command
lets you play with this from the CLI.

Each stream is declared as `LABEL=TOPIC:POLICY`:

```text
vireon mux sub \
  --stream video=video.frame:latest_only \
  --stream audio=audio.frame:realtime_drop_old \
  --stream chat=chat.msg:reliable_ordered
# [video   ] opened stream 4   (latest_only)         → video.frame
# [audio   ] opened stream 8   (realtime_drop_old)   → audio.frame
# [chat    ] opened stream 12  (reliable_ordered)     → chat.msg
# listening on 3 streams over 1 connection — Ctrl+C to exit
```

Publish to many streams from one connection:

```text
vireon mux pub \
  --stream video=video.frame:latest_only \
  --stream chat=chat.msg:reliable_ordered \
  --send video=frame-1 --send chat=hello \
  --send video=frame-2 --send chat=world
# [video   ] frame-1 → stream 4
# [chat    ] hello   → stream 8
# [video   ] frame-2 → stream 4
# [chat    ] world   → stream 8
# ok (4 sends across 2 streams on 1 connection)
```

Options:

| Flag | Meaning |
|------|---------|
| `--stream LABEL=TOPIC:POLICY` | Declare a stream (repeat for N streams) |
| `--send LABEL=PAYLOAD` | (mux pub) Route a payload to its labelled stream (payload may contain `=`) |
| `--format text\|json` | (mux sub) JSON includes `"stream":"LABEL"` |
| `--count N` | (mux sub) Exit after N total messages across all streams |
| `--delay MS` | (mux pub) Sleep between sends — useful for watching interleaving |

## Examples

Two-terminal pub/sub smoke test:

```text
# Terminal 1
vireon sub "chat.*"

# Terminal 2
vireon pub chat.hello "world"
vireon pub chat.test "second message"
```

JSON-line output for piping into `jq`:

```text
vireon sub "telemetry.*" --format json | jq .payload
```

## Notes

- The server's default ACL requires **two or more dot-separated segments**
  in topics — `sensor.temp` works, `sensor` is silently denied.
- A client never receives its own publishes (server filters origin on
  fan-out). Use two terminals or two `vireon` invocations to test
  round-trips.
- `ping` measures the QUIC handshake RTT (TLS + ALPN + first flight),
  not an application-level echo — there is no `PING` opcode on the wire.

## License

GNU AFFERO GENERAL PUBLIC LICENSE v3.0 — see
[`LICENSE`](https://github.com/vireon-duplex/vireon) at the repo root.
