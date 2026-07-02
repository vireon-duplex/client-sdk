# Vireon Client SDK (Rust)

Production-grade async Rust client SDK and CLI for the
[Vireon](https://github.com/vireon-duplex/vireon) QUIC-native pub/sub
runtime.

## Workspace members

| Crate | Description |
|-------|-------------|
| [`client-sdk/`](client-sdk/) | Async Rust SDK (`vireon-sdk`) — the primary API for talking to a Vireon server |
| [`vireon-cli/`](vireon-cli/) | Redis-cli-style command-line tool for manual testing and operations |

## Build

```text
cargo build --release
```

## CLI quick start

```text
./target/release/vireon ping                          # health check
./target/release/vireon sub "sensor.*"                # subscribe
./target/release/vireon pub sensor.temp "23.5C"       # publish
```

See [`client-sdk/README.md`](client-sdk/README.md) for the SDK API and
[`vireon-cli/README.md`](vireon-cli/README.md) for full CLI usage.

## License

GNU AFFERO GENERAL PUBLIC LICENSE v3.0
