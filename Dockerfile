# Dockerfile — Vireon CLI
# Multi-stage build: Rust builder → minimal Debian runtime.
# Produces a container that acts as a one-shot CLI against any Vireon server.

FROM rust:1.85-slim AS builder
WORKDIR /app
COPY . .
RUN cargo build --release -p vireon-cli
RUN cp target/*/release/vireon /tmp/vireon

FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /tmp/vireon /usr/local/bin/vireon
ENTRYPOINT ["vireon"]
CMD ["--help"]
