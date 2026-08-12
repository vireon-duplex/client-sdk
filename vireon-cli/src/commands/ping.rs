//! `vireon ping` — health check.

use std::time::Instant;

use vireon_sdk::ClientBuilder;

use crate::error::CliError;

/// Connect, report the TLS+QUIC handshake RTT, and exit.
///
/// Also fires a trivial subscribe/unsubscribe to exercise the control
/// plane — there is no `PING` opcode on the wire.
pub async fn run_ping(builder: ClientBuilder, addr: &str) -> Result<(), CliError> {
    let t = Instant::now();
    let client = builder.connect().await?;
    let rtt = t.elapsed();
    let _ = client.subscribe("__vireon_cli_ping__").await;
    println!(
        "pong (connect RTT: {:.2} ms, addr: {addr})",
        rtt.as_secs_f64() * 1000.0
    );
    let _ = client.close().await;
    Ok(())
}
