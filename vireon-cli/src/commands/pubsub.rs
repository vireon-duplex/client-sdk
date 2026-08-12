//! `vireon pub` and `vireon sub` — default-channel operations.

use std::path::PathBuf;

use vireon_sdk::ClientBuilder;

use crate::error::CliError;
use crate::payload::read_payload;
use crate::recv::recv_loop;

/// Publish a single message on the default channel, then exit.
pub async fn run_pub(
    builder: ClientBuilder,
    topic: String,
    payload: Option<String>,
    file: Option<PathBuf>,
    stdin: bool,
) -> Result<(), CliError> {
    let bytes = read_payload(payload, file, stdin)?;
    let client = builder.connect().await?;
    client.publish(&topic, bytes).await?;
    let _ = client.close().await;
    println!("ok");
    Ok(())
}

/// Subscribe to `pattern` on the default channel and print messages
/// until Ctrl+C or `count` is reached.
pub async fn run_sub(
    builder: ClientBuilder,
    pattern: String,
    format: String,
    count: Option<u64>,
    addr: String,
) -> Result<(), CliError> {
    let client = builder.connect().await?;
    let mut sub = client.subscribe(&pattern).await?;
    println!("subscribed to {pattern} on {addr} — Ctrl+C to exit");
    recv_loop(&mut sub, &format, count).await;
    let _ = client.close().await;
    Ok(())
}
