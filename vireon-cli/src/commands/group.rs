//! `vireon group sub` — consumer-group operations.

use vireon_sdk::ClientBuilder;

use crate::cli::GroupOp;
use crate::error::CliError;
use crate::recv::recv_loop;

/// Dispatch a `group` subcommand.
pub async fn run_group(op: GroupOp, builder: ClientBuilder) -> Result<(), CliError> {
    match op {
        GroupOp::Sub {
            topic,
            group,
            consumer,
            format,
            count,
        } => group_sub(builder, topic, group, consumer, format, count).await,
    }
}

async fn group_sub(
    builder: ClientBuilder,
    topic: String,
    group: String,
    consumer: String,
    format: String,
    count: Option<u64>,
) -> Result<(), CliError> {
    let client = builder.connect().await?;
    let mut g = client.subscribe_group(&topic, &group, &consumer).await?;
    println!("consumer {consumer} joined group {group} on {topic} — Ctrl+C to exit");
    recv_loop(&mut g, &format, count).await;
    let _ = client.close().await;
    Ok(())
}
