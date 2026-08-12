//! `vireon stream pub|sub` — dedicated-stream operations.

use std::path::PathBuf;

use vireon_sdk::{ClientBuilder, StreamSpec};

use crate::cli::StreamOp;
use crate::error::CliError;
use crate::parse::{parse_policy, policy_name};
use crate::payload::read_payload;
use crate::recv::recv_loop;

/// Dispatch a `stream` subcommand.
pub async fn run_stream(op: StreamOp, builder: ClientBuilder) -> Result<(), CliError> {
    match op {
        StreamOp::Pub {
            topic,
            payload,
            file,
            stdin,
            policy,
        } => stream_pub(builder, topic, payload, file, stdin, policy).await,
        StreamOp::Sub {
            topic,
            policy,
            format,
            count,
        } => stream_sub(builder, topic, policy, format, count).await,
    }
}

async fn stream_pub(
    builder: ClientBuilder,
    topic: String,
    payload: Option<String>,
    file: Option<PathBuf>,
    stdin: bool,
    policy: String,
) -> Result<(), CliError> {
    let policy = parse_policy(&policy)?;
    let bytes = read_payload(payload, file, stdin)?;
    let client = builder.connect().await?;
    let spec = StreamSpec::new(policy).with_topic(topic.clone());
    let stream = client.open_stream(spec).await?;
    stream.publish(&topic, bytes).await?;
    let _ = stream.close().await;
    let _ = client.close().await;
    println!("ok (stream {})", policy_name(policy));
    Ok(())
}

async fn stream_sub(
    builder: ClientBuilder,
    topic: String,
    policy: String,
    format: String,
    count: Option<u64>,
) -> Result<(), CliError> {
    let policy = parse_policy(&policy)?;
    let client = builder.connect().await?;
    let spec = StreamSpec::new(policy).with_topic(topic.clone());
    let mut stream = client.open_stream(spec).await?;
    println!(
        "streaming {topic} on stream id {} ({}) — Ctrl+C to exit",
        stream.stream_id(),
        policy_name(policy),
    );
    recv_loop(&mut stream, &format, count).await;
    let _ = client.close().await;
    Ok(())
}
