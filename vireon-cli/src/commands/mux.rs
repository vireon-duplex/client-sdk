//! `vireon mux sub|pub` — multi-stream multiplexing on ONE connection.
//!
//! This is Vireon's headline feature: many dedicated streams, each with
//! its own [`DeliveryPolicy`], share a single QUIC connection so
//! head-of-line blocking is isolated per stream.

use std::collections::{HashMap, HashSet};
use std::io::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use vireon_sdk::{ClientBuilder, DeliveryPolicy, StreamHandle, StreamSpec};

use crate::cli::MuxOp;
use crate::error::CliError;
use crate::output::print_tagged_msg;
use crate::parse::{parse_send_spec, parse_stream_spec, policy_name};

/// Dispatch a `mux` subcommand.
pub async fn run_mux(op: MuxOp, builder: ClientBuilder) -> Result<(), CliError> {
    match op {
        MuxOp::Sub {
            streams,
            format,
            count,
        } => mux_sub(builder, streams, format, count).await,
        MuxOp::Pub {
            streams,
            sends,
            delay,
        } => mux_pub(builder, streams, sends, delay).await,
    }
}

/// Open N dedicated streams on ONE connection and print interleaved
/// messages tagged with each stream's label.
async fn mux_sub(
    builder: ClientBuilder,
    streams: Vec<String>,
    format: String,
    count: Option<u64>,
) -> Result<(), CliError> {
    // Parse all stream specs up-front so we fail fast on a bad spec.
    let specs: Vec<(String, String, DeliveryPolicy)> = streams
        .iter()
        .map(|s| parse_stream_spec(s))
        .collect::<Result<_, _>>()?;
    if specs.is_empty() {
        return Err(CliError::BadArg(
            "mux sub requires at least one --stream".into(),
        ));
    }

    let client = builder.connect().await?;

    // Open N dedicated streams on this single connection.
    let mut handles: Vec<StreamHandle> = Vec::with_capacity(specs.len());
    let mut labels: Vec<String> = Vec::with_capacity(specs.len());
    for (label, topic, policy) in &specs {
        let spec = StreamSpec::new(*policy).with_topic(topic.clone());
        let stream = client.open_stream(spec).await?;
        println!(
            "[{label:<8}] opened stream {:<3} ({}) → {topic}",
            stream.stream_id(),
            policy_name(*policy),
        );
        handles.push(stream);
        labels.push(label.clone());
    }
    println!(
        "listening on {} streams over 1 connection — Ctrl+C to exit",
        handles.len()
    );
    let _ = std::io::stdout().flush();

    // Spawn one task per stream; each prints tagged messages.
    let total = Arc::new(AtomicU64::new(0));
    let done = Arc::new(tokio::sync::Notify::new());
    let mut join = Vec::with_capacity(handles.len());
    for (mut stream, label) in handles.into_iter().zip(labels.into_iter()) {
        let total = total.clone();
        let done = done.clone();
        let fmt = format.clone();
        let limit = count;
        join.push(tokio::spawn(async move {
            loop {
                tokio::select! {
                    msg = stream.recv() => match msg {
                        Some(m) => {
                            print_tagged_msg(&label, &m, &fmt);
                            let _ = std::io::stdout().flush();
                            if let Some(c) = limit {
                                let prev = total.fetch_add(1, Ordering::Relaxed);
                                if prev + 1 >= c {
                                    // Signal main task to tear everything down.
                                    done.notify_waiters();
                                    return;
                                }
                            }
                        }
                        None => {
                            eprintln!("[{label}] (stream closed)");
                            return;
                        }
                    },
                    _ = tokio::signal::ctrl_c() => return,
                }
            }
        }));
    }

    // Wait for Ctrl+C OR a worker hitting --count.
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            eprintln!("(interrupted — closing all streams)");
        }
        _ = done.notified() => {
            eprintln!("(count reached — closing all streams)");
        }
    }
    for h in join {
        h.abort();
    }
    let _ = client.close().await;
    Ok(())
}

/// Open N dedicated streams on ONE connection, then publish each
/// `--send` item to its labelled stream.
async fn mux_pub(
    builder: ClientBuilder,
    streams: Vec<String>,
    sends: Vec<String>,
    delay: u64,
) -> Result<(), CliError> {
    let specs: Vec<(String, String, DeliveryPolicy)> = streams
        .iter()
        .map(|s| parse_stream_spec(s))
        .collect::<Result<_, _>>()?;
    let send_items: Vec<(String, Bytes)> = sends
        .iter()
        .map(|s| parse_send_spec(s))
        .collect::<Result<_, _>>()?;

    // Validate that every --send label has a matching --stream declaration.
    let declared: HashSet<&str> = specs.iter().map(|(l, _, _)| l.as_str()).collect();
    for (label, _) in &send_items {
        if !declared.contains(label.as_str()) {
            return Err(CliError::BadArg(format!(
                "send label '{label}' has no matching --stream declaration"
            )));
        }
    }

    let client = builder.connect().await?;

    // Open one stream per declared spec, keyed by label. Reuses the
    // same connection for every stream — that's the whole point.
    let mut by_label: HashMap<String, (String, StreamHandle)> =
        HashMap::with_capacity(specs.len());
    for (label, topic, policy) in &specs {
        let spec = StreamSpec::new(*policy).with_topic(topic.clone());
        let stream = client.open_stream(spec).await?;
        println!(
            "[{label:<8}] opened stream {:<3} ({}) → {topic}",
            stream.stream_id(),
            policy_name(*policy),
        );
        by_label.insert(label.clone(), (topic.clone(), stream));
    }
    let _ = std::io::stdout().flush();

    let delay = if delay == 0 {
        None
    } else {
        Some(Duration::from_millis(delay))
    };

    let mut sent = 0u64;
    for (label, payload) in send_items {
        let (topic, stream) = match by_label.get(&label) {
            Some(entry) => entry,
            None => return Err(CliError::BadArg(format!("internal: label '{label}' missing"))),
        };
        stream.publish(topic, payload.clone()).await?;
        let payload_str = String::from_utf8_lossy(&payload);
        println!(
            "[{label:<8}] {payload_str} → stream {}",
            stream.stream_id(),
        );
        let _ = std::io::stdout().flush();
        sent += 1;
        if let Some(d) = delay {
            tokio::time::sleep(d).await;
        }
    }

    // Close streams + connection.
    for (_, (_, stream)) in by_label {
        let _ = stream.close().await;
    }
    let _ = client.close().await;
    println!(
        "ok ({sent} sends across {} streams on 1 connection)",
        specs.len()
    );
    Ok(())
}
