//! Quickstart — connect, subscribe, publish, and receive in ~25 lines.
//!
//! The server does **not** echo a publish back to the connection that sent
//! it (origin-skip), so this demo uses two clients: one subscribes, the other
//! publishes.
//!
//! ## Run
//!
//! 1. Generate a dev cert:
//!    ```text
//!    openssl req -x509 -newkey rsa:2048 -keyout key.pem -out cert.pem \
//!      -days 1 -nodes -subj "/CN=localhost"
//!    ```
//! 2. Start a server:
//!    ```text
//!    cargo run -p quic-server -- --cert cert.pem --key key.pem --port 4433
//!    ```
//! 3. Run this example:
//!    ```text
//!    cargo run -p vireon-sdk --example quickstart
//!    ```

#![allow(clippy::print_stdout)]

use std::time::Duration;

use vireon_sdk::{ClientBuilder, DeliveryPolicy, StreamSpec, TlsVerify};

const ADDR: &str = "127.0.0.1:4433";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Two independent connections on the same server.
    let sub_client = ClientBuilder::new(ADDR)
        .sni("localhost")
        .tls_verify(TlsVerify::DangerAcceptInvalid)
        .connect()
        .await?;
    let pub_client = ClientBuilder::new(ADDR)
        .sni("localhost")
        .tls_verify(TlsVerify::DangerAcceptInvalid)
        .connect()
        .await?;

    // Default-channel subscription with a single-segment wildcard.
    let mut sub = sub_client.subscribe("chat.*").await?;
    println!("[quickstart] subscribed to chat.*");

    // Give the server a moment to register the subscription before publishing.
    tokio::time::sleep(Duration::from_millis(50)).await;

    pub_client
        .publish("chat.hello", b"hello from vireon-sdk")
        .await?;
    println!("[quickstart] published chat.hello");

    match tokio::time::timeout(Duration::from_secs(2), sub.recv()).await {
        Ok(Some(msg)) => {
            let topic = String::from_utf8_lossy(&msg.topic);
            let payload = String::from_utf8_lossy(&msg.payload);
            println!("[quickstart] recv  topic={topic} payload={payload}");
        }
        _ => println!("[quickstart] no message received within 2s"),
    }

    // Dedicated stream with its own delivery policy — true HOL isolation.
    let mut stream = sub_client
        .open_stream(StreamSpec::new(DeliveryPolicy::LatestOnly).with_topic("cursor.move"))
        .await?;
    println!("[quickstart] opened dedicated stream {} (LatestOnly)", stream.stream_id());

    tokio::time::sleep(Duration::from_millis(50)).await;
    pub_client.publish("cursor.move", b"move(10,20)").await?;
    if let Ok(Some(msg)) = tokio::time::timeout(Duration::from_secs(2), stream.recv()).await {
        println!(
            "[quickstart] stream recv topic={} payload={}",
            String::from_utf8_lossy(&msg.topic),
            String::from_utf8_lossy(&msg.payload),
        );
    }

    sub_client.close().await.ok();
    pub_client.close().await.ok();
    Ok(())
}
