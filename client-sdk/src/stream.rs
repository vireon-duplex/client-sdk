//! Dedicated logical streams with per-stream delivery semantics.
//!
//! Opening a [`StreamHandle`] via [`crate::Client::open_stream`] allocates a
//! dedicated QUIC bidirectional stream and declares its [`DeliveryPolicy`] to
//! the server with a `StreamOpen` frame. The server records the policy for
//! that stream (`quic-server/.../application.rs:2144` → `bind_send_policy`) and
//! applies it to every frame it fans out onto that stream.
//!
//! Because the server records the **transport stream id** the `Subscribe`
//! arrived on (`Subscriber.quic_stream_id`), deliveries for a subscription
//! issued on a dedicated stream come back on that same stream — giving genuine
//! head-of-line blocking isolation between streams. Congestion or retransmission
//! on one dedicated stream never blocks another.

use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};

use crate::DeliveryPolicy;
use crate::connection::{ConnCmd, StreamSel};
use crate::error::PublishError;
use crate::message::Message;

/// Specification for a dedicated stream.
///
/// `policy` is mandatory; `topic` optionally scopes the stream's implicit
/// subscription to a single topic (when `None`, the stream subscribes to the
/// catch-all pattern `"*"`).
#[derive(Clone, Debug)]
pub struct StreamSpec {
    /// Per-stream egress policy declared to the server at `StreamOpen` time.
    pub policy: DeliveryPolicy,
    /// Optional single topic to subscribe on this stream (`None` ⇒ `"*"`).
    pub topic: Option<String>,
}

impl StreamSpec {
    /// Create a spec with the given policy and a catch-all subscription.
    #[must_use]
    pub fn new(policy: DeliveryPolicy) -> Self {
        Self {
            policy,
            topic: None,
        }
    }

    /// Scope this stream's subscription to a single topic.
    #[must_use]
    pub fn with_topic(mut self, topic: impl Into<String>) -> Self {
        self.topic = Some(topic.into());
        self
    }
}

/// Handle to a dedicated QUIC stream with its own delivery policy.
///
/// Produced by [`Client::open_stream`]. Cheap to clone is *not* supported —
/// the handle owns the message receiver. Share via a wrapping channel if
/// multiple consumers are needed.
///
/// [`Client::open_stream`]: crate::Client::open_stream
pub struct StreamHandle {
    /// Transport stream id of this dedicated stream.
    stream_id: u64,
    /// Receiver for frames delivered on `stream_id`.
    rx: mpsc::Receiver<Message>,
    /// Back-channel to the connection task (for `publish`).
    cmd_tx: mpsc::Sender<ConnCmd>,
    /// Mirror of the transport's pending-bytes counter (shared with the
    /// connection task). Read by [`Self::pending_bytes`] for backpressure.
    pending_shared: Arc<std::sync::atomic::AtomicUsize>,
}

impl StreamHandle {
    /// Construct a handle from the connection task.
    #[must_use]
    pub(crate) fn new(
        stream_id: u64,
        rx: mpsc::Receiver<Message>,
        cmd_tx: mpsc::Sender<ConnCmd>,
        pending_shared: Arc<std::sync::atomic::AtomicUsize>,
    ) -> Self {
        Self {
            stream_id,
            rx,
            cmd_tx,
            pending_shared,
        }
    }

    /// The transport stream id backing this handle.
    #[inline]
    #[must_use]
    pub fn stream_id(&self) -> u64 {
        self.stream_id
    }

    /// Total bytes buffered in `Transport::pending` across ALL streams
    /// on this client's connection (not just this stream). Non-zero
    /// means the subscriber is falling behind and the server has
    /// stopped accepting new data on some streams.
    ///
    /// Publishers can check this before `try_publish` to apply early
    /// backpressure — yielding briefly when the value is high keeps the
    /// in-flight gap small enough that `close()` can drain within its
    /// timeout.
    #[must_use]
    pub fn pending_bytes(&self) -> usize {
        self.pending_shared
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Await the next message delivered on this stream, or `None` when the
    /// stream or connection has been closed.
    #[inline]
    pub async fn recv(&mut self) -> Option<Message> {
        self.rx.recv().await
    }

    /// Publish on this dedicated stream (uses the stream's declared policy on
    /// the egress side to matching subscribers).
    ///
    /// This routes the publish through the same connection task as
    /// [`Client::publish`](crate::Client::publish), but tags it with the
    /// stream selector for this dedicated stream so it leaves on this
    /// stream's id.
    pub async fn publish(
        &self,
        topic: &str,
        payload: impl crate::message::Payload,
    ) -> Result<(), PublishError> {
        let payload: Bytes = payload.into_bytes();
        let (resp_tx, resp_rx) = oneshot::channel();
        let cmd = ConnCmd::Publish {
            topic: topic.to_string(),
            payload,
            stream: StreamSel::Dedicated(self.stream_id),
            resp: resp_tx,
        };
        self.cmd_tx
            .send(cmd)
            .await
            .map_err(|_| PublishError::NotConnected)?;
        resp_rx.await.map_err(|_| PublishError::NotConnected)?
    }

    /// Fire-and-forget publish on this dedicated stream — enqueues the
    /// frame without waiting for the connection task to confirm.
    ///
    /// Avoids the per-publish oneshot round-trip (~3 ms scheduler latency)
    /// that `publish().await` costs. On a full command channel, returns
    /// `Err(NotConnected)` — the caller should `yield_now().await` and
    /// retry, or use `publish().await` for built-in backpressure.
    ///
    /// Because each dedicated stream has its own QUIC flow-control budget,
    /// concurrent `try_publish` calls on different `StreamHandle`s do NOT
    /// block each other at the transport layer.
    ///
    /// # Errors
    /// [`PublishError::NotConnected`] if the connection is gone or the
    /// command channel is full.
    pub fn try_publish(
        &self,
        topic: &str,
        payload: impl crate::message::Payload,
    ) -> Result<(), PublishError> {
        let payload: Bytes = payload.into_bytes();
        let (resp_tx, _resp_rx) = oneshot::channel();
        self.cmd_tx
            .try_send(ConnCmd::Publish {
                topic: topic.to_string(),
                payload,
                stream: StreamSel::Dedicated(self.stream_id),
                resp: resp_tx,
            })
            .map_err(|_| PublishError::NotConnected)
    }
}
