//! Dedicated logical streams with per-stream delivery semantics.
//!
//! Opening a [`StreamHandle`] via [`crate::Client::open_stream`] allocates a
//! dedicated QUIC bidirectional stream and declares its [`DeliveryPolicy`] to
//! the server with a `StreamOpen` frame. The server records the policy for
//! that stream and applies it to every frame it fans out onto that stream.
//!
//! Because the server records the **transport stream id** the `Subscribe`
//! arrived on, deliveries for a subscription
//! issued on a dedicated stream come back on that same stream — giving genuine
//! head-of-line blocking isolation between streams. Congestion or retransmission
//! on one dedicated stream never blocks another.

use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};

use crate::DeliveryPolicy;
use crate::StreamPriority;
use crate::connection::{ConnCmd, StreamSel};
use crate::error::{PublishError, StreamError};
use crate::message::Message;

/// Content category for a dedicated stream.
///
/// Each variant maps to a sensible default [`DeliveryPolicy`] — call
/// `conn.open_stream(StreamType::Audio).await?` for the common case,
/// or build a full [`StreamSpec`] when you need fine-grained control.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum StreamType {
    /// Realtime audio — latency-sensitive, stale samples are useless.
    /// Default policy: [`DeliveryPolicy::RealtimeDropOld`].
    Audio,
    /// Realtime video — same reasoning as audio.
    /// Default policy: [`DeliveryPolicy::RealtimeDropOld`].
    Video,
    /// Chat / text — messages must arrive in order and complete.
    /// Default policy: [`DeliveryPolicy::ReliableOrdered`].
    Text,
    /// State mutations / presence / config — must not be lost.
    /// Default policy: [`DeliveryPolicy::ReliableOrdered`].
    Event,
    /// AI token streams / LLM responses — ordered, lossless.
    /// Default policy: [`DeliveryPolicy::ReliableOrdered`].
    Ai,
    /// Bulk file transfer — ordered not required, lossless.
    /// Default policy: [`DeliveryPolicy::ReliableUnordered`].
    File,
    /// User-defined — caller sets the policy explicitly.
    /// Default policy: [`DeliveryPolicy::ReliableOrdered`].
    Custom,
}

impl StreamType {
    /// Returns the default [`DeliveryPolicy`] for this stream type.
    #[inline]
    #[must_use]
    pub const fn default_policy(self) -> DeliveryPolicy {
        match self {
            Self::Audio | Self::Video => DeliveryPolicy::RealtimeDropOld,
            Self::Text | Self::Event | Self::Ai | Self::Custom => DeliveryPolicy::ReliableOrdered,
            Self::File => DeliveryPolicy::ReliableUnordered,
        }
    }

    /// Returns the default [`StreamPriority`] for this stream type.
    ///
    /// Decoupled from [`default_policy`](Self::default_policy): a `Text`
    /// stream and an `Ai` stream share `ReliableOrdered` policy but may
    /// still differ in urgency. Callers can override via
    /// [`StreamSpec::with_priority`].
    #[inline]
    #[must_use]
    pub const fn default_priority(self) -> StreamPriority {
        match self {
            Self::Audio => StreamPriority::Critical,
            Self::Video | Self::Event => StreamPriority::High,
            Self::Text | Self::Ai | Self::Custom => StreamPriority::Normal,
            Self::File => StreamPriority::Low,
        }
    }
}

/// `conn.open_stream(StreamType::Audio)` — uses the type's default policy
/// and priority.
impl From<StreamType> for StreamSpec {
    #[inline]
    fn from(t: StreamType) -> Self {
        Self {
            policy: t.default_policy(),
            priority: t.default_priority(),
            topic: None,
            stream_type: t,
        }
    }
}

/// Specification for a dedicated stream.
///
/// `policy` is mandatory; `topic` optionally scopes the stream's implicit
/// subscription to a single topic (when `None`, the stream subscribes to the
/// catch-all pattern `"*"`).
#[derive(Clone, Debug)]
pub struct StreamSpec {
    /// Per-stream egress policy declared to the server at `StreamOpen` time.
    pub policy: DeliveryPolicy,
    /// Per-stream egress priority declared to the server at `StreamOpen`
    /// time. Decoupled from `policy` — a Critical audio stream and a Low
    /// file-sync stream may share a realtime policy or diverge freely.
    pub priority: StreamPriority,
    /// Optional single topic to subscribe on this stream (`None` ⇒ `"*"`).
    pub topic: Option<String>,
    /// Content category — informational only, does not affect the wire
    /// protocol (policy is what the server uses). Defaults to `Custom`.
    pub stream_type: StreamType,
}

impl StreamSpec {
    /// Create a spec with the given policy, a catch-all subscription, and
    /// the default priority ([`StreamPriority::Normal`]).
    #[must_use]
    pub fn new(policy: DeliveryPolicy) -> Self {
        Self {
            policy,
            priority: StreamPriority::Normal,
            topic: None,
            stream_type: StreamType::Custom,
        }
    }

    /// Create a spec from a [`StreamType`], using its default policy and
    /// default priority.
    #[must_use]
    pub fn typed(stream_type: StreamType) -> Self {
        Self {
            policy: stream_type.default_policy(),
            priority: stream_type.default_priority(),
            topic: None,
            stream_type,
        }
    }

    /// Scope this stream's subscription to a single topic.
    #[must_use]
    pub fn with_topic(mut self, topic: impl Into<String>) -> Self {
        self.topic = Some(topic.into());
        self
    }

    /// Override the egress priority. By default the priority comes from
    /// the [`StreamType`] (see [`StreamType::default_priority`]); use this
    /// to promote or demote an individual stream.
    #[must_use]
    pub fn with_priority(mut self, priority: StreamPriority) -> Self {
        self.priority = priority;
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

    /// Non-blocking receive. Returns `Some(msg)` if a message is buffered in
    /// the channel, or `None` if the channel is empty (or closed).
    ///
    /// Use after `recv().await` to drain additional buffered messages without
    /// paying the async scheduler overhead per message.
    #[inline]
    pub fn try_recv(&mut self) -> Option<Message> {
        self.rx.try_recv().ok()
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

    /// Gracefully close this stream. Consumes `self` — the handle is no
    /// longer usable after closing. Sends a `StreamClose` frame so the
    /// server tears down its per-stream state immediately.
    ///
    /// This is also called automatically on `Drop`, but calling `close()`
    /// explicitly lets you observe errors.
    pub async fn close(self) -> Result<(), StreamError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let cmd = ConnCmd::CloseStream {
            stream_id: self.stream_id,
            resp: resp_tx,
        };
        let _ = self.cmd_tx.send(cmd).await;
        // Mark as closed so Drop doesn't double-send.
        std::mem::forget(self);
        resp_rx.await.map_err(|_| StreamError::Closed)?
    }

    /// Abruptly reset this stream with a reason code. The server tears
    /// down its per-stream send policy. The handle remains usable for
    /// receiving any already-buffered messages.
    pub async fn reset(&self, reason: u32) -> Result<(), StreamError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let cmd = ConnCmd::ResetStream {
            stream_id: self.stream_id,
            reason,
            resp: resp_tx,
        };
        self.cmd_tx
            .send(cmd)
            .await
            .map_err(|_| StreamError::Closed)?;
        resp_rx.await.map_err(|_| StreamError::Closed)?
    }

    /// Pause delivery on this stream. The server stops sending new frames
    /// but keeps buffering for reliable streams (the gap is replayed on
    /// resume). For realtime streams, frames during pause are dropped.
    pub async fn pause(&self) -> Result<(), StreamError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let cmd = ConnCmd::PauseStream {
            stream_id: self.stream_id,
            resp: resp_tx,
        };
        self.cmd_tx
            .send(cmd)
            .await
            .map_err(|_| StreamError::Closed)?;
        resp_rx.await.map_err(|_| StreamError::Closed)?
    }

    /// Resume delivery on a previously-paused stream. Reliable streams
    /// receive the buffered gap; realtime streams resume from now.
    pub async fn resume(&self) -> Result<(), StreamError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let cmd = ConnCmd::ResumeStream {
            stream_id: self.stream_id,
            resp: resp_tx,
        };
        self.cmd_tx
            .send(cmd)
            .await
            .map_err(|_| StreamError::Closed)?;
        resp_rx.await.map_err(|_| StreamError::Closed)?
    }
}

/// On drop, fire-and-forget a `StreamClose` so the server cleans up
/// even if the caller forgets to call [`StreamHandle::close`].
impl Drop for StreamHandle {
    fn drop(&mut self) {
        let (resp_tx, _resp_rx) = oneshot::channel();
        let _ = self.cmd_tx.try_send(ConnCmd::CloseStream {
            stream_id: self.stream_id,
            resp: resp_tx,
        });
    }
}
