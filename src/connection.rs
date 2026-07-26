//! The connection task, command channel, and subscriber routing.
//!
//! [`Client`] is a cheap, [`Clone`] handle holding only the command-channel
//! sender. All real work happens in the background [`run`] task, which owns the
//! [`Transport`] (and therefore the `!Sync` `quiche::Connection`) and a routing
//! table that demultiplexes inbound frames to subscriptions / dedicated
//! streams.
//!
//! ## Inbound routing
//!
//! The server records the **transport stream id** a `Subscribe` arrives on
//! (`application-layer/pubsub-engine/src/registry.rs:35` — `Subscriber.quic_stream_id`)
//! and fans matching publishes back onto that same stream. So:
//!
//! * default-channel subscriptions share QUIC stream 0 — inbound frames there
//!   are matched against the local subscription patterns and fanned out;
//! * dedicated streams ([`StreamHandle`]) receive frames purely by stream id —
//!   no pattern matching, which is what gives them head-of-line isolation.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use frame::codec::FrameHeader;
use frame::header::{MessageType, Seq, StreamId};
use send_policy::StreamOpenMeta;
use tokio::sync::{mpsc, oneshot};

use crate::config::ClientConfig;
use crate::error::{ConnectError, PublishError, RpcError, StreamError, SubscribeError};
use crate::message::Message;
use crate::pubsub::Subscription;
use crate::stream::{StreamHandle, StreamSpec};
use crate::transport::Transport;
use crate::DeliveryPolicy;

/// Maximum commands processed per outer-loop iteration. Without this cap,
/// a `try_publish` flood monopolises the worker thread inside the batch
/// drain loop, starving other tasks (notably the subscriber's connection
/// task on the same tokio worker).
const MAX_CMD_BATCH: usize = 64;

/// Monotonic correlation-id source for `Client::rpc`. Starts at 1 so 0 is
/// reserved as "no cid assigned" (defensive — never appears on the wire
/// because the counter is incremented before use).
static RPC_CID_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Allocate the next 128-bit RPC correlation id.
///
/// Embeds the AtomicU64 counter in the low 64 bits; the high 64 bits are
/// zero (reserved for a future per-connection epoch to harden against
/// cross-reconnect collisions). u64 overflow at ~1.8×10¹⁹ calls per
/// process is not a concern in practice.
fn next_rpc_cid() -> u128 {
    let n = RPC_CID_COUNTER.fetch_add(1, Ordering::Relaxed);
    n as u128
}

/// QUIC stream id used for the shared default pub/sub channel. The first
/// client-initiated bidirectional stream is 0; dedicated streams start at
/// [`FIRST_DEDICATED_STREAM`] and advance by 4.
const DEFAULT_STREAM: u64 = 0;

/// First dedicated bidirectional stream id (client-initiated bidi ids are
/// `0, 4, 8, …` — bit 0 = bidirectional, bit 1 = client-initiated).
const FIRST_DEDICATED_STREAM: u64 = 4;

/// Which transport stream a publish leaves on.
#[derive(Clone, Copy, Debug)]
pub(crate) enum StreamSel {
    /// The shared default pub/sub stream (0).
    Default,
    /// A dedicated stream previously opened via [`Client::open_stream`].
    ///
    /// [`Client::open_stream`]: crate::Client::open_stream
    Dedicated(u64),
}

/// Commands sent from [`Client`] handles to the background task.
pub(crate) enum ConnCmd {
    /// Publish `payload` to `topic` on the selected stream.
    Publish {
        topic: String,
        payload: Bytes,
        stream: StreamSel,
        resp: oneshot::Sender<Result<(), PublishError>>,
    },
    /// Subscribe to a pattern on the default channel.
    Subscribe {
        pattern: String,
        qos: u8,
        resp: oneshot::Sender<Result<Subscription, SubscribeError>>,
    },
    /// Remove a previously-registered subscription.
    Unsubscribe {
        pattern: String,
        resp: oneshot::Sender<Result<(), SubscribeError>>,
    },
    /// Open a dedicated stream with a delivery policy.
    OpenStream {
        spec: StreamSpec,
        resp: oneshot::Sender<Result<StreamHandle, StreamError>>,
    },
    /// Register a pending RPC reply handler.
    ///
    /// The connection task (1) lazily subscribes to `reply_topic` if not
    /// already subscribed, (2) stores `cid → resp` in `pending_rpcs` so
    /// that the next inbound publish on the reply topic whose payload
    /// starts with `cid` (16-byte BE u128) is routed to this oneshot
    /// with the cid prefix stripped.
    RegisterRpcReply {
        cid: u128,
        reply_topic: String,
        resp: oneshot::Sender<Message>,
    },
    /// Cancel a pending RPC reply handler (drop without delivering).
    ///
    /// Sent by `Client::rpc` when the caller's timeout elapses or the
    /// receiver is dropped. Removes the entry from `pending_rpcs`.
    CancelRpc { cid: u128 },
    /// Close the connection.
    Close {
        resp: oneshot::Sender<Result<(), ConnectError>>,
    },
}

/// A cloneable handle to a Vireon connection.
///
/// Construct via [`crate::ClientBuilder::connect`]. Dropping the last clone
/// causes the background task to exit and the connection to close.
#[derive(Clone)]
pub struct Client {
    tx: mpsc::Sender<ConnCmd>,
    /// Mirror of the transport's pending-bytes counter. Updated by the
    /// connection task on every `stream_send` / `flush_pending`; read by
    /// publishers via [`Self::pending_bytes`] to detect QUIC flow-control
    /// backpressure from the subscriber before the cmd channel fills.
    pending_shared: Arc<std::sync::atomic::AtomicUsize>,
}

impl Client {
    /// Construct the public handle. Called by [`crate::ClientBuilder::connect`].
    #[must_use]
    pub(crate) fn new(
        tx: mpsc::Sender<ConnCmd>,
        pending_shared: Arc<std::sync::atomic::AtomicUsize>,
    ) -> Self {
        Self { tx, pending_shared }
    }

    /// Total bytes buffered in `Transport::pending` awaiting a QUIC
    /// flow-control window from the peer. Non-zero means the subscriber
    /// is falling behind and the server has stopped accepting new data
    /// on the affected streams.
    ///
    /// Publishers can check this before `try_publish` to apply
    /// early backpressure — yielding briefly when the value is high
    /// keeps the in-flight gap small enough that `close()` can drain
    /// within its timeout.
    #[must_use]
    pub fn pending_bytes(&self) -> usize {
        self.pending_shared.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Subscribe to a topic pattern on the default channel.
    ///
    /// `pattern` may use `*` as a single-segment wildcard (`"sensor.*"` matches
    /// `"sensor.temp"`). The returned [`Subscription`] yields every message
    /// whose topic matches.
    ///
    /// # Errors
    /// [`SubscribeError::NotConnected`] if the connection is gone.
    pub async fn subscribe(&self, pattern: &str) -> Result<Subscription, SubscribeError> {
        self.subscribe_with_qos(pattern, crate::message::Qos::default()).await
    }

    /// Subscribe with an explicit QoS byte.
    ///
    /// # Errors
    /// See [`Client::subscribe`].
    ///
    /// [`Client::subscribe`]: Self::subscribe
    pub async fn subscribe_with_qos(
        &self,
        pattern: &str,
        qos: crate::message::Qos,
    ) -> Result<Subscription, SubscribeError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let cmd = ConnCmd::Subscribe {
            pattern: pattern.to_string(),
            qos: qos.as_byte(),
            resp: resp_tx,
        };
        self.tx
            .send(cmd)
            .await
            .map_err(|_| SubscribeError::NotConnected)?;
        resp_rx.await.map_err(|_| SubscribeError::NotConnected)?
    }

    /// Remove the first subscription whose pattern equals `pattern` exactly.
    ///
    /// # Errors
    /// [`SubscribeError::NotConnected`] if the connection is gone.
    pub async fn unsubscribe(&self, pattern: &str) -> Result<(), SubscribeError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let cmd = ConnCmd::Unsubscribe {
            pattern: pattern.to_string(),
            resp: resp_tx,
        };
        self.tx
            .send(cmd)
            .await
            .map_err(|_| SubscribeError::NotConnected)?;
        resp_rx.await.map_err(|_| SubscribeError::NotConnected)?
    }

    /// Publish `payload` to `topic` on the default channel.
    ///
    /// # Errors
    /// [`PublishError::NotConnected`] if the connection is gone, or
    /// [`PublishError::TooLarge`] if the payload exceeds the configured cap.
    pub async fn publish(&self, topic: &str, payload: impl crate::message::Payload) -> Result<(), PublishError> {
        let payload: Bytes = payload.into_bytes();
        let (resp_tx, resp_rx) = oneshot::channel();
        let cmd = ConnCmd::Publish {
            topic: topic.to_string(),
            payload,
            stream: StreamSel::Default,
            resp: resp_tx,
        };
        self.tx
            .send(cmd)
            .await
            .map_err(|_| PublishError::NotConnected)?;
        resp_rx.await.map_err(|_| PublishError::NotConnected)?
    }

    /// Publish with bounded auto-retry on transient errors.
    ///
    /// Retries [`PublishError::NotConnected`] up to `max_attempts` times
    /// with exponential backoff (`initial_backoff * 2^attempt`, capped at
    /// `max_backoff`). Useful when the connection is mid-reconnect: the
    /// application can fire-and-forget instead of wiring its own retry
    /// loop. Non-transient errors (`TooLarge`, `EncodingFailed`) short-
    /// circuit on the first attempt.
    ///
    /// Total elapsed time is bounded by `max_attempts * max_backoff`.
    /// Set `max_attempts = 0` for "try once, no retry" (equivalent to
    /// [`Client::publish`]).
    ///
    /// # Errors
    /// The last [`PublishError`] encountered. `NotConnected` if every
    /// attempt failed during a reconnect window.
    pub async fn publish_with_retries(
        &self,
        topic: &str,
        payload: impl crate::message::Payload,
        max_attempts: u32,
        initial_backoff: Duration,
        max_backoff: Duration,
    ) -> Result<(), PublishError> {
        // Encode once; reuse the Bytes on every attempt (cheap clone).
        let payload: Bytes = payload.into_bytes();
        let mut last_err = PublishError::NotConnected;
        for attempt in 0..=max_attempts {
            match self.publish(topic, payload.clone()).await {
                Ok(()) => return Ok(()),
                Err(PublishError::NotConnected) => {
                    last_err = PublishError::NotConnected;
                    if attempt == max_attempts {
                        break;
                    }
                    let backoff = backoff_for(attempt, initial_backoff, max_backoff);
                    tokio::time::sleep(backoff).await;
                    continue;
                }
                // Non-transient — surface immediately.
                Err(other) => return Err(other),
            }
        }
        Err(last_err)
    }

    /// Request/reply RPC over the default pub/sub channel.
    ///
    /// Publishes `payload` to `request_topic` and awaits the first reply
    /// published to `reply_topic` with a matching correlation id, returning
    /// it as a [`Message`] whose payload has the cid prefix stripped.
    ///
    /// ## Wire convention
    ///
    /// The request payload is framed as `[cid: 16-byte BE u128][app_payload]`.
    /// The responder MUST publish its reply to `reply_topic` with the same
    /// `[cid: 16-byte BE u128][reply_payload]` layout. Replies whose first
    /// 16 bytes do not match a pending cid are delivered to normal
    /// subscribers on `reply_topic` (i.e. the RPC layer never disturbs
    /// ordinary pub/sub traffic on the reply topic).
    ///
    /// The connection task lazily subscribes to `reply_topic` on first use
    /// and stays subscribed for the connection's lifetime — repeated `rpc`
    /// calls reuse the same subscription.
    ///
    /// ## Errors
    ///
    /// - [`RpcError::Timeout`] if no reply arrives within `timeout`.
    /// - [`RpcError::NotConnected`] if the connection drops while waiting.
    /// - [`RpcError::SubscribeFailed`] / [`RpcError::PublishFailed`] propagate
    ///   the underlying pub/sub errors.
    ///
    /// # Errors
    /// See [`RpcError`].
    pub async fn rpc(
        &self,
        request_topic: &str,
        payload: impl crate::message::Payload,
        reply_topic: &str,
        timeout: Duration,
    ) -> Result<Message, RpcError> {
        let app_payload: Bytes = payload.into_bytes();
        let cid = next_rpc_cid();

        // Register the reply handler before publishing so the connection
        // task is guaranteed to be ready to intercept the reply even if
        // the responder is faster than the scheduler round-trip.
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(ConnCmd::RegisterRpcReply {
                cid,
                reply_topic: reply_topic.to_string(),
                resp: tx,
            })
            .await
            .map_err(|_| RpcError::NotConnected)?;

        // Frame the request: `[cid BE][app_payload]`.
        let mut framed = Vec::with_capacity(16 + app_payload.len());
        framed.extend_from_slice(&cid.to_be_bytes());
        framed.extend_from_slice(&app_payload);
        if let Err(e) = self.publish(request_topic, framed).await {
            // Best-effort cancel to free the slot; ignore send error since
            // the connection is already failing.
            let _ = self.tx.try_send(ConnCmd::CancelRpc { cid });
            return Err(RpcError::PublishFailed(e));
        }

        // Await reply with timeout.
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(msg)) => Ok(msg),
            Ok(Err(_)) => Err(RpcError::NotConnected),
            Err(_) => {
                let _ = self.tx.try_send(ConnCmd::CancelRpc { cid });
                Err(RpcError::Timeout(timeout))
            }
        }
    }

    /// Fire-and-forget publish — enqueues the frame without waiting for the
    /// connection task to confirm.  This avoids the per-publish oneshot
    /// round-trip (which costs ~2 ms of scheduler latency) and is the right
    /// choice for high-throughput publishing where the caller does not need
    /// per-message confirmation.
    ///
    /// Returns `Err(NotConnected)` only when the command channel is full or
    /// the connection task has exited.  The actual frame encode + send
    /// happens asynchronously in the background.
    ///
    /// # Errors
    /// [`PublishError::NotConnected`] if the connection is gone or the
    /// command channel is full.
    pub fn try_publish(&self, topic: &str, payload: impl crate::message::Payload) -> Result<(), PublishError> {
        let payload: Bytes = payload.into_bytes();
        let (resp_tx, _resp_rx) = oneshot::channel();
        let cmd = ConnCmd::Publish {
            topic: topic.to_string(),
            payload,
            stream: StreamSel::Default,
            resp: resp_tx,
        };
        self.tx
            .try_send(cmd)
            .map_err(|_| PublishError::NotConnected)
    }

    /// Open a dedicated stream with its own delivery policy and (optional)
    /// single-topic subscription. See [`StreamSpec`].
    ///
    /// # Errors
    /// [`StreamError::NotConnected`] if the connection is gone.
    pub async fn open_stream(&self, spec: StreamSpec) -> Result<StreamHandle, StreamError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let cmd = ConnCmd::OpenStream { spec, resp: resp_tx };
        self.tx
            .send(cmd)
            .await
            .map_err(|_| StreamError::NotConnected)?;
        resp_rx.await.map_err(|_| StreamError::NotConnected)?
    }

    /// Close the connection and stop the background task.
    ///
    /// # Errors
    /// [`ConnectError::ClientClosed`] if already closed.
    pub async fn close(&self) -> Result<(), ConnectError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let cmd = ConnCmd::Close { resp: resp_tx };
        self.tx
            .send(cmd)
            .await
            .map_err(|_| ConnectError::Closed("client already closed".into()))?;
        resp_rx
            .await
            .map_err(|_| ConnectError::Closed("connection task exited".into()))?
    }
}

/// Exponential backoff for the `attempt`-th retry (0-indexed):
/// `initial * 2^attempt`, clamped to `max`.
///
/// Mirrors [`ReconnectPolicy::backoff_for`](crate::config::ReconnectPolicy::backoff_for)
/// but as a free function so [`Client::publish_with_retries`] can compute a
/// schedule without constructing a full `ReconnectPolicy`.
fn backoff_for(attempt: u32, initial: Duration, max: Duration) -> Duration {
    let shift = attempt.min(31);
    let initial_ms = initial.as_millis() as u64;
    let base = initial_ms.saturating_mul(1u64 << shift);
    let max_ms = max.as_millis().max(1) as u64;
    Duration::from_millis(base.min(max_ms))
}

// ── the background task ─────────────────────────────────────────────

/// Connection-task state: subscriber routing tables and stream allocation.
struct TaskState {
    /// Default-channel subscriptions: pattern + qos + message sink.
    subs: Vec<SubEntry>,
    /// Dedicated streams keyed by transport stream id.
    streams: HashMap<u64, StreamEntry>,
    /// Next dedicated bidirectional stream id.
    next_bidi: u64,
    /// Depth for newly-created subscriber channels.
    subscriber_buffer: usize,
    /// Per-stream outgoing sequence counter.
    seqs: HashMap<u64, u64>,
    /// Max payload size (defensive).
    max_message_size: usize,
    /// Back-reference to the command channel, embedded in `StreamHandle`s.
    cmd_tx: mpsc::Sender<ConnCmd>,
    /// Mirror of the transport's pending-bytes counter, embedded in
    /// `StreamHandle`s so publishers can observe flow-control backpressure.
    pending_shared: Arc<AtomicUsize>,
    /// Pending response for a Close command — sent after the graceful
    /// drain completes so `Client::close()` returns only after all
    /// in-flight publishes have been flushed (or the drain timed out).
    close_resp: Option<oneshot::Sender<Result<(), ConnectError>>>,
    /// Aggregate drop count for the default channel. Logged every
    /// `DROP_LOG_INTERVAL`-th drop instead of per-message — the
    /// per-drop WARN drowned server logs under sustained overload.
    default_drops: u64,
    /// Aggregate drop count for dedicated streams. Same rationale.
    stream_drops: u64,
    /// Pending RPC reply handlers keyed by 16-byte correlation id.
    /// Populated by `RegisterRpcReply`; drained when a matching inbound
    /// publish arrives on a `rpc_reply_topics` topic, or by `CancelRpc`
    /// (sent on caller timeout / drop).
    pending_rpcs: HashMap<u128, oneshot::Sender<Message>>,
    /// Reply topics the task has already sent a Subscribe frame for.
    /// Membership means: "inbound publishes on this topic MAY be RPC
    /// replies — peek the first 16 payload bytes for a matching cid
    /// before normal fan-out."
    rpc_reply_topics: HashSet<String>,
}

/// Log drop counters every Nth drop. Chosen so a brief overload
/// typically yields 1 summary line, not thousands.
const DROP_LOG_INTERVAL: u64 = 1024;

/// One default-channel subscription, retained for reconnect replay.
struct SubEntry {
    pattern: String,
    qos: u8,
    tx: mpsc::Sender<Message>,
}

/// One dedicated-stream registration, retained for reconnect replay.
struct StreamEntry {
    policy: DeliveryPolicy,
    topic: Option<String>,
    tx: mpsc::Sender<Message>,
}

impl TaskState {
    fn new(
        subscriber_buffer: usize,
        max_message_size: usize,
        cmd_tx: mpsc::Sender<ConnCmd>,
        pending_shared: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            subs: Vec::new(),
            streams: HashMap::new(),
            next_bidi: FIRST_DEDICATED_STREAM,
            subscriber_buffer,
            seqs: HashMap::new(),
            max_message_size,
            cmd_tx,
            pending_shared,
            close_resp: None,
            default_drops: 0,
            stream_drops: 0,
            pending_rpcs: HashMap::new(),
            rpc_reply_topics: HashSet::new(),
        }
    }

    /// Next outgoing sequence number for `stream_id`.
    fn next_seq(&mut self, stream_id: u64) -> Seq {
        let e = self.seqs.entry(stream_id).or_insert(0);
        let s = *e;
        *e = e.wrapping_add(1);
        Seq::new(s)
    }

    /// Route a decoded inbound frame to the right subscriber(s).
    fn dispatch(&mut self, sid: u64, frame: frame::codec::Frame) {
        if frame.msg_type != MessageType::Publish {
            // Subscribe/Unsubscribe acks and other control frames are not
            // surfaced to users in v1. Log at debug for diagnostics.
            tracing::debug!(
                stream_id = sid,
                msg_type = ?frame.msg_type,
                "[client] ignoring non-publish inbound frame"
            );
            return;
        }

        // Zero-copy parse of `topic_len:u16 + topic + payload` from frame.payload.
        let payload: Bytes = frame.payload;
        if payload.len() < 2 {
            tracing::warn!(stream_id = sid, "[client] publish payload truncated");
            return;
        }
        let topic_len = u16::from_be_bytes([payload[0], payload[1]]) as usize;
        if payload.len() < 2 + topic_len {
            tracing::warn!(stream_id = sid, "[client] publish topic truncated");
            return;
        }
        let topic_bytes = payload.slice(2..2 + topic_len);
        let body = payload.slice(2 + topic_len..);

        let msg = Message {
            topic: topic_bytes.clone(),
            payload: body,
            seq: frame.seq.get(),
            stream_id: sid,
        };

        // ── RPC reply interceptor ──────────────────────────────────────
        //
        // Only inspects inbound publishes when ALL three conditions hold:
        //   1. There is at least one pending RPC (cheap empty check first
        //      to short-circuit when no RPC is in flight — zero overhead
        //      on normal pub/sub traffic).
        //   2. This topic is registered as a reply topic (precise routing:
        //      a stray 16-byte-prefix collision on an unrelated topic is
        //      never intercepted).
        //   3. Payload is at least 16 bytes (the cid prefix).
        //
        // On match: strip the 16-byte cid, deliver to the oneshot, and
        // skip normal fan-out. On no-match: fall through unchanged.
        if !self.pending_rpcs.is_empty()
            && !self.rpc_reply_topics.is_empty()
            && msg.payload.len() >= 16
        {
            let topic_str = String::from_utf8_lossy(&topic_bytes);
            if self.rpc_reply_topics.contains(topic_str.as_ref()) {
                let mut cid_bytes = [0u8; 16];
                cid_bytes.copy_from_slice(&msg.payload[..16]);
                let cid = u128::from_be_bytes(cid_bytes);
                if let Some(tx) = self.pending_rpcs.remove(&cid) {
                    let stripped = Message {
                        topic: msg.topic.clone(),
                        payload: msg.payload.slice(16..),
                        seq: msg.seq,
                        stream_id: msg.stream_id,
                    };
                    // Receiver dropped (caller cancelled/panicked): the
                    // entry is already removed above, so the leak is
                    // bounded by `CancelRpc` cleanup on the timeout path.
                    let _ = tx.send(stripped);
                    return;
                }
            }
        }

        if sid == DEFAULT_STREAM {
            // Fan out to every default-channel subscription whose pattern matches.
            let topic_str = String::from_utf8_lossy(&topic_bytes);
            for sub in &self.subs {
                if pattern_matches(&sub.pattern, &topic_str) {
                    // try_send: never block the I/O task on a slow consumer.
                    // A full channel drops the oldest-by-policy message.
                    if let Err(mpsc::error::TrySendError::Full(_)) = sub.tx.try_send(msg.clone()) {
                        self.default_drops = self.default_drops.wrapping_add(1);
                        if self.default_drops % DROP_LOG_INTERVAL == 0 {
                            tracing::warn!(
                                pattern = %sub.pattern,
                                total_drops = self.default_drops,
                                "[client] subscriber channel full — dropped {DROP_LOG_INTERVAL} messages (backpressure)"
                            );
                        }
                    }
                }
            }
        } else {
            // Dedicated stream: deliver by stream id, no pattern matching.
            if let Some(entry) = self.streams.get(&sid) {
                if let Err(mpsc::error::TrySendError::Full(_)) = entry.tx.try_send(msg) {
                    self.stream_drops = self.stream_drops.wrapping_add(1);
                    if self.stream_drops % DROP_LOG_INTERVAL == 0 {
                        tracing::warn!(
                            stream_id = sid,
                            total_drops = self.stream_drops,
                            "[client] dedicated-stream channel full — dropped {DROP_LOG_INTERVAL} messages (backpressure)"
                        );
                    }
                }
            }
        }
    }

    /// Handle one command. Returns `true` if the task should exit after this.
    fn handle_cmd(&mut self, cmd: ConnCmd, transport: &mut Transport) -> bool {
        match cmd {
            ConnCmd::Publish { topic, payload, stream, resp } => {
                let out = self.handle_publish(&topic, &payload, stream, transport);
                let _ = resp.send(out);
                false
            }
            ConnCmd::Subscribe { pattern, qos, resp } => {
                let out = self.handle_subscribe(&pattern, qos, transport);
                let _ = resp.send(out);
                false
            }
            ConnCmd::Unsubscribe { pattern, resp } => {
                self.subs.retain(|s| s.pattern != pattern);
                let out = self.encode_and_send(MessageType::Unsubscribe, &encode_unsubscribe(&pattern), DEFAULT_STREAM, transport)
                    .map_err(|_| SubscribeError::NotConnected);
                let _ = resp.send(out);
                false
            }
            ConnCmd::OpenStream { spec, resp } => {
                let out = self.handle_open_stream(spec, transport);
                let _ = resp.send(out);
                false
            }
            ConnCmd::RegisterRpcReply { cid, reply_topic, resp } => {
                // Lazy-subscribe to the reply topic on first use. Once
                // joined, the topic is marked in `rpc_reply_topics` so the
                // dispatch interceptor routes matching replies. Idempotent
                // — repeated registrations for the same topic are free.
                if !self.rpc_reply_topics.contains(&reply_topic) {
                    let buf = encode_subscribe(&reply_topic, 0);
                    let header = FrameHeader::new(StreamId::new(DEFAULT_STREAM), MessageType::Subscribe)
                        .with_seq(self.next_seq(DEFAULT_STREAM));
                    if let Err(e) = self.encode_and_send_raw(header, &buf, DEFAULT_STREAM, transport) {
                        tracing::warn!(
                            reply_topic = %reply_topic,
                            error = %e,
                            "[client] rpc: failed to subscribe to reply topic"
                        );
                        // Don't register the handler if we couldn't sub —
                        // the caller's await on `resp` returns Closed when
                        // the task drops `resp` here.
                        return false;
                    }
                    if let Err(e) = transport.flush() {
                        tracing::warn!(error = %e, "[client] rpc: flush failed");
                    }
                    self.rpc_reply_topics.insert(reply_topic);
                }
                self.pending_rpcs.insert(cid, resp);
                false
            }
            ConnCmd::CancelRpc { cid } => {
                self.pending_rpcs.remove(&cid);
                false
            }
            ConnCmd::Close { resp } => {
                // Store the response — the main loop sends it after the
                // graceful drain completes. This ensures Client::close()
                // returns only after all pending writes are flushed.
                self.close_resp = Some(resp);
                true
            }
        }
    }

    fn handle_publish(
        &mut self,
        topic: &str,
        payload: &[u8],
        stream: StreamSel,
        transport: &mut Transport,
    ) -> Result<(), PublishError> {
        if payload.len() > self.max_message_size {
            return Err(PublishError::TooLarge(payload.len()));
        }
        let sid = match stream {
            StreamSel::Default => DEFAULT_STREAM,
            StreamSel::Dedicated(id) => id,
        };
        let buf = encode_publish(topic, payload);
        let header = FrameHeader::new(StreamId::new(sid), MessageType::Publish)
            .with_seq(self.next_seq(sid));
        // Bytes land in quiche's per-stream send buffer; the outer run() loop
        // flushes once per iteration. Skipping a per-publish flush here is what
        // makes batch publishes share a single UDP send syscall.
        transport
            .send_frame(header, &buf, sid)
            .map_err(|_| PublishError::NotConnected)?;
        Ok(())
    }

    fn handle_subscribe(
        &mut self,
        pattern: &str,
        qos: u8,
        transport: &mut Transport,
    ) -> Result<Subscription, SubscribeError> {
        let (tx, rx) = mpsc::channel(self.subscriber_buffer);
        self.subs.push(SubEntry {
            pattern: pattern.to_string(),
            qos,
            tx,
        });

        let buf = encode_subscribe(pattern, qos);
        let header = FrameHeader::new(StreamId::new(DEFAULT_STREAM), MessageType::Subscribe)
            .with_seq(self.next_seq(DEFAULT_STREAM));
        self.encode_and_send_raw(header, &buf, DEFAULT_STREAM, transport)
            .map_err(|_| SubscribeError::NotConnected)?;
        transport.flush().map_err(|_| SubscribeError::NotConnected)?;

        Ok(Subscription::new(rx))
    }

    fn handle_open_stream(
        &mut self,
        spec: StreamSpec,
        transport: &mut Transport,
    ) -> Result<StreamHandle, StreamError> {
        let sid = self.next_bidi;
        self.next_bidi = self.next_bidi.checked_add(4).ok_or(StreamError::NoCapacity)?;

        let (tx, rx) = mpsc::channel(self.subscriber_buffer);
        self.streams.insert(sid, StreamEntry {
            policy: spec.policy,
            topic: spec.topic.clone(),
            tx,
        });

        // 1) Declare the stream's delivery policy via StreamOpen. The payload
        //    MUST be the `StreamOpenMeta` byte — the server decodes exactly
        //    this at application.rs:2144. Do NOT use frame::payload::StreamOpen
        //    (different encoding: a direction byte the server ignores).
        let policy_bytes = StreamOpenMeta::new(spec.policy).encode();
        let header = FrameHeader::new(StreamId::new(sid), MessageType::StreamOpen)
            .with_seq(self.next_seq(sid));
        self.encode_and_send_raw(header, &policy_bytes, sid, transport)
            .map_err(|_| StreamError::NotConnected)?;

        // 2) Subscribe on this dedicated stream so the server routes matching
        //    publishes back onto `sid` (applying the declared policy).
        let pattern = spec.topic.as_deref().unwrap_or("*");
        let sub_buf = encode_subscribe(pattern, 0);
        let sub_header = FrameHeader::new(StreamId::new(sid), MessageType::Subscribe)
            .with_seq(self.next_seq(sid));
        self.encode_and_send_raw(sub_header, &sub_buf, sid, transport)
            .map_err(|_| StreamError::NotConnected)?;

        transport.flush().map_err(|_| StreamError::NotConnected)?;

        Ok(StreamHandle::new(sid, rx, self.cmd_tx.clone(), self.pending_shared.clone()))
    }

    /// Re-send every active subscription and re-open every dedicated stream
    /// on a freshly-established transport. Called by [`run`] after a successful
    /// reconnect so the server repopulates its subscriber registry + policy
    /// bindings without the application noticing the interruption.
    ///
    /// Per-stream sequence counters are reset to zero — the server assigns its
    /// own inbound seqs, and our outbound seqs are purely advisory on a new
    /// connection.
    fn replay_all(&mut self, transport: &mut Transport) {
        self.seqs.clear();

        // Snapshot the replay plan so we can freely mutate `self.seqs` while
        // iterating — borrowing `&self.subs` / `&self.streams` across a
        // `&mut self.next_seq` call would not pass the borrow checker.
        let sub_replay: Vec<(String, u8)> = self
            .subs
            .iter()
            .map(|s| (s.pattern.clone(), s.qos))
            .collect();
        let stream_replay: Vec<(u64, DeliveryPolicy, Option<String>)> = self
            .streams
            .iter()
            .map(|(sid, e)| (*sid, e.policy, e.topic.clone()))
            .collect();

        // Default-channel subscriptions.
        for (pattern, qos) in &sub_replay {
            let buf = encode_subscribe(pattern, *qos);
            let header = FrameHeader::new(StreamId::new(DEFAULT_STREAM), MessageType::Subscribe)
                .with_seq(self.next_seq(DEFAULT_STREAM));
            if let Err(e) = transport.send_frame(header, &buf, DEFAULT_STREAM) {
                tracing::warn!(
                    pattern = %pattern,
                    error = %e,
                    "[client] replay: subscribe send failed"
                );
                return;
            }
        }

        // Dedicated streams: StreamOpen (policy byte) + Subscribe.
        for (sid, policy, topic) in &stream_replay {
            let policy_bytes = StreamOpenMeta::new(*policy).encode();
            let h1 = FrameHeader::new(StreamId::new(*sid), MessageType::StreamOpen)
                .with_seq(self.next_seq(*sid));
            if let Err(e) = transport.send_frame(h1, &policy_bytes, *sid) {
                tracing::warn!(
                    stream_id = sid,
                    error = %e,
                    "[client] replay: StreamOpen send failed"
                );
                return;
            }
            let pattern = topic.as_deref().unwrap_or("*");
            let buf = encode_subscribe(pattern, 0);
            let h2 = FrameHeader::new(StreamId::new(*sid), MessageType::Subscribe)
                .with_seq(self.next_seq(*sid));
            if let Err(e) = transport.send_frame(h2, &buf, *sid) {
                tracing::warn!(
                    stream_id = sid,
                    error = %e,
                    "[client] replay: subscribe send failed"
                );
                return;
            }
        }

        if let Err(e) = transport.flush() {
            tracing::warn!(error = %e, "[client] replay: flush failed");
        }
    }

    fn encode_and_send_raw(
        &self,
        header: FrameHeader,
        payload: &[u8],
        sid: u64,
        transport: &mut Transport,
    ) -> Result<(), ConnectError> {
        transport.send_frame(header, payload, sid)
    }

    fn encode_and_send(
        &mut self,
        msg_type: MessageType,
        payload: &[u8],
        sid: u64,
        transport: &mut Transport,
    ) -> Result<(), ConnectError> {
        let header = FrameHeader::new(StreamId::new(sid), msg_type).with_seq(self.next_seq(sid));
        transport.send_frame(header, payload, sid)
    }
}

/// The connection-task entry point.
pub(crate) async fn run(
    cfg: ClientConfig,
    mut rx: mpsc::Receiver<ConnCmd>,
    cmd_tx: mpsc::Sender<ConnCmd>,
    pending_shared: Arc<AtomicUsize>,
    ready: oneshot::Sender<Result<(), ConnectError>>,
) {
    let mut transport = match Transport::connect(&cfg).await {
        Ok(mut t) => {
            // Wire the shared pending-bytes mirror into the transport
            // so publishers can observe flow-control backpressure.
            t.set_pending_shared(pending_shared.clone());
            // Connection is up; unblock ClientBuilder::connect.
            let _ = ready.send(Ok(()));
            t
        }
        Err(e) => {
            let _ = ready.send(Err(e));
            return;
        }
    };

    let mut state = TaskState::new(cfg.subscriber_buffer, cfg.max_message_size, cmd_tx, pending_shared.clone());

    // Dead-peer detection: quiche 0.22 has no built-in keepalive and ICMP
    // port-unreachable is not reliably surfaced on loopback. So we use a
    // dual approach:
    //
    // 1. Heartbeat: send a Subscribe every 1 s to elicit server ACKs.
    // 2. Idle check: if no datagram received for HEARTBEAT_TIMEOUT (3 s),
    //    force-close the quiche connection so the reconnect FSM fires.
    //
    // A live server ACKs every heartbeat, keeping last_recv fresh. A dead
    // server stops ACKing, and after 3 s the idle check fires.
    const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(3);
    let mut heartbeat = tokio::time::interval(Duration::from_secs(1));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Skip the immediate first tick (fires at t=0).
    heartbeat.tick().await;
    let mut last_recv = Instant::now();

    loop {
        // 1. Drain socket → quiche (may deliver MAX_STREAM_DATA that opens
        //    the flow-control window for previously-partial writes).
        if transport.drain_recv() {
            last_recv = Instant::now();
        }
        // 2. Retry any pending partial writes now that the window may have
        //    opened. Must run before decoding inbound frames and before
        //    flush so the byte stream stays ordered.
        transport.flush_pending();
        // 3. Decode + route any complete frames.
        transport.process_readable(|sid, frame| state.dispatch(sid, frame));
        // 4. Flush pending output.
        if let Err(e) = transport.flush() {
            tracing::warn!(error = %e, "[client] flush error");
        }

        // 4. Dead-peer detection: if no data received for HEARTBEAT_TIMEOUT,
        //    the server is presumed dead. Force-close so is_closed() returns
        //    true and the reconnect FSM fires.
        if !transport.is_closed() && last_recv.elapsed() > HEARTBEAT_TIMEOUT {
            tracing::info!(
                idle = ?last_recv.elapsed(),
                "[client] no data from server — closing for reconnect"
            );
            transport.close();
        }

        // 5. Connection teardown? Try reconnect per the configured policy.
        if transport.is_closed() {
            if let Some(new_t) = reconnect(&cfg, &mut rx).await {
                transport = new_t;
                last_recv = Instant::now();
                tracing::info!("[client] reconnected — replaying subscriptions");
                state.replay_all(&mut transport);
                continue;
            }
            tracing::info!("[client] connection closed; task exiting");
            break;
        }

        // 6. Wait for the next event: a command, socket readability, or a timer.
        let next = transport.next_event_deadline(None);
        tokio::select! {
            biased;
            _ = heartbeat.tick() => {
                // Dead-peer probe: send a Subscribe for a private topic on
                // the default stream. This generates outgoing QUIC traffic
                // that elicits an ACK from a live server. If no ACK arrives
                // within HEARTBEAT_TIMEOUT, the idle check below closes the
                // connection for reconnect.
                // Two-segment topic to satisfy the server's default ACL
                // (`allow("*.*", None, ALL)`).  The server sends a response
                // regardless of whether the subscribe is accepted or denied,
                // so this still elicits traffic for dead-peer detection.
                let buf = encode_subscribe("_heartbeat.probe", 0);
                let header = FrameHeader::new(StreamId::new(DEFAULT_STREAM), MessageType::Subscribe)
                    .with_seq(state.next_seq(DEFAULT_STREAM));
                if let Err(e) = transport.send_frame(header, &buf, DEFAULT_STREAM) {
                    tracing::warn!(error = %e, "[client] heartbeat send failed");
                }
            }
            cmd = rx.recv() => {
                match cmd {
                    Some(c) => {
                        let mut exit = state.handle_cmd(c, &mut transport);
                        // Drain additional commands already queued so they
                        // share the next flush — amortises the UDP send syscall.
                        // Cap at MAX_CMD_BATCH to avoid monopolising the worker
                        // thread when publishers flood the channel via
                        // `try_publish`; without the cap, a 15 k msg/s burst
                        // blocks the task for ~15 ms, starving the subscriber's
                        // connection task on the same worker.
                        let mut batch = 1;
                        while !exit && batch < MAX_CMD_BATCH {
                            match rx.try_recv() {
                                Ok(c) => {
                                    exit = state.handle_cmd(c, &mut transport);
                                    batch += 1;
                                }
                                Err(mpsc::error::TryRecvError::Empty) => break,
                                Err(mpsc::error::TryRecvError::Disconnected) => {
                                    transport.close();
                                    return;
                                }
                            }
                        }
                        if exit {
                            // Process any remaining commands enqueued BEFORE
                            // Close arrived. With a deep cmd channel, many
                            // Publish commands may still be queued; if we
                            // skip them, their data is lost. Each Publish
                            // lands in quiche's send buffer or Transport's
                            // pending — the subsequent drain flushes both.
                            while let Ok(cmd) = rx.try_recv() {
                                let _ = state.handle_cmd(cmd, &mut transport);
                            }
                            // Graceful drain: flush any partial-write tails
                            // buffered in Transport::pending before sending
                            // CONNECTION_CLOSE. Without this, frames buffered
                            // by stream_send (awaiting a flow-control window
                            // from the peer) would be silently dropped.
                            //
                            // 10 s covers the worst-case cmd-channel backlog:
                            // CAP=4096 × 64 KiB = 256 MiB, at ~35 MiB/s that's
                            // ~7 s. With small frames (8 KiB) pending is
                            // typically <1 MB and drain returns in <100 ms.
                            const DRAIN_TIMEOUT: Duration = Duration::from_secs(10);
                            if !transport.drain_pending_gracefully(DRAIN_TIMEOUT).await {
                                tracing::warn!(
                                    pending_bytes = transport.pending_bytes(),
                                    "[client] drain timed out — closing with unwritten data"
                                );
                            }
                            transport.close();
                            if let Some(resp) = state.close_resp.take() {
                                let _ = resp.send(Ok(()));
                            }
                            break;
                        }
                    }
                    None => {
                        // All Client handles dropped — drain pending writes
                        // before closing so in-flight publishes aren't lost.
                        const DRAIN_TIMEOUT: Duration = Duration::from_secs(10);
                        if !transport.drain_pending_gracefully(DRAIN_TIMEOUT).await {
                            tracing::warn!(
                                pending_bytes = transport.pending_bytes(),
                                "[client] drain timed out on drop — closing with unwritten data"
                            );
                        }
                        transport.close();
                        break;
                    }
                }
            }
            _ = transport.wait_for_event(next) => {}
        }
        // 7. If we slept past the quiche deadline, fire loss-recovery timers.
        if std::time::Instant::now() >= next {
            transport.fire_timeout();
        }
    }
}

/// Attempt to re-establish the connection per `cfg.reconnect`.
///
/// Loops up to `max_attempts` times with exponential backoff. Returns
/// `None` immediately if reconnect is disabled (`max_attempts == 0`) or
/// after all attempts are exhausted. While sleeping between attempts the
/// task still selects on the command channel so that dropping every
/// `Client` handle aborts reconnect quickly.
async fn reconnect(cfg: &ClientConfig, rx: &mut mpsc::Receiver<ConnCmd>) -> Option<Transport> {
    let policy = &cfg.reconnect;
    if policy.max_attempts == 0 {
        return None;
    }

    for attempt in 0..policy.max_attempts {
        let backoff = policy.backoff_for(attempt);
        tracing::info!(
            attempt = attempt + 1,
            max = policy.max_attempts,
            backoff_ms = backoff.as_millis(),
            "[client] reconnect: sleeping before retry"
        );

        // Sleep, but bail out if every Client handle is dropped mid-wait.
        tokio::select! {
            biased;
            // Drain any commands that arrive during the wait — we can't
            // service them on a dead transport, so respond NotConnected
            // and keep waiting. This keeps the caller from hanging on a
            // publish that can never succeed while reconnect is pending.
            cmd = rx.recv() => {
                match cmd {
                    Some(c) => fail_cmd_not_connected(c),
                    None => return None,
                }
            }
            _ = tokio::time::sleep(backoff) => {}
        }

        match Transport::connect(cfg).await {
            Ok(t) => {
                tracing::info!(
                    attempt = attempt + 1,
                    "[client] reconnect: established new connection"
                );
                return Some(t);
            }
            Err(e) => {
                tracing::warn!(
                    attempt = attempt + 1,
                    error = %e,
                    "[client] reconnect: attempt failed"
                );
            }
        }
    }
    None
}

/// Respond to a command with the appropriate "connection lost" error so the
/// caller doesn't block forever on a response that can never be produced
/// while the transport is down.
fn fail_cmd_not_connected(cmd: ConnCmd) {
    match cmd {
        ConnCmd::Publish { resp, .. } => {
            let _ = resp.send(Err(PublishError::NotConnected));
        }
        ConnCmd::Subscribe { resp, .. } => {
            let _ = resp.send(Err(SubscribeError::NotConnected));
        }
        ConnCmd::Unsubscribe { resp, .. } => {
            let _ = resp.send(Err(SubscribeError::NotConnected));
        }
        ConnCmd::OpenStream { resp, .. } => {
            let _ = resp.send(Err(StreamError::NotConnected));
        }
        ConnCmd::RegisterRpcReply { resp, .. } => {
            // Receiver awaits with a timeout — dropping `resp` here makes
            // the caller's oneshot resolve to `RpcError::NotConnected`.
            drop(resp);
        }
        ConnCmd::CancelRpc { .. } => {
            // Nothing to ack — CancelRpc is fire-and-forget cleanup.
        }
        ConnCmd::Close { resp } => {
            let _ = resp.send(Err(ConnectError::Closed(
                "close requested during reconnect backoff".into(),
            )));
        }
    }
}

// ── payload encoders (manual, unambiguous wire layout) ─────────────

/// `pattern_len(u16) + pattern + qos(u8)`
fn encode_subscribe(pattern: &str, qos: u8) -> Vec<u8> {
    let len = u16::try_from(pattern.len()).unwrap_or(u16::MAX);
    let mut buf = Vec::with_capacity(2 + pattern.len() + 1);
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(pattern.as_bytes());
    buf.push(qos);
    buf
}

/// `pattern_len(u16) + pattern`
fn encode_unsubscribe(pattern: &str) -> Vec<u8> {
    let len = u16::try_from(pattern.len()).unwrap_or(u16::MAX);
    let mut buf = Vec::with_capacity(2 + pattern.len());
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(pattern.as_bytes());
    buf
}

/// `topic_len(u16) + topic + opaque payload`
fn encode_publish(topic: &str, payload: &[u8]) -> Vec<u8> {
    let len = u16::try_from(topic.len()).unwrap_or(u16::MAX);
    let mut buf = Vec::with_capacity(2 + topic.len() + payload.len());
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(topic.as_bytes());
    buf.extend_from_slice(payload);
    buf
}

// ── pattern matching (mirrors the server's segment-based wildcard) ──

/// `true` when `pattern` matches `topic`. `*` matches exactly one segment;
/// the segment counts must be equal — mirroring `pubsub-engine`'s matcher.
fn pattern_matches(pattern: &str, topic: &str) -> bool {
    let p = pattern.split('.');
    let t = topic.split('.');
    let mut pi = p;
    let mut ti = t;
    loop {
        match (pi.next(), ti.next()) {
            (None, None) => return true,
            (Some("*"), Some(_)) => continue,
            (Some(a), Some(b)) if a == b => continue,
            _ => return false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_match() {
        assert!(pattern_matches("sensor.temp", "sensor.temp"));
        assert!(!pattern_matches("sensor.temp", "sensor.humidity"));
    }

    #[test]
    fn single_segment_wildcard() {
        assert!(pattern_matches("sensor.*", "sensor.temp"));
        assert!(pattern_matches("sensor.*", "sensor.humidity"));
        assert!(!pattern_matches("sensor.*", "sensor")); // segment count differs
        assert!(!pattern_matches("sensor.*", "sensor.nested.temp"));
    }

    #[test]
    fn multi_segment_wildcard() {
        assert!(pattern_matches("*.*", "a.b"));
        assert!(!pattern_matches("*.*", "a.b.c"));
    }

    #[test]
    fn subscribe_encode_roundtrip_layout() {
        let b = encode_subscribe("sensor.*", 1);
        assert_eq!(b.len(), 2 + "sensor.*".len() + 1);
        assert_eq!(u16::from_be_bytes([b[0], b[1]]), 8);
        assert_eq!(b[b.len() - 1], 1);
    }

    #[test]
    fn publish_encode_layout() {
        let b = encode_publish("t", &[1, 2, 3]);
        assert_eq!(b, &[0, 1, b't', 1, 2, 3]);
    }

    #[test]
    fn backoff_for_zero_returns_initial() {
        let initial = Duration::from_millis(100);
        let max = Duration::from_secs(10);
        // attempt 0: initial * 2^0 = initial
        assert_eq!(backoff_for(0, initial, max), initial);
    }

    #[test]
    fn backoff_for_grows_exponentially() {
        let initial = Duration::from_millis(100);
        let max = Duration::from_secs(60);
        // attempt 3: 100ms * 2^3 = 800ms
        assert_eq!(backoff_for(3, initial, max), Duration::from_millis(800));
    }

    #[test]
    fn backoff_for_clamps_at_max() {
        let initial = Duration::from_millis(500);
        let max = Duration::from_secs(1);
        // attempt 20 would be ~5242880000 ms but clamps to 1s
        assert_eq!(backoff_for(20, initial, max), max);
    }

    #[test]
    fn backoff_for_handles_max_zero_gracefully() {
        // max_ms floored to 1ms to avoid Duration::from_millis(0) edge cases.
        let initial = Duration::from_millis(10);
        let max = Duration::from_millis(0);
        assert_eq!(backoff_for(5, initial, max), Duration::from_millis(1));
    }

    #[test]
    fn backoff_for_does_not_panic_on_large_attempt() {
        // attempt saturates at 31 shifts; should never panic.
        let initial = Duration::from_millis(1);
        let max = Duration::from_secs(60);
        let _ = backoff_for(u32::MAX, initial, max);
        let _ = backoff_for(31, initial, max);
    }

    // ── RPC dispatch interceptor tests ────────────────────────────────
    //
    // The interceptor lives inside TaskState::dispatch, so we exercise it
    // by constructing a TaskState, registering a handler, and feeding it a
    // synthetic inbound frame. No network / server required.

    fn make_state() -> TaskState {
        let (tx, _rx) = mpsc::channel(8);
        let pending = Arc::new(AtomicUsize::new(0));
        TaskState::new(8, 1024 * 1024, tx, pending)
    }

    fn make_publish_frame(topic: &str, body: &[u8]) -> frame::codec::Frame {
        use frame::header::{FrameFlags, Seq, StreamId};
        let mut payload = Vec::with_capacity(2 + topic.len() + body.len());
        let topic_len = u16::try_from(topic.len()).unwrap_or(u16::MAX);
        payload.extend_from_slice(&topic_len.to_be_bytes());
        payload.extend_from_slice(topic.as_bytes());
        payload.extend_from_slice(body);
        frame::codec::Frame {
            stream_id: StreamId::new(DEFAULT_STREAM),
            seq: Seq::new(1),
            msg_type: MessageType::Publish,
            flags: FrameFlags::NONE,
            payload: Bytes::from(payload),
        }
    }

    #[tokio::test]
    async fn rpc_interceptor_routes_matching_reply_to_oneshot() {
        let mut st = make_state();
        let cid: u128 = 0x0123_4567_89AB_CDEF_0123_4567_89AB_CDEFu128;
        st.rpc_reply_topics.insert("svc.reply".into());

        let (tx, rx) = oneshot::channel();
        st.pending_rpcs.insert(cid, tx);

        // Reply body = 16-byte cid BE + app payload "hello"
        let mut body = cid.to_be_bytes().to_vec();
        body.extend_from_slice(b"hello");
        st.dispatch(DEFAULT_STREAM, make_publish_frame("svc.reply", &body));

        let msg = rx.await.expect("reply should arrive");
        assert_eq!(msg.payload.as_ref(), b"hello");
        assert_eq!(msg.stream_id, DEFAULT_STREAM);
        // Slot must be drained.
        assert!(st.pending_rpcs.is_empty());
    }

    #[tokio::test]
    async fn rpc_interceptor_strips_cid_but_preserves_topic() {
        let mut st = make_state();
        let cid = 42u128;
        st.rpc_reply_topics.insert("_rpc.reply".into());
        let (tx, rx) = oneshot::channel();
        st.pending_rpcs.insert(cid, tx);

        st.dispatch(DEFAULT_STREAM, make_publish_frame("_rpc.reply", &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 42, 9, 9]));
        let msg = rx.await.unwrap();
        // Topic is preserved verbatim; only the payload's cid prefix is stripped.
        assert_eq!(msg.topic.as_ref(), b"_rpc.reply");
        assert_eq!(msg.payload.as_ref(), &[9, 9]);
    }

    #[tokio::test]
    async fn rpc_interceptor_ignores_unmatched_cid_on_reply_topic() {
        let mut st = make_state();
        st.rpc_reply_topics.insert("svc.reply".into());
        let (tx, mut rx) = oneshot::channel();
        st.pending_rpcs.insert(100u128, tx);

        // Wrong cid — should NOT be routed to oneshot. Should fall through
        // to normal fan-out (which is empty, so the message is silently
        // dropped — but the oneshot stays pending).
        let wrong_cid = 999u128;
        st.dispatch(
            DEFAULT_STREAM,
            make_publish_frame("svc.reply", &wrong_cid.to_be_bytes()),
        );

        // Oneshot still pending — no immediate reply.
        tokio::select! {
            biased;
            _ = &mut rx => panic!("unmatched cid must not deliver"),
            _ = tokio::time::sleep(Duration::from_millis(20)) => {}
        }
        assert_eq!(st.pending_rpcs.len(), 1);
    }

    #[tokio::test]
    async fn rpc_interceptor_skipped_when_no_pending_rpc() {
        // No pending RPCs at all — interceptor is a no-op even on a known
        // reply topic. Important: zero overhead path on regular traffic.
        let mut st = make_state();
        st.rpc_reply_topics.insert("svc.reply".into());
        // Should not panic / not interfere with fan-out.
        st.dispatch(
            DEFAULT_STREAM,
            make_publish_frame("svc.reply", &[0u8; 32]),
        );
        // Nothing to assert beyond "didn't panic". The fan-out path runs
        // and finds zero matching subs (state has none registered).
    }

    #[tokio::test]
    async fn rpc_interceptor_skipped_when_topic_not_registered() {
        // Even with a pending RPC, replies on an unrelated topic must NOT
        // be intercepted — otherwise regular publishes whose first 16
        // bytes happen to collide with a cid would be silently swallowed.
        let mut st = make_state();
        let cid = 7u128;
        let (tx, _rx) = oneshot::channel();
        st.pending_rpcs.insert(cid, tx);
        // Note: "unrelated.topic" is NOT in rpc_reply_topics.

        // Send a publish with first 16 bytes == cid on an unrelated topic.
        st.dispatch(
            DEFAULT_STREAM,
            make_publish_frame("unrelated.topic", &cid.to_be_bytes()),
        );

        // The pending entry should remain because interceptor didn't fire.
        assert_eq!(st.pending_rpcs.len(), 1);
    }

    #[tokio::test]
    async fn rpc_interceptor_handles_short_payload_gracefully() {
        // Reply with < 16-byte payload must NOT be intercepted even on a
        // reply topic (defensive: short replies fall through to fan-out).
        let mut st = make_state();
        st.rpc_reply_topics.insert("svc.reply".into());
        let (tx, _rx) = oneshot::channel();
        st.pending_rpcs.insert(1u128, tx);

        st.dispatch(DEFAULT_STREAM, make_publish_frame("svc.reply", &[1, 2, 3]));
        assert_eq!(st.pending_rpcs.len(), 1, "short payload must not match");
    }

    #[tokio::test]
    async fn next_rpc_cid_is_strictly_monotonic() {
        let a = next_rpc_cid();
        let b = next_rpc_cid();
        let c = next_rpc_cid();
        assert!(b > a, "cid must increase: a={a}, b={b}");
        assert!(c > b, "cid must increase: b={b}, c={c}");
    }
}
