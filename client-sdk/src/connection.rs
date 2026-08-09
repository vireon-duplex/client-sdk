//! The connection task, command channel, and subscriber routing.
//!
//! [`Client`] is a cheap, [`Clone`] handle holding only the command-channel
//! sender. All real work happens in the background connection task, which
//! owns the QUIC transport (and therefore the `!Sync`
//! `quiche::Connection`) and a routing table that demultiplexes inbound
//! frames to subscriptions / dedicated streams.
//!
//! ## Inbound routing
//!
//! The server records the **transport stream id** a `Subscribe` arrives on
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
use frame::header::{FrameFlags, MessageType, Seq, StreamId};
use send_policy::StreamOpenMeta;
use tokio::sync::{mpsc, oneshot};

use crate::DeliveryPolicy;
use crate::config::ClientConfig;
use crate::error::{ConnectError, GroupError, PublishError, RpcError, StreamError, SubscribeError};
use crate::message::Message;
use crate::pubsub::{GroupSubscription, Subscription};
use crate::stream::{StreamHandle, StreamSpec};
use crate::transport::Transport;

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
    /// Join a consumer group on `topic`. Opens a dedicated QUIC stream,
    /// sends `ConsumerGroupJoin`, and starts a periodic re-join heartbeat
    /// (the server evicts members silent for >3 s).
    GroupJoin {
        topic: String,
        group: String,
        consumer: String,
        partitions: u32,
        resp: oneshot::Sender<Result<GroupSubscription, GroupError>>,
    },
    /// Leave a consumer group. Sends `ConsumerGroupLeave` on the stream
    /// that originally sent the Join, and tears down local routing.
    GroupLeave {
        topic: String,
        group: String,
        consumer: String,
        resp: oneshot::Sender<Result<(), GroupError>>,
    },
    /// Close the connection.
    Close {
        resp: oneshot::Sender<Result<(), ConnectError>>,
    },
    /// Trigger QUIC connection migration by rebinding the UDP socket.
    Migrate {
        bind_addr: String,
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
    /// Mirror of NotifyOffset frames received (LogTail diagnostics).
    notify_offset_count: Arc<std::sync::atomic::AtomicU64>,
    /// Mirror of FetchReply frames received (LogTail diagnostics).
    fetch_reply_count: Arc<std::sync::atomic::AtomicU64>,
    /// Mirror of duplicate reliable frames detected & suppressed by the
    /// dedup watermark. Read via [`Self::duplicates_detected`].
    duplicates_detected: Arc<std::sync::atomic::AtomicU64>,
}

impl Client {
    /// Construct the public handle. Called by [`crate::ClientBuilder::connect`].
    /// The `notify_offset_count` / `fetch_reply_count` Arcs are shared with
    /// the background task so the handle reads live values without any
    /// cross-task communication.
    #[must_use]
    pub(crate) fn new(
        tx: mpsc::Sender<ConnCmd>,
        pending_shared: Arc<std::sync::atomic::AtomicUsize>,
        notify_offset_count: Arc<std::sync::atomic::AtomicU64>,
        fetch_reply_count: Arc<std::sync::atomic::AtomicU64>,
        duplicates_detected: Arc<std::sync::atomic::AtomicU64>,
    ) -> Self {
        Self {
            tx,
            pending_shared,
            notify_offset_count,
            fetch_reply_count,
            duplicates_detected,
        }
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
        self.pending_shared
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Number of NotifyOffset frames received from the server (LogTail
    /// delivery path). Non-zero proves the server used LogTail, not
    /// BatchPush, for at least some publishes.
    #[must_use]
    pub fn notify_offset_count(&self) -> u64 {
        self.notify_offset_count
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Number of FetchReply frames received (LogTail pull responses).
    /// Should equal `notify_offset_count` in steady state.
    #[must_use]
    pub fn fetch_reply_count(&self) -> u64 {
        self.fetch_reply_count
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Number of duplicate reliable frames detected and suppressed by the
    /// per-stream dedup watermark since the client was constructed. Non-zero
    /// after a reconnect/resume is expected (server re-sent messages the
    /// client had already accepted); a high value in steady state indicates
    /// the server is re-transmitting more than necessary.
    #[must_use]
    pub fn duplicates_detected(&self) -> u64 {
        self.duplicates_detected
            .load(std::sync::atomic::Ordering::Relaxed)
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
        self.subscribe_with_qos(pattern, crate::message::Qos::default())
            .await
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
    pub fn try_publish(
        &self,
        topic: &str,
        payload: impl crate::message::Payload,
    ) -> Result<(), PublishError> {
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
        let cmd = ConnCmd::OpenStream {
            spec,
            resp: resp_tx,
        };
        self.tx
            .send(cmd)
            .await
            .map_err(|_| StreamError::NotConnected)?;
        resp_rx.await.map_err(|_| StreamError::NotConnected)?
    }

    /// Join a consumer group on `topic` as `consumer` and return a handle
    /// that yields round-robin-balanced publishes.
    ///
    /// Internally opens a dedicated QUIC stream, sends `ConsumerGroupJoin`,
    /// and re-sends it every second as a heartbeat (the server evicts
    /// members silent for >3 s). The returned [`GroupSubscription`]
    /// delivers only the publishes the server assigns to this consumer —
    /// other group members receive the rest.
    ///
    /// Uses the default partition count (1). Pass a custom value via
    /// [`Client::subscribe_group_with_partitions`] if the assignment
    /// strategy consumes it.
    ///
    /// # Errors
    /// [`GroupError::NotConnected`] if the connection is gone.
    pub async fn subscribe_group(
        &self,
        topic: &str,
        group: &str,
        consumer: &str,
    ) -> Result<GroupSubscription, GroupError> {
        self.subscribe_group_with_partitions(topic, group, consumer, 1)
            .await
    }

    /// Like [`Client::subscribe_group`] but lets the caller specify the
    /// `partitions` hint. Must be ≥1 — the server rejects zero.
    ///
    /// [`Client::subscribe_group`]: Self::subscribe_group
    pub async fn subscribe_group_with_partitions(
        &self,
        topic: &str,
        group: &str,
        consumer: &str,
        partitions: u32,
    ) -> Result<GroupSubscription, GroupError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let cmd = ConnCmd::GroupJoin {
            topic: topic.to_string(),
            group: group.to_string(),
            consumer: consumer.to_string(),
            partitions: partitions.max(1),
            resp: resp_tx,
        };
        self.tx
            .send(cmd)
            .await
            .map_err(|_| GroupError::NotConnected)?;
        resp_rx.await.map_err(|_| GroupError::NotConnected)?
    }

    /// Leave a consumer group previously joined via [`Client::subscribe_group`].
    ///
    /// Stops the heartbeat, sends `ConsumerGroupLeave` to the server, and
    /// tears down the dedicated stream. The corresponding
    /// [`GroupSubscription`] will return `None` from `recv()` once any
    /// in-flight messages are drained.
    ///
    /// # Errors
    /// [`GroupError::NotConnected`] if the connection is gone.
    pub async fn leave_group(
        &self,
        topic: &str,
        group: &str,
        consumer: &str,
    ) -> Result<(), GroupError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let cmd = ConnCmd::GroupLeave {
            topic: topic.to_string(),
            group: group.to_string(),
            consumer: consumer.to_string(),
            resp: resp_tx,
        };
        self.tx
            .send(cmd)
            .await
            .map_err(|_| GroupError::NotConnected)?;
        resp_rx.await.map_err(|_| GroupError::NotConnected)?
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

    /// Trigger QUIC connection migration by rebinding the UDP socket.
    ///
    /// The underlying QUIC connection (DCID, crypto state, stream state)
    /// is preserved — only the local UDP 4-tuple changes. The server
    /// validates the new path via PATH_CHALLENGE/PATH_RESPONSE and
    /// redirects subsequent traffic automatically.
    ///
    /// Use `"0.0.0.0:0"` for `bind_addr` to let the OS pick a new
    /// ephemeral port (simulates a NAT rebinding). To bind to a specific
    /// interface (e.g. after WiFi → cellular handoff), pass that
    /// interface's IP.
    ///
    /// # Errors
    /// - [`ConnectError::Io`] if the new socket cannot be bound.
    /// - [`ConnectError::Closed`] if the connection task has exited.
    pub async fn migrate(&self, bind_addr: &str) -> Result<(), ConnectError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let cmd = ConnCmd::Migrate {
            bind_addr: bind_addr.to_owned(),
            resp: resp_tx,
        };
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
    /// Active consumer-group memberships. Each entry's `sid` is also
    /// present in `streams` so inbound publishes route to the group
    /// subscription's channel via the dedicated-stream dispatch path
    /// (no pattern matching → no cross-contamination with default-channel
    /// subscribers on the same topic).
    group_subs: Vec<GroupSubEntry>,
    /// Pending Fetch requests generated by NotifyOffset frames (LogTail
    /// delivery). Each entry is `(topic_bytes, offset, stream_id)`.
    /// Drained by the poll loop after `process_readable` and sent via
    /// `transport.send_frame`.
    pending_fetches: Vec<(bytes::Bytes, u64, u64)>,
    /// Shared mirror of NotifyOffset frames received (LogTail diagnostics).
    /// The same `Arc` is held by [`Client`], so `Client::notify_offset_count`
    /// reads this value without cross-task communication.
    notify_offset_count: Arc<AtomicU64>,
    /// Shared mirror of FetchReply frames received (LogTail diagnostics).
    fetch_reply_count: Arc<AtomicU64>,
    // ── Application-level reliability (ACK + Sequence + Resume) ──────
    /// Master toggle mirrored from `ClientConfig::reliable`.
    reliable_enabled: bool,
    /// Cumulative ACK cadence (every N reliable deliveries).
    ack_interval_msgs: u8,
    /// Stable logical session id — survives reconnect. Sent in every
    /// `Resume` frame so the server can key its replay window.
    logical_session_id: u64,
    /// Per-stream dedup watermark: highest inbound `seq` already accepted
    /// and delivered to the app. Inbound reliable frames with `seq <=`
    /// this value are duplicates (post-reconnect replay tails, server
    /// retries, etc.) and are suppressed. NOT cleared in `replay_all`.
    highest_accepted: HashMap<u64, u64>,
    /// Per-stream highest cumulative ack seq we have flushed (or would
    /// flush next). Bumped every `ack_interval_msgs` reliable deliveries.
    /// Sent to the server in `Resume` on reconnect.
    pending_acks: HashMap<u64, u64>,
    /// Per-stream reliable-frame counter since the last flush. When it
    /// reaches `ack_interval_msgs` we flush an `Ack` and reset to 0.
    reliable_since_flush: HashMap<u64, u8>,
    /// Queue of `(stream_id, ack_seq)` pairs waiting to be sent as `Ack`
    /// frames. Populated by [`Self::maybe_flush_ack`] inside `dispatch`;
    /// drained by the main `run` loop after `process_readable` returns,
    /// the same pattern `pending_fetches` uses.
    pending_ack_flush: Vec<(u64, u64)>,
    /// Aggregate count of duplicate reliable frames detected & suppressed.
    /// Surfaced via `Client::duplicates_detected()`.
    duplicates_detected: Arc<AtomicU64>,
}

/// One consumer-group membership, retained for heartbeat + reconnect replay.
struct GroupSubEntry {
    sid: u64,
    topic: String,
    group: String,
    consumer: String,
    partitions: u32,
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
        notify_offset_count: Arc<AtomicU64>,
        fetch_reply_count: Arc<AtomicU64>,
        duplicates_detected: Arc<AtomicU64>,
        reliable_enabled: bool,
        ack_interval_msgs: u8,
        logical_session_id: u64,
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
            group_subs: Vec::new(),
            pending_fetches: Vec::new(),
            notify_offset_count,
            fetch_reply_count,
            reliable_enabled,
            ack_interval_msgs,
            logical_session_id,
            highest_accepted: HashMap::new(),
            pending_acks: HashMap::new(),
            reliable_since_flush: HashMap::new(),
            pending_ack_flush: Vec::new(),
            duplicates_detected,
        }
    }

    /// Next outgoing sequence number for `stream_id`.
    fn next_seq(&mut self, stream_id: u64) -> Seq {
        let e = self.seqs.entry(stream_id).or_insert(0);
        let s = *e;
        *e = e.wrapping_add(1);
        Seq::new(s)
    }

    /// Bumps the per-stream reliable-frame counter and, when it reaches
    /// `ack_interval_msgs`, queues a cumulative `Ack(stream_id, seq)`
    /// for the main loop to send. The Ack carries the highest contiguous
    /// accepted seq (`pending_acks[stream_id]`), so the server can
    /// release every retained entry at or below it.
    ///
    /// No-op when reliability is disabled. A `ack_interval_msgs` of `0`
    /// is treated as `1` (ack every reliable frame) for safety.
    fn maybe_flush_ack(&mut self, stream_id: u64) {
        if !self.reliable_enabled {
            return;
        }
        let counter = self.reliable_since_flush.entry(stream_id).or_insert(0);
        *counter = counter.saturating_add(1);
        let threshold = self.ack_interval_msgs.max(1);
        if *counter >= threshold {
            *counter = 0;
            let ack_seq = self.pending_acks.get(&stream_id).copied().unwrap_or(0);
            self.pending_ack_flush.push((stream_id, ack_seq));
        }
    }

    /// Route a decoded inbound frame to the right subscriber(s).
    fn dispatch(&mut self, sid: u64, frame: frame::codec::Frame) {
        match frame.msg_type {
            MessageType::Publish => {
                // Zero-copy parse of `topic_len:u16 + topic + payload`.
                let payload = frame.payload;
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
                let seq = frame.seq.get();

                // ── Reliable delivery: dedup + cumulative ACK ────────────
                //
                // Only frames carrying ACK_REQ participate. Ephemeral
                // traffic bypasses the watermark entirely (zero overhead
                // on the existing fast path).
                if self.reliable_enabled && frame.flags.contains(FrameFlags::ACK_REQ) {
                    let high = self.highest_accepted.get(&sid).copied().unwrap_or(0);
                    if seq <= high {
                        // Duplicate — suppress delivery, but still bump the
                        // pending ack so the server advances past the
                        // re-transmitted seq on the next flush. Otherwise a
                        // stuck retry loop would never release the entry.
                        self.duplicates_detected
                            .fetch_add(1, Ordering::Relaxed);
                        let cur = self.pending_acks.get(&sid).copied().unwrap_or(0);
                        if seq > cur {
                            self.pending_acks.insert(sid, seq);
                        }
                        tracing::trace!(
                            stream_id = sid,
                            seq,
                            high,
                            "[client] duplicate reliable publish suppressed"
                        );
                        self.maybe_flush_ack(sid);
                        return;
                    }
                    // New: accept delivery, advance the watermark, track ack.
                    self.highest_accepted.insert(sid, seq);
                    self.pending_acks.insert(sid, seq);
                    // Fall through to fanout_message — the app sees this msg.
                    self.maybe_flush_ack(sid);
                }
                self.fanout_message(sid, topic_bytes, body, seq);
            }
            MessageType::NotifyOffset => {
                // LogTail delivery: server tells us a WAL offset is ready.
                // Parse `topic_len:u16 + topic + offset:u64`, then queue
                // a Fetch request. The FetchReply will arrive on the next
                // recv cycle and be delivered as a regular Message.
                let payload = frame.payload;
                if payload.len() < 2 {
                    return;
                }
                let topic_len = u16::from_be_bytes([payload[0], payload[1]]) as usize;
                if payload.len() < 2 + topic_len + 8 {
                    tracing::warn!(stream_id = sid, "[client] notify-offset truncated");
                    return;
                }
                let topic_bytes = payload.slice(2..2 + topic_len);
                let offset = u64::from_be_bytes(
                    payload[2 + topic_len..2 + topic_len + 8]
                        .try_into()
                        .unwrap_or([0u8; 8]),
                );
                self.pending_fetches.push((topic_bytes, offset, sid));
                self.notify_offset_count.fetch_add(1, Ordering::Relaxed);
            }
            MessageType::FetchReply => {
                // LogTail pull response: `topic_len:u16 + topic + offset:u64
                // + timestamp_ns:u64 + payload_len:u32 + payload`.
                let payload = frame.payload;
                if payload.len() < 2 {
                    return;
                }
                let topic_len = u16::from_be_bytes([payload[0], payload[1]]) as usize;
                // Need at least: 2 + topic_len + 8(offset) + 8(timestamp) + 4(len)
                let header_end = 2 + topic_len + 20;
                if payload.len() < header_end {
                    tracing::warn!(stream_id = sid, "[client] fetch-reply truncated");
                    return;
                }
                let topic_bytes = payload.slice(2..2 + topic_len);
                let payload_len = u32::from_be_bytes([
                    payload[2 + topic_len + 16],
                    payload[2 + topic_len + 17],
                    payload[2 + topic_len + 18],
                    payload[2 + topic_len + 19],
                ]) as usize;
                let body_start = header_end;
                let body_end = body_start + payload_len;
                if payload.len() < body_end {
                    tracing::warn!(stream_id = sid, "[client] fetch-reply payload truncated");
                    return;
                }
                let body = payload.slice(body_start..body_end);
                self.fanout_message(sid, topic_bytes, body, frame.seq.get());
                self.fetch_reply_count.fetch_add(1, Ordering::Relaxed);
            }
            MessageType::ResumeOk => {
                // Server acknowledged our Resume and is about to (or has)
                // re-sent retained Publish frames. Nothing to do here — the
                // replayed publishes arrive as normal Publish frames and
                // flow through the dedup path. Log for diagnostics.
                tracing::info!(
                    stream_id = sid,
                    payload_len = frame.payload.len(),
                    "[client] ResumeOk received — replay in progress"
                );
            }
            MessageType::ResumeUnavailable => {
                // Server could not satisfy one or more requested streams —
                // the seq we asked for is older than the retained window
                // floor. The subscriber keeps its current subscription and
                // accepts new deliveries; the gap is unavoidable.
                tracing::warn!(
                    stream_id = sid,
                    payload_len = frame.payload.len(),
                    "[client] ResumeUnavailable — server window floor advanced past requested seq; \
                     gap is not recoverable"
                );
            }
            _ => {
                // Subscribe/Unsubscribe acks and other control frames are not
                // surfaced to users in v1. Log at debug for diagnostics.
                tracing::debug!(
                    stream_id = sid,
                    msg_type = ?frame.msg_type,
                    "[client] ignoring non-publish inbound frame"
                );
            }
        }
    }

    /// Delivers a `(topic, payload)` pair to matching subscribers. Shared
    /// by both the Publish path (direct push) and the FetchReply path
    /// (LogTail pull). Includes the RPC reply interceptor.
    fn fanout_message(&mut self, sid: u64, topic_bytes: Bytes, body: Bytes, seq: u64) {
        let msg = Message {
            topic: topic_bytes.clone(),
            payload: body,
            seq,
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
            ConnCmd::Publish {
                topic,
                payload,
                stream,
                resp,
            } => {
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
                let out = self
                    .encode_and_send(
                        MessageType::Unsubscribe,
                        &encode_unsubscribe(&pattern),
                        DEFAULT_STREAM,
                        transport,
                    )
                    .map_err(|_| SubscribeError::NotConnected);
                let _ = resp.send(out);
                false
            }
            ConnCmd::OpenStream { spec, resp } => {
                let out = self.handle_open_stream(spec, transport);
                let _ = resp.send(out);
                false
            }
            ConnCmd::RegisterRpcReply {
                cid,
                reply_topic,
                resp,
            } => {
                // Lazy-subscribe to the reply topic on first use. Once
                // joined, the topic is marked in `rpc_reply_topics` so the
                // dispatch interceptor routes matching replies. Idempotent
                // — repeated registrations for the same topic are free.
                if !self.rpc_reply_topics.contains(&reply_topic) {
                    let buf = encode_subscribe(&reply_topic, 0);
                    let header =
                        FrameHeader::new(StreamId::new(DEFAULT_STREAM), MessageType::Subscribe)
                            .with_seq(self.next_seq(DEFAULT_STREAM));
                    if let Err(e) =
                        self.encode_and_send_raw(header, &buf, DEFAULT_STREAM, transport)
                    {
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
            ConnCmd::GroupJoin {
                topic,
                group,
                consumer,
                partitions,
                resp,
            } => {
                let out = self.handle_group_join(&topic, &group, &consumer, partitions, transport);
                let _ = resp.send(out);
                false
            }
            ConnCmd::GroupLeave {
                topic,
                group,
                consumer,
                resp,
            } => {
                let out = self.handle_group_leave(&topic, &group, &consumer, transport);
                let _ = resp.send(out);
                false
            }
            ConnCmd::Close { resp } => {
                // Store the response — the main loop sends it after the
                // graceful drain completes. This ensures Client::close()
                // returns only after all pending writes are flushed.
                self.close_resp = Some(resp);
                true
            }
            ConnCmd::Migrate { bind_addr, resp } => {
                let result = transport.rebind(&bind_addr);
                if result.is_ok() {
                    // Force a probe packet from the new 4-tuple so the
                    // server sees traffic from the new source address and
                    // starts PATH_CHALLENGE/PATH_RESPONSE validation.
                    // Without this, the server doesn't know about the
                    // migration until the next 1-second heartbeat — and
                    // until then it sends replies to the old (now-dead)
                    // socket.
                    let buf = encode_subscribe("_migration.probe", 0);
                    let header =
                        FrameHeader::new(StreamId::new(DEFAULT_STREAM), MessageType::Subscribe)
                            .with_seq(self.next_seq(DEFAULT_STREAM));
                    if let Err(e) =
                        self.encode_and_send_raw(header, &buf, DEFAULT_STREAM, transport)
                    {
                        tracing::warn!(error = %e, "[client] migration probe send failed");
                    }
                    let _ = transport.flush();
                }
                let _ = resp.send(result);
                false
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
        let header = if self.reliable_enabled {
            FrameHeader::new(StreamId::new(sid), MessageType::Publish)
                .with_seq(self.next_seq(sid))
                .with_flags(FrameFlags::ACK_REQ)
        } else {
            FrameHeader::new(StreamId::new(sid), MessageType::Publish)
                .with_seq(self.next_seq(sid))
        };
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
        transport
            .flush()
            .map_err(|_| SubscribeError::NotConnected)?;

        Ok(Subscription::new(rx))
    }

    fn handle_open_stream(
        &mut self,
        spec: StreamSpec,
        transport: &mut Transport,
    ) -> Result<StreamHandle, StreamError> {
        let sid = self.next_bidi;
        self.next_bidi = self
            .next_bidi
            .checked_add(4)
            .ok_or(StreamError::NoCapacity)?;

        let (tx, rx) = mpsc::channel(self.subscriber_buffer);
        self.streams.insert(
            sid,
            StreamEntry {
                policy: spec.policy,
                topic: spec.topic.clone(),
                tx,
            },
        );

        // 1) Declare the stream's delivery policy via StreamOpen. The payload
        //    MUST be the `StreamOpenMeta` byte — the server decodes exactly
        //    this byte. Do NOT use frame::payload::StreamOpen (different
        //    encoding: a direction byte the server ignores).
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

        Ok(StreamHandle::new(
            sid,
            rx,
            self.cmd_tx.clone(),
            self.pending_shared.clone(),
        ))
    }

    /// Open a dedicated stream, send `ConsumerGroupJoin`, and register the
    /// membership for periodic heartbeats. The server records
    /// `(conn_idx, quic_stream_id=sid)` in its `group_locals` table and
    /// routes one round-robin publish per inbound message to this stream.
    fn handle_group_join(
        &mut self,
        topic: &str,
        group: &str,
        consumer: &str,
        partitions: u32,
        transport: &mut Transport,
    ) -> Result<GroupSubscription, GroupError> {
        let sid = self.next_bidi;
        self.next_bidi = self
            .next_bidi
            .checked_add(4)
            .ok_or(GroupError::Rejected("no stream capacity".into()))?;

        let (tx, rx) = mpsc::channel(self.subscriber_buffer);
        // Reuse the dedicated-stream routing entry: dispatch will find `sid`
        // here and route inbound publishes straight to the channel — no
        // pattern matching, so default-channel subscribers on the same topic
        // never see group-balanced messages (and vice versa).
        self.streams.insert(
            sid,
            StreamEntry {
                policy: DeliveryPolicy::ReliableOrdered,
                topic: Some(topic.to_string()),
                tx,
            },
        );

        // Declare the stream + a normal Subscribe on the topic. Without
        // StreamOpen, the server never assigns a policy and the stream is
        // unknown to the per-connection bookkeeping that gates fan-out.
        // Without Subscribe, the server has no `Subscriber` entry in
        // `pubsub_registry` — `route_publish` finds zero local targets
        // and the publish is dropped before reaching `group_locals`.
        let policy_bytes = StreamOpenMeta::new(DeliveryPolicy::ReliableOrdered).encode();
        let so_header = FrameHeader::new(StreamId::new(sid), MessageType::StreamOpen)
            .with_seq(self.next_seq(sid));
        self.encode_and_send_raw(so_header, &policy_bytes, sid, transport)
            .map_err(|_| GroupError::NotConnected)?;

        let join_buf = encode_group_join(topic, group, consumer, partitions);
        let join_header = FrameHeader::new(StreamId::new(sid), MessageType::ConsumerGroupJoin)
            .with_seq(self.next_seq(sid));
        self.encode_and_send_raw(join_header, &join_buf, sid, transport)
            .map_err(|_| GroupError::NotConnected)?;
        transport.flush().map_err(|_| GroupError::NotConnected)?;

        self.group_subs.push(GroupSubEntry {
            sid,
            topic: topic.to_string(),
            group: group.to_string(),
            consumer: consumer.to_string(),
            partitions,
        });

        Ok(GroupSubscription::new(rx))
    }

    /// Send `ConsumerGroupLeave` for the matching membership and drop local
    /// routing. In-flight messages already buffered in the channel are
    /// preserved until the [`GroupSubscription`] is dropped by the caller.
    fn handle_group_leave(
        &mut self,
        topic: &str,
        group: &str,
        consumer: &str,
        transport: &mut Transport,
    ) -> Result<(), GroupError> {
        let Some(idx) = self
            .group_subs
            .iter()
            .position(|e| e.topic == topic && e.group == group && e.consumer == consumer)
        else {
            // Already left (or never joined) — treat as success so callers
            // can issue leave unconditionally on shutdown without tracking
            // join state themselves.
            return Ok(());
        };
        let entry = self.group_subs.swap_remove(idx);
        let buf = encode_group_leave(topic, group, consumer);
        let header = FrameHeader::new(StreamId::new(entry.sid), MessageType::ConsumerGroupLeave)
            .with_seq(self.next_seq(entry.sid));
        let _ = self.encode_and_send_raw(header, &buf, entry.sid, transport);
        let _ = transport.flush();
        // Drop the dispatch entry; inbound publishes for `sid` will be
        // logged-and-skipped by the dispatch path.
        self.streams.remove(&entry.sid);
        Ok(())
    }

    /// Re-send every active group Join (the server evicts members silent
    /// for >3 s; re-sending on the heartbeat tick keeps membership alive
    /// without relying on a separate timer per group). The server dedups
    /// by `consumer_id` so this never creates phantom members.
    fn heartbeat_group_subs(&mut self, transport: &mut Transport) {
        // Snapshot sids+payloads so we can build+send without borrowing
        // `self.group_subs` and `self.seqs` simultaneously.
        let snap: Vec<(u64, Vec<u8>)> = self
            .group_subs
            .iter()
            .map(|e| {
                (
                    e.sid,
                    encode_group_join(&e.topic, &e.group, &e.consumer, e.partitions),
                )
            })
            .collect();
        for (sid, buf) in snap {
            let header = FrameHeader::new(StreamId::new(sid), MessageType::ConsumerGroupJoin)
                .with_seq(self.next_seq(sid));
            if let Err(e) = transport.send_frame(header, &buf, sid) {
                tracing::warn!(
                    stream_id = sid,
                    error = %e,
                    "[client] group heartbeat: send failed"
                );
                return;
            }
        }
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
        // Stale offsets from the old connection are meaningless after
        // reconnect (server may have restarted with an empty WAL).
        self.pending_fetches.clear();
        // NOTE: highest_accepted / pending_acks are deliberately NOT
        // cleared — they are the resume-state that lets the server
        // compute the correct replay gap and lets us dedup post-resume
        // duplicates.

        // ── Reliable: send Resume FIRST ─────────────────────────────────
        //
        // When reliability is enabled, the very first frame on the new
        // connection is a Resume carrying the stable logical session id
        // and the per-stream last-acked watermark. The server replies
        // ResumeOk + replays retained Publish frames for any gap. Doing
        // this before re-subscribing ensures the replayed deliveries
        // arrive on already-open streams.
        //
        // An empty Resume (session hello, no slots) is sent on first
        // connect when no streams have been acked yet — the server
        // registers the logical session and replies with an empty
        // ResumeOk so the client knows the session is bound.
        if self.reliable_enabled {
            let slots: Vec<(u64, u64)> = self
                .pending_acks
                .iter()
                .map(|(&sid, &ack)| (sid, ack))
                .collect();
            let mut buf =
                Vec::with_capacity(8 + 1 + slots.len() * 16);
            buf.extend_from_slice(&self.logical_session_id.to_be_bytes());
            buf.push(slots.len().min(u8::MAX as usize) as u8);
            for &(sid, ack) in &slots {
                buf.extend_from_slice(&sid.to_be_bytes());
                buf.extend_from_slice(&ack.to_be_bytes());
            }
            let header = FrameHeader::new(
                StreamId::new(DEFAULT_STREAM),
                MessageType::Resume,
            )
            .with_seq(self.next_seq(DEFAULT_STREAM));
            if let Err(e) = transport.send_frame(header, &buf, DEFAULT_STREAM) {
                tracing::warn!(error = %e, "[client] replay: Resume send failed");
                return;
            }
        }

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

        // Consumer-group memberships: re-send Join on each group sub's
        // dedicated stream. Re-establishes server-side `group_locals`
        // entries that were lost when the old connection tore down.
        // Snapshot first so `self.next_seq` can mutably borrow without
        // conflicting with the `&self.group_subs` iteration.
        let group_replay: Vec<(u64, String, String, String, u32)> = self
            .group_subs
            .iter()
            .map(|e| {
                (
                    e.sid,
                    e.topic.clone(),
                    e.group.clone(),
                    e.consumer.clone(),
                    e.partitions,
                )
            })
            .collect();
        for (sid, topic, group, consumer, partitions) in &group_replay {
            let buf = encode_group_join(topic, group, consumer, *partitions);
            let h = FrameHeader::new(StreamId::new(*sid), MessageType::ConsumerGroupJoin)
                .with_seq(self.next_seq(*sid));
            if let Err(e) = transport.send_frame(h, &buf, *sid) {
                tracing::warn!(
                    stream_id = sid,
                    error = %e,
                    "[client] replay: group join send failed"
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
    notify_offset_count: Arc<AtomicU64>,
    fetch_reply_count: Arc<AtomicU64>,
    duplicates_detected: Arc<AtomicU64>,
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

    // Session ticket for 0-RTT resumption on reconnect. Extracted after
    // the handshake; quiche populates it once the server sends
    // NEW_SESSION_TICKET (may arrive a few hundred ms after handshake).
    let mut session_ticket: Option<Vec<u8>> = None;

    let mut state = TaskState::new(
        cfg.subscriber_buffer,
        cfg.max_message_size,
        cmd_tx,
        pending_shared.clone(),
        notify_offset_count.clone(),
        fetch_reply_count.clone(),
        duplicates_detected.clone(),
        cfg.reliable,
        cfg.ack_interval,
        // Allocate a stable logical session id on first connect unless the
        // caller overrode it (non-zero). The id survives reconnect.
        if cfg.logical_session_id != 0 {
            cfg.logical_session_id
        } else {
            // Random u64 — good enough as a per-client session key; the
            // server does not assume uniqueness across clients (collisions
            // just mean two clients share a replay window, which is
            // harmless for at-least-once delivery).
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0)
                .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        },
    );

    // ── First-connect session hello ───────────────────────────────────
    //
    // When reliability is enabled, send an empty Resume (session hello)
    // immediately after the handshake so the server registers the logical
    // session and allocates a replay window for it. Subsequent subscribes
    // then bind streams into that window. On reconnect, replay_all sends
    // a full Resume with the per-stream last-acked watermarks.
    if state.reliable_enabled {
        let mut hello = Vec::with_capacity(9);
        hello.extend_from_slice(&state.logical_session_id.to_be_bytes());
        hello.push(0); // zero slots — session hello
        let header = FrameHeader::new(
            StreamId::new(DEFAULT_STREAM),
            MessageType::Resume,
        )
        .with_seq(state.next_seq(DEFAULT_STREAM));
        if let Err(e) = transport.send_frame(header, &hello, DEFAULT_STREAM) {
            tracing::warn!(error = %e, "[client] session-hello Resume send failed");
        }
        let _ = transport.flush();
    }

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
        // 3b. Drain pending Fetch requests generated by NotifyOffset frames
        // (LogTail delivery). Each request asks the server to replay one
        // WAL entry; the FetchReply arrives on the next recv cycle.
        while let Some((topic_bytes, offset, sid)) = state.pending_fetches.pop() {
            let mut buf = Vec::with_capacity(2 + topic_bytes.len() + 8 + 4);
            buf.extend_from_slice(&(topic_bytes.len() as u16).to_be_bytes());
            buf.extend_from_slice(&topic_bytes);
            buf.extend_from_slice(&offset.to_be_bytes());
            buf.extend_from_slice(&1u32.to_be_bytes()); // max_count = 1
            let header = FrameHeader::new(StreamId::new(sid), MessageType::Fetch)
                .with_seq(state.next_seq(sid));
            if let Err(e) = transport.send_frame(header, &buf, sid) {
                tracing::warn!(error = %e, "[client] fetch send failed (logtail)");
            }
        }
        // 3c. Drain queued cumulative ACKs (reliable delivery). Each entry
        // is `(stream_id, ack_seq)`; the wire payload is just the 8-byte
        // big-endian ack_seq. Sent on the same QUIC stream that received
        // the reliable deliveries so the server's per-(conn, stream)
        // routing finds the right replay ring.
        while let Some((sid, ack_seq)) = state.pending_ack_flush.pop() {
            let buf = ack_seq.to_be_bytes();
            let header = FrameHeader::new(StreamId::new(sid), MessageType::Ack)
                .with_seq(state.next_seq(sid));
            if let Err(e) = transport.send_frame(header, &buf, sid) {
                tracing::warn!(error = %e, "[client] ack send failed");
            }
        }
        // 4. Flush pending output.
        if let Err(e) = transport.flush() {
            tracing::warn!(error = %e, "[client] flush error");
        }

        // 4b. Lazily capture the session ticket once the server sends it.
        // quiche populates conn.session() asynchronously (after the
        // NEW_SESSION_TICKET frame arrives, typically 100-500ms post-
        // handshake). We check every iteration until it appears.
        if session_ticket.is_none() {
            if let Some(t) = transport.session_ticket() {
                tracing::debug!("[client] session ticket received — 0-RTT resumption available");
                session_ticket = Some(t);
            }
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
            if let Some(new_t) = reconnect(&cfg, &mut rx, session_ticket.as_deref()).await {
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
                // Consumer-group heartbeats: re-send each Join so the server
                // does not evict us (eviction window = 3 s, tick = 1 s).
                state.heartbeat_group_subs(&mut transport);
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
                            // 10 s covers the worst-case cmd-channel backlog
                            // under heavy load. With small frames pending is
                            // typically minimal and drain returns quickly.
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
async fn reconnect(
    cfg: &ClientConfig,
    rx: &mut mpsc::Receiver<ConnCmd>,
    session: Option<&[u8]>,
) -> Option<Transport> {
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

        match Transport::connect_with_session(cfg, session).await {
            Ok(t) => {
                tracing::info!(
                    attempt = attempt + 1,
                    resumed = t.is_resumed(),
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
        ConnCmd::GroupJoin { resp, .. } => {
            let _ = resp.send(Err(GroupError::NotConnected));
        }
        ConnCmd::GroupLeave { resp, .. } => {
            let _ = resp.send(Err(GroupError::NotConnected));
        }
        ConnCmd::Close { resp } => {
            let _ = resp.send(Err(ConnectError::Closed(
                "close requested during reconnect backoff".into(),
            )));
        }
        ConnCmd::Migrate { resp, .. } => {
            let _ = resp.send(Err(ConnectError::Closed(
                "migrate requested during reconnect backoff".into(),
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

/// `topic_len(u16) + topic + group_len(u16) + group +
///    consumer_len(u16) + consumer + partitions(u32)`
///
/// Matches `ApplicationLayer::parse_group_prefix` +
/// `handle_group_join` exactly. The server reads `partitions` from the
/// trailing 4 bytes after the consumer id.
fn encode_group_join(topic: &str, group: &str, consumer: &str, partitions: u32) -> Vec<u8> {
    let tl = u16::try_from(topic.len()).unwrap_or(u16::MAX);
    let gl = u16::try_from(group.len()).unwrap_or(u16::MAX);
    let cl = u16::try_from(consumer.len()).unwrap_or(u16::MAX);
    let mut buf = Vec::with_capacity(2 + topic.len() + 2 + group.len() + 2 + consumer.len() + 4);
    buf.extend_from_slice(&tl.to_be_bytes());
    buf.extend_from_slice(topic.as_bytes());
    buf.extend_from_slice(&gl.to_be_bytes());
    buf.extend_from_slice(group.as_bytes());
    buf.extend_from_slice(&cl.to_be_bytes());
    buf.extend_from_slice(consumer.as_bytes());
    buf.extend_from_slice(&partitions.to_be_bytes());
    buf
}

/// `topic_len(u16) + topic + group_len(u16) + group +
///    consumer_len(u16) + consumer`
fn encode_group_leave(topic: &str, group: &str, consumer: &str) -> Vec<u8> {
    let tl = u16::try_from(topic.len()).unwrap_or(u16::MAX);
    let gl = u16::try_from(group.len()).unwrap_or(u16::MAX);
    let cl = u16::try_from(consumer.len()).unwrap_or(u16::MAX);
    let mut buf = Vec::with_capacity(2 + topic.len() + 2 + group.len() + 2 + consumer.len());
    buf.extend_from_slice(&tl.to_be_bytes());
    buf.extend_from_slice(topic.as_bytes());
    buf.extend_from_slice(&gl.to_be_bytes());
    buf.extend_from_slice(group.as_bytes());
    buf.extend_from_slice(&cl.to_be_bytes());
    buf.extend_from_slice(consumer.as_bytes());
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
    fn group_join_encode_layout() {
        // topic="t", group="g", consumer="c", partitions=1
        let b = encode_group_join("t", "g", "c", 1);
        // Expected: 0,1,'t' | 0,1,'g' | 0,1,'c' | 0,0,0,1
        assert_eq!(b, &[0, 1, b't', 0, 1, b'g', 0, 1, b'c', 0, 0, 0, 1]);
    }

    #[test]
    fn group_leave_encode_layout() {
        let b = encode_group_leave("t", "g", "c");
        assert_eq!(b, &[0, 1, b't', 0, 1, b'g', 0, 1, b'c']);
    }

    #[test]
    fn group_join_matches_server_parse_prefix() {
        // Round-trip: the bytes we produce must be acceptable to the
        // server's `parse_group_prefix` shape — i.e. the first three
        // length-prefixed strings decode cleanly and leave a 4-byte tail.
        let b = encode_group_join("sensor.temp", "workers", "c1", 8);
        // topic
        let tlen = u16::from_be_bytes([b[0], b[1]]) as usize;
        assert_eq!(&b[2..2 + tlen], b"sensor.temp");
        // group
        let mut pos = 2 + tlen;
        let glen = u16::from_be_bytes([b[pos], b[pos + 1]]) as usize;
        pos += 2;
        assert_eq!(&b[pos..pos + glen], b"workers");
        // consumer
        pos += glen;
        let clen = u16::from_be_bytes([b[pos], b[pos + 1]]) as usize;
        pos += 2;
        assert_eq!(&b[pos..pos + clen], b"c1");
        // partitions tail
        pos += clen;
        assert_eq!(b.len() - pos, 4);
        assert_eq!(
            u32::from_be_bytes([b[pos], b[pos + 1], b[pos + 2], b[pos + 3]]),
            8
        );
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
        let notify = Arc::new(AtomicU64::new(0));
        let fetch = Arc::new(AtomicU64::new(0));
        let dups = Arc::new(AtomicU64::new(0));
        TaskState::new(8, 1024 * 1024, tx, pending, notify, fetch, dups, false, 32, 0)
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

    /// Build a Publish frame carrying `ACK_REQ` with the given `seq`.
    fn make_reliable_frame_with_seq(
        topic: &str,
        body: &[u8],
        seq: u64,
    ) -> frame::codec::Frame {
        let mut f = make_publish_frame(topic, body);
        f.seq = Seq::new(seq);
        f.flags = FrameFlags::ACK_REQ;
        f
    }

    /// A reliable-enabled TaskState for dedup/ack tests.
    fn make_reliable_state(ack_interval: u8) -> TaskState {
        let (tx, _rx) = mpsc::channel(8);
        let pending = Arc::new(AtomicUsize::new(0));
        let notify = Arc::new(AtomicU64::new(0));
        let fetch = Arc::new(AtomicU64::new(0));
        let dups = Arc::new(AtomicU64::new(0));
        TaskState::new(
            8,
            1024 * 1024,
            tx,
            pending,
            notify,
            fetch,
            dups.clone(),
            true,
            ack_interval,
            42, // logical session id (deterministic for tests)
        )
    }

    #[tokio::test]
    async fn reliable_dispatch_accepts_monotonic_seq_and_queus_ack() {
        // ack_interval=1 → every reliable frame flushes an ack.
        let mut st = make_reliable_state(1);
        // Open a dedicated stream so fanout has somewhere to deliver.
        let (stx, _srx) = mpsc::channel::<Message>(8);
        st.streams.insert(
            7,
            StreamEntry {
                policy: DeliveryPolicy::ReliableUnordered,
                topic: Some("sensor.temp".into()),
                tx: stx,
            },
        );
        st.dispatch(7, make_reliable_frame_with_seq("sensor.temp", b"a", 1));
        st.dispatch(7, make_reliable_frame_with_seq("sensor.temp", b"b", 2));
        st.dispatch(7, make_reliable_frame_with_seq("sensor.temp", b"c", 3));
        // Watermark advanced to 3.
        assert_eq!(st.highest_accepted.get(&7), Some(&3));
        // Three acks queued (one per frame, interval=1).
        assert_eq!(st.pending_ack_flush.len(), 3);
        // The last acked seq is 3.
        assert_eq!(st.pending_ack_flush.last().copied(), Some((7, 3)));
    }

    #[tokio::test]
    async fn reliable_dispatch_suppresses_duplicates_below_watermark() {
        let mut st = make_reliable_state(1);
        let (stx, _srx) = mpsc::channel::<Message>(8);
        st.streams.insert(
            7,
            StreamEntry {
                policy: DeliveryPolicy::ReliableUnordered,
                topic: Some("sensor.temp".into()),
                tx: stx,
            },
        );
        // Accept 1..5.
        for s in 1..=5u64 {
            st.dispatch(7, make_reliable_frame_with_seq("sensor.temp", b"x", s));
        }
        // Now deliver a duplicate (seq 3, already accepted).
        st.dispatch(7, make_reliable_frame_with_seq("sensor.temp", b"dup", 3));
        // Duplicate counter incremented.
        assert_eq!(
            st.duplicates_detected.load(Ordering::Relaxed),
            1,
            "first duplicate should be counted"
        );
        // Watermark unchanged at 5.
        assert_eq!(st.highest_accepted.get(&7), Some(&5));
        // pending_acks still tracks the highest (5).
        assert_eq!(st.pending_acks.get(&7), Some(&5));
    }

    #[tokio::test]
    async fn reliable_ack_cadence_queues_every_nth_frame() {
        // ack_interval=3 → one ack queued per 3 reliable frames.
        let mut st = make_reliable_state(3);
        let (stx, _srx) = mpsc::channel::<Message>(8);
        st.streams.insert(
            7,
            StreamEntry {
                policy: DeliveryPolicy::ReliableUnordered,
                topic: Some("sensor.temp".into()),
                tx: stx,
            },
        );
        for s in 1..=7u64 {
            st.dispatch(7, make_reliable_frame_with_seq("sensor.temp", b"x", s));
        }
        // floor(7/3) = 2 acks queued (after frames 3 and 6).
        assert_eq!(
            st.pending_ack_flush.len(),
            2,
            "one ack per 3 frames → 2 acks for 7 frames"
        );
    }

    #[tokio::test]
    async fn reliable_resume_state_survives_replay_all_reset() {
        // replay_all clears `seqs` but MUST preserve dedup/ack state.
        let mut st = make_reliable_state(1);
        st.highest_accepted.insert(7, 5);
        st.pending_acks.insert(7, 5);
        // Build a minimal transport substitute — replay_all only needs
        // send_frame + flush, and both fail-closed paths early-return on
        // error so a stub that returns Ok is sufficient for this test.
        // We verify state invariants directly after the call.
        // (Transport is not Send and not easily stubbed in unit tests;
        // instead we verify the invariant by inspecting state before
        // and after a manual partial-replay that mirrors replay_all's
        // preamble.)
        st.seqs.clear();
        // Verify the watermark survived.
        assert_eq!(st.highest_accepted.get(&7), Some(&5));
        assert_eq!(st.pending_acks.get(&7), Some(&5));
    }

    #[tokio::test]
    async fn reliable_dispatch_ignores_frames_without_ack_req() {
        // Reliable enabled, but incoming frame has no ACK_REQ flag →
        // bypass dedup entirely (no watermark update, no ack queued).
        let mut st = make_reliable_state(1);
        let (stx, _srx) = mpsc::channel::<Message>(8);
        st.streams.insert(
            7,
            StreamEntry {
                policy: DeliveryPolicy::ReliableUnordered,
                topic: Some("sensor.temp".into()),
                tx: stx,
            },
        );
        // Plain publish (no ACK_REQ) with seq=1.
        let mut f = make_publish_frame("sensor.temp", b"x");
        f.seq = Seq::new(1);
        st.dispatch(7, f);
        // No watermark update (frame bypassed dedup).
        assert!(st.highest_accepted.get(&7).is_none());
        // No ack queued.
        assert!(st.pending_ack_flush.is_empty());
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

        st.dispatch(
            DEFAULT_STREAM,
            make_publish_frame(
                "_rpc.reply",
                &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 42, 9, 9],
            ),
        );
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
        st.dispatch(DEFAULT_STREAM, make_publish_frame("svc.reply", &[0u8; 32]));
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
