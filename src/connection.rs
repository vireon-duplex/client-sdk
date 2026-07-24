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

use std::collections::HashMap;

use bytes::Bytes;
use frame::codec::FrameHeader;
use frame::header::{MessageType, Seq, StreamId};
use send_policy::StreamOpenMeta;
use tokio::sync::{mpsc, oneshot};

use crate::config::ClientConfig;
use crate::error::{ConnectError, PublishError, StreamError, SubscribeError};
use crate::message::Message;
use crate::pubsub::Subscription;
use crate::stream::{StreamHandle, StreamSpec};
use crate::transport::Transport;
use crate::DeliveryPolicy;

/// Depth of the command channel between [`Client`] handles and the task.
pub(crate) const CMD_CHANNEL_CAP: usize = 256;

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
}

impl Client {
    /// Construct the public handle. Called by [`crate::ClientBuilder::connect`].
    #[must_use]
    pub(crate) fn new(tx: mpsc::Sender<ConnCmd>) -> Self {
        Self { tx }
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
}

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
    fn new(subscriber_buffer: usize, max_message_size: usize, cmd_tx: mpsc::Sender<ConnCmd>) -> Self {
        Self {
            subs: Vec::new(),
            streams: HashMap::new(),
            next_bidi: FIRST_DEDICATED_STREAM,
            subscriber_buffer,
            seqs: HashMap::new(),
            max_message_size,
            cmd_tx,
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

        if sid == DEFAULT_STREAM {
            // Fan out to every default-channel subscription whose pattern matches.
            let topic_str = String::from_utf8_lossy(&topic_bytes);
            for sub in &self.subs {
                if pattern_matches(&sub.pattern, &topic_str) {
                    // try_send: never block the I/O task on a slow consumer.
                    // A full channel drops the oldest-by-policy message; log it.
                    if let Err(mpsc::error::TrySendError::Full(_)) = sub.tx.try_send(msg.clone()) {
                        tracing::warn!(
                            pattern = %sub.pattern,
                            "[client] subscriber channel full; dropping message (backpressure)"
                        );
                    }
                }
            }
        } else {
            // Dedicated stream: deliver by stream id, no pattern matching.
            if let Some(entry) = self.streams.get(&sid) {
                if let Err(mpsc::error::TrySendError::Full(_)) = entry.tx.try_send(msg) {
                    tracing::warn!(
                        stream_id = sid,
                        "[client] dedicated-stream channel full; dropping message"
                    );
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
            ConnCmd::Close { resp } => {
                transport.close();
                let _ = resp.send(Ok(()));
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
        transport
            .send_frame(header, &buf, sid)
            .map_err(|_| PublishError::NotConnected)?;
        transport.flush().map_err(|_| PublishError::NotConnected)?;
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

        Ok(StreamHandle::new(sid, rx, self.cmd_tx.clone()))
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
    ready: oneshot::Sender<Result<(), ConnectError>>,
) {
    let mut transport = match Transport::connect(&cfg).await {
        Ok(t) => {
            // Connection is up; unblock ClientBuilder::connect.
            let _ = ready.send(Ok(()));
            t
        }
        Err(e) => {
            let _ = ready.send(Err(e));
            return;
        }
    };

    let mut state = TaskState::new(cfg.subscriber_buffer, cfg.max_message_size, cmd_tx);

    loop {
        // 1. Drain socket → quiche.
        transport.drain_recv();
        // 2. Decode + route any complete frames.
        transport.process_readable(|sid, frame| state.dispatch(sid, frame));
        // 3. Flush pending output.
        if let Err(e) = transport.flush() {
            tracing::warn!(error = %e, "[client] flush error");
        }

        // 4. Connection teardown? Try reconnect per the configured policy.
        if transport.is_closed() {
            if let Some(new_t) = reconnect(&cfg, &mut rx).await {
                transport = new_t;
                tracing::info!("[client] reconnected — replaying subscriptions");
                state.replay_all(&mut transport);
                continue;
            }
            tracing::info!("[client] connection closed; task exiting");
            break;
        }

        // 5. Wait for the next event: a command, socket readability, or a timer.
        let next = transport.next_event_deadline(None);
        tokio::select! {
            biased;
            cmd = rx.recv() => {
                match cmd {
                    Some(c) => {
                        let exit = state.handle_cmd(c, &mut transport);
                        if exit {
                            break;
                        }
                    }
                    None => {
                        // All Client handles dropped — shut down gracefully.
                        transport.close();
                        break;
                    }
                }
            }
            _ = transport.wait_for_event(next) => {}
        }
        // 6. If we slept past the quiche deadline, fire loss-recovery timers.
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
}
