//! Low-level QUIC transport: tokio UDP socket + `quiche::Connection`.
//!
//! The transport owns:
//!
//! * the `!Sync` [`quiche::Connection`] (must live on a single task),
//! * a non-blocking [`tokio::net::UdpSocket`],
//! * reusable datagram buffers,
//! * one incremental [`FrameDecoder`] per QUIC stream (frames may span
//!   datagrams, so each stream accumulates independently).
//!
//! The connection task ([`crate::connection`]) drives the socket via the
//! `drain_*` / `flush` helpers and feeds decoded frames to its routing table.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use bytes::BytesMut;
use frame::codec::{Frame, FrameDecoder};
use quiche::Connection;
use tokio::net::UdpSocket;

use crate::config::{ClientConfig, ClientIdentity, TlsVerify};
use crate::error::ConnectError;

/// ALPN protocol the client offers. The server advertises
/// `["hq-interop","http/1.1","h3-29"]`;
/// `h3-29` is the common value both sides accept.
const ALPN: &[u8] = b"h3-29";

/// Maximum UDP payload size (IPv4 max: 65535 − 20 − 8).
const DGRAM_BUF: usize = 65507;

/// Handshake must complete within this deadline.
const HANDSHAKE_DEADLINE: Duration = Duration::from_secs(10);

/// Threshold for the "eager" encode path (MPI-inspired). Frames whose
/// wire size fits in this stack buffer avoid all heap interaction — no
/// `BytesMut::clear`, no `reserve`, no `advance_mut`. The vast majority
/// of pub/sub control frames (Subscribe, Ack, small Publishes) are well
/// under this threshold.
const STACK_ENCODE_MAX: usize = 2048;

/// The transport-layer state owned by the connection task.
pub(crate) struct Transport {
    conn: Connection,
    sock: UdpSocket,
    peer: SocketAddr,
    local: SocketAddr,
    send_buf: Vec<u8>,
    recv_buf: Vec<u8>,
    /// Per-stream incremental decoders. A frame can be split across datagrams,
    /// so each stream keeps its own accumulator.
    decoders: HashMap<u64, FrameDecoder>,
    /// Per-stream pending bytes from partial `stream_send` writes. When
    /// QUIC flow control limits a write, the unwritten tail is buffered
    /// here and retried on the next iteration after the peer opens the
    /// window (MAX_STREAM_DATA). Without this, `stream_send` would
    /// silently drop the tail and corrupt the byte stream — causing
    /// CrcMismatch + decoder desync on the server side.
    pending: HashMap<u64, BytesMut>,
    /// Shared atomic mirror of total `pending` bytes. Updated on every
    /// stream_send / flush_pending so publishers can check backpressure
    /// via [`Client::pending_bytes`] without entering the connection task.
    /// Unlike the cmd-channel-full signal (which fires only after CAP
    /// commands queue up), this reflects real-time QUIC flow-control
    /// pressure from the subscriber side.
    pending_shared: Arc<std::sync::atomic::AtomicUsize>,
    /// Reusable encode buffer for `send_frame`. Avoids a `BytesMut::with_capacity`
    /// allocation per frame — cleared and re-filled each call.
    encode_buf: BytesMut,
}

impl Transport {
    /// Establish the QUIC connection: bind a UDP socket, start the quiche
    /// handshake, and drive it to completion (or [`HANDSHAKE_DEADLINE`]).
    ///
    /// On success the connection is established and ALPN-negotiated.
    pub(crate) async fn connect(cfg: &ClientConfig) -> Result<Self, ConnectError> {
        Self::connect_with_session(cfg, None).await
    }

    /// Like [`connect`](Self::connect) but offers a previously stored
    /// session ticket for abbreviated handshake (0-RTT resumption).
    pub(crate) async fn connect_with_session(
        cfg: &ClientConfig,
        session: Option<&[u8]>,
    ) -> Result<Self, ConnectError> {
        let peer: SocketAddr = format!("{}:{}", cfg.host, cfg.port)
            .parse()
            .map_err(|e| ConnectError::Config(format!("invalid server address: {e}")))?;

        let bind_addr = if peer.is_ipv6() {
            "[::]:0"
        } else {
            "0.0.0.0:0"
        };
        let sock = UdpSocket::bind(bind_addr).await?;
        let local = sock.local_addr()?;
        // Pre-connect the socket so we can use try_recv/try_send (no send_to addr
        // lookups on the hot path) and so ICMP errors surface on recv.
        sock.connect(peer).await?;

        let mut qcfg =
            build_quiche_config(&cfg.tls, cfg.idle_timeout, cfg.client_identity.as_ref())?;
        let scid = quiche::ConnectionId::from_vec(gen_scid());
        let mut conn = quiche::connect(Some(&cfg.sni), &scid, local, peer, &mut qcfg)?;

        // Offer a previously stored session ticket for 0-RTT resumption.
        // Must be called before any packets are sent.
        if let Some(ticket) = session {
            let _ = conn.set_session(ticket);
        }

        let mut t = Self {
            conn,
            sock,
            peer,
            local,
            send_buf: vec![0u8; DGRAM_BUF],
            recv_buf: vec![0u8; DGRAM_BUF],
            decoders: HashMap::new(),
            pending: HashMap::new(),
            pending_shared: Arc::new(AtomicUsize::new(0)),
            encode_buf: BytesMut::new(),
        };

        t.handshake().await?;
        Ok(t)
    }

    /// Drive the TLS handshake to completion.
    async fn handshake(&mut self) -> Result<(), ConnectError> {
        let deadline = Instant::now() + HANDSHAKE_DEADLINE;

        // Emit initial ClientHello flight.
        self.flush()?;

        loop {
            if self.conn.is_established() {
                // Validate ALPN — a mismatch means the server is not Vireon.
                let proto = self.conn.application_proto();
                if proto != ALPN {
                    return Err(ConnectError::AlpnNegotiation);
                }
                return Ok(());
            }
            if self.conn.is_closed() {
                return Err(self.closed_reason());
            }
            if Instant::now() >= deadline {
                return Err(ConnectError::HandshakeTimeout(HANDSHAKE_DEADLINE));
            }

            // Progress: drain any incoming, flush any pending.
            self.drain_recv();
            self.flush()?;

            let next = self.next_event_deadline(Some(deadline));
            tokio::select! {
                biased;
                _ = self.sock.readable() => {}
                _ = tokio::time::sleep_until(tokio::time::Instant::from_std(next)) => {}
            }
            // Only fire loss-recovery timers if we actually slept past the
            // deadline; otherwise a cmd/readable woke us and we re-loop.
            if Instant::now() >= next {
                self.fire_timeout();
            }
        }
    }

    /// Read all currently-available datagrams and feed them to quiche.
    /// `WouldBlock` (no more datagrams) is the normal termination.
    /// Returns `true` if at least one datagram was received.
    pub(crate) fn drain_recv(&mut self) -> bool {
        let mut got_data = false;
        loop {
            match self.sock.try_recv_from(&mut self.recv_buf) {
                Ok((n, from)) => {
                    got_data = true;
                    let info = quiche::RecvInfo {
                        from,
                        to: self.local,
                    };
                    match self.conn.recv(&mut self.recv_buf[..n], info) {
                        Ok(_) | Err(quiche::Error::Done) => {}
                        Err(e) => {
                            tracing::warn!(error = %e, "[transport] quiche recv error");
                            return got_data;
                        }
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => return got_data,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                // Connected-UDP ICMP port-unreachable: the peer process was
                // killed (no graceful CONNECTION_CLOSE). Close the quiche
                // connection so is_closed() returns true and the reconnect
                // FSM fires immediately instead of waiting for the 30s idle
                // timeout.
                Err(e) if e.kind() == std::io::ErrorKind::ConnectionRefused => {
                    tracing::info!("[transport] peer unreachable (connection refused)");
                    let _ = self.conn.close(true, 0x00, b"peer unreachable");
                    return got_data;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "[transport] socket recv error");
                    return got_data;
                }
            }
        }
    }

    /// Decode every complete frame currently readable on any QUIC stream and
    /// invoke `f` with `(quic_stream_id, frame)`. `f` may route the frame into
    /// the connection task's subscriber table.
    pub(crate) fn process_readable(&mut self, mut f: impl FnMut(u64, Frame)) {
        // Collect first: `readable()` borrows `conn` immutably but `stream_recv`
        // needs `&mut self.conn`.
        let readable: Vec<u64> = self.conn.readable().collect();
        for sid in readable {
            loop {
                match self.conn.stream_recv(sid, &mut self.recv_buf) {
                    Ok((n, _flags)) => {
                        if n == 0 {
                            break;
                        }
                        let dec = self
                            .decoders
                            .entry(sid)
                            .or_insert_with(|| FrameDecoder::new().skip_crc(true));
                        dec.push(&self.recv_buf[..n]);
                        // Drain every complete frame the decoder now holds.
                        loop {
                            match dec.decode_frame() {
                                Ok(Some(frame)) => f(sid, frame),
                                Ok(None) => break,
                                Err(e) => {
                                    tracing::warn!(
                                        stream_id = sid,
                                        error = ?e,
                                        "[transport] malformed frame; resetting stream decoder"
                                    );
                                    dec.clear();
                                    break;
                                }
                            }
                        }
                    }
                    Err(quiche::Error::Done) => break,
                    Err(quiche::Error::BufferTooShort) => {
                        // Frame larger than recv buffer — grow and retry once.
                        if self.recv_buf.len() < 8 * DGRAM_BUF {
                            self.recv_buf.resize(self.recv_buf.len() * 2, 0);
                            continue;
                        }
                        tracing::warn!(stream_id = sid, "[transport] stream data exceeds buffer");
                        break;
                    }
                    Err(e) => {
                        tracing::warn!(stream_id = sid, error = %e, "[transport] stream_recv error");
                        break;
                    }
                }
            }
        }
    }

    /// Encode and send a frame on `stream_id`. The bytes are written via quiche's
    /// stream buffer; call [`flush`] afterwards to put them on the wire.
    pub(crate) fn send_frame(
        &mut self,
        header: frame::codec::FrameHeader,
        payload: &[u8],
        stream_id: u64,
    ) -> Result<(), ConnectError> {
        let wire_size = frame::codec::HEADER_SIZE + payload.len() + frame::codec::CRC_SIZE;

        if wire_size <= STACK_ENCODE_MAX {
            // Eager path (MPI-inspired): stack-allocated encode buffer.
            // Zero heap interaction for small frames — the majority of
            // pub/sub control messages (Subscribe, Ack, heartbeat, small
            // Publishes) fit here, avoiding BytesMut overhead entirely.
            let mut stack_buf = [0u8; STACK_ENCODE_MAX];
            let written = frame::codec::encode_into_slice(
                &mut stack_buf[..wire_size],
                header,
                payload,
            )
            .map_err(|e| ConnectError::Config(format!("frame encode failed: {e}")))?;
            return self.stream_send(stream_id, &stack_buf[..written]);
        }

        // Rendezvous path: heap buffer for large payloads (> 2 KiB).
        // encode_buf is reused across calls — clear + reserve is zero-alloc
        // after warmup (capacity grows once to the largest frame and stays).
        self.encode_buf.clear();
        self.encode_buf.reserve(wire_size);
        frame::codec::encode_into(&mut self.encode_buf, header, payload)
            .map_err(|e| ConnectError::Config(format!("frame encode failed: {e}")))?;
        let encoded = std::mem::take(&mut self.encode_buf);
        let result = self.stream_send(stream_id, &encoded);
        self.encode_buf = encoded;
        result
    }

    /// Send raw bytes on a QUIC stream (opens it implicitly on first write).
    ///
    /// Handles partial writes: when QUIC flow control limits the write
    /// (`stream_send` returns fewer bytes than requested, or `Error::Done`),
    /// the unwritten tail is buffered in [`Transport::pending`] and retried
    /// by [`flush_pending`] on subsequent iterations. This prevents the
    /// silent frame-truncation that caused server-side `CrcMismatch` and
    /// decoder desync.
    fn stream_send(&mut self, stream_id: u64, data: &[u8]) -> Result<(), ConnectError> {
        // If there's already pending data for this stream, append to it.
        // The new data must go AFTER the buffered tail to preserve byte
        // ordering on the stream.
        let owned;
        let buf: &[u8] = match self.pending.get_mut(&stream_id) {
            Some(pending) => {
                pending.extend_from_slice(data);
                owned = std::mem::take(pending);
                &owned
            }
            None => data,
        };

        let mut offset = 0;
        while offset < buf.len() {
            match self.conn.stream_send(stream_id, &buf[offset..], false) {
                Ok(n) => {
                    offset += n;
                    // n == 0 means no capacity; stop looping and buffer
                    // the rest. flush_pending will retry after the peer
                    // opens the window.
                    if n == 0 {
                        break;
                    }
                }
                Err(quiche::Error::Done) => {
                    // No capacity right now; break and buffer the rest.
                    break;
                }
                Err(quiche::Error::FinalSize) => {
                    self.pending.remove(&stream_id);
                    self.sync_pending_shared();
                    return Err(ConnectError::Closed("stream already closed".into()));
                }
                Err(e) => {
                    self.pending.remove(&stream_id);
                    self.sync_pending_shared();
                    return Err(ConnectError::Quiche(e));
                }
            }
        }

        if offset < buf.len() {
            // Store the unwritten tail for retry on the next iteration.
            // Single allocation: BytesMut::from_iter copies the slice
            // directly (previously .to_vec() + from_iter = 2 allocs).
            self.pending
                .insert(stream_id, BytesMut::from_iter(&buf[offset..]));
        } else {
            // Everything written; clear any stale pending entry.
            self.pending.remove(&stream_id);
        }
        self.sync_pending_shared();

        Ok(())
    }

    /// Drain pending partial writes with a timeout. Loops until all
    /// buffered tails are flushed or `timeout` elapses. Returns `true`
    /// if everything was drained, `false` on timeout.
    ///
    /// Called by the connection task during graceful shutdown to ensure
    /// in-flight publishes aren't silently dropped when `close()` fires.
    pub(crate) async fn drain_pending_gracefully(&mut self, timeout: Duration) -> bool {
        if !self.has_pending() {
            return true;
        }
        let deadline = Instant::now() + timeout;
        while self.has_pending() && Instant::now() < deadline {
            self.drain_recv();
            self.flush_pending();
            if let Err(e) = self.flush() {
                tracing::warn!(error = %e, "[transport] drain flush error");
                return false;
            }
            if self.has_pending() {
                let next = self.next_event_deadline(Some(deadline));
                self.wait_for_event(next).await;
            }
        }
        !self.has_pending()
    }

    /// Returns `true` if any stream has buffered partial-write data awaiting
    /// a flow-control window from the peer.
    #[inline]
    pub(crate) fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    /// Total bytes buffered across all streams in [`Transport::pending`].
    /// Used for diagnostic logging during graceful drain.
    pub(crate) fn pending_bytes(&self) -> usize {
        self.pending.values().map(BytesMut::len).sum()
    }

    /// Returns a clone of the shared atomic for [`Client::pending_bytes`].
    /// The connection task updates this on every stream_send / flush_pending;
    /// publishers read it to detect QUIC flow-control backpressure before
    /// the cmd channel fills.
    #[allow(dead_code)]
    pub(crate) fn pending_shared(&self) -> Arc<AtomicUsize> {
        self.pending_shared.clone()
    }

    /// Replace the internal pending-shared mirror with the one shared
    /// with the Client. Called once by the connection task after
    /// [`Transport::connect`] so the Client sees live updates.
    pub(crate) fn set_pending_shared(&mut self, shared: Arc<AtomicUsize>) {
        self.pending_shared = shared;
        self.sync_pending_shared();
    }

    /// Sync the shared atomic to match the actual pending total. Called
    /// after every mutation of `pending` so publishers see a current value.
    fn sync_pending_shared(&self) {
        self.pending_shared
            .store(self.pending_bytes(), Ordering::Relaxed);
    }

    /// Retry pending partial writes before flushing. Called at the top of
    /// each connection-task iteration so buffered tails get drained as soon
    /// as the peer opens the flow-control window (MAX_STREAM_DATA).
    ///
    /// Uses `stream_capacity` to skip streams that still have no room,
    /// avoiding unnecessary `stream_send` calls that would just return 0.
    pub(crate) fn flush_pending(&mut self) {
        if self.pending.is_empty() {
            return;
        }
        // Snapshot stream IDs to avoid borrow conflicts during iteration.
        let ids: Vec<u64> = self.pending.keys().copied().collect();
        for sid in ids {
            // Take ownership of the buffer so we can call stream_send
            // (which needs &mut self) without holding a borrow on pending.
            let Some(buf) = self.pending.remove(&sid) else {
                continue;
            };
            // Re-send the buffered tail. If it still partial-writes,
            // stream_send will re-buffer the remainder.
            if let Err(e) = self.stream_send(sid, &buf) {
                tracing::warn!(stream_id = sid, error = %e, "[transport] flush_pending failed");
            }
        }
    }

    /// Drain quiche's send queue onto the UDP socket.
    pub(crate) fn flush(&mut self) -> Result<(), ConnectError> {
        loop {
            match self.conn.send(&mut self.send_buf) {
                Ok((n, info)) => match self.sock.try_send_to(&self.send_buf[..n], info.to) {
                    Ok(_) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(e) => return Err(ConnectError::Io(e)),
                },
                Err(quiche::Error::Done) => return Ok(()),
                Err(e) => return Err(ConnectError::Quiche(e)),
            }
        }
    }

    /// Fire quiche's loss-recovery timers. The run loop calls this only when
    /// the deadline returned by [`Self::next_event_deadline`] has actually
    /// elapsed — quiche's `on_timeout` should not be called prematurely.
    /// quiche 0.22's `on_timeout` takes no arguments (it derives "now" from
    /// its configured clock internally).
    pub(crate) fn fire_timeout(&mut self) {
        self.conn.on_timeout();
    }

    /// Earliest instant the run loop should wake: `now + conn.timeout()`, or a
    /// 200 ms keep-alive poll when quiche reports no pending timer. `cap`
    /// clamps the result (e.g. the handshake deadline).
    ///
    /// `conn.timeout()` returns an `Option<Duration>` (time *until* the next
    /// loss-recovery event), so the deadline is `now + duration`.
    pub(crate) fn next_event_deadline(&self, cap: Option<Instant>) -> Instant {
        let now = Instant::now();
        let keepalive = now + Duration::from_millis(200);
        let quiche_t = match self.conn.timeout() {
            Some(d) => now + d,
            None => keepalive,
        };
        match cap {
            Some(c) => quiche_t.min(c).min(keepalive),
            None => quiche_t.min(keepalive),
        }
    }

    /// Wait until the socket is readable or `deadline` is reached, whichever
    /// comes first. Called by the connection task between pump cycles so the
    /// task never busy-loops. Borrows `&self` so it composes with the `&mut`
    /// pump methods called before and after.
    pub(crate) async fn wait_for_event(&self, deadline: Instant) {
        tokio::select! {
            biased;
            _ = self.sock.readable() => {}
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {}
        }
    }

    /// `true` once the QUIC connection is closed (by us, the peer, or an error).
    #[inline]
    pub(crate) fn is_closed(&self) -> bool {
        self.conn.is_closed()
    }

    /// Trigger QUIC connection migration by rebinding the UDP socket to a
    /// new local address.
    ///
    /// The quiche `Connection` is **not** recreated — it retains the same
    /// DCID, so the server recognises subsequent packets as belonging to the
    /// existing connection. The server validates the new path with
    /// PATH_CHALLENGE/PATH_RESPONSE automatically (migration is enabled
    /// server-side via `ExtensionFlags.migration`).
    ///
    /// On success:
    /// - `self.sock` is replaced with a fresh socket bound to `bind_addr`
    ///   (use `"0.0.0.0:0"` for a new ephemeral port).
    /// - `self.local` reflects the new local address.
    /// - A probe packet is flushed immediately so the server begins path
    ///   validation without waiting for the next publish.
    ///
    /// Typical triggers: WiFi → cellular handoff, VPN connect/disconnect,
    /// or any scenario where the local IP changes but the session should
    /// survive.
    pub(crate) fn rebind(&mut self, bind_addr: &str) -> Result<(), ConnectError> {
        // Use std::net for synchronous bind+connect, then hand off to tokio.
        // Both are fast syscalls (no handshake for UDP); this keeps the
        // method callable from the sync handle_cmd path.
        let std_sock = std::net::UdpSocket::bind(bind_addr)?;
        std_sock.set_nonblocking(true)?;
        std_sock.connect(self.peer)?;

        let new_sock = UdpSocket::from_std(std_sock)?;
        let new_local = new_sock.local_addr()?;

        self.sock = new_sock;
        self.local = new_local;

        // Flush any pending quiche output so the server sees traffic from
        // the new 4-tuple immediately and starts path validation.
        self.flush()?;

        tracing::info!(
            local = %new_local,
            peer = %self.peer,
            "[transport] connection migrated — UDP socket rebound"
        );
        Ok(())
    }

    /// Close the connection gracefully with a peer-app error code.
    pub(crate) fn close(&mut self) {
        let _ = self.conn.close(true, 0x00, b"bye");
        let _ = self.flush();
    }

    /// Extract the session ticket (if available) for use in 0-RTT
    /// resumption on a future reconnect. Returns `None` until the
    /// server has sent NEW_SESSION_TICKET (typically after the
    /// handshake completes).
    pub(crate) fn session_ticket(&self) -> Option<Vec<u8>> {
        self.conn.session().map(|s| s.to_vec())
    }

    /// `true` if this connection was established via session resumption
    /// (abbreviated handshake) rather than a full TLS handshake.
    pub(crate) fn is_resumed(&self) -> bool {
        self.conn.is_resumed()
    }

    /// Build a [`ConnectError`] describing why the connection closed.
    /// v1 keeps this opaque; quiche 0.22 exposes richer detail via
    /// `peer_error()` / `local_error()` that can be surfaced later.
    fn closed_reason(&self) -> ConnectError {
        ConnectError::Closed("connection closed by peer".into())
    }

    /// Borrowed access for the connection task (e.g. to query application_proto).
    #[inline]
    #[allow(dead_code)]
    pub(crate) fn conn(&self) -> &Connection {
        &self.conn
    }
}

// ── quiche config ──────────────────────────────────────────────────

/// Build a `quiche::Config` matching the demo client's transport settings and
/// applying the requested [`TlsVerify`] policy plus optional client identity.
fn build_quiche_config(
    tls: &TlsVerify,
    idle_timeout: Duration,
    client_identity: Option<&ClientIdentity>,
) -> Result<quiche::Config, ConnectError> {
    let mut config = quiche::Config::new(quiche::PROTOCOL_VERSION)?;
    config.set_application_protos(&[ALPN])?;

    // ── Client identity (mTLS) ─────────────────────────────────────
    // Loaded before verify_peer so the cert chain is ready when the
    // handshake starts. Independent of TlsVerify: a client can both
    // pin the server cert (Strict/Pinned) and present its own.
    if let Some(id) = client_identity {
        let cert_str = id.cert.to_str().ok_or_else(|| {
            ConnectError::Tls(format!("cert path not valid UTF-8: {}", id.cert.display()))
        })?;
        let key_str = id.key.to_str().ok_or_else(|| {
            ConnectError::Tls(format!("key path not valid UTF-8: {}", id.key.display()))
        })?;
        config
            .load_cert_chain_from_pem_file(cert_str)
            .map_err(|e| ConnectError::Tls(format!("failed to load client cert: {e}")))?;
        config
            .load_priv_key_from_pem_file(key_str)
            .map_err(|e| ConnectError::Tls(format!("failed to load client key: {e}")))?;
    }
    // The effective idle timeout is min(client, server). The server default
    // is 30 s; honour the user's request but never go below 1 s to avoid
    // pathologically aggressive teardowns on slow links.
    let idle_ms = idle_timeout.as_millis().max(1000) as u64;
    config.set_max_idle_timeout(idle_ms);
    config.set_max_recv_udp_payload_size(DGRAM_BUF);
    config.set_max_send_udp_payload_size(DGRAM_BUF);
    config.set_initial_max_data(100 * 1024 * 1024);
    config.set_initial_max_stream_data_bidi_local(10 * 1024 * 1024);
    config.set_initial_max_stream_data_bidi_remote(10 * 1024 * 1024);
    config.set_initial_max_stream_data_uni(10 * 1024 * 1024);
    config.set_initial_max_streams_bidi(1024);
    config.set_initial_max_streams_uni(1024);
    config.set_disable_active_migration(false);
    // Enable session resumption (0-RTT early data on reconnect).
    config.enable_early_data();
    config.discover_pmtu(false);
    config.set_cc_algorithm(quiche::CongestionControlAlgorithm::CUBIC);

    // ── TLS verification ────────────────────────────────────────────
    // quiche 0.22 exposes verify_peer + CA-file loading via Config. A true
    // pinning / trust-on-first-use verifier needs a custom certificate
    // verifier (tracked separately); v1 maps those to the dev (non-verifying)
    // path so the common cases are honest about what they do.
    match tls {
        TlsVerify::Strict { ca } => {
            let ca_str = ca.to_str().ok_or_else(|| {
                ConnectError::Tls(format!("CA path not valid UTF-8: {}", ca.display()))
            })?;
            config
                .load_verify_locations_from_file(ca_str)
                .map_err(|e| ConnectError::Tls(format!("failed to load CA bundle: {e}")))?;
            config.verify_peer(true);
        }
        TlsVerify::Pinned { .. } => {
            tracing::warn!(
                "[tls] Pinned verification is not yet enforced (no custom verifier); \
                 treating as trust-on-first-use for v1"
            );
            config.verify_peer(false);
        }
        TlsVerify::Tofu | TlsVerify::DangerAcceptInvalid => {
            config.verify_peer(false);
        }
    }

    Ok(config)
}

/// Monotonically-increasing 8-byte source connection id (debug-friendly).
fn gen_scid() -> Vec<u8> {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(1);
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    n.to_be_bytes().to_vec()
}
