//! Connection pool that multiplexes N independent QUIC connections.
//!
//! A single [`Client`] funnels every publish through one command channel and
//! one QUIC connection — fine for moderate load, but the command channel
//! becomes the bottleneck once `try_publish` starts hitting `NotConnected`
//! under bursty fan-out. [`ClientPool`] removes that bottleneck by round-
//! robining publishes across N members.
//!
//! ## What the pool does
//!
//! - **Publish round-robin** with failover: `try_publish` tries members in
//!   round-robin order until one accepts; `publish().await` picks the next
//!   member and awaits it, falling back to the next on `NotConnected`.
//! - **Aggregate backpressure**: `pending_bytes()` sums across all members,
//!   so a producer can throttle when the pool as a whole is saturating.
//! - **Per-member access**: `member(idx)` exposes the underlying [`Client`]
//!   for `subscribe` / `open_stream` — those operations are per-connection
//!   and cannot be transparently multiplexed (the server tracks which
//!   connection owns each subscription).
//!
//! ## What the pool does NOT do
//!
//! - **Subscribe fan-out**: calling `subscribe("foo.*")` on every member
//!   would deliver N copies of each message (once per subscribed
//!   connection). Subscribe on a specific member instead.
//! - **Stream multiplexing**: a dedicated QUIC stream is bound to the
//!   connection that opened it. Open streams via `member(idx).open_stream()`.
//!
//! ## Reconnect
//!
//! Each member's connection task reconnects independently per its
//! [`ReconnectPolicy`](crate::ReconnectPolicy). The pool keeps routing
//! publishes to whichever members are alive; a dead member simply returns
//! `NotConnected` and the failover walks past it.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;

use crate::config::ClientBuilder;
use crate::connection::Client;
use crate::error::{ConnectError, PublishError};
use crate::message::Payload;

/// Shared inner state — cheap to clone via [`ClientPool`]'s `Clone` impl.
struct PoolInner {
    members: Vec<Client>,
    next: AtomicUsize,
}

/// Pool of N independent [`Client`] connections for higher aggregate
/// publish throughput.
///
/// Construct with [`ClientPool::connect`]. See the [module docs](self) for
/// the semantics and limitations.
#[derive(Clone)]
pub struct ClientPool {
    inner: Arc<PoolInner>,
}

impl ClientPool {
    /// Connect `n` independent clients using clones of `builder`.
    ///
    /// The members connect concurrently; the call returns once every member
    /// has completed its handshake (or the first failure short-circuits).
    /// On failure, members that did connect are dropped (their background
    /// tasks exit when the `Client` handles drop).
    ///
    /// # Errors
    /// [`ConnectError::Config`] if `n == 0`; otherwise the first
    /// [`ConnectError`] encountered from any member.
    pub async fn connect(builder: ClientBuilder, n: usize) -> Result<Self, ConnectError> {
        if n == 0 {
            return Err(ConnectError::Config(
                "pool size must be greater than zero".into(),
            ));
        }

        // Spawn N connect tasks concurrently so the handshakes overlap.
        let mut handles = Vec::with_capacity(n);
        for _ in 0..n {
            let b = builder.clone();
            handles.push(tokio::spawn(async move { b.connect().await }));
        }

        let mut members = Vec::with_capacity(n);
        for h in handles {
            match h.await {
                Ok(Ok(c)) => members.push(c),
                Ok(Err(e)) => return Err(e),
                Err(join_err) => {
                    return Err(ConnectError::Closed(format!(
                        "pool member connect task panicked: {join_err}"
                    )));
                }
            }
        }

        Ok(Self {
            inner: Arc::new(PoolInner {
                members,
                next: AtomicUsize::new(0),
            }),
        })
    }

    /// Build a pool from already-connected clients.
    ///
    /// Useful when the caller wants to control the connection strategy
    /// (e.g. sequential connects with custom retry logic) rather than
    /// relying on the concurrent connect-all-at-once path in
    /// [`connect`](Self::connect).
    ///
    /// # Panics
    /// Panics if `members` is empty.
    #[must_use]
    pub fn from_clients(members: Vec<Client>) -> Self {
        assert!(
            !members.is_empty(),
            "ClientPool::from_clients requires at least one member"
        );
        Self {
            inner: Arc::new(PoolInner {
                members,
                next: AtomicUsize::new(0),
            }),
        }
    }

    /// Number of pool members.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.members.len()
    }

    /// Whether the pool is empty (always `false` after a successful
    /// [`connect`](Self::connect), since `n == 0` is rejected).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.members.is_empty()
    }

    /// Access a specific pool member by index (for `subscribe` /
    /// `open_stream` — those operations are connection-bound and cannot be
    /// transparently multiplexed).
    ///
    /// # Panics
    /// Panics if `idx >= len()`.
    #[must_use]
    pub fn member(&self, idx: usize) -> &Client {
        &self.inner.members[idx]
    }

    /// Total bytes buffered across ALL members' pending-write queues.
    /// Non-zero means the pool is falling behind — producers should yield
    /// briefly to let the server drain.
    #[must_use]
    pub fn pending_bytes(&self) -> usize {
        self.inner
            .members
            .iter()
            .map(|c| c.pending_bytes())
            .sum()
    }

    /// Pick the next round-robin index (wrap-around).
    fn next_idx(&self) -> usize {
        let n = self.inner.members.len();
        // Relaxed is fine: we only need spread, not strict fairness.
        let prev = self.inner.next.fetch_add(1, Ordering::Relaxed);
        prev % n
    }

    /// Fire-and-forget publish — round-robins across members, trying each
    /// until one accepts the command.
    ///
    /// Unlike [`Client::try_publish`], this version walks the pool on
    /// `NotConnected` so a single saturated member does not fail the
    /// publish. Returns `NotConnected` only if every member's command
    /// channel is full or closed.
    ///
    /// # Errors
    /// [`PublishError::NotConnected`] if no member accepted the publish.
    pub fn try_publish(&self, topic: &str, payload: impl Payload) -> Result<(), PublishError> {
        let bytes: Bytes = payload.into_bytes();
        let n = self.inner.members.len();
        let start = self.next_idx();
        for i in 0..n {
            let idx = (start + i) % n;
            // Bytes clone is cheap (ref-counted).
            if self.inner.members[idx]
                .try_publish(topic, bytes.clone())
                .is_ok()
            {
                return Ok(());
            }
        }
        Err(PublishError::NotConnected)
    }

    /// Async publish — picks the next member round-robin and awaits the
    /// connection task's confirmation.
    ///
    /// Falls back to the next member on `NotConnected` so a dead member
    /// doesn't block the publish indefinitely.
    ///
    /// # Errors
    /// The first non-`NotConnected` error short-circuits. If every member
    /// returns `NotConnected`, returns `NotConnected`.
    pub async fn publish(&self, topic: &str, payload: impl Payload) -> Result<(), PublishError> {
        let bytes: Bytes = payload.into_bytes();
        let n = self.inner.members.len();
        let start = self.next_idx();
        for i in 0..n {
            let idx = (start + i) % n;
            match self.inner.members[idx].publish(topic, bytes.clone()).await {
                Ok(()) => return Ok(()),
                Err(PublishError::NotConnected) => continue,
                Err(other) => return Err(other),
            }
        }
        Err(PublishError::NotConnected)
    }

    /// Close every member. Each member drains its pending writes per its
    /// own close path (see [`Client::close`]).
    ///
    /// # Errors
    /// Returns the first [`ConnectError`] encountered; other members are
    /// still attempted but their errors are swallowed.
    pub async fn close(&self) -> Result<(), ConnectError> {
        let mut first_err: Option<ConnectError> = None;
        for c in &self.inner.members {
            if let Err(e) = c.close().await {
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}
