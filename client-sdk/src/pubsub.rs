//! The default-channel subscription handle.
//!
//! A [`Subscription`] is the receiving end of a topic-pattern subscription on
//! the shared default QUIC stream (stream 0). Messages that match the pattern
//! are delivered here. For per-stream delivery semantics and HOL isolation,
//! open a dedicated stream via [`Client::open_stream`] instead.
//!
//! [`Client::open_stream`]: crate::Client::open_stream

use tokio::sync::mpsc;

use crate::message::Message;

/// Receiver for messages matching a subscription pattern.
///
/// Created by [`Client::subscribe`]; not constructable externally.
///
/// [`Client::subscribe`]: crate::Client::subscribe
pub struct Subscription {
    rx: mpsc::Receiver<Message>,
}

impl Subscription {
    /// Construct from a freshly-created receiver. Called only by the
    /// connection task after it has registered the subscription and stored the
    /// matching sender in its routing table.
    #[inline]
    #[must_use]
    pub(crate) fn new(rx: mpsc::Receiver<Message>) -> Self {
        Self { rx }
    }

    /// Await the next matching message, or `None` when the subscription has
    /// been closed (client dropped, connection torn down, or channel drained).
    #[inline]
    pub async fn recv(&mut self) -> Option<Message> {
        self.rx.recv().await
    }
}
