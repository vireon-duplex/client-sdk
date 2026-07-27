//! Received-message and QoS types shared across the SDK surface.

use bytes::Bytes;

/// A message delivered to a [`Subscription`] or [`StreamHandle`].
///
/// `topic` and `payload` are reference-counted [`Bytes`], so cloning a
/// `Message` (e.g. to fan it out to several consumers) never copies the body.
///
/// [`Subscription`]: crate::Subscription
/// [`StreamHandle`]: crate::StreamHandle
#[derive(Clone, Debug)]
pub struct Message {
    /// The destination topic the publisher addressed.
    pub topic: Bytes,
    /// Opaque application bytes — Vireon never inspects these.
    pub payload: Bytes,
    /// Per-stream sequence number assigned by the sender.
    pub seq: u64,
    /// Logical stream id this frame travelled on (matches `stream_id()` on a
    /// [`StreamHandle`], or `0` for the default pub/sub channel).
    ///
    /// [`StreamHandle`]: crate::StreamHandle
    pub stream_id: u64,
}

/// Quality-of-service hint attached to a subscription.
///
/// The byte value is written into the `Subscribe` payload and is
/// application-defined on the server side. The two common interpretations are
/// provided here for ergonomics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Qos {
    /// At-most-once delivery (default).
    AtMostOnce = 0,
    /// At-least-once delivery.
    AtLeastOnce = 1,
}

impl Qos {
    /// Wire byte for this QoS.
    #[inline]
    #[must_use]
    pub const fn as_byte(self) -> u8 {
        self as u8
    }

    /// Reconstruct a QoS from its wire byte. Any non-zero value is treated as
    /// [`Qos::AtLeastOnce`].
    #[inline]
    #[must_use]
    pub const fn from_byte(b: u8) -> Self {
        if b >= 1 {
            Self::AtLeastOnce
        } else {
            Self::AtMostOnce
        }
    }
}

impl Default for Qos {
    fn default() -> Self {
        Self::AtMostOnce
    }
}

// ── Payload: accept anything bytes-like at the publish call site ────

/// Anything that can be converted into a message payload.
///
/// Implemented for the common byte containers so callers can write
/// `client.publish("t", b"hello")`, `client.publish("t", "hello")`,
/// `client.publish("t", vec![...])`, or `client.publish("t", bytes)`.
pub trait Payload {
    /// Consume into [`Bytes`].
    fn into_bytes(self) -> Bytes;
}

impl Payload for Bytes {
    #[inline]
    fn into_bytes(self) -> Bytes {
        self
    }
}

impl Payload for Vec<u8> {
    #[inline]
    fn into_bytes(self) -> Bytes {
        Bytes::from(self)
    }
}

impl Payload for &[u8] {
    #[inline]
    fn into_bytes(self) -> Bytes {
        Bytes::copy_from_slice(self)
    }
}

impl<const N: usize> Payload for &[u8; N] {
    #[inline]
    fn into_bytes(self) -> Bytes {
        Bytes::copy_from_slice(self.as_slice())
    }
}

impl Payload for &str {
    #[inline]
    fn into_bytes(self) -> Bytes {
        Bytes::copy_from_slice(self.as_bytes())
    }
}

impl Payload for String {
    #[inline]
    fn into_bytes(self) -> Bytes {
        Bytes::from(self)
    }
}
