//! Generic receive loop shared by `sub`, `stream sub`, and `group sub`.
//!
//! The [`MsgRecv`] trait erases the difference between the three SDK
//! receiver types ([`Subscription`], [`StreamHandle`], [`GroupSubscription`])
//! so a single [`recv_loop`] drives all of them with identical Ctrl+C /
//! `--count` semantics.

use std::io::Write as _;

use vireon_sdk::{GroupSubscription, Message, StreamHandle, Subscription};

use crate::output::print_msg;

/// Erased async `recv` over the three SDK receiver types.
pub trait MsgRecv {
    /// Await the next message, or `None` if the underlying channel closed.
    async fn recv(&mut self) -> Option<Message>;
}

impl MsgRecv for Subscription {
    async fn recv(&mut self) -> Option<Message> {
        Subscription::recv(self).await
    }
}

impl MsgRecv for StreamHandle {
    async fn recv(&mut self) -> Option<Message> {
        StreamHandle::recv(self).await
    }
}

impl MsgRecv for GroupSubscription {
    async fn recv(&mut self) -> Option<Message> {
        GroupSubscription::recv(self).await
    }
}

/// Print messages from `rx` until the channel closes, Ctrl+C is pressed,
/// or `count` messages have been received (if set).
pub async fn recv_loop<R: MsgRecv + Unpin>(rx: &mut R, format: &str, count: Option<u64>) {
    let mut n = 0u64;
    loop {
        let msg = tokio::select! {
            m = rx.recv() => match m {
                Some(m) => m,
                None => {
                    eprintln!("(channel closed)");
                    return;
                }
            },
            _ = tokio::signal::ctrl_c() => {
                eprintln!("(interrupted)");
                return;
            }
        };
        print_msg(&msg, format);
        n += 1;
        let _ = std::io::stdout().flush();
        if let Some(c) = count {
            if n >= c {
                return;
            }
        }
    }
}
