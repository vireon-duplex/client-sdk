//! Per-command handlers. Each submodule owns exactly one CLI subcommand
//! (or sub-subcommand) and exposes a single `run` / `run_*` entry point.

pub(crate) mod group;
pub(crate) mod mux;
pub(crate) mod ping;
pub(crate) mod pubsub;
pub(crate) mod stream;

pub(crate) use group::run_group;
pub(crate) use mux::run_mux;
pub(crate) use ping::run_ping;
pub(crate) use pubsub::{run_pub, run_sub};
pub(crate) use stream::run_stream;
