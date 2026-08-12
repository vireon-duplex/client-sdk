//! `vireon` — CLI for the Vireon QUIC pub/sub runtime.
//!
//! A redis-cli-style tool for manual testing and operations against a Vireon
//! server. Built on `vireon_sdk` (it does NOT reimplement the wire protocol).
//!
//! ```text
//! vireon ping
//! vireon pub sensor.temp "23.5C"
//! vireon sub "sensor.*"
//! vireon stream pub video.frame data.bin --policy latest_only
//! vireon group sub jobs.tasks workers worker-1
//! vireon mux sub --stream video=video.frame:latest_only \
//!                --stream chat=chat.msg:reliable_ordered
//! ```

#![allow(clippy::print_stdout)]
#![allow(clippy::print_stderr)]

mod cli;
mod commands;
mod error;
mod output;
mod parse;
mod payload;
mod recv;

use clap::Parser;

use vireon_sdk::{ClientBuilder, ClientIdentity};

use crate::cli::{Cli, Command};
use crate::error::CliError;
use crate::parse::parse_tls_verify;

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let cli = Cli::parse();

    if let Err(e) = run(cli).await {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

/// Build the [`ClientBuilder`] from the global CLI options, then dispatch
/// the requested subcommand.
async fn run(cli: Cli) -> Result<(), CliError> {
    let addr = cli.addr.clone();
    let builder = build_builder(&cli)?;

    match cli.command {
        Command::Ping => commands::run_ping(builder, &addr).await,
        Command::Pub {
            topic,
            payload,
            file,
            stdin,
        } => commands::run_pub(builder, topic, payload, file, stdin).await,
        Command::Sub {
            pattern,
            format,
            count,
        } => commands::run_sub(builder, pattern, format, count, addr).await,
        Command::Stream { op } => commands::run_stream(op, builder).await,
        Command::Group { op } => commands::run_group(op, builder).await,
        Command::Mux { op } => commands::run_mux(op, builder).await,
    }
}

/// Translate the global CLI flags (`--addr`, `--tls-verify`, `--sni`,
/// `--client-cert`, `--client-key`) into a configured [`ClientBuilder`].
fn build_builder(cli: &Cli) -> Result<ClientBuilder, CliError> {
    let tls = parse_tls_verify(&cli.tls_verify)?;
    let mut builder = ClientBuilder::new(&cli.addr);
    if let Some(sni) = &cli.sni {
        builder = builder.sni(sni);
    }
    builder = builder.tls_verify(tls);
    if let (Some(cert), Some(key)) = (&cli.client_cert, &cli.client_key) {
        builder = builder.client_identity(ClientIdentity {
            cert: cert.clone(),
            key: key.clone(),
        });
    }
    Ok(builder)
}
