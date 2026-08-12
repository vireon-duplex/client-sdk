//! Payload acquisition: inline arg / `--file` / `--stdin`.

use std::io::Read as _;
use std::path::PathBuf;

use bytes::Bytes;

use crate::error::CliError;

/// Resolve a publish payload from exactly one of the three sources.
///
/// Precedence: `--stdin` > `--file` > inline positional. This mirrors the
/// original `pub` / `stream pub` behaviour.
pub fn read_payload(
    inline: Option<String>,
    file: Option<PathBuf>,
    stdin: bool,
) -> Result<Bytes, CliError> {
    if stdin {
        let mut buf = Vec::new();
        std::io::stdin()
            .read_to_end(&mut buf)?;
        return Ok(Bytes::from(buf));
    }
    if let Some(path) = file {
        let buf = std::fs::read(&path).map_err(|e| {
            CliError::BadArg(format!("read {}: {e}", path.display()))
        })?;
        return Ok(Bytes::from(buf));
    }
    if let Some(s) = inline {
        return Ok(Bytes::from(s.into_bytes()));
    }
    Err(CliError::BadArg(
        "no payload provided (pass an inline arg, --file, or --stdin)".into(),
    ))
}
