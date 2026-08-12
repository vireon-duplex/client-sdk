//! Message printing — text and JSON-line formats.
//!
//! Shared by the single-stream receive loop ([`print_msg`]) and the
//! multiplexed `mux sub` loop ([`print_tagged_msg`]).

use vireon_sdk::Message;

/// Print a message in the requested format (`text` or `json`).
///
/// JSON output is a single compact line per message with the payload
/// escaped as a JSON string (control chars → `\uXXXX`).
pub fn print_msg(msg: &Message, format: &str) {
    let topic = String::from_utf8_lossy(&msg.topic);
    match format {
        "json" => {
            let payload = String::from_utf8_lossy(&msg.payload);
            let payload_escaped = json_escape(&payload);
            println!(
                "{{\"topic\":\"{topic}\",\"payload\":\"{payload_escaped}\",\"seq\":{},\"stream_id\":{}}}",
                msg.seq, msg.stream_id
            );
        }
        _ => {
            let payload = String::from_utf8_lossy(&msg.payload);
            println!("{topic} = {payload}");
        }
    }
}

/// Tagged print for `mux sub` — each message is prefixed with `[label]`.
///
/// JSON variant adds a `"stream":"LABEL"` field so downstream tooling can
/// demux the interleaved lines.
pub fn print_tagged_msg(label: &str, msg: &Message, format: &str) {
    let topic = String::from_utf8_lossy(&msg.topic);
    match format {
        "json" => {
            let payload = String::from_utf8_lossy(&msg.payload);
            let payload_escaped = json_escape(&payload);
            println!(
                "{{\"stream\":\"{label}\",\"topic\":\"{topic}\",\"payload\":\"{payload_escaped}\",\"seq\":{},\"stream_id\":{}}}",
                msg.seq, msg.stream_id
            );
        }
        _ => {
            let payload = String::from_utf8_lossy(&msg.payload);
            println!("[{label:<8}] {topic} = {payload}");
        }
    }
}

/// Escape a string for embedding inside a JSON `"..."` literal.
fn json_escape(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            '"' => "\\\"".to_string(),
            '\\' => "\\\\".to_string(),
            '\n' => "\\n".to_string(),
            '\r' => "\\r".to_string(),
            '\t' => "\\t".to_string(),
            c if (c as u32) < 0x20 => format!("\\u{:04x}", c as u32),
            c => c.to_string(),
        })
        .collect()
}
