//! The volume reached over a socket, so actus never links the engine.
//!
//! The protocol is one request a line and one answer a line, each a JSON object, and a
//! payload crosses as hex for the reason a payload crosses that way at the engine: a line
//! is text and a payload is bytes.
//!
//! This is one [`Volume`] among the implementations a build can pass. The names and the
//! record are the mapping's, in [`super::volume`]; what is here is only how the verbs
//! travel.

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;

use super::volume::Volume;

/// How long a call waits for the store's answer before giving up.
///
/// A store that stops answering must not hold a saver thread forever, and every verb is
/// either a read or an idempotent write, so the call is worth failing instead of waiting on.
const CALL_TIMEOUT: Duration = Duration::from_secs(30);

/// One connection's two halves, kept together so a buffered read cannot lose the bytes
/// that follow the answer it wanted.
struct Link {
    reader: BufReader<UnixStream>,
    writer: UnixStream,
}

pub struct SocketVolume {
    /// The socket the volume is served on.
    socket: PathBuf,
    link: Mutex<Option<Link>>,
}

impl SocketVolume {
    pub fn new(socket: PathBuf) -> Self {
        Self {
            socket,
            link: Mutex::new(None),
        }
    }

    fn connect(&self) -> Result<Link, String> {
        let writer = UnixStream::connect(&self.socket)
            .map_err(|e| format!("connect to {}: {e}", self.socket.display()))?;
        let reader = writer
            .try_clone()
            .map_err(|e| format!("clone the store connection: {e}"))?;
        writer
            .set_read_timeout(Some(CALL_TIMEOUT))
            .map_err(|e| format!("set the store read timeout: {e}"))?;
        Ok(Link {
            reader: BufReader::new(reader),
            writer,
        })
    }

    /// One request and its answer. A connection that fails is dropped and the request is
    /// sent once more: every verb this store uses is a read or an idempotent write, so a
    /// repeat after an unreadable answer cannot land twice.
    fn call(&self, request: &serde_json::Value) -> Result<serde_json::Value, String> {
        let mut guard = self.link.lock().unwrap();
        let mut last = String::from("the store was not reached");
        // Two attempts: the second is the retry after a dropped connection.
        for _ in 0..2 {
            if guard.is_none() {
                match self.connect() {
                    Ok(link) => *guard = Some(link),
                    Err(why) => {
                        last = why;
                        continue;
                    }
                }
            }
            let link = guard.as_mut().expect("a link was just opened");
            let line = format!("{request}\n");
            let sent = link
                .writer
                .write_all(line.as_bytes())
                .and_then(|()| link.writer.flush());
            if let Err(e) = sent {
                last = format!("write to the store: {e}");
                *guard = None;
                continue;
            }
            let mut answer = String::new();
            match link.reader.read_line(&mut answer) {
                Ok(0) => {
                    last = "the store closed the connection".to_string();
                    *guard = None;
                    continue;
                }
                Ok(_) => {}
                Err(e) => {
                    last = format!("read from the store: {e}");
                    *guard = None;
                    continue;
                }
            }
            let value: serde_json::Value = serde_json::from_str(answer.trim())
                .map_err(|e| format!("the store's answer is not JSON: {e}"))?;
            if value["ok"] == serde_json::Value::Bool(true) {
                return Ok(value);
            }
            return Err(format!(
                "the store refused: {}",
                value["error"].as_str().unwrap_or("no reason given")
            ));
        }
        Err(last)
    }
}

impl Volume for SocketVolume {
    fn count(&self) -> Result<u64, String> {
        let described = self.call(&serde_json::json!({ "verb": "describe" }))?;
        Ok(described["count"].as_u64().unwrap_or(0))
    }

    fn write_named(
        &self,
        name: &str,
        origin: &str,
        media_type: &str,
        payload: &[u8],
        creator: &str,
    ) -> Result<(), String> {
        let request = serde_json::json!({
            "verb": "write_fact_named",
            "name": name,
            "origin": origin,
            "media_type": media_type,
            "hex": to_hex(payload),
            "creator": creator,
        });
        self.call(&request).map(|_| ())
    }

    fn read_payload(&self, name: &str) -> Result<Option<Vec<u8>>, String> {
        let request = serde_json::json!({ "verb": "read_payload", "name": name });
        let answer = self.call(&request)?;
        match answer["hex"].as_str() {
            Some(hex) => Ok(Some(from_hex(hex)?)),
            None => Ok(None),
        }
    }

    fn place(&self) -> String {
        self.socket.display().to_string()
    }
}

fn to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        text.push(HEX[(byte >> 4) as usize] as char);
        text.push(HEX[(byte & 0x0f) as usize] as char);
    }
    text
}

fn from_hex(text: &str) -> Result<Vec<u8>, String> {
    let digits = text.as_bytes();
    if !digits.len().is_multiple_of(2) {
        return Err(format!(
            "hex has {} digits, which is not a whole number of bytes",
            digits.len()
        ));
    }
    let mut bytes = Vec::with_capacity(digits.len() / 2);
    for pair in digits.as_chunks::<2>().0 {
        let high = hex_digit(pair[0])?;
        let low = hex_digit(pair[1])?;
        bytes.push((high << 4) | low);
    }
    Ok(bytes)
}

fn hex_digit(digit: u8) -> Result<u8, String> {
    match digit {
        b'0'..=b'9' => Ok(digit - b'0'),
        b'a'..=b'f' => Ok(digit - b'a' + 10),
        b'A'..=b'F' => Ok(digit - b'A' + 10),
        other => Err(format!("{:?} is not a hex digit", other as char)),
    }
}
