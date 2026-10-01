//! The record as records in a volume, reached over a socket.
//!
//! The volume belongs to a deployment and this store is the consumer side of the seam: it
//! speaks a wire protocol so actus never links the engine and never has to name it. The
//! protocol is one request a line and one answer a line, each a JSON object, and a payload
//! crosses as hex for the reason a payload crosses that way at the engine: a line is text
//! and a payload is bytes.
//!
//! What this store keeps is actus's vocabulary, mapped onto names.
//!
//! | What | Name |
//! |---|---|
//! | one message | `thread/<id>/<pos>` |
//! | one title version | `thread/<id>/title/<n>` |
//!
//! A name resolves to a record's address, so a record never changes and a growing value
//! cannot live under one name. That is why a title is one record per change rather than one
//! value, and why the reading side takes the last version instead of overwriting one.
//!
//! A name is also not enumerable. An extent is therefore discovered by reading upward until
//! a name answers nothing, and the list of threads has to come from somewhere that can be
//! read: `heads.json` beside this store, which holds the thread list, the routing fields,
//! and the title text as a cache. Losing it loses the list and not the records, because the
//! records are reached by name and the list is the one thing a name cannot supply.
//!
//! Once `heads.json` exists the volume is the authority and the document beside it is no
//! longer read. Switching back and forth is therefore a switch rather than a merge, and the
//! document is left untouched so that a switch back is complete.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

use super::RecordStore;
use crate::agent::{ThreadMessage, ThreadSession};

/// The origin a message record carries.
const MESSAGE_ORIGIN: &str = "message";
/// The origin a title record carries.
const TITLE_ORIGIN: &str = "title";
const MESSAGE_MEDIA_TYPE: &str = "application/json";
const TITLE_MEDIA_TYPE: &str = "text/plain; charset=utf-8";

/// The creator every record this store writes is attributed to.
///
/// A label for the writer rather than an address of one instance, so a default is allowed
/// and it is overridable: a deployment that wants its own attribution names it here.
const CREATOR_ENV: &str = "ACTUS_RECORD_STORE_CREATOR";
const DEFAULT_CREATOR: &str = "actus";

/// How long a call waits for the store's answer before giving up.
///
/// A store that stops answering must not hold a saver thread forever, and every verb is
/// either a read or an idempotent write, so the call is worth failing instead of waiting on.
const CALL_TIMEOUT: Duration = Duration::from_secs(30);

/// The name of a thread's head document, which is actus's own and is not a record.
const HEAD_FILE: &str = "heads.json";

/// One connection's two halves, kept together so a buffered read cannot lose the bytes
/// that follow the answer it wanted.
struct Link {
    reader: BufReader<UnixStream>,
    writer: UnixStream,
}

/// What the volume holds for one thread's title.
#[derive(Clone, Default)]
struct TitleState {
    versions: u64,
    text: Option<String>,
}

pub struct SocketStore {
    /// The socket the volume is served on.
    socket: PathBuf,
    /// Actus's own document for this volume: the thread list and what a listing reads.
    head: PathBuf,
    /// The document this store seeds from when it has no head of its own, which is the
    /// record the host was serving until the store took over. It is read and never written.
    seed: PathBuf,
    link: Mutex<Option<Link>>,
    /// Message positions already in the volume, so a persist appends the tail and not the
    /// whole thread. Zero for a thread seeded from the document.
    deposited: Mutex<HashMap<String, u64>>,
    titled: Mutex<HashMap<String, TitleState>>,
}

impl SocketStore {
    pub fn new(socket: PathBuf, dir: &Path) -> Self {
        Self {
            socket,
            head: dir.join(HEAD_FILE),
            seed: dir.join(super::DOCUMENT_FILE),
            link: Mutex::new(None),
            deposited: Mutex::new(HashMap::new()),
            titled: Mutex::new(HashMap::new()),
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

    fn put(&self, name: &str, origin: &str, media_type: &str, bytes: &[u8]) -> Result<(), String> {
        let request = serde_json::json!({
            "verb": "write_fact_named",
            "name": name,
            "origin": origin,
            "media_type": media_type,
            "hex": to_hex(bytes),
            "creator": creator(),
        });
        self.call(&request).map(|_| ())
    }

    fn payload(&self, name: &str) -> Result<Option<Vec<u8>>, String> {
        let request = serde_json::json!({ "verb": "read_payload", "name": name });
        let answer = self.call(&request)?;
        match answer["hex"].as_str() {
            Some(hex) => Ok(Some(from_hex(hex)?)),
            None => Ok(None),
        }
    }

    fn read_messages(&self, id: &str) -> Result<(Vec<ThreadMessage>, u64), String> {
        let mut messages = Vec::new();
        loop {
            let name = message_name(id, messages.len() as u64);
            match self.payload(&name)? {
                Some(bytes) => {
                    let message: ThreadMessage = serde_json::from_slice(&bytes)
                        .map_err(|e| format!("{name} is not a message: {e}"))?;
                    messages.push(message);
                }
                None => break,
            }
        }
        let cursor = messages.len() as u64;
        Ok((messages, cursor))
    }

    fn read_titles(&self, id: &str) -> Result<TitleState, String> {
        let mut state = TitleState::default();
        loop {
            let name = title_name(id, state.versions);
            match self.payload(&name)? {
                Some(bytes) => {
                    state.text = Some(
                        String::from_utf8(bytes)
                            .map_err(|e| format!("{name} is not text: {e}"))?,
                    );
                    state.versions += 1;
                }
                None => break,
            }
        }
        Ok(state)
    }

    /// Read a thread document, or nothing when it is not there.
    fn read_document(path: &Path) -> Result<Option<HashMap<String, ThreadSession>>, String> {
        if !path.exists() {
            return Ok(None);
        }
        let content = std::fs::read_to_string(path)
            .map_err(|e| format!("read {}: {e}", path.display()))?;
        let threads: HashMap<String, ThreadSession> = serde_json::from_str(&content)
            .map_err(|e| format!("{} is not a thread document: {e}", path.display()))?;
        Ok(Some(threads))
    }
}

impl RecordStore for SocketStore {
    fn load(&self) -> Result<HashMap<String, ThreadSession>, String> {
        let mut threads = match Self::read_document(&self.head)? {
            Some(threads) => threads,
            None => match Self::read_document(&self.seed)? {
                Some(threads) => {
                    tracing::info!(
                        "the record store has no head of its own; seeding from {}",
                        self.seed.display()
                    );
                    return Ok(threads);
                }
                None => HashMap::new(),
            },
        };
        if self.seed.exists() {
            tracing::info!(
                "{} is the authority for this volume, so {} is not read",
                self.head.display(),
                self.seed.display()
            );
        }
        for (id, thread) in threads.iter_mut() {
            let (messages, cursor) = self.read_messages(id)?;
            thread.messages = messages;
            self.deposited.lock().unwrap().insert(id.clone(), cursor);
            let title = self.read_titles(id)?;
            if title.text.is_some() {
                thread.title = title.text.clone();
            }
            self.titled.lock().unwrap().insert(id.clone(), title);
        }
        tracing::info!(
            "Loaded {} threads from the record store on {}",
            threads.len(),
            self.socket.display()
        );
        Ok(threads)
    }

    fn persist(&self, mut threads: HashMap<String, ThreadSession>) -> Result<(), String> {
        for (id, thread) in &threads {
            let from = {
                let deposited = self.deposited.lock().unwrap();
                deposited.get(id).copied().unwrap_or(0)
            };
            for position in from..thread.messages.len() as u64 {
                let message = &thread.messages[position as usize];
                let bytes = serde_json::to_vec(message)
                    .map_err(|e| format!("serialize a message of {id}: {e}"))?;
                self.put(
                    &message_name(id, position),
                    MESSAGE_ORIGIN,
                    MESSAGE_MEDIA_TYPE,
                    &bytes,
                )?;
            }
            self.deposited
                .lock()
                .unwrap()
                .insert(id.clone(), thread.messages.len() as u64);

            let mut titled = self.titled.lock().unwrap();
            let state = titled.entry(id.clone()).or_default();
            if thread.title != state.text {
                match &thread.title {
                    Some(title) => self.put(
                        &title_name(id, state.versions),
                        TITLE_ORIGIN,
                        TITLE_MEDIA_TYPE,
                        title.as_bytes(),
                    )?,
                    // A title that is gone is a version with nothing in it, so a reader
                    // sees the change instead of the version before it.
                    None => self.put(
                        &title_name(id, state.versions),
                        TITLE_ORIGIN,
                        TITLE_MEDIA_TYPE,
                        b"",
                    )?,
                }
                state.versions += 1;
                state.text = thread.title.clone();
            }
        }

        // The head is actus's own: the thread list, the routing fields, and the title text
        // as a cache. It carries no messages, which is what keeps a persist small, and the
        // snapshot this call was given is taken apart rather than copied to write it.
        for thread in threads.values_mut() {
            thread.messages.clear();
        }
        let json = serde_json::to_string(&threads).map_err(|e| format!("serialize heads: {e}"))?;
        std::fs::write(&self.head, json).map_err(|e| format!("write {}: {e}", self.head.display()))
    }
}

/// The creator a record is attributed to, from the environment.
fn creator() -> String {
    std::env::var(CREATOR_ENV).unwrap_or_else(|_| DEFAULT_CREATOR.to_string())
}

fn message_name(id: &str, position: u64) -> String {
    format!("thread/{id}/{position}")
}

fn title_name(id: &str, version: u64) -> String {
    format!("thread/{id}/title/{version}")
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
    if digits.len() % 2 != 0 {
        return Err(format!(
            "hex has {} digits, which is not a whole number of bytes",
            digits.len()
        ));
    }
    let mut bytes = Vec::with_capacity(digits.len() / 2);
    for pair in digits.chunks(2) {
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
