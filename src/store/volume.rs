//! The record mapped onto a volume's names, and the verbs a volume answers.
//!
//! A name resolves to a record's address, so a record never changes and a growing value
//! cannot live under one name. That fixes the shapes: one message is one record at a
//! position, a title is one record per change, and the thread list comes from a document
//! this store owns, because a name is not enumerable.
//!
//! The mapping is separable from what carries it. [`Volume`] is the three verbs a store
//! needs and [`VolumeStore`] is the mapping over any of them. Two implementations exist: a
//! socket that reaches an engine in another process, and, for a build that links the
//! engine, the engine itself in this one. The names and the record are the same either
//! way, which is what lets a deployment move between them.
//!
//! The head carries what a windowed reader needs and the volume cannot cheaply answer: the
//! thread list, the routing fields, the title, and each thread's message count. With the
//! count in the head, a reader asks for a window of a thread by position and holds the
//! window, and the index costs one small read rather than the volume.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

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

/// The name of a thread's head document, which is actus's own and is not a record.
const HEAD_FILE: &str = "heads.json";

/// What the volume holds for one thread's title.
#[derive(Clone, Default)]
struct TitleState {
    versions: u64,
    text: Option<String>,
}

/// The head shape this build writes. A head that does not carry it is from another shape,
/// and reading one as this one would answer an empty thread list rather than fail.
const HEAD_VERSION: u32 = 1;

/// What the head file can hold: this build's shape, or the shape before the counts existed.
///
/// The bare shape is recognized rather than refused, because the volume is the authority
/// for the counts and the migration is derivable: a thread's count is one read per name
/// until the names run out, which is what the index did before the head carried them. An
/// unknown version is refused, since nothing derives it.
#[derive(serde::Deserialize)]
#[serde(untagged)]
enum HeadFile {
    Versioned(Head),
    Bare(HashMap<String, ThreadSession>),
}

/// Actus's own document for a volume.
///
/// It carries the thread list and the routing fields, as it always did, and two things the
/// record cannot supply cheaply: how many messages a thread holds, and how many title
/// versions it has. Both are derived from the records and are rebuilt from the volume by
/// the migration path, so losing the head loses the list and not the records.
///
/// The version names this shape and is required, so a head of a version this build does not
/// read is refused rather than parsed with defaulted fields: read as this one it would
/// answer no threads, the first persist would write that back, and the list would be gone
/// while the records stayed, which is the kind of loss that looks like an empty account
/// rather than an error. The shape before the counts carried no version at all, which
/// [`HeadFile`] recognizes and migrates.
#[derive(Default, serde::Serialize, serde::Deserialize)]
struct Head {
    version: u32,
    #[serde(default)]
    threads: HashMap<String, ThreadSession>,
    #[serde(default)]
    counts: HashMap<String, usize>,
    #[serde(default)]
    title_versions: HashMap<String, u64>,
}

/// The verbs a volume answers, whatever carries them.
pub trait Volume: Send + Sync {
    /// How many records the volume holds.
    fn count(&self) -> Result<u64, String>;

    /// Write a payload under a name. A name already taken with a different payload is a
    /// conflict the volume refuses.
    fn write_named(
        &self,
        name: &str,
        origin: &str,
        media_type: &str,
        payload: &[u8],
        creator: &str,
    ) -> Result<(), String>;

    /// The payload under a name, or nothing when no record carries it.
    fn read_payload(&self, name: &str) -> Result<Option<Vec<u8>>, String>;

    /// What kind of volume this is, as the short noun a person reads.
    ///
    /// It names the shape rather than the instance, so a store's description reads as
    /// `ktema volume at /path` and a reader can tell the engine from a socket or a double.
    /// The default is for a volume that is only ever a double.
    fn kind(&self) -> &'static str {
        "unnamed"
    }

    /// Where the volume is, for a message a person reads.
    fn place(&self) -> String;
}

pub struct VolumeStore<V: Volume> {
    volume: V,
    /// Actus's own document for this volume: the thread list, the counts, and what a
    /// listing reads.
    head: PathBuf,
    /// The document this store seeds from when it has no head of its own, which is the
    /// record the host was serving until the store took over. It is read and never written.
    seed: PathBuf,
    /// Message positions already in the volume, so a persist appends the tail and not the
    /// whole thread. Zero for a thread seeded from the document.
    deposited: Mutex<HashMap<String, u64>>,
    titled: Mutex<HashMap<String, TitleState>>,
    /// Messages per thread, from the head. A window needs a thread's end, and this is what
    /// supplies it without reading the volume.
    counts: Mutex<HashMap<String, usize>>,
}

impl<V: Volume> VolumeStore<V> {
    pub fn new(volume: V, dir: &Path) -> Self {
        Self {
            volume,
            head: dir.join(HEAD_FILE),
            seed: dir.join(super::DOCUMENT_FILE),
            deposited: Mutex::new(HashMap::new()),
            titled: Mutex::new(HashMap::new()),
            counts: Mutex::new(HashMap::new()),
        }
    }

    fn put(&self, name: &str, origin: &str, media_type: &str, bytes: &[u8]) -> Result<(), String> {
        self.volume
            .write_named(name, origin, media_type, bytes, &creator())
    }

    fn payload(&self, name: &str) -> Result<Option<Vec<u8>>, String> {
        self.volume.read_payload(name)
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
                        String::from_utf8(bytes).map_err(|e| format!("{name} is not text: {e}"))?,
                    );
                    state.versions += 1;
                }
                None => break,
            }
        }
        Ok(state)
    }

    /// Read actus's head, or nothing when it is not there.
    ///
    /// The shape before the counts existed is recognized and migrated: its counts are read
    /// out of the volume, which is the authority for them, so a host that upgrades keeps its
    /// list and its counts. An unknown version is refused, because nothing derives it, and a
    /// head read as an empty one is the loss this refusal exists for.
    fn read_head(&self) -> Result<Option<Head>, String> {
        if !self.head.exists() {
            return Ok(None);
        }
        let content = std::fs::read_to_string(&self.head)
            .map_err(|e| format!("read {}: {e}", self.head.display()))?;
        let file: HeadFile = serde_json::from_str(&content).map_err(|e| {
            format!(
                "the head at {} is not one this build reads ({e}). Delete it to seed from the document, which is not written here: rm {}",
                self.head.display(),
                self.head.display(),
            )
        })?;
        match file {
            HeadFile::Versioned(head) => {
                if head.version != HEAD_VERSION {
                    return Err(format!(
                        "the head at {} is version {}, and this build reads version {}. Delete it to seed from the document, which is not written here: rm {}",
                        self.head.display(),
                        head.version,
                        HEAD_VERSION,
                        self.head.display(),
                    ));
                }
                Ok(Some(head))
            }
            HeadFile::Bare(threads) => {
                tracing::info!(
                    "the head at {} is the shape before the counts; reading them out of {}",
                    self.head.display(),
                    self.volume.place()
                );
                let mut counts = HashMap::new();
                let mut title_versions = HashMap::new();
                let mut indexed = HashMap::new();
                for (id, thread) in threads {
                    let (messages, _) = self.read_messages(&id)?;
                    counts.insert(id.clone(), messages.len());
                    let title = self.read_titles(&id)?;
                    title_versions.insert(id.clone(), title.versions);
                    let mut thread = thread;
                    // A listing orders by the last message's time, and a head written before
                    // this shape carries none: it is taken here, where the messages are
                    // already in hand, rather than left to a read per thread later.
                    if thread.updated_at.is_none() {
                        thread.updated_at = messages.last().map(|m| m.timestamp);
                    }
                    thread.messages.clear();
                    if title.text.is_some() {
                        thread.title = title.text;
                    }
                    indexed.insert(id, thread);
                }
                Ok(Some(Head {
                    version: HEAD_VERSION,
                    threads: indexed,
                    counts,
                    title_versions,
                }))
            }
        }
    }

    /// Read a thread document, or nothing when it is not there.
    fn read_document(path: &Path) -> Result<Option<HashMap<String, ThreadSession>>, String> {
        if !path.exists() {
            return Ok(None);
        }
        let content =
            std::fs::read_to_string(path).map_err(|e| format!("read {}: {e}", path.display()))?;
        let threads: HashMap<String, ThreadSession> = serde_json::from_str(&content)
            .map_err(|e| format!("{} is not a thread document: {e}", path.display()))?;
        Ok(Some(threads))
    }

    /// Take a thread set as the index: keep the metadata, remember the counts, and let the
    /// messages go.
    fn index_of(&self, threads: HashMap<String, ThreadSession>) -> HashMap<String, ThreadSession> {
        {
            let mut counts = self.counts.lock().unwrap();
            counts.clear();
            for (id, thread) in &threads {
                counts.insert(id.clone(), thread.messages.len());
            }
        }
        {
            let mut titled = self.titled.lock().unwrap();
            titled.clear();
            for (id, thread) in &threads {
                // A seed has a title and no title record, so its version count starts at
                // zero and the first change deposits the first version.
                titled.insert(
                    id.clone(),
                    TitleState {
                        versions: 0,
                        text: thread.title.clone(),
                    },
                );
            }
        }
        let mut threads = threads;
        for thread in threads.values_mut() {
            // The last message's time is what a listing orders by and the messages are in
            // hand here, so it is kept rather than derived from the record later.
            if thread.updated_at.is_none() {
                thread.updated_at = thread.messages.last().map(|m| m.timestamp);
            }
            thread.messages.clear();
        }
        threads
    }

    /// Messages per thread, from the head. One small read, and the cache the load paths fill.
    fn head_counts(&self, id: &str) -> Result<usize, String> {
        if let Some(n) = self.counts.lock().unwrap().get(id) {
            return Ok(*n);
        }
        Ok(self
            .read_head()?
            .and_then(|head| head.counts.get(id).copied())
            .unwrap_or(0))
    }

    /// Refuse to seed onto a volume that already holds another writer's records.
    ///
    /// A record never changes, so a name this store would take and did not write is a name
    /// it can never take: every deposit is refused, and a turn then lives only in memory
    /// until the process ends. That is a startup failure here instead, and the message
    /// names both ways out.
    fn ensure_a_fresh_volume(
        &self,
        threads: &HashMap<String, ThreadSession>,
    ) -> Result<(), String> {
        let count = self.volume.count()?;
        if count == 0 {
            return Ok(());
        }
        for (id, thread) in threads {
            let Some(first) = thread.messages.first() else {
                continue;
            };
            let name = message_name(id, 0);
            let Some(held) = self.payload(&name)? else {
                continue;
            };
            let ours = serde_json::to_vec(first)
                .map_err(|e| format!("serialize a message of {id}: {e}"))?;
            if held != ours {
                return Err(format!(
                    "the volume on {} holds {count} records, and {name} is not the record this store would write there. A volume belongs to one writer: give that volume up, or serve the document with {}=document.",
                    self.volume.place(),
                    super::STORE_ENV,
                ));
            }
        }
        Ok(())
    }
}

impl<V: Volume> RecordStore for VolumeStore<V> {
    fn describe(&self) -> String {
        format!("{} volume at {}", self.volume.kind(), self.volume.place())
    }

    /// This store appends a record per message, so a snapshot that carries a thread's
    /// metadata and not its messages is a thread with nothing new to record.
    fn needs_whole_state(&self) -> bool {
        false
    }

    fn load(&self) -> Result<HashMap<String, ThreadSession>, String> {
        let mut threads = match self.read_head()? {
            Some(head) => {
                *self.counts.lock().unwrap() = head.counts.clone();
                {
                    let mut titled = self.titled.lock().unwrap();
                    for (id, thread) in &head.threads {
                        titled.insert(
                            id.clone(),
                            TitleState {
                                versions: head.title_versions.get(id).copied().unwrap_or(0),
                                text: thread.title.clone(),
                            },
                        );
                    }
                }
                head.threads
            }
            None => match Self::read_document(&self.seed)? {
                Some(threads) => {
                    self.ensure_a_fresh_volume(&threads)?;
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
            self.volume.place()
        );
        Ok(threads)
    }

    /// The thread list without its messages.
    ///
    /// A head that exists answers this without reading the volume: it carries the list, the
    /// counts, and the titles. A first run has no head and reads the seed, which is the
    /// document this host was serving.
    fn load_index(&self) -> Result<HashMap<String, ThreadSession>, String> {
        let head = match self.read_head()? {
            Some(head) => head,
            None => {
                let Some(seed) = Self::read_document(&self.seed)? else {
                    return Ok(HashMap::new());
                };
                self.ensure_a_fresh_volume(&seed)?;
                tracing::info!(
                    "the record store has no head of its own; the index is {}",
                    self.seed.display()
                );
                return Ok(self.index_of(seed));
            }
        };
        *self.counts.lock().unwrap() = head.counts.clone();
        {
            let mut titled = self.titled.lock().unwrap();
            titled.clear();
            for (id, thread) in &head.threads {
                titled.insert(
                    id.clone(),
                    TitleState {
                        versions: head.title_versions.get(id).copied().unwrap_or(0),
                        text: thread.title.clone(),
                    },
                );
            }
        }
        {
            let mut deposited = self.deposited.lock().unwrap();
            deposited.clear();
            for (id, count) in &head.counts {
                deposited.insert(id.clone(), *count as u64);
            }
        }
        // A head written before a thread's activity time was carried has none for it, and a
        // listing orders by it. The thread's last record answers it, one read per thread that
        // lacks one, and the next persist writes it back. A thread with no messages has no
        // time to take and keeps whatever it has.
        let counts = head.counts.clone();
        let mut threads = head.threads;
        for (id, thread) in threads.iter_mut() {
            if thread.updated_at.is_some() {
                continue;
            }
            let count = counts.get(id).copied().unwrap_or(0);
            if count == 0 {
                continue;
            }
            let Some(bytes) = self.payload(&message_name(id, count as u64 - 1))? else {
                continue;
            };
            if let Ok(last) = serde_json::from_slice::<ThreadMessage>(&bytes) {
                thread.updated_at = Some(last.timestamp);
            }
        }
        Ok(threads)
    }

    fn message_count(&self, id: &str) -> Result<usize, String> {
        self.head_counts(id)
    }

    /// A window of a thread's messages.
    ///
    /// The read is bounded by `limit`: a name per position, and one miss to end it. What it
    /// holds is the window, whatever the thread's length, which is the property the
    /// document cannot offer.
    fn load_messages(
        &self,
        id: &str,
        from: usize,
        limit: usize,
    ) -> Result<Vec<ThreadMessage>, String> {
        let mut messages = Vec::new();
        if limit == 0 {
            return Ok(messages);
        }
        for position in from..from.saturating_add(limit) {
            let name = message_name(id, position as u64);
            match self.payload(&name)? {
                Some(bytes) => {
                    let message: ThreadMessage = serde_json::from_slice(&bytes)
                        .map_err(|e| format!("{name} is not a message: {e}"))?;
                    messages.push(message);
                }
                None => break,
            }
        }
        Ok(messages)
    }

    fn persist(&self, mut threads: HashMap<String, ThreadSession>) -> Result<(), String> {
        for (id, thread) in &threads {
            // The cursor is how many messages this store holds for the thread, and it is
            // what says where a caller's list begins. A caller that holds only the threads
            // it touched sends the rest with no messages, which is not a thread that lost
            // them: a record cannot be removed, so the cursor never moves back, and a list
            // shorter than it writes nothing.
            let held = {
                let deposited = self.deposited.lock().unwrap();
                deposited.get(id).copied().unwrap_or(0)
            };
            let total = thread.messages.len() as u64;
            for position in held.min(total)..total {
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
                .insert(id.clone(), held.max(total));

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

        // The head is actus's own: the thread list, the routing fields, the title as a
        // cache, and the counts a window needs. It carries no messages, which is what keeps
        // a persist small, and the snapshot this call was given is taken apart rather than
        // copied to write it.
        let mut head = Head {
            version: HEAD_VERSION,
            ..Default::default()
        };
        {
            // A thread the caller does not hold arrives with no messages, and its count is
            // the one the head already carries. The two are taken together, higher wins,
            // because a thread whose records are not all in hand must not read as empty.
            let before = self.counts.lock().unwrap().clone();
            let mut counts = HashMap::new();
            for (id, thread) in &threads {
                let held = thread.messages.len();
                counts.insert(id.clone(), held.max(before.get(id).copied().unwrap_or(0)));
            }
            *self.counts.lock().unwrap() = counts.clone();
            head.counts = counts;
        }
        {
            let titled = self.titled.lock().unwrap();
            for (id, state) in titled.iter() {
                head.title_versions.insert(id.clone(), state.versions);
            }
        }
        for (id, thread) in threads.iter_mut() {
            thread.messages.clear();
            head.threads.insert(id.clone(), thread.clone());
        }
        let json = serde_json::to_string(&head).map_err(|e| format!("serialize heads: {e}"))?;
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
