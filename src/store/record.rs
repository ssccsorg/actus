//! The store-facing rule, shared by every backend that keeps a thread record.
//!
//! A backend owns its threads and the lock around them. What it does not own is how a
//! record is read and written, which is a property of the store and not of the platform:
//! whether the whole state or an index is loaded, when a thread's messages are read, and
//! what a persist writes. That half lives here, so a second backend that records is not a
//! second copy of the rule.
//!
//! The caller keeps the maps. A backend held its threads in a field long before this
//! existed and reads them directly all over its own code, so this borrows them rather than
//! taking them over; what it takes over is every decision that touches the store.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::agent::{ThreadMessage, ThreadSession};
use crate::store::{RecordStore, RecordedMessage};

/// The store a backend records through, with the one rule for reading and writing it.
#[derive(Clone)]
pub struct Record {
    store: Arc<dyn RecordStore>,
}

impl Record {
    pub fn new(store: Arc<dyn RecordStore>) -> Self {
        Self { store }
    }

    /// The threads to start from, and which of them are in hand.
    ///
    /// A store that records one message at a time does not need every thread's messages
    /// when a thread is worked on, so the index is what is loaded and a thread's messages
    /// are read when the thread is first touched. A store that writes one document needs
    /// the whole state, and it is loaded whole, in which case every thread is in hand.
    pub fn load(&self) -> Result<(HashMap<String, ThreadSession>, HashSet<String>), String> {
        let whole = self.store.needs_whole_state();
        let threads = if whole {
            self.store.load()?
        } else {
            self.store.load_index()?
        };
        let held = if whole {
            threads.keys().cloned().collect()
        } else {
            HashSet::new()
        };
        Ok((threads, held))
    }

    /// Which store holds the record, for a client that shows it.
    pub fn describe(&self) -> String {
        self.store.describe()
    }

    /// Read a thread's messages into hand, if they are not already there.
    ///
    /// A thread the index holds is metadata only, so anything that reads a thread's
    /// messages asks for them first. That is what keeps a listing and a health check from
    /// paying for a volume they do not read, and what makes the first turn of a resumed
    /// thread the moment its history is read.
    pub fn hold(
        &self,
        threads: &mut HashMap<String, ThreadSession>,
        held: &mut HashSet<String>,
        thread_id: &str,
    ) -> Result<(), String> {
        if held.contains(thread_id) || !threads.contains_key(thread_id) {
            return Ok(());
        }
        let messages = self.store.load_messages(thread_id, 0, usize::MAX)?;
        if let Some(thread) = threads.get_mut(thread_id) {
            thread.messages = messages;
        }
        held.insert(thread_id.to_string());
        Ok(())
    }

    /// How many messages a thread holds.
    ///
    /// From memory when the thread is in hand and from the store otherwise, which is what
    /// a listing counts without reading the volume behind it.
    pub fn message_count(
        &self,
        threads: &HashMap<String, ThreadSession>,
        held: &HashSet<String>,
        thread_id: &str,
    ) -> Result<usize, String> {
        if held.contains(thread_id) {
            if let Some(thread) = threads.get(thread_id) {
                return Ok(thread.messages.len());
            }
        }
        self.store.message_count(thread_id)
    }

    /// A window of a thread's messages, by position.
    ///
    /// From memory when the thread is in hand and from the store otherwise, so a view
    /// opens on a conversation without the whole of it being read into this process.
    pub fn window(
        &self,
        threads: &HashMap<String, ThreadSession>,
        held: &HashSet<String>,
        thread_id: &str,
        from: usize,
        limit: usize,
    ) -> Result<Vec<ThreadMessage>, String> {
        if held.contains(thread_id) {
            if let Some(thread) = threads.get(thread_id) {
                let end = from.saturating_add(limit).min(thread.messages.len());
                if from >= end {
                    return Ok(Vec::new());
                }
                return Ok(thread.messages[from..end].to_vec());
            }
        }
        self.store.load_messages(thread_id, from, limit)
    }

    /// The messages a span of time found, which is the volume's own axis rather than a
    /// thread's window.
    ///
    /// Nothing of the caller's maps takes part: the span is answered from the records, so a
    /// thread this backend never read into hand is in the answer all the same.
    pub fn between(
        &self,
        since: Option<u64>,
        until: Option<u64>,
    ) -> Result<Vec<RecordedMessage>, String> {
        self.store.read_between(since, until)
    }

    /// Write the record back, consuming the caller's snapshot.
    pub fn persist(&self, threads: HashMap<String, ThreadSession>) -> Result<(), String> {
        self.store.persist(threads)
    }
}
