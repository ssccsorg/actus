//! Where a thread's record lives.
//!
//! Actus carries acts and holds the board, and the record a person reads is not actus's
//! to keep in one shape. This module is that seam: a `RecordStore` an implementation
//! provides, chosen from the environment so a deployment decides where history lives and
//! a host that decides nothing keeps the behavior it had.
//!
//! Two rules bind every implementation, and both come from the shared-module constraint.
//! A store takes its configuration from the caller or the environment and never from a
//! constant. A store the operator selected and that cannot be reached fails at startup
//! rather than falling back to another one, because a fallback hides the misconfiguration
//! that chose it.

mod document;
mod record;
mod volume;

pub use document::DocumentStore;
pub use record::Record;
pub use volume::{Volume, VolumeStore};

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use crate::agent::{ThreadMessage, ThreadSession};

/// The store a deployment selected. Absent means the document.
pub const STORE_ENV: &str = "ACTUS_RECORD_STORE";

/// The document that holds the record.
const DOCUMENT_FILE: &str = "threads.json";

/// The store a host gets when it names none. Overridable, and that is why it is allowed.
const DEFAULT_STORE: &str = "document";

/// Every store name a deployment can select, in one place.
///
/// The selector below resolves these, and the integrity suite asserts it covers every one:
/// a name added here without a suite entry fails the suite rather than reaching a
/// deployment unexercised. A name a build composes in rather than names, which is what the
/// product does with the engine, is covered on that side.
pub const STORES: &[&str] = &["document"];

/// A thread's record, kept and read back outside the router.
///
/// The record is what a person reads: a thread, its title, and its messages. Everything
/// else a thread carries is actus's own state and stays with actus, so an implementation
/// is free to keep the record anywhere it can read it back.
pub trait RecordStore: Send + Sync {
    /// Which store holds the record, for a client that shows it.
    ///
    /// A client's question is which store is serving, and a place answers that only for a
    /// reader who already knows the shape it belongs to, so an implementation names both.
    /// The default says only that the store did not say, because a store named as one thing
    /// while being another is worse than a store that names nothing.
    fn describe(&self) -> String {
        "unreported".to_string()
    }

    /// Every thread the store holds, with its messages.
    fn load(&self) -> Result<HashMap<String, ThreadSession>, String>;

    /// Whether [`persist`](Self::persist) needs every thread's messages.
    ///
    /// A store that writes one document needs the whole state, because what it writes
    /// replaces what it had. A store that records one message at a time does not: a thread
    /// whose messages the caller does not hold is a thread with no new record, and the
    /// store's own count and title state carry what it already has. A caller that holds only
    /// the threads it touched, which is what keeps a host's memory off the volume's size,
    /// asks this before it decides.
    fn needs_whole_state(&self) -> bool {
        true
    }

    /// Persist the current state of every thread. [`load`](Self::load) must read back what
    /// this wrote.
    ///
    /// The snapshot is the caller's copy and this call consumes it: a caller that keeps its
    /// own state passes a copy, and an implementation may take the snapshot apart rather
    /// than copy it a second time.
    fn persist(&self, threads: HashMap<String, ThreadSession>) -> Result<(), String>;

    /// Every thread the store holds, without its messages.
    ///
    /// A caller that only needs what a thread says about itself, its identifier, its title,
    /// its routing fields, and how many messages it holds, asks this rather than [`load`]
    /// and holds a listing's worth of state rather than the record's.
    ///
    /// The default strips the messages from [`load`], which is what a store that keeps one
    /// document can do and no better. A store that can answer from an index overrides it.
    fn load_index(&self) -> Result<HashMap<String, ThreadSession>, String> {
        let mut threads = self.load()?;
        for thread in threads.values_mut() {
            thread.messages.clear();
        }
        Ok(threads)
    }

    /// How many messages a thread holds.
    ///
    /// A window needs a thread's end, and the end is this. The default answers from
    /// [`load`]; a store whose index carries the count answers without reading the volume.
    fn message_count(&self, id: &str) -> Result<usize, String> {
        Ok(self
            .load()?
            .get(id)
            .map(|thread| thread.messages.len())
            .unwrap_or(0))
    }

    /// A window of a thread's messages, by position.
    ///
    /// `from` is a message's position in the thread and `limit` bounds how many are
    /// returned, so what a caller holds is the window rather than the thread. The default
    /// loads the whole store and slices, which is what a store that cannot do better
    /// answers; a store that addresses a record by name reads the window alone.
    fn load_messages(
        &self,
        id: &str,
        from: usize,
        limit: usize,
    ) -> Result<Vec<ThreadMessage>, String> {
        let threads = self.load()?;
        let Some(thread) = threads.get(id) else {
            return Ok(Vec::new());
        };
        let end = from.saturating_add(limit).min(thread.messages.len());
        if from >= end {
            return Ok(Vec::new());
        }
        Ok(thread.messages[from..end].to_vec())
    }
}

/// Open the store this deployment selected. `dir` is the agent's own directory, and an
/// implementation keeps what it needs under it.
pub fn open(dir: &Path) -> Result<Arc<dyn RecordStore>, String> {
    let name = std::env::var(STORE_ENV).unwrap_or_else(|_| DEFAULT_STORE.to_string());
    match name.as_str() {
        "document" => Ok(Arc::new(DocumentStore::new(dir.join(DOCUMENT_FILE)))),
        other => Err(format!(
            "{STORE_ENV}={other} names no store: the stores are {}",
            STORES.join(", ")
        )),
    }
}
