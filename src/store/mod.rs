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
mod socket;
mod volume;

pub use document::DocumentStore;
pub use socket::SocketVolume;
pub use volume::{Volume, VolumeStore};

/// The store that reaches an engine in another process, over a socket.
pub type SocketStore = VolumeStore<SocketVolume>;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::agent::{ThreadMessage, ThreadSession};

/// The store a deployment selected. Absent means the document.
pub const STORE_ENV: &str = "ACTUS_RECORD_STORE";

/// The unix socket a `socket` store connects to. When it is not set, the socket is
/// `store.sock` in the agent's own directory, which is what lets one host run one volume
/// per agent without a per-agent variable to name it.
pub const STORE_SOCKET_ENV: &str = "ACTUS_RECORD_STORE_SOCKET";

/// The document that holds the record, named once for both stores: it is where the
/// document store writes and where the socket store seeds from.
const DOCUMENT_FILE: &str = "threads.json";

/// The store a host gets when it names none. Overridable, and that is why it is allowed.
const DEFAULT_STORE: &str = "document";

/// Every store name a deployment can select, in one place.
///
/// The selector below resolves these, and the integrity suite asserts it covers every one:
/// a name added here without a suite entry fails the suite rather than reaching a
/// deployment unexercised. A name a build composes in rather than names, which is what the
/// product does with the engine, is covered on that side.
pub const STORES: &[&str] = &["document", "socket"];

/// The socket a `socket` store uses when the environment names none.
const DEFAULT_SOCKET_FILE: &str = "store.sock";

/// A thread's record, kept and read back outside the router.
///
/// The record is what a person reads: a thread, its title, and its messages. Everything
/// else a thread carries is actus's own state and stays with actus, so an implementation
/// is free to keep the record anywhere it can read it back.
pub trait RecordStore: Send + Sync {
    /// Every thread the store holds, with its messages.
    fn load(&self) -> Result<HashMap<String, ThreadSession>, String>;

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
        "socket" => {
            // The socket is the deployment's address. An explicit one is used as it is;
            // otherwise it follows the agent's own directory, because the environment is
            // one value for the process and a host runs one volume per agent.
            let socket = std::env::var(STORE_SOCKET_ENV)
                .map(PathBuf::from)
                .unwrap_or_else(|_| dir.join(DEFAULT_SOCKET_FILE));
            Ok(Arc::new(VolumeStore::new(SocketVolume::new(socket), dir)))
        }
        other => Err(format!(
            "{STORE_ENV}={other} names no store: the stores are {}",
            STORES.join(", ")
        )),
    }
}
