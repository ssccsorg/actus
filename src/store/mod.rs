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

pub use document::DocumentStore;
pub use socket::SocketStore;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::agent::ThreadSession;

/// The store a deployment selected. Absent means the document.
pub const STORE_ENV: &str = "ACTUS_RECORD_STORE";

/// The unix socket a `socket` store connects to. When it is not set, the socket is
/// `store.sock` in the agent's own directory, which is what lets one host run one volume
/// per agent without a per-agent variable to name it.
pub const STORE_SOCKET_ENV: &str = "ACTUS_RECORD_STORE_SOCKET";

/// The socket a `socket` store uses when the environment names none.
pub const DEFAULT_SOCKET_FILE: &str = "store.sock";

/// The store a host gets when it names none. Overridable, and that is why it is allowed.
pub const DEFAULT_STORE: &str = "document";

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
    fn persist(&self, threads: &HashMap<String, ThreadSession>) -> Result<(), String>;
}

/// Open the store this deployment selected. `dir` is the agent's own directory, and an
/// implementation keeps what it needs under it.
pub fn open(dir: &Path) -> Result<Arc<dyn RecordStore>, String> {
    let name = std::env::var(STORE_ENV).unwrap_or_else(|_| DEFAULT_STORE.to_string());
    match name.as_str() {
        "document" => Ok(Arc::new(DocumentStore::new(dir.join("threads.json")))),
        "socket" => {
            // The socket is the deployment's address. An explicit one is used as it is;
            // otherwise it follows the agent's own directory, because the environment is
            // one value for the process and a host runs one volume per agent.
            let socket = std::env::var(STORE_SOCKET_ENV)
                .map(PathBuf::from)
                .unwrap_or_else(|_| dir.join(DEFAULT_SOCKET_FILE));
            Ok(Arc::new(SocketStore::new(socket, dir)))
        }
        other => Err(format!(
            "{STORE_ENV}={other} names no store: the stores are {DEFAULT_STORE} and socket"
        )),
    }
}
