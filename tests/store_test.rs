//! The default store, which is what a switch back has to be.
//!
//! A host that names no store gets the document, and a host that names the document gets
//! it too: one value restores the behavior actus had before the seam, with the messages in
//! one file and no volume, no socket, and no head beside it.

use std::collections::HashMap;

use actus::agent::{ThreadMessage, ThreadSession};
use actus::store::{RecordStore, SocketStore, SocketVolume};

fn message(role: &str, content: &str) -> ThreadMessage {
    ThreadMessage {
        role: role.to_string(),
        content: content.to_string(),
        message_id: Some(format!("m-{content}")),
        entry_type: Some("text".to_string()),
        tool_name: None,
        tool_status: None,
        author: None,
        timestamp: chrono::Utc::now(),
    }
}

fn stored(content: &str) -> HashMap<String, ThreadSession> {
    let mut threads = HashMap::new();
    threads.insert(
        "t1".to_string(),
        ThreadSession {
            id: "t1".to_string(),
            title: Some("a title".to_string()),
            parent: None,
            messages: vec![message("user", content), message("assistant", "answer")],
            created_at: chrono::Utc::now(),
            updated_at: None,
            completed: true,
            acp_thread_id: Some("acp-1".to_string()),
            turn_completed: 1,
        },
    );
    threads
}

/// The document is the store a host gets when it names nothing, it carries the messages,
/// and a store opened again over it reads them back.
#[test]
fn the_document_is_the_default_and_holds_the_messages() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = actus::store::open(dir.path()).expect("no name is the document");
    assert_eq!(store.load().expect("an empty directory").len(), 0);

    store.persist(stored("hi")).expect("persist");

    let document: HashMap<String, ThreadSession> =
        serde_json::from_str(&std::fs::read_to_string(dir.path().join("threads.json")).expect("the document"))
            .expect("a thread document");
    assert_eq!(document["t1"].messages.len(), 2, "the messages are in the document");
    assert_eq!(document["t1"].title.as_deref(), Some("a title"));
    assert!(
        !dir.path().join("heads.json").exists(),
        "the document is the record, so there is no head beside it"
    );

    let restarted = actus::store::open(dir.path()).expect("the document again");
    let read = restarted.load().expect("load");
    assert_eq!(read["t1"].messages.len(), 2);

    // Naming the document is the same store, which is the value a deployment switches
    // back with. Set last, because the environment is one value for the process.
    unsafe { std::env::set_var("ACTUS_RECORD_STORE", "document") };
    let named = actus::store::open(dir.path()).expect("the named document");
    let read = named.load().expect("load");
    assert_eq!(read["t1"].messages.len(), 2);
    unsafe { std::env::remove_var("ACTUS_RECORD_STORE") };
}

/// The socket store and the document store are different files. A volume that cannot be
/// reached fails when the manager loads, which is startup, and it leaves the document
/// exactly as it was, so a switch back is complete.
#[test]
fn a_volume_that_cannot_be_reached_fails_at_startup() {
    let dir = tempfile::tempdir().expect("tempdir");

    let document = actus::store::open(dir.path()).expect("the document");
    document.persist(stored("from the document")).expect("persist");
    let before = std::fs::read_to_string(dir.path().join("threads.json")).expect("the document");

    // Nothing serves this socket, so the store fails where it is opened rather than
    // answering from the document and calling it a volume.
    let socket = SocketStore::new(SocketVolume::new(dir.path().join("store.sock")), dir.path());
    let error = match socket.load() {
        Ok(_) => panic!("a volume nobody serves must fail at startup"),
        Err(error) => error,
    };
    assert!(error.contains("connect"), "{error}");

    let after = std::fs::read_to_string(dir.path().join("threads.json")).expect("the document");
    assert_eq!(before, after, "the document was not touched");
    assert!(!dir.path().join("heads.json").exists(), "no head was written");
}
