//! The default store, which is what a switch back has to be.
//!
//! A host that names no store gets the document, and a host that names the document gets
//! it too: one value restores the behavior actus had before the seam, with the messages in
//! one file and no head beside it.

use std::collections::HashMap;

use actus::agent::{ThreadMessage, ThreadSession};

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
