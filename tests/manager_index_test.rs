//! The manager over a store that does not need the whole state.
//!
//! What this pins is the property a host serves on: a stack reading a volume holds the
//! threads it is working on and not the volume. The manager loads the index, a listing and a
//! count answer without the messages, a window is read from the record, and a thread's
//! history is read when the thread is first worked on rather than at startup.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use actus::agent::{ThreadMessage, ThreadSession};
use actus::store::{RecordStore, Volume, VolumeStore};
use actus::telos::TelosManager;

// ── A volume in memory, with the reads it served counted ────────────────

#[derive(Default)]
struct MemInner {
    records: Mutex<HashMap<String, Vec<u8>>>,
    reads: Mutex<usize>,
}

#[derive(Clone, Default)]
struct MemVolume {
    inner: Arc<MemInner>,
}

impl MemVolume {
    fn reads(&self) -> usize {
        *self.inner.reads.lock().unwrap()
    }
}

impl Volume for MemVolume {
    fn count(&self) -> Result<u64, String> {
        Ok(self.inner.records.lock().unwrap().len() as u64)
    }

    fn write_named(
        &self,
        name: &str,
        _origin: &str,
        _media_type: &str,
        payload: &[u8],
        _creator: &str,
    ) -> Result<(), String> {
        let mut records = self.inner.records.lock().unwrap();
        match records.get(name) {
            Some(held) if held.as_slice() != payload => {
                Err(format!("{name} is taken with a different payload"))
            }
            _ => {
                records.insert(name.to_string(), payload.to_vec());
                Ok(())
            }
        }
    }

    fn read_payload(&self, name: &str) -> Result<Option<Vec<u8>>, String> {
        *self.inner.reads.lock().unwrap() += 1;
        Ok(self.inner.records.lock().unwrap().get(name).cloned())
    }

    fn place(&self) -> String {
        "memory".to_string()
    }
}

// ── A thread to serve ──────────────────────────────────────────────────

fn thread_with(n: usize) -> ThreadSession {
    let messages = (0..n)
        .map(|i| ThreadMessage {
            role: if i % 2 == 0 { "user" } else { "assistant" }.to_string(),
            content: format!("msg-{i}"),
            message_id: Some(format!("m-{i}")),
            entry_type: Some("text".to_string()),
            tool_name: None,
            tool_status: None,
            author: None,
            timestamp: chrono::Utc::now(),
        })
        .collect();
    ThreadSession {
        id: "t1".to_string(),
        title: Some("a title".to_string()),
        parent: None,
        messages,
        created_at: chrono::Utc::now(),
        updated_at: None,
        completed: true,
        acp_thread_id: Some("acp-1".to_string()),
        turn_completed: 1,
    }
}

fn manager(dir: &std::path::Path, volume: &MemVolume) -> TelosManager {
    TelosManager::with_store(
        "ses_test".to_string(),
        "127.0.0.1:0".to_string(),
        Arc::new(VolumeStore::new(volume.clone(), dir)),
    )
    .expect("a manager over the volume")
}

/// The manager holds the index, not the volume: a listing and a count answer without the
/// messages, a window costs the window, and a thread's history is read when it is worked on.
#[test]
fn a_thread_is_read_when_it_is_worked_on_and_not_before() {
    let dir = tempfile::tempdir().expect("dir");
    let volume = MemVolume::default();
    let mut initial = HashMap::new();
    initial.insert("t1".to_string(), thread_with(6));
    VolumeStore::new(volume.clone(), dir.path())
        .persist(initial)
        .expect("persist the record");

    let mut mgr = manager(dir.path(), &volume);
    assert_eq!(mgr.threads.len(), 1, "the index names the thread");
    assert!(mgr.held.is_empty(), "and no thread is in hand");
    assert!(
        mgr.threads["t1"].messages.is_empty(),
        "a thread of the index carries no messages"
    );
    assert!(
        mgr.threads["t1"].updated_at.is_some(),
        "and its activity time is taken from the last record"
    );
    assert_eq!(
        mgr.message_count("t1").expect("count"),
        6,
        "the count is the record's, not a message list's"
    );

    // A window is read from the record, and it costs the window.
    let before = volume.reads();
    let window = mgr.window("t1", 2, 2).expect("window");
    assert_eq!(window.len(), 2);
    assert_eq!(window[0].content, "msg-2");
    assert_eq!(volume.reads() - before, 2, "a window reads its window");

    // The first turn of the thread reads its history, which is what the context injection
    // is built from, and the thread is in hand from then on.
    let prepared = mgr.prepare_message("t1", "again");
    assert!(
        prepared.contains("[Previous conversation]"),
        "the history was read for the context"
    );
    assert!(!mgr.threads["t1"].messages.is_empty(), "and is in hand");

    // A turn appends, and the persist writes the tail: what the record already holds is not
    // written again.
    mgr.add_message("t1", "user", "again", Some("m-new".to_string()));
    mgr.flush_threads();

    let record = VolumeStore::new(volume.clone(), dir.path());
    let read = record.load().expect("load");
    assert_eq!(read["t1"].messages.len(), 7, "the record holds the new message");
    assert_eq!(
        read["t1"].messages[6].content, "again",
        "at the position after the last"
    );
    assert_eq!(
        read["t1"].messages[0].content, "msg-0",
        "and the earliest message is unchanged"
    );
    assert_eq!(record.message_count("t1").expect("count"), 7);
}

/// A thread made after the index was loaded is in hand, because its messages are the ones
/// this process wrote. A store read for it would drop them.
#[test]
fn a_thread_made_here_keeps_what_is_written_to_it() {
    let dir = tempfile::tempdir().expect("dir");
    let volume = MemVolume::default();
    let mut mgr = manager(dir.path(), &volume);

    let id = mgr.get_or_create_thread(None);
    assert!(mgr.held.contains(&id), "a thread made here is in hand");
    mgr.add_message(&id, "user", "hello", Some("m-1".to_string()));
    mgr.add_message(&id, "assistant", "hi", Some("m-2".to_string()));
    assert_eq!(mgr.message_count(&id).expect("count"), 2);
    mgr.flush_threads();

    let read = VolumeStore::new(volume.clone(), dir.path())
        .load()
        .expect("load");
    assert_eq!(
        read[&id].messages.len(),
        2,
        "the two messages this process wrote are in the record"
    );
}
