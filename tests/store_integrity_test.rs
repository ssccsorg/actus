//! The record store's integrity, checked the same way for every shape it has.
//!
//! The suite runs over each store a build can select, so a new shape is covered by being
//! added to `modes` and by nothing else. What it pins:
//!
//! - a round trip is exact: what a persist wrote, a load reads back, message for message
//!   and field for field;
//! - a write is append-only: a second persist of the same state writes no record, and one
//!   more message writes exactly one;
//! - an index and a count agree with the record, and a count survives a restart;
//! - a window holds the window and nothing more, and the reads it makes are bounded by the
//!   window rather than by the thread, which is the property the document cannot offer;
//! - two shapes of the same store agree on the same input, which is what a deployment
//!   moves between them on.
//!
//! The last two are the interesting ones. A change that makes a load hold the whole volume,
//! or that makes a window read past its end, fails here rather than in production.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use actus::agent::{ThreadMessage, ThreadSession};
use actus::store::{RecordStore, Volume, VolumeStore};

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

    fn reset_reads(&self) {
        *self.inner.reads.lock().unwrap() = 0;
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
            // A record never changes, so a name taken with a different payload is a
            // conflict the volume refuses, exactly as the engine refuses one.
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

// ── Fixtures ────────────────────────────────────────────────────────────

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

/// A thread of `n` messages, alternating role, with a title and routing fields set.
fn thread(n: usize) -> ThreadSession {
    let messages = (0..n)
        .map(|i| {
            let role = if i % 2 == 0 { "user" } else { "assistant" };
            message(role, &format!("msg-{i}"))
        })
        .collect();
    ThreadSession {
        id: "t1".to_string(),
        title: Some("a title".to_string()),
        parent: None,
        messages,
        created_at: chrono::Utc::now(),
        updated_at: Some(chrono::Utc::now()),
        completed: true,
        acp_thread_id: Some("acp-1".to_string()),
        turn_completed: 1,
    }
}

fn state(n: usize) -> HashMap<String, ThreadSession> {
    let mut threads = HashMap::new();
    threads.insert("t1".to_string(), thread(n));
    threads
}

fn volume_store(dir: &Path) -> (Arc<dyn RecordStore>, MemVolume) {
    let volume = MemVolume::default();
    let store = VolumeStore::new(volume.clone(), dir);
    (Arc::new(store), volume)
}

fn document_store(dir: &Path) -> Arc<dyn RecordStore> {
    actus::store::open(dir).expect("no store named is the document")
}

// ── The contract, for every shape ───────────────────────────────────────

/// Everything the contract promises, run against one store.
fn contract(store: &dyn RecordStore) {
    // The store names itself and where it is, which is what a client shows a person. The
    // default says it did not, so a store that leaves it is a client showing nothing.
    let described = store.describe();
    assert!(
        !described.is_empty() && described != "unreported",
        "the store names itself: {described}"
    );

    // A round trip is exact, field for field and message for message.
    store.persist(state(5)).expect("persist");
    let read = store.load().expect("load");
    let want = thread(5);
    assert_eq!(read["t1"].messages.len(), 5, "every message comes back");
    for (got, expected) in read["t1"].messages.iter().zip(want.messages.iter()) {
        assert_eq!(got.role, expected.role);
        assert_eq!(got.content, expected.content);
        assert_eq!(got.message_id, expected.message_id);
        assert_eq!(got.entry_type, expected.entry_type);
    }
    assert_eq!(read["t1"].title.as_deref(), Some("a title"));
    assert_eq!(read["t1"].acp_thread_id.as_deref(), Some("acp-1"));
    assert!(read["t1"].completed);

    // The index agrees with the record and holds no messages.
    let index = store.load_index().expect("index");
    assert_eq!(index["t1"].messages.len(), 0, "the index holds no messages");
    assert_eq!(index["t1"].title.as_deref(), Some("a title"));
    assert_eq!(store.message_count("t1").expect("count"), 5);

    // A window holds the window.
    let window = store.load_messages("t1", 1, 2).expect("window");
    assert_eq!(window.len(), 2);
    assert_eq!(window[0].content, "msg-1");
    assert_eq!(window[1].content, "msg-2");
    // A window past the end is empty rather than an error, and a window that starts at the
    // end is empty too.
    assert!(store
        .load_messages("t1", 5, 3)
        .expect("past the end")
        .is_empty());
    assert!(store
        .load_messages("t1", 9, 3)
        .expect("past the end")
        .is_empty());

    // A snapshot that carries a thread's metadata and not its messages is what a caller
    // holding only the threads it touched sends. A store that does not need the whole state
    // keeps what it has, which is what keeps a host's memory off the volume's size.
    if !store.needs_whole_state() {
        let before = store.message_count("t1").expect("count");
        let mut touched = store.load_index().expect("index");
        let thread = touched.get_mut("t1").expect("the thread is in the index");
        thread.messages.clear();
        thread.title = Some("a new title".to_string());
        store.persist(touched).expect("persist a metadata-only snapshot");
        assert_eq!(
            store.message_count("t1").expect("count"),
            before,
            "the count of a thread the caller does not hold survives"
        );
        let read = store.load().expect("load");
        assert_eq!(
            read["t1"].messages.len(),
            before,
            "and so do the messages"
        );
        assert_eq!(
            read["t1"].title.as_deref(),
            Some("a new title"),
            "a metadata change the caller does hold lands"
        );
    }
}

/// A head written before the counts existed is migrated, not read as an empty one.
///
/// This is the loss the shape change could hide: the head before the counts existed was a
/// bare map of threads, and read as this build's it would answer no threads at all. The
/// next persist would write that emptiness back, the records would stay in the volume, and
/// the account would look empty instead of broken. The counts and title versions live in
/// the volume, which is the authority for them, so the migration derives them and the host
/// keeps its list.
#[test]
fn a_head_before_the_counts_is_migrated() {
    let dir = tempfile::tempdir().expect("dir");
    let volume = MemVolume::default();

    // A store writes the records and its own head; the head is then replaced by the older
    // bare shape, which is what a host that upgrades has on disk.
    VolumeStore::new(volume.clone(), dir.path())
        .persist(state(3))
        .expect("persist");
    let mut older = HashMap::new();
    older.insert("t1".to_string(), thread(0));
    std::fs::write(
        dir.path().join("heads.json"),
        serde_json::to_string(&older).expect("serialize"),
    )
    .expect("write the older head");

    // A store opened over it keeps the list and derives the counts from the volume.
    let reopened = VolumeStore::new(volume, dir.path());
    let index = reopened.load_index().expect("the older head migrates");
    assert_eq!(index.len(), 1, "the thread list survives");
    assert_eq!(
        index["t1"].messages.len(),
        0,
        "the index holds no messages"
    );
    assert_eq!(index["t1"].title.as_deref(), Some("a title"));
    assert_eq!(reopened.message_count("t1").expect("count"), 3);
    assert_eq!(
        reopened.load().expect("load")["t1"].messages.len(),
        3,
        "and the record is intact"
    );
}

/// A head of a version this build does not read is refused, and the refusal names the file.
///
/// The version is what tells a head of another shape from this one, and nothing derives an
/// unknown version, so it is refused rather than read as an empty list.
#[test]
fn a_head_of_an_unknown_version_is_refused() {
    let dir = tempfile::tempdir().expect("dir");
    let volume = MemVolume::default();

    std::fs::write(
        dir.path().join("heads.json"),
        serde_json::json!({
            "version": 99,
            "threads": {},
            "counts": {},
            "title_versions": {},
        })
        .to_string(),
    )
    .expect("write a head of another version");

    let store = VolumeStore::new(volume, dir.path());
    let error = store
        .load_index()
        .expect_err("a version this build does not read is refused");
    assert!(error.contains("heads.json"), "the file is named: {error}");
    assert!(error.contains("version 99"), "the version is named: {error}");

    // The refusal is in the load path the server takes, not only the index.
    let error = store.load().expect_err("and in load too");
    assert!(error.contains("heads.json"), "{error}");
}

/// The contract above, run against every store a deployment can select.
///
/// The list is the selector's own, so a store added there without an entry here fails this
/// test with what to do about it. That is the guard: a shape cannot reach a deployment
/// unexercised.
#[test]
fn every_selectable_store_is_covered() {
    for name in actus::store::STORES {
        match *name {
            "document" => {
                let dir = tempfile::tempdir().expect("dir");
                contract(&*document_store(dir.path()));
            }
            other => panic!(
                "'{other}' is selectable and the integrity suite does not cover it: add it here"
            ),
        }
    }
}

/// The volume shape, held to the same contract.
///
/// No selector reaches it any more: a composition supplies it, which is what this
/// deployment does with the engine, and that is the shape a device's record lives in. So it
/// is exercised here by the name it has rather than under a store name nothing can choose.
#[test]
fn the_composed_shape_is_held_to_the_same_contract() {
    let dir = tempfile::tempdir().expect("dir");
    let (volume, _) = volume_store(dir.path());
    contract(&*volume);
}

/// The same two shapes see the same input the same way, which is what a deployment moves
/// between them on.
#[test]
fn every_shape_agrees_on_the_same_input() {
    let a = tempfile::tempdir().expect("dir");
    let b = tempfile::tempdir().expect("dir");
    let document = document_store(a.path());
    let (volume, _) = volume_store(b.path());

    document.persist(state(7)).expect("persist document");
    volume.persist(state(7)).expect("persist volume");

    let from_document = document.load_index().expect("index");
    let from_volume = volume.load_index().expect("index");
    let ids = |threads: &HashMap<String, ThreadSession>| {
        let mut ids: Vec<String> = threads.keys().cloned().collect();
        ids.sort();
        ids
    };
    assert_eq!(
        ids(&from_document),
        ids(&from_volume),
        "the same thread ids"
    );
    assert_eq!(
        from_document["t1"].title, from_volume["t1"].title,
        "the same title"
    );
    assert_eq!(
        document.message_count("t1").expect("count"),
        volume.message_count("t1").expect("count"),
        "the same count"
    );
}

// ── The properties that only a name-addressed store has ────────────────

/// A window reads the window. The reads a window makes are bounded by its size, not by the
/// thread, which is the whole of what a document store cannot do.
#[test]
fn a_window_reads_only_the_window() {
    let dir = tempfile::tempdir().expect("dir");
    let (store, volume) = volume_store(dir.path());
    store.persist(state(200)).expect("persist");

    volume.reset_reads();
    let window = store.load_messages("t1", 100, 10).expect("window");
    assert_eq!(window.len(), 10);
    // One read per position in the window, and no miss because the window ends inside the
    // thread. The thread holds two hundred messages and was not read to answer ten.
    assert_eq!(
        volume.reads(),
        10,
        "a window reads its window, not the thread"
    );

    // A window that reaches the end spends one more read on the miss that ends it.
    volume.reset_reads();
    let tail = store.load_messages("t1", 195, 10).expect("tail");
    assert_eq!(tail.len(), 5);
    assert_eq!(
        volume.reads(),
        6,
        "five messages and the miss that ends them"
    );

    let small = tempfile::tempdir().expect("dir");
    let (short, short_volume) = volume_store(small.path());
    short.persist(state(20)).expect("persist");
    short_volume.reset_reads();
    let window = short.load_messages("t1", 10, 10).expect("window");
    assert_eq!(window.len(), 10);
    assert_eq!(
        short_volume.reads(),
        10,
        "the same reads at a tenth of the thread"
    );
}

/// The index of a store with a head costs no volume reads, which is what makes a listing
/// and a health check cheap over a volume of any size.
#[test]
fn the_index_of_a_head_costs_no_volume_reads() {
    let dir = tempfile::tempdir().expect("dir");
    let (store, volume) = volume_store(dir.path());
    store.persist(state(50)).expect("persist");

    // A second store over the same head answers the index without touching the volume.
    let reopened = MemVolume::default();
    let copy = copy_volume(&volume, &reopened);
    let second = VolumeStore::new(reopened.clone(), dir.path());
    assert_eq!(copy, 50 + 1, "fifty messages and a title version");

    let before = reopened.reads();
    let index = second.load_index().expect("index");
    assert_eq!(index["t1"].messages.len(), 0);
    assert_eq!(second.message_count("t1").expect("count"), 50);
    assert_eq!(
        reopened.reads(),
        before,
        "the head answers the index without reading the volume"
    );
}

/// Copies every record from one volume into another, so a second store can be opened over
/// the same names without a filesystem.
fn copy_volume(from: &MemVolume, to: &MemVolume) -> usize {
    let records = from.inner.records.lock().unwrap();
    let mut into = to.inner.records.lock().unwrap();
    for (name, payload) in records.iter() {
        into.insert(name.clone(), payload.clone());
    }
    into.len()
}

/// A volume that already holds another writer's records is refused at seed, and the
/// refusal names the record, because a record never changes and every deposit would fail.
#[test]
fn a_volume_another_writer_filled_is_refused() {
    // A host that has been serving the document keeps it as the seed, and the volume it
    // seeds onto holds a different record under the name this store would take.
    let dir = tempfile::tempdir().expect("dir");
    document_store(dir.path())
        .persist(state(2))
        .expect("persist the document");

    let volume = MemVolume::default();
    {
        let mut records = volume.inner.records.lock().unwrap();
        records.insert(
            "thread/t1/0".to_string(),
            serde_json::to_vec(&message("user", "a record from another writer"))
                .expect("serialize"),
        );
    }

    let store = VolumeStore::new(volume, dir.path());
    let error = store
        .load()
        .expect_err("a volume of another writer is refused");
    assert!(
        error.contains("thread/t1/0"),
        "the record is named: {error}"
    );
    assert!(
        error.contains("document"),
        "the message names the way out: {error}"
    );
}
