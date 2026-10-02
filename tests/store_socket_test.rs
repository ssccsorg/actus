//! The record store over a socket: what it deposits, and what it reads back.
//!
//! The daemon that serves a volume is another repository's, so this holds actus's side of
//! the seam against a stand-in that keeps the one rule the wire depends on: a name taken
//! with the payload it holds is a retry, and a name taken with a different payload is
//! refused. A deposit that was not byte-identical would fail here rather than quietly
//! leaving a second record.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::sync::{Arc, Mutex};

use actus::agent::{ThreadMessage, ThreadSession};
use actus::store::{RecordStore, SocketStore, SocketVolume};

/// A stand-in for the volume: names to payloads, with the engine's conflict rule.
#[derive(Default)]
struct Volume {
    records: HashMap<String, Vec<u8>>,
    refused: usize,
}

fn serve(volume: Arc<Mutex<Volume>>, listener: UnixListener) {
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { continue };
            let volume = Arc::clone(&volume);
            std::thread::spawn(move || handle(stream, volume));
        }
    });
}

fn handle(stream: UnixStream, volume: Arc<Mutex<Volume>>) {
    let reader = BufReader::new(stream.try_clone().expect("clone"));
    let mut writer = stream;
    for line in reader.lines() {
        let Ok(line) = line else { return };
        if line.trim().is_empty() {
            continue;
        }
        let request: serde_json::Value = serde_json::from_str(line.trim()).expect("a request");
        let answer = match request["verb"].as_str().unwrap_or_default() {
            "write_fact_named" => {
                let name = request["name"].as_str().expect("a name").to_string();
                let payload = from_hex(request["hex"].as_str().expect("a payload"));
                let mut volume = volume.lock().unwrap();
                match volume.records.get(&name) {
                    Some(held) if held != &payload => {
                        volume.refused += 1;
                        serde_json::json!({ "ok": false, "error": "that name holds another payload" })
                    }
                    _ => {
                        volume.records.insert(name, payload);
                        serde_json::json!({ "ok": true, "id": "f_000000" })
                    }
                }
            }
            "describe" => {
                let volume = volume.lock().unwrap();
                serde_json::json!({ "ok": true, "format": "test", "count": volume.records.len() })
            }
            "read_payload" => {
                let name = request["name"].as_str().expect("a name");
                let volume = volume.lock().unwrap();
                match volume.records.get(name) {
                    Some(payload) => serde_json::json!({ "ok": true, "hex": to_hex(payload) }),
                    None => serde_json::json!({ "ok": true, "hex": null }),
                }
            }
            other => serde_json::json!({ "ok": false, "error": format!("no verb {other}") }),
        };
        if writeln!(writer, "{answer}").and_then(|()| writer.flush()).is_err() {
            return;
        }
    }
}

fn bind(dir: &std::path::Path, name: &str) -> (Arc<Mutex<Volume>>, std::path::PathBuf) {
    let socket = dir.join(name);
    let listener = UnixListener::bind(&socket).expect("bind");
    let volume = Arc::new(Mutex::new(Volume::default()));
    serve(Arc::clone(&volume), listener);
    (volume, socket)
}

fn message(role: &str, content: &str) -> ThreadMessage {
    ThreadMessage {
        role: role.to_string(),
        content: content.to_string(),
        message_id: Some(format!("m-{content}")),
        entry_type: Some("text".to_string()),
        tool_name: None,
        tool_status: None,
        timestamp: chrono::Utc::now(),
    }
}

fn thread(messages: Vec<ThreadMessage>, title: Option<&str>) -> ThreadSession {
    ThreadSession {
        id: "t1".to_string(),
        title: title.map(str::to_string),
        parent: None,
        messages,
        created_at: chrono::Utc::now(),
        updated_at: None,
        completed: true,
        acp_thread_id: Some("acp-1".to_string()),
        turn_completed: 1,
    }
}

fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn from_hex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).expect("hex"))
        .collect()
}

/// A deposit lands one record per message and one per title, and a second store over the
/// same volume reads them back and appends nothing it already wrote.
#[test]
fn a_thread_round_trips_through_record_names() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (volume, socket) = bind(dir.path(), "volume.sock");
    let store = SocketStore::new(SocketVolume::new(socket.clone()), dir.path());

    assert_eq!(store.load().expect("load an empty volume").len(), 0);

    let mut threads = HashMap::new();
    threads.insert(
        "t1".to_string(),
        thread(
            vec![message("user", "hi"), message("assistant", "hello")],
            Some("hi"),
        ),
    );
    store.persist(threads.clone()).expect("persist");

    {
        let volume = volume.lock().unwrap();
        assert!(volume.records.contains_key("thread/t1/0"), "the first message");
        assert!(volume.records.contains_key("thread/t1/1"), "the second message");
        assert!(
            volume.records.contains_key("thread/t1/title/0"),
            "the first title version"
        );
        assert_eq!(volume.records.len(), 3, "one record per item and nothing else");
        assert_eq!(volume.refused, 0);
    }

    // The head is actus's own document, and it carries no messages: it is what keeps a
    // persist small.
    let head: HashMap<String, ThreadSession> =
        serde_json::from_str(&std::fs::read_to_string(dir.path().join("heads.json")).expect("head"))
            .expect("a thread document");
    assert!(head["t1"].messages.is_empty(), "the head holds a thread list, not a log");

    // A store that has just started reads the same thread back.
    let restarted = SocketStore::new(SocketVolume::new(socket.clone()), dir.path());
    let read = restarted.load().expect("load");
    assert_eq!(read["t1"].messages.len(), 2);
    assert_eq!(read["t1"].messages[0].content, "hi");
    assert_eq!(read["t1"].title.as_deref(), Some("hi"));
    assert_eq!(read["t1"].acp_thread_id.as_deref(), Some("acp-1"));

    // A second persist of the same state writes nothing: the same name with the payload
    // it holds is a retry, and the stand-in would refuse anything else.
    restarted.persist(read.clone()).expect("persist again");
    {
        let volume = volume.lock().unwrap();
        assert_eq!(volume.records.len(), 3, "a repeat appended nothing");
        assert_eq!(volume.refused, 0, "a repeat wrote a different payload");
    }
}

/// A message added and a title changed are a tail append and one more title version.
#[test]
fn an_extension_appends_only_what_is_new() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (volume, socket) = bind(dir.path(), "volume.sock");
    let store = SocketStore::new(SocketVolume::new(socket.clone()), dir.path());

    let mut threads = HashMap::new();
    threads.insert(
        "t1".to_string(),
        thread(vec![message("user", "hi")], Some("hi")),
    );
    store.persist(threads.clone()).expect("persist");
    assert_eq!(volume.lock().unwrap().records.len(), 2);

    let mut grown = threads.clone();
    let thread = grown.get_mut("t1").unwrap();
    thread.messages.push(message("assistant", "hello"));
    thread.title = Some("hi again".to_string());
    store.persist(grown.clone()).expect("persist the extension");

    {
        let volume = volume.lock().unwrap();
        assert_eq!(volume.records.len(), 4, "one message and one title version");
        assert!(volume.records.contains_key("thread/t1/1"));
        assert!(volume.records.contains_key("thread/t1/title/1"));
        assert_eq!(volume.refused, 0);
    }

    let read = SocketStore::new(SocketVolume::new(socket), dir.path())
        .load()
        .expect("load");
    assert_eq!(read["t1"].messages.len(), 2);
    assert_eq!(read["t1"].title.as_deref(), Some("hi again"), "the last version wins");
}

/// A host that has been serving the document keeps its history: the first start reads the
/// document, and the first persist deposits it as records.
#[test]
fn a_first_start_seeds_from_the_document() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (volume, socket) = bind(dir.path(), "volume.sock");

    let mut threads = HashMap::new();
    threads.insert(
        "t1".to_string(),
        thread(vec![message("user", "from the document")], Some("old")),
    );
    std::fs::write(
        dir.path().join("threads.json"),
        serde_json::to_string(&threads).expect("serialize"),
    )
    .expect("write the document");

    let store = SocketStore::new(SocketVolume::new(socket.clone()), dir.path());
    let loaded = store.load().expect("load");
    assert_eq!(loaded["t1"].messages.len(), 1, "the document is the seed");
    assert_eq!(volume.lock().unwrap().records.len(), 0, "nothing was deposited yet");

    store.persist(loaded.clone()).expect("persist");
    assert_eq!(volume.lock().unwrap().records.len(), 2, "a message and a title");

    // The document is read and never written, so giving the store up is complete.
    let document: HashMap<String, ThreadSession> =
        serde_json::from_str(&std::fs::read_to_string(dir.path().join("threads.json")).unwrap())
            .unwrap();
    assert_eq!(document["t1"].messages.len(), 1);
}

/// A volume another writer filled is refused when the store seeds, because a name this
/// store would take and did not write is a volume it can never add to: every deposit would
/// be refused and a turn would live only in memory.
#[test]
fn a_volume_another_writer_filled_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (volume, socket) = bind(dir.path(), "volume.sock");
    volume.lock().unwrap().records.insert(
        "thread/t1/0".to_string(),
        b"{\"role\":\"user\",\"content\":\"another writer\"}".to_vec(),
    );

    let mut threads = HashMap::new();
    threads.insert(
        "t1".to_string(),
        thread(vec![message("user", "mine")], None),
    );
    std::fs::write(
        dir.path().join("threads.json"),
        serde_json::to_string(&threads).expect("serialize"),
    )
    .expect("the document");

    let store = SocketStore::new(SocketVolume::new(socket), dir.path());
    let error = match store.load() {
        Ok(_) => panic!("a volume another writer filled must be refused"),
        Err(error) => error,
    };
    assert!(
        error.contains("one writer") && error.contains("thread/t1/0"),
        "{error}"
    );
}

/// The environment selects the store, and a socket store with no variable naming a socket
/// takes the one that follows the agent's own directory. A name that is not a store is a
/// misconfiguration rather than a reason to serve the document instead.
#[test]
fn the_environment_selects_the_store() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (volume, _socket) = bind(dir.path(), "store.sock");

    unsafe {
        std::env::set_var("ACTUS_RECORD_STORE", "socket");
        std::env::remove_var("ACTUS_RECORD_STORE_SOCKET");
    }
    let store = actus::store::open(dir.path()).expect("the default socket is the store");
    assert_eq!(store.load().expect("load").len(), 0);

    let mut threads = HashMap::new();
    threads.insert("t1".to_string(), thread(vec![message("user", "hi")], None));
    store.persist(threads.clone()).expect("persist");
    assert!(volume.lock().unwrap().records.contains_key("thread/t1/0"));

    unsafe { std::env::set_var("ACTUS_RECORD_STORE", "elsewhere") };
    let error = match actus::store::open(dir.path()) {
        Ok(_) => panic!("a name that is not a store must fail"),
        Err(error) => error,
    };
    assert!(error.contains("names no store"), "{error}");
    unsafe { std::env::remove_var("ACTUS_RECORD_STORE") };
}
