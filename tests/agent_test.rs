// Integration tests for the agent execution fabric (issue #9).

use std::sync::Arc;

use actus::agent::{AgentBackend, AgentKind, AgentRegistry};
use actus::zed::backend::ZedBackend;
use actus::zed::{WsCommandTx, ZedManager};
use tokio::sync::RwLock;

fn zed_manager(dir: &std::path::Path) -> ZedManager {
    ZedManager::new(
        "ses_test".to_string(),
        "127.0.0.1:9999".to_string(),
        dir,
    )
}

#[test]
fn agent_kind_roundtrip() {
    assert_eq!(AgentKind::parse("zed"), Some(AgentKind::Zed));
    assert_eq!(AgentKind::parse("langgraph"), Some(AgentKind::LangGraph));
    assert_eq!(AgentKind::parse("native"), Some(AgentKind::Native));
    assert_eq!(AgentKind::parse("unknown"), None);
    assert_eq!(AgentKind::Zed.as_str(), "zed");
    assert_eq!(serde_json::to_string(&AgentKind::Zed).unwrap(), "\"zed\"");
    assert_eq!(
        serde_json::from_str::<AgentKind>("\"langgraph\"").unwrap(),
        AgentKind::LangGraph
    );
}

fn zed_backend() -> ZedBackend {
    let dir = tempfile::tempdir().expect("tempdir");
    let manager = Arc::new(RwLock::new(ZedManager::new(
        "ses_test".to_string(),
        "127.0.0.1:9999".to_string(),
        dir.path(),
    )));
    let ws_tx: WsCommandTx = Arc::new(tokio::sync::Mutex::new(None));
    ZedBackend { manager, ws_tx }
}

#[tokio::test]
async fn registry_default_agent_status() {
    let mut registry = AgentRegistry::new();
    registry.register(Arc::new(zed_backend()), true);

    let default = registry.default_agent().expect("default agent");
    let status = default.status().await;
    assert_eq!(status.name, "zed");
    assert_eq!(status.kind, AgentKind::Zed);
    assert!(!status.connected);
    assert!(!status.ready);

    let statuses = registry.statuses().await;
    assert_eq!(statuses.len(), 1);
    assert_eq!(statuses[0].name, "zed");
}

#[tokio::test]
async fn registry_get_by_name() {
    let mut registry = AgentRegistry::new();
    registry.register(Arc::new(zed_backend()), true);

    assert!(registry.get("zed").is_some());
    assert!(registry.get("missing").is_none());
}

#[tokio::test]
async fn submit_fails_when_not_connected() {
    let backend = zed_backend();
    let err = backend.submit(None, "hello").await.expect_err("must fail");
    assert!(
        err.contains("not connected") || err.contains("not ready"),
        "unexpected error: {}",
        err
    );
}

/// A failed submit must not leak its request mapping: the entry is
/// inserted before the WebSocket send, and removed on both failure paths
/// (no sender, closed sender). Without the cleanup the map grows without
/// bound across repeated failed submissions.
#[tokio::test]
async fn failed_submit_cleans_pending_requests() {
    let backend = zed_backend();

    // Not connected: submit fails before inserting a mapping.
    {
        let mgr = backend.manager.read().await;
        assert!(mgr.pending_requests.is_empty());
    }
    let _ = backend.submit(None, "hello").await;
    {
        let mgr = backend.manager.read().await;
        assert!(mgr.pending_requests.is_empty());
    }

    // Connected but sender channel closed: submit inserts the mapping,
    // the send fails, and the mapping must be removed again.
    {
        let mut mgr = backend.manager.write().await;
        mgr.zed_connected = true;
        mgr.agent_ready = true;
    }
    {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        drop(rx); // closed sender
        let mut guard = backend.ws_tx.lock().await;
        *guard = Some(tx);
    }
    let _ = backend.submit(None, "hello").await;
    {
        let mgr = backend.manager.read().await;
        assert!(
            mgr.pending_requests.is_empty(),
            "closed-sender submit must not leak a request mapping"
        );
    }
}

/// Truncation of titles and conversation context must never split a
/// multi-byte UTF-8 character. The pre-fix code sliced at raw byte
/// offsets (`&s[..80]`, `&s[..2000]`), which panics on non-ASCII input
/// longer than the limit.
#[test]
fn truncation_never_splits_multibyte_chars() {
    let dir = tempfile::tempdir().unwrap();
    let mut mgr = zed_manager(dir.path());
    let tid = mgr.get_or_create_thread(None);

    // Title: a long string of multi-byte chars (Korean) plus ASCII.
    let long_title = format!("{}end", "한글".repeat(60)); // > 80 bytes, boundary not at 80
    mgr.set_title(&tid, &long_title);
    let title = mgr.threads.get(&tid).unwrap().title.clone().unwrap();
    assert!(title.is_char_boundary(title.len()));
    assert!(title.len() <= 80 + 3, "title too long: {}", title.len());
    assert!(title.ends_with("..."), "long title must carry ellipsis");

    // Conversation context: add history with a long multi-byte message,
    // then a new user message, and format the context.
    mgr.add_message(&tid, "user", &"가나다".repeat(3000), None);
    mgr.add_message(&tid, "assistant", "ok", None);
    mgr.add_message(&tid, "user", "next", None);
    let ctx = mgr.format_conversation_context(&tid).unwrap();
    assert!(ctx.contains("[Previous conversation]"));
    assert!(ctx.is_char_boundary(ctx.len()));
}

/// A stale acp_thread_id from a previous session must be cleared when
/// the thread is prepared again, and the reverse map entry dropped, so a
/// resumed thread does not resume the wrong Zed thread.
#[test]
fn prepare_message_clears_stale_acp_mapping() {
    let dir = tempfile::tempdir().unwrap();
    let mut mgr = zed_manager(dir.path());
    let tid = mgr.get_or_create_thread(None);

    // Simulate a persisted thread with an acp id from a past session.
    {
        let thread = mgr.threads.get_mut(&tid).unwrap();
        thread.acp_thread_id = Some("acp_stale".to_string());
        thread.messages.push(actus::agent::ThreadMessage {
            role: "user".to_string(),
            content: "hello".to_string(),
            message_id: None,
            entry_type: None,
            tool_name: None,
            tool_status: None,
            timestamp: chrono::Utc::now(),
        });
        thread.messages.push(actus::agent::ThreadMessage {
            role: "assistant".to_string(),
            content: "hi".to_string(),
            message_id: None,
            entry_type: Some("text".to_string()),
            tool_name: None,
            tool_status: None,
            timestamp: chrono::Utc::now(),
        });
    }
    mgr.thread_id_map.insert("acp_stale".to_string(), tid.clone());

    // First prepare in this session clears the stale mapping.
    let msg = mgr.prepare_message(&tid, "again");
    assert!(mgr.get_acp_thread_id(&tid).is_none());
    assert!(!mgr.thread_id_map.contains_key("acp_stale"));
    assert!(msg.contains("[Previous conversation]"), "context must be injected");

    // submit marks the thread activated; a second prepare then keeps the
    // (now established) mapping untouched and sends the raw message.
    mgr.threads_activated.insert(tid.clone());
    let msg2 = mgr.prepare_message(&tid, "again2");
    assert_eq!(msg2, "again2");
}

/// Persisted threads whose turn_completed drifted past the assistant
/// message count (an errored or cancelled turn bumped the counter without
/// adding a message) must be repaired on load, or poll/SSE index past the
/// end of the message array and the client sees `completed: true` with no
/// content.
#[test]
fn load_threads_repairs_turn_counter_drift() {
    use actus::agent::ThreadMessage;
    use std::io::Write;

    let dir = tempfile::tempdir().unwrap();
    let threads_file = dir.path().join("threads.json");

    // Two completed turns (two assistant messages) but a drifted counter.
    let thread = actus::agent::ThreadSession {
        id: "t1".to_string(),
        title: Some("drift".to_string()),
        messages: vec![
            ThreadMessage {
                role: "user".to_string(),
                content: "hi".to_string(),
                message_id: None,
                entry_type: None,
                tool_name: None,
                tool_status: None,
                timestamp: chrono::Utc::now(),
            },
            ThreadMessage {
                role: "assistant".to_string(),
                content: "hello".to_string(),
                message_id: None,
                entry_type: Some("text".to_string()),
                tool_name: None,
                tool_status: None,
                timestamp: chrono::Utc::now(),
            },
            ThreadMessage {
                role: "user".to_string(),
                content: "again".to_string(),
                message_id: None,
                entry_type: None,
                tool_name: None,
                tool_status: None,
                timestamp: chrono::Utc::now(),
            },
            ThreadMessage {
                role: "assistant".to_string(),
                content: "again hello".to_string(),
                message_id: None,
                entry_type: Some("text".to_string()),
                tool_name: None,
                tool_status: None,
                timestamp: chrono::Utc::now(),
            },
        ],
        created_at: chrono::Utc::now(),
        completed: true,
        acp_thread_id: None,
        turn_completed: 4, // drifted: only 2 assistant messages exist
    };
    let mut map = std::collections::HashMap::new();
    map.insert("t1".to_string(), thread);
    let mut f = std::fs::File::create(&threads_file).unwrap();
    f.write_all(serde_json::to_string_pretty(&map).unwrap().as_bytes())
        .unwrap();

    let loaded = ZedManager::load_threads(&threads_file);
    let repaired = loaded.get("t1").expect("thread loaded");
    assert_eq!(repaired.turn_completed, 2, "counter must be repaired to message count");

    // Tool-call assistant messages must not count toward the turn counter.
    let mut map2 = map.clone();
    let thread = map2.get_mut("t1").unwrap();
    thread.messages.push(ThreadMessage {
        role: "assistant".to_string(),
        content: "".to_string(),
        message_id: None,
        entry_type: Some("tool_call".to_string()),
        tool_name: Some("search".to_string()),
        tool_status: None,
        timestamp: chrono::Utc::now(),
    });
    let mut f = std::fs::File::create(&threads_file).unwrap();
    f.write_all(serde_json::to_string_pretty(&map2).unwrap().as_bytes())
        .unwrap();
    let loaded = ZedManager::load_threads(&threads_file);
    let repaired = loaded.get("t1").unwrap();
    assert_eq!(repaired.turn_completed, 2, "tool_call entries must be excluded");
}

#[tokio::test]
async fn threads_empty_when_no_state() {
    let backend = zed_backend();
    assert!(backend.threads().await.is_empty());
    assert!(backend.thread("missing").await.is_none());
}
