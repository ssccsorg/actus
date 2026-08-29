// Integration tests for the agent execution fabric (issue #9).

use std::sync::Arc;

use actus::agent::{AgentBackend, AgentKind, AgentRegistry};
use actus::zed::backend::ZedBackend;
use actus::zed::control::handle_zed_event;
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

/// Streaming updates to the same message id must replace the existing
/// message in place, wherever it sits in the thread. Zed emits updates for
/// interleaved messages (thinking, tool call, answer), so the same id
/// reappears non-consecutively; appending a duplicate each time swells a
/// turn into dozens of messages and breaks poll/SSE.
#[test]
fn add_message_full_replaces_by_id_anywhere() {
    let dir = tempfile::tempdir().unwrap();
    let mut mgr = zed_manager(dir.path());
    let tid = mgr.get_or_create_thread(None);

    mgr.add_message_full(
        &tid,
        "assistant",
        "thinking v1",
        Some("acp:1".to_string()),
        Some("text".to_string()),
        None,
        None,
    );
    mgr.add_message_full(
        &tid,
        "assistant",
        "tool pending",
        Some("acp:2".to_string()),
        Some("tool_call".to_string()),
        Some("list".to_string()),
        Some("Pending".to_string()),
    );
    // Re-emission of the earlier id must update in place, not append.
    mgr.add_message_full(
        &tid,
        "assistant",
        "thinking v2",
        Some("acp:1".to_string()),
        Some("text".to_string()),
        None,
        None,
    );
    mgr.add_message_full(
        &tid,
        "assistant",
        "tool done",
        Some("acp:2".to_string()),
        Some("tool_call".to_string()),
        Some("list".to_string()),
        Some("Completed".to_string()),
    );
    mgr.add_message_full(
        &tid,
        "assistant",
        "answer",
        Some("acp:3".to_string()),
        Some("text".to_string()),
        None,
        None,
    );

    let msgs = &mgr.threads.get(&tid).unwrap().messages;
    assert_eq!(msgs.len(), 3, "updates must replace, not append");
    assert_eq!(msgs[0].content, "thinking v2");
    assert_eq!(msgs[0].message_id.as_deref(), Some("acp:1"));
    assert_eq!(msgs[1].content, "tool done");
    assert_eq!(msgs[1].tool_status.as_deref(), Some("Completed"));
    assert_eq!(msgs[2].content, "answer");
}

/// Two different ACP threads can both number their messages from 1; the
/// scoped id (`acp_thread_id:message_id`) must keep them distinct so a
/// later thread's message never overwrites an earlier thread's message of
/// the same numeric id.
#[test]
fn scoped_message_ids_do_not_collide_across_acp_threads() {
    let dir = tempfile::tempdir().unwrap();
    let mut mgr = zed_manager(dir.path());
    let tid = mgr.get_or_create_thread(None);

    mgr.add_message_full(
        &tid,
        "assistant",
        "first thread answer",
        Some("acp-thread-a:1".to_string()),
        Some("text".to_string()),
        None,
        None,
    );
    // A new ACP thread reuses the numeric id 1; the scoped ids differ.
    mgr.add_message_full(
        &tid,
        "assistant",
        "second thread answer",
        Some("acp-thread-b:1".to_string()),
        Some("text".to_string()),
        None,
        None,
    );

    let msgs = &mgr.threads.get(&tid).unwrap().messages;
    assert_eq!(msgs.len(), 2, "same numeric id from different ACP threads must coexist");
    assert_eq!(msgs[0].content, "first thread answer");
    assert_eq!(msgs[1].content, "second thread answer");
}

/// A streaming update to a message from the current ACP thread must match
/// only that thread's message, never a same-numeric-id message from an
/// earlier thread.
#[test]
fn scoped_id_update_targets_only_its_acp_thread() {
    let dir = tempfile::tempdir().unwrap();
    let mut mgr = zed_manager(dir.path());
    let tid = mgr.get_or_create_thread(None);

    mgr.add_message_full(
        &tid,
        "assistant",
        "old thread thinking",
        Some("acp-old:1".to_string()),
        Some("text".to_string()),
        None,
        None,
    );
    mgr.add_message_full(
        &tid,
        "assistant",
        "new thread thinking v1",
        Some("acp-new:1".to_string()),
        Some("text".to_string()),
        None,
        None,
    );
    mgr.add_message_full(
        &tid,
        "assistant",
        "new thread thinking v2",
        Some("acp-new:1".to_string()),
        Some("text".to_string()),
        None,
        None,
    );

    let msgs = &mgr.threads.get(&tid).unwrap().messages;
    assert_eq!(msgs.len(), 2);
    assert_eq!(msgs[0].content, "old thread thinking");
    assert_eq!(msgs[1].content, "new thread thinking v2");
}

#[tokio::test]
async fn threads_empty_when_no_state() {
    let backend = zed_backend();
    assert!(backend.threads().await.is_empty());
    assert!(backend.thread("missing").await.is_none());
}

/// A consumed request mapping must drop duplicate completion events: the
/// sentinel (empty string) replaces the thread id on first consumption, so
/// a replayed message_completed for the same request_id cannot bump
/// turn_completed twice.
#[tokio::test]
async fn duplicate_completion_consumed_by_sentinel() {
    let dir = tempfile::tempdir().unwrap();
    let manager = Arc::new(RwLock::new(zed_manager(dir.path())));

    // Set up the local thread and ACP mapping as the submit path would.
    {
        let mut mgr = manager.write().await;
        let tid = mgr.get_or_create_thread(None);
        mgr.add_message(&tid, "user", "hello", None);
        mgr.add_message(&tid, "assistant", "hi there", Some("acp:1".to_string()));
        mgr.thread_id_map.insert("acp-1".to_string(), tid.clone());
        mgr.pending_requests.insert("req-1".to_string(), tid.clone());
    }

    // First completion: consumes the mapping, bumps the counter.
    handle_zed_event(
        &manager,
        r#"{"event_type":"message_completed","data":{"acp_thread_id":"acp-1","request_id":"req-1"}}"#,
    )
    .await;

    // Duplicate completion: sentinel present, must be ignored.
    handle_zed_event(
        &manager,
        r#"{"event_type":"message_completed","data":{"acp_thread_id":"acp-1","request_id":"req-1"}}"#,
    )
    .await;

    let mgr = manager.read().await;
    let tid = mgr.thread_id_map.get("acp-1").unwrap().clone();
    let thread = mgr.threads.get(&tid).unwrap();
    assert_eq!(thread.turn_completed, 1, "duplicate must not bump the counter");
    // The mapping is consumed, not deleted, so the sentinel is observable.
    assert_eq!(mgr.pending_requests.get("req-1").map(String::as_str), Some(""));
}

/// A completion with no assistant output must record an error message so
/// consumers do not see a silent success with zero content.
#[tokio::test]
async fn empty_completion_records_error() {
    let dir = tempfile::tempdir().unwrap();
    let manager = Arc::new(RwLock::new(zed_manager(dir.path())));

    {
        let mut mgr = manager.write().await;
        let tid = mgr.get_or_create_thread(None);
        mgr.add_message(&tid, "user", "hello", None);
        mgr.thread_id_map.insert("acp-2".to_string(), tid.clone());
        mgr.pending_requests.insert("req-2".to_string(), tid.clone());
    }

    handle_zed_event(
        &manager,
        r#"{"event_type":"message_completed","data":{"acp_thread_id":"acp-2","request_id":"req-2"}}"#,
    )
    .await;

    let mgr = manager.read().await;
    let tid = mgr.thread_id_map.get("acp-2").unwrap().clone();
    let thread = mgr.threads.get(&tid).unwrap();
    let last = thread.messages.last().unwrap();
    assert_eq!(last.role, "assistant");
    assert_eq!(last.entry_type.as_deref(), Some("error"));
    assert!(last.content.contains("empty response"));
    assert_eq!(thread.turn_completed, 1);
}

/// An error then a completion for the same request must not double-bump:
/// chat_response_error consumes the mapping, so the follow-up completion
/// is dropped by the sentinel.
#[tokio::test]
async fn error_then_completion_consumes_once() {
    let dir = tempfile::tempdir().unwrap();
    let manager = Arc::new(RwLock::new(zed_manager(dir.path())));

    {
        let mut mgr = manager.write().await;
        let tid = mgr.get_or_create_thread(None);
        mgr.add_message(&tid, "user", "hello", None);
        mgr.thread_id_map.insert("acp-3".to_string(), tid.clone());
        mgr.pending_requests.insert("req-3".to_string(), tid.clone());
    }

    handle_zed_event(
        &manager,
        r#"{"event_type":"chat_response_error","data":{"request_id":"req-3","error":"boom"}}"#,
    )
    .await;
    handle_zed_event(
        &manager,
        r#"{"event_type":"message_completed","data":{"acp_thread_id":"acp-3","request_id":"req-3"}}"#,
    )
    .await;

    let mgr = manager.read().await;
    let tid = mgr.thread_id_map.get("acp-3").unwrap().clone();
    let thread = mgr.threads.get(&tid).unwrap();
    assert_eq!(thread.turn_completed, 1, "error+completion must bump once");
    // The error message, not an empty-response error, is the last entry.
    let last = thread.messages.last().unwrap();
    assert_eq!(last.entry_type.as_deref(), Some("error"));
    assert!(last.content.contains("boom"));
}

/// A follow-up turn's replay of a previous turn's entry (same scoped id and
/// content) must be dropped: after turn completion the entries are
/// snapshotted, and a new turn receiving the same (id, content) is a
/// wrapper replay, not new content.
#[tokio::test]
async fn replay_of_prior_turn_entry_is_dropped() {
    let dir = tempfile::tempdir().unwrap();
    let manager = Arc::new(RwLock::new(zed_manager(dir.path())));

    // Simulate turn 1: thinking + tool call + answer, then completion.
    {
        let mut mgr = manager.write().await;
        let tid = mgr.get_or_create_thread(None);
        mgr.add_message(&tid, "user", "first", None);
        mgr.thread_id_map.insert("acp-4".to_string(), tid.clone());
        mgr.pending_requests.insert("req-4".to_string(), tid.clone());
    }
    handle_zed_event(
        &manager,
        r#"{"event_type":"message_added","data":{"acp_thread_id":"acp-4","message_id":"1","role":"assistant","content":"<thinking>first</thinking>","entry_type":"text"}}"#,
    )
    .await;
    handle_zed_event(
        &manager,
        r#"{"event_type":"message_added","data":{"acp_thread_id":"acp-4","message_id":"2","role":"assistant","content":"**Tool Call: ls**","entry_type":"tool_call","tool_name":"ls","tool_status":"Completed"}}"#,
    )
    .await;
    handle_zed_event(
        &manager,
        r#"{"event_type":"message_added","data":{"acp_thread_id":"acp-4","message_id":"3","role":"assistant","content":"answer one","entry_type":"text"}}"#,
    )
    .await;
    handle_zed_event(
        &manager,
        r#"{"event_type":"message_completed","data":{"acp_thread_id":"acp-4","request_id":"req-4"}}"#,
    )
    .await;

    let msg_count_after_turn1 = {
        let mgr = manager.read().await;
        let tid = mgr.thread_id_map.get("acp-4").unwrap().clone();
        mgr.threads.get(&tid).unwrap().messages.len()
    };

    // Turn 2 starts: the wrapper replays turn 1 entries before new content.
    {
        let mut mgr = manager.write().await;
        let tid = mgr.thread_id_map.get("acp-4").unwrap().clone();
        mgr.add_message(&tid, "user", "second", None);
        mgr.pending_requests.insert("req-5".to_string(), tid.clone());
    }
    handle_zed_event(
        &manager,
        r#"{"event_type":"message_added","data":{"acp_thread_id":"acp-4","message_id":"1","role":"assistant","content":"<thinking>first</thinking>","entry_type":"text"}}"#,
    )
    .await;
    handle_zed_event(
        &manager,
        r#"{"event_type":"message_added","data":{"acp_thread_id":"acp-4","message_id":"2","role":"assistant","content":"**Tool Call: ls**","entry_type":"tool_call","tool_name":"ls","tool_status":"Completed"}}"#,
    )
    .await;
    // Genuinely new content under a reused id: must be accepted.
    handle_zed_event(
        &manager,
        r#"{"event_type":"message_added","data":{"acp_thread_id":"acp-4","message_id":"3","role":"assistant","content":"<thinking>second</thinking>","entry_type":"text"}}"#,
    )
    .await;

    let mgr = manager.read().await;
    let tid = mgr.thread_id_map.get("acp-4").unwrap().clone();
    let thread = mgr.threads.get(&tid).unwrap();
    // Turn 1 messages (user first + 3 assistant) plus the turn 2 user and
    // one new-id-3 content; the two replayed ids 1 and 2 are dropped.
    let assistant_msgs: Vec<_> = thread
        .messages
        .iter()
        .filter(|m| m.role == "assistant")
        .collect();
    assert_eq!(
        assistant_msgs.len(),
        4,
        "replays must be dropped; new content accepted (got {})",
        assistant_msgs.len()
    );
    assert!(msg_count_after_turn1 < thread.messages.len());
    // The last assistant message carries the new turn's content.
    let last_asst = assistant_msgs.last().unwrap();
    assert!(last_asst.content.contains("second"));
}

/// consume_request must bound the sentinel map: once the cap is exceeded,
/// old consumed entries are pruned while the most recent sentinel and all
/// active (non-empty) mappings survive.
#[test]
fn consume_request_prunes_old_sentinels() {
    let dir = tempfile::tempdir().unwrap();
    let mut mgr = zed_manager(dir.path());
    mgr.sentinel_cap = 4;

    // Fill with active mappings plus consumed sentinels past the cap.
    mgr.pending_requests.insert("active-1".to_string(), "tid-a".to_string());
    for i in 0..6 {
        mgr.consume_request(&format!("req-{}", i));
    }

    assert!(
        mgr.pending_requests.len() <= mgr.sentinel_cap,
        "map must stay bounded (len {})",
        mgr.pending_requests.len()
    );
    // The active mapping is never pruned.
    assert_eq!(mgr.pending_requests.get("active-1").map(String::as_str), Some("tid-a"));
    // The most recent sentinel survives for duplicate detection.
    assert_eq!(mgr.pending_requests.get("req-5").map(String::as_str), Some(""));
}

/// A thread_created for a request whose mapping was already consumed (empty
/// sentinel) is a stale replay and must not map to an empty local id.
#[tokio::test]
async fn thread_created_after_consumption_is_ignored() {
    let dir = tempfile::tempdir().unwrap();
    let manager = Arc::new(RwLock::new(zed_manager(dir.path())));

    {
        let mut mgr = manager.write().await;
        let tid = mgr.get_or_create_thread(None);
        mgr.add_message(&tid, "user", "hello", None);
        mgr.pending_requests.insert("req-6".to_string(), tid.clone());
        // Turn already ended: mapping consumed to the empty sentinel.
        mgr.consume_request("req-6");
    }

    handle_zed_event(
        &manager,
        r#"{"event_type":"thread_created","data":{"acp_thread_id":"acp-6","request_id":"req-6"}}"#,
    )
    .await;

    let mgr = manager.read().await;
    // The stale acp id must not be mapped to an empty local id.
    assert!(
        !mgr.thread_id_map.contains_key("acp-6"),
        "consumed request must not create a mapping"
    );
}

/// chat_response_error then thread_created for the same request: the error
/// consumes the mapping, so the late thread_created is a stale replay and
/// is ignored, leaving no empty mapping.
#[tokio::test]
async fn error_then_thread_created_ignored() {
    let dir = tempfile::tempdir().unwrap();
    let manager = Arc::new(RwLock::new(zed_manager(dir.path())));

    {
        let mut mgr = manager.write().await;
        let tid = mgr.get_or_create_thread(None);
        mgr.add_message(&tid, "user", "hello", None);
        mgr.thread_id_map.insert("acp-7".to_string(), tid.clone());
        mgr.pending_requests.insert("req-7".to_string(), tid.clone());
    }

    handle_zed_event(
        &manager,
        r#"{"event_type":"chat_response_error","data":{"request_id":"req-7","error":"boom"}}"#,
    )
    .await;
    handle_zed_event(
        &manager,
        r#"{"event_type":"thread_created","data":{"acp_thread_id":"acp-7b","request_id":"req-7"}}"#,
    )
    .await;

    let mgr = manager.read().await;
    assert!(
        !mgr.thread_id_map.contains_key("acp-7b"),
        "late thread_created after error must be ignored"
    );
    assert_eq!(mgr.pending_requests.get("req-7").map(String::as_str), Some(""));
}
