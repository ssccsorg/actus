// Serialization round-trip tests for the agent-client protocol types (issue #9).
//
// The SyncEvent enum is adjacently tagged (`event_type` + `data`), which
// is the wire format the executor emits over the WebSocket. These tests pin that
// format and prove every variant survives a JSON round trip, including
// the defaulted fields of MessageAdded.

use actus::acp_ws::types::{IncomingChatMessage, OutgoingMessage, SyncEvent};

#[test]
fn sync_event_roundtrip_all_variants() {
    let cases = vec![
        SyncEvent::ThreadCreated {
            acp_thread_id: "acp-1".to_string(),
            request_id: "req-1".to_string(),
        },
        SyncEvent::ThreadTitleChanged {
            acp_thread_id: "acp-1".to_string(),
            title: "Fix the parser".to_string(),
        },
        SyncEvent::MessageAdded {
            acp_thread_id: "acp-1".to_string(),
            message_id: "msg-1".to_string(),
            role: "assistant".to_string(),
            content: "hello".to_string(),
            request_id: "req-1".to_string(),
            entry_type: "text".to_string(),
            tool_name: String::new(),
            tool_status: String::new(),
            timestamp: 1_700_000_000,
        },
        SyncEvent::MessageCompleted {
            acp_thread_id: "acp-1".to_string(),
            message_id: "msg-1".to_string(),
            request_id: "req-1".to_string(),
        },
        SyncEvent::ChatResponseError {
            request_id: "req-1".to_string(),
            error: "agent turn aborted".to_string(),
        },
        SyncEvent::AgentReady {
            agent_name: "telos".to_string(),
            thread_id: Some("acp-1".to_string()),
        },
        SyncEvent::AgentReady {
            agent_name: "telos".to_string(),
            thread_id: None,
        },
        SyncEvent::TurnCancelled {
            request_id: "req-1".to_string(),
            status: "cancelled".to_string(),
        },
        SyncEvent::ToolCallAuthorizationRequested {
            acp_thread_id: "acp-1".to_string(),
            tool_call_id: "tool-1".to_string(),
            tool_name: "bash".to_string(),
        },
    ];

    for event in cases {
        let json = serde_json::to_string(&event).unwrap();
        let back: SyncEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(back, event, "round trip failed for {json}");
    }
}

#[test]
fn sync_event_wire_format_has_tag_and_data() {
    // The wire format must be { "event_type": ..., "data": ... } so the
    // serde representation matches what Telos actually emits.
    let event = SyncEvent::ThreadCreated {
        acp_thread_id: "acp-9".to_string(),
        request_id: "req-9".to_string(),
    };
    let value: serde_json::Value = serde_json::to_value(&event).unwrap();
    assert_eq!(value["event_type"], "thread_created");
    assert_eq!(value["data"]["acp_thread_id"], "acp-9");
    assert_eq!(value["data"]["request_id"], "req-9");

    // Parsing a raw wire-format frame produced by Telos must work.
    let raw = r#"{
        "event_type": "message_added",
        "data": {
            "acp_thread_id": "acp-9",
            "message_id": "msg-9",
            "role": "assistant",
            "content": "hi",
            "timestamp": 1700000000
        }
    }"#;
    let parsed: SyncEvent = serde_json::from_str(raw).unwrap();
    match parsed {
        SyncEvent::MessageAdded { acp_thread_id, role, .. } => {
            assert_eq!(acp_thread_id, "acp-9");
            assert_eq!(role, "assistant");
        }
        other => panic!("expected MessageAdded, got {other:?}"),
    }
}

#[test]
fn message_added_defaults_missing_metadata() {
    // request_id, entry_type, tool_name, tool_status are optional on the
    // wire; missing fields must deserialize to empty strings.
    let raw = r#"{
        "event_type": "message_added",
        "data": {
            "acp_thread_id": "acp-1",
            "message_id": "msg-1",
            "role": "user",
            "content": "ping",
            "timestamp": 1700000000
        }
    }"#;
    let parsed: SyncEvent = serde_json::from_str(raw).unwrap();
    match parsed {
        SyncEvent::MessageAdded {
            request_id,
            entry_type,
            tool_name,
            tool_status,
            ..
        } => {
            assert_eq!(request_id, "");
            assert_eq!(entry_type, "");
            assert_eq!(tool_name, "");
            assert_eq!(tool_status, "");
        }
        other => panic!("expected MessageAdded, got {other:?}"),
    }
}

#[test]
fn into_outgoing_message_maps_event_to_wire() {
    let event = SyncEvent::ToolCallAuthorizationRequested {
        acp_thread_id: "acp-1".to_string(),
        tool_call_id: "tool-1".to_string(),
        tool_name: "bash".to_string(),
    };
    let msg = event.into_outgoing_message();
    assert_eq!(msg.event_type, "tool_call_authorization_requested");
    assert_eq!(msg.data["acp_thread_id"], "acp-1");
    assert_eq!(msg.data["tool_call_id"], "tool-1");
    assert_eq!(msg.data["tool_name"], "bash");
}

#[test]
fn outgoing_message_roundtrip() {
    let msg = OutgoingMessage {
        event_type: "message_added".to_string(),
        data: serde_json::json!({ "acp_thread_id": "acp-1", "message_id": "msg-1" }),
    };
    let json = serde_json::to_string(&msg).unwrap();
    let back: OutgoingMessage = serde_json::from_str(&json).unwrap();
    assert_eq!(back.event_type, "message_added");
    assert_eq!(back.data["message_id"], "msg-1");
}

#[test]
fn incoming_chat_message_roundtrip_and_defaults() {
    let msg = IncomingChatMessage {
        acp_thread_id: Some("acp-1".to_string()),
        message: "hello".to_string(),
        request_id: "req-1".to_string(),
        agent_name: Some("telos".to_string()),
        interrupt: true,
    };
    let json = serde_json::to_string(&msg).unwrap();
    let back: IncomingChatMessage = serde_json::from_str(&json).unwrap();
    assert_eq!(back.acp_thread_id.as_deref(), Some("acp-1"));
    assert_eq!(back.message, "hello");
    assert_eq!(back.agent_name.as_deref(), Some("telos"));
    assert!(back.interrupt);

    // Optional fields default when absent.
    let raw = r#"{"acp_thread_id": null, "message": "hi", "request_id": "req-2"}"#;
    let parsed: IncomingChatMessage = serde_json::from_str(raw).unwrap();
    assert_eq!(parsed.acp_thread_id, None);
    assert_eq!(parsed.agent_name, None);
    assert!(!parsed.interrupt);
}

/// The contract tolerates a field the executor left out: a peer that says less is speaking the
/// same contract. A field of the wrong type is refused, and so is an event the contract does
/// not name, which is the difference between writing the contract down and reading whatever
/// arrives.
#[test]
fn an_absent_field_is_tolerated_and_a_wrong_type_is_not() {
    let sparse =
        r#"{"event_type":"message_added","data":{"acp_thread_id":"a","message_id":"1","content":"hi"}}"#;
    let event: SyncEvent = serde_json::from_str(sparse).expect("a sparse event parses");
    match event {
        SyncEvent::MessageAdded {
            role,
            entry_type,
            timestamp,
            ..
        } => {
            assert!(
                role.is_empty(),
                "an absent role is empty, and the dispatch names the default"
            );
            assert!(entry_type.is_empty());
            assert_eq!(timestamp, 0);
        }
        other => panic!("not the variant the tag named: {other:?}"),
    }

    let wrong_type = r#"{"event_type":"message_added","data":{"acp_thread_id":"a","content":5}}"#;
    assert!(
        serde_json::from_str::<SyncEvent>(wrong_type).is_err(),
        "a field of the wrong type is refused"
    );

    let unknown = r#"{"event_type":"something_new","data":{}}"#;
    assert!(
        serde_json::from_str::<SyncEvent>(unknown).is_err(),
        "an event the contract does not name is refused, and the caller ignores it"
    );
}
