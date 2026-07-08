#![allow(dead_code)]

// Protocol types for Zed headless communication.
//
// Ported from helixml/zed's external_websocket_sync crate (types.rs)
// with only the types actually needed by actus. The original crate
// contained HTTP server types, MCP config, thread summaries, etc.
// that are not relevant for a headless server that manages its own
// lifecycle and thread state.
//
// See helix/crates/external_websocket_sync/src/types.rs for reference.

use serde::{Deserialize, Serialize};

/// Outgoing WebSocket message from Zed to actus.
/// Matches the API's SyncMessage format: { event_type, data }.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OutgoingMessage {
    pub event_type: String,
    pub data: serde_json::Value,
}

/// Events that Zed sends to actus via WebSocket.
/// Per WEBSOCKET_PROTOCOL_SPEC — Zed is stateless and only knows
/// about acp_thread_id.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "event_type", content = "data")]
pub enum SyncEvent {
    /// Sent when Zed creates a new ACP thread in response to a chat_message.
    #[serde(rename = "thread_created")]
    ThreadCreated {
        acp_thread_id: String,
        request_id: String,
    },
    /// Sent when thread title changes in Zed.
    #[serde(rename = "thread_title_changed")]
    ThreadTitleChanged {
        acp_thread_id: String,
        title: String,
    },
    /// Sent while AI is streaming response content.
    /// `entry_type` distinguishes "text" (assistant prose) from
    /// "tool_call" (tool invocation).
    #[serde(rename = "message_added")]
    MessageAdded {
        acp_thread_id: String,
        message_id: String,
        role: String,
        content: String,
        #[serde(default)]
        request_id: String,
        #[serde(default)]
        entry_type: String,
        #[serde(default)]
        tool_name: String,
        #[serde(default)]
        tool_status: String,
        timestamp: i64,
    },
    /// Sent when AI finishes responding.
    #[serde(rename = "message_completed")]
    MessageCompleted {
        acp_thread_id: String,
        message_id: String,
        request_id: String,
    },
    /// Sent when a turn aborts (agent crash, max tokens, etc.).
    #[serde(rename = "chat_response_error")]
    ChatResponseError {
        request_id: String,
        error: String,
    },
    /// Sent when the agent has finished initialization and is ready.
    #[serde(rename = "agent_ready")]
    AgentReady {
        agent_name: String,
        thread_id: Option<String>,
    },
    /// Response to cancel_current_turn.
    #[serde(rename = "turn_cancelled")]
    TurnCancelled {
        request_id: String,
        status: String,
    },
}

impl SyncEvent {
    /// Convert to OutgoingMessage wire format.
    pub fn into_outgoing_message(self) -> OutgoingMessage {
        let (event_type, data) = match self {
            SyncEvent::ThreadCreated { acp_thread_id, request_id } => (
                "thread_created".to_string(),
                serde_json::json!({ "acp_thread_id": acp_thread_id, "request_id": request_id }),
            ),
            SyncEvent::ThreadTitleChanged { acp_thread_id, title } => (
                "thread_title_changed".to_string(),
                serde_json::json!({ "acp_thread_id": acp_thread_id, "title": title }),
            ),
            SyncEvent::MessageAdded { acp_thread_id, message_id, role, content, request_id, entry_type, tool_name, tool_status, timestamp } => (
                "message_added".to_string(),
                serde_json::json!({
                    "acp_thread_id": acp_thread_id,
                    "message_id": message_id,
                    "role": role,
                    "content": content,
                    "request_id": request_id,
                    "entry_type": entry_type,
                    "tool_name": tool_name,
                    "tool_status": tool_status,
                    "timestamp": timestamp,
                }),
            ),
            SyncEvent::MessageCompleted { acp_thread_id, message_id, request_id } => (
                "message_completed".to_string(),
                serde_json::json!({ "acp_thread_id": acp_thread_id, "message_id": message_id, "request_id": request_id }),
            ),
            SyncEvent::ChatResponseError { request_id, error } => (
                "chat_response_error".to_string(),
                serde_json::json!({ "request_id": request_id, "error": error }),
            ),
            SyncEvent::AgentReady { agent_name, thread_id } => (
                "agent_ready".to_string(),
                serde_json::json!({ "agent_name": agent_name, "thread_id": thread_id }),
            ),
            SyncEvent::TurnCancelled { request_id, status } => (
                "turn_cancelled".to_string(),
                serde_json::json!({ "request_id": request_id, "status": status }),
            ),
        };
        OutgoingMessage { event_type, data }
    }
}

/// Incoming command from actus to Zed.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IncomingChatMessage {
    /// None = create new thread, Some(id) = use existing.
    pub acp_thread_id: Option<String>,
    pub message: String,
    pub request_id: String,
    #[serde(default)]
    pub agent_name: Option<String>,
    /// If true, cancel the current running turn before sending.
    #[serde(default)]
    pub interrupt: bool,
}
