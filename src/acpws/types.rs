#![allow(dead_code)]

// The agent sync contract: what an executor sends over the WebSocket, and what actus
// sends back.
//
// This is the actus side of a contract whose fuller form is the executor's own wire
// protocol. Only the events and commands actus depends on are here, and every field is
// tolerated as absent: a peer that omits one is speaking the contract with less detail
// rather than a different one. A field of a wrong type is not tolerated, which is the
// point of writing the contract down.
//
// actus is a peer, not the authority. An event this file does not name is ignored rather
// than refused, so a newer executor can send one before this end learns it.

use serde::{Deserialize, Serialize};

/// Outgoing WebSocket message from Telos to actus.
/// Matches the API's SyncMessage format: { event_type, data }.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OutgoingMessage {
    pub event_type: String,
    pub data: serde_json::Value,
}

/// Events that Telos sends to actus via WebSocket.
/// Per WEBSOCKET_PROTOCOL_SPEC — Telos is stateless and only knows
/// about acp_thread_id.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "event_type", content = "data")]
pub enum SyncEvent {
    /// Sent when Telos creates a new ACP thread in response to a chat_message.
    #[serde(rename = "thread_created")]
    ThreadCreated {
        #[serde(default)]
        acp_thread_id: String,
        #[serde(default)]
        request_id: String,
    },
    /// Sent when thread title changes in Telos.
    #[serde(rename = "thread_title_changed")]
    ThreadTitleChanged {
        #[serde(default)]
        acp_thread_id: String,
        #[serde(default)]
        title: String,
    },
    /// Sent while AI is streaming response content.
    /// `entry_type` distinguishes "text" (assistant prose) from
    /// "tool_call" (tool invocation).
    #[serde(rename = "message_added")]
    MessageAdded {
        #[serde(default)]
        acp_thread_id: String,
        #[serde(default)]
        message_id: String,
        #[serde(default)]
        role: String,
        #[serde(default)]
        content: String,
        #[serde(default)]
        request_id: String,
        #[serde(default)]
        entry_type: String,
        #[serde(default)]
        tool_name: String,
        #[serde(default)]
        tool_status: String,
        #[serde(default)]
        timestamp: i64,
    },
    /// Sent when AI finishes responding.
    #[serde(rename = "message_completed")]
    MessageCompleted {
        #[serde(default)]
        acp_thread_id: String,
        #[serde(default)]
        message_id: String,
        #[serde(default)]
        request_id: String,
    },
    /// Sent when a turn aborts (agent crash, max tokens, etc.).
    #[serde(rename = "chat_response_error")]
    ChatResponseError {
        #[serde(default)]
        request_id: String,
        #[serde(default)]
        error: String,
    },
    /// Sent when the agent has finished initialization and is ready.
    #[serde(rename = "agent_ready")]
    AgentReady {
        #[serde(default)]
        agent_name: String,
        #[serde(default)]
        thread_id: Option<String>,
    },
    /// Response to cancel_current_turn.
    #[serde(rename = "turn_cancelled")]
    TurnCancelled {
        #[serde(default)]
        request_id: String,
        #[serde(default)]
        status: String,
    },
    /// Sent when the agent requests permission for a tool call (ask mode).
    #[serde(rename = "tool_call_authorization_requested")]
    ToolCallAuthorizationRequested {
        #[serde(default)]
        acp_thread_id: String,
        #[serde(default)]
        tool_call_id: String,
        #[serde(default)]
        tool_name: String,
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
            SyncEvent::ToolCallAuthorizationRequested {
                acp_thread_id,
                tool_call_id,
                tool_name,
            } => (
                "tool_call_authorization_requested".to_string(),
                serde_json::json!({
                    "acp_thread_id": acp_thread_id,
                    "tool_call_id": tool_call_id,
                    "tool_name": tool_name,
                }),
            ),
        };
        OutgoingMessage { event_type, data }
    }
}

/// Incoming command from actus to Telos.
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
