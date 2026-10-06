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

/// Outgoing WebSocket message from the executor to actus.
/// Matches the API's SyncMessage format: { event_type, data }.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OutgoingMessage {
    pub event_type: String,
    pub data: serde_json::Value,
}

/// Events the executor sends to actus over the WebSocket.
/// Per WEBSOCKET_PROTOCOL_SPEC — the executor is stateless and only knows
/// about acp_thread_id.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "event_type", content = "data")]
pub enum SyncEvent {
    /// Sent when the executor creates a new ACP thread in response to a chat_message.
    #[serde(rename = "thread_created")]
    ThreadCreated {
        #[serde(default)]
        acp_thread_id: String,
        #[serde(default)]
        request_id: String,
    },
    /// Sent when the executor changes a thread title.
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

/// A command actus sends to the executor.
///
/// The tag is `type` and the body is `data`, which is the shape the socket carries. A field
/// is present when it is meaningful rather than always, and a null is a value rather than an
/// absence: `thinking_effort: null` tells the executor to leave the thread's own level alone.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum Command {
    /// Ask for a turn. A thread id names an existing conversation; none opens a new one.
    #[serde(rename = "chat_message")]
    ChatMessage {
        #[serde(default)]
        acp_thread_id: Option<String>,
        message: String,
        request_id: String,
        #[serde(default)]
        thinking_effort: Option<String>,
    },
    /// Stop the turn the request id names. Without one, the command names no turn.
    #[serde(rename = "cancel_current_turn")]
    CancelCurrentTurn {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        request_id: Option<String>,
    },
    /// Answer a tool call the executor is blocked on.
    #[serde(rename = "resolve_tool_call_authorization")]
    ResolveToolCallAuthorization {
        acp_thread_id: String,
        allow: bool,
        tool_call_id: String,
    },
}

impl Command {
    /// The command as the string the socket carries.
    pub fn to_json(&self) -> Result<String, String> {
        serde_json::to_string(self).map_err(|error| format!("serialize a command: {error}"))
    }
}
