// Agent execution fabric.
//
// Actus weaves heterogeneous agent platforms behind one thin execution
// interface, the same way neXus weaves heterogeneous FIH storage types
// behind one knowledge fabric. A platform adapter (TelosBackend now, a
// LangGraph or Native adapter later) implements `AgentBackend`; the
// registry maps agent names to running adapters; HTTP handlers talk only
// to the trait.

use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::watch;

pub mod config;
pub mod ext_cli;
pub mod native;

/// Supported agent platform kinds. Adding a platform means adding a kind
/// and an `AgentBackend` adapter; the rest of actus is unchanged.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AgentKind {
    /// Telos over ACP/WebSocket. Default agent when a Telos binary and
    /// provider credentials are available.
    Telos,
    /// LangGraph Server over REST/SSE. Future adapter.
    LangGraph,
    /// In-process Rust agent loop. Reference adapter (deterministic,
    /// no LLM); proves the fabric seam and lets actus run without Telos.
    Native,
    /// Any external CLI binary as an auxiliary agent. Raw transport: per
    /// turn, actus spawns `bin <args...> <prompt>` and records the output.
    /// Ante is the first attached binary (`cli_args = ["-p"]`).
    #[serde(rename = "ext_cli")]
    ExtCli,
}

impl AgentKind {
    pub const ALL: [AgentKind; 4] = [
        AgentKind::Telos,
        AgentKind::LangGraph,
        AgentKind::Native,
        AgentKind::ExtCli,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            AgentKind::Telos => "telos",
            AgentKind::LangGraph => "langgraph",
            AgentKind::Native => "native",
            AgentKind::ExtCli => "ext_cli",
        }
    }

    /// Strict lookup of a kind by its wire name. Returns None for
    /// unknown names.
    pub fn parse(s: &str) -> Option<AgentKind> {
        AgentKind::ALL.iter().find(|k| k.as_str() == s).copied()
    }

    /// Declared capabilities for this kind. A full ACP agent (Telos) is
    /// sessionful; a raw-CLI agent is a one-shot, parallel act.
    pub fn capabilities(self) -> AgentCapabilities {
        match self {
            AgentKind::Telos => AgentCapabilities {
                sessionful: true,
                streaming: true,
                tools: true,
                approval: true,
                parallel: false,
                transport: "acp_ws",
            },
            AgentKind::LangGraph => AgentCapabilities {
                sessionful: true,
                streaming: true,
                tools: true,
                approval: false,
                parallel: false,
                transport: "rest_sse",
            },
            AgentKind::Native => AgentCapabilities {
                sessionful: true,
                streaming: false,
                tools: false,
                approval: false,
                parallel: false,
                transport: "inproc",
            },
            AgentKind::ExtCli => AgentCapabilities {
                sessionful: false,
                streaming: false,
                tools: false,
                approval: false,
                parallel: true,
                transport: "cli",
            },
        }
    }
}

/// Declared capabilities of one agent backend. Upper layers (the CLI, a
/// future kineTic orchestrator) read these to decide which agent fits a
/// situation instead of assuming every agent is a full session.
#[derive(Clone, Debug, Serialize)]
pub struct AgentCapabilities {
    /// Multi-turn conversation with resume (Telos, Native).
    pub sessionful: bool,
    /// Intermediate streaming events during a turn.
    pub streaming: bool,
    /// Tool calls exposed to the client.
    pub tools: bool,
    /// Tool approval surface.
    pub approval: bool,
    /// Concurrent turns on one instance (one-shot acts).
    pub parallel: bool,
    /// Transport used to drive the agent.
    pub transport: &'static str,
}

/// Runtime state snapshot of one agent backend.
#[derive(Clone, Debug, Serialize)]
pub struct AgentStatus {
    pub name: String,
    pub kind: AgentKind,
    pub connected: bool,
    pub ready: bool,
    pub capabilities: AgentCapabilities,
    /// Launch-time failure detail. None when the backend started or when
    /// the backend has no launch probe (sessionful adapters report
    /// readiness through their own connection state).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

/// A tool-call authorization awaiting a human decision (ask mode).
#[derive(Clone, Debug, Serialize)]
pub struct PendingAuthorization {
    /// Platform-side thread id the tool call belongs to.
    pub platform_thread_id: String,
    pub tool_call_id: String,
    pub tool_name: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// Truncate a first user message into a thread title.
pub(crate) fn truncate_title(message: &str) -> String {
    let flat = message.split_whitespace().collect::<Vec<_>>().join(" ");
    if flat.chars().count() <= 60 {
        flat
    } else {
        flat.chars().take(60).collect::<String>() + "..."
    }
}

/// Receipt returned by `AgentBackend::submit`.
#[derive(Clone, Debug)]
pub struct SubmitReceipt {
    /// Local thread id (created when `thread_id` was None).
    pub thread_id: String,
    /// Request id used to correlate events for this turn.
    pub request_id: String,
    /// True when this submit created a fresh thread.
    pub is_new: bool,
}

/// Who dispatched a turn into another agent: the controlling agent and
/// its thread. Populated when a meta agent submits through the control
/// surface with a parent thread id.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ThreadParent {
    pub agent: String,
    pub thread_id: String,
}

/// One conversation thread, shared across all platform adapters.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ThreadSession {
    pub id: String,
    /// Auto-generated from the first user message.
    pub title: Option<String>,
    pub messages: Vec<ThreadMessage>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// True when the last assistant response is complete.
    pub completed: bool,
    /// Platform-side thread id (ACP thread id for Telos). Kept on the
    /// session so persisted files stay backward compatible; a future
    /// platform adapter maps its own id into this field.
    pub acp_thread_id: Option<String>,
    /// Monotonically increasing turn counter. SSE consumers wait for
    /// `turn_completed` to exceed the value captured at submit time.
    pub turn_completed: u64,
    /// Dispatch origin of the first turn, when a meta agent submitted it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<ThreadParent>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ThreadMessage {
    pub role: String,
    pub content: String,
    pub message_id: Option<String>,
    pub entry_type: Option<String>,
    pub tool_name: Option<String>,
    pub tool_status: Option<String>,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// Uniform execution interface implemented by every platform adapter.
#[async_trait::async_trait]
pub trait AgentBackend: Send + Sync {
    fn name(&self) -> &str;

    fn kind(&self) -> AgentKind;

    /// Current connection and readiness state.
    async fn status(&self) -> AgentStatus;

    /// Submit a chat message, creating or resuming the given thread.
    /// Errors when the agent is not connected or not ready.
    async fn submit(&self, thread_id: Option<&str>, message: &str)
        -> Result<SubmitReceipt, String>;

    /// Submit with dispatch origin metadata (which meta agent and which
    /// of its threads started this turn). Backends that cannot record it
    /// fall back to the plain submit.
    async fn submit_with_options(
        &self,
        thread_id: Option<&str>,
        message: &str,
        _parent: Option<ThreadParent>,
    ) -> Result<SubmitReceipt, String> {
        self.submit(thread_id, message).await
    }

    /// Cancel the running turn, if any.
    async fn cancel(&self) -> Result<(), String>;

    /// Cancel one turn by its submit receipt request id. Backends without
    /// per-request tracking fall back to the agent-wide cancel.
    async fn cancel_request(&self, _request_id: &str) -> Result<(), String> {
        self.cancel().await
    }

    /// Snapshot of one thread, if it exists.
    async fn thread(&self, thread_id: &str) -> Option<ThreadSession>;

    /// Snapshot of all threads owned by this backend.
    async fn threads(&self) -> Vec<ThreadSession>;

    /// Watch channel that fires on every thread state change. Polling
    /// source for SSE consumers.
    async fn subscribe(&self) -> watch::Receiver<u64>;

    /// Tool-call authorizations currently awaiting a human decision.
    async fn pending_tool_calls(&self) -> Vec<PendingAuthorization>;

    /// Resolve a pending tool-call authorization (approve or reject).
    async fn resolve_tool_call(
        &self,
        platform_thread_id: &str,
        tool_call_id: &str,
        allow: bool,
    ) -> Result<(), String>;

    /// Create a fresh thread immediately (without sending a message).
    async fn create_thread(&self) -> Result<String, String>;
}

/// Registry of running agent backends. The default agent serves the
/// chat and thread endpoints until per-agent routing lands.
#[derive(Default)]
pub struct AgentRegistry {
    agents: HashMap<String, Arc<dyn AgentBackend>>,
    default_name: Option<String>,
}

impl AgentRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, backend: Arc<dyn AgentBackend>, default: bool) {
        if default {
            self.default_name = Some(backend.name().to_string());
        }
        self.agents.insert(backend.name().to_string(), backend);
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn AgentBackend>> {
        self.agents.get(name).cloned()
    }

    pub fn default_agent(&self) -> Option<Arc<dyn AgentBackend>> {
        self.default_name
            .as_ref()
            .and_then(|name| self.agents.get(name))
            .cloned()
    }

    pub async fn statuses(&self) -> Vec<AgentStatus> {
        let mut out: Vec<AgentStatus> = Vec::with_capacity(self.agents.len());
        for backend in self.agents.values() {
            out.push(backend.status().await);
        }
        out
    }
}
