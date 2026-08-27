// Agent execution fabric.
//
// Actus weaves heterogeneous agent platforms behind one thin execution
// interface, the same way neXus weaves heterogeneous FIH storage types
// behind one knowledge fabric. A platform adapter (ZedBackend now, a
// LangGraph or Native adapter later) implements `AgentBackend`; the
// registry maps agent names to running adapters; HTTP handlers talk only
// to the trait.

use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::watch;

pub mod config;

/// Supported agent platform kinds. Adding a platform means adding a kind
/// and an `AgentBackend` adapter; the rest of actus is unchanged.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AgentKind {
    /// Zed headless over ACP/WebSocket (helix fork). Default agent.
    Zed,
    /// LangGraph Server over REST/SSE. Future adapter.
    LangGraph,
    /// In-process Rust agent loop. Future adapter.
    Native,
}

impl AgentKind {
    pub const ALL: [AgentKind; 3] = [AgentKind::Zed, AgentKind::LangGraph, AgentKind::Native];

    pub fn as_str(self) -> &'static str {
        match self {
            AgentKind::Zed => "zed",
            AgentKind::LangGraph => "langgraph",
            AgentKind::Native => "native",
        }
    }

    /// Strict lookup of a kind by its wire name. Returns None for
    /// unknown names.
    pub fn parse(s: &str) -> Option<AgentKind> {
        AgentKind::ALL.iter().find(|k| k.as_str() == s).copied()
    }
}

/// Runtime state snapshot of one agent backend.
#[derive(Clone, Debug, Serialize)]
pub struct AgentStatus {
    pub name: String,
    pub kind: AgentKind,
    pub connected: bool,
    pub ready: bool,
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
    /// Platform-side thread id (ACP thread id for Zed). Kept on the
    /// session so persisted files stay backward compatible; a future
    /// platform adapter maps its own id into this field.
    pub acp_thread_id: Option<String>,
    /// Monotonically increasing turn counter. SSE consumers wait for
    /// `turn_completed` to exceed the value captured at submit time.
    pub turn_completed: u64,
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
    async fn submit(
        &self,
        thread_id: Option<&str>,
        message: &str,
    ) -> Result<SubmitReceipt, String>;

    /// Cancel the running turn, if any.
    async fn cancel(&self) -> Result<(), String>;

    /// Snapshot of one thread, if it exists.
    async fn thread(&self, thread_id: &str) -> Option<ThreadSession>;

    /// Snapshot of all threads owned by this backend.
    async fn threads(&self) -> Vec<ThreadSession>;

    /// Watch channel that fires on every thread state change. Polling
    /// source for SSE consumers.
    async fn subscribe(&self) -> watch::Receiver<u64>;
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
