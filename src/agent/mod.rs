// Agent execution fabric.
//
// Actus weaves heterogeneous agent platforms behind one thin execution
// interface, the same way neXus weaves heterogeneous FIH storage types
// behind one knowledge fabric. A platform adapter (AcpWsBackend now, a
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
    /// An agent that speaks the agent-client protocol over a WebSocket. The default
    /// kind. What runs on the other end is a deployment choice; the contract is the
    /// protocol, and the executor implementing it is named by the configuration.
    #[serde(rename = "acp_ws", alias = "telos")]
    AcpWs,
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
        AgentKind::AcpWs,
        AgentKind::LangGraph,
        AgentKind::Native,
        AgentKind::ExtCli,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            AgentKind::AcpWs => "acp_ws",
            AgentKind::LangGraph => "langgraph",
            AgentKind::Native => "native",
            AgentKind::ExtCli => "ext_cli",
        }
    }

    /// Lookup of a kind by its wire name. `telos` is the name this kind carried before
    /// it was named for what it is, and stays accepted so a configuration written
    /// against the old name keeps parsing.
    pub fn parse(s: &str) -> Option<AgentKind> {
        if s == "telos" {
            return Some(AgentKind::AcpWs);
        }
        AgentKind::ALL.iter().find(|k| k.as_str() == s).copied()
    }

    /// Declared capabilities for this kind. A full ACP agent (Telos) is
    /// sessionful; a raw-CLI agent is a one-shot, parallel act.
    pub fn capabilities(self) -> AgentCapabilities {
        match self {
            AgentKind::AcpWs => AgentCapabilities {
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
    /// A counter that moves whenever this agent's threads change. A viewer compares
    /// it to learn that something arrived without reading the thread, which is how an
    /// open conversation stays live while another participant writes to it.
    pub thread_version: u64,
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
    /// When this thread last did anything, as its backend reckons it. Absent
    /// when the backend keeps no such time, which leaves a caller to fall
    /// back to the last message or to `created_at`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
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

/// The role a note carries: something a person said in a thread that is not addressed to the
/// agent. No turn runs for it, and no turn's context is built from it, because
/// `format_conversation_context` takes a `user` message and an `assistant` message whose entry
/// type is `text`. The string lives here so the adapters that record one and the filter that keeps
/// it out of the model cannot disagree about it.
pub const NOTE_ROLE: &str = "note";

/// One message in a thread's record.
///
/// `author` is who wrote it, as the client that recorded it named them. A message the agent
/// wrote has none, because the agent is the thread's own voice. A shared thread holds what
/// several people said, so the name is what tells them apart in the record, and actus carries
/// it without reading it: which id a client speaks as is the client's to decide, the same way
/// the prefix a person types is.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ThreadMessage {
    pub role: String,
    pub content: String,
    pub message_id: Option<String>,
    pub entry_type: Option<String>,
    pub tool_name: Option<String>,
    pub tool_status: Option<String>,
    #[serde(default)]
    pub author: Option<String>,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

impl ThreadSession {
    /// The time of the last thing written in the thread, which is what a listing
    /// sorts on when the backend reports no activity time of its own. `None` for
    /// a thread nobody has written in.
    pub fn last_message_at(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        self.messages.iter().map(|message| message.timestamp).max()
    }
}

/// Uniform execution interface implemented by every platform adapter.
#[async_trait::async_trait]
pub trait AgentBackend: Send + Sync {
    fn name(&self) -> &str;

    fn kind(&self) -> AgentKind;

    /// What this agent works in, in the backend's own terms: a workspace
    /// directory for an agent that edits files, a queue or a job for an
    /// execution controller. Opaque to actus, which carries it to clients
    /// without reading it. `None` for a backend with no such notion, which
    /// leaves a client to group by agent name instead.
    fn scope(&self) -> Option<String> {
        None
    }

    /// Which store holds this agent's record, for a client that shows it.
    ///
    /// A backend that keeps its threads in its own memory has no store to name, and
    /// saying so is what lets a client tell a stack serving a volume from one holding
    /// the conversation for as long as the process lives.
    async fn record_store(&self) -> String {
        "in memory".to_string()
    }

    /// Current connection and readiness state.
    async fn status(&self) -> AgentStatus;

    /// Submit a chat message, creating or resuming the given thread.
    /// Errors when the agent is not connected or not ready.
    async fn submit(&self, thread_id: Option<&str>, message: &str)
        -> Result<SubmitReceipt, String>;

    /// Submit with dispatch origin metadata (which meta agent and which
    /// of its threads started this turn), the reasoning effort the turn
    /// runs at, and the author of the message. Backends that cannot record
    /// or carry any of them fall back to the plain submit.
    ///
    /// `author` is who the client says is speaking. A shared thread holds several people, so
    /// the name is what the record keeps of who said what. actus does not resolve it: the client
    /// the request came from is what knows, and a client that names nobody records no author.
    async fn submit_with_options(
        &self,
        thread_id: Option<&str>,
        message: &str,
        _parent: Option<ThreadParent>,
        _thinking_effort: Option<&str>,
        _author: Option<&str>,
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

    /// How many messages a thread holds, from the backend's own record.
    ///
    /// The default counts the snapshot [`thread`](Self::thread) returns, which is what a
    /// backend holding its threads in memory can do. A backend over a store answers from the
    /// store's index, which is what lets a listing count without holding the record.
    async fn message_count(&self, thread_id: &str) -> usize {
        self.thread(thread_id)
            .await
            .map(|thread| thread.messages.len())
            .unwrap_or(0)
    }

    /// A window of a thread's messages, by position.
    ///
    /// The default takes the window from the snapshot [`thread`](Self::thread) returns,
    /// which is what a backend holding its threads in memory can do. A backend over a store
    /// reads the window alone, so a view costs the window and not the conversation.
    async fn messages_window(
        &self,
        thread_id: &str,
        from: usize,
        limit: usize,
    ) -> Vec<ThreadMessage> {
        let Some(thread) = self.thread(thread_id).await else {
            return Vec::new();
        };
        let end = from.saturating_add(limit).min(thread.messages.len());
        if from >= end {
            return Vec::new();
        }
        thread.messages[from..end].to_vec()
    }

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

    /// Append a note to a thread's record and return the thread it landed in, creating the
    /// thread when the caller names none.
    ///
    /// A note is something a person said in the thread that is not addressed to the agent. It is
    /// kept like any other message, in order, so a reader sees it where it was said, and the role
    /// it carries ([`NOTE_ROLE`]) keeps it out of every turn's context. The agent is not involved,
    /// so unlike [`submit`](Self::submit) neither the backend's connection nor its readiness is a
    /// precondition, and no turn is pending afterwards. A note opens a thread, and names it,
    /// because the first thing said in a thread is what the thread is called.
    ///
    /// A backend that has no record to append to reports that it does not record notes.
    ///
    /// `author` names who said the note, the same way
    /// [`submit_with_options`](Self::submit_with_options) names who said a message.
    async fn append_note(
        &self,
        _thread_id: Option<&str>,
        _content: &str,
        _author: Option<&str>,
    ) -> Result<String, String> {
        Err("this backend does not record notes".to_string())
    }
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

    /// Every agent this host runs, by name in sorted order, so the same listing
    /// comes back from one call to the next.
    pub fn agents(&self) -> Vec<(String, Arc<dyn AgentBackend>)> {
        let mut names: Vec<&String> = self.agents.keys().collect();
        names.sort();
        names
            .into_iter()
            .map(|name| (name.clone(), self.agents[name].clone()))
            .collect()
    }

    pub async fn statuses(&self) -> Vec<AgentStatus> {
        let mut out: Vec<AgentStatus> = Vec::with_capacity(self.agents.len());
        for backend in self.agents.values() {
            out.push(backend.status().await);
        }
        out
    }
}
