// Agent management — process lifecycle, the WebSocket bridge, and protocol types.

pub mod backend;
pub mod control;
pub mod types;

use crate::agent::config::{AgentSpec, ToolApproval};
use crate::agent::{PendingAuthorization, ThreadMessage, ThreadSession};
use crate::store::Record;
use std::sync::atomic::{AtomicBool, Ordering};

/// Channel sender for WebSocket commands to the executor. Shared between
/// `AppState` and `AcpwsManager` so cancel can send without acquiring the
/// `AcpwsManager` RwLock (avoiding lock contention with long-running SSE
/// handlers).
pub type WsCommandTx = Arc<tokio::sync::Mutex<Option<mpsc::UnboundedSender<String>>>>;

// ACP-over-WebSocket manager: connection, session management, and settings bootstrap.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json;
use tokio::sync::{mpsc, watch, Notify, RwLock};
use uuid::Uuid;

use crate::acpws::types::Command;
use crate::util::truncate_utf8;

/// Manages a single executor WebSocket connection and message dispatch.
#[allow(dead_code)]
pub struct AcpwsManager {
    pub session_id: String,
    pub ws_host: String,
    pub agent_connected: bool,
    pub agent_ready: bool,
    /// Channel to send WebSocket commands to the executor
    pub ws_tx: Option<mpsc::UnboundedSender<String>>,
    /// Threads managed by this executor session, as their metadata and, for a thread this
    /// session has touched, its messages. A store that does not need the whole state is
    /// asked for the index, and a thread's messages are read from it when the thread is
    /// first worked on, so what a host holds is the threads in use rather than the volume.
    pub threads: HashMap<String, ThreadSession>,
    /// The threads of `threads` whose messages are in hand, which is what `hold` fills and
    /// `window` and `message_count` read. A store that needs the whole state loads every
    /// thread, so the set is every id and the behavior is the one this manager always had.
    pub held: HashSet<String>,
    /// Mapping from request_id to acp_thread_id (for correlating responses)
    pub pending_requests: HashMap<String, String>,
    /// Mapping from the platform thread id to the local thread id (for reverse lookup)
    pub thread_id_map: HashMap<String, String>,
    /// Where this agent's thread record lives. The rule for reading and writing it is the
    /// same for every backend that keeps one, so it is held once and borrowed here.
    pub record: Record,
    /// Threads that have been activated (context sent) in the current executor session
    pub threads_activated: HashSet<String>,
    /// Notifier for thread state changes (SSE consumers)
    pub thread_notify: watch::Sender<u64>,
    /// Waiters for threads whose acp_thread_id is being established
    pub thread_waiters: HashMap<String, Arc<Notify>>,
    /// Monotonically increasing reconnect counter. Incremented each time
    /// a new WS connection is established (for SSE consumers to detect).
    pub reconnect_count: u64,
    /// Timestamp of the last PING received from the executor (for keepalive).
    pub last_ping_time: Instant,
    /// Timestamp of the last meaningful SSE event (message_added,
    /// message_completed, thread_created, agent_ready).
    /// Used to detect stuck connections where SSE events stop arriving.
    pub last_sse_event_time: Instant,
    /// Pending chat messages that need to be re-sent after reconnection.
    /// Stores (request_id, thread_id, message) tuples.
    pub pending_chat_queue: Vec<(String, String, String)>,
    /// Dirty flag for the debounced background thread-saver.
    pub threads_dirty: AtomicBool,
    /// Tool-call authorizations awaiting a human decision, keyed by
    /// tool_call_id (ask mode).
    pub pending_authorizations: HashMap<String, PendingAuthorization>,
    /// Snapshot of (scoped message id → content) from the most recent
    /// completed turn of each thread. The executor.s flush_streaming_throttle
    /// resends ALL ACP thread entries on turn completion and replays prior
    /// turn entries on follow-up turns; a resend whose id and content both
    /// match this snapshot is a replay and is dropped instead of being
    /// treated as new content.
    pub prior_message_content: HashMap<String, String>,
    /// Consumed request ids (empty-string values in `pending_requests`)
    /// kept for duplicate detection. Bounded so the map cannot grow without
    /// limit on a long-running process.
    pub sentinel_cap: usize,
}

impl AcpwsManager {
    /// Open the manager over the store this deployment's environment selects.
    pub fn new(session_id: String, ws_host: String, threads_dir: &Path) -> Result<Self, String> {
        Self::with_store(session_id, ws_host, crate::store::open(threads_dir)?)
    }

    /// The same, with the store the caller supplies.
    ///
    /// The composition root calls this. A build that links an engine of its own passes a
    /// store that reaches that engine, so one process serves and no socket or second
    /// supervisor sits between the record and the server.
    pub fn with_store(
        session_id: String,
        ws_host: String,
        store: Arc<dyn crate::store::RecordStore>,
    ) -> Result<Self, String> {
        // A store that records one message at a time does not need every thread's messages
        // to persist, so the index is what is loaded and the messages of a thread are read
        // when the thread is worked on. A store that writes one document needs the whole
        // state, and it is loaded whole, which is the behavior this manager always had.
        let record = Record::new(store);
        let (threads, held) = record.load()?;

        // Rebuild thread_id_map from persisted threads that have an acp_thread_id
        let mut thread_id_map = HashMap::new();
        for (local_id, thread) in &threads {
            if let Some(acp_id) = &thread.acp_thread_id {
                thread_id_map.insert(acp_id.clone(), local_id.clone());
            }
        }

        let (thread_notify, _) = watch::channel(0u64);

        Ok(Self {
            session_id,
            ws_host,
            agent_connected: false,
            agent_ready: false,
            ws_tx: None,
            threads,
            held,
            pending_requests: HashMap::new(),
            thread_id_map,
            record,
            threads_activated: HashSet::new(),
            thread_notify,
            thread_waiters: HashMap::new(),
            reconnect_count: 0,
            last_ping_time: Instant::now(),
            last_sse_event_time: Instant::now(),
            pending_chat_queue: Vec::new(),
            pending_authorizations: HashMap::new(),
            prior_message_content: HashMap::new(),
            sentinel_cap: 512,
            threads_dirty: AtomicBool::new(false),
        })
    }

    /// Read a thread's messages into hand, if they are not already there.
    ///
    /// A thread the index holds is metadata only, so anything that reads a thread's messages
    /// asks for them first. That is what keeps a listing and a health check from paying for a
    /// volume they do not read, and what makes the first turn of a resumed thread the moment
    /// its history is read.
    fn hold(&mut self, thread_id: &str) -> Result<(), String> {
        self.record.hold(&mut self.threads, &mut self.held, thread_id)
    }

    /// How many messages a thread holds.
    ///
    /// From memory when the thread is in hand and from the store otherwise, which is what a
    /// listing counts without reading the volume behind it.
    pub fn message_count(&self, thread_id: &str) -> Result<usize, String> {
        self.record
            .message_count(&self.threads, &self.held, thread_id)
    }

    /// A window of a thread's messages, by position.
    ///
    /// From memory when the thread is in hand and from the store otherwise, so a view opens
    /// on a conversation without the whole of it being read into this process.
    pub fn window(
        &self,
        thread_id: &str,
        from: usize,
        limit: usize,
    ) -> Result<Vec<ThreadMessage>, String> {
        self.record
            .window(&self.threads, &self.held, thread_id, from, limit)
    }

    /// The last `limit` messages of a thread, which is what a live turn's reader wants.
    pub fn tail(&self, thread_id: &str, limit: usize) -> Result<Vec<ThreadMessage>, String> {
        let total = self.message_count(thread_id)?;
        self.window(thread_id, total.saturating_sub(limit), limit)
    }

    /// Prepare the user message, injecting conversation context if this
    /// thread has not been activated in the current executor session yet.
    pub fn prepare_message(&mut self, thread_id: &str, user_message: &str) -> String {
        if !self.threads_activated.contains(thread_id) {
            // Clear stale acp_thread_id from previous sessions
            if let Some(thread) = self.threads.get_mut(thread_id) {
                thread.acp_thread_id = None;
            }
            self.thread_id_map.retain(|_, v| v != thread_id);
            // The context is the whole conversation, so the thread's history is read here.
            // A store that cannot answer it is named rather than silently dropping the
            // context: a turn without it reads to the model as a new conversation.
            if let Err(e) = self.hold(thread_id) {
                tracing::warn!("reading the history of {thread_id} failed: {e}");
            }
            if let Some(ctx) = self.format_conversation_context(thread_id) {
                return format!("{}\n\n{}", ctx, user_message);
            }
        }
        user_message.to_string()
    }

    pub fn set_ws_tx(&mut self, tx: mpsc::UnboundedSender<String>) {
        self.ws_tx = Some(tx);
    }

    pub fn get_or_create_thread(&mut self, thread_id: Option<&str>) -> String {
        let id = thread_id
            .filter(|t| !t.is_empty())
            .map(|t| t.to_string())
            .unwrap_or_else(|| Uuid::new_v4().to_string());

        if !self.threads.contains_key(&id) {
            self.threads.insert(
                id.clone(),
                ThreadSession {
                    id: id.clone(),
                    title: None,
                    messages: vec![],
                    created_at: chrono::Utc::now(),
                    updated_at: None,
                    completed: false,
                    acp_thread_id: None,
                    turn_completed: 0,
                    parent: None,
                },
            );
            // A thread made here has its messages in hand by construction, so it is not one
            // the index describes: without this, the first turn on it would read the thread
            // back from the store and drop what was written in between.
            self.held.insert(id.clone());
            self.notify_thread_change();
            self.save_threads();
        }
        id
    }

    /// Look up the ACP thread ID for a given local thread ID.
    /// ACP threads are created by the executor and stored in thread_id_map.
    /// Returns None for new threads (the executor will create a fresh ACP thread).
    pub fn get_acp_thread_id(&self, local_id: &str) -> Option<String> {
        for (acp_id, lid) in &self.thread_id_map {
            if lid == local_id {
                return Some(acp_id.clone());
            }
        }
        None
    }

    /// Format the conversation history as a context string for context injection.
    /// Returns None if there are no previous user/assistant messages.
    pub fn format_conversation_context(&self, thread_id: &str) -> Option<String> {
        let thread = self.threads.get(thread_id)?;
        let history: Vec<&str> = thread
            .messages
            .iter()
            .filter(|m| {
                m.role == "user"
                    || (m.role == "assistant" && m.entry_type.as_deref() == Some("text"))
            })
            .map(|m| m.content.as_str())
            .collect();
        if history.is_empty() || history.len() <= 1 {
            return None; // No previous context or just the current message
        }
        // Take all but the last message (that's the current one being sent)
        let past = &history[..history.len() - 1];
        let mut ctx = String::from("[Previous conversation]\n");
        for msg in past {
            // Truncate very long messages to avoid token waste; the cut
            // must land on a UTF-8 char boundary or the slice panics.
            ctx.push_str(truncate_utf8(msg, 2000));
            ctx.push_str("\n\n");
        }
        ctx.push_str("[Continue from above]\n");
        Some(ctx)
    }

    pub fn add_message(
        &mut self,
        thread_id: &str,
        role: &str,
        content: &str,
        message_id: Option<String>,
    ) {
        self.add_message_full(thread_id, role, content, message_id, None, None, None, None)
    }

    /// Record the (scoped message id → content) snapshot of the most
    /// recent assistant messages in a thread. Called when a turn ends so a
    /// follow-up turn's replay of these entries can be recognized and
    /// dropped. Only the trailing assistant block is snapshotted; the
    /// trailing block ends at the last user message, which is the turn
    /// boundary.
    pub fn record_prior_entries(&mut self, thread_id: &str) {
        if let Err(e) = self.hold(thread_id) {
            tracing::warn!("reading the history of {thread_id} failed: {e}");
        }
        let Some(thread) = self.threads.get(thread_id) else {
            return;
        };
        for m in thread
            .messages
            .iter()
            .rev()
            .take_while(|m| m.role == "assistant")
        {
            if let Some(id) = &m.message_id {
                self.prior_message_content
                    .insert(id.clone(), m.content.clone());
            }
        }
    }

    /// Consume a request mapping by replacing the thread id with an empty
    /// sentinel, keeping it for duplicate detection. Old sentinels are
    /// pruned once the cap is reached so the map stays bounded; the most
    /// recent sentinel is always kept because a duplicate event for the
    /// current turn is the one most likely to arrive.
    pub fn consume_request(&mut self, request_id: &str) {
        if request_id.is_empty() {
            return;
        }
        self.pending_requests
            .insert(request_id.to_string(), String::new());
        if self.pending_requests.len() > self.sentinel_cap {
            // Drop enough consumed entries (empty values) to get back under
            // the cap. The entry just inserted is always kept; active
            // (non-empty) entries are never pruned.
            let keep = request_id.to_string();
            let excess = self.pending_requests.len() - self.sentinel_cap;
            let mut stale: Vec<String> = self
                .pending_requests
                .iter()
                .filter(|(k, v)| v.is_empty() && **k != keep)
                .map(|(k, _)| k.clone())
                .take(excess)
                .collect();
            for k in stale.drain(..) {
                self.pending_requests.remove(&k);
            }
        }
    }

    /// Append a message to a thread, replacing an existing message with the
    /// same id when present (streaming updates in place). The metadata trio
    /// mirrors the fields of the wire `message_added` event, so the
    /// signature is intentionally wide.
    ///
    /// Matching is by id anywhere in the thread, not just the last message:
    /// The executor emits updates for several interleaved messages in one turn
    /// (thinking, tool call, then the answer), so the same id reappears
    /// non-consecutively. Comparing only against the tail used to append a
    /// duplicate every time, swelling a single turn into dozens of
    /// messages and breaking poll/SSE turn indexing.
    ///
    /// A brand-new id whose (id, content) pair exactly matches the prior
    /// turn's snapshot is a replay of a previous turn's entry and is
    /// dropped; the wrapper replays entries when a new turn starts.
    #[allow(clippy::too_many_arguments)]
    pub fn add_message_full(
        &mut self,
        thread_id: &str,
        role: &str,
        content: &str,
        message_id: Option<String>,
        entry_type: Option<String>,
        tool_name: Option<String>,
        tool_status: Option<String>,
        author: Option<&str>,
    ) {
        // What a message is compared against is the thread, so a thread the index holds is
        // read before the compare: without it an id already in the record would be appended
        // as a second copy, which is the swelling this compare exists to prevent.
        if let Err(e) = self.hold(thread_id) {
            tracing::error!("reading the history of {thread_id} failed: {e}");
        }
        if let Some(thread) = self.threads.get_mut(thread_id) {
            if let Some(ref mid) = message_id {
                // Same-turn streaming update: the id exists in the thread
                // and was not part of the prior turn's snapshot, so this is
                // cumulative content for the current message; replace it.
                let is_prior = self.prior_message_content.contains_key(mid);
                if !is_prior {
                    if let Some(existing) = thread
                        .messages
                        .iter_mut()
                        .find(|m| m.message_id.as_deref() == Some(mid))
                    {
                        existing.content = content.to_string();
                        existing.entry_type = entry_type;
                        existing.tool_name = tool_name;
                        existing.tool_status = tool_status;
                        thread.updated_at = Some(chrono::Utc::now());
                        self.notify_thread_change();
                        self.save_threads();
                        return;
                    }
                } else if self
                    .prior_message_content
                    .get(mid)
                    .map(|prior| prior == content)
                    .unwrap_or(false)
                {
                    // The id belongs to the prior turn and the content is
                    // unchanged: a wrapper replay of that turn's entry.
                    // Drop it rather than duplicating it.
                    tracing::debug!(
                        "Dropping replay of prior turn entry id {} (content unchanged)",
                        mid
                    );
                    return;
                }
                // The id belongs to the prior turn but the content differs:
                // the wrapper restarted and renumbered, or the new turn
                // legitimately reuses the id. Append as new content; do not
                // overwrite the prior turn's entry.
            }

            thread.messages.push(ThreadMessage {
                role: role.to_string(),
                content: content.to_string(),
                message_id,
                entry_type,
                tool_name,
                tool_status,
                author: author.map(str::to_string),
                timestamp: chrono::Utc::now(),
            });
            // A listing orders by this and never reads the messages, so it is carried in the
            // thread's metadata rather than derived from the record on every listing.
            thread.updated_at = Some(chrono::Utc::now());
        }
        self.notify_thread_change();
        self.save_threads();
    }

    /// Set the thread title from the raw user message (before context injection).
    pub fn set_title(&mut self, thread_id: &str, title: &str) {
        if let Some(thread) = self.threads.get_mut(thread_id) {
            if thread.title.is_none() {
                let mut truncated = truncate_utf8(title, 80).to_string();
                if title.len() > 80 {
                    truncated.push_str("...");
                }
                thread.title = Some(truncated);
                self.notify_thread_change();
                self.save_threads();
            }
        }
    }

    /// Notify SSE consumers that thread state has changed.
    ///
    /// `send_modify` takes the watch channel's internal write lock once
    /// and increments in place. The previous borrow-then-send pattern
    /// held the read lock while requesting the write lock, which
    /// deadlocked on the non-reentrant RwLock inside `watch`.
    pub fn notify_thread_change(&self) {
        self.thread_notify.send_modify(|v| *v = v.wrapping_add(1));
    }

    /// Send a cancel_current_turn command to the executor over the WebSocket.
    pub fn cancel_current_turn(&self) -> Result<(), String> {
        let cmd = Command::CancelCurrentTurn { request_id: None }.to_json()?;
        self.send_command(&cmd)
    }

    /// Persist all threads to the JSON file.
    pub fn save_threads(&self) {
        // Debounced: mark dirty and let the background saver persist. The
        // previous implementation wrote the full file synchronously while
        // the caller held the manager write lock; on a slow or full disk
        // that blocked every other manager user and froze the server.
        self.threads_dirty.store(true, Ordering::SeqCst);
    }

    /// Write the full threads file synchronously. Used at shutdown, where
    /// the process is about to exit and the debounced saver may not run.
    pub fn flush_threads(&self) {
        if let Err(e) = self.record.persist(self.threads.clone()) {
            tracing::error!("Failed to persist threads: {}", e);
        }
    }

    /// Background persistence loop: every second, if the dirty flag is set,
    /// snapshot the threads under a read lock, release it, and write the
    /// file on a blocking thread. Keeps the heavy JSON serialization and
    /// disk write off the manager lock and off the async runtime.
    pub fn spawn_thread_saver(manager: Arc<RwLock<AcpwsManager>>) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let dirty = manager
                    .read()
                    .await
                    .threads_dirty
                    .swap(false, Ordering::SeqCst);
                if !dirty {
                    continue;
                }
                let (snapshot, record) = {
                    let mgr = manager.read().await;
                    (mgr.threads.clone(), mgr.record.clone())
                };
                tokio::task::spawn_blocking(move || {
                    if let Err(e) = record.persist(snapshot) {
                        tracing::error!("Failed to persist threads: {}", e);
                    }
                })
                .await
                .ok();
            }
        });
    }

    /// Send a JSON command to the executor over the WebSocket. Returns error if not connected.
    pub fn send_command(&self, cmd: &str) -> Result<(), String> {
        match &self.ws_tx {
            Some(tx) => tx.send(cmd.to_string()).map_err(|e| e.to_string()),
            None => Err("WebSocket not connected".to_string()),
        }
    }
}

/// One marker a declared launch may carry, with the value actus supplies for it.
///
/// The list and the resolver are one table, so the names an error reports cannot drift
/// from the names the resolver accepts.
type LaunchMarker = (&'static str, fn(&Launch<'_>) -> String);

/// The markers a declared launch may carry. Each names a value actus owns and a
/// deployment cannot know: a path it derived, a port it bound, a token it generated at
/// startup.
static LAUNCH_MARKERS: [LaunchMarker; 8] = [
    ("workdir", |launch| launch.workdir.display().to_string()),
    ("user_data_dir", |launch| {
        launch.user_data_dir.display().to_string()
    }),
    ("session_id", |launch| launch.session_id.to_string()),
    ("ws_url", |launch| launch.ws_url.to_string()),
    ("token", |launch| launch.token.to_string()),
    ("agent_name", |launch| launch.agent_name.to_string()),
    ("http_port", |launch| launch.http_port.to_string()),
    ("tool_approval", |launch| {
        launch.tool_approval.as_str().to_string()
    }),
];

/// The argv of the built-in launch, used when the configuration declares none. A
/// deployment that declares its own replaces it, which is how a different executor runs
/// on this fabric without actus naming it.
///
/// Public so a deployment can hold its own declaration against it while the built-in is
/// still here.
pub const DEFAULT_LAUNCH_ARGS: [&str; 5] = [
    "--headless",
    "--allow-multiple-instances",
    "--user-data-dir",
    "{user_data_dir}",
    "{workdir}",
];

/// The environment of the built-in launch, with the same contract as
/// `DEFAULT_LAUNCH_ARGS`.
///
/// The `TELOS_*` entries are the names the contract carried before it was named for the
/// protocol. They are carried here, and in the deploy's generated config, for an
/// executor built before the rename, and they go away once the executor artifacts are
/// built after it.
pub const DEFAULT_LAUNCH_ENV: [(&str, &str); 15] = [
    ("ACPWS_EXTERNAL_SYNC_ENABLED", "true"),
    ("ACPWS_WEBSOCKET_SYNC_ENABLED", "true"),
    ("ACPWS_WS_URL", "{ws_url}"),
    ("ACPWS_WS_TOKEN", "{token}"),
    ("ACPWS_STATELESS", "1"),
    ("ACPWS_SESSION_ID", "{session_id}"),
    ("ACPWS_TOOL_APPROVAL", "{tool_approval}"),
    ("RUST_LOG", "info"),
    ("TELOS_EXTERNAL_SYNC_ENABLED", "true"),
    ("TELOS_WEBSOCKET_SYNC_ENABLED", "true"),
    ("TELOS_WS_URL", "{ws_url}"),
    ("TELOS_WS_TOKEN", "{token}"),
    ("TELOS_STATELESS", "1"),
    ("TELOS_SESSION_ID", "{session_id}"),
    ("TELOS_TOOL_APPROVAL", "{tool_approval}"),
];

/// One process launch: what the configuration declared, and the values actus
/// fills into it.
pub struct Launch<'a> {
    /// The agent this launch is for. Every error names it.
    pub agent_name: &'a str,
    /// The argv the configuration declared. Empty takes the built-in launch, and a
    /// declaration replaces it whole: a deployment that changes one argument writes all
    /// of them.
    pub args: &'a [String],
    /// The environment the configuration declared, with the same contract as `args`,
    /// empty to take the built-in launch.
    pub env: &'a HashMap<String, String>,
    /// The binary to run.
    pub bin: &'a Path,
    pub workdir: &'a Path,
    pub user_data_dir: &'a Path,
    pub session_id: &'a str,
    /// host:port the executor connects back to.
    pub ws_url: &'a str,
    /// The token generated for this process. The executor presents it on the
    /// WebSocket handshake and it authorizes the actus API. Actus does not verify
    /// the handshake yet, so it is a value rather than a constant: a fixed one
    /// would be shared by every deployment, and adding verification later would
    /// then mean changing this contract.
    pub token: &'a str,
    pub http_port: u16,
    pub tool_approval: ToolApproval,
}

/// The argv and environment of one launch, with the markers resolved.
struct LaunchPlan {
    args: Vec<String>,
    env: Vec<(String, String)>,
}

impl Launch<'_> {
    /// The value actus supplies for one marker.
    fn value(&self, marker: &str) -> Option<String> {
        Some(match marker {
            "workdir" => self.workdir.display().to_string(),
            "user_data_dir" => self.user_data_dir.display().to_string(),
            "session_id" => self.session_id.to_string(),
            "ws_url" => self.ws_url.to_string(),
            "token" => self.token.to_string(),
            "agent_name" => self.agent_name.to_string(),
            "http_port" => self.http_port.to_string(),
            "tool_approval" => self.tool_approval.as_str().to_string(),
            _ => return None,
        })
    }

    /// Replace every `{marker}` in one declared value.
    ///
    /// `{{` and `}}` are a literal brace. A marker actus does not supply fails
    /// the launch rather than passing through as text: a declaration that reaches
    /// the executor with its marker intact is a declaration that does nothing.
    fn resolve(&self, value: &str) -> anyhow::Result<String> {
        let mut out = String::with_capacity(value.len());
        let mut rest = value;
        while let Some(at) = rest.find(['{', '}']) {
            out.push_str(&rest[..at]);
            let tail = &rest[at..];
            if let Some(literal) = tail.strip_prefix("{{") {
                out.push('{');
                rest = literal;
                continue;
            }
            if let Some(literal) = tail.strip_prefix("}}") {
                out.push('}');
                rest = literal;
                continue;
            }
            if tail.starts_with('{') {
                let close = tail.find('}').ok_or_else(|| {
                    anyhow::anyhow!("a '{{' opens a marker that no '}}' closes in {value:?}")
                })?;
                let marker = &tail[1..close];
                let resolved = self.value(marker).ok_or_else(|| {
                    anyhow::anyhow!(
                        "'{{{marker}}}' is not a marker actus supplies; it supplies {}",
                        marker_names().join(", ")
                    )
                })?;
                out.push_str(&resolved);
                rest = &tail[close + 1..];
                continue;
            }
            // A closing brace no opening one paired is ordinary text.
            out.push('}');
            rest = &tail[1..];
        }
        out.push_str(rest);
        Ok(out)
    }

    /// The argv and environment of the launch, with the markers resolved.
    fn declared(&self) -> anyhow::Result<LaunchPlan> {
        let args: Vec<String> = if self.args.is_empty() {
            DEFAULT_LAUNCH_ARGS.map(str::to_string).to_vec()
        } else {
            self.args.to_vec()
        };
        let env: Vec<(String, String)> = if self.env.is_empty() {
            DEFAULT_LAUNCH_ENV
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect()
        } else {
            self.env
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect()
        };

        let mut resolved_args = Vec::with_capacity(args.len());
        for arg in &args {
            resolved_args.push(
                self.resolve(arg)
                    .map_err(|e| anyhow::anyhow!("launch_args: {e}"))?,
            );
        }
        let mut resolved_env = Vec::with_capacity(env.len());
        for (key, value) in &env {
            resolved_env.push((
                key.clone(),
                self.resolve(value)
                    .map_err(|e| anyhow::anyhow!("launch_env[{key}]: {e}"))?,
            ));
        }
        Ok(LaunchPlan {
            args: resolved_args,
            env: resolved_env,
        })
    }
}

/// The names a declared launch may carry, for the message that refuses one it may not.
fn marker_names() -> Vec<&'static str> {
    LAUNCH_MARKERS.iter().map(|(name, _)| *name).collect()
}

/// Build the process command for one launch, kept separate from spawning so the
/// contract is unit-testable without a real executor.
pub fn launch_command(
    launch: &Launch<'_>,
    stderr_log: std::fs::File,
) -> anyhow::Result<std::process::Command> {
    let plan = launch
        .declared()
        .map_err(|e| anyhow::anyhow!("agent '{}': {e}", launch.agent_name))?;
    let mut cmd = std::process::Command::new(launch.bin);
    cmd.args(plan.args);
    for (key, value) in plan.env {
        cmd.env(key, value);
    }
    // The control MCP proxy is actus's own, spawned by the executor as a stdio
    // server, and it reads these to reach this process and to name itself. They
    // are actus's interface rather than the executor's, so a deployment does not
    // restate them.
    cmd.env("ACTUS_AGENT_NAME", launch.agent_name)
        .env("ACTUS_HTTP_PORT", launch.http_port.to_string())
        .env("ACTUS_API_TOKEN", launch.token)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::from(stderr_log));
    Ok(cmd)
}

/// Spawn the agent process for one launch.
pub async fn launch_agent(
    launch: &Launch<'_>,
    stderr_log: &Path,
) -> anyhow::Result<std::process::Child> {
    tracing::info!("Launching agent '{}'", launch.agent_name);

    let stderr_log = std::fs::File::create(stderr_log)
        .map_err(|e| anyhow::anyhow!("cannot create stderr log {}: {}", stderr_log.display(), e))?;
    let mut cmd = launch_command(launch, stderr_log)?;
    let child = cmd
        .spawn()
        .map_err(|e| anyhow::anyhow!("cannot spawn {}: {}", launch.bin.display(), e))?;

    tracing::info!(
        "Agent '{}' started (PID {:?})",
        launch.agent_name,
        child.id()
    );
    Ok(child)
}

// ── Agent settings bootstrap ─────────────────────────────────────────────

/// Resolve a declared value to the string the agent's settings carry.
///
/// Every `$NAME` in the value is read from the actus environment, so a token
/// reaches an MCP server without being written to the config file, and a header
/// that decorates one (`Bearer $NAME`) keeps its own text.
///
/// `required` is whether the server starts with the agent. For a server that
/// starts, a name that is not set fails the launch and names the variable: an
/// empty token would leave the server running and failing on every call, which is
/// harder to see than a launch that names the variable once. A server that does
/// not start has its value left as written and the missing name reported, because
/// requiring it would make every launch depend on a credential for a server nobody
/// runs; the agent can still turn that server on, and the report is what says the
/// credential is missing when it does.
pub fn resolve_declared(
    declared: &std::collections::HashMap<String, String>,
    server: &str,
    kind: &str,
    required: bool,
) -> anyhow::Result<std::collections::HashMap<String, String>> {
    let mut resolved = std::collections::HashMap::with_capacity(declared.len());
    for (key, value) in declared {
        let resolved_value = resolve_variables(value, |name| match std::env::var(name) {
            Ok(value) => Ok(value),
            Err(_) if required => Err(anyhow::anyhow!(
                "MCP server '{server}': {kind} '{key}' refers to ${name}, which is not set"
            )),
            Err(_) => {
                tracing::warn!(
                    "MCP server '{server}': {kind} '{key}' refers to ${name}, which is not set; \
                     the server does not start, and turning it on will need it"
                );
                Ok(format!("${name}"))
            }
        })?;
        resolved.insert(key.clone(), resolved_value);
    }
    Ok(resolved)
}

/// Replace every `$NAME` in `value` with `lookup(NAME)`, where a name starts with a
/// letter or an underscore. A `$` that no name follows stays literal, so an amount
/// like `$5` or a bare dollar sign is unchanged rather than being read as a variable.
fn resolve_variables(
    value: &str,
    lookup: impl Fn(&str) -> anyhow::Result<String>,
) -> anyhow::Result<String> {
    let bytes = value.as_bytes();
    let mut out = String::with_capacity(value.len());
    let mut index = 0;
    while index < bytes.len() {
        let starts_name = bytes[index] == b'$'
            && bytes
                .get(index + 1)
                .is_some_and(|next| next.is_ascii_alphabetic() || *next == b'_');
        if !starts_name {
            out.push(bytes[index] as char);
            index += 1;
            continue;
        }

        let name_start = index + 1;
        let mut name_end = name_start;
        while name_end < bytes.len()
            && (bytes[name_end].is_ascii_alphanumeric() || bytes[name_end] == b'_')
        {
            name_end += 1;
        }
        out.push_str(&lookup(&value[name_start..name_end])?);
        index = name_end;
    }
    Ok(out)
}

/// What writes the executor's settings before it is launched.
///
/// A settings file is the executor's format: which keys exist, how they nest, and what a
/// headless run needs from them are properties of the program that reads it. Actus knows
/// the values it resolved, the endpoint and the reasoning effort, the MCP catalogue, the
/// approval policy, and the data dir the executor was told to use, and hands them over.
/// This renders them into that format, so a deployment that runs another executor
/// supplies another writer and actus is unchanged.
pub trait SettingsWriter: Send + Sync {
    /// Write the settings the executor reads into the data dir it was told to use.
    fn write(&self, data_dir: &Path, spec: &AgentSpec) -> anyhow::Result<()>;
}

/// The settings shape of the executor this stack runs today.
pub struct DefaultSettings;

impl SettingsWriter for DefaultSettings {
    fn write(&self, data_dir: &Path, spec: &AgentSpec) -> anyhow::Result<()> {
        write_executor_settings(data_dir, spec)
    }
}

/// The built-in writer, for a composition that names none.
pub fn default_settings() -> Arc<dyn SettingsWriter> {
    Arc::new(DefaultSettings)
}

/// Write the settings the built-in writer produces.
///
/// Public because it is the shape the tests pin, and because a deployment that runs this
/// executor wants something to check its own writer against.
pub fn ensure_agent_settings(data_dir: &Path, spec: &AgentSpec) -> anyhow::Result<()> {
    DefaultSettings.write(data_dir, spec)
}

fn write_executor_settings(data_dir: &Path, spec: &AgentSpec) -> anyhow::Result<()> {
    use std::fs;
    use std::io::Write;

    let api_key = spec.api_key.as_deref().ok_or_else(|| {
        anyhow::anyhow!(
            "agent '{}': LLM API key required (LLM_API_KEY or --api-key)",
            spec.name
        )
    })?;
    let provider = spec.provider.as_str();
    let base_url = spec.base_url.as_str();
    let model_name = spec.model.as_str();
    let model_display = spec.model_display.as_str();
    let reasoning_effort = spec.reasoning_effort.as_str();
    let mcp = &spec.mcp;
    let tool_approval = spec.tool_approval;

    let settings_dir = data_dir.join("config");
    fs::create_dir_all(&settings_dir)?;
    let settings_file = settings_dir.join("settings.json");

    let mut settings: serde_json::Value = if settings_file.exists() {
        serde_json::from_str(&fs::read_to_string(&settings_file)?)?
    } else {
        serde_json::json!({})
    };

    // Inject the OpenAI-compatible endpoint only when the operator
    // supplied a base URL and a model name (local environment or agent
    // config). Without them actus writes no provider entry: an empty
    // api_url or model would break the agent's settings parse, and actus
    // does not invent endpoint values.
    let endpoint_configured = !base_url.trim().is_empty() && !model_name.trim().is_empty();
    if endpoint_configured {
        if settings
            .get("language_models")
            .and_then(|lm| lm.get("openai_compatible"))
            .and_then(|oc| oc.get(provider))
            .is_none()
        {
            let mut model = serde_json::json!({
                "name": model_name,
                "display_name": model_display,
                "max_tokens": 65536,
                "max_output_tokens": 8192,
                "tool_use": true,
            });
            // The agent's OpenAI-compatible provider decides whether a model can
            // think from this field, and sends the level with every request. A
            // level of `none` is written as no field at all: that is the one
            // shape that asks for no reasoning parameter, which is what a model
            // that rejects the parameter needs.
            if reasoning_effort != "none" {
                model["reasoning_effort"] = serde_json::json!(reasoning_effort);
            }
            settings["language_models"]["openai_compatible"][provider] = serde_json::json!({
                "api_url": base_url,
                "available_models": [model],
            });
        }
    } else {
        tracing::warn!(
            "LLM endpoint not configured (set LLM_BASE_URL and LLM_MODEL in the local environment or the agent's config.toml); skipping provider injection"
        );
    }

    // A thread reads its thinking state from `agent.default_model` and nowhere
    // else, so without this a launched agent thinks at the provider's default
    // while its editor sibling thinks at the level the operator chose. Written
    // only when nothing set it, so a hand-edited file stays the operator's.
    if endpoint_configured && settings["agent"]["default_model"].is_null() {
        let enable_thinking = reasoning_effort != "none";
        settings["agent"]["default_model"] = serde_json::json!({
            "provider": provider,
            "model": model_name,
            "enable_thinking": enable_thinking,
            "effort": if enable_thinking {
                serde_json::Value::String(reasoning_effort.to_string())
            } else {
                serde_json::Value::Null
            },
        });
    }

    // The terminal is the tool a headless agent runs commands with, and it is the one the
    // agent's permission gate can refuse outright: a command containing a shell
    // substitution is denied whenever the tool's effective decision is not an
    // unconditional allow, and that happens before an approval could be asked for. The
    // refusal protects a per-command approval prompt, so a deployment that starts its
    // agents with `always` has nothing left for it to protect and gets the tool opened
    // up here. Any other mode keeps the agent's own setting, prompt and all.
    //
    // Only the terminal's own default is written, and only when nothing set it, so a rule
    // an operator wrote by hand stays theirs.
    if tool_approval == crate::agent::config::ToolApproval::Always {
        let permissions = &mut settings["agent"]["tool_permissions"];
        if permissions["tools"]["terminal"]["default"].is_null() {
            permissions["tools"]["terminal"]["default"] = serde_json::json!("allow");
        }
    }

    // MCP servers: map each declaration to the executor.s `context_servers` entry.
    // Stdio servers become `{ command, args, env }`; HTTP servers become
    // `{ url, headers }`. The headless agent's context server registry
    // starts the enabled ones and exposes their tools to the model, and the
    // disabled ones stay in the agent's catalog, which is what the agent's
    // own `enable_context_server` tool reads.
    //
    // Declared entries are merged over whatever the file holds, so a server
    // the operator added by hand survives. A value of the form `$NAME` is
    // resolved from the actus environment here, and an unset name is an
    // error: the server process would otherwise start with an empty token and
    // fail on every call instead of once, at launch, with the name to fix.
    for s in mcp {
        let mut obj = serde_json::Map::new();
        if let Some(url) = &s.url {
            obj.insert("url".to_string(), serde_json::json!(url));
            let headers = resolve_declared(&s.headers, &s.name, "header", s.enabled)?;
            if !headers.is_empty() {
                obj.insert("headers".to_string(), serde_json::json!(headers));
            }
        } else {
            if let Some(cmd) = &s.command {
                obj.insert("command".to_string(), serde_json::json!(cmd));
            }
            if !s.args.is_empty() {
                obj.insert("args".to_string(), serde_json::json!(s.args));
            }
            let env = resolve_declared(&s.env, &s.name, "env value", s.enabled)?;
            if !env.is_empty() {
                obj.insert("env".to_string(), serde_json::json!(env));
            }
            if let Some(t) = s.timeout {
                obj.insert("timeout".to_string(), serde_json::json!(t));
            }
        }
        obj.insert("enabled".to_string(), serde_json::json!(s.enabled));
        settings["context_servers"][s.name.as_str()] = serde_json::Value::Object(obj);
    }
    if !mcp.is_empty() {
        let enabled = mcp.iter().filter(|s| s.enabled).count();
        tracing::info!(
            "Wrote {} MCP server(s) to settings, {} of them enabled",
            mcp.len(),
            enabled
        );
    }

    let mut f = fs::File::create(&settings_file)?;
    f.write_all(serde_json::to_string_pretty(&settings)?.as_bytes())?;

    let creds_dir = data_dir.join("credentials");
    fs::create_dir_all(&creds_dir)?;
    let creds_file = creds_dir.join("credentials.json");

    let mut creds = serde_json::Map::new();
    let mut provider_creds = serde_json::Map::new();
    provider_creds.insert(
        "api_key".to_string(),
        serde_json::Value::String(api_key.to_string()),
    );
    creds.insert(
        format!("provider/{}", provider),
        serde_json::Value::Object(provider_creds),
    );
    let creds = serde_json::Value::Object(creds);

    let mut f = fs::File::create(&creds_file)?;
    f.write_all(serde_json::to_string_pretty(&creds)?.as_bytes())?;

    tracing::info!("Executor settings written to {}", settings_file.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{launch_command, resolve_variables, Launch};
    use crate::agent::config::ToolApproval;
    use std::collections::HashMap;
    use std::path::Path;

    fn launch_with<'a>(
        bin: &'a Path,
        workdir: &'a Path,
        user_data_dir: &'a Path,
        args: &'a [String],
        env: &'a HashMap<String, String>,
    ) -> Launch<'a> {
        Launch {
            agent_name: "telos",
            args,
            env,
            bin,
            workdir,
            user_data_dir,
            session_id: "ses_actus-test",
            ws_url: "127.0.0.1:8080",
            token: "process-token-7f3a",
            http_port: 9090,
            tool_approval: ToolApproval::Always,
        }
    }

    fn args_of(cmd: &std::process::Command) -> Vec<String> {
        cmd.get_args()
            .map(|a| a.to_string_lossy().into_owned())
            .collect()
    }

    fn envs_of(cmd: &std::process::Command) -> HashMap<String, String> {
        cmd.get_envs()
            .filter_map(|(k, v)| {
                v.map(|value| {
                    (
                        k.to_string_lossy().into_owned(),
                        value.to_string_lossy().into_owned(),
                    )
                })
            })
            .collect()
    }

    /// A configuration that declares no launch still gets the executor this stack
    /// runs today, both its argv and its environment.
    #[test]
    fn the_built_in_launch_sets_expected_args_and_env() {
        let dir = tempfile::tempdir().unwrap();
        let workdir = dir.path().join("work");
        std::fs::create_dir_all(&workdir).unwrap();
        let user_data_dir = dir.path().join("user");
        std::fs::create_dir_all(&user_data_dir).unwrap();
        let log = std::fs::File::create(dir.path().join("agent.log")).unwrap();
        let bin = dir.path().join("tel");

        let no_env = HashMap::new();
        let launch = launch_with(&bin, &workdir, &user_data_dir, &[], &no_env);
        let cmd = launch_command(&launch, log).unwrap();

        let args = args_of(&cmd);
        let pair = [
            "--user-data-dir".to_string(),
            user_data_dir.to_string_lossy().into_owned(),
        ];
        assert!(args.windows(2).any(|w| w == pair), "args: {args:?}");
        assert!(args.contains(&workdir.to_string_lossy().into_owned()));
        assert!(args.iter().any(|a| a == "--headless"));
        assert!(args.iter().any(|a| a == "--allow-multiple-instances"));

        let envs = envs_of(&cmd);
        let expect = [
            ("ACPWS_EXTERNAL_SYNC_ENABLED", "true"),
            ("ACPWS_WEBSOCKET_SYNC_ENABLED", "true"),
            ("ACPWS_WS_URL", "127.0.0.1:8080"),
            ("ACPWS_WS_TOKEN", "process-token-7f3a"),
            ("ACPWS_STATELESS", "1"),
            ("ACPWS_SESSION_ID", "ses_actus-test"),
            ("ACPWS_TOOL_APPROVAL", "always"),
            ("RUST_LOG", "info"),
        ];
        for (key, value) in expect {
            assert_eq!(envs.get(key).map(String::as_str), Some(value), "env {key}");
        }

        // The names the contract carried before the rename are carried too, which is
        // what lets an executor built before it on this launch. They go away with the
        // executor artifacts.
        let legacy = [
            ("TELOS_EXTERNAL_SYNC_ENABLED", "true"),
            ("TELOS_WEBSOCKET_SYNC_ENABLED", "true"),
            ("TELOS_WS_URL", "127.0.0.1:8080"),
            ("TELOS_WS_TOKEN", "process-token-7f3a"),
            ("TELOS_STATELESS", "1"),
            ("TELOS_SESSION_ID", "ses_actus-test"),
            ("TELOS_TOOL_APPROVAL", "always"),
        ];
        for (key, value) in legacy {
            assert_eq!(envs.get(key).map(String::as_str), Some(value), "env {key}");
        }
    }

    /// The variables actus sets for its own control proxy are actus's interface,
    /// so a deployment does not restate them and a declaration replaces the
    /// executor's argv and environment only.
    #[test]
    fn actus_sets_its_own_proxy_env_whatever_the_declaration_says() {
        let dir = tempfile::tempdir().unwrap();
        let log = std::fs::File::create(dir.path().join("agent.log")).unwrap();
        let bin = dir.path().join("tel");
        let args = vec!["--serve".to_string()];
        let env = HashMap::from([("WS".to_string(), "wss://{ws_url}".to_string())]);

        let launch = launch_with(&bin, dir.path(), dir.path(), &args, &env);
        let cmd = launch_command(&launch, log).unwrap();

        let envs = envs_of(&cmd);
        assert_eq!(
            envs.get("ACTUS_AGENT_NAME").map(String::as_str),
            Some("telos")
        );
        assert_eq!(
            envs.get("ACTUS_HTTP_PORT").map(String::as_str),
            Some("9090")
        );
        assert_eq!(
            envs.get("ACTUS_API_TOKEN").map(String::as_str),
            Some("process-token-7f3a")
        );
        assert_eq!(args_of(&cmd), vec!["--serve"]);
        assert_eq!(
            envs.get("WS").map(String::as_str),
            Some("wss://127.0.0.1:8080")
        );
    }

    /// A declaration names the executor's own arguments and environment, and the
    /// markers are the values actus owns. A marker actus does not supply fails
    /// the launch rather than reaching the executor as text, and `{{`/`}}` are a
    /// literal brace.
    #[test]
    fn a_declared_launch_resolves_its_markers_and_refuses_an_unknown_one() {
        let dir = tempfile::tempdir().unwrap();
        let bin = dir.path().join("tel");
        let args = vec!["--headless".to_string(), "{workdir}".to_string()];
        let env = HashMap::from([
            ("SESSION".to_string(), "{session_id}".to_string()),
            ("POLICY".to_string(), "{tool_approval}".to_string()),
            ("LITERAL".to_string(), "{{braces}}".to_string()),
        ]);

        let log = std::fs::File::create(dir.path().join("agent.log")).unwrap();
        let launch = launch_with(&bin, dir.path(), dir.path(), &args, &env);
        let cmd = launch_command(&launch, log).unwrap();
        assert_eq!(
            args_of(&cmd),
            vec!["--headless".to_string(), dir.path().display().to_string()]
        );
        let envs = envs_of(&cmd);
        assert_eq!(
            envs.get("SESSION").map(String::as_str),
            Some("ses_actus-test")
        );
        assert_eq!(envs.get("POLICY").map(String::as_str), Some("always"));
        assert_eq!(envs.get("LITERAL").map(String::as_str), Some("{braces}"));
        assert!(!envs.contains_key("ACPWS_WS_URL"), "envs: {envs:?}");
        assert!(!envs.contains_key("TELOS_WS_URL"), "envs: {envs:?}");

        let unknown = vec!["{ws_socket}".to_string()];
        let log = std::fs::File::create(dir.path().join("second.log")).unwrap();
        let launch = launch_with(&bin, dir.path(), dir.path(), &unknown, &env);
        let error = launch_command(&launch, log).unwrap_err().to_string();
        assert!(error.contains("agent 'telos'"), "{error}");
        assert!(error.contains("ws_socket"), "{error}");
    }

    #[test]
    fn a_variable_is_replaced_where_it_appears() {
        let resolved = resolve_variables("Bearer $TOKEN", |name| {
            assert_eq!(name, "TOKEN");
            Ok("abc".to_string())
        })
        .unwrap();
        assert_eq!(resolved, "Bearer abc");

        let resolved = resolve_variables("$A-$B", |name| Ok(name.to_lowercase())).unwrap();
        assert_eq!(resolved, "a-b");
    }

    /// A dollar sign that no name follows is ordinary text, so a value that
    /// carries one is not mangled and does not have to be escaped.
    #[test]
    fn a_lone_dollar_sign_is_left_alone() {
        let resolved = resolve_variables("costs $5 and a bare $", |name| {
            panic!("no variable is named in this value, got {name}")
        })
        .unwrap();
        assert_eq!(resolved, "costs $5 and a bare $");
    }

    #[test]
    fn an_unset_variable_names_itself_in_the_error() {
        let error = resolve_variables("$KLETOS_ABSENT_TEST", |name| {
            Err(anyhow::anyhow!("refers to ${name}, which is not set"))
        })
        .unwrap_err()
        .to_string();
        assert!(error.contains("$KLETOS_ABSENT_TEST"), "{error}");
    }
}
