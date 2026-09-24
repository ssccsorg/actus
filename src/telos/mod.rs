// Telos management — process lifecycle, WebSocket bridge, and protocol types.

pub mod backend;
pub mod control;
pub mod types;

use crate::agent::config::AgentSpec;
use crate::agent::{PendingAuthorization, ThreadMessage, ThreadSession};
use std::sync::atomic::{AtomicBool, Ordering};

/// Channel sender for WebSocket commands to Telos. Shared between
/// `AppState` and `TelosManager` so cancel can send without acquiring the
/// `TelosManager` RwLock (avoiding lock contention with long-running SSE
/// handlers).
pub type WsCommandTx = Arc<tokio::sync::Mutex<Option<mpsc::UnboundedSender<String>>>>;

/// Truncate `s` to at most `max` bytes without splitting a UTF-8
/// character. Returns the original string when it is already short
/// enough; otherwise the longest prefix that ends on a char boundary.
fn truncate_utf8(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

// Telos manager — WebSocket connection, session management, and settings bootstrap

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json;
use tokio::sync::{mpsc, watch, Notify, RwLock};
use uuid::Uuid;

/// Manages a single Telos WebSocket connection and message dispatch.
#[allow(dead_code)]
pub struct TelosManager {
    pub session_id: String,
    pub ws_host: String,
    pub telos_connected: bool,
    pub agent_ready: bool,
    /// Channel to send WebSocket commands to Telos
    pub ws_tx: Option<mpsc::UnboundedSender<String>>,
    /// Threads managed by this Telos instance
    pub threads: HashMap<String, ThreadSession>,
    /// Mapping from request_id to acp_thread_id (for correlating responses)
    pub pending_requests: HashMap<String, String>,
    /// Mapping from telos_thread_id to local_thread_id (for reverse lookup)
    pub thread_id_map: HashMap<String, String>,
    /// Path to the threads persistence file
    pub threads_file: PathBuf,
    /// Threads that have been activated (context sent) in the current Telos session
    pub threads_activated: HashSet<String>,
    /// Notifier for thread state changes (SSE consumers)
    pub thread_notify: watch::Sender<u64>,
    /// Waiters for threads whose acp_thread_id is being established
    pub thread_waiters: HashMap<String, Arc<Notify>>,
    /// Monotonically increasing reconnect counter. Incremented each time
    /// a new WS connection is established (for SSE consumers to detect).
    pub reconnect_count: u64,
    /// Timestamp of the last PING received from Telos (for keepalive).
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
    /// completed turn of each thread. Telos's flush_streaming_throttle
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

impl TelosManager {
    pub fn new(session_id: String, ws_host: String, threads_dir: &Path) -> Self {
        let threads_file = threads_dir.join("threads.json");
        let threads = Self::load_threads(&threads_file);

        // Rebuild thread_id_map from persisted threads that have an acp_thread_id
        let mut thread_id_map = HashMap::new();
        for (local_id, thread) in &threads {
            if let Some(acp_id) = &thread.acp_thread_id {
                thread_id_map.insert(acp_id.clone(), local_id.clone());
            }
        }

        let (thread_notify, _) = watch::channel(0u64);

        Self {
            session_id,
            ws_host,
            telos_connected: false,
            agent_ready: false,
            ws_tx: None,
            threads,
            pending_requests: HashMap::new(),
            thread_id_map,
            threads_file,
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
        }
    }

    /// Prepare the user message, injecting conversation context if this
    /// thread has not been activated in the current Telos session yet.
    pub fn prepare_message(&mut self, thread_id: &str, user_message: &str) -> String {
        if !self.threads_activated.contains(thread_id) {
            // Clear stale acp_thread_id from previous sessions
            if let Some(thread) = self.threads.get_mut(thread_id) {
                thread.acp_thread_id = None;
            }
            self.thread_id_map.retain(|_, v| v != thread_id);
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
                    completed: false,
                    acp_thread_id: None,
                    turn_completed: 0,
                    parent: None,
                },
            );
            self.notify_thread_change();
            self.save_threads();
        }
        id
    }

    /// Look up the ACP thread ID for a given local thread ID.
    /// ACP threads are created by Telos and stored in thread_id_map.
    /// Returns None for new threads (Telos will create a fresh ACP thread).
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
        self.add_message_full(thread_id, role, content, message_id, None, None, None)
    }

    /// Record the (scoped message id → content) snapshot of the most
    /// recent assistant messages in a thread. Called when a turn ends so a
    /// follow-up turn's replay of these entries can be recognized and
    /// dropped. Only the trailing assistant block is snapshotted; the
    /// trailing block ends at the last user message, which is the turn
    /// boundary.
    pub fn record_prior_entries(&mut self, thread_id: &str) {
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
    /// Telos emits updates for several interleaved messages in one turn
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
    ) {
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
                timestamp: chrono::Utc::now(),
            });
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

    /// Send a cancel_current_turn command to Telos via WebSocket.
    pub fn cancel_current_turn(&self) -> Result<(), String> {
        let cmd = serde_json::json!({
            "type": "cancel_current_turn",
            "data": {}
        });
        self.send_command(&cmd.to_string())
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
        match serde_json::to_string_pretty(&self.threads) {
            Ok(json) => {
                if let Err(e) = std::fs::write(&self.threads_file, &json) {
                    tracing::error!("Failed to write threads file: {}", e);
                }
            }
            Err(e) => {
                tracing::error!("Failed to serialize threads: {}", e);
            }
        }
    }

    /// Background persistence loop: every second, if the dirty flag is set,
    /// snapshot the threads under a read lock, release it, and write the
    /// file on a blocking thread. Keeps the heavy JSON serialization and
    /// disk write off the manager lock and off the async runtime.
    pub fn spawn_thread_saver(manager: Arc<RwLock<TelosManager>>) {
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
                let (snapshot, path) = {
                    let mgr = manager.read().await;
                    (mgr.threads.clone(), mgr.threads_file.clone())
                };
                tokio::task::spawn_blocking(move || {
                    match serde_json::to_string_pretty(&snapshot) {
                        Ok(json) => {
                            if let Err(e) = std::fs::write(&path, &json) {
                                tracing::error!("Failed to write threads file: {}", e);
                            }
                        }
                        Err(e) => tracing::error!("Failed to serialize threads: {}", e),
                    }
                })
                .await
                .ok();
            }
        });
    }

    /// Load threads from a JSON file. Returns an empty map if the file does not exist or is unreadable.
    /// Fills in missing titles from the first user message for backward compatibility.
    pub fn load_threads(path: &Path) -> HashMap<String, ThreadSession> {
        if !path.exists() {
            return HashMap::new();
        }
        match std::fs::read_to_string(path) {
            Ok(content) => match serde_json::from_str::<HashMap<String, ThreadSession>>(&content) {
                Ok(mut threads) => {
                    // Backfill titles for threads saved before the title field existed
                    for thread in threads.values_mut() {
                        if thread.title.is_none() {
                            if let Some(first_user) =
                                thread.messages.iter().find(|m| m.role == "user")
                            {
                                let content = first_user.content.trim();
                                let mut truncated = truncate_utf8(content, 80).to_string();
                                if content.len() > 80 {
                                    truncated.push_str("...");
                                }
                                thread.title = Some(truncated);
                            }
                        }
                    }
                    // Repair a historical bug where streaming updates to the
                    // same message id were appended instead of replaced
                    // (Telos emits thinking, tool call, and answer messages
                    // with interleaved ids), swelling a turn into dozens of
                    // duplicate entries. Keep the last occurrence of each id
                    // and drop the earlier duplicates; user messages and
                    // id-less entries are kept as-is.
                    for thread in threads.values_mut() {
                        let mut seen: std::collections::HashSet<String> =
                            std::collections::HashSet::new();
                        let mut kept: Vec<ThreadMessage> =
                            Vec::with_capacity(thread.messages.len());
                        for m in std::mem::take(&mut thread.messages) {
                            match &m.message_id {
                                Some(id) if !seen.insert(id.clone()) => {
                                    tracing::warn!(
                                        "Dropping duplicate message id {} in thread {}",
                                        id,
                                        thread.id
                                    );
                                }
                                _ => kept.push(m),
                            }
                        }
                        thread.messages = kept;
                    }
                    // Repair a historical index drift: an errored or
                    // cancelled turn used to bump turn_completed without
                    // adding an assistant message, so persisted threads
                    // can have turn_completed > assistant-message count.
                    // Poll and SSE address assistant messages by turn
                    // index, so the counter must not point past them.
                    for thread in threads.values_mut() {
                        let text_assistants = thread
                            .messages
                            .iter()
                            .filter(|m| {
                                m.role == "assistant"
                                    && m.entry_type.as_deref() != Some("tool_call")
                            })
                            .count();
                        if thread.turn_completed > text_assistants as u64 {
                            tracing::warn!(
                                "Repairing turn_completed {} -> {} ({} assistant msgs) for thread {}",
                                thread.turn_completed,
                                text_assistants,
                                text_assistants,
                                thread.id
                            );
                            thread.turn_completed = text_assistants as u64;
                        }
                    }
                    tracing::info!("Loaded {} threads from {}", threads.len(), path.display());
                    threads
                }
                Err(e) => {
                    tracing::error!(
                        "Failed to deserialize threads from {}: {}",
                        path.display(),
                        e
                    );
                    HashMap::new()
                }
            },
            Err(e) => {
                tracing::error!("Failed to read threads file {}: {}", path.display(), e);
                HashMap::new()
            }
        }
    }

    /// Send a JSON command to Telos via WebSocket. Returns error if not connected.
    pub fn send_command(&self, cmd: &str) -> Result<(), String> {
        match &self.ws_tx {
            Some(tx) => tx.send(cmd.to_string()).map_err(|e| e.to_string()),
            None => Err("WebSocket not connected".to_string()),
        }
    }
}

/// Build the telos launch command. Kept separate from spawning so the
/// launch contract (argv and the `TELOS_*` env set) is unit-testable
/// without a real telos binary.
fn telos_command(
    bin_path: &Path,
    workdir: &Path,
    user_data_dir: &Path,
    session_id: &str,
    ws_host: &str,
    tool_approval: crate::agent::config::ToolApproval,
    agent_name: &str,
    http_port: u16,
    api_token: &str,
    stderr_log: std::fs::File,
) -> std::process::Command {
    let mut cmd = std::process::Command::new(bin_path);
    cmd.args(["--headless", "--allow-multiple-instances"])
        .arg("--user-data-dir")
        .arg(user_data_dir)
        .arg(workdir)
        .env("TELOS_EXTERNAL_SYNC_ENABLED", "true")
        .env("TELOS_WEBSOCKET_SYNC_ENABLED", "true")
        .env("TELOS_WS_URL", ws_host)
        // Telos presents this on the WebSocket handshake. Actus does not verify
        // it yet, so it is the token generated for this process rather than a
        // constant: a fixed value would be shared by every deployment, and
        // adding verification later would then mean changing this contract.
        .env("TELOS_WS_TOKEN", api_token)
        .env("TELOS_STATELESS", "1")
        .env("TELOS_SESSION_ID", session_id)
        .env("TELOS_TOOL_APPROVAL", tool_approval.as_str())
        // The control MCP proxy (`actus control`), spawned by the agent as
        // a stdio MCP server, inherits these to reach the actus HTTP API
        // and to identify itself as this agent.
        .env("ACTUS_AGENT_NAME", agent_name)
        .env("ACTUS_HTTP_PORT", http_port.to_string())
        .env("ACTUS_API_TOKEN", api_token)
        .env("RUST_LOG", "info")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::from(stderr_log));
    cmd
}

pub async fn launch_telos(
    bin_path: &Path,
    workdir: &Path,
    user_data_dir: &Path,
    session_id: &str,
    ws_host: &str,
    tool_approval: crate::agent::config::ToolApproval,
    agent_name: &str,
    http_port: u16,
    api_token: &str,
    stderr_log: &Path,
) -> anyhow::Result<std::process::Child> {
    tracing::info!("Launching telos...");

    let stderr_log = std::fs::File::create(stderr_log)
        .map_err(|e| anyhow::anyhow!("cannot create stderr log {}: {}", stderr_log.display(), e))?;
    let mut cmd = telos_command(
        bin_path,
        workdir,
        user_data_dir,
        session_id,
        ws_host,
        tool_approval,
        agent_name,
        http_port,
        api_token,
        stderr_log,
    );
    let child = cmd
        .spawn()
        .map_err(|e| anyhow::anyhow!("cannot spawn {}: {}", bin_path.display(), e))?;

    tracing::info!("telos started (PID: {:?})", child.id());
    Ok(child)
}

// ── Telos settings bootstrap ─────────────────────────────────────────────

/// Resolve a declared value to the string the agent's settings carry.
///
/// Every `$NAME` in the value is read from the actus environment, so a token
/// reaches an MCP server without being written to the config file, and a header
/// that decorates one (`Bearer $NAME`) keeps its own text. A name that is not set
/// is an error: an empty token would leave the server running and failing on every
/// call, which is harder to see than a launch that names the variable once.
pub fn resolve_declared(
    declared: &std::collections::HashMap<String, String>,
    server: &str,
    kind: &str,
) -> anyhow::Result<std::collections::HashMap<String, String>> {
    let mut resolved = std::collections::HashMap::with_capacity(declared.len());
    for (key, value) in declared {
        resolved.insert(
            key.clone(),
            resolve_variables(value, |name| {
                std::env::var(name).map_err(|_| {
                    anyhow::anyhow!(
                        "MCP server '{server}': {kind} '{key}' refers to ${name}, which is not set"
                    )
                })
            })?,
        );
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

pub fn ensure_telos_settings(data_dir: &Path, spec: &AgentSpec) -> anyhow::Result<()> {
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

    // MCP servers: map each declaration to Telos's `context_servers` entry.
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
            let headers = resolve_declared(&s.headers, &s.name, "header")?;
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
            let env = resolve_declared(&s.env, &s.name, "env value")?;
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

    tracing::info!("Telos settings written to {}", settings_file.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{resolve_variables, telos_command};
    use std::collections::HashMap;

    #[test]
    fn launch_contract_sets_expected_args_and_env() {
        let dir = tempfile::tempdir().unwrap();
        let workdir = dir.path().join("work");
        std::fs::create_dir_all(&workdir).unwrap();
        let user_data_dir = dir.path().join("user");
        std::fs::create_dir_all(&user_data_dir).unwrap();
        let log = std::fs::File::create(dir.path().join("telos.log")).unwrap();
        let bin = dir.path().join("tel");

        let cmd = telos_command(
            &bin,
            &workdir,
            &user_data_dir,
            "ses_actus-test",
            "127.0.0.1:8080",
            crate::agent::config::ToolApproval::Always,
            "telos",
            9090,
            "process-token-7f3a",
            log,
        );

        let args: Vec<String> = cmd
            .get_args()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        let pair = [
            "--user-data-dir".to_string(),
            user_data_dir.to_string_lossy().into_owned(),
        ];
        assert!(args.windows(2).any(|w| w == pair), "args: {args:?}");
        assert!(args.contains(&workdir.to_string_lossy().into_owned()));
        assert!(args.iter().any(|a| a == "--headless"));
        assert!(args.iter().any(|a| a == "--allow-multiple-instances"));

        let envs: HashMap<String, String> = cmd
            .get_envs()
            .filter_map(|(k, v)| {
                v.map(|value| {
                    (
                        k.to_string_lossy().into_owned(),
                        value.to_string_lossy().into_owned(),
                    )
                })
            })
            .collect();
        let expect = [
            ("TELOS_EXTERNAL_SYNC_ENABLED", "true"),
            ("TELOS_WEBSOCKET_SYNC_ENABLED", "true"),
            ("TELOS_WS_URL", "127.0.0.1:8080"),
            ("TELOS_WS_TOKEN", "process-token-7f3a"),
            ("TELOS_STATELESS", "1"),
            ("TELOS_SESSION_ID", "ses_actus-test"),
            ("TELOS_TOOL_APPROVAL", "always"),
            ("ACTUS_AGENT_NAME", "telos"),
            ("ACTUS_HTTP_PORT", "9090"),
            ("ACTUS_API_TOKEN", "process-token-7f3a"),
            ("RUST_LOG", "info"),
        ];
        for (key, value) in expect {
            assert_eq!(envs.get(key).map(String::as_str), Some(value), "env {key}");
        }
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
