// Telos management — process lifecycle, WebSocket bridge, and protocol types.

pub mod backend;
pub mod control;
pub mod types;

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
use tokio::process::Command;
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

pub async fn launch_telos(
    bin_path: &Path,
    workdir: &Path,
    user_data_dir: &Path,
    session_id: &str,
    ws_host: &str,
    tool_approval: &str,
    stderr_log: &Path,
) -> anyhow::Result<tokio::process::Child> {
    tracing::info!("Launching telos...");

    let stderr_log = std::fs::File::create(stderr_log)
        .map_err(|e| anyhow::anyhow!("cannot create stderr log {}: {}", stderr_log.display(), e))?;
    let child = Command::new(bin_path)
        .args(["--headless", "--allow-multiple-instances"])
        .arg("--user-data-dir")
        .arg(user_data_dir)
        .arg(workdir)
        .env("TELOS_EXTERNAL_SYNC_ENABLED", "true")
        .env("TELOS_WEBSOCKET_SYNC_ENABLED", "true")
        .env("TELOS_WS_URL", ws_host)
        .env("TELOS_WS_TOKEN", "test-token")
        .env("TELOS_STATELESS", "1")
        .env("TELOS_SESSION_ID", session_id)
        .env("TELOS_TOOL_APPROVAL", tool_approval)
        .env("RUST_LOG", "info")
        .stdout(std::process::Stdio::null())
        .stderr(stderr_log)
        .spawn()
        .map_err(|e| anyhow::anyhow!("cannot spawn {}: {}", bin_path.display(), e))?;

    tracing::info!("telos started (PID: {:?})", child.id());
    Ok(child)
}

// ── Telos settings bootstrap ─────────────────────────────────────────────

pub fn ensure_telos_settings(
    data_dir: &Path,
    api_key: &str,
    provider: &str,
    base_url: &str,
    model_name: &str,
    model_display: &str,
    mcp: &[crate::agent::config::McpServer],
) -> anyhow::Result<()> {
    use std::fs;
    use std::io::Write;

    let settings_dir = data_dir.join("config");
    fs::create_dir_all(&settings_dir)?;
    let settings_file = settings_dir.join("settings.json");

    let mut settings: serde_json::Value = if settings_file.exists() {
        serde_json::from_str(&fs::read_to_string(&settings_file)?)?
    } else {
        serde_json::json!({})
    };

    if settings
        .get("language_models")
        .and_then(|lm| lm.get("openai_compatible"))
        .and_then(|oc| oc.get(provider))
        .is_none()
    {
        settings["language_models"]["openai_compatible"][provider] = serde_json::json!({
            "api_url": base_url,
            "available_models": [{
                "name": model_name,
                "display_name": model_display,
                "max_tokens": 65536,
                "max_output_tokens": 8192,
                "tool_use": true,
            }],
        });
    }

    // MCP servers: map each declaration to Telos's `context_servers` entry.
    // Stdio servers become `{ command, args, env }`; HTTP servers become
    // `{ url, headers }`. The headless agent's context server registry
    // starts these and exposes their tools to the model.
    if !mcp.is_empty() {
        let mut servers = serde_json::Map::new();
        for s in mcp {
            let content = if let Some(url) = &s.url {
                let mut obj = serde_json::Map::new();
                obj.insert("url".to_string(), serde_json::json!(url));
                if !s.headers.is_empty() {
                    obj.insert("headers".to_string(), serde_json::json!(s.headers));
                }
                serde_json::Value::Object(obj)
            } else {
                let mut obj = serde_json::Map::new();
                if let Some(cmd) = &s.command {
                    obj.insert("command".to_string(), serde_json::json!(cmd));
                }
                if !s.args.is_empty() {
                    obj.insert("args".to_string(), serde_json::json!(s.args));
                }
                if !s.env.is_empty() {
                    obj.insert("env".to_string(), serde_json::json!(s.env));
                }
                if let Some(t) = s.timeout {
                    obj.insert("timeout".to_string(), serde_json::json!(t));
                }
                serde_json::Value::Object(obj)
            };
            servers.insert(s.name.clone(), content);
        }
        settings["context_servers"] = serde_json::Value::Object(servers);
        tracing::info!("Wrote {} MCP server(s) to settings", mcp.len());
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
