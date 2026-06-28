// Zed manager — WebSocket connection, session management, and settings bootstrap

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::process::Command;
use tokio::sync::{RwLock, mpsc, watch, Notify};
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

/// Manages a single Zed WebSocket connection and message dispatch.
#[allow(dead_code)]
pub struct ZedManager {
    pub session_id: String,
    pub ws_host: String,
    pub zed_connected: bool,
    pub agent_ready: bool,
    /// Channel to send WebSocket commands to Zed
    pub ws_tx: Option<mpsc::UnboundedSender<String>>,
    /// Threads managed by this Zed instance
    pub threads: HashMap<String, ThreadSession>,
    /// Mapping from request_id to acp_thread_id (for correlating responses)
    pub pending_requests: HashMap<String, String>,
    /// Mapping from zed_thread_id to local_thread_id (for reverse lookup)
    pub thread_id_map: HashMap<String, String>,
    /// Path to the threads persistence file
    pub threads_file: PathBuf,
    /// Threads that have been activated (context sent) in the current Zed session
    pub threads_activated: HashSet<String>,
    /// Notifier for thread state changes (SSE consumers)
    pub thread_notify: watch::Sender<u64>,
    /// Waiters for threads whose acp_thread_id is being established
    pub thread_waiters: HashMap<String, Arc<Notify>>,
    /// Monotonically increasing reconnect counter. Incremented each time
    /// a new WS connection is established (for SSE consumers to detect).
    pub reconnect_count: u64,
    /// Timestamp of the last event received from the WebSocket.
    /// Used to detect stuck connections where Zed stops sending events.
    pub last_event_time: Instant,
    /// Pending chat messages that need to be re-sent after reconnection.
    /// Stores (request_id, thread_id, message) tuples.
    pub pending_chat_queue: Vec<(String, String, String)>,
}

/// A single conversation thread.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ThreadSession {
    pub id: String,
    /// Auto-generated from first user message (first 80 chars, "..." appended if truncated)
    pub title: Option<String>,
    pub messages: Vec<ThreadMessage>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// True when the last assistant response is complete (message_completed received)
    pub completed: bool,
    /// Zed-side ACP thread ID for continuing conversations across restarts
    pub acp_thread_id: Option<String>,
    /// Monotonically increasing turn counter. Each SSE stream waits for its
    /// turn to complete by watching thread.turn_completed >= its captured turn_id.
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

impl ZedManager {
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
            zed_connected: false,
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
            last_event_time: Instant::now(),
            pending_chat_queue: Vec::new(),
        }
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
    /// ACP threads are created by Zed and stored in thread_id_map.
    /// Returns None for new threads (Zed will create a fresh ACP thread).
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
            // Truncate very long messages to avoid token waste
            let truncated = if msg.len() > 2000 { &msg[..2000] } else { msg };
            ctx.push_str(truncated);
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
                if let Some(last) = thread.messages.last_mut() {
                    if last.message_id.as_deref() == Some(mid) {
                        last.content = content.to_string();
                        self.notify_thread_change();
                        self.save_threads();
                        return;
                    }
                }
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
                let truncated = if title.len() > 80 {
                    format!("{}...", &title[..80])
                } else {
                    title.to_string()
                };
                thread.title = Some(truncated);
                self.notify_thread_change();
                self.save_threads();
            }
        }
    }

    /// Notify SSE consumers that thread state has changed.
    pub fn notify_thread_change(&self) {
        let _ = self
            .thread_notify
            .send(self.thread_notify.borrow().wrapping_add(1));
    }

    /// Send a cancel_current_turn command to Zed via WebSocket.
    pub fn cancel_current_turn(&self) -> Result<(), String> {
        let cmd = serde_json::json!({
            "type": "cancel_current_turn",
            "data": {}
        });
        self.send_command(&cmd.to_string())
    }

    /// Persist all threads to the JSON file.
    pub fn save_threads(&self) {
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
                                let truncated = if content.len() > 80 {
                                    format!("{}...", &content[..80])
                                } else {
                                    content.to_string()
                                };
                                thread.title = Some(truncated);
                            }
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

    /// Send a JSON command to Zed via WebSocket. Returns error if not connected.
    pub fn send_command(&self, cmd: &str) -> Result<(), String> {
        match &self.ws_tx {
            Some(tx) => tx.send(cmd.to_string()).map_err(|e| e.to_string()),
            None => Err("WebSocket not connected".to_string()),
        }
    }
}

// ── WebSocket server (Zed connects to us) ──────────────────────────────

pub async fn run_ws_server(
    ws_host: &str,
    zed_manager: Arc<RwLock<ZedManager>>,
    ws_tx: crate::server::WsCommandTx,
) -> anyhow::Result<()> {
    let port = ws_host.split(':').nth(1).unwrap_or("8080");
    let listener = TcpListener::bind(&format!("127.0.0.1:{}", port)).await?;
    tracing::info!(
        "WebSocket server listening on ws://127.0.0.1:{}",
        listener.local_addr()?.port()
    );

    // Connection loop: accept → read → disconnect → accept again
    loop {
        let (stream, peer) = listener.accept().await?;
        tracing::info!("Zed connecting from {}", peer);

        let ws_stream = accept_async(stream).await?;
        let (mut write, mut read) = ws_stream.split();

        // Increment reconnect counter and set zed_connected
        {
            let mut mgr = zed_manager.write().await;
            mgr.reconnect_count += 1;
            mgr.zed_connected = true;
            mgr.agent_ready = false;
            tracing::info!(
                "Zed WebSocket connected (reconnect #{})",
                mgr.reconnect_count
            );
        }

        // Create channel for sending commands to Zed
        let (tx, mut rx) = mpsc::unbounded_channel::<String>();
        let tx_for_shared = tx.clone();

        // Channel for resending pending messages (must be created BEFORE
        // set_ws_tx so the pending resend can use the new channel)
        let (resend_tx, mut resend_rx) = mpsc::unbounded_channel::<String>();

        {
            let mut mgr = zed_manager.write().await;
            mgr.set_ws_tx(tx);
        }

        // Set the shared ws_tx for lock-free command sending
        {
            let mut tx_guard = ws_tx.lock().await;
            *tx_guard = Some(tx_for_shared);
        }
        tracing::info!("Zed WebSocket re-established");

        // Resend any pending chat messages that were queued before reconnection.
        // These are messages that were sent via SSE but never received a response
        // because the WebSocket connection dropped.
        {
            let mgr = zed_manager.read().await;
            for (rid, tid, cmd) in &mgr.pending_chat_queue {
                tracing::info!(
                    "Resending pending message request_id={}, thread_id={}",
                    &rid[..rid.len().min(12)],
                    &tid[..tid.len().min(12)]
                );
                if let Err(e) = resend_tx.send(cmd.clone()) {
                    tracing::error!("Failed to queue resend: {}", e);
                }
            }
        }

        // Merge resend_rx into the main write loop so both normal and
        // reconnection-resend commands go through the same writer.
        // rx is for normal chat_message commands, resend_rx is for
        // commands retried after a WebSocket reconnection.

        // Clone for the spawned tasks so the originals stay in the outer loop
        let zed_manager_for_read = zed_manager.clone();
        let ws_tx_for_read = ws_tx.clone();

        // Spawn write task: forward commands from both normal and
        // resend channels to WebSocket
        let write_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(cmd) = rx.recv() => {
                        if let Err(e) = write.send(Message::Text(cmd.into())).await {
                            tracing::error!("WebSocket write error: {}", e);
                            break;
                        }
                    },
                    Some(cmd) = resend_rx.recv() => {
                        if let Err(e) = write.send(Message::Text(cmd.into())).await {
                            tracing::error!("WebSocket resend error: {}", e);
                            break;
                        }
                    },
                }
            }
        });

        // Read loop with periodic health check
        let read_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    msg = read.next() => {
                        match msg {
                            Some(Ok(Message::Text(text))) => {
                                tracing::debug!("WS Text msg ({} bytes): {}", text.len(), &text[..text.len().min(300)]);
                                handle_zed_event(&zed_manager_for_read, &text).await;
                            }
                            Some(Ok(Message::Binary(data))) => {
                                tracing::debug!("WS Binary msg ({} bytes): {:?}", data.len(), &data[..data.len().min(100)]);
                                if let Ok(text) = String::from_utf8(data.to_vec()) {
                                    handle_zed_event(&zed_manager_for_read, &text).await;
                                } else {
                                    tracing::warn!("Received non-UTF-8 binary WebSocket message ({} bytes)", data.len());
                                }
                            }
                            Some(Ok(Message::Ping(_))) => {}
                            Some(Ok(Message::Close(_))) => {
                                tracing::info!("Zed WebSocket closed");
                                {
                                    let mut mgr = zed_manager_for_read.write().await;
                                    mgr.zed_connected = false;
                                }
                                {
                                    let mut guard = ws_tx_for_read.lock().await;
                                    *guard = None;
                                }
                                break;
                            }
                            Some(Err(e)) => {
                                tracing::error!("WebSocket read error: {}", e);
                                {
                                    let mut mgr = zed_manager_for_read.write().await;
                                    mgr.zed_connected = false;
                                }
                                {
                                    let mut guard = ws_tx_for_read.lock().await;
                                    *guard = None;
                                }
                                break;
                            }
                            None => {
                                tracing::info!("WebSocket stream ended");
                                {
                                    let mut mgr = zed_manager_for_read.write().await;
                                    mgr.zed_connected = false;
                                }
                                {
                                    let mut guard = ws_tx_for_read.lock().await;
                                    *guard = None;
                                }
                                break;
                            }
                            _ => {}
                        }
                    },
                    // Periodic health check: break if zed_connected was
                    // set to false by the health monitor task.
                    _ = tokio::time::sleep(Duration::from_millis(500)) => {
                        if !zed_manager_for_read.read().await.zed_connected {
                            break;
                        }
                    }
                }
            }
        });

        read_handle.await?;
        write_handle.abort();

        // Clear ws_tx on disconnect
        {
            let mut tx_guard = ws_tx.lock().await;
            *tx_guard = None;
        }

        tracing::info!("WS connection lost, waiting for next connection...");
    }
}

async fn handle_zed_event(zed_manager: &Arc<RwLock<ZedManager>>, text: &str) {
    tracing::debug!("WS event received: {}", &text[..text.len().min(200)]);

    let msg: serde_json::Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!("Failed to parse WS event as JSON: {} (text: {})", e, &text[..text.len().min(100)]);
            return;
        },
    };

    let event_type = msg.get("event_type").and_then(|v| v.as_str()).unwrap_or("");
    tracing::debug!("WS event type: '{}'", event_type);

    // Update last_event_time for health monitor
    {
        let mut mgr = zed_manager.write().await;
        mgr.last_event_time = Instant::now();
    }

    let data = msg
        .get("data")
        .and_then(|v| v.as_object())
        .cloned()
        .unwrap_or_default();

    match event_type {
        "ping" => {}
        "agent_ready" => {
            let mut mgr = zed_manager.write().await;
            mgr.agent_ready = true;
            tracing::info!(
                "Agent ready ({})",
                data.get("agent_name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("?")
            );
        }
        "thread_created" => {
            let acp_id = data
                .get("acp_thread_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let rid = data
                .get("request_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let mut mgr = zed_manager.write().await;
            tracing::info!("Thread created: {}", acp_id);
            // Look up the original local thread_id from pending_requests
            // and store the acp_thread_id on the local thread for persistence
            if let Some(local_id) = mgr.pending_requests.get(&rid).cloned() {
                mgr.thread_id_map.insert(acp_id.clone(), local_id.clone());
                if let Some(local_thread) = mgr.threads.get_mut(&local_id) {
                    local_thread.acp_thread_id = Some(acp_id.clone());
                }
                // Notify any waiter waiting for this thread's acp_thread_id
                if let Some(waiter) = mgr.thread_waiters.remove(&local_id) {
                    waiter.notify_one();
                }
            }
            mgr.get_or_create_thread(Some(&acp_id));
            mgr.pending_requests.insert(rid, acp_id);
            mgr.notify_thread_change();
            mgr.save_threads();
        }
        "message_added" => {
            let acp_id = data
                .get("acp_thread_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let content = data
                .get("content")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            tracing::debug!("message_added: acp_id={}, role={}, content_len={}",
                &acp_id[..acp_id.len().min(12)],
                data.get("role").and_then(|v| v.as_str()).unwrap_or("?"),
                content.len());
            let role = data
                .get("role")
                .and_then(|v| v.as_str())
                .unwrap_or("assistant")
                .to_string();
            let msg_id = data
                .get("message_id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let entry_type = data
                .get("entry_type")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let tool_name = data
                .get("tool_name")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let tool_status = data
                .get("tool_status")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let mut mgr = zed_manager.write().await;
            mgr.add_message_full(
                &acp_id,
                &role,
                &content,
                msg_id.clone(),
                entry_type.clone(),
                tool_name.clone(),
                tool_status.clone(),
            );
            // Mirror to the original local thread
            if let Some(local_id) = mgr.thread_id_map.get(&acp_id).cloned() {
                mgr.add_message_full(
                    &local_id,
                    &role,
                    &content,
                    msg_id,
                    entry_type,
                    tool_name,
                    tool_status,
                );
            }
            mgr.notify_thread_change();
            mgr.save_threads();
        }
        "message_completed" => {
            let acp_id = data
                .get("acp_thread_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let mut mgr = zed_manager.write().await;
            if let Some(thread) = mgr.threads.get_mut(&acp_id) {
                thread.completed = true;
                thread.turn_completed = thread.turn_completed.wrapping_add(1);
            }
            if let Some(local_id) = mgr.thread_id_map.get(&acp_id).cloned() {
                if let Some(thread) = mgr.threads.get_mut(&local_id) {
                    thread.completed = true;
                    thread.turn_completed = thread.turn_completed.wrapping_add(1);
                }
            }
            mgr.notify_thread_change();
            mgr.save_threads();
            tracing::info!(
                "Message complete for thread {}",
                &acp_id[..acp_id.len().min(12)]
            );
        }
        "chat_response_error" => {
            let error = data.get("error").and_then(|v| v.as_str()).unwrap_or("?");
            tracing::error!("Chat response error: {}", error);
        }
        _ => {
            tracing::debug!("Unhandled event: {}", event_type);
        }
    }
}

// ── Launch Zed headless ────────────────────────────────────────────────

pub async fn launch_zed(
    bin_path: &Path,
    workdir: &Path,
    user_data_dir: &Path,
    session_id: &str,
    ws_host: &str,
) -> anyhow::Result<tokio::process::Child> {
    tracing::info!("Launching Zed headless...");

    let stderr_log = std::fs::File::create("/tmp/-headless.log")?;
    let child = Command::new(bin_path)
        .args(["--headless", "--allow-multiple-instances"])
        .arg("--user-data-dir")
        .arg(user_data_dir)
        .arg(workdir)
        .env("ZED_EXTERNAL_SYNC_ENABLED", "true")
        .env("ZED_WEBSOCKET_SYNC_ENABLED", "true")
        .env("ZED_HELIX_URL", ws_host)
        .env("ZED_HELIX_TOKEN", "test-token")
        .env("HELIX_SESSION_ID", session_id)
        .env("ZED_STATELESS", "1")
        .env("ZED_WORK_DIR", workdir)
        .env("RUST_LOG", "info")
        .stdout(std::process::Stdio::null())
        .stderr(stderr_log)
        .spawn()?;

    tracing::info!("Zed started (PID: {:?})", child.id());
    Ok(child)
}

// ── Zed settings bootstrap ─────────────────────────────────────────────

pub fn ensure_zed_settings(
    data_dir: &Path,
    api_key: &str,
    provider: &str,
    base_url: &str,
    model_name: &str,
    model_display: &str,
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

    tracing::info!("Zed settings written to {}", settings_file.display());
    Ok(())
}
