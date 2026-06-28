// HTTP API server — axum-based REST endpoints

use axum::response::sse::Event;
use axum::{
    Router,
    extract::{Query, State},
    http::StatusCode,
    response::{Json, Sse},
    routing::{get, post},
};
use futures_util::stream::Stream;
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, Notify, mpsc};

use std::path::PathBuf;

use crate::files;
use crate::git;
use crate::zed::ZedManager;

/// Channel sender for WebSocket commands to Zed.
/// Shared between AppState and ZedManager so cancel can send
/// without acquiring the ZedManager RwLock (avoiding lock contention
/// with long-running SSE handlers).
pub type WsCommandTx = Arc<tokio::sync::Mutex<Option<mpsc::UnboundedSender<String>>>>;

// ── Helpers ─────────────────────────────────────────────────────────────

/// Prepare the user message, injecting conversation context if this thread
/// hasn't been activated in the current Zed session yet.
fn prepare_and_clear_acp(mgr: &mut ZedManager, thread_id: &str, user_message: &str) -> String {
    if !mgr.threads_activated.contains(thread_id) {
        // Clear stale acp_thread_id from previous sessions
        if let Some(thread) = mgr.threads.get_mut(thread_id) {
            thread.acp_thread_id = None;
        }
        mgr.thread_id_map.retain(|_, v| v != thread_id);
        if let Some(ctx) = mgr.format_conversation_context(thread_id) {
            return format!("{}\n\n{}", ctx, user_message);
        }
    }
    user_message.to_string()
}

// ── App State ──────────────────────────────────────────────────────────

pub struct AppState {
    pub zed_manager: Arc<RwLock<ZedManager>>,
    pub ws_tx: WsCommandTx,
    pub workdir: PathBuf,
}

impl AppState {
    pub fn new(zed_manager: Arc<RwLock<ZedManager>>, ws_tx: WsCommandTx, workdir: PathBuf) -> Self {
        Self {
            zed_manager,
            ws_tx,
            workdir,
        }
    }
}

type SharedState = Arc<AppState>;

// ── Models ─────────────────────────────────────────────────────────────

#[derive(Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub zed_connected: bool,
    pub agent_ready: bool,
    pub active_threads: usize,
}

#[derive(Serialize, Deserialize)]
pub struct ChatRequest {
    pub message: String,
    pub thread_id: Option<String>,
    #[serde(default)]
    pub require_approval: bool,
}

#[derive(Serialize)]
pub struct ChatResponse {
    pub task_id: String,
    pub status: String,
    pub thread_id: String,
}

#[derive(Serialize)]
#[allow(dead_code)]
pub struct TaskStatus {
    pub id: String,
    pub status: String,
    pub thread_id: String,
    pub message: String,
    pub created_at: String,
}

#[derive(Serialize)]
pub struct ThreadListResponse {
    pub threads: Vec<ThreadSummary>,
}

#[derive(Serialize)]
pub struct ThreadSummary {
    pub id: String,
    pub title: Option<String>,
    pub message_count: usize,
    pub created_at: String,
}

#[derive(Serialize)]
pub struct ThreadDetailResponse {
    pub id: String,
    pub title: Option<String>,
    pub messages: Vec<serde_json::Value>,
    pub created_at: String,
    pub completed: bool,
}

// ── Handlers ───────────────────────────────────────────────────────────

async fn health(State(state): State<SharedState>) -> Json<HealthResponse> {
    let mgr = state.zed_manager.read().await;
    Json(HealthResponse {
        status: "ok".to_string(),
        zed_connected: mgr.zed_connected,
        agent_ready: mgr.agent_ready,
        active_threads: mgr.threads.len(),
    })
}

/// Non-streaming async chat: submit and get a task_id + thread_id back.
async fn chat_async(
    State(state): State<SharedState>,
    Json(req): Json<ChatRequest>,
) -> Result<Json<ChatResponse>, StatusCode> {
    let mut mgr = state.zed_manager.write().await;

    if !mgr.zed_connected || !mgr.agent_ready {
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }

    let thread_id = mgr.get_or_create_thread(req.thread_id.as_deref());

    // Set title from the raw user message before context injection
    mgr.set_title(&thread_id, &req.message);

    // Enrich with context (for sending to Zed), but store raw message in thread
    let enriched = prepare_and_clear_acp(&mut mgr, &thread_id, &req.message);
    mgr.add_message(&thread_id, "user", &req.message, None);

    let request_id = uuid::Uuid::new_v4().to_string();

    let acp_id = mgr.get_acp_thread_id(&thread_id);

    let cmd = serde_json::json!({
        "type": "chat_message",
        "data": {
            "message": enriched,
            "request_id": request_id,
            "acp_thread_id": acp_id,
        }
    });

    mgr.pending_requests
        .insert(request_id.clone(), thread_id.clone());
    drop(mgr);

    send_ws_command(&state.ws_tx, &cmd.to_string()).await?;

    // Mark thread as activated
    {
        let mut mgr = state.zed_manager.write().await;
        mgr.threads_activated.insert(thread_id.clone());
    }

    Ok(Json(ChatResponse {
        task_id: uuid::Uuid::new_v4().to_string(),
        status: "approved".to_string(),
        thread_id,
    }))
}

/// Streaming chat: POST /v1/chat returns SSE events until complete.
async fn chat_stream(
    State(state): State<SharedState>,
    Json(req): Json<ChatRequest>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, StatusCode> {
    let thread_id;
    let request_id;
    let message: String;
    let is_new;

    // Acquire write lock, create thread, send command, then release.
    {
        let mut mgr = state.zed_manager.write().await;
        if !mgr.zed_connected || !mgr.agent_ready {
            return Err(StatusCode::SERVICE_UNAVAILABLE);
        }

        thread_id = mgr.get_or_create_thread(req.thread_id.as_deref());

        // Track whether this is a new thread (no prior messages)
        is_new = mgr.threads.get(&thread_id).map_or(true, |t| t.messages.is_empty());

        // Set title from the raw user message before context injection
        mgr.set_title(&thread_id, &req.message);

        // Enrich with context (for sending to Zed), but store raw message in thread
        message = prepare_and_clear_acp(&mut mgr, &thread_id, &req.message);
        mgr.add_message(&thread_id, "user", &req.message, None);

        request_id = uuid::Uuid::new_v4().to_string();
        mgr.pending_requests
            .insert(request_id.clone(), thread_id.clone());
    }

    // Send the WS command FIRST (acp_thread_id may be null for new threads)
    let acp_id = {
        let mgr = state.zed_manager.read().await;
        mgr.get_acp_thread_id(&thread_id)
    };

    send_ws_command(
        &state.ws_tx,
        &serde_json::json!({
            "type": "chat_message",
            "data": {
                "message": message,
                "request_id": request_id,
                "acp_thread_id": acp_id,
            }
        })
        .to_string(),
    )
    .await?;

    // If this is a resumed thread waiting for acp_thread_id establishment,
    // wait for the thread_created notification from the Zed WebSocket
    let is_resume_wait = !is_new && acp_id.is_none() && {
        let mgr = state.zed_manager.read().await;
        mgr.threads_activated.contains(&thread_id)
    };
    if is_resume_wait {
        let waiter = Arc::new(Notify::new());
        {
            let mut mgr = state.zed_manager.write().await;
            mgr.thread_waiters.insert(thread_id.clone(), waiter.clone());
        }
        tokio::select! {
            _ = waiter.notified() => {},
            _ = tokio::time::sleep(Duration::from_secs(20)) => {},
        }
    }

    // Mark thread as activated (after send + wait to avoid false wait triggers)
    {
        let mut mgr = state.zed_manager.write().await;
        mgr.threads_activated.insert(thread_id.clone());
    }

    // Build SSE stream: poll with backoff, using the watch channel for notification.
    // Each SSE stream captures the current turn_id and waits for
    // thread.turn_completed > turn_id, so multiple SSE streams on
    // the same thread don't interfere with each other.
    let state_clone = state.clone();
    let tid = thread_id.clone();
    let turn_id = {
        let mgr = state_clone.zed_manager.read().await;
        mgr.threads.get(&tid).map(|t| t.turn_completed).unwrap_or(0)
    };

    let stream = async_stream::stream! {
        let event_name = if is_new { "thread_created" } else { "thread_resumed" };
        yield Ok(Event::default()
            .event(event_name)
            .data(serde_json::to_string(&serde_json::json!({
                "thread_id": tid.clone(),
            })).unwrap()));

        // When resuming, start last_content at the current assistant content
        // so we only emit new deltas, not old messages.
        let mut last_content = {
            let mgr_b = state_clone.zed_manager.read().await;
            mgr_b.threads.get(&tid).and_then(|t| {
                t.messages
                    .iter()
                    .rev()
                    .find(|m| m.role == "assistant")
                    .map(|m| m.content.clone())
            }).unwrap_or_default()
        };
        let mut done = false;
        let mut rx = state_clone.zed_manager.read().await.thread_notify.subscribe();

        while !done {
            // Wait for notification or poll at 100ms intervals
            tokio::select! {
                _ = rx.changed() => {},
                _ = tokio::time::sleep(Duration::from_millis(100)) => {},
            }

            let mgr = state_clone.zed_manager.read().await;
            let thread = mgr.threads.get(&tid);

            if let Some(thread) = thread {
                // Find last assistant message (with tool metadata)
                let last_msg = thread
                    .messages
                    .iter()
                    .rev()
                    .find(|m| m.role == "assistant");
                let assistant_content = last_msg.map(|m| m.content.as_str()).unwrap_or("");
                let entry_type = last_msg.and_then(|m| m.entry_type.as_deref());
                let tool_name = last_msg.and_then(|m| m.tool_name.as_deref());
                let tool_status = last_msg.and_then(|m| m.tool_status.as_deref());

                // Yield delta
                if assistant_content.len() > last_content.len() {
                    let delta = &assistant_content[last_content.len()..];
                    last_content = assistant_content.to_string();

                    yield Ok(Event::default()
                        .event("message_added")
                        .data(serde_json::to_string(&serde_json::json!({
                            "thread_id": tid.clone(),
                            "content": delta,
                            "entry_type": entry_type,
                            "tool_name": tool_name,
                            "tool_status": tool_status,
                        })).unwrap()));
                }

                // Check completion: wait for the turn we started
                if thread.turn_completed > turn_id {
                    yield Ok(Event::default()
                        .event("message_completed")
                        .data(serde_json::to_string(&serde_json::json!({
                            "thread_id": tid.clone(),
                        })).unwrap()));
                    done = true;
                }
            }

            drop(mgr);
        }
    };

    Ok(Sse::new(stream))
}

async fn list_threads(State(state): State<SharedState>) -> Json<ThreadListResponse> {
    let mgr = state.zed_manager.read().await;
    let mut threads: Vec<ThreadSummary> = mgr
        .threads
        .values()
        .map(|t| ThreadSummary {
            id: t.id.clone(),
            title: t.title.clone(),
            message_count: t.messages.len(),
            created_at: t.created_at.to_rfc3339(),
        })
        .collect();
    threads.sort_by(|a, b| b.created_at.cmp(&a.created_at));
    Json(ThreadListResponse { threads })
}

async fn get_thread(
    State(state): State<SharedState>,
    axum::extract::Path(thread_id): axum::extract::Path<String>,
) -> Result<Json<ThreadDetailResponse>, StatusCode> {
    let mgr = state.zed_manager.read().await;
    match mgr.threads.get(&thread_id) {
        Some(thread) => {
            let messages: Vec<serde_json::Value> = thread
                .messages
                .iter()
                .map(|m| {
                    serde_json::json!({
                        "role": m.role,
                        "content": m.content,
                        "message_id": m.message_id,
                        "entry_type": m.entry_type,
                        "tool_name": m.tool_name,
                        "tool_status": m.tool_status,
                        "timestamp": m.timestamp.to_rfc3339(),
                    })
                })
                .collect();
            Ok(Json(ThreadDetailResponse {
                id: thread.id.clone(),
                title: thread.title.clone(),
                messages,
                created_at: thread.created_at.to_rfc3339(),
                completed: thread.completed,
            }))
        }
        None => Err(StatusCode::NOT_FOUND),
    }
}

// ── File search endpoints ──────────────────────────────────────────────

#[derive(Deserialize)]
pub struct FileQuery {
    q: Option<String>,
    dir: Option<String>,
    #[serde(default = "default_max_results")]
    max: usize,
}

fn default_max_results() -> usize {
    100
}

#[derive(Serialize)]
pub struct FileSearchResponse {
    pub files: Vec<files::FileEntry>,
    pub count: usize,
    pub truncated: bool,
}

/// GET /v1/files — search files in the project workspace.
async fn search_files_handler(
    State(state): State<SharedState>,
    params: Query<FileQuery>,
) -> Json<FileSearchResponse> {
    let opts = files::FileSearchOptions {
        query: params.q.clone().unwrap_or_default(),
        dir: params.dir.clone(),
        max_results: params.max,
        ..Default::default()
    };
    let results = files::search_files(&state.workdir, &opts);
    let max = if opts.max_results > 0 {
        opts.max_results
    } else {
        100
    };
    let truncated = results.len() > max;
    let files: Vec<_> = results.into_iter().take(max).collect();
    let count = files.len();
    Json(FileSearchResponse {
        files,
        count,
        truncated,
    })
}

/// GET /v1/files/mention — format results as a mention string for chat.
async fn mention_files_handler(
    State(state): State<SharedState>,
    params: Query<FileQuery>,
) -> Json<serde_json::Value> {
    let query = params.q.clone().unwrap_or_default();
    let opts = files::FileSearchOptions {
        query: query.clone(),
        dir: params.dir.clone(),
        max_results: 10,
        ..Default::default()
    };
    let results = files::search_files(&state.workdir, &opts);
    let mention = files::format_mention(&results, &query);
    Json(serde_json::json!({
        "mention": mention,
        "count": results.len(),
    }))
}

// ── WebSocket command sender ───────────────────────────────────────────

async fn send_ws_command(
    ws_tx: &WsCommandTx,
    cmd: &str,
) -> Result<(), StatusCode> {
    let tx_guard = ws_tx.lock().await;
    match &*tx_guard {
        Some(tx) => tx.send(cmd.to_string()).map_err(|e| {
            tracing::error!("Failed to send WS command: {}", e);
            StatusCode::SERVICE_UNAVAILABLE
        }),
        None => {
            tracing::error!("Cannot send WS command: not connected");
            Err(StatusCode::SERVICE_UNAVAILABLE)
        }
    }
}

// ── Git endpoints ──────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct GitLogQuery {
    #[serde(default = "default_git_log_count")]
    max: usize,
}

fn default_git_log_count() -> usize { 10 }

#[derive(Deserialize)]
pub struct GitDiffQuery {
    #[serde(default)]
    staged: bool,
}

/// GET /v1/git/status — git working tree status.
async fn git_status(
    State(state): State<SharedState>,
) -> Json<serde_json::Value> {
    match git::get_status(&state.workdir) {
        Ok(Some(status)) => Json(serde_json::json!({"ok": true, "status": status})),
        Ok(None) => Json(serde_json::json!({"ok": false, "error": "Not a git repository"})),
        Err(e) => Json(serde_json::json!({"ok": false, "error": e})),
    }
}

/// GET /v1/git/diff — git diff (unstaged by default, ?staged=true for staged).
async fn git_diff(
    State(state): State<SharedState>,
    params: Query<GitDiffQuery>,
) -> Json<serde_json::Value> {
    match git::get_diff(&state.workdir, params.staged) {
        Ok(Some(diff)) => Json(serde_json::json!({"ok": true, "diff": diff})),
        Ok(None) => Json(serde_json::json!({"ok": false, "error": "Not a git repository"})),
        Err(e) => Json(serde_json::json!({"ok": false, "error": e})),
    }
}

/// GET /v1/git/log — recent commit history.
async fn git_log(
    State(state): State<SharedState>,
    params: Query<GitLogQuery>,
) -> Json<serde_json::Value> {
    match git::get_log(&state.workdir, params.max) {
        Ok(Some(commits)) => Json(serde_json::json!({"ok": true, "commits": commits})),
        Ok(None) => Json(serde_json::json!({"ok": false, "error": "Not a git repository"})),
        Err(e) => Json(serde_json::json!({"ok": false, "error": e})),
    }
}

// ── Cancel endpoint ────────────────────────────────────────────────────

async fn cancel_turn(
    State(state): State<SharedState>,
) -> Json<serde_json::Value> {
    let tx_guard = state.ws_tx.lock().await;
    match &*tx_guard {
        Some(tx) => {
            let cmd = serde_json::json!({
                "type": "cancel_current_turn",
                "data": {}
            });
            if tx.send(cmd.to_string()).is_ok() {
                Json(serde_json::json!({"status": "cancelled"}))
            } else {
                tracing::error!("Cancel failed: WebSocket channel closed");
                Json(serde_json::json!({"status": "error", "error": "WebSocket not connected"}))
            }
        }
        None => {
            Json(serde_json::json!({"status": "error", "error": "WebSocket not connected"}))
        }
    }
}

// ── Router ─────────────────────────────────────────────────────────────

pub async fn run_http_server(addr: &str, state: SharedState) -> anyhow::Result<()> {
    let app = Router::new()
        .route("/health", get(health))
        .route("/v1/chat", post(chat_stream))
        .route("/v1/chat/async", post(chat_async))
        .route("/v1/cancel", post(cancel_turn))
        .route("/v1/threads", get(list_threads))
        .route("/v1/threads/{thread_id}", get(get_thread))
        .route("/v1/files", get(search_files_handler))
        .route("/v1/files/mention", get(mention_files_handler))
        .route("/v1/git/status", get(git_status))
        .route("/v1/git/diff", get(git_diff))
        .route("/v1/git/log", get(git_log))
        .layer(tower_http::cors::CorsLayer::permissive())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("HTTP API server listening on http://{}", addr);

    axum::serve(listener, app).await?;
    Ok(())
}
