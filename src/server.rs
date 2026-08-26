// HTTP API server — axum-based REST endpoints

use axum::response::sse::Event;
use axum::{
    Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{Json, Sse},
    routing::{get, post},
};
use futures_util::stream::Stream;
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use std::path::PathBuf;

use crate::agent::{AgentBackend, AgentRegistry, AgentStatus};
use crate::files;
use crate::git;
pub use crate::zed::WsCommandTx;

// ── App State ──────────────────────────────────────────────────────────

pub struct AppState {
    /// Running agent backends. Handlers talk only to the default agent
    /// through the `AgentBackend` trait until per-agent routing lands.
    pub agents: AgentRegistry,
    pub ws_tx: WsCommandTx,
    pub workdir: PathBuf,
}

impl AppState {
    pub fn new(agents: AgentRegistry, ws_tx: WsCommandTx, workdir: PathBuf) -> Self {
        Self {
            agents,
            ws_tx,
            workdir,
        }
    }
}

type SharedState = Arc<AppState>;

/// Default agent serving the chat and thread endpoints.
async fn default_agent(state: &SharedState) -> Result<Arc<dyn AgentBackend>, StatusCode> {
    state
        .agents
        .default_agent()
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)
}

// ── Models ─────────────────────────────────────────────────────────────

#[derive(Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub zed_connected: bool,
    pub agent_ready: bool,
    pub active_threads: usize,
    /// Per-agent runtime state from the execution fabric.
    pub agents: Vec<AgentStatus>,
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
    pub turn_completed: u64,
}

// ── Handlers ───────────────────────────────────────────────────────────

async fn health(State(state): State<SharedState>) -> Json<HealthResponse> {
    let agents = state.agents.statuses().await;
    let (zed_connected, agent_ready, active_threads) = match state.agents.default_agent() {
        Some(agent) => {
            let status = agent.status().await;
            let threads = agent.threads().await.len();
            (status.connected, status.ready, threads)
        }
        None => (false, false, 0),
    };
    Json(HealthResponse {
        status: "ok".to_string(),
        zed_connected,
        agent_ready,
        active_threads,
        agents,
    })
}

/// Non-streaming async chat: submit and get a task_id + thread_id back.
async fn chat_async(
    State(state): State<SharedState>,
    Json(req): Json<ChatRequest>,
) -> Result<Json<ChatResponse>, StatusCode> {
    let agent = default_agent(&state).await?;
    let receipt = agent
        .submit(req.thread_id.as_deref(), &req.message)
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;

    Ok(Json(ChatResponse {
        task_id: uuid::Uuid::new_v4().to_string(),
        status: "approved".to_string(),
        thread_id: receipt.thread_id,
    }))
}

/// Streaming chat: POST /v1/chat returns SSE events until complete.
async fn chat_stream(
    State(state): State<SharedState>,
    Json(req): Json<ChatRequest>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, StatusCode> {
    let agent = default_agent(&state).await?;

    // Submit through the fabric: thread creation, context injection,
    // command send, and the resume wait happen inside the adapter.
    let receipt = agent
        .submit(req.thread_id.as_deref(), &req.message)
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;

    let tid = receipt.thread_id.clone();
    let is_new = receipt.is_new;
    // Build SSE stream: poll with backoff, using the watch channel for
    // notification. Each SSE stream captures the current turn_id and
    // waits for thread.turn_completed > turn_id, so multiple SSE streams
    // on the same thread don't interfere with each other.
    let turn_id = agent
        .thread(&tid)
        .await
        .map(|t| t.turn_completed)
        .unwrap_or(0);

    tracing::debug!(
        "chat_stream: SSE stream created for thread {} (turn_id={}, new={})",
        tid, turn_id, is_new
    );
    let agent_stream = agent.clone();
    let stream = async_stream::stream! {
        tracing::debug!("SSE stream starting for thread {} (turn_id={}, new={})", tid, turn_id, is_new);
        let event_name = if is_new { "thread_created" } else { "thread_resumed" };
        yield Ok(Event::default()
            .event(event_name)
            .data(serde_json::to_string(&serde_json::json!({
                "thread_id": tid.clone(),
            })).unwrap()));

        // When resuming, start last_content at the current assistant content
        // so we only emit new deltas, not old messages.
        let mut last_content = agent_stream
            .thread(&tid)
            .await
            .and_then(|t| {
                t.messages
                    .iter()
                    .rev()
                    .find(|m| m.role == "assistant")
                    .map(|m| m.content.clone())
            })
            .unwrap_or_default();
        let mut done = false;
        let mut rx = agent_stream.subscribe().await;

        let mut poll_count = 0u64;
        let start = std::time::Instant::now();
        let max_wait = Duration::from_secs(120); // 2 min max before timeout
        while !done {
            // Wait for notification or poll at 100ms intervals
            tokio::select! {
                _ = rx.changed() => {
                    tracing::debug!("SSE stream {}: notification received (poll #{})", tid, poll_count);
                },
                _ = tokio::time::sleep(Duration::from_millis(100)) => {},
            }

            // Log every 10th poll so we can see the loop is alive
            if poll_count % 10 == 0 {
                tracing::debug!("SSE pool {}: iter #{}", tid, poll_count);
            }

            let thread = agent_stream.thread(&tid).await;

            if let Some(thread) = thread {
                let msg_count = thread.messages.len();
                let turn_comp = thread.turn_completed;
                let last_assistant = thread
                    .messages
                    .iter()
                    .rev()
                    .find(|m| m.role == "assistant")
                    .map(|m| (m.content.len(), m.entry_type.as_deref().unwrap_or("")));

                if poll_count % 100 == 0 || msg_count > 1 {
                    tracing::debug!(
                        "SSE pool {}: msgs={}, turn={}/{}, last_assistant_len={:?}, last_content_len={}",
                        poll_count, msg_count, turn_comp, turn_id, last_assistant, last_content.len()
                    );
                }

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

                    tracing::debug!("SSE stream {}: yielding {} bytes delta", tid, delta.len());
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
                    tracing::debug!("SSE stream {}: turn completed ({} > {})", tid, thread.turn_completed, turn_id);
                    yield Ok(Event::default()
                        .event("message_completed")
                        .data(serde_json::to_string(&serde_json::json!({
                            "thread_id": tid.clone(),
                        })).unwrap()));
                    done = true;
                }
            } else {
                if poll_count % 50 == 0 {
                    tracing::debug!("SSE stream {}: thread not found after {} polls", tid, poll_count);
                }
            }

            poll_count += 1;

            // Timeout: if no response within max_wait, emit a timeout event
            if start.elapsed() > max_wait {
                tracing::warn!("SSE stream {}: timeout after {}s ({} polls)", tid, max_wait.as_secs(), poll_count);
                yield Ok(Event::default()
                    .event("error")
                    .data(serde_json::to_string(&serde_json::json!({
                        "thread_id": tid.clone(),
                        "error": "timeout: no response from agent",
                    })).unwrap()));
                done = true;
            }
        }
    };

    Ok(Sse::new(stream))
}

async fn list_threads(State(state): State<SharedState>) -> Json<ThreadListResponse> {
    let mut threads: Vec<ThreadSummary> = Vec::new();
    if let Ok(agent) = default_agent(&state).await {
        let sessions = agent.threads().await;
        threads = sessions
            .iter()
            .map(|t| ThreadSummary {
                id: t.id.clone(),
                title: t.title.clone(),
                message_count: t.messages.len(),
                created_at: t.created_at.to_rfc3339(),
            })
            .collect();
        threads.sort_by(|a, b| b.created_at.cmp(&a.created_at));
    }
    Json(ThreadListResponse { threads })
}

async fn get_thread(
    State(state): State<SharedState>,
    Path(thread_id): Path<String>,
) -> Result<Json<ThreadDetailResponse>, StatusCode> {
    let agent = default_agent(&state).await?;
    match agent.thread(&thread_id).await {
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
                turn_completed: thread.turn_completed,
            }))
        }
        None => Err(StatusCode::NOT_FOUND),
    }
}

// ── Thread polling endpoint ────────────────────────────────────────────

#[derive(Deserialize)]
pub struct PollQuery {
    /// The content length the client already has for the latest assistant message.
    /// New content beyond this length is returned as the delta.
    since: Option<usize>,
    /// The turn_completed value the client last observed.
    /// If omitted, defaults to 0. Poll returns `completed: true` when
    /// thread.turn_completed exceeds this value (i.e., a new turn finished).
    turn: Option<u64>,
}

#[derive(Serialize)]
pub struct PollResponse {
    /// Delta content since the last known content length, if any.
    pub new_content: Option<String>,
    /// Whether the thread's current turn is complete.
    pub completed: bool,
    /// Total content length of the latest assistant message (for the next poll).
    pub content_len: usize,
}

/// Poll for new thread state.
///
/// Alternative to SSE for clients that cannot maintain a persistent connection
/// or when WebSocket events from Zed are unreliable. The client calls this
/// endpoint at regular intervals (e.g., every 500ms), passing `since` as the
/// content length returned by the previous response. If the assistant message
/// has grown, the delta is returned in `new_content`.
async fn poll_thread(
    State(state): State<SharedState>,
    axum::extract::Path(thread_id): axum::extract::Path<String>,
    Query(query): Query<PollQuery>,
) -> Result<Json<PollResponse>, StatusCode> {
    let agent = default_agent(&state).await?;
    let thread = agent.thread(&thread_id).await.ok_or(StatusCode::NOT_FOUND)?;

    let since = query.since.unwrap_or(0);
    let known_turn = query.turn.unwrap_or(0);

    // Find the latest assistant message for delta computation
    let last_msg = thread.messages.iter().rev().find(|m| m.role == "assistant");

    match last_msg {
        Some(msg) if msg.content.len() > since => {
            let delta = msg.content[since..].to_string();
            Ok(Json(PollResponse {
                new_content: Some(delta),
                completed: thread.turn_completed > known_turn,
                content_len: msg.content.len(),
            }))
        }
        Some(msg) => {
            // No new content; report completion or no-change
            Ok(Json(PollResponse {
                new_content: None,
                completed: thread.turn_completed > known_turn,
                content_len: msg.content.len(),
            }))
        }
        None => {
            // No assistant message exists yet
            Ok(Json(PollResponse {
                new_content: None,
                completed: thread.turn_completed > known_turn,
                content_len: 0,
            }))
        }
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
        .route("/v1/threads/{thread_id}/poll", get(poll_thread))
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
