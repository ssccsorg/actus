// HTTP API server — axum-based REST endpoints

use axum::response::sse::Event;
use axum::{
    extract::{Path, Query, Request, State},
    http::{header, HeaderValue, Method, StatusCode},
    middleware::{self, Next},
    response::{Json, Response, Sse},
    routing::{get, post},
    Router,
};
use futures_util::stream::Stream;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use std::path::PathBuf;

use crate::agent::{AgentBackend, AgentRegistry, AgentStatus};
use crate::context;
use crate::files;
use crate::git;
pub use crate::telos::WsCommandTx;

// ── App State ──────────────────────────────────────────────────────────

pub struct AppState {
    /// Running agent backends. Handlers route by agent name or fall back
    /// to the default agent.
    pub agents: AgentRegistry,
    pub workdir: PathBuf,
    /// Bearer token required on every route except /health. None disables
    /// authentication.
    pub api_token: Option<String>,
}

impl AppState {
    pub fn new(agents: AgentRegistry, workdir: PathBuf, api_token: Option<String>) -> Self {
        Self {
            agents,
            workdir,
            api_token,
        }
    }
}

/// Bearer-token gate for every route except /health. Byte comparison is
/// constant-time for equal-length tokens; a length mismatch returns
/// immediately, so the length of a presented token is observable. Tokens
/// are generated with a fixed length, so this leaks nothing useful.
async fn require_auth(
    State(state): State<SharedState>,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    // Readiness probes (runner.py, run.sh) hit /health before a token is
    // available; it exposes only connectivity booleans and agent names.
    if req.uri().path() == "/health" {
        return Ok(next.run(req).await);
    }
    let Some(token) = &state.api_token else {
        return Ok(next.run(req).await);
    };
    let provided = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .ok_or(StatusCode::UNAUTHORIZED)?;
    if !constant_time_eq(provided.as_bytes(), token.as_bytes()) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok(next.run(req).await)
}

/// Compare byte strings without early exit on a mismatching byte. Lengths
/// are compared first; an equal-length comparison then runs in time
/// proportional to the length regardless of content.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b) {
        diff |= x ^ y;
    }
    diff == 0
}

pub type SharedState = Arc<AppState>;

/// Default agent serving endpoints that do not name an agent.
async fn default_agent(state: &SharedState) -> Result<Arc<dyn AgentBackend>, StatusCode> {
    state
        .agents
        .default_agent()
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)
}

/// Resolve the requested agent, falling back to the default when no name
/// is given. Unknown names yield 404.
async fn agent_for(
    state: &SharedState,
    name: Option<&str>,
) -> Result<Arc<dyn AgentBackend>, StatusCode> {
    match name {
        Some(n) if !n.is_empty() => state.agents.get(n).ok_or(StatusCode::NOT_FOUND),
        _ => default_agent(state).await,
    }
}

// ── Models ─────────────────────────────────────────────────────────────

#[derive(Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub telos_connected: bool,
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
    /// Agent name to route to; defaults to the fabric default agent.
    #[serde(default)]
    pub agent: Option<String>,
}

#[derive(Serialize)]
pub struct ChatResponse {
    pub task_id: String,
    pub status: String,
    pub thread_id: String,
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

#[derive(Deserialize)]
pub struct AgentQuery {
    pub agent: Option<String>,
}

async fn health(State(state): State<SharedState>) -> Json<HealthResponse> {
    let agents = state.agents.statuses().await;
    let (telos_connected, agent_ready, active_threads) = match state.agents.default_agent() {
        Some(agent) => {
            let status = agent.status().await;
            let threads = agent.threads().await.len();
            (status.connected, status.ready, threads)
        }
        None => (false, false, 0),
    };
    Json(HealthResponse {
        status: "ok".to_string(),
        telos_connected,
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
    let agent = agent_for(&state, req.agent.as_deref()).await?;
    let receipt = agent
        .submit(req.thread_id.as_deref(), &req.message)
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;

    Ok(Json(ChatResponse {
        task_id: uuid::Uuid::new_v4().to_string(),
        // The message is accepted for execution, not yet approved; the
        // hardcoded "approved" from the earlier approval design was
        // misleading because approval only applies in ask mode.
        status: "submitted".to_string(),
        thread_id: receipt.thread_id,
    }))
}

/// Streaming chat: POST /v1/chat returns SSE events until complete.
async fn chat_stream(
    State(state): State<SharedState>,
    Json(req): Json<ChatRequest>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, StatusCode> {
    let agent = agent_for(&state, req.agent.as_deref()).await?;

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
        tid,
        turn_id,
        is_new
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

        // The most recent assistant message is the only source of deltas.
        // A turn emits several messages (thinking, tool calls, answer), so
        // index-by-turn lookup is not possible; seeding last_content from
        // the latest message means a resumed thread never re-emits old
        // content.
        let mut last_content = agent_stream
            .thread(&tid)
            .await
            .map(|t| {
                t.messages
                    .iter()
                    .rev()
                    .find(|m| m.role == "assistant")
                    .map(|m| m.content.clone())
                    .unwrap_or_default()
            })
            .unwrap_or_default();
        let mut done = false;
        let mut rx = agent_stream.subscribe().await;

        let mut poll_count = 0u64;
        let start = std::time::Instant::now();
        // Emit a ping event after long periods of silence so intermediaries
        // do not drop the stream while the agent is thinking.
        let mut last_emit = std::time::Instant::now();
        // Long turns (code review, multi-tool research) take minutes; the
        // stream ends on turn completion, so this is a stuck-agent safety
        // ceiling, not the expected path. 30 minutes matches the CLI poll.
        let max_wait = Duration::from_secs(1800); // 30 min max before timeout
        while !done {
            // Wait for notification or poll at 100ms intervals
            tokio::select! {
                _ = rx.changed() => {
                    tracing::debug!("SSE stream {}: notification received (poll #{})", tid, poll_count);
                },
                _ = tokio::time::sleep(Duration::from_millis(100)) => {},
            }

            // Log every 10th poll so we can see the loop is alive
            if poll_count.is_multiple_of(10) {
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

                if poll_count.is_multiple_of(100) || msg_count > 1 {
                    tracing::debug!(
                        "SSE pool {}: msgs={}, turn={}/{}, last_assistant_len={:?}, last_content_len={}",
                        poll_count, msg_count, turn_comp, turn_id, last_assistant, last_content.len()
                    );
                }

                // Find the most recent assistant message (with tool metadata).
                let last_msg = thread
                    .messages
                    .iter()
                    .rev()
                    .find(|m| m.role == "assistant");
                let assistant_content = last_msg.map(|m| m.content.as_str()).unwrap_or("");
                let entry_type = last_msg.and_then(|m| m.entry_type.as_deref());
                let tool_name = last_msg.and_then(|m| m.tool_name.as_deref());
                let tool_status = last_msg.and_then(|m| m.tool_status.as_deref());

                // Yield delta. Robust to model edits: when the message
                // content does not extend the previously emitted content
                // (rewritten mid-stream), emit the full new content
                // instead of an invalid slice.
                if assistant_content != last_content.as_str() {
                    let delta = if assistant_content.starts_with(&last_content) {
                        &assistant_content[last_content.len()..]
                    } else {
                        assistant_content
                    };
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
                    last_emit = std::time::Instant::now();
                } else if last_emit.elapsed() >= Duration::from_secs(15) {
                    yield Ok(Event::default().event("ping").data("{}"));
                    last_emit = std::time::Instant::now();
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
                if poll_count.is_multiple_of(50) {
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

/// POST /v1/threads — create a fresh thread immediately (no message).
async fn create_thread_handler(
    State(state): State<SharedState>,
    Query(q): Query<AgentQuery>,
) -> Json<serde_json::Value> {
    match agent_for(&state, q.agent.as_deref()).await {
        Ok(agent) => match agent.create_thread().await {
            Ok(tid) => Json(serde_json::json!({"status": "created", "thread_id": tid})),
            Err(e) => Json(serde_json::json!({"status": "error", "error": e})),
        },
        Err(_) => Json(serde_json::json!({"status": "error", "error": "agent not found"})),
    }
}

async fn list_threads(
    State(state): State<SharedState>,
    Query(q): Query<AgentQuery>,
) -> Json<ThreadListResponse> {
    let mut threads: Vec<ThreadSummary> = Vec::new();
    if let Ok(agent) = agent_for(&state, q.agent.as_deref()).await {
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
    Query(q): Query<AgentQuery>,
) -> Result<Json<ThreadDetailResponse>, StatusCode> {
    let agent = agent_for(&state, q.agent.as_deref()).await?;
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
    /// Agent name to route to; defaults to the fabric default agent.
    agent: Option<String>,
    /// The turn_completed value the client last observed.
    /// If omitted, defaults to 0. Poll returns `completed: true` when
    /// thread.turn_completed exceeds this value (i.e., a new turn finished).
    turn: Option<u64>,
}

#[derive(Serialize)]
pub struct PollResponse {
    /// Full content of the most recent assistant message. Clients diff
    /// against what they have already displayed.
    pub new_content: Option<String>,
    /// Whether the thread's current turn is complete.
    pub completed: bool,
    /// Total content length of the current assistant message.
    pub content_len: usize,
    /// Identity of the served message, so clients can detect that a new
    /// message (thinking, tool call, answer) replaced the previous one.
    pub message_id: Option<String>,
    /// Metadata for rendering: entry_type (text/tool_call), tool_name,
    /// tool_status. Lets the CLI show the agent's tool activity.
    pub entry_type: Option<String>,
    pub tool_name: Option<String>,
    pub tool_status: Option<String>,
}

/// Poll for new thread state.
///
/// Alternative to SSE for clients that cannot maintain a persistent connection
/// or when WebSocket events from Telos are unreliable. The client calls this
/// endpoint at regular intervals (e.g., every 500ms).
///
/// The response serves the full content of the most recent assistant
/// message. A single turn emits several assistant messages in order
/// (thinking, tool calls, then the answer), so indexing by turn number is
/// not possible; instead the client diffs each served message against what
/// it has already displayed and uses message_id to detect a brand-new
/// message. Persisted threads whose turn counter drifted past the message
/// count are repaired on load. The `since` parameter is accepted for
/// backward compatibility and ignored.
async fn poll_thread(
    State(state): State<SharedState>,
    axum::extract::Path(thread_id): axum::extract::Path<String>,
    Query(query): Query<PollQuery>,
) -> Result<Json<PollResponse>, StatusCode> {
    let agent = agent_for(&state, query.agent.as_deref()).await?;
    let thread = agent
        .thread(&thread_id)
        .await
        .ok_or(StatusCode::NOT_FOUND)?;

    let known_turn = query.turn.unwrap_or(0);

    // The most recent assistant message. Pre-fix code addressed messages by
    // turn index (`assistants[known_turn]`), which broke as soon as one
    // turn produced several assistant messages (thinking, tool calls,
    // answer): the index landed on an arbitrary message from an earlier
    // turn, so the client saw stale content and never the actual answer.
    let current = thread.messages.iter().rev().find(|m| m.role == "assistant");

    let completed = thread.turn_completed > known_turn;
    match current {
        Some(msg) => Ok(Json(PollResponse {
            new_content: Some(msg.content.clone()),
            completed,
            content_len: msg.content.len(),
            message_id: msg.message_id.clone(),
            entry_type: msg.entry_type.clone(),
            tool_name: msg.tool_name.clone(),
            tool_status: msg.tool_status.clone(),
        })),
        None => Ok(Json(PollResponse {
            new_content: None,
            completed,
            content_len: 0,
            message_id: None,
            entry_type: None,
            tool_name: None,
            tool_status: None,
        })),
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
    let max = if params.max > 0 { params.max } else { 100 };
    let opts = files::FileSearchOptions {
        query: params.q.clone().unwrap_or_default(),
        dir: params.dir.clone(),
        max_results: max,
        ..Default::default()
    };
    // The tree walk is synchronous and can be slow on large workspaces;
    // keep it off the async runtime.
    let workdir = state.workdir.clone();
    let result = tokio::task::spawn_blocking(move || files::search_files(&workdir, &opts))
        .await
        .unwrap_or_default(); // panicked walk: an empty list is a valid response
    let count = result.files.len();
    Json(FileSearchResponse {
        files: result.files,
        count,
        truncated: result.truncated,
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
    let workdir = state.workdir.clone();
    let result = tokio::task::spawn_blocking(move || files::search_files(&workdir, &opts))
        .await
        .unwrap_or_default(); // panicked walk: an empty list is a valid response
    let mention = files::format_mention(&result.files, &query);
    Json(serde_json::json!({
        "mention": mention,
        "count": result.files.len(),
    }))
}

// ── Context mention endpoints ──────────────────────────────────────────

/// GET /v1/symbols?q= — definition-pattern symbol search for @ mentions.
#[derive(Deserialize)]
pub struct SymbolQuery {
    q: String,
    #[serde(default)]
    max: Option<usize>,
}

async fn search_symbols_handler(
    State(state): State<SharedState>,
    params: Query<SymbolQuery>,
) -> Json<serde_json::Value> {
    let workdir = state.workdir.clone();
    let q = params.q.clone();
    let max = params.max.unwrap_or(20);
    let symbols = tokio::task::spawn_blocking(move || context::search_symbols(&workdir, &q, max))
        .await
        .unwrap_or_default(); // panicked walk: an empty list is a valid response
    Json(serde_json::json!({
        "symbols": symbols,
        "count": symbols.len(),
    }))
}

/// GET /v1/rules — project rule files (AGENTS.md, *.mdc) as mention context.
async fn rules_handler(State(state): State<SharedState>) -> Json<serde_json::Value> {
    let workdir = state.workdir.clone();
    let rules = tokio::task::spawn_blocking(move || context::find_rules(&workdir))
        .await
        .unwrap_or_default(); // panicked walk: an empty list is a valid response
    Json(serde_json::json!({
        "rules": rules,
        "count": rules.len(),
    }))
}

#[derive(Deserialize)]
pub struct FetchQuery {
    url: String,
}

/// Strip HTML tags crudely; good enough for mention context injection.
fn strip_html(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut in_tag = false;
    for c in s.chars() {
        match c {
            '<' => in_tag = true,
            '>' => in_tag = false,
            _ if !in_tag => out.push(c),
            _ => {}
        }
    }
    out
}

/// Cap on fetched response bodies; the content is only used as mention
/// context, so a multi-megabyte page is a waste of memory.
const MAX_FETCH_BYTES: usize = 4 * 1024 * 1024;

/// True when `ip` is loopback, private, link-local, or otherwise not a
/// public address. Keeps /v1/fetch from reaching internal services.
fn is_non_public_ip(ip: std::net::IpAddr) -> bool {
    match ip {
        std::net::IpAddr::V4(v4) => {
            v4.is_loopback()
                || v4.is_private()
                || v4.is_link_local()
                || v4.is_unspecified()
                || v4.is_broadcast()
        }
        std::net::IpAddr::V6(v6) => {
            if let Some(v4) = v6.to_ipv4_mapped() {
                return is_non_public_ip(std::net::IpAddr::V4(v4));
            }
            v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_unique_local()
                || v6.is_unicast_link_local()
        }
    }
}

/// Validate a fetch target and build a client with a timeout and no
/// redirects. Redirects are disabled because a response could bounce to
/// an internal address after the host check already passed.
///
/// The host check runs at resolution time and the connection re-resolves
/// DNS, so a hostile resolver could swap the address between check and
/// connect (DNS rebinding). This is accepted for a loopback-bound server
/// whose fetch endpoint only adds mention context; treat the guard as
/// defense in depth, not a general-purpose SSRF boundary.
async fn fetch_client(url_str: &str) -> Result<(String, reqwest::Client), String> {
    let parsed = reqwest::Url::parse(url_str).map_err(|e| format!("invalid url: {}", e))?;
    let scheme = parsed.scheme();
    if scheme != "http" && scheme != "https" {
        return Err("url must use http or https".to_string());
    }
    let host = parsed
        .host_str()
        .ok_or_else(|| "url has no host".to_string())?;
    let port = parsed
        .port_or_known_default()
        .ok_or_else(|| "url has no port".to_string())?;
    let host_lower = host.to_lowercase();
    if host_lower == "localhost" || host_lower.ends_with(".localhost") {
        return Err("url host must be a public address".to_string());
    }
    match host.parse::<std::net::IpAddr>() {
        Ok(ip) => {
            if is_non_public_ip(ip) {
                return Err("url host must be a public address".to_string());
            }
        }
        Err(_) => {
            let addrs = tokio::net::lookup_host((host, port))
                .await
                .map_err(|e| format!("cannot resolve host: {}", e))?;
            for addr in addrs {
                if is_non_public_ip(addr.ip()) {
                    return Err("url host resolves to a private address".to_string());
                }
            }
        }
    }
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|e| e.to_string())?;
    Ok((parsed.to_string(), client))
}

/// GET /v1/fetch?url= — fetch a public URL and return its text for @ mention.
async fn fetch_handler(Query(q): Query<FetchQuery>) -> Json<serde_json::Value> {
    let url = q.url.trim().to_string();
    if url.is_empty() {
        return Json(serde_json::json!({"ok": false, "error": "url is required"}));
    }
    let (url, client) = match fetch_client(&url).await {
        Ok(v) => v,
        Err(e) => return Json(serde_json::json!({"ok": false, "error": e})),
    };
    match client.get(&url).send().await {
        Ok(resp) => match resp.error_for_status() {
            Ok(resp) => {
                let mut body: Vec<u8> = Vec::new();
                let mut stream = resp.bytes_stream();
                while let Some(chunk) = stream.next().await {
                    match chunk {
                        Ok(c) => {
                            if body.len() + c.len() > MAX_FETCH_BYTES {
                                return Json(serde_json::json!({
                                    "ok": false,
                                    "error": "response too large"
                                }));
                            }
                            body.extend_from_slice(&c);
                        }
                        Err(e) => {
                            return Json(serde_json::json!({"ok": false, "error": e.to_string()}))
                        }
                    }
                }
                let text = strip_html(&String::from_utf8_lossy(&body));
                let content = if text.len() > 12_000 {
                    let mut end = 12_000;
                    while !text.is_char_boundary(end) {
                        end -= 1;
                    }
                    format!("{}...", &text[..end])
                } else {
                    text.to_string()
                };
                Json(serde_json::json!({"ok": true, "url": url, "content": content}))
            }
            Err(e) => Json(serde_json::json!({"ok": false, "error": e.to_string()})),
        },
        Err(e) => Json(serde_json::json!({"ok": false, "error": e.to_string()})),
    }
}

// ── Git endpoints ──────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct GitLogQuery {
    #[serde(default = "default_git_log_count")]
    max: usize,
}

fn default_git_log_count() -> usize {
    10
}

#[derive(Deserialize)]
pub struct GitDiffQuery {
    #[serde(default)]
    staged: bool,
}

/// GET /v1/git/status — git working tree status.
async fn git_status(State(state): State<SharedState>) -> Json<serde_json::Value> {
    let workdir = state.workdir.clone();
    let result = tokio::task::spawn_blocking(move || git::get_status(&workdir)).await;
    match result {
        Ok(Ok(Some(status))) => Json(serde_json::json!({"ok": true, "status": status})),
        Ok(Ok(None)) => Json(serde_json::json!({"ok": false, "error": "Not a git repository"})),
        Ok(Err(e)) => Json(serde_json::json!({"ok": false, "error": e})),
        Err(_) => Json(serde_json::json!({"ok": false, "error": "git check interrupted"})),
    }
}

/// GET /v1/git/diff — git diff (unstaged by default, ?staged=true for staged).
async fn git_diff(
    State(state): State<SharedState>,
    params: Query<GitDiffQuery>,
) -> Json<serde_json::Value> {
    let workdir = state.workdir.clone();
    let staged = params.staged;
    let result = tokio::task::spawn_blocking(move || git::get_diff(&workdir, staged)).await;
    match result {
        Ok(Ok(Some(diff))) => Json(serde_json::json!({"ok": true, "diff": diff})),
        Ok(Ok(None)) => Json(serde_json::json!({"ok": false, "error": "Not a git repository"})),
        Ok(Err(e)) => Json(serde_json::json!({"ok": false, "error": e})),
        Err(_) => Json(serde_json::json!({"ok": false, "error": "git diff interrupted"})),
    }
}

/// GET /v1/git/log — recent commit history.
async fn git_log(
    State(state): State<SharedState>,
    params: Query<GitLogQuery>,
) -> Json<serde_json::Value> {
    let workdir = state.workdir.clone();
    let max = params.max;
    let result = tokio::task::spawn_blocking(move || git::get_log(&workdir, max)).await;
    match result {
        Ok(Ok(Some(commits))) => Json(serde_json::json!({"ok": true, "commits": commits})),
        Ok(Ok(None)) => Json(serde_json::json!({"ok": false, "error": "Not a git repository"})),
        Ok(Err(e)) => Json(serde_json::json!({"ok": false, "error": e})),
        Err(_) => Json(serde_json::json!({"ok": false, "error": "git log interrupted"})),
    }
}

// ── Tool-call authorization endpoints ───────────────────────────────────

#[derive(Deserialize)]
pub struct AgentRouteQuery {
    agent: Option<String>,
}

/// GET /v1/agents/tool-calls/pending — authorizations awaiting a decision.
async fn pending_tool_calls_handler(
    State(state): State<SharedState>,
    Query(q): Query<AgentRouteQuery>,
) -> Json<serde_json::Value> {
    match agent_for(&state, q.agent.as_deref()).await {
        Ok(agent) => {
            let pending = agent.pending_tool_calls().await;
            Json(serde_json::json!({"pending": pending, "count": pending.len()}))
        }
        Err(_) => Json(serde_json::json!({"pending": [], "count": 0})),
    }
}

#[derive(Deserialize)]
pub struct ResolveToolCallRequest {
    pub agent: Option<String>,
    pub platform_thread_id: String,
    pub tool_call_id: String,
    pub allow: bool,
}

/// POST /v1/agents/tool-calls/resolve — approve or reject a pending tool call.
async fn resolve_tool_call_handler(
    State(state): State<SharedState>,
    Json(req): Json<ResolveToolCallRequest>,
) -> Json<serde_json::Value> {
    match agent_for(&state, req.agent.as_deref()).await {
        Ok(agent) => match agent
            .resolve_tool_call(&req.platform_thread_id, &req.tool_call_id, req.allow)
            .await
        {
            Ok(()) => Json(serde_json::json!({"status": "resolved", "allow": req.allow})),
            Err(e) => Json(serde_json::json!({"status": "error", "error": e})),
        },
        Err(_) => Json(serde_json::json!({"status": "error", "error": "agent not found"})),
    }
}

// ── Cancel endpoint ────────────────────────────────────────────────────

/// Body for POST /v1/cancel. All fields optional: an empty body cancels
/// the default agent's whole turn, as before. `agent` selects another
/// agent; `request_id` narrows the cancel to one turn of that agent when
/// the backend tracks per-request state (parallel raw-CLI agents).
#[derive(Deserialize, Default)]
struct CancelRequest {
    pub agent: Option<String>,
    pub request_id: Option<String>,
}

async fn cancel_turn(
    State(state): State<SharedState>,
    body: axum::body::Bytes,
) -> Json<serde_json::Value> {
    let req: CancelRequest = if body.is_empty() {
        CancelRequest::default()
    } else {
        serde_json::from_slice(&body).unwrap_or_default()
    };
    let agent = match &req.agent {
        Some(name) => agent_for(&state, Some(name)).await,
        None => default_agent(&state).await,
    };
    let agent = match agent {
        Ok(agent) => agent,
        Err(_) => {
            return Json(serde_json::json!({
                "status": "error",
                "error": "agent not found or not ready"
            }))
        }
    };
    let result = match &req.request_id {
        Some(request_id) => agent.cancel_request(request_id).await,
        None => agent.cancel().await,
    };
    match result {
        Ok(()) => Json(serde_json::json!({ "status": "cancelled" })),
        Err(e) => Json(serde_json::json!({ "status": "error", "error": e })),
    }
}

// ── Router ─────────────────────────────────────────────────────────────

/// Build the axum router over the given state. Kept separate from
/// `run_http_server` so integration tests can serve the same router on
/// an ephemeral port without binding a fixed address.
///
/// `cors_origins` lists origins allowed to call the API from a browser.
/// Empty means no CORS headers are sent, so browsers enforce same-origin
/// and cross-origin reads are blocked. The CORS layer sits outside the
/// auth middleware so preflight OPTIONS requests are answered before the
/// token check runs.
pub fn build_router(state: SharedState, cors_origins: &[String]) -> Router {
    let router = Router::new()
        .route("/health", get(health))
        .route("/v1/chat", post(chat_stream))
        .route("/v1/chat/async", post(chat_async))
        .route("/v1/cancel", post(cancel_turn))
        .route(
            "/v1/agents/tool-calls/pending",
            get(pending_tool_calls_handler),
        )
        .route(
            "/v1/agents/tool-calls/resolve",
            post(resolve_tool_call_handler),
        )
        .route("/v1/threads", get(list_threads))
        .route("/v1/threads", post(create_thread_handler))
        .route("/v1/threads/{thread_id}", get(get_thread))
        .route("/v1/threads/{thread_id}/poll", get(poll_thread))
        .route("/v1/files", get(search_files_handler))
        .route("/v1/files/mention", get(mention_files_handler))
        .route("/v1/symbols", get(search_symbols_handler))
        .route("/v1/rules", get(rules_handler))
        .route("/v1/fetch", get(fetch_handler))
        .route("/v1/git/status", get(git_status))
        .route("/v1/git/diff", get(git_diff))
        .route("/v1/git/log", get(git_log))
        .with_state(state.clone());

    let router = router.layer(middleware::from_fn_with_state(state.clone(), require_auth));

    if cors_origins.is_empty() {
        router
    } else {
        let origins: Vec<HeaderValue> =
            cors_origins.iter().filter_map(|o| o.parse().ok()).collect();
        router.layer(
            tower_http::cors::CorsLayer::new()
                .allow_origin(origins)
                // The API only reads the bearer token and JSON bodies, so
                // the preflight response is pinned to those plus OPTIONS.
                .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
                .allow_headers([header::AUTHORIZATION, header::CONTENT_TYPE]),
        )
    }
}

pub async fn run_http_server(
    addr: &str,
    state: SharedState,
    cors_origins: Vec<String>,
) -> anyhow::Result<()> {
    let app = build_router(state, &cors_origins);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("HTTP API server listening on http://{}", addr);

    axum::serve(listener, app).await?;
    Ok(())
}
