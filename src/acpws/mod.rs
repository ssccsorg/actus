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

/// The entry type a user message carries while its turn waits for the agent.
///
/// A queued turn is a user message whose state says it has not run, so the state lives where the
/// record already carries per-message state rather than in a second place that could disagree
/// with the thread. Dispatch clears it, which is what makes the message the turn that runs.
pub const QUEUED_ENTRY: &str = "queued";

/// A turn accepted while the agent was running another one.
///
/// The message is already in the thread. What waits here is the command, and this list is what
/// says the turn has not been sent. Entries are held in arrival order across the agent, which
/// keeps every thread's own order because a thread's entries are appended in that order, and the
/// order across threads is the agent's own single lock made visible.
#[derive(Clone, Debug)]
pub struct QueuedTurn {
    pub request_id: String,
    pub thread_id: String,
    /// The message as the caller typed it. The command carries the prepared form, and the
    /// preparation waits for dispatch because it reads the thread's history, which the turn that
    /// is running is still writing.
    pub message: String,
    pub thinking_effort: Option<String>,
    /// The id the waiting message was recorded under, so dispatch can mark it sent.
    pub message_id: String,
}

// ACP-over-WebSocket manager: connection, session management, and settings bootstrap.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

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
    /// Turns accepted while this agent was running another one, in arrival order. A different
    /// thing from `pending_chat_queue`, which holds commands that have already gone to the
    /// agent: a reconnection resends those and must not resend these, because these were never
    /// sent.
    pub queued_turns: Vec<QueuedTurn>,
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
            queued_turns: Vec::new(),
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
            // A turn that is still waiting is not part of the turn that ended, so it does not
            // end the trailing block: skipping it makes the snapshot the completed turn's own
            // answer, which is what a replay of that answer has to be compared against.
            .filter(|m| m.entry_type.as_deref() != Some(QUEUED_ENTRY))
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

    /// Whether this agent is running a turn, which is what makes an arriving message wait.
    ///
    /// `pending_requests` holds a live mapping while a turn is in flight and a blank sentinel
    /// once it has been consumed, so a non-empty value is what "running" means.
    pub fn has_live_request(&self) -> bool {
        self.pending_requests
            .values()
            .any(|thread_id| !thread_id.is_empty())
    }

    /// Send the turn at the front of the queue, if this agent is free.
    ///
    /// Called where a turn's request id leaves `pending_requests`, which is where a turn ends.
    /// What it repeats from a submit is the part after the command is built: the message is
    /// prepared against the thread's history now rather than when it was accepted, because the
    /// turn that was running has written to the thread in between, and the request id starts
    /// counting only now.
    pub fn dispatch_queued(&mut self) -> Result<Option<String>, String> {
        if self.has_live_request() || self.queued_turns.is_empty() {
            return Ok(None);
        }
        let turn = self.queued_turns.remove(0);
        let enriched = self.prepare_message(&turn.thread_id, &turn.message);
        let cmd = Command::ChatMessage {
            acp_thread_id: self.get_acp_thread_id(&turn.thread_id),
            message: enriched,
            request_id: turn.request_id.clone(),
            thinking_effort: turn.thinking_effort.clone(),
        }
        .to_json()?;
        if let Err(e) = self.send_command(&cmd) {
            // Nothing carried the command, so the turn keeps its place and waits for a
            // connection. It is not lost, and it has not been sent.
            self.queued_turns.insert(0, turn);
            return Err(e);
        }
        // The message said it was waiting. The command is out, so it is now the turn that runs,
        // and the state it carries says so to every client that reads the thread.
        self.add_message_full(
            &turn.thread_id,
            "user",
            &turn.message,
            Some(turn.message_id.clone()),
            None,
            None,
            None,
            None,
        );
        self.pending_requests
            .insert(turn.request_id.clone(), turn.thread_id.clone());
        self.threads_activated.insert(turn.thread_id.clone());
        if let Some(thread) = self.threads.get_mut(&turn.thread_id) {
            thread.completed = false;
        }
        self.pending_chat_queue
            .push((turn.request_id.clone(), turn.thread_id.clone(), cmd));
        self.notify_thread_change();
        Ok(Some(turn.request_id))
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

/// One process launch: what the configuration declared, and the values actus
/// fills into it.
pub struct Launch<'a> {
    /// The agent this launch is for. Every error names it.
    pub agent_name: &'a str,
    /// The argv the configuration declared. A declaration replaces the whole launch: a
    /// deployment that changes one argument writes all of them, and actus carries no
    /// argv of its own.
    pub args: &'a [String],
    /// The environment the configuration declared, with the same contract as `args`.
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
    ///
    /// A declaration is the deployment's, so an empty one is refused by name rather than
    /// filled from a built-in: actus carries no executor's argv or environment, because
    /// both are that executor's and a stack component that knew them would not be one.
    fn declared(&self) -> anyhow::Result<LaunchPlan> {
        if self.args.is_empty() {
            return Err(anyhow::anyhow!(
                "the configuration declares no launch_args, and actus carries none of its own"
            ));
        }
        if self.env.is_empty() {
            return Err(anyhow::anyhow!(
                "the configuration declares no launch_env, and actus carries none of its own"
            ));
        }
        let args: Vec<String> = self.args.to_vec();
        let env: Vec<(String, String)> = self
            .env
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();

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

/// The spec a settings writer receives: every value a declared context server reads from
/// the environment is resolved here, against this process's environment.
///
/// The resolution is actus's because the environment is: a `$NAME` names a value this
/// process holds, and a deployment that supplied a writer would otherwise restate the
/// policy for reading it, in a crate that does not own the value. The refusal an unset
/// name earns lives in `resolve_declared`, so it is one decision in one place, made
/// before the writer runs.
pub fn resolve_mcp(spec: &AgentSpec) -> anyhow::Result<AgentSpec> {
    let mut resolved = spec.clone();
    for server in &mut resolved.mcp {
        server.env = resolve_declared(&server.env, &server.name, "env value", server.enabled)?;
        server.headers = resolve_declared(&server.headers, &server.name, "header", server.enabled)?;
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

/// Hand the executor's settings to the writer that renders them: the one step between a
/// parsed declaration and a launched executor.
///
/// The reference a declared server carries is resolved first, so the writer receives values
/// and a name that is not set fails here, naming the variable, rather than reaching the
/// executor as text. A composition that names no writer is a deployment that runs no
/// executor whose format actus carries, and it is refused by name: a launch that wrote no
/// settings at all would start a program that then fails on its own first read.
pub fn write_settings(
    writer: Option<&dyn SettingsWriter>,
    data_dir: &Path,
    spec: &AgentSpec,
) -> anyhow::Result<()> {
    let spec = resolve_mcp(spec).map_err(|e| anyhow::anyhow!("agent '{}': {e}", spec.name))?;
    let writer = writer.ok_or_else(|| {
        anyhow::anyhow!(
            "agent '{}': the kind 'acpws' starts a program whose settings format is the \
             program's, and this composition names no writer for it",
            spec.name
        )
    })?;
    writer.write(data_dir, &spec)
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

    /// The launch is the deployment's, so one that declares nothing is refused by name:
    /// actus carries no executor's argv or environment, and a launch that fell back to one
    /// would run a program nobody declared.
    #[test]
    fn a_launch_that_declares_nothing_is_refused_by_name() {
        let dir = tempfile::tempdir().unwrap();
        let bin = dir.path().join("tel");
        let no_env = HashMap::new();

        let log = std::fs::File::create(dir.path().join("agent.log")).unwrap();
        let launch = launch_with(&bin, dir.path(), dir.path(), &[], &no_env);
        let error = launch_command(&launch, log).unwrap_err().to_string();
        assert!(error.contains("agent 'telos'"), "{error}");
        assert!(error.contains("launch_args"), "{error}");

        let args = vec!["--headless".to_string()];
        let log = std::fs::File::create(dir.path().join("second.log")).unwrap();
        let launch = launch_with(&bin, dir.path(), dir.path(), &args, &no_env);
        let error = launch_command(&launch, log).unwrap_err().to_string();
        assert!(error.contains("agent 'telos'"), "{error}");
        assert!(error.contains("launch_env"), "{error}");
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
