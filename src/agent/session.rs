// The session layer: what one thread and one turn are, in one place.
//
// Every auxiliary adapter carried its own copy of this before: a thread map, a
// session constructor, an allocation function, and the same three mutations
// around a turn (title from the first message, the user message and the
// incomplete mark, then the reply and the completion). The copies are what let
// the kinds drift, each answering durability, completion, titling, and
// notification its own way. The store owns those answers so an adapter owns
// only what its platform does: run one turn and hand back what it produced.
//
// Persistence is deliberately absent. The auxiliary kinds keep their threads
// in memory today, and this store is the single place where a later change
// makes them durable, which is the parity work of issue #18.

use std::collections::HashMap;

use tokio::sync::{watch, RwLock};

use crate::agent::{truncate_title, ThreadMessage, ThreadParent, ThreadSession};

/// One thread map, one change notification, and the bookkeeping of a turn.
pub struct ThreadStore {
    /// Prefix a thread id gets when the caller names none, so threads from
    /// different kinds stay distinguishable in a listing.
    prefix: &'static str,
    threads: RwLock<HashMap<String, ThreadSession>>,
    notify: watch::Sender<u64>,
}

/// The reply an adapter records when its turn ends.
pub struct TurnReply {
    pub content: String,
    /// The request id the turn ran under, when the adapter mints one.
    pub message_id: Option<String>,
}

impl TurnReply {
    /// A reply as the auxiliary kinds record it: an agent message.
    pub fn message(content: impl Into<String>, message_id: Option<String>) -> Self {
        Self {
            content: content.into(),
            message_id,
        }
    }
}

impl ThreadStore {
    pub fn new(prefix: &'static str) -> Self {
        let (notify, _) = watch::channel(0u64);
        Self {
            prefix,
            threads: RwLock::new(HashMap::new()),
            notify,
        }
    }

    fn blank_session(&self, id: String) -> ThreadSession {
        ThreadSession {
            id,
            title: None,
            messages: Vec::new(),
            created_at: chrono::Utc::now(),
            updated_at: None,
            // A thread with no turn in flight is complete, which is what a
            // reader sees before the first submit and after the last reply.
            completed: true,
            acp_thread_id: None,
            turn_completed: 0,
            parent: None,
        }
    }

    /// The thread to write into: the named one, allocating it when the name is
    /// new, or a fresh one when no name was given. The flag reports whether
    /// the thread held no message before this call, which is what a submit
    /// receipt reports as `is_new`.
    pub async fn get_or_create(&self, thread_id: Option<&str>) -> (String, bool) {
        let mut threads = self.threads.write().await;
        let tid = match thread_id {
            Some(t) if threads.contains_key(t) => t.to_string(),
            Some(t) => {
                let tid = t.to_string();
                threads.insert(tid.clone(), self.blank_session(tid.clone()));
                tid
            }
            None => {
                let tid = format!("{}-{}", self.prefix, uuid::Uuid::new_v4());
                threads.insert(tid.clone(), self.blank_session(tid.clone()));
                tid
            }
        };
        let is_new = threads
            .get(&tid)
            .map(|t| t.messages.is_empty())
            .unwrap_or(true);
        (tid, is_new)
    }

    /// A fresh thread with no turn in it yet.
    pub async fn create_thread(&self) -> String {
        self.get_or_create(None).await.0
    }

    /// Open a turn: title the thread from its first message, record where the
    /// turn came from, write the user message, and mark the thread incomplete.
    /// The dispatch origin is recorded once per thread, on the turn that
    /// created it.
    ///
    /// This does not wake subscribers, because the kinds do not agree on
    /// whether opening a turn should: one notifies at the submit and another
    /// notifies only when the turn ends. An adapter that wants the wakeup asks
    /// for it with `notify`, and unifying the two stays a deliberate change
    /// rather than a side effect of sharing this code.
    pub async fn begin_turn(
        &self,
        thread_id: &str,
        message: &str,
        parent: Option<ThreadParent>,
    ) -> Result<(), String> {
        let now = chrono::Utc::now();
        let mut threads = self.threads.write().await;
        let session = threads
            .get_mut(thread_id)
            .ok_or_else(|| format!("thread '{thread_id}' vanished"))?;
        if session.title.is_none() {
            session.title = Some(truncate_title(message));
        }
        if session.parent.is_none() {
            session.parent = parent;
        }
        session.messages.push(ThreadMessage {
            role: "user".to_string(),
            content: message.to_string(),
            message_id: None,
            entry_type: None,
            tool_name: None,
            tool_status: None,
            timestamp: now,
        });
        session.completed = false;
        Ok(())
    }

    /// Close a turn: append the reply, mark the thread complete, and count the
    /// turn, which is what a consumer waiting on `turn_completed` observes.
    pub async fn finish_turn(&self, thread_id: &str, reply: TurnReply) -> Result<(), String> {
        let now = chrono::Utc::now();
        {
            let mut threads = self.threads.write().await;
            let session = threads
                .get_mut(thread_id)
                .ok_or_else(|| format!("thread '{thread_id}' vanished"))?;
            session.messages.push(reply_message(reply, now));
            session.completed = true;
            session.turn_completed += 1;
        }
        self.notify();
        Ok(())
    }

    /// Open and close a turn in one mutation, for an adapter whose turn
    /// completes where it starts: the thread is never observably incomplete,
    /// and its one timestamp is shared by the request and the reply.
    pub async fn run_turn(
        &self,
        thread_id: &str,
        message: &str,
        parent: Option<ThreadParent>,
        reply: TurnReply,
    ) -> Result<(), String> {
        let now = chrono::Utc::now();
        {
            let mut threads = self.threads.write().await;
            let session = threads
                .get_mut(thread_id)
                .ok_or_else(|| format!("thread '{thread_id}' vanished"))?;
            if session.title.is_none() {
                session.title = Some(truncate_title(message));
            }
            if session.parent.is_none() {
                session.parent = parent;
            }
            session.messages.push(ThreadMessage {
                role: "user".to_string(),
                content: message.to_string(),
                message_id: None,
                entry_type: None,
                tool_name: None,
                tool_status: None,
                timestamp: now,
            });
            session.messages.push(reply_message(reply, now));
            session.completed = true;
            session.turn_completed += 1;
        }
        self.notify();
        Ok(())
    }

    /// Whether any thread carries a reply under this request id. A turn that
    /// finished has no running entry left, so this is how a cancel tells a
    /// turn that already answered from an id this agent never saw.
    pub async fn has_reply(&self, request_id: &str) -> bool {
        self.threads.read().await.values().any(|session| {
            session
                .messages
                .iter()
                .any(|message| message.message_id.as_deref() == Some(request_id))
        })
    }

    pub async fn thread(&self, thread_id: &str) -> Option<ThreadSession> {
        self.threads.read().await.get(thread_id).cloned()
    }

    pub async fn threads(&self) -> Vec<ThreadSession> {
        self.threads.read().await.values().cloned().collect()
    }

    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.notify.subscribe()
    }

    /// Wake every subscriber because a thread changed. Calls that open or
    /// close a turn do this themselves; an adapter that changes a thread in
    /// some other way calls it directly.
    pub fn notify(&self) {
        let _ = self
            .notify
            .send(chrono::Utc::now().timestamp_millis() as u64);
    }
}

fn reply_message(reply: TurnReply, now: chrono::DateTime<chrono::Utc>) -> ThreadMessage {
    ThreadMessage {
        role: "assistant".to_string(),
        content: reply.content,
        message_id: reply.message_id,
        entry_type: Some("agent_message".to_string()),
        tool_name: None,
        tool_status: None,
        timestamp: now,
    }
}
