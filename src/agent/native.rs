// NativeBackend — reference in-process adapter implementing the agent
// fabric trait.
//
// The adapter is deterministic and needs no LLM, no Telos binary, and no
// network. It answers every submission with a canned reply so that the
// fabric seam (AgentBackend, AgentRegistry, HTTP handlers) can be
// exercised end to end without any external agent. It exists to prove
// that Telos is one adapter among several: adding a real platform is a
// new `AgentBackend` implementation plus one registry entry, with no
// change to the server.

use std::collections::HashMap;

use tokio::sync::watch;
use tokio::sync::RwLock;

use crate::agent::{
    truncate_title, AgentBackend, AgentKind, AgentStatus, PendingAuthorization, SubmitReceipt,
    ThreadMessage, ThreadSession,
};

/// Deterministic in-process agent instance.
pub struct NativeAgent {
    name: String,
    threads: RwLock<HashMap<String, ThreadSession>>,
    notify: watch::Sender<u64>,
}

impl NativeAgent {
    pub fn new(name: String) -> Self {
        let (notify, _) = watch::channel(0u64);
        Self {
            name,
            threads: RwLock::new(HashMap::new()),
            notify,
        }
    }

    fn blank_session(id: String) -> ThreadSession {
        ThreadSession {
            id,
            title: None,
            messages: Vec::new(),
            created_at: chrono::Utc::now(),
            completed: true,
            acp_thread_id: None,
            turn_completed: 0,
        }
    }

    async fn get_or_create(&self, thread_id: Option<&str>) -> (String, bool) {
        let mut threads = self.threads.write().await;
        let tid = match thread_id {
            Some(t) if threads.contains_key(t) => t.to_string(),
            Some(t) => {
                let tid = t.to_string();
                threads.insert(tid.clone(), Self::blank_session(tid.clone()));
                tid
            }
            None => {
                let tid = format!("native-{}", uuid::Uuid::new_v4());
                threads.insert(tid.clone(), Self::blank_session(tid.clone()));
                tid
            }
        };
        let is_new = threads
            .get(&tid)
            .map(|t| t.messages.is_empty())
            .unwrap_or(true);
        (tid, is_new)
    }
}

#[async_trait::async_trait]
impl AgentBackend for NativeAgent {
    fn name(&self) -> &str {
        &self.name
    }

    fn kind(&self) -> AgentKind {
        AgentKind::Native
    }

    async fn status(&self) -> AgentStatus {
        AgentStatus {
            name: self.name.clone(),
            kind: AgentKind::Native,
            connected: true,
            ready: true,
        }
    }

    async fn submit(
        &self,
        thread_id: Option<&str>,
        message: &str,
    ) -> Result<SubmitReceipt, String> {
        let (tid, is_new) = self.get_or_create(thread_id).await;
        let request_id = uuid::Uuid::new_v4().to_string();
        let now = chrono::Utc::now();

        {
            let mut threads = self.threads.write().await;
            let session = threads
                .get_mut(&tid)
                .ok_or_else(|| format!("thread '{}' vanished", tid))?;
            if session.title.is_none() {
                session.title = Some(truncate_title(message));
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
            session.messages.push(ThreadMessage {
                role: "assistant".to_string(),
                content: format!("[native reference agent] received: {message}"),
                message_id: Some(request_id.clone()),
                entry_type: Some("agent_message".to_string()),
                tool_name: None,
                tool_status: None,
                timestamp: now,
            });
            session.completed = true;
            session.turn_completed += 1;
        }

        let _ = self.notify.send(now.timestamp_millis() as u64);
        Ok(SubmitReceipt {
            thread_id: tid,
            request_id,
            is_new,
        })
    }

    async fn cancel(&self) -> Result<(), String> {
        Ok(())
    }

    async fn thread(&self, thread_id: &str) -> Option<ThreadSession> {
        self.threads.read().await.get(thread_id).cloned()
    }

    async fn threads(&self) -> Vec<ThreadSession> {
        self.threads.read().await.values().cloned().collect()
    }

    async fn subscribe(&self) -> watch::Receiver<u64> {
        self.notify.subscribe()
    }

    async fn pending_tool_calls(&self) -> Vec<PendingAuthorization> {
        Vec::new()
    }

    async fn resolve_tool_call(
        &self,
        _platform_thread_id: &str,
        _tool_call_id: &str,
        _allow: bool,
    ) -> Result<(), String> {
        Ok(())
    }

    async fn create_thread(&self) -> Result<String, String> {
        let (tid, _) = self.get_or_create(None).await;
        Ok(tid)
    }
}
