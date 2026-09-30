// NativeBackend — reference in-process adapter implementing the agent
// fabric trait.
//
// The adapter is deterministic and needs no LLM, no external binary, and no
// network. It answers every submission with a canned reply so that the
// fabric seam (AgentBackend, AgentRegistry, HTTP handlers) can be
// exercised end to end without any external agent. It exists to prove that
// a platform is an adapter plus a factory registration, with no change to
// the server.
//
// It is also the adapter the session layer is easiest to see through: the
// reply is produced inside the request, so the turn opens and closes in one
// mutation and a reader never sees the thread incomplete.

use std::sync::Arc;

use tokio::sync::watch;

use crate::agent::adapter::{AgentFactory, LaunchContext, LaunchedAgent};
use crate::agent::config::AgentSpec;
use crate::agent::session::{ThreadStore, TurnReply};
use crate::agent::{
    AgentBackend, AgentCapabilities, AgentStatus, PendingAuthorization, SubmitReceipt,
    ThreadSession,
};

/// Deterministic in-process agent instance.
pub struct NativeAgent {
    name: String,
    store: ThreadStore,
}

impl NativeAgent {
    pub fn new(name: String) -> Self {
        Self {
            name,
            store: ThreadStore::new("native"),
        }
    }
}

#[async_trait::async_trait]
impl AgentBackend for NativeAgent {
    fn name(&self) -> &str {
        &self.name
    }

    fn kind(&self) -> &'static str {
        "native"
    }

    fn capabilities(&self) -> AgentCapabilities {
        // In-process, deterministic, no tool surface: it proves the seam
        // rather than acting on the workspace.
        AgentCapabilities {
            sessionful: true,
            streaming: false,
            tools: false,
            approval: false,
            parallel: false,
            transport: "inproc",
        }
    }

    async fn status(&self) -> AgentStatus {
        AgentStatus {
            name: self.name.clone(),
            kind: self.kind().to_string(),
            connected: true,
            ready: true,
            capabilities: self.capabilities(),
            last_error: None,
        }
    }

    async fn submit(
        &self,
        thread_id: Option<&str>,
        message: &str,
    ) -> Result<SubmitReceipt, String> {
        let (tid, is_new) = self.store.get_or_create(thread_id).await;
        let request_id = uuid::Uuid::new_v4().to_string();
        let reply = TurnReply::message(
            format!("[native reference agent] received: {message}"),
            Some(request_id.clone()),
        );
        self.store.run_turn(&tid, message, None, reply).await?;
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
        self.store.thread(thread_id).await
    }

    async fn threads(&self) -> Vec<ThreadSession> {
        self.store.threads().await
    }

    async fn subscribe(&self) -> watch::Receiver<u64> {
        self.store.subscribe()
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
        Ok(self.store.create_thread().await)
    }
}

/// Factory for the `native` kind: the in-process reference adapter.
pub struct NativeFactory;

#[async_trait::async_trait]
impl AgentFactory for NativeFactory {
    fn kind(&self) -> &'static str {
        "native"
    }

    async fn launch(&self, spec: &AgentSpec, _ctx: &LaunchContext) -> anyhow::Result<LaunchedAgent> {
        tracing::info!(
            "Agent '{}' running (native reference adapter, in-process)",
            spec.name
        );
        Ok(LaunchedAgent {
            backend: Arc::new(NativeAgent::new(spec.name.clone())),
            children: Vec::new(),
        })
    }
}
