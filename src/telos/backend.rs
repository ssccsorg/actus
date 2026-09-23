// TelosBackend — ACP/WebSocket adapter implementing the agent fabric trait.
//
// Runs one Telos process behind the `AgentBackend` interface.
// ACP-over-WebSocket details (connection loop, event dispatch, reconnect)
// stay inside `telos::control`; this adapter owns thread state, context
// injection, command submission, and the resume wait.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tokio::sync::{Notify, RwLock};

use crate::agent::{
    AgentBackend, AgentKind, AgentStatus, PendingAuthorization, SubmitReceipt, ThreadParent,
    ThreadSession,
};
use crate::telos::{TelosManager, WsCommandTx};

/// One Telos agent instance behind the fabric interface.
pub struct TelosBackend {
    /// Name this instance answers to. The registry keys on it and routing
    /// looks it up, so it has to be the configured agent name: a constant
    /// would make every telos agent collide on one registry entry and leave
    /// all but the last unreachable.
    pub name: String,
    pub manager: Arc<RwLock<TelosManager>>,
    /// Shared command channel, kept in sync with `TelosManager::ws_tx` by
    /// `telos::control`. Sending through the shared channel means a stale
    /// manager-side sender after a reconnect cannot silently drop a
    /// message.
    pub ws_tx: WsCommandTx,
}

/// The wire command that cancels a turn.
///
/// Telos resolves the turn from `request_id`: the command names no thread, so a body
/// without one names no turn, which is why a cancel that sent an empty body could not
/// stop anything and was answered as a noop. The empty form is kept for the case where
/// no turn is in flight.
fn cancel_command(request_id: Option<&str>) -> String {
    match request_id {
        Some(request_id) => serde_json::json!({
            "type": "cancel_current_turn",
            "data": { "request_id": request_id }
        }),
        None => serde_json::json!({
            "type": "cancel_current_turn",
            "data": {}
        }),
    }
    .to_string()
}

#[async_trait::async_trait]
impl AgentBackend for TelosBackend {
    fn name(&self) -> &str {
        &self.name
    }

    fn kind(&self) -> AgentKind {
        AgentKind::Telos
    }

    async fn status(&self) -> AgentStatus {
        let mgr = self.manager.read().await;
        AgentStatus {
            name: self.name().to_string(),
            kind: self.kind(),
            connected: mgr.telos_connected,
            ready: mgr.agent_ready,
            capabilities: self.kind().capabilities(),
            last_error: None,
        }
    }

    async fn submit(
        &self,
        thread_id: Option<&str>,
        message: &str,
    ) -> Result<SubmitReceipt, String> {
        self.submit_with_options(thread_id, message, None, None).await
    }

    async fn submit_with_options(
        &self,
        thread_id: Option<&str>,
        message: &str,
        _parent: Option<ThreadParent>,
        thinking_effort: Option<&str>,
    ) -> Result<SubmitReceipt, String> {
        // Prepare thread state under the write lock, then release before
        // sending so the command channel is not blocked by thread work.
        let (thread_id, request_id, is_new, cmd) = {
            let mut mgr = self.manager.write().await;
            if !mgr.telos_connected || !mgr.agent_ready {
                return Err("agent not connected or not ready".to_string());
            }
            let tid = mgr.get_or_create_thread(thread_id);
            let is_new = match mgr.threads.get(&tid) {
                Some(t) => t.messages.is_empty(),
                None => true,
            };
            mgr.set_title(&tid, message);
            let enriched = mgr.prepare_message(&tid, message);
            mgr.add_message(&tid, "user", message, None);
            let rid = uuid::Uuid::new_v4().to_string();
            let acp_id = mgr.get_acp_thread_id(&tid);
            let cmd = serde_json::json!({
                "type": "chat_message",
                "data": {
                    "message": enriched,
                    "request_id": rid.clone(),
                    "acp_thread_id": acp_id,
                    // The reasoning effort for this turn. Telos maps it onto the
                    // provider's own scale, and a null leaves the thread alone.
                    "thinking_effort": thinking_effort,
                }
            })
            .to_string();
            mgr.pending_requests.insert(rid.clone(), tid.clone());
            (tid, rid, is_new, cmd)
        };

        {
            let guard = self.ws_tx.lock().await;
            match &*guard {
                Some(tx) => {
                    if let Err(e) = tx.send(cmd.clone()) {
                        // The request never reached the agent; drop the
                        // mapping so it does not leak and the health
                        // monitor does not treat this as an active turn.
                        let mut mgr = self.manager.write().await;
                        mgr.pending_requests.remove(&request_id);
                        return Err(e.to_string());
                    }
                }
                None => {
                    let mut mgr = self.manager.write().await;
                    mgr.pending_requests.remove(&request_id);
                    return Err("WebSocket not connected".to_string());
                }
            }
        }

        // Queue for resend after a reconnect, same as the previous
        // server-side flow.
        {
            let mut mgr = self.manager.write().await;
            mgr.pending_chat_queue
                .push((request_id.clone(), thread_id.clone(), cmd));
        }

        // When resuming a thread whose platform thread id is not yet
        // established, wait for `thread_created` (bounded) so the caller
        // can build its SSE stream after the mapping exists.
        let should_wait = {
            let mgr = self.manager.read().await;
            let no_acp = mgr.get_acp_thread_id(&thread_id).is_none();
            !is_new && no_acp && mgr.threads_activated.contains(&thread_id)
        };
        if should_wait {
            let waiter = Arc::new(Notify::new());
            {
                let mut mgr = self.manager.write().await;
                mgr.thread_waiters.insert(thread_id.clone(), waiter.clone());
            }
            tokio::select! {
                _ = waiter.notified() => {},
                _ = tokio::time::sleep(Duration::from_secs(20)) => {},
            }
            // The platform thread mapping was not established within the
            // bound. The message may or may not be processed, but its
            // events cannot be correlated, so fail the submit instead of
            // leaving the caller waiting on a turn that never resolves.
            let mapping_ok = self
                .manager
                .read()
                .await
                .get_acp_thread_id(&thread_id)
                .is_some();
            if !mapping_ok {
                let mut mgr = self.manager.write().await;
                mgr.pending_requests.remove(&request_id);
                mgr.pending_chat_queue
                    .retain(|(rid, _, _)| rid != &request_id);
                // Drop the waiter too, otherwise it stays mapped until a
                // thread_created event that may never arrive.
                mgr.thread_waiters.remove(&thread_id);
                return Err("timed out waiting for the platform thread mapping".to_string());
            }
        }

        {
            let mut mgr = self.manager.write().await;
            mgr.threads_activated.insert(thread_id.clone());
        }

        Ok(SubmitReceipt {
            thread_id,
            request_id,
            is_new,
        })
    }

    async fn cancel(&self) -> Result<(), String> {
        // Telos resolves the turn to cancel by request id, so an agent-wide cancel has
        // to name the turns this agent is running. One agent runs one turn at a time, so
        // that is normally one id.
        let live = self.live_requests().await;
        if live.is_empty() {
            // Nothing is in flight, so there is no turn to name and Telos answers the
            // empty command as a noop. Sending it keeps the one thing a cancel always
            // does, submitting a command.
            return self.send_command(cancel_command(None)).await;
        }
        for request_id in live {
            self.send_command(cancel_command(Some(&request_id))).await?;
        }
        Ok(())
    }

    async fn cancel_request(&self, request_id: &str) -> Result<(), String> {
        self.send_command(cancel_command(Some(request_id))).await
    }

    async fn thread(&self, thread_id: &str) -> Option<ThreadSession> {
        self.manager.read().await.threads.get(thread_id).cloned()
    }

    async fn threads(&self) -> Vec<ThreadSession> {
        self.manager
            .read()
            .await
            .threads
            .values()
            .cloned()
            .collect()
    }

    async fn subscribe(&self) -> watch::Receiver<u64> {
        self.manager.read().await.thread_notify.subscribe()
    }

    async fn pending_tool_calls(&self) -> Vec<PendingAuthorization> {
        self.manager
            .read()
            .await
            .pending_authorizations
            .values()
            .cloned()
            .collect()
    }

    async fn create_thread(&self) -> Result<String, String> {
        let mut mgr = self.manager.write().await;
        let tid = mgr.get_or_create_thread(None);
        Ok(tid)
    }

    async fn resolve_tool_call(
        &self,
        platform_thread_id: &str,
        tool_call_id: &str,
        allow: bool,
    ) -> Result<(), String> {
        // Clear the pending entry first so a repeated resolve is a no-op.
        {
            let mut mgr = self.manager.write().await;
            mgr.pending_authorizations.remove(tool_call_id);
        }
        let cmd = serde_json::json!({
            "type": "resolve_tool_call_authorization",
            "data": {
                "acp_thread_id": platform_thread_id,
                "tool_call_id": tool_call_id,
                "allow": allow,
            }
        })
        .to_string();
        self.send_command(cmd).await
    }
}

impl TelosBackend {
    /// The turns this agent is running, by request id.
    ///
    /// `pending_requests` holds a live mapping while a turn is in flight and a blank
    /// sentinel once it has been consumed, so a non-empty value is what "in flight"
    /// means. A stale entry that was never consumed costs nothing: Telos answers a
    /// request id it does not know with a noop.
    async fn live_requests(&self) -> Vec<String> {
        let mgr = self.manager.read().await;
        let mut live: Vec<String> = mgr
            .pending_requests
            .iter()
            .filter(|(_, thread_id)| !thread_id.is_empty())
            .map(|(request_id, _)| request_id.clone())
            .collect();
        live.sort();
        live
    }

    /// Send one command over the shared channel, which is cleared on disconnect, so a
    /// stale manager-side sender cannot silently drop it.
    async fn send_command(&self, cmd: String) -> Result<(), String> {
        let guard = self.ws_tx.lock().await;
        match &*guard {
            Some(tx) => tx.send(cmd).map_err(|e| e.to_string()),
            None => Err("WebSocket not connected".to_string()),
        }
    }
}
