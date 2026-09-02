// ZedBackend — ACP/WebSocket adapter implementing the agent fabric trait.
//
// Runs one headless Zed process behind the `AgentBackend` interface.
// ACP-over-WebSocket details (connection loop, event dispatch, reconnect)
// stay inside `zed::control`; this adapter owns thread state, context
// injection, command submission, and the resume wait.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tokio::sync::{Notify, RwLock};

use crate::agent::{
    AgentBackend, AgentKind, AgentStatus, PendingAuthorization, SubmitReceipt, ThreadSession,
};
use crate::zed::{WsCommandTx, ZedManager};

/// One headless Zed agent instance behind the fabric interface.
pub struct ZedBackend {
    pub manager: Arc<RwLock<ZedManager>>,
    /// Shared command channel, kept in sync with `ZedManager::ws_tx` by
    /// `zed::control`. Sending through the shared channel means a stale
    /// manager-side sender after a reconnect cannot silently drop a
    /// message.
    pub ws_tx: WsCommandTx,
}

#[async_trait::async_trait]
impl AgentBackend for ZedBackend {
    fn name(&self) -> &str {
        "zed"
    }

    fn kind(&self) -> AgentKind {
        AgentKind::Zed
    }

    async fn status(&self) -> AgentStatus {
        let mgr = self.manager.read().await;
        AgentStatus {
            name: self.name().to_string(),
            kind: self.kind(),
            connected: mgr.zed_connected,
            ready: mgr.agent_ready,
        }
    }

    async fn submit(
        &self,
        thread_id: Option<&str>,
        message: &str,
    ) -> Result<SubmitReceipt, String> {
        // Prepare thread state under the write lock, then release before
        // sending so the command channel is not blocked by thread work.
        let (thread_id, request_id, is_new, cmd) = {
            let mut mgr = self.manager.write().await;
            if !mgr.zed_connected || !mgr.agent_ready {
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
        let cmd = serde_json::json!({
            "type": "cancel_current_turn",
            "data": {}
        })
        .to_string();
        // Send through the shared channel: it is cleared on disconnect,
        // so a stale manager-side sender cannot silently drop the cancel.
        let guard = self.ws_tx.lock().await;
        match &*guard {
            Some(tx) => tx.send(cmd).map_err(|e| e.to_string()),
            None => Err("WebSocket not connected".to_string()),
        }
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
        let guard = self.ws_tx.lock().await;
        match &*guard {
            Some(tx) => tx.send(cmd).map_err(|e| e.to_string()),
            None => Err("WebSocket not connected".to_string()),
        }
    }
}
