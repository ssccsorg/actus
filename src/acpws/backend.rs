// AcpwsBackend — ACP/WebSocket adapter implementing the agent fabric trait.
//
// Runs one executor process behind the `AgentBackend` interface.
// ACP-over-WebSocket details (connection loop, event dispatch, reconnect)
// stay inside `acpws::control`; this adapter owns thread state, context
// injection, command submission, and the resume wait.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tokio::sync::{Notify, RwLock};

use crate::acpws::types::Command;
use crate::acpws::{AcpwsManager, WsCommandTx};
use crate::agent::{
    AgentBackend, AgentKind, AgentStatus, PendingAuthorization, SubmitReceipt, ThreadMessage,
    ThreadParent, ThreadSession, NOTE_ROLE,
};
use crate::store::RecordedMessage;

/// One executor agent instance behind the fabric interface.
pub struct AcpwsBackend {
    /// Name this instance answers to. The registry keys on it and routing
    /// looks it up, so it has to be the configured agent name: a constant
    /// would make every agent of this kind collide on one registry entry and leave
    /// all but the last unreachable.
    pub name: String,
    pub manager: Arc<RwLock<AcpwsManager>>,
    /// Shared command channel, kept in sync with `AcpwsManager::ws_tx` by
    /// `acpws::control`. Sending through the shared channel means a stale
    /// manager-side sender after a reconnect cannot silently drop a
    /// message.
    pub ws_tx: WsCommandTx,
    /// The project this agent was launched for. The executor is told one workdir at
    /// launch and opens a worktree for it, so this is what its threads belong
    /// to; the backend answers with it rather than the listing deriving it
    /// from the configuration, which is what keeps the notion a backend's.
    pub scope: Option<String>,
}

/// The wire command that cancels a turn.
///
/// The executor resolves the turn from `request_id`: the command names no thread, so a body
/// without one names no turn, which is why a cancel that sent an empty body could not
/// stop anything and was answered as a noop. The empty form is kept for the case where
/// no turn is in flight.
fn cancel_command(request_id: Option<&str>) -> Result<String, String> {
    Command::CancelCurrentTurn {
        request_id: request_id.map(str::to_string),
    }
    .to_json()
}

#[async_trait::async_trait]
impl AgentBackend for AcpwsBackend {
    fn name(&self) -> &str {
        &self.name
    }

    fn kind(&self) -> AgentKind {
        AgentKind::Acpws
    }

    fn scope(&self) -> Option<String> {
        self.scope.clone()
    }

    async fn record_store(&self) -> String {
        self.manager.read().await.record.describe()
    }

    async fn status(&self) -> AgentStatus {
        let mgr = self.manager.read().await;
        // Read before the struct literal, because the borrow's temporary has to be dropped
        // while `mgr` is still alive.
        let thread_version = *mgr.thread_notify.borrow();
        AgentStatus {
            name: self.name().to_string(),
            kind: self.kind(),
            connected: mgr.agent_connected,
            ready: mgr.agent_ready,
            capabilities: self.kind().capabilities(),
            last_error: None,
            thread_version,
        }
    }

    async fn submit(
        &self,
        thread_id: Option<&str>,
        message: &str,
    ) -> Result<SubmitReceipt, String> {
        self.submit_with_options(thread_id, message, None, None, None)
            .await
    }

    async fn submit_with_options(
        &self,
        thread_id: Option<&str>,
        message: &str,
        _parent: Option<ThreadParent>,
        thinking_effort: Option<&str>,
        author: Option<&str>,
    ) -> Result<SubmitReceipt, String> {
        // Prepare thread state under the write lock, then release before
        // sending so the command channel is not blocked by thread work.
        let (thread_id, request_id, is_new, cmd) = {
            let mut mgr = self.manager.write().await;
            if !mgr.agent_connected || !mgr.agent_ready {
                return Err("agent not connected or not ready".to_string());
            }
            let tid = mgr.get_or_create_thread(thread_id);
            // A thread the index holds has no messages in hand, so the count is what says
            // whether this is its first turn. Read off the messages, every resumed thread
            // would read as new.
            let is_new = match mgr.message_count(&tid) {
                Ok(n) => n == 0,
                Err(e) => {
                    tracing::warn!("cannot count the messages of {tid}: {e}");
                    true
                }
            };
            mgr.set_title(&tid, message);
            let enriched = mgr.prepare_message(&tid, message);
            mgr.add_message_full(&tid, "user", message, None, None, None, None, author);
            let rid = uuid::Uuid::new_v4().to_string();
            let acp_id = mgr.get_acp_thread_id(&tid);
            let cmd = Command::ChatMessage {
                acp_thread_id: acp_id,
                message: enriched,
                request_id: rid.clone(),
                // The reasoning effort for this turn. The executor maps it onto the
                // provider's own scale, and a null leaves the thread alone.
                thinking_effort: thinking_effort.map(str::to_string),
            }
            .to_json()?;
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
            // From here the turn is in flight, so the thread is not a completed one.
            // Set after every path that can still fail the submit, so a submit that
            // never reached the agent does not leave a thread reading as running.
            if let Some(thread) = mgr.threads.get_mut(&thread_id) {
                thread.completed = false;
            }
            mgr.threads_activated.insert(thread_id.clone());
        }

        Ok(SubmitReceipt {
            thread_id,
            request_id,
            is_new,
        })
    }

    async fn cancel(&self) -> Result<(), String> {
        // The executor resolves the turn to cancel by request id, so an agent-wide cancel has
        // to name the turns this agent is running. One agent runs one turn at a time, so
        // that is normally one id.
        let live = self.live_requests().await;
        if live.is_empty() {
            // Nothing is in flight, so there is no turn to name and the executor answers the
            // empty command as a noop. Sending it keeps the one thing a cancel always
            // does, submitting a command.
            return self.send_command(cancel_command(None)?).await;
        }
        for request_id in live {
            self.send_command(cancel_command(Some(&request_id))?)
                .await?;
        }
        Ok(())
    }

    async fn cancel_request(&self, request_id: &str) -> Result<(), String> {
        self.send_command(cancel_command(Some(request_id))?).await
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

    async fn message_count(&self, thread_id: &str) -> usize {
        self.manager
            .read()
            .await
            .message_count(thread_id)
            .unwrap_or(0)
    }

    async fn messages_between(
        &self,
        since: Option<u64>,
        until: Option<u64>,
    ) -> Result<Vec<RecordedMessage>, String> {
        // The record is cloned out of the manager so the read, which reaches the medium,
        // does not hold the lock a turn needs.
        let record = self.manager.read().await.record.clone();
        record.between(since, until)
    }

    async fn messages_window(
        &self,
        thread_id: &str,
        from: usize,
        limit: usize,
    ) -> Vec<ThreadMessage> {
        self.manager
            .read()
            .await
            .window(thread_id, from, limit)
            .unwrap_or_default()
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

    async fn append_note(
        &self,
        thread_id: Option<&str>,
        content: &str,
        author: Option<&str>,
    ) -> Result<String, String> {
        // The record is the only thing this touches: no command goes to the agent, so an agent
        // that is not connected is not in the way, and what is pending afterwards is what was
        // pending before. A note opens a thread the way a message does, and names it, because
        // the first thing said in a thread is what the thread is called.
        let mut mgr = self.manager.write().await;
        let tid = mgr.get_or_create_thread(thread_id);
        mgr.set_title(&tid, content);
        mgr.add_message_full(
            &tid,
            NOTE_ROLE,
            content,
            Some(uuid::Uuid::new_v4().to_string()),
            None,
            None,
            None,
            author,
        );
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
        let cmd = Command::ResolveToolCallAuthorization {
            acp_thread_id: platform_thread_id.to_string(),
            allow,
            tool_call_id: tool_call_id.to_string(),
        }
        .to_json()?;
        self.send_command(cmd).await
    }
}

impl AcpwsBackend {
    /// The turns this agent is running, by request id.
    ///
    /// `pending_requests` holds a live mapping while a turn is in flight and a blank
    /// sentinel once it has been consumed, so a non-empty value is what "in flight"
    /// means. A stale entry that was never consumed costs nothing: the executor answers a
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
