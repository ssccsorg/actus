// ExtCliAgent: generic raw-CLI adapter for the agent fabric.
//
// Any external binary with a command-line interface can be attached as an
// auxiliary agent: per turn, actus spawns `bin <cli_args...> <prompt>` as
// a child process in the workdir, with the process environment inherited
// so provider credentials pass through. The finished output is recorded
// in the thread and the completion is announced on the notify channel, so
// the HTTP polling and SSE flows behave like every other backend.
//
// The first attached binary is Ante (bin `ante`, `cli_args = ["-p"]`),
// the trial auxiliary agent for lightweight input/output tasks next to
// the professional Telos core. The adapter itself carries no product
// name: any CLI that accepts a prompt as its final argument plugs in with
// no code change.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::AsyncReadExt;
use tokio::process::Child;
use tokio::sync::{watch, Mutex, RwLock};

use crate::agent::{
    truncate_title, AgentBackend, AgentKind, AgentStatus, PendingAuthorization, SubmitReceipt,
    ThreadMessage, ThreadSession,
};

/// Cap on the recorded assistant message, in characters. A CLI can emit
/// long output; a thread message stays bounded.
const MAX_REPLY_CHARS: usize = 200_000;

pub struct ExtCliAgent {
    name: String,
    bin: PathBuf,
    args: Vec<String>,
    timeout: Duration,
    workdir: PathBuf,
    threads: Arc<RwLock<HashMap<String, ThreadSession>>>,
    running: Arc<Mutex<Option<Child>>>,
    notify: watch::Sender<u64>,
}

impl ExtCliAgent {
    pub fn new(
        name: impl Into<String>,
        bin: PathBuf,
        args: Vec<String>,
        timeout_secs: u64,
        workdir: PathBuf,
    ) -> Self {
        let (notify, _) = watch::channel(0u64);
        Self {
            name: name.into(),
            bin,
            args,
            timeout: Duration::from_secs(timeout_secs.max(1)),
            workdir,
            threads: Arc::new(RwLock::new(HashMap::new())),
            running: Arc::new(Mutex::new(None)),
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
                let tid = format!("cli-{}", uuid::Uuid::new_v4());
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
impl AgentBackend for ExtCliAgent {
    fn name(&self) -> &str {
        &self.name
    }

    fn kind(&self) -> AgentKind {
        AgentKind::ExtCli
    }

    async fn status(&self) -> AgentStatus {
        let present = self.bin.exists();
        AgentStatus {
            name: self.name.clone(),
            kind: AgentKind::ExtCli,
            connected: present,
            ready: present,
        }
    }

    async fn submit(
        &self,
        thread_id: Option<&str>,
        message: &str,
    ) -> Result<SubmitReceipt, String> {
        if self.running.lock().await.is_some() {
            return Err(format!(
                "agent '{}' busy: a turn is already running",
                self.name
            ));
        }

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
            session.completed = false;
        }

        let mut cmd = tokio::process::Command::new(&self.bin);
        cmd.args(&self.args)
            .arg(message)
            .current_dir(&self.workdir)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        let child = cmd
            .spawn()
            .map_err(|e| format!("cannot start {}: {}", self.bin.display(), e))?;
        {
            let mut running = self.running.lock().await;
            *running = Some(child);
        }

        let threads = self.threads.clone();
        let running = self.running.clone();
        let notify = self.notify.clone();
        let timeout = self.timeout;
        let task_thread_id = tid.clone();
        let task_request_id = request_id.clone();

        tokio::spawn(async move {
            let child = running.lock().await.take();
            let content = match child {
                None => "[ext-cli] cancelled".to_string(),
                Some(mut child) => {
                    let out_pipe = child.stdout.take();
                    let err_pipe = child.stderr.take();
                    let waited = {
                        let mut wait = Box::pin(child.wait());
                        let mut out = Box::pin(read_pipe(out_pipe));
                        let mut err = Box::pin(read_pipe(err_pipe));
                        let joined = async { tokio::join!(&mut wait, &mut out, &mut err) };
                        tokio::time::timeout(timeout, joined).await
                    };
                    match waited {
                        Ok((wait_result, stdout, stderr)) => match wait_result {
                            Ok(status) if status.success() => {
                                let text = String::from_utf8_lossy(&stdout);
                                let text = text.trim();
                                if text.is_empty() {
                                    "[ext-cli] finished with no output".to_string()
                                } else {
                                    text.chars().take(MAX_REPLY_CHARS).collect()
                                }
                            }
                            Ok(status) => {
                                let stderr = String::from_utf8_lossy(&stderr);
                                let stderr = stderr.trim();
                                if stderr.is_empty() {
                                    format!("[ext-cli] exited with {}", status)
                                } else {
                                    format!(
                                        "[ext-cli] exited with {}: {}",
                                        status,
                                        stderr.chars().take(MAX_REPLY_CHARS).collect::<String>()
                                    )
                                }
                            }
                            Err(e) => format!("[ext-cli] failed: {}", e),
                        },
                        Err(_) => {
                            let _ = child.kill().await;
                            format!("[ext-cli] timed out after {}s", timeout.as_secs())
                        }
                    }
                }
            };

            let now = chrono::Utc::now();
            let mut threads = threads.write().await;
            if let Some(session) = threads.get_mut(&task_thread_id) {
                session.messages.push(ThreadMessage {
                    role: "assistant".to_string(),
                    content,
                    message_id: Some(task_request_id),
                    entry_type: Some("agent_message".to_string()),
                    tool_name: None,
                    tool_status: None,
                    timestamp: now,
                });
                session.completed = true;
                session.turn_completed += 1;
            }
            drop(threads);
            let _ = notify.send(now.timestamp_millis() as u64);
        });

        Ok(SubmitReceipt {
            thread_id: tid,
            request_id,
            is_new,
        })
    }

    async fn cancel(&self) -> Result<(), String> {
        let mut running = self.running.lock().await;
        if let Some(mut child) = running.take() {
            let _ = child.kill().await;
        }
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

/// Drain a captured child pipe to EOF.
async fn read_pipe<R: tokio::io::AsyncRead + Unpin>(mut pipe: Option<R>) -> Vec<u8> {
    match pipe.as_mut() {
        Some(reader) => {
            let mut buf = Vec::new();
            let _ = reader.read_to_end(&mut buf).await;
            buf
        }
        None => Vec::new(),
    }
}
