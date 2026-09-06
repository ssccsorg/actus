// ExtCliAgent: declarative raw-CLI adapter for the agent fabric.
//
// Any external binary with a command-line interface can be attached as an
// auxiliary agent without code changes. Per turn, actus spawns the binary
// with the agent's declared argument template and records the finished
// output in the thread. One-shot acts are parallel: separate turns run as
// separate child processes and complete independently.
//
// The invocation contract is declarative, because CLIs differ:
//   - `cli_args` may carry a `{prompt}` marker that is replaced by the
//     message; without a marker the message is appended as the final
//     argument (for example Ante: ["-p", "{prompt}"]).
//   - `cli_prompt = "stdin"` writes the message to the child's stdin
//     instead of passing it as an argument.
//   - `cli_env` adds per-agent environment over the inherited server
//     environment, so each CLI can carry its own credentials.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Child;
use tokio::sync::{watch, Mutex, RwLock};

use crate::agent::config::PromptMode;
use crate::agent::{
    truncate_title, AgentBackend, AgentKind, AgentStatus, PendingAuthorization, SubmitReceipt,
    ThreadMessage, ThreadSession,
};

/// Cap on the recorded assistant message, in characters. A CLI can emit
/// long output; a thread message stays bounded.
const MAX_REPLY_CHARS: usize = 200_000;

/// Marker replaced by the prompt message inside `cli_args`.
const PROMPT_MARKER: &str = "{prompt}";

/// Poll interval for child exit and cancellation checks.
const CHILD_POLL_MS: u64 = 100;

/// How one spawned turn ended.
enum ChildOutcome {
    Cancelled,
    TimedOut,
    Exited(Result<std::process::ExitStatus, std::io::Error>),
}

/// Kill the direct child and its process group, then reap it. The direct
/// child may be a shell or launcher that spawned its own children;
/// orphaned grandchildren would keep the output pipes open until they
/// exit on their own and delay the recorded reply.
#[cfg(unix)]
async fn kill_child_group(child: &mut tokio::process::Child) {
    if let Some(pid) = child.id() {
        let _ = child.kill().await;
        unsafe {
            libc::kill(-(pid as i32), libc::SIGKILL);
        }
        let _ = child.wait().await;
    }
}

pub struct ExtCliAgent {
    name: String,
    bin: PathBuf,
    args: Vec<String>,
    env: HashMap<String, String>,
    prompt_mode: PromptMode,
    timeout: Duration,
    workdir: PathBuf,
    threads: Arc<RwLock<HashMap<String, ThreadSession>>>,
    running: Arc<Mutex<HashMap<String, Child>>>,
    notify: watch::Sender<u64>,
}

impl ExtCliAgent {
    pub fn new(
        name: impl Into<String>,
        bin: PathBuf,
        args: Vec<String>,
        env: HashMap<String, String>,
        prompt_mode: PromptMode,
        timeout_secs: u64,
        workdir: PathBuf,
    ) -> Self {
        let (notify, _) = watch::channel(0u64);
        Self {
            name: name.into(),
            bin,
            args,
            env,
            prompt_mode,
            timeout: Duration::from_secs(timeout_secs.max(1)),
            workdir,
            threads: Arc::new(RwLock::new(HashMap::new())),
            running: Arc::new(Mutex::new(HashMap::new())),
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
            capabilities: AgentKind::ExtCli.capabilities(),
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
            session.completed = false;
        }

        // Resolve the argument template for this turn.
        let mut cmd_args: Vec<String> = Vec::with_capacity(self.args.len() + 1);
        let mut replaced = false;
        for arg in &self.args {
            if arg.contains(PROMPT_MARKER) {
                cmd_args.push(arg.replace(PROMPT_MARKER, message));
                replaced = true;
            } else {
                cmd_args.push(arg.clone());
            }
        }
        if self.prompt_mode == PromptMode::Arg && !replaced {
            cmd_args.push(message.to_string());
        }

        let mut cmd = tokio::process::Command::new(&self.bin);
        #[cfg(unix)]
        cmd.process_group(0);
        cmd.args(&cmd_args)
            .current_dir(&self.workdir)
            .envs(resolve_env(&self.env))
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        if self.prompt_mode == PromptMode::Stdin {
            cmd.stdin(std::process::Stdio::piped());
        } else {
            cmd.stdin(std::process::Stdio::null());
        }
        let mut child = cmd
            .spawn()
            .map_err(|e| format!("cannot start {}: {}", self.bin.display(), e))?;

        if self.prompt_mode == PromptMode::Stdin {
            if let Some(mut stdin) = child.stdin.take() {
                let mut payload = message.as_bytes().to_vec();
                payload.push(b'\n');
                let _ = stdin.write_all(&payload).await;
                drop(stdin);
            }
        }

        // Detach the output pipes before the child handle moves into
        // `running`; the worker drains them while the handle stays in the
        // map so cancel paths can reach it.
        let out_pipe = child.stdout.take();
        let err_pipe = child.stderr.take();
        {
            let mut running = self.running.lock().await;
            running.insert(request_id.clone(), child);
        }

        let threads = self.threads.clone();
        let running = self.running.clone();
        let notify = self.notify.clone();
        let timeout = self.timeout;
        let task_thread_id = tid.clone();
        let task_request_id = request_id.clone();

        tokio::spawn(async move {
            // Drain stdout and stderr concurrently while the child runs.
            // The child handle stays in `running` under its request id for
            // the whole turn, so cancel paths can kill and reap it; the
            // poll loop below reaps the child when it exits on its own.
            let out_task = tokio::spawn(read_pipe(out_pipe));
            let err_task = tokio::spawn(read_pipe(err_pipe));
            let deadline = tokio::time::Instant::now() + timeout;

            let outcome = loop {
                let mut guard = running.lock().await;
                // A missing entry means a cancel path removed the child
                // while this loop slept.
                let Some(child_ref) = guard.get_mut(&task_request_id) else {
                    break ChildOutcome::Cancelled;
                };
                match child_ref.try_wait() {
                    Ok(Some(status)) => {
                        guard.remove(&task_request_id);
                        break ChildOutcome::Exited(Ok(status));
                    }
                    Ok(None) => {
                        if tokio::time::Instant::now() >= deadline {
                            if let Some(mut child) = guard.remove(&task_request_id) {
                                kill_child_group(&mut child).await;
                            }
                            break ChildOutcome::TimedOut;
                        }
                    }
                    Err(e) => {
                        guard.remove(&task_request_id);
                        break ChildOutcome::Exited(Err(e));
                    }
                }
                drop(guard);
                tokio::time::sleep(Duration::from_millis(CHILD_POLL_MS)).await;
            };

            let mut stdout = Vec::new();
            let mut stderr = Vec::new();
            if matches!(outcome, ChildOutcome::Exited(_)) {
                stdout = out_task.await.unwrap_or_default();
                stderr = err_task.await.unwrap_or_default();
            } else {
                // The group kill above closed the pipes; abort the
                // readers so a straggler holding a pipe cannot delay the
                // recorded cancellation or timeout marker.
                out_task.abort();
                err_task.abort();
            }
            let content = match outcome {
                ChildOutcome::Cancelled => "[ext-cli] cancelled".to_string(),
                ChildOutcome::TimedOut => {
                    format!("[ext-cli] timed out after {}s", timeout.as_secs())
                }
                ChildOutcome::Exited(result) => match result {
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
        for (_, mut child) in running.drain() {
            kill_child_group(&mut child).await;
        }
        Ok(())
    }

    async fn cancel_request(&self, request_id: &str) -> Result<(), String> {
        // Kill and reap only the child of the named turn. A turn that
        // already finished has no entry left and cancelling it is a
        // no-op. The poll loop of that turn observes the missing entry
        // and records a cancellation marker.
        let mut running = self.running.lock().await;
        if let Some(mut child) = running.remove(request_id) {
            kill_child_group(&mut child).await;
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

/// Resolve declared environment for the child. A value of the form
/// `$NAME` is replaced by the server process environment variable NAME,
/// so profiles can map credentials without duplicating secrets, for
/// example `{ ANTE_API_KEY = \"$LLM_API_KEY\" }`. Other values pass
/// through literally. Inherited environment is always kept.
fn resolve_env(
    declared: &std::collections::HashMap<String, String>,
) -> std::collections::HashMap<String, String> {
    let mut resolved = std::collections::HashMap::with_capacity(declared.len());
    for (key, value) in declared {
        if let Some(name) = value.strip_prefix('$') {
            resolved.insert(key.clone(), std::env::var(name).unwrap_or_default());
        } else {
            resolved.insert(key.clone(), value.clone());
        }
    }
    resolved
}
