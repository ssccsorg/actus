// BrowserAgent — a fill-only browser executor behind the agent fabric.
//
// A turn is a plan: JSON steps drawn from the operations this adapter
// implements. The implemented set is the policy. There is no click, no key
// press, no JavaScript, and no submit anywhere in it, so a turn opens a
// page, reads it, and enters values into fields, and a person submits. An
// operation the adapter does not implement fails to parse before any process
// starts, and the option that names operations can only narrow the set.
//
// Every step is one `agent-browser` invocation, so the CLI's daemon owns the
// browser and this adapter stays a wrapper: it builds the argv, enforces the
// per-step ceiling, publishes the running process so a cancel can stop it,
// and records what each step printed. It reads no knowledge base: the values
// it types arrive in the plan, from whoever planned the turn.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::{watch, Mutex, RwLock};

use crate::agent::adapter::{AgentFactory, LaunchContext, LaunchedAgent};
use crate::agent::config::AgentSpec;
use crate::agent::{
    truncate_title, AgentBackend, AgentCapabilities, AgentStatus, PendingAuthorization,
    SubmitReceipt, ThreadMessage, ThreadSession,
};

/// The operations this adapter implements, which is the whole of its policy.
/// `click`, `press`, `eval`, and `submit` are absent by construction: no step
/// variant exists for them, so no configuration can name them.
pub const IMPLEMENTED_OPS: [&str; 12] = [
    "open",
    "snapshot",
    "fill",
    "get_attr",
    "get_text",
    "get_value",
    "get_title",
    "get_url",
    "screenshot",
    "tabs",
    "wait",
    "close",
];

/// Ceiling for a plan's `wait` step. A page settles in seconds; a plan that
/// asks for longer is a plan that means to hold the browser.
const MAX_WAIT_MS: u64 = 30_000;

/// Ceiling for one step's recorded output. A snapshot is long, and a thread
/// message stays bounded.
const MAX_DETAIL_CHARS: usize = 4000;

fn default_bin() -> PathBuf {
    PathBuf::from("agent-browser")
}

fn default_timeout() -> u64 {
    60
}

/// The declaration a browser agent reads from its options table.
#[derive(Deserialize)]
pub struct BrowserOptions {
    /// `agent-browser` binary path or PATH name.
    #[serde(default = "default_bin")]
    pub bin: PathBuf,
    /// CDP endpoint (port or url) of a browser to drive. Unset means the CLI
    /// launches the browser it manages for the session.
    #[serde(default)]
    pub cdp: Option<String>,
    /// Show the browser window. A person watches the fill and submits.
    #[serde(default)]
    pub headed: Option<bool>,
    /// Chrome profile to reuse login state from.
    #[serde(default)]
    pub profile: Option<String>,
    /// Session name; isolates one browser and its tabs from other agents.
    /// Defaults to the agent name.
    #[serde(default)]
    pub session: Option<String>,
    /// Page init script, registered before the first navigation. This is
    /// where a DOM-level submit guard goes when a deployment wants one.
    #[serde(default)]
    pub init_script: Option<PathBuf>,
    /// Restrict network destinations, passed through as the CLI's
    /// `--allowed-domains`.
    #[serde(default)]
    pub domains: Option<Vec<String>>,
    /// Narrow the implemented operation set, for example to
    /// `["open", "snapshot", "fill"]`. Naming an operation the adapter does
    /// not implement fails the config load.
    #[serde(default)]
    pub ops: Option<Vec<String>>,
    /// Environment for the browser CLI, for the variables it reads
    /// (`AGENT_BROWSER_*` and the like). A value of the form `$NAME` is
    /// resolved from the server environment when the agent starts, so a
    /// credential stays out of the config file.
    #[serde(default)]
    pub env: HashMap<String, String>,
    /// Per-step timeout in seconds.
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,
}

/// One step of a plan. The variants are the vocabulary this adapter
/// implements; a plan that names anything else does not parse.
#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Step {
    /// Navigate to a URL.
    Open {
        url: String,
    },
    /// Accessibility snapshot with element refs, for the next step or for a
    /// person to read.
    Snapshot {
        #[serde(default)]
        interactive: bool,
    },
    /// Clear a field and enter a value.
    Fill {
        target: String,
        value: String,
    },
    /// Read an attribute of a field, such as `maxlength`.
    GetAttr {
        target: String,
        name: String,
    },
    GetText {
        target: String,
    },
    GetValue {
        target: String,
    },
    GetTitle,
    GetUrl,
    /// Save a screenshot, so the reviewed state is on disk.
    Screenshot {
        #[serde(default)]
        path: Option<String>,
        #[serde(default)]
        full: bool,
    },
    /// List the session's tabs.
    Tabs,
    /// Pause, so a slow page can settle.
    Wait {
        ms: u64,
    },
    /// Close the browser.
    Close,
}

impl Step {
    /// The operation name, as it appears in a plan and in a record.
    pub fn name(&self) -> &'static str {
        match self {
            Step::Open { .. } => "open",
            Step::Snapshot { .. } => "snapshot",
            Step::Fill { .. } => "fill",
            Step::GetAttr { .. } => "get_attr",
            Step::GetText { .. } => "get_text",
            Step::GetValue { .. } => "get_value",
            Step::GetTitle => "get_title",
            Step::GetUrl => "get_url",
            Step::Screenshot { .. } => "screenshot",
            Step::Tabs => "tabs",
            Step::Wait { .. } => "wait",
            Step::Close => "close",
        }
    }
}

#[derive(Deserialize)]
struct Plan {
    steps: Vec<Step>,
}

/// What one turn records: the operations it ran and what each printed, so the
/// state a person reviews before submitting is inspectable.
#[derive(Serialize)]
struct TurnRecord {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    steps: Vec<StepRecord>,
}

#[derive(Serialize)]
struct StepRecord {
    op: &'static str,
    ok: bool,
    #[serde(skip_serializing_if = "String::is_empty")]
    detail: String,
}

/// What a running turn is doing, so a cancel can stop it.
struct TurnState {
    /// Process id of the step in flight, when one is.
    child: Option<u32>,
    cancelled: bool,
}

enum StepOutcome {
    Done(String),
    Failed(String),
    Cancelled,
}

/// Everything one turn needs to run, cloned out of the agent so the turn can
/// proceed in the background while the agent keeps serving.
#[derive(Clone)]
struct TurnRunner {
    bin: PathBuf,
    /// Global arguments (`--session`, `--cdp`, `--profile`, ...) placed before
    /// every subcommand.
    globals: Vec<String>,
    env: HashMap<String, String>,
    /// The operation set this agent accepts; None means all implemented ones.
    ops: Option<Vec<String>>,
    timeout: Duration,
    workdir: PathBuf,
    running: Arc<Mutex<HashMap<String, TurnState>>>,
}

impl TurnRunner {
    /// The argv of one step: the shared globals, then the subcommand.
    fn argv(&self, step: &Step) -> Vec<String> {
        let mut args = self.globals.clone();
        match step {
            Step::Open { url } => {
                args.push("open".to_string());
                args.push(url.clone());
            }
            Step::Snapshot { interactive } => {
                args.push("snapshot".to_string());
                if *interactive {
                    args.push("-i".to_string());
                }
            }
            Step::Fill { target, value } => {
                args.push("fill".to_string());
                args.push(target.clone());
                args.push(value.clone());
            }
            Step::GetAttr { target, name } => {
                args.push("get".to_string());
                args.push("attr".to_string());
                args.push(target.clone());
                args.push(name.clone());
            }
            Step::GetText { target } => {
                args.push("get".to_string());
                args.push("text".to_string());
                args.push(target.clone());
            }
            Step::GetValue { target } => {
                args.push("get".to_string());
                args.push("value".to_string());
                args.push(target.clone());
            }
            Step::GetTitle => {
                args.push("get".to_string());
                args.push("title".to_string());
            }
            Step::GetUrl => {
                args.push("get".to_string());
                args.push("url".to_string());
            }
            Step::Screenshot { path, full } => {
                args.push("screenshot".to_string());
                if let Some(path) = path {
                    args.push(path.clone());
                }
                if *full {
                    args.push("--full".to_string());
                }
            }
            Step::Tabs => {
                args.push("tab".to_string());
                args.push("list".to_string());
            }
            Step::Wait { ms } => {
                args.push("wait".to_string());
                args.push(ms.to_string());
            }
            Step::Close => args.push("close".to_string()),
        }
        args
    }

    /// Refusals that need no process: an operation outside this agent's set,
    /// and a pause over the ceiling. The plan's own vocabulary is enforced by
    /// the parser, which knows no other operations.
    fn check_plan(&self, steps: &[Step]) -> Result<(), String> {
        for step in steps {
            if let Some(ops) = &self.ops {
                if !ops.iter().any(|op| op == step.name()) {
                    return Err(format!(
                        "the plan asks for '{}', and this agent runs only: {}",
                        step.name(),
                        ops.join(", ")
                    ));
                }
            }
            if let Step::Wait { ms } = step {
                if *ms > MAX_WAIT_MS {
                    return Err(format!("wait {ms} ms exceeds the {MAX_WAIT_MS} ms ceiling"));
                }
            }
        }
        Ok(())
    }

    async fn is_cancelled(&self, request_id: &str) -> bool {
        self.running
            .lock()
            .await
            .get(request_id)
            .map(|state| state.cancelled)
            .unwrap_or(false)
    }

    /// Run the plan, one step at a time, stopping at the first failure: a
    /// fill after an open that did not happen would land nowhere.
    async fn run_plan(&self, request_id: &str, steps: &[Step]) -> TurnRecord {
        let mut records: Vec<StepRecord> = Vec::with_capacity(steps.len());
        for step in steps {
            if self.is_cancelled(request_id).await {
                return TurnRecord {
                    ok: false,
                    error: Some("cancelled".to_string()),
                    steps: records,
                };
            }
            let args = self.argv(step);
            match self.run_step(request_id, &args).await {
                StepOutcome::Done(detail) => records.push(StepRecord {
                    op: step.name(),
                    ok: true,
                    detail,
                }),
                StepOutcome::Failed(detail) => {
                    records.push(StepRecord {
                        op: step.name(),
                        ok: false,
                        detail,
                    });
                    return TurnRecord {
                        ok: false,
                        error: None,
                        steps: records,
                    };
                }
                StepOutcome::Cancelled => {
                    return TurnRecord {
                        ok: false,
                        error: Some("cancelled".to_string()),
                        steps: records,
                    }
                }
            }
        }
        TurnRecord {
            ok: true,
            error: None,
            steps: records,
        }
    }

    /// Run one step: build the process, publish it for a cancel, enforce the
    /// ceiling, and return what it printed.
    async fn run_step(&self, request_id: &str, args: &[String]) -> StepOutcome {
        let mut command = tokio::process::Command::new(&self.bin);
        command
            .args(args)
            .envs(&self.env)
            .current_dir(&self.workdir)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let child = match command.spawn() {
            Ok(child) => child,
            Err(e) => {
                return StepOutcome::Failed(format!("cannot start {}: {}", self.bin.display(), e))
            }
        };

        // Publish the process so a cancel can stop this step. A cancel that
        // arrived while the process was starting is answered here, before the
        // step runs.
        {
            let mut running = self.running.lock().await;
            match running.get_mut(request_id) {
                Some(state) if state.cancelled => {
                    if let Some(pid) = child.id() {
                        unsafe {
                            libc::kill(pid as i32, libc::SIGKILL);
                        }
                    }
                    let mut child = child;
                    let _ = child.wait().await;
                    return StepOutcome::Cancelled;
                }
                Some(state) => state.child = child.id(),
                None => {}
            }
        }

        let result = tokio::time::timeout(self.timeout, child.wait_with_output()).await;
        {
            let mut running = self.running.lock().await;
            if let Some(state) = running.get_mut(request_id) {
                state.child = None;
            }
        }
        if self.is_cancelled(request_id).await {
            return StepOutcome::Cancelled;
        }
        match result {
            Ok(Ok(output)) => {
                let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
                let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
                if output.status.success() {
                    StepOutcome::Done(cap_detail(stdout))
                } else {
                    let code = output
                        .status
                        .code()
                        .map(|code| code.to_string())
                        .unwrap_or_else(|| "a signal".to_string());
                    let detail = if stderr.is_empty() {
                        format!("`{}` exited with {}", args.join(" "), code)
                    } else {
                        format!(
                            "`{}` exited with {}: {}",
                            args.join(" "),
                            code,
                            cap_detail(stderr)
                        )
                    };
                    StepOutcome::Failed(detail)
                }
            }
            Ok(Err(e)) => {
                StepOutcome::Failed(format!("cannot wait for {}: {}", self.bin.display(), e))
            }
            Err(_) => StepOutcome::Failed(format!(
                "`{}` timed out after {}s",
                args.join(" "),
                self.timeout.as_secs()
            )),
        }
    }
}

/// One browser agent instance: the CLI it drives, the threads it records, and
/// the lock that keeps one turn on the browser at a time.
pub struct BrowserAgent {
    name: String,
    runner: TurnRunner,
    /// Turns run one at a time: one browser is one page, and two turns
    /// interleaving would fill the same fields twice.
    turn_lock: Arc<Mutex<()>>,
    threads: Arc<RwLock<HashMap<String, ThreadSession>>>,
    notify: watch::Sender<u64>,
    /// Launch probe failure at construction; readiness derives from it.
    start_error: Option<String>,
}

impl BrowserAgent {
    pub fn new(name: String, options: &BrowserOptions, workdir: PathBuf) -> Self {
        let (notify, _) = watch::channel(0u64);
        let start_error = probe_binary(&options.bin);
        Self {
            runner: TurnRunner {
                globals: global_args(&name, options),
                bin: options.bin.clone(),
                env: resolve_env(&options.env),
                ops: options.ops.clone(),
                timeout: Duration::from_secs(options.timeout_secs.max(1)),
                workdir,
                running: Arc::new(Mutex::new(HashMap::new())),
            },
            turn_lock: Arc::new(Mutex::new(())),
            threads: Arc::new(RwLock::new(HashMap::new())),
            notify,
            start_error,
            name,
        }
    }

    /// Launch probe failure, if any. Upper layers log it at registration.
    pub fn probe_error(&self) -> Option<&str> {
        self.start_error.as_deref()
    }

    /// Whether the configured binary resolves. A bare command name (no path
    /// separator) resolves through PATH and cannot be checked with `exists()`;
    /// the launch probe is authoritative for it.
    fn bin_present(&self) -> bool {
        let bin = &self.runner.bin;
        if bin.components().count() == 1 && !bin.is_absolute() {
            true
        } else {
            bin.exists()
        }
    }

    fn blank_session(id: String) -> ThreadSession {
        ThreadSession {
            id,
            title: None,
            messages: Vec::new(),
            created_at: chrono::Utc::now(),
            updated_at: None,
            completed: true,
            acp_thread_id: None,
            turn_completed: 0,
            parent: None,
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
                let tid = format!("browser-{}", uuid::Uuid::new_v4());
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

/// The arguments every step shares: the session that isolates this agent's
/// browser, where to attach, how the window is shown, and the optional
/// guards.
fn global_args(agent_name: &str, options: &BrowserOptions) -> Vec<String> {
    let session = options
        .session
        .clone()
        .unwrap_or_else(|| agent_name.to_string());
    let mut args = vec!["--session".to_string(), session];
    if let Some(cdp) = &options.cdp {
        args.push("--cdp".to_string());
        args.push(cdp.clone());
    }
    if let Some(profile) = &options.profile {
        args.push("--profile".to_string());
        args.push(profile.clone());
    }
    if let Some(headed) = options.headed {
        args.push("--headed".to_string());
        if !headed {
            args.push("false".to_string());
        }
    }
    if let Some(script) = &options.init_script {
        args.push("--init-script".to_string());
        args.push(script.display().to_string());
    }
    if let Some(domains) = &options.domains {
        args.push("--allowed-domains".to_string());
        args.push(domains.join(","));
    }
    args
}

/// Resolve declared environment for the browser CLI. A value of the form
/// `$NAME` is replaced by the server environment variable NAME, so a
/// credential reaches the CLI without being written to the config file.
fn resolve_env(declared: &HashMap<String, String>) -> HashMap<String, String> {
    let mut resolved = HashMap::with_capacity(declared.len());
    for (key, value) in declared {
        if let Some(name) = value.strip_prefix('$') {
            resolved.insert(key.clone(), std::env::var(name).unwrap_or_default());
        } else {
            resolved.insert(key.clone(), value.clone());
        }
    }
    resolved
}

/// Cap one step's recorded output.
fn cap_detail(text: String) -> String {
    if text.chars().count() <= MAX_DETAIL_CHARS {
        return text;
    }
    let mut capped: String = text.chars().take(MAX_DETAIL_CHARS).collect();
    capped.push_str("\n[truncated]");
    capped
}

/// Probe launchability at construction: run the CLI's help with closed stdio
/// and a short ceiling. A CLI that starts counts as ready even when it exits
/// on its own; one that cannot be spawned records why, which is what the
/// health status shows.
fn probe_binary(bin: &Path) -> Option<String> {
    use std::process::{Command, Stdio};
    use std::time::Instant;
    let mut child = match Command::new(bin)
        .arg("--help")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(child) => child,
        Err(e) => return Some(format!("cannot start: {}", e)),
    };
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return None,
            Ok(None) => {}
            Err(e) => return Some(format!("cannot wait: {}", e)),
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return None;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}

/// Parse a turn's plan: `{"steps": [...]}` or a bare `[...]`.
fn parse_plan(message: &str) -> Result<Vec<Step>, String> {
    let trimmed = message.trim();
    let parsed = if trimmed.starts_with('[') {
        serde_json::from_str::<Vec<Step>>(trimmed)
    } else {
        serde_json::from_str::<Plan>(trimmed).map(|plan| plan.steps)
    };
    parsed.map_err(|error| {
        format!(
            "cannot read the plan: {error}. A plan is \
             {{\"steps\":[{{\"op\":\"open\",\"url\":\"https://example.com\"}},\
             {{\"op\":\"fill\",\"target\":\"@e3\",\"value\":\"...\"}}]}}; \
             implemented operations: {}",
            IMPLEMENTED_OPS.join(", ")
        )
    })
}

/// Factory for the `browser` kind: a fill-only browser executor driving the
/// `agent-browser` CLI.
pub struct BrowserFactory;

#[async_trait::async_trait]
impl AgentFactory for BrowserFactory {
    fn kind(&self) -> &'static str {
        "browser"
    }

    /// The declaration is checked before anything launches. Narrowing the
    /// operation set can only name operations the adapter implements; the
    /// set it cannot name is the point of the adapter.
    fn validate(&self, spec: &AgentSpec, _all: &[AgentSpec]) -> Result<(), String> {
        let options: BrowserOptions = spec.options()?;
        if let Some(ops) = &options.ops {
            for op in ops {
                if !IMPLEMENTED_OPS.contains(&op.as_str()) {
                    return Err(format!(
                        "agent '{}': '{}' is not an operation this adapter implements; \
                         implemented operations: {}",
                        spec.name,
                        op,
                        IMPLEMENTED_OPS.join(", ")
                    ));
                }
            }
        }
        Ok(())
    }

    async fn launch(&self, spec: &AgentSpec, ctx: &LaunchContext) -> anyhow::Result<LaunchedAgent> {
        let options: BrowserOptions = spec.options().map_err(anyhow::Error::msg)?;
        let workdir = match &spec.workdir {
            Some(p) => std::fs::canonicalize(p).map_err(|e| {
                anyhow::anyhow!(
                    "agent '{}': cannot resolve workdir {}: {}",
                    spec.name,
                    p.display(),
                    e
                )
            })?,
            None => ctx.workdir.clone(),
        };
        let backend = BrowserAgent::new(spec.name.clone(), &options, workdir.clone());
        if let Some(err) = backend.probe_error() {
            tracing::warn!(
                "Agent '{}': launch probe failed ({}); health reports ready=false",
                spec.name,
                err
            );
        }
        tracing::info!(
            "Agent '{}' running (browser adapter, bin {}, workdir {})",
            spec.name,
            options.bin.display(),
            workdir.display()
        );
        Ok(LaunchedAgent {
            backend: Arc::new(backend),
            children: Vec::new(),
        })
    }
}

#[async_trait::async_trait]
impl AgentBackend for BrowserAgent {
    fn name(&self) -> &str {
        &self.name
    }

    fn kind(&self) -> &'static str {
        "browser"
    }

    fn capabilities(&self) -> AgentCapabilities {
        // A browser keeps its page between turns, so the agent is sessionful,
        // and one browser is one page: turns run one at a time.
        AgentCapabilities {
            sessionful: true,
            streaming: false,
            tools: false,
            approval: false,
            parallel: false,
            transport: "browser",
        }
    }

    async fn status(&self) -> AgentStatus {
        let present = self.bin_present();
        let last_error = match (&self.start_error, present) {
            (Some(e), _) => Some(e.clone()),
            (None, true) => None,
            (None, false) => Some("binary not found".to_string()),
        };
        AgentStatus {
            name: self.name.clone(),
            kind: self.kind().to_string(),
            connected: last_error.is_none(),
            ready: last_error.is_none(),
            capabilities: self.capabilities(),
            last_error,
        }
    }

    async fn submit(
        &self,
        thread_id: Option<&str>,
        message: &str,
    ) -> Result<SubmitReceipt, String> {
        self.submit_with_options(thread_id, message, None, None)
            .await
    }

    async fn submit_with_options(
        &self,
        thread_id: Option<&str>,
        message: &str,
        _parent: Option<crate::agent::ThreadParent>,
        _thinking_effort: Option<&str>,
    ) -> Result<SubmitReceipt, String> {
        let (tid, is_new) = self.get_or_create(thread_id).await;
        let request_id = uuid::Uuid::new_v4().to_string();
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
                timestamp: chrono::Utc::now(),
            });
            session.completed = false;
        }
        {
            let mut running = self.runner.running.lock().await;
            running.insert(
                request_id.clone(),
                TurnState {
                    child: None,
                    cancelled: false,
                },
            );
        }
        let _ = self
            .notify
            .send(chrono::Utc::now().timestamp_millis() as u64);

        // The turn runs in the background: the receipt returns now, the
        // thread reports the record when it lands, and a cancel can stop the
        // step in flight.
        let runner = self.runner.clone();
        let turn_lock = self.turn_lock.clone();
        let threads = self.threads.clone();
        let notify = self.notify.clone();
        let task_request_id = request_id.clone();
        let task_thread_id = tid.clone();
        let plan = message.to_string();
        tokio::spawn(async move {
            // One browser, one turn at a time.
            let _turn = turn_lock.lock().await;
            let record = match parse_plan(&plan) {
                Err(error) => TurnRecord {
                    ok: false,
                    error: Some(error),
                    steps: Vec::new(),
                },
                Ok(steps) => match runner.check_plan(&steps) {
                    Err(error) => TurnRecord {
                        ok: false,
                        error: Some(error),
                        steps: Vec::new(),
                    },
                    Ok(()) => runner.run_plan(&task_request_id, &steps).await,
                },
            };
            runner.running.lock().await.remove(&task_request_id);

            let reply = serde_json::to_string_pretty(&record).unwrap_or_else(|_| "{}".to_string());
            let mut threads = threads.write().await;
            if let Some(session) = threads.get_mut(&task_thread_id) {
                session.messages.push(ThreadMessage {
                    role: "assistant".to_string(),
                    content: reply,
                    message_id: Some(task_request_id),
                    entry_type: Some("agent_message".to_string()),
                    tool_name: None,
                    tool_status: None,
                    timestamp: chrono::Utc::now(),
                });
                session.completed = true;
                session.turn_completed += 1;
            }
            drop(threads);
            let _ = notify.send(chrono::Utc::now().timestamp_millis() as u64);
        });

        Ok(SubmitReceipt {
            thread_id: tid,
            request_id,
            is_new,
        })
    }

    async fn cancel(&self) -> Result<(), String> {
        let mut running = self.runner.running.lock().await;
        for state in running.values_mut() {
            state.cancelled = true;
            if let Some(pid) = state.child {
                unsafe {
                    libc::kill(pid as i32, libc::SIGKILL);
                }
            }
        }
        Ok(())
    }

    async fn cancel_request(&self, request_id: &str) -> Result<(), String> {
        let mut running = self.runner.running.lock().await;
        if let Some(state) = running.get_mut(request_id) {
            state.cancelled = true;
            if let Some(pid) = state.child {
                unsafe {
                    libc::kill(pid as i32, libc::SIGKILL);
                }
            }
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
