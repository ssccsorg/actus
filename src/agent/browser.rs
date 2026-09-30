// BrowserAgent: a fill-only browser executor behind the agent fabric.
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
//
// The record is the surface a person reads before submitting, so it says what
// happened rather than what was asked. A step that already finished is
// recorded with its own outcome even when a cancel arrives in the same
// instant, and a step stopped mid-flight is recorded as stopped instead of
// dropped, because its effect on the page is then unknown.
//
// The CLI has no `--` separator, so a plan string that begins with `-` is
// refused before a process starts: the CLI would read it as an option.
// Measured against agent-browser 0.38.1, a dash-leading fill value is
// consumed as an option, the field is left empty, and the command still exits
// 0, which is a silent no-op; a dash-leading target fails loudly as an
// unmatched selector.

use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;
use tokio::process::Child;
use tokio::sync::{watch, Mutex, RwLock};

use crate::agent::adapter::{AgentFactory, LaunchContext, LaunchedAgent};
use crate::agent::config::AgentSpec;
use crate::agent::{
    truncate_title, AgentBackend, AgentCapabilities, AgentStatus, PendingAuthorization,
    SubmitReceipt, ThreadMessage, ThreadParent, ThreadSession,
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

/// Ceiling for one plan's step count, so a single turn cannot hold the
/// browser indefinitely.
const MAX_STEPS: usize = 64;

/// Ceiling for a plan's `wait` step. A page settles in seconds; a plan that
/// asks for longer is a plan that means to hold the browser.
const MAX_WAIT_MS: u64 = 30_000;

/// Ceiling for a whole plan, over and above the per-step timeout, which is
/// configurable and therefore not a bound on its own.
const MAX_TURN_SECS: u64 = 600;

/// Ceiling for one step's recorded output. A snapshot is long, and a thread
/// message stays bounded.
const MAX_DETAIL_CHARS: usize = 4000;

/// Poll interval for child exit and cancellation.
const CHILD_POLL_MS: u64 = 100;

/// Grace window for the launch probe. `--help` prints in milliseconds; the
/// window only covers a cold filesystem.
const PROBE_GRACE_MS: u64 = 2_000;

/// Poll interval inside the launch probe.
const PROBE_STEP_MS: u64 = 25;

/// Recorded detail when a step was stopped rather than finishing. The page
/// state at that step is not knowable from here, and the record says so.
const CANCELLED_IN_FLIGHT: &str = "cancelled while running; the page state at this step is unknown";
const CANCELLED_AS_STARTED: &str =
    "cancelled as the step started; the page state at this step is unknown";

fn default_bin() -> PathBuf {
    PathBuf::from("agent-browser")
}

fn default_timeout() -> u64 {
    60
}

/// The declaration a browser agent reads from its options table. An unknown
/// key is a load error: the tables this adapter reads are policy, and a
/// misspelled `ops` would drop the narrowing without saying so.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BrowserOptions {
    /// `agent-browser` binary path or PATH name.
    #[serde(default = "default_bin")]
    pub bin: PathBuf,
    /// CDP endpoint (port or url) of a browser to drive. Unset means the CLI
    /// launches the browser it manages for the session.
    #[serde(default)]
    pub cdp: Option<String>,
    /// Daemon socket namespace, so one host can run more than one browser
    /// fleet. Unset means the CLI's own default, shared with every other
    /// agent that names none.
    #[serde(default)]
    pub namespace: Option<String>,
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
    /// `--allowed-domains`. The CLI refuses this with `cdp` or `profile`,
    /// because containment cannot be installed over a browser it did not
    /// launch, so the combination is refused at load.
    #[serde(default)]
    pub domains: Option<Vec<String>>,
    /// Narrow the implemented operation set, for example to
    /// `["open", "snapshot", "fill"]`. Naming an operation the adapter does
    /// not implement fails the config load.
    #[serde(default)]
    pub ops: Option<Vec<String>>,
    /// Environment for the browser CLI, for the variables it reads
    /// (`AGENT_BROWSER_*` and the like). A value of the form `$NAME` is
    /// resolved from the server environment when the config loads, so a
    /// credential stays out of the config file; a name the server
    /// environment does not carry fails the load.
    #[serde(default)]
    pub env: HashMap<String, String>,
    /// Per-step timeout in seconds.
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,
}

/// One step of a plan. The variants are the vocabulary this adapter
/// implements; a plan that names anything else does not parse.
#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
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
    /// Save a screenshot, so the reviewed state is on disk. The path is
    /// relative to the agent's workdir and is passed on as an absolute path.
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
#[serde(deny_unknown_fields)]
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

/// What a running turn is doing, so a cancel can stop it. The process is kept
/// as the handle rather than as a pid: a pid that is published and cleared
/// around a wait can name a recycled process by the time it is signalled.
struct TurnState {
    child: Option<Child>,
    cancelled: bool,
}

enum StepOutcome {
    Done(String),
    Failed(String),
    /// The step started and was stopped. Its effect on the page is unknown,
    /// so the record carries it rather than dropping it.
    Cancelled(String),
}

/// What one observation of the step in flight decides. The process is
/// observed before the cancel flag: a step that already finished is recorded
/// as what it did, and a cancel stops the steps after it.
enum Poll {
    Running,
    Exited(ExitStatus),
    WaitFailed(std::io::Error),
    Cancel,
    Deadline,
    Gone,
}

/// Everything one turn needs to run, cloned out of the agent so the turn can
/// proceed in the background while the agent keeps serving.
#[derive(Clone)]
struct TurnRunner {
    bin: PathBuf,
    /// Global arguments (`--session`, `--namespace`, `--cdp`, ...) placed
    /// before every subcommand.
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
                    // The daemon resolves a relative path against its own
                    // working directory, so the workdir is applied here and
                    // the CLI is handed an absolute one.
                    args.push(self.workdir.join(path).display().to_string());
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
    /// a plan over the step ceiling or without steps, a pause over its
    /// ceiling, a positional the CLI would read as an option, and a
    /// screenshot path that could land outside the agent's directory. The
    /// plan's own vocabulary is enforced by the parser, which knows no other
    /// operations.
    fn check_plan(&self, steps: &[Step]) -> Result<(), String> {
        if steps.is_empty() {
            return Err("the plan has no steps".to_string());
        }
        if steps.len() > MAX_STEPS {
            return Err(format!(
                "the plan has {} steps and the ceiling is {MAX_STEPS}",
                steps.len()
            ));
        }
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
            match step {
                Step::Open { url } => check_positional("url", url)?,
                Step::Fill { target, value } => {
                    check_positional("target", target)?;
                    check_positional("value", value)?;
                }
                Step::GetAttr { target, name } => {
                    check_positional("target", target)?;
                    check_positional("name", name)?;
                }
                Step::GetText { target } | Step::GetValue { target } => {
                    check_positional("target", target)?
                }
                Step::Screenshot {
                    path: Some(path), ..
                } => check_screenshot_path(path)?,
                Step::Wait { ms } if *ms > MAX_WAIT_MS => {
                    return Err(format!("wait {ms} ms exceeds the {MAX_WAIT_MS} ms ceiling"));
                }
                _ => {}
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
    /// fill after an open that did not happen would land nowhere. The plan
    /// gets a budget of its own so a long plan cannot hold the browser for
    /// longer than the per-step ceiling times the step ceiling would allow.
    async fn run_plan(&self, request_id: &str, steps: &[Step]) -> TurnRecord {
        let mut records: Vec<StepRecord> = Vec::with_capacity(steps.len());
        let budget = Duration::from_secs(
            self.timeout
                .as_secs()
                .saturating_mul(steps.len().max(1) as u64)
                .min(MAX_TURN_SECS),
        );
        let deadline = tokio::time::Instant::now() + budget;
        for step in steps {
            if self.is_cancelled(request_id).await {
                return TurnRecord {
                    ok: false,
                    error: Some("cancelled".to_string()),
                    steps: records,
                };
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining < Duration::from_millis(CHILD_POLL_MS) {
                return TurnRecord {
                    ok: false,
                    error: Some(format!(
                        "the plan exceeded its {}-second budget",
                        budget.as_secs()
                    )),
                    steps: records,
                };
            }
            let args = self.argv(step);
            let step_budget = self.timeout.min(remaining);
            match self.run_step(request_id, &args, step_budget).await {
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
                StepOutcome::Cancelled(detail) => {
                    records.push(StepRecord {
                        op: step.name(),
                        ok: false,
                        detail,
                    });
                    return TurnRecord {
                        ok: false,
                        error: Some("cancelled".to_string()),
                        steps: records,
                    };
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
    /// ceiling, and return what it printed. The child handle stays published
    /// for the whole step, and the poll loop below is the only place a step
    /// is killed, so a finished process is always observed as finished.
    async fn run_step(&self, request_id: &str, args: &[String], budget: Duration) -> StepOutcome {
        let mut command = tokio::process::Command::new(&self.bin);
        #[cfg(unix)]
        command.process_group(0);
        command
            .args(args)
            .envs(&self.env)
            .current_dir(&self.workdir)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(e) => {
                return StepOutcome::Failed(format!("cannot start {}: {}", self.bin.display(), e))
            }
        };

        // Detach the output pipes before the handle is published; the readers
        // drain them while the handle stays reachable for a cancel.
        let out_pipe = child.stdout.take();
        let err_pipe = child.stderr.take();
        {
            let mut running = self.running.lock().await;
            match running.get_mut(request_id) {
                Some(state) if state.cancelled => {
                    kill_child_group(&mut child).await;
                    return StepOutcome::Cancelled(CANCELLED_AS_STARTED.to_string());
                }
                Some(state) => state.child = Some(child),
                None => {
                    kill_child_group(&mut child).await;
                    return StepOutcome::Cancelled(CANCELLED_AS_STARTED.to_string());
                }
            }
        }

        let out_task = tokio::spawn(read_pipe(out_pipe));
        let err_task = tokio::spawn(read_pipe(err_pipe));
        let deadline = tokio::time::Instant::now() + budget;
        let ending = loop {
            let decision = {
                let mut running = self.running.lock().await;
                match running.get_mut(request_id) {
                    None => Poll::Gone,
                    Some(state) => poll_step(state, deadline),
                }
            };
            match decision {
                Poll::Running => tokio::time::sleep(Duration::from_millis(CHILD_POLL_MS)).await,
                Poll::Exited(status) => break Ending::Exited(status),
                Poll::WaitFailed(error) => break Ending::WaitFailed(error),
                Poll::Gone => break Ending::Cancelled,
                Poll::Cancel | Poll::Deadline => {
                    // The child is taken under the lock and killed outside it.
                    let taken = {
                        let mut running = self.running.lock().await;
                        running
                            .get_mut(request_id)
                            .and_then(|state| state.child.take())
                    };
                    if let Some(mut child) = taken {
                        kill_child_group(&mut child).await;
                    }
                    break match decision {
                        Poll::Deadline => Ending::TimedOut,
                        _ => Ending::Cancelled,
                    };
                }
            }
        };

        // A process that exited on its own has closed its pipes or is about
        // to, so the readers are awaited. Anything else was killed: the
        // readers are aborted so a straggler holding a pipe cannot delay the
        // recorded outcome.
        let (stdout, stderr) = if matches!(ending, Ending::Exited(_)) {
            (
                out_task.await.unwrap_or_default(),
                err_task.await.unwrap_or_default(),
            )
        } else {
            out_task.abort();
            err_task.abort();
            (Vec::new(), Vec::new())
        };

        match ending {
            Ending::Exited(status) => {
                let stdout = String::from_utf8_lossy(&stdout).trim().to_string();
                let stderr = String::from_utf8_lossy(&stderr).trim().to_string();
                if status.success() {
                    StepOutcome::Done(cap_detail(stdout))
                } else {
                    let detail = if stderr.is_empty() {
                        format!("`{}` {}", args.join(" "), describe_exit(&status))
                    } else {
                        format!(
                            "`{}` {}: {}",
                            args.join(" "),
                            describe_exit(&status),
                            cap_detail(stderr)
                        )
                    };
                    StepOutcome::Failed(detail)
                }
            }
            Ending::WaitFailed(error) => {
                StepOutcome::Failed(format!("cannot wait for {}: {}", self.bin.display(), error))
            }
            Ending::Cancelled => StepOutcome::Cancelled(CANCELLED_IN_FLIGHT.to_string()),
            Ending::TimedOut => StepOutcome::Failed(format!(
                "`{}` timed out after {}s",
                args.join(" "),
                budget.as_secs()
            )),
        }
    }
}

/// How a step's process ended, as the poll loop decided it.
enum Ending {
    Exited(ExitStatus),
    WaitFailed(std::io::Error),
    Cancelled,
    TimedOut,
}

/// Observe the step's process once. The exit is read before the cancel flag,
/// so a cancel that arrives after the process finished still records what the
/// process did.
fn poll_step(state: &mut TurnState, deadline: tokio::time::Instant) -> Poll {
    let exited = match state.child.as_mut() {
        None => None,
        Some(child) => match child.try_wait() {
            Ok(Some(status)) => Some(Ok(status)),
            Ok(None) => None,
            Err(error) => Some(Err(error)),
        },
    };
    if let Some(result) = exited {
        state.child = None;
        return match result {
            Ok(status) => Poll::Exited(status),
            Err(error) => Poll::WaitFailed(error),
        };
    }
    if state.child.is_none() {
        return if state.cancelled {
            Poll::Cancel
        } else {
            Poll::Gone
        };
    }
    if state.cancelled {
        Poll::Cancel
    } else if tokio::time::Instant::now() >= deadline {
        Poll::Deadline
    } else {
        Poll::Running
    }
}

/// Kill a step's process group and reap it. The direct child is the CLI, and
/// the group covers what it spawned; a straggler holding the output pipes
/// would otherwise delay the recorded outcome. The group signal goes out
/// before the reap, while the group is known to exist.
async fn kill_child_group(child: &mut Child) {
    if let Some(pid) = child.id() {
        #[cfg(unix)]
        unsafe {
            libc::kill(-(pid as i32), libc::SIGKILL);
        }
        let _ = child.start_kill();
        let _ = child.wait().await;
    }
}

/// Drain a captured pipe to EOF.
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

/// Describe an exit status for the record: its code, or the signal that ended
/// the process.
fn describe_exit(status: &ExitStatus) -> String {
    if let Some(code) = status.code() {
        return format!("exited with {code}");
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        if let Some(signal) = status.signal() {
            return format!("was killed by signal {signal}");
        }
    }
    "exited with a signal".to_string()
}

/// A positional argument the CLI would read as an option. The CLI has no
/// `--` separator and no way to pass a leading dash as data, so the plan is
/// refused here instead of turning into a step that does nothing.
fn check_positional(field: &str, value: &str) -> Result<(), String> {
    if value.starts_with('-') {
        return Err(format!(
            "{field} '{value}' begins with '-', which the browser CLI reads as an option; \
             a positional that begins with '-' cannot be passed through it"
        ));
    }
    Ok(())
}

/// A screenshot path has to be one the CLI reads as a path: with one
/// positional, a name that does not look like a file is taken as a selector.
/// The path stays inside the agent's workdir, and it is passed on as an
/// absolute path because the CLI's daemon resolves a relative one against its
/// own working directory.
fn check_screenshot_path(path: &str) -> Result<(), String> {
    const IMAGE_EXTENSIONS: [&str; 4] = ["png", "jpg", "jpeg", "webp"];
    let candidate = Path::new(path);
    if candidate.is_absolute() {
        return Err(format!(
            "screenshot path '{path}' is absolute; a path relative to the agent's workdir is \
             what this adapter can keep inside it"
        ));
    }
    if candidate
        .components()
        .any(|part| !matches!(part, Component::Normal(_)))
    {
        return Err(format!(
            "screenshot path '{path}' has a '..' or root component; the path stays inside the \
             agent's workdir"
        ));
    }
    let extension = candidate
        .extension()
        .and_then(|extension| extension.to_str())
        .map(str::to_ascii_lowercase);
    match extension.as_deref() {
        Some(extension) if IMAGE_EXTENSIONS.contains(&extension) => Ok(()),
        _ => Err(format!(
            "screenshot path '{path}' has no image extension; the CLI reads a single positional \
             without one as a selector, so the path has to end in {}",
            IMAGE_EXTENSIONS.join(", ")
        )),
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
        let env = resolve_env(&options.env);
        // The probe runs with the declaration's environment, so what it
        // reports is what a step will get.
        let start_error = probe_binary(&options.bin, &workdir, &env);
        Self {
            runner: TurnRunner {
                globals: global_args(&name, options),
                bin: options.bin.clone(),
                env,
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
    /// the launch probe is authoritative for it. A relative path with a
    /// separator resolves against the agent's workdir, which is the directory
    /// the steps run in.
    fn bin_present(&self) -> bool {
        let bin = &self.runner.bin;
        if bin.is_absolute() {
            bin.exists()
        } else if bin.components().count() == 1 {
            true
        } else {
            self.runner.workdir.join(bin).exists()
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

    /// Whether a finished turn recorded a reply under this request id.
    async fn has_reply(&self, request_id: &str) -> bool {
        self.threads.read().await.values().any(|session| {
            session
                .messages
                .iter()
                .any(|message| message.message_id.as_deref() == Some(request_id))
        })
    }
}

/// The arguments every step shares: the session that isolates this agent's
/// browser, the daemon namespace, where to attach, how the window is shown,
/// and the optional guards.
fn global_args(agent_name: &str, options: &BrowserOptions) -> Vec<String> {
    let session = options
        .session
        .clone()
        .unwrap_or_else(|| agent_name.to_string());
    let mut args = vec!["--session".to_string(), session];
    if let Some(namespace) = &options.namespace {
        args.push("--namespace".to_string());
        args.push(namespace.clone());
    }
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
/// credential reaches the CLI without being written to the config file. A
/// name the server environment does not carry is refused at config load, so
/// the empty value this would resolve cannot reach a running agent.
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

/// Probe launchability at construction: run the CLI's help from the agent's
/// workdir and with the declaration's environment, with closed stdio and a
/// short ceiling, so the probe sees what a step sees. A CLI that starts
/// counts as ready even when it exits on its own; one that cannot be spawned
/// records why, which is what the health status shows.
fn probe_binary(bin: &Path, workdir: &Path, env: &HashMap<String, String>) -> Option<String> {
    use std::process::{Command, Stdio};
    use std::time::Instant;
    let mut child = match Command::new(bin)
        .arg("--help")
        .current_dir(workdir)
        .envs(env)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(child) => child,
        Err(e) => return Some(format!("cannot start: {}", e)),
    };
    let deadline = Instant::now() + Duration::from_millis(PROBE_GRACE_MS);
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return None,
            Ok(None) => {}
            Err(e) => {
                let _ = child.kill();
                let _ = child.wait();
                return Some(format!("cannot wait: {}", e));
            }
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return None;
        }
        std::thread::sleep(Duration::from_millis(PROBE_STEP_MS));
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
    /// set it cannot name is the point of the adapter. The two combinations
    /// the CLI refuses are refused here, with the CLI's own reason, and a
    /// credential the server environment cannot supply fails the load rather
    /// than resolving to an empty value.
    fn validate(&self, spec: &AgentSpec, _all: &[AgentSpec]) -> Result<(), String> {
        let options: BrowserOptions = spec.options()?;
        if options.bin.as_os_str().is_empty() {
            return Err(format!("agent '{}': `bin` is empty", spec.name));
        }
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
        if options.domains.is_some() {
            if options.cdp.is_some() {
                return Err(format!(
                    "agent '{}': `domains` cannot be combined with `cdp`; the browser CLI \
                     rejects `--allowed-domains` with `--cdp` because network containment \
                     cannot be installed before the connected browser runs its pages",
                    spec.name
                ));
            }
            if options.profile.is_some() {
                return Err(format!(
                    "agent '{}': `domains` cannot be combined with `profile`; the browser CLI \
                     rejects `--allowed-domains` with `--profile` because Chrome may restore \
                     existing pages before network containment is installed",
                    spec.name
                ));
            }
        }
        for (key, value) in &options.env {
            if let Some(name) = value.strip_prefix('$') {
                if std::env::var(name).is_err() {
                    return Err(format!(
                        "agent '{}': env '{key}' is '$NAME' and the server environment has no \
                         {name}",
                        spec.name
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
        parent: Option<ThreadParent>,
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
            // Record the dispatch origin once: the first submit into a
            // thread decides its parent.
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

            let reply = serde_json::to_string_pretty(&record).unwrap_or_else(|error| {
                // A record that cannot be serialized is a bug in the record,
                // and the thread still has to carry an answer.
                serde_json::json!({
                    "ok": false,
                    "error": format!("the record could not be serialized: {error}"),
                    "steps": [],
                })
                .to_string()
            });
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

    /// Stop every turn this agent holds. The flag is what the poll loop
    /// reads, so the loop kills the step in flight and reaps it, and a turn
    /// that has not started a step never starts one.
    async fn cancel(&self) -> Result<(), String> {
        let mut running = self.runner.running.lock().await;
        for state in running.values_mut() {
            state.cancelled = true;
        }
        Ok(())
    }

    /// Cancel one turn. A turn that already finished has no entry left and is
    /// answered from the reply it recorded, since cancelling it is a no-op;
    /// an id this agent never saw is an error rather than a silent success.
    async fn cancel_request(&self, request_id: &str) -> Result<(), String> {
        {
            let mut running = self.runner.running.lock().await;
            if let Some(state) = running.get_mut(request_id) {
                state.cancelled = true;
                return Ok(());
            }
        }
        if self.has_reply(request_id).await {
            return Ok(());
        }
        Err(format!(
            "agent '{}' has no turn '{request_id}': it is not in flight and no reply carries \
             that id",
            self.name
        ))
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

    /// The browser adapter declares no tool surface, so there is nothing to
    /// resolve and a caller reaching this endpoint for it is confused. The
    /// fabric surfaces the error instead of reporting success for work that
    /// was not done.
    async fn resolve_tool_call(
        &self,
        _platform_thread_id: &str,
        _tool_call_id: &str,
        _allow: bool,
    ) -> Result<(), String> {
        Err(
            "the browser adapter has no pending tool calls: it declares no tool surface"
                .to_string(),
        )
    }

    async fn create_thread(&self) -> Result<String, String> {
        let (tid, _) = self.get_or_create(None).await;
        Ok(tid)
    }
}
