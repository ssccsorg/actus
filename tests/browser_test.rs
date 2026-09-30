// Integration tests for the browser agent adapter (issue #37).
//
// A stub `agent-browser` script stands in for the CLI: it records the argv,
// the pid, and the working directory of every invocation, and it answers each
// operation. The policy that matters here is structural, so the tests check
// what the adapter promises: every implemented operation reaches the CLI as a
// fixed argv, an operation the adapter does not implement never reaches a
// process at all, a string the CLI would read as an option is refused before
// a process starts, and the record says what happened to every step that
// started.

use std::collections::HashMap;
use std::path::Path;
use std::time::{Duration, Instant};

use actus::agent::adapter::FactoryRegistry;
use actus::agent::browser::{BrowserAgent, BrowserFactory, BrowserOptions, Step, IMPLEMENTED_OPS};
use actus::agent::config::load_config;
use actus::agent::AgentBackend;

/// Write an executable stub CLI, from a child process so no write handle to
/// the exec target survives in this process (see tests/ext_cli_test.rs).
fn stub_bin(dir: &Path) -> std::path::PathBuf {
    use std::io::Write;
    use std::process::{Command, Stdio};

    let path = dir.join("agent-browser-stub");
    let mut writer = Command::new("sh")
        .arg("-c")
        .arg("cat > \"$0\" && chmod 755 \"$0\"")
        .arg(&path)
        .stdin(Stdio::piped())
        .spawn()
        .expect("spawn stub writer");
    let mut stdin = writer.stdin.take().expect("stub writer stdin");
    stdin.write_all(STUB.as_bytes()).expect("write stub body");
    drop(stdin);
    let status = writer.wait().expect("wait for stub writer");
    assert!(status.success(), "stub writer failed: {status}");
    path
}

const STUB: &str = r#"#!/usr/bin/env python3
"""Stub agent-browser: record the invocation, answer the operation."""
import json, os, sys, time

args = sys.argv[1:]
log = os.environ.get("BROWSER_STUB_LOG")
if log:
    with open(log, "a") as handle:
        handle.write(json.dumps({"args": args, "pid": os.getpid(), "cwd": os.getcwd()}) + "\n")

if args == ["--help"]:
    print("stub help")
    sys.exit(0)

globals_with_values = {
    "--session", "--namespace", "--cdp", "--profile", "--init-script", "--allowed-domains",
}
rest = []
index = 0
while index < len(args):
    arg = args[index]
    if arg in globals_with_values:
        index += 2
        continue
    if arg == "--headed":
        if index + 1 < len(args) and args[index + 1] in ("true", "false"):
            index += 2
        else:
            index += 1
        continue
    rest.append(arg)
    index += 1

command = rest[0] if rest else ""

sleep_on = os.environ.get("BROWSER_STUB_SLEEP_ON", "")
if sleep_on and command == sleep_on:
    time.sleep(float(os.environ.get("BROWSER_STUB_SLEEP", "0")))

fail_on = os.environ.get("BROWSER_STUB_FAIL_ON", "")
if fail_on and command == fail_on:
    print(f"stub: {command} refused", file=sys.stderr)
    sys.exit(3)

if command == "open":
    print(f"Opened {rest[1]}")
elif command == "snapshot":
    print("@e1 [textbox] Name\n@e2 [textbox] Email\n@e3 [textbox] Bio")
elif command == "fill":
    print(f"Filled {rest[1]}")
elif command == "get":
    what = rest[1] if len(rest) > 1 else ""
    answers = {"attr": "40", "text": "label text", "title": "Example Form", "url": "https://example.com/form"}
    print(answers.get(what, "value"))
elif command == "screenshot":
    print(f"Saved screenshot to {rest[1] if len(rest) > 1 else 'default'}")
elif command == "tab":
    print("t1 about:blank")
elif command == "wait":
    print("waited")
elif command == "close":
    print("Closed")
else:
    print(f"stub: unknown command {command}", file=sys.stderr)
    sys.exit(2)
"#;

fn options(bin: std::path::PathBuf, log: &Path) -> BrowserOptions {
    BrowserOptions {
        bin,
        cdp: Some("9222".to_string()),
        namespace: Some("review".to_string()),
        headed: Some(true),
        profile: None,
        session: None,
        init_script: None,
        domains: None,
        ops: None,
        env: HashMap::from([("BROWSER_STUB_LOG".to_string(), log.display().to_string())]),
        timeout_secs: 20,
    }
}

fn agent(dir: &Path, name: &str, log: &Path) -> BrowserAgent {
    BrowserAgent::new(
        name.to_string(),
        &options(stub_bin(dir), log),
        dir.to_path_buf(),
    )
}

/// One invocation of the stub, as the stub recorded it.
struct Call {
    args: Vec<String>,
    pid: u32,
    cwd: String,
}

fn calls(path: &Path) -> Vec<Call> {
    let text = std::fs::read_to_string(path).unwrap_or_default();
    text.lines()
        .map(|line| {
            let value: serde_json::Value = serde_json::from_str(line).expect("stub log line");
            Call {
                args: value["args"]
                    .as_array()
                    .expect("args")
                    .iter()
                    .map(|arg| arg.as_str().expect("argv string").to_string())
                    .collect(),
                pid: value["pid"].as_u64().expect("pid") as u32,
                cwd: value["cwd"].as_str().expect("cwd").to_string(),
            }
        })
        .collect()
}

/// The argv of every step invocation, minus the launch probe.
fn invocations(path: &Path) -> Vec<Vec<String>> {
    calls(path)
        .into_iter()
        .filter(|call| call.args.first().map(String::as_str) != Some("--help"))
        .map(|call| call.args)
        .collect()
}

/// Whether a process is still there. The stub logs its own pid, so a step
/// that should have been killed can be checked against the OS.
fn alive(pid: u32) -> bool {
    unsafe { libc::kill(pid as i32, 0) == 0 }
}

async fn wait_for_reply(agent: &BrowserAgent, thread_id: &str) -> String {
    for _ in 0..600 {
        if let Some(session) = agent.thread(thread_id).await {
            if session.completed {
                return session
                    .messages
                    .last()
                    .map(|message| message.content.clone())
                    .unwrap_or_default();
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("browser turn did not complete in time");
}

/// The reply recorded under one request id, once it lands. Two turns on one
/// thread carry two replies, so a test that starts both reads each by its id
/// rather than by the thread's newest message.
async fn reply_for(agent: &BrowserAgent, thread_id: &str, request_id: &str) -> String {
    for _ in 0..600 {
        if let Some(session) = agent.thread(thread_id).await {
            if let Some(message) = session
                .messages
                .iter()
                .rev()
                .find(|message| message.message_id.as_deref() == Some(request_id))
            {
                return message.content.clone();
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("no reply landed for request {request_id}");
}

fn record(reply: &str) -> serde_json::Value {
    serde_json::from_str(reply).expect("the reply is a record")
}

/// The globals every step carries with the options `options()` builds.
fn globals() -> Vec<String> {
    [
        "--session",
        "browser1",
        "--namespace",
        "review",
        "--cdp",
        "9222",
        "--headed",
    ]
    .iter()
    .map(|arg| arg.to_string())
    .collect()
}

fn expect(args: &[&str]) -> Vec<String> {
    args.iter().map(|arg| arg.to_string()).collect()
}

async fn refuse(agent: &BrowserAgent, plan: &str, needle: &str, context: &str) {
    let receipt = agent.submit(None, plan).await.unwrap();
    let reply = wait_for_reply(agent, &receipt.thread_id).await;
    let record = record(&reply);
    assert_eq!(record["ok"], false, "{context}: {reply}");
    let error = record["error"].as_str().unwrap_or_default();
    assert!(error.contains(needle), "{context}: {reply}");
}

const PLAN: &str = r#"{"steps":[
  {"op":"open","url":"https://example.com/form"},
  {"op":"snapshot"},
  {"op":"fill","target":"@e3","value":"Taeho Lee"},
  {"op":"get_attr","target":"@e3","name":"maxlength"}
]}"#;

#[tokio::test]
async fn a_plan_runs_each_step_and_records_what_it_did() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("invocations.jsonl");
    let agent = agent(dir.path(), "browser1", &log);

    let receipt = agent.submit(None, PLAN).await.unwrap();
    let reply = wait_for_reply(&agent, &receipt.thread_id).await;
    let record = record(&reply);

    assert_eq!(record["ok"], true, "{reply}");
    let steps = record["steps"].as_array().unwrap();
    assert_eq!(steps.len(), 4);
    assert_eq!(steps[0]["op"], "open");
    assert_eq!(steps[1]["op"], "snapshot");
    assert!(steps[1]["detail"].as_str().unwrap().contains("@e3"));
    assert_eq!(steps[2]["op"], "fill");
    assert!(steps[2]["detail"].as_str().unwrap().contains("Filled @e3"));
    assert_eq!(steps[3]["op"], "get_attr");
    assert_eq!(steps[3]["detail"], "40", "the field's maxlength came back");

    // Each step is one CLI invocation, with the shared globals first and the
    // operation's own arguments after them.
    let calls = invocations(&log);
    assert_eq!(calls.len(), 4, "{calls:?}");
    let mut want = globals();
    want.extend(expect(&["open", "https://example.com/form"]));
    assert_eq!(calls[0], want);
    let mut want = globals();
    want.extend(expect(&["fill", "@e3", "Taeho Lee"]));
    assert_eq!(calls[2], want);
    let mut want = globals();
    want.extend(expect(&["get", "attr", "@e3", "maxlength"]));
    assert_eq!(calls[3], want);
}

/// Every operation in the vocabulary reaches the CLI as one fixed argv. This
/// is the adapter's whole translation surface, pinned operation by operation.
#[tokio::test]
async fn every_implemented_operation_reaches_the_cli_with_its_own_argv() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("invocations.jsonl");
    let agent = agent(dir.path(), "browser1", &log);

    let plan = r#"{"steps":[
      {"op":"open","url":"https://example.com/form"},
      {"op":"snapshot","interactive":true},
      {"op":"fill","target":"@e3","value":"Taeho Lee"},
      {"op":"get_attr","target":"@e3","name":"maxlength"},
      {"op":"get_text","target":"@e1"},
      {"op":"get_value","target":"@e3"},
      {"op":"get_title"},
      {"op":"get_url"},
      {"op":"screenshot","path":"shots/a.png","full":true},
      {"op":"tabs"},
      {"op":"wait","ms":250},
      {"op":"close"}
    ]}"#;
    let receipt = agent.submit(None, plan).await.unwrap();
    let reply = wait_for_reply(&agent, &receipt.thread_id).await;
    let record = record(&reply);
    assert_eq!(record["ok"], true, "{reply}");
    assert_eq!(record["steps"].as_array().unwrap().len(), 12);

    let shot = dir.path().join("shots/a.png").display().to_string();
    let expected: Vec<Vec<&str>> = vec![
        vec!["open", "https://example.com/form"],
        vec!["snapshot", "-i"],
        vec!["fill", "@e3", "Taeho Lee"],
        vec!["get", "attr", "@e3", "maxlength"],
        vec!["get", "text", "@e1"],
        vec!["get", "value", "@e3"],
        vec!["get", "title"],
        vec!["get", "url"],
        vec!["screenshot", &shot, "--full"],
        vec!["tab", "list"],
        vec!["wait", "250"],
        vec!["close"],
    ];
    let calls = invocations(&log);
    assert_eq!(calls.len(), expected.len(), "{calls:?}");
    for (call, args) in calls.iter().zip(expected.iter()) {
        let mut want = globals();
        want.extend(expect(args));
        assert_eq!(call, &want);
    }
}

/// The optional globals take their documented forms and a fixed order, so a
/// declaration cannot reorder the argv underneath a step.
#[tokio::test]
async fn the_optional_globals_appear_in_a_fixed_order() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("invocations.jsonl");
    let mut opts = options(stub_bin(dir.path()), &log);
    opts.cdp = None;
    opts.namespace = None;
    opts.session = Some("preview".to_string());
    opts.headed = Some(false);
    opts.init_script = Some(dir.path().join("guard.js"));
    opts.domains = Some(vec![
        "example.com".to_string(),
        "docs.example.com".to_string(),
    ]);
    let agent = BrowserAgent::new("browser1".to_string(), &opts, dir.path().to_path_buf());

    let receipt = agent
        .submit(None, r#"{"steps":[{"op":"get_title"}]}"#)
        .await
        .unwrap();
    let _ = wait_for_reply(&agent, &receipt.thread_id).await;

    let calls = invocations(&log);
    assert_eq!(calls.len(), 1, "{calls:?}");
    let want = vec![
        "--session".to_string(),
        "preview".to_string(),
        "--headed".to_string(),
        "false".to_string(),
        "--init-script".to_string(),
        dir.path().join("guard.js").display().to_string(),
        "--allowed-domains".to_string(),
        "example.com,docs.example.com".to_string(),
        "get".to_string(),
        "title".to_string(),
    ];
    assert_eq!(calls[0], want);
}

#[tokio::test]
async fn a_thread_keeps_the_plan_and_the_record() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("invocations.jsonl");
    let agent = agent(dir.path(), "browser1", &log);

    let receipt = agent.submit(None, PLAN).await.unwrap();
    let _ = wait_for_reply(&agent, &receipt.thread_id).await;
    let session = agent.thread(&receipt.thread_id).await.unwrap();

    assert_eq!(session.messages.len(), 2);
    assert_eq!(session.messages[0].role, "user");
    assert_eq!(session.messages[0].content, PLAN);
    assert_eq!(session.messages[1].role, "assistant");
    assert!(session.title.as_deref().unwrap().starts_with("{\"steps\":"));
}

/// The probe and the steps run in the agent's working directory, so the CLI's
/// project configuration and a relative `bin` resolve the same way at both
/// points.
#[tokio::test]
async fn the_probe_and_every_step_run_in_the_agents_workdir() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("invocations.jsonl");
    let agent = agent(dir.path(), "browser1", &log);

    let receipt = agent
        .submit(None, r#"{"steps":[{"op":"get_title"}]}"#)
        .await
        .unwrap();
    let _ = wait_for_reply(&agent, &receipt.thread_id).await;

    let workdir = std::fs::canonicalize(dir.path()).unwrap();
    let recorded = calls(&log);
    assert_eq!(recorded.len(), 2, "the probe and the step");
    assert_eq!(recorded[0].args, expect(&["--help"]));
    for call in &recorded {
        assert_eq!(call.cwd, workdir.display().to_string(), "{:?}", call.args);
    }
}

/// A person submits the form, so no operation that could submit it exists.
/// A plan naming one does not parse, and no process is started for it.
#[tokio::test]
async fn an_operation_the_adapter_does_not_implement_never_reaches_a_process() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("invocations.jsonl");
    let agent = agent(dir.path(), "browser1", &log);

    let receipt = agent
        .submit(None, r#"{"steps":[{"op":"click","target":"@e9"}]}"#)
        .await
        .unwrap();
    let reply = wait_for_reply(&agent, &receipt.thread_id).await;
    let record = record(&reply);

    assert_eq!(record["ok"], false);
    let error = record["error"].as_str().unwrap();
    assert!(error.contains("click"), "{error}");
    assert!(
        error.contains("implemented operations"),
        "the refusal names the vocabulary: {error}"
    );
    assert!(
        invocations(&log).is_empty(),
        "a refused plan starts no process"
    );
}

#[tokio::test]
async fn a_narrowed_operation_set_refuses_a_step_outside_it() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("invocations.jsonl");
    let mut opts = options(stub_bin(dir.path()), &log);
    opts.ops = Some(vec!["open".to_string(), "snapshot".to_string()]);
    let agent = BrowserAgent::new("browser1".to_string(), &opts, dir.path().to_path_buf());

    let receipt = agent.submit(None, PLAN).await.unwrap();
    let reply = wait_for_reply(&agent, &receipt.thread_id).await;
    let record = record(&reply);

    assert_eq!(record["ok"], false);
    let error = record["error"].as_str().unwrap();
    assert!(error.contains("fill"), "{error}");
    assert!(error.contains("runs only: open, snapshot"), "{error}");

    // The steps before the refused one are not run either: the plan is
    // checked before the first process starts.
    assert!(invocations(&log).is_empty(), "{:?}", invocations(&log));
}

#[tokio::test]
async fn an_empty_operation_set_refuses_every_step() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("invocations.jsonl");
    let mut opts = options(stub_bin(dir.path()), &log);
    opts.ops = Some(Vec::new());
    let agent = BrowserAgent::new("browser1".to_string(), &opts, dir.path().to_path_buf());

    let receipt = agent
        .submit(None, r#"{"steps":[{"op":"get_title"}]}"#)
        .await
        .unwrap();
    let reply = wait_for_reply(&agent, &receipt.thread_id).await;
    let record = record(&reply);

    assert_eq!(record["ok"], false);
    assert!(
        record["error"].as_str().unwrap().contains("runs only:"),
        "{reply}"
    );
    assert!(invocations(&log).is_empty());
}

#[tokio::test]
async fn a_plan_with_no_steps_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("invocations.jsonl");
    let agent = agent(dir.path(), "browser1", &log);

    refuse(&agent, r#"{"steps":[]}"#, "no steps", "an empty plan").await;
    assert!(invocations(&log).is_empty());
}

#[tokio::test]
async fn a_plan_over_the_step_ceiling_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("invocations.jsonl");
    let agent = agent(dir.path(), "browser1", &log);

    let steps: Vec<&str> = std::iter::repeat_n(r#"{"op":"get_title"}"#, 65).collect();
    let plan = format!("{{\"steps\":[{}]}}", steps.join(","));
    refuse(&agent, &plan, "ceiling is 64", "65 steps").await;
    assert!(invocations(&log).is_empty());
}

#[tokio::test]
async fn a_pause_over_the_ceiling_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("invocations.jsonl");
    let agent = agent(dir.path(), "browser1", &log);

    refuse(
        &agent,
        r#"{"steps":[{"op":"wait","ms":60000}]}"#,
        "60000",
        "a pause over the ceiling",
    )
    .await;
    assert!(invocations(&log).is_empty());
}

/// The CLI has no `--` separator: a positional that begins with `-` is read
/// as an option. Measured against 0.38.1, a dash-leading fill value is
/// consumed as an option, the field is left empty, and the command still
/// exits 0, so a plan carrying one has to be refused before a process starts.
#[tokio::test]
async fn a_dash_leading_positional_is_refused_before_any_process_starts() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("invocations.jsonl");
    let agent = agent(dir.path(), "browser1", &log);

    let cases = [
        (
            r#"{"steps":[{"op":"fill","target":"-x","value":"ok"}]}"#,
            "target",
        ),
        (
            r#"{"steps":[{"op":"fill","target":"@e3","value":"--json"}]}"#,
            "value",
        ),
        (r#"{"steps":[{"op":"open","url":"--session"}]}"#, "url"),
        (
            r#"{"steps":[{"op":"get_attr","target":"@e3","name":"-a"}]}"#,
            "name",
        ),
    ];
    for (plan, field) in cases {
        refuse(&agent, plan, field, plan).await;
        let error = {
            let receipt = agent.submit(None, plan).await.unwrap();
            let reply = wait_for_reply(&agent, &receipt.thread_id).await;
            record(&reply)["error"].as_str().unwrap().to_string()
        };
        assert!(error.contains("begins with '-'"), "{plan}: {error}");
    }
    assert!(
        invocations(&log).is_empty(),
        "a refused plan starts no process"
    );
}

/// A screenshot path stays inside the agent's workdir, and it has to look
/// like a file: the CLI reads a single positional without an image extension
/// as a selector.
#[tokio::test]
async fn a_screenshot_path_outside_the_workdir_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("invocations.jsonl");
    let agent = agent(dir.path(), "browser1", &log);

    for path in [
        "/etc/actus-should-not-write.png",
        "../escape.png",
        "bareword",
    ] {
        let plan = format!(r#"{{"steps":[{{"op":"screenshot","path":"{path}"}}]}}"#);
        refuse(&agent, &plan, "screenshot path", &plan).await;
    }
    assert!(invocations(&log).is_empty());
}

#[tokio::test]
async fn a_screenshot_path_is_passed_as_an_absolute_path() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("invocations.jsonl");
    let agent = agent(dir.path(), "browser1", &log);

    let receipt = agent
        .submit(
            None,
            r#"{"steps":[{"op":"screenshot","path":"shots/a.png"}]}"#,
        )
        .await
        .unwrap();
    let record = record(&wait_for_reply(&agent, &receipt.thread_id).await);
    assert_eq!(record["ok"], true, "{record}");

    let calls = invocations(&log);
    assert_eq!(calls.len(), 1);
    let mut want = globals();
    want.extend(vec!["screenshot".to_string(), {
        dir.path().join("shots/a.png").display().to_string()
    }]);
    assert_eq!(calls[0], want, "the daemon resolves a relative path itself");
}

/// An unknown field inside a step is a plan error rather than a key the
/// adapter silently drops: `{"op":"fill", "submit":true}` has no meaning here
/// and must not parse as a fill.
#[tokio::test]
async fn an_unknown_step_field_is_refused_before_any_process_starts() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("invocations.jsonl");
    let agent = agent(dir.path(), "browser1", &log);

    refuse(
        &agent,
        r#"{"steps":[{"op":"fill","target":"@e3","value":"x","submit":true}]}"#,
        "cannot read the plan",
        "a step with a field the adapter does not declare",
    )
    .await;
    refuse(
        &agent,
        r#"{"steps":[{"op":"get_title"}],"extra":1}"#,
        "cannot read the plan",
        "a plan with an unknown field",
    )
    .await;
    assert!(invocations(&log).is_empty());
}

#[tokio::test]
async fn a_failing_step_stops_the_plan_and_names_its_output() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("invocations.jsonl");
    let mut opts = options(stub_bin(dir.path()), &log);
    opts.env
        .insert("BROWSER_STUB_FAIL_ON".to_string(), "fill".to_string());
    let agent = BrowserAgent::new("browser1".to_string(), &opts, dir.path().to_path_buf());

    let receipt = agent
        .submit(
            None,
            r#"{"steps":[{"op":"open","url":"https://example.com/form"},
                         {"op":"fill","target":"@e3","value":"x"},
                         {"op":"get_title"}]}"#,
        )
        .await
        .unwrap();
    let record = record(&wait_for_reply(&agent, &receipt.thread_id).await);

    assert_eq!(record["ok"], false, "{record}");
    assert!(
        record.get("error").is_none(),
        "a step failure names the step rather than the plan: {record}"
    );
    let steps = record["steps"].as_array().unwrap();
    assert_eq!(steps.len(), 2, "the step after the failure never ran");
    assert_eq!(steps[0]["ok"], true);
    assert_eq!(steps[1]["op"], "fill");
    assert_eq!(steps[1]["ok"], false);
    let detail = steps[1]["detail"].as_str().unwrap();
    assert!(detail.contains("exited with 3"), "{detail}");
    assert!(detail.contains("refused"), "{detail}");
    assert_eq!(invocations(&log).len(), 2);
}

/// A step over its ceiling is killed with its process group and recorded as a
/// timeout; the process is gone by the time the record lands.
#[tokio::test]
async fn a_step_that_times_out_records_it_and_leaves_no_process() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("invocations.jsonl");
    let mut opts = options(stub_bin(dir.path()), &log);
    opts.timeout_secs = 1;
    opts.env
        .insert("BROWSER_STUB_SLEEP_ON".to_string(), "snapshot".to_string());
    opts.env
        .insert("BROWSER_STUB_SLEEP".to_string(), "30".to_string());
    let agent = BrowserAgent::new("browser1".to_string(), &opts, dir.path().to_path_buf());

    let receipt = agent
        .submit(None, r#"{"steps":[{"op":"snapshot"},{"op":"get_title"}]}"#)
        .await
        .unwrap();
    let started = Instant::now();
    let record = record(&wait_for_reply(&agent, &receipt.thread_id).await);

    assert_eq!(record["ok"], false, "{record}");
    let steps = record["steps"].as_array().unwrap();
    assert_eq!(steps.len(), 1, "the step after the timeout never ran");
    let detail = steps[0]["detail"].as_str().unwrap();
    assert!(detail.contains("timed out after 1s"), "{detail}");
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "a timed-out step does not wait out the sleeping process"
    );

    let recorded = calls(&log);
    let pid = recorded.last().expect("the sleeping step").pid;
    assert!(!alive(pid), "the timed-out process was killed and reaped");
    assert_eq!(invocations(&log).len(), 1);
}

/// One browser is one page: a second turn on the same thread waits for the
/// first, so its first invocation follows the first turn's last one.
#[tokio::test]
async fn two_turns_on_one_thread_serialize() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("invocations.jsonl");
    let mut opts = options(stub_bin(dir.path()), &log);
    opts.env
        .insert("BROWSER_STUB_SLEEP_ON".to_string(), "open".to_string());
    opts.env
        .insert("BROWSER_STUB_SLEEP".to_string(), "0.4".to_string());
    let agent = BrowserAgent::new("browser1".to_string(), &opts, dir.path().to_path_buf());

    let first = agent
        .submit(
            None,
            r#"{"steps":[{"op":"open","url":"https://a.example"},
                         {"op":"snapshot"}]}"#,
        )
        .await
        .unwrap();
    let second = agent
        .submit(
            Some(&first.thread_id),
            r#"{"steps":[{"op":"open","url":"https://b.example"}]}"#,
        )
        .await
        .unwrap();

    assert_eq!(
        record(&reply_for(&agent, &first.thread_id, &first.request_id).await)["ok"],
        true
    );
    let second_reply = reply_for(&agent, &second.thread_id, &second.request_id).await;
    assert_eq!(record(&second_reply)["ok"], true, "{second_reply}");

    let calls = invocations(&log);
    let last_arguments: Vec<String> = calls
        .iter()
        .map(|args| args.last().cloned().unwrap_or_default())
        .collect();
    assert_eq!(
        last_arguments,
        expect(&["https://a.example", "snapshot", "https://b.example"]),
        "the second turn's first step follows the first turn's last step: {calls:?}"
    );
}

#[tokio::test]
async fn cancel_stops_the_step_in_flight_and_records_it() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("invocations.jsonl");
    let mut opts = options(stub_bin(dir.path()), &log);
    opts.env
        .insert("BROWSER_STUB_SLEEP_ON".to_string(), "snapshot".to_string());
    opts.env
        .insert("BROWSER_STUB_SLEEP".to_string(), "10".to_string());
    let agent = BrowserAgent::new("browser1".to_string(), &opts, dir.path().to_path_buf());

    let receipt = agent
        .submit(None, r#"{"steps":[{"op":"snapshot"},{"op":"get_title"}]}"#)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;
    let started = Instant::now();
    agent.cancel_request(&receipt.request_id).await.unwrap();
    let record = record(&wait_for_reply(&agent, &receipt.thread_id).await);

    assert_eq!(record["ok"], false);
    assert_eq!(record["error"], "cancelled", "{record}");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "a cancelled turn does not wait out the step"
    );
    let steps = record["steps"].as_array().unwrap();
    assert_eq!(steps.len(), 1, "the killed step is in the record: {record}");
    assert_eq!(steps[0]["op"], "snapshot");
    assert_eq!(steps[0]["ok"], false);
    assert!(
        steps[0]["detail"]
            .as_str()
            .unwrap()
            .contains("the page state at this step is unknown"),
        "{record}"
    );
    assert_eq!(
        invocations(&log).len(),
        1,
        "the step after the cancelled one never runs"
    );
}

/// A turn waiting for the browser is cancelled before it starts: it records
/// the cancellation and its plan never reaches a process.
#[tokio::test]
async fn a_cancel_of_a_queued_turn_prevents_its_first_step() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("invocations.jsonl");
    let mut opts = options(stub_bin(dir.path()), &log);
    opts.env
        .insert("BROWSER_STUB_SLEEP_ON".to_string(), "open".to_string());
    opts.env
        .insert("BROWSER_STUB_SLEEP".to_string(), "0.4".to_string());
    let agent = BrowserAgent::new("browser1".to_string(), &opts, dir.path().to_path_buf());

    let first = agent
        .submit(
            None,
            r#"{"steps":[{"op":"open","url":"https://a.example"}]}"#,
        )
        .await
        .unwrap();
    let queued = agent
        .submit(
            Some(&first.thread_id),
            r#"{"steps":[{"op":"open","url":"https://b.example"}]}"#,
        )
        .await
        .unwrap();
    agent.cancel_request(&queued.request_id).await.unwrap();

    assert_eq!(
        record(&reply_for(&agent, &first.thread_id, &first.request_id).await)["ok"],
        true
    );
    let reply = reply_for(&agent, &queued.thread_id, &queued.request_id).await;
    assert_eq!(record(&reply)["error"], "cancelled", "{reply}");
    let called: Vec<Vec<String>> = invocations(&log);
    assert!(
        !called
            .iter()
            .any(|args| args.last().map(String::as_str) == Some("https://b.example")),
        "the queued plan never reached a process: {called:?}"
    );
}

/// Cancelling a turn that already finished is a no-op answered from the
/// reply it recorded: the record stands.
#[tokio::test]
async fn a_cancel_after_the_turn_finished_keeps_the_record() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("invocations.jsonl");
    let agent = agent(dir.path(), "browser1", &log);

    let receipt = agent
        .submit(None, r#"{"steps":[{"op":"get_title"}]}"#)
        .await
        .unwrap();
    let before = wait_for_reply(&agent, &receipt.thread_id).await;
    assert_eq!(record(&before)["ok"], true);

    agent.cancel_request(&receipt.request_id).await.unwrap();
    let after = wait_for_reply(&agent, &receipt.thread_id).await;
    assert_eq!(before, after, "the recorded reply is untouched");
}

#[tokio::test]
async fn a_cancel_of_an_unknown_request_id_is_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("invocations.jsonl");
    let agent = agent(dir.path(), "browser1", &log);

    let error = agent
        .cancel_request("no-such-request")
        .await
        .expect_err("an id this agent never saw is not a cancellation");
    assert!(error.contains("no-such-request"), "{error}");
    assert!(error.contains("browser1"), "{error}");
}

#[tokio::test]
async fn the_browser_adapter_has_no_tool_calls_to_resolve() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("invocations.jsonl");
    let agent = agent(dir.path(), "browser1", &log);

    assert!(agent.pending_tool_calls().await.is_empty());
    let error = agent
        .resolve_tool_call("thread", "call", true)
        .await
        .expect_err("there is no tool surface");
    assert!(error.contains("no pending tool calls"), "{error}");
}

#[tokio::test]
async fn status_reports_the_browser_shape() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("invocations.jsonl");
    let agent = agent(dir.path(), "browser1", &log);

    let status = agent.status().await;
    assert_eq!(status.kind, "browser");
    assert!(status.ready);
    assert_eq!(status.capabilities.transport, "browser");
    assert!(status.capabilities.sessionful);
    assert!(!status.capabilities.parallel, "one browser, one turn");
    assert!(!status.capabilities.tools);
}

#[tokio::test]
async fn a_missing_binary_reports_ready_false() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = options(
        dir.path().join("no-such-agent-browser"),
        &dir.path().join("log"),
    );
    opts.cdp = None;
    let agent = BrowserAgent::new("browser1".to_string(), &opts, dir.path().to_path_buf());

    let status = agent.status().await;
    assert!(!status.ready);
    assert!(status.last_error.is_some(), "{status:?}");
}

fn registry() -> FactoryRegistry {
    let mut factories = FactoryRegistry::new();
    factories.register(std::sync::Arc::new(BrowserFactory));
    factories
}

fn write_config(dir: &Path, body: &str) -> std::path::PathBuf {
    let cfg = dir.join("config.toml");
    std::fs::write(&cfg, body).unwrap();
    cfg
}

#[test]
fn the_config_carries_the_browser_declaration() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(
        dir.path(),
        r#"
[[agents]]
name = "preview"
kind = "browser"
cdp = "9222"
namespace = "fleet-a"
headed = true
profile = "Default"
session = "preview"
init_script = "/tmp/guard.js"
ops = ["open", "snapshot", "fill"]
timeout_secs = 45
"#,
    );

    let specs = load_config(Some(&cfg), &registry()).unwrap();
    let options: BrowserOptions = specs[0].options().unwrap();
    assert_eq!(options.cdp.as_deref(), Some("9222"));
    assert_eq!(options.namespace.as_deref(), Some("fleet-a"));
    assert_eq!(options.headed, Some(true));
    assert_eq!(options.profile.as_deref(), Some("Default"));
    assert_eq!(options.session.as_deref(), Some("preview"));
    assert_eq!(
        options.init_script.as_deref(),
        Some(Path::new("/tmp/guard.js"))
    );
    assert_eq!(options.timeout_secs, 45);
    assert_eq!(
        options.ops.as_deref(),
        Some(
            &[
                "open".to_string(),
                "snapshot".to_string(),
                "fill".to_string()
            ][..]
        )
    );
}

#[test]
fn domains_alone_is_accepted() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(
        dir.path(),
        "[[agents]]\nname = \"preview\"\nkind = \"browser\"\ndomains = [\"example.com\"]\n",
    );

    let specs = load_config(Some(&cfg), &registry()).unwrap();
    let options: BrowserOptions = specs[0].options().unwrap();
    assert_eq!(
        options.domains.as_deref(),
        Some(&["example.com".to_string()][..])
    );
}

/// The browser CLI refuses `--allowed-domains` with `--cdp` or `--profile`,
/// because containment cannot be installed over a browser it did not launch.
/// A declaration that asks for both is refused at load, with that reason.
#[test]
fn domains_with_cdp_or_profile_fails_the_load() {
    let dir = tempfile::tempdir().unwrap();
    for (body, other) in [
        ("cdp = \"9222\"\n", "cdp"),
        ("profile = \"Default\"\n", "profile"),
    ] {
        let cfg = write_config(
            dir.path(),
            &format!(
                "[[agents]]\nname = \"preview\"\nkind = \"browser\"\n{body}domains = [\"example.com\"]\n"
            ),
        );
        let error = load_config(Some(&cfg), &registry()).unwrap_err();
        assert!(error.contains("agent 'preview'"), "{error}");
        assert!(error.contains("domains"), "{error}");
        assert!(error.contains(other), "{error}");
    }
}

/// A key the adapter does not declare is a load error: a misspelled `ops`
/// would drop the narrowing without saying so.
#[test]
fn an_unknown_option_key_fails_the_load() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(
        dir.path(),
        "[[agents]]\nname = \"preview\"\nkind = \"browser\"\nopss = [\"open\"]\n",
    );

    let error = load_config(Some(&cfg), &registry()).unwrap_err();
    assert!(error.contains("agent 'preview'"), "{error}");
    assert!(error.contains("opss"), "{error}");
}

/// `$NAME` is resolved from the server environment at load, and a name that
/// environment does not carry fails the load rather than resolving to an
/// empty credential.
#[test]
fn an_env_value_naming_a_missing_variable_fails_the_load() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(
        dir.path(),
        "[[agents]]\nname = \"preview\"\nkind = \"browser\"\nenv = { TOKEN = \"$NO_SUCH_ACTUS_TEST_VAR\" }\n",
    );

    let error = load_config(Some(&cfg), &registry()).unwrap_err();
    assert!(error.contains("agent 'preview'"), "{error}");
    assert!(error.contains("NO_SUCH_ACTUS_TEST_VAR"), "{error}");
}

/// Narrowing the set can only name implemented operations: a config that
/// asks for a click is refused at load, and the error names the agent.
#[test]
fn narrowing_cannot_add_an_operation() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(
        dir.path(),
        "[[agents]]\nname = \"preview\"\nkind = \"browser\"\nops = [\"open\", \"click\"]\n",
    );

    let error = load_config(Some(&cfg), &registry()).unwrap_err();
    assert!(error.contains("agent 'preview'"), "{error}");
    assert!(error.contains("'click'"), "{error}");
    assert!(error.contains("implemented operations"), "{error}");
}

/// The published vocabulary and the parser's variants are the same set: a
/// step variant without a name, or a name without a variant, is a policy gap.
#[test]
fn implemented_ops_and_step_names_agree() {
    let steps = vec![
        Step::Open { url: String::new() },
        Step::Snapshot { interactive: false },
        Step::Fill {
            target: String::new(),
            value: String::new(),
        },
        Step::GetAttr {
            target: String::new(),
            name: String::new(),
        },
        Step::GetText {
            target: String::new(),
        },
        Step::GetValue {
            target: String::new(),
        },
        Step::GetTitle,
        Step::GetUrl,
        Step::Screenshot {
            path: None,
            full: false,
        },
        Step::Tabs,
        Step::Wait { ms: 0 },
        Step::Close,
    ];
    let mut names: Vec<&str> = steps.iter().map(Step::name).collect();
    names.sort_unstable();
    names.dedup();
    let mut published = IMPLEMENTED_OPS.to_vec();
    published.sort_unstable();
    assert_eq!(names, published);
    assert_eq!(published.len(), 12, "one name per implemented operation");
}
