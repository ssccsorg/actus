// Integration tests for the browser agent adapter (issue #37).
//
// A stub `agent-browser` script stands in for the CLI: it records the argv of
// every invocation and answers each operation. The policy that matters here
// is structural, so the tests check two things the adapter promises: every
// implemented operation reaches the CLI as a fixed argv, and an operation the
// adapter does not implement never reaches a process at all.

use std::collections::HashMap;
use std::path::Path;
use std::time::{Duration, Instant};

use actus::agent::adapter::FactoryRegistry;
use actus::agent::browser::{BrowserAgent, BrowserFactory, BrowserOptions};
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
"""Stub agent-browser: record the argv, answer the operation."""
import json, os, sys, time

args = sys.argv[1:]
log = os.environ.get("BROWSER_STUB_LOG")
if log:
    with open(log, "a") as handle:
        handle.write(json.dumps(args) + "\n")

if args == ["--help"]:
    print("stub help")
    sys.exit(0)

globals_with_values = {"--session", "--cdp", "--profile", "--init-script", "--allowed-domains"}
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

sleep_on = os.environ.get("BROWSER_STUB_SLEEP_ON", "")
if sleep_on and rest and rest[0] == sleep_on:
    time.sleep(float(os.environ.get("BROWSER_STUB_SLEEP", "0")))

command = rest[0] if rest else ""
if command == "open":
    print(f"Opened {rest[1]}")
elif command == "snapshot":
    print("@e1 [textbox] Name\n@e2 [textbox] Email\n@e3 [textbox] Bio")
elif command == "fill":
    print(f"Filled {rest[1]}")
elif command == "get":
    what = rest[1] if len(rest) > 1 else ""
    print({"attr": "40", "url": "https://example.com/form", "title": "Example Form"}.get(what, "value"))
elif command == "screenshot":
    print("Saved screenshot")
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

/// The argv of every invocation that reached the stub, minus the launch probe.
fn invocations(path: &Path) -> Vec<Vec<String>> {
    let text = std::fs::read_to_string(path).unwrap_or_default();
    text.lines()
        .map(|line| serde_json::from_str::<Vec<String>>(line).expect("stub log line"))
        .filter(|argv| argv.first().map(String::as_str) != Some("--help"))
        .collect()
}

async fn wait_for_reply(agent: &BrowserAgent, thread_id: &str) -> String {
    for _ in 0..400 {
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

fn record(reply: &str) -> serde_json::Value {
    serde_json::from_str(reply).expect("the reply is a record")
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
    assert_eq!(
        calls[0],
        vec![
            "--session",
            "browser1",
            "--cdp",
            "9222",
            "--headed",
            "open",
            "https://example.com/form"
        ]
    );
    assert_eq!(
        calls[2],
        vec![
            "--session",
            "browser1",
            "--cdp",
            "9222",
            "--headed",
            "fill",
            "@e3",
            "Taeho Lee"
        ]
    );
    assert_eq!(
        calls[3],
        vec![
            "--session",
            "browser1",
            "--cdp",
            "9222",
            "--headed",
            "get",
            "attr",
            "@e3",
            "maxlength"
        ]
    );
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
async fn a_pause_over_the_ceiling_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("invocations.jsonl");
    let agent = agent(dir.path(), "browser1", &log);

    let receipt = agent
        .submit(None, r#"{"steps":[{"op":"wait","ms":60000}]}"#)
        .await
        .unwrap();
    let record = record(&wait_for_reply(&agent, &receipt.thread_id).await);

    assert_eq!(record["ok"], false);
    assert!(
        record["error"].as_str().unwrap().contains("60000"),
        "{record}"
    );
    assert!(invocations(&log).is_empty());
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
    assert_eq!(
        invocations(&log).len(),
        1,
        "the step after the cancelled one never runs"
    );
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

#[test]
fn the_config_carries_the_browser_declaration() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = dir.path().join("config.toml");
    std::fs::write(
        &cfg,
        r#"
[[agents]]
name = "preview"
kind = "browser"
cdp = "9222"
headed = true
profile = "Default"
init_script = "/tmp/guard.js"
domains = ["example.com"]
ops = ["open", "snapshot", "fill"]
timeout_secs = 45
"#,
    )
    .unwrap();

    let specs = load_config(Some(&cfg), &registry()).unwrap();
    let options: BrowserOptions = specs[0].options().unwrap();
    assert_eq!(options.cdp.as_deref(), Some("9222"));
    assert_eq!(options.headed, Some(true));
    assert_eq!(options.profile.as_deref(), Some("Default"));
    assert_eq!(
        options.init_script.as_deref(),
        Some(Path::new("/tmp/guard.js"))
    );
    assert_eq!(
        options.domains.as_deref(),
        Some(&["example.com".to_string()][..])
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

/// Narrowing the set can only name implemented operations: a config that
/// asks for a click is refused at load, and the error names the agent.
#[test]
fn narrowing_cannot_add_an_operation() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = dir.path().join("config.toml");
    std::fs::write(
        &cfg,
        "[[agents]]\nname = \"preview\"\nkind = \"browser\"\nops = [\"open\", \"click\"]\n",
    )
    .unwrap();

    let error = load_config(Some(&cfg), &registry()).unwrap_err();
    assert!(error.contains("agent 'preview'"), "{error}");
    assert!(error.contains("'click'"), "{error}");
    assert!(error.contains("implemented operations"), "{error}");
}
