// Integration tests for the raw-CLI agent adapter.
//
// A stub CLI script stands in for any binary: the adapter spawns
// `bin <args...> <prompt>` (or writes the prompt to stdin), inherits the
// environment plus the declared `cli_env`, and records stdout. One test
// mirrors the Ante profile (`cli_args = ["-p", "{prompt}"]`).

use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::time::Duration;

use actus::agent::config::PromptMode;
use actus::agent::ext_cli::ExtCliAgent;
use actus::agent::AgentBackend;

fn stub_bin(dir: &Path, name: &str, body: &str) -> std::path::PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, body).unwrap();
    let mut perms = std::fs::metadata(&path).unwrap().permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&path, perms).unwrap();
    path
}

fn agent_with(dir: &Path, bin: std::path::PathBuf, args: Vec<String>) -> ExtCliAgent {
    ExtCliAgent::new(
        "aux",
        bin,
        args,
        HashMap::new(),
        PromptMode::Arg,
        30,
        dir.to_path_buf(),
    )
}

async fn wait_for_assistant(agent: &ExtCliAgent, thread_id: &str) -> String {
    for _ in 0..200 {
        if let Some(session) = agent.thread(thread_id).await {
            if session.completed {
                return session
                    .messages
                    .last()
                    .map(|m| m.content.clone())
                    .unwrap_or_default();
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("cli turn did not complete in time");
}

#[tokio::test]
async fn ext_cli_appends_prompt_without_marker() {
    let dir = tempfile::tempdir().unwrap();
    let bin = stub_bin(dir.path(), "stub-cli", "#!/bin/sh\necho \"got: $*\"\n");
    let agent = agent_with(dir.path(), bin, Vec::new());

    let receipt = agent.submit(None, "summarize src").await.unwrap();
    let content = wait_for_assistant(&agent, &receipt.thread_id).await;

    assert!(
        content.contains("got: summarize src"),
        "unexpected: {content}"
    );
    let session = agent.thread(&receipt.thread_id).await.unwrap();
    assert_eq!(session.messages.len(), 2);
    assert_eq!(session.messages[1].role, "assistant");
    assert!(session.completed);
}

#[tokio::test]
async fn ext_cli_workdir_controls_child_cwd() {
    let dir = tempfile::tempdir().unwrap();
    let work_subdir = dir.path().join("agent-project");
    std::fs::create_dir_all(&work_subdir).unwrap();
    let bin = stub_bin(dir.path(), "pwd-cli", "#!/bin/sh\npwd\n");
    let agent = ExtCliAgent::new(
        "aux",
        bin,
        Vec::new(),
        HashMap::new(),
        PromptMode::Arg,
        30,
        work_subdir.clone(),
    );

    let receipt = agent.submit(None, "where am I").await.unwrap();
    let content = wait_for_assistant(&agent, &receipt.thread_id).await;

    let expected = std::fs::canonicalize(&work_subdir).unwrap();
    assert_eq!(content, expected.to_string_lossy(), "unexpected: {content}");
}

#[tokio::test]
async fn ext_cli_marker_profile_replaces_prompt() {
    // Mirrors the Ante profile: fixed flags plus a {prompt} marker.
    let dir = tempfile::tempdir().unwrap();
    let bin = stub_bin(
        dir.path(),
        "flag-cli",
        "#!/bin/sh\necho \"flag=$1 prompt=$2\"\n",
    );
    let agent = agent_with(
        dir.path(),
        bin,
        vec!["-p".to_string(), "{prompt}".to_string()],
    );

    let receipt = agent.submit(None, "hello world").await.unwrap();
    let content = wait_for_assistant(&agent, &receipt.thread_id).await;

    assert!(content.contains("flag=-p"), "unexpected: {content}");
    assert!(
        content.contains("prompt=hello world"),
        "unexpected: {content}"
    );
}

#[tokio::test]
async fn ext_cli_stdin_mode_writes_prompt_to_stdin() {
    let dir = tempfile::tempdir().unwrap();
    let bin = stub_bin(dir.path(), "stdin-cli", "#!/bin/sh\ncat\n");
    let agent = ExtCliAgent::new(
        "aux",
        bin,
        Vec::new(),
        HashMap::new(),
        PromptMode::Stdin,
        30,
        dir.path().to_path_buf(),
    );

    let receipt = agent.submit(None, "read this from stdin").await.unwrap();
    let content = wait_for_assistant(&agent, &receipt.thread_id).await;

    assert_eq!(content, "read this from stdin");
}

#[tokio::test]
async fn ext_cli_runs_turns_in_parallel() {
    let dir = tempfile::tempdir().unwrap();
    let bin = stub_bin(
        dir.path(),
        "slow-cli",
        "#!/bin/sh\nsleep 0.2\necho \"done: $*\"\n",
    );
    let agent = agent_with(dir.path(), bin, Vec::new());

    let (r1, r2) = tokio::join!(agent.submit(None, "first"), agent.submit(None, "second"));
    let (r1, r2) = (r1.unwrap(), r2.unwrap());

    let (c1, c2) = tokio::join!(
        wait_for_assistant(&agent, &r1.thread_id),
        wait_for_assistant(&agent, &r2.thread_id)
    );
    assert!(c1.contains("done: first"), "unexpected: {c1}");
    assert!(c2.contains("done: second"), "unexpected: {c2}");
}

#[tokio::test]
async fn ext_cli_env_literal_and_passthrough() {
    let dir = tempfile::tempdir().unwrap();
    let bin = stub_bin(
        dir.path(),
        "env-cli",
        "#!/bin/sh\necho \"lit=$MY_LIT pass=$MY_PASS\"\n",
    );
    std::env::set_var("EXTCLI_TEST_SOURCE", "from-server");
    let env = HashMap::from([
        ("MY_LIT".to_string(), "abc".to_string()),
        ("MY_PASS".to_string(), "$EXTCLI_TEST_SOURCE".to_string()),
    ]);
    let agent = ExtCliAgent::new(
        "aux",
        bin,
        Vec::new(),
        env,
        PromptMode::Arg,
        30,
        dir.path().to_path_buf(),
    );
    let receipt = agent.submit(None, "hi").await.unwrap();
    let content = wait_for_assistant(&agent, &receipt.thread_id).await;
    std::env::remove_var("EXTCLI_TEST_SOURCE");
    assert!(content.contains("lit=abc"), "unexpected: {content}");
    assert!(
        content.contains("pass=from-server"),
        "unexpected: {content}"
    );
}

#[tokio::test]
async fn ext_cli_failure_records_exit_diagnostics() {
    let dir = tempfile::tempdir().unwrap();
    let bin = stub_bin(
        dir.path(),
        "failing-cli",
        "#!/bin/sh\necho \"boom\" >&2\nexit 3\n",
    );
    let agent = agent_with(dir.path(), bin, Vec::new());

    let receipt = agent.submit(None, "do the thing").await.unwrap();
    let content = wait_for_assistant(&agent, &receipt.thread_id).await;

    assert!(
        content.contains("[ext-cli] exited with"),
        "unexpected: {content}"
    );
    assert!(content.contains("boom"), "stderr missing: {content}");
}

#[tokio::test]
async fn ext_cli_missing_binary_fails_submit() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("does-not-exist");
    let agent = agent_with(dir.path(), missing, Vec::new());

    let err = agent.submit(None, "hello").await.unwrap_err();
    assert!(err.contains("cannot start"), "unexpected: {err}");
}

#[tokio::test]
async fn ext_cli_probe_reports_unlaunchable_binary() {
    // A missing binary and a non-executable file must surface in the
    // health status at registration, before any submit happens.
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("does-not-exist");
    let agent = agent_with(dir.path(), missing, Vec::new());
    let status = agent.status().await;
    assert!(!status.ready);
    let err = status.last_error.as_deref().unwrap();
    assert!(err.contains("cannot start"), "unexpected: {err}");

    let not_exec = dir.path().join("not-exec");
    std::fs::write(&not_exec, "#!/bin/sh\necho hi\n").unwrap();
    let agent = agent_with(dir.path(), not_exec, Vec::new());
    let status = agent.status().await;
    assert!(!status.ready);
    let err = status.last_error.as_deref().unwrap();
    assert!(err.contains("cannot start"), "unexpected: {err}");
}

#[tokio::test]
async fn ext_cli_probe_accepts_launchable_binary() {
    let dir = tempfile::tempdir().unwrap();
    let bin = stub_bin(dir.path(), "ok-cli", "#!/bin/sh\necho ok\n");
    let agent = agent_with(dir.path(), bin, Vec::new());
    let status = agent.status().await;
    assert!(status.ready, "unexpected: {status:?}");
    assert!(status.last_error.is_none(), "unexpected: {status:?}");
}

#[tokio::test]
async fn ext_cli_cancel_request_kills_only_one_turn() {
    let dir = tempfile::tempdir().unwrap();
    let bin = stub_bin(
        dir.path(),
        "slow-cli",
        "#!/bin/sh\nsleep 2\necho \"done: $*\"\n",
    );
    let agent = agent_with(dir.path(), bin, Vec::new());

    let r1 = agent.submit(None, "first").await.unwrap();
    let r2 = agent.submit(None, "second").await.unwrap();
    // Both children are running under distinct request ids. Cancelling
    // the first turn must leave the second child untouched.
    agent.cancel_request(&r1.request_id).await.unwrap();

    let c1 = wait_for_assistant(&agent, &r1.thread_id).await;
    assert_eq!(c1, "[ext-cli] cancelled", "unexpected: {c1}");
    let c2 = wait_for_assistant(&agent, &r2.thread_id).await;
    assert!(c2.contains("done: second"), "unexpected: {c2}");
}

#[tokio::test]
async fn ext_cli_cancel_request_on_same_thread_keeps_second_turn() {
    // Regression: the running map is keyed by request id, so a second
    // submit on the same thread no longer overwrites the first child's
    // handle and a request-scoped cancel cannot kill the wrong turn.
    let dir = tempfile::tempdir().unwrap();
    let bin = stub_bin(
        dir.path(),
        "slow-cli",
        "#!/bin/sh\nsleep 1\necho \"done: $*\"\n",
    );
    let agent = agent_with(dir.path(), bin, Vec::new());

    let r1 = agent.submit(None, "first").await.unwrap();
    let r2 = agent.submit(Some(&r1.thread_id), "second").await.unwrap();
    assert_ne!(r1.request_id, r2.request_id);
    agent.cancel_request(&r1.request_id).await.unwrap();

    // The second turn still runs to completion on the same thread; its
    // reply must appear after the cancellation marker of the first turn.
    let mut done = String::new();
    for _ in 0..200 {
        if let Some(session) = agent.thread(&r1.thread_id).await {
            if let Some(last) = session
                .messages
                .iter()
                .rev()
                .find(|m| m.role == "assistant")
            {
                if last.content.contains("done: second") {
                    done = last.content.clone();
                    break;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(done.contains("done: second"), "unexpected: {done}");

    let session = agent.thread(&r1.thread_id).await.unwrap();
    let assistants: Vec<&str> = session
        .messages
        .iter()
        .filter(|m| m.role == "assistant")
        .map(|m| m.content.as_str())
        .collect();
    assert_eq!(assistants.len(), 2, "messages: {assistants:?}");
    assert!(
        assistants.iter().any(|c| *c == "[ext-cli] cancelled"),
        "messages: {assistants:?}"
    );
}

#[tokio::test]
async fn ext_cli_timeout_kills_child() {
    let dir = tempfile::tempdir().unwrap();
    let bin = stub_bin(dir.path(), "long-cli", "#!/bin/sh\nsleep 5\necho late\n");
    let agent = ExtCliAgent::new(
        "aux",
        bin,
        Vec::new(),
        HashMap::new(),
        PromptMode::Arg,
        1,
        dir.path().to_path_buf(),
    );

    let receipt = agent.submit(None, "slow turn").await.unwrap();
    let content = wait_for_assistant(&agent, &receipt.thread_id).await;
    assert!(
        content.contains("[ext-cli] timed out after 1s"),
        "unexpected: {content}"
    );
}

#[tokio::test]
async fn ext_cli_cancel_kills_all_running_turns() {
    let dir = tempfile::tempdir().unwrap();
    let bin = stub_bin(
        dir.path(),
        "slow-cli",
        "#!/bin/sh\nsleep 2\necho \"done: $*\"\n",
    );
    let agent = agent_with(dir.path(), bin, Vec::new());

    let r1 = agent.submit(None, "one").await.unwrap();
    let r2 = agent.submit(None, "two").await.unwrap();
    // The agent-wide cancel kills every running child of this agent.
    agent.cancel().await.unwrap();

    let c1 = wait_for_assistant(&agent, &r1.thread_id).await;
    let c2 = wait_for_assistant(&agent, &r2.thread_id).await;
    assert_eq!(c1, "[ext-cli] cancelled", "unexpected: {c1}");
    assert_eq!(c2, "[ext-cli] cancelled", "unexpected: {c2}");
}

#[tokio::test]
async fn ext_cli_cancel_after_completion_is_a_noop() {
    let dir = tempfile::tempdir().unwrap();
    let bin = stub_bin(dir.path(), "ok-cli", "#!/bin/sh\necho done\n");
    let agent = agent_with(dir.path(), bin, Vec::new());

    let receipt = agent.submit(None, "quick").await.unwrap();
    let content = wait_for_assistant(&agent, &receipt.thread_id).await;
    assert_eq!(content, "done");

    // A finished turn has no running child left; cancelling it by id
    // succeeds without touching the recorded reply.
    agent.cancel_request(&receipt.request_id).await.unwrap();
    let session = agent.thread(&receipt.thread_id).await.unwrap();
    let last = session.messages.last().unwrap();
    assert_eq!(last.content, "done");
}

#[tokio::test]
async fn ext_cli_probe_accepts_long_running_binary() {
    // A binary that stays alive without input must not block the launch
    // probe: it is killed after the grace window and the agent registers
    // as ready.
    let dir = tempfile::tempdir().unwrap();
    let bin = stub_bin(dir.path(), "hang-cli", "#!/bin/sh\nsleep 30\n");
    let started = std::time::Instant::now();
    let agent = agent_with(dir.path(), bin, Vec::new());
    assert!(
        started.elapsed() < std::time::Duration::from_secs(5),
        "probe blocked on a hanging binary"
    );
    let status = agent.status().await;
    assert!(status.ready, "unexpected: {status:?}");
}

#[tokio::test]
async fn ext_cli_status_reports_binary_removed_after_construction() {
    let dir = tempfile::tempdir().unwrap();
    let bin = stub_bin(dir.path(), "ok-cli", "#!/bin/sh\necho ok\n");
    let agent = agent_with(dir.path(), bin.clone(), Vec::new());
    assert!(agent.status().await.ready);

    std::fs::remove_file(&bin).unwrap();
    let status = agent.status().await;
    assert!(!status.ready);
    assert_eq!(status.last_error.as_deref(), Some("binary not found"));
}
