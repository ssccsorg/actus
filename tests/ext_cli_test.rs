// Integration tests for the raw-CLI agent adapter.
//
// A fake CLI script stands in for any binary: the adapter spawns
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

fn fake_bin(dir: &Path, name: &str, body: &str) -> std::path::PathBuf {
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
    let bin = fake_bin(dir.path(), "fake-cli", "#!/bin/sh\necho \"got: $*\"\n");
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
async fn ext_cli_marker_profile_replaces_prompt() {
    // Mirrors the Ante profile: fixed flags plus a {prompt} marker.
    let dir = tempfile::tempdir().unwrap();
    let bin = fake_bin(
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
    let bin = fake_bin(dir.path(), "stdin-cli", "#!/bin/sh\ncat\n");
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
    let bin = fake_bin(
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
async fn ext_cli_failure_records_exit_diagnostics() {
    let dir = tempfile::tempdir().unwrap();
    let bin = fake_bin(
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
