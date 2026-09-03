// Integration tests for the raw-CLI agent adapter.
//
// A fake CLI script stands in for any binary: the adapter only spawns
// `bin <cli_args...> <prompt>`, inherits the environment, and records
// stdout, so a shell script exercises the full path without network or
// an LLM key. One test mirrors the Ante profile (`cli_args = ["-p"]`).

use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::time::Duration;

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
async fn cli_success_records_stdout() {
    let dir = tempfile::tempdir().unwrap();
    let bin = fake_bin(
        dir.path(),
        "fake-cli",
        "#!/bin/sh\necho \"fake-cli-reply: $*\"\n",
    );
    let agent = ExtCliAgent::new("aux", bin, Vec::new(), 30, dir.path().to_path_buf());

    let receipt = agent.submit(None, "summarize src").await.unwrap();
    let content = wait_for_assistant(&agent, &receipt.thread_id).await;

    assert!(content.contains("fake-cli-reply"), "unexpected: {content}");
    assert!(
        content.contains("summarize src"),
        "prompt arg missing: {content}"
    );
    let session = agent.thread(&receipt.thread_id).await.unwrap();
    assert_eq!(session.messages.len(), 2, "user plus assistant expected");
    assert_eq!(session.messages[0].role, "user");
    assert_eq!(session.messages[1].role, "assistant");
    assert!(session.completed);
}

#[tokio::test]
async fn cli_prompt_flag_profile_receives_flag_and_prompt() {
    // Mirrors the Ante profile: fixed flag before the prompt argument.
    let dir = tempfile::tempdir().unwrap();
    let bin = fake_bin(
        dir.path(),
        "flag-cli",
        "#!/bin/sh\necho \"flag=$1 prompt=$2\"\n",
    );
    let agent = ExtCliAgent::new(
        "ante",
        bin,
        vec!["-p".to_string()],
        30,
        dir.path().to_path_buf(),
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
async fn cli_failure_records_exit_diagnostics() {
    let dir = tempfile::tempdir().unwrap();
    let bin = fake_bin(
        dir.path(),
        "failing-cli",
        "#!/bin/sh\necho \"boom\" >&2\nexit 3\n",
    );
    let agent = ExtCliAgent::new("aux", bin, Vec::new(), 30, dir.path().to_path_buf());

    let receipt = agent.submit(None, "do the thing").await.unwrap();
    let content = wait_for_assistant(&agent, &receipt.thread_id).await;

    assert!(
        content.contains("[ext-cli] exited with"),
        "unexpected: {content}"
    );
    assert!(content.contains("boom"), "stderr missing: {content}");
}

#[tokio::test]
async fn cli_missing_binary_fails_submit() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("does-not-exist");
    let agent = ExtCliAgent::new("aux", missing, Vec::new(), 5, dir.path().to_path_buf());

    let err = agent.submit(None, "hello").await.unwrap_err();
    assert!(err.contains("cannot start"), "unexpected: {err}");
}
