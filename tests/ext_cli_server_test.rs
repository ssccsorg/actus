// HTTP-level coverage for a raw-CLI agent (ext_cli) behind the full
// router: chat submit, thread poll, parallel turns, request-scoped
// cancellation through POST /v1/cancel, and health reporting of a
// launch probe failure. The agent is a real python3 process, so process
// spawn, pipe capture, and exit paths are exercised end to end.

use std::collections::HashMap;
use std::sync::Arc;

use actus::agent::config::{ControlPolicy, ControlRule, PromptMode};
use actus::agent::ext_cli::ExtCliAgent;
use actus::agent::AgentRegistry;
use actus::server::{build_router, AppState, SharedState};
use tokio::net::TcpListener;

/// Deterministic one-shot agent: echoes the prompt, or sleeps five
/// seconds when the prompt starts with `slow:`.
const CLI_AGENT_PY: &str = r#"
import sys, time
msg = sys.argv[1] if len(sys.argv) > 1 else ""
if msg.startswith("slow:"):
    time.sleep(5)
print("echo:" + msg)
"#;

fn client() -> reqwest::Client {
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        reqwest::header::AUTHORIZATION,
        reqwest::header::HeaderValue::from_static("Bearer test-token"),
    );
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(15))
        .default_headers(headers)
        .build()
        .expect("client")
}

/// State with one default ext_cli agent running the python3 stub in the
/// given workdir.
fn test_state_with_agent(bin: std::path::PathBuf, workdir: &std::path::Path) -> SharedState {
    test_state_with_agent_and_policy(bin, workdir, ControlPolicy::default())
}

/// State with one default ext_cli agent and a meta-agent control policy.
fn test_state_with_agent_and_policy(
    bin: std::path::PathBuf,
    workdir: &std::path::Path,
    policy: ControlPolicy,
) -> SharedState {
    let agent = ExtCliAgent::new(
        "aux",
        bin,
        vec!["-c".to_string(), CLI_AGENT_PY.to_string()],
        HashMap::new(),
        PromptMode::Arg,
        30,
        workdir.to_path_buf(),
    );
    let mut registry = AgentRegistry::new();
    registry.register(Arc::new(agent), true);
    Arc::new(AppState::new_with_policy(
        registry,
        workdir.to_path_buf(),
        Some("test-token".to_string()),
        policy,
    ))
}

/// Bind an ephemeral port and serve the production router.
async fn spawn_server(state: SharedState) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr");
    let handle = tokio::spawn(async move {
        axum::serve(listener, build_router(state, &[]))
            .await
            .expect("serve");
    });
    (format!("http://{addr}"), handle)
}

/// Submit one turn to the aux agent and return the parsed response.
async fn submit_chat(base: &str, message: &str) -> serde_json::Value {
    client()
        .post(format!("{base}/v1/chat/async"))
        .json(&serde_json::json!({"agent": "aux", "message": message}))
        .send()
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap()
}

/// Poll the thread until the turn completes; return the new content.
async fn wait_completed(base: &str, thread_id: &str) -> String {
    let client = client();
    for _ in 0..200 {
        let r = client
            .get(format!("{base}/v1/threads/{thread_id}/poll?agent=aux"))
            .send()
            .await
            .unwrap();
        let body: serde_json::Value = r.json().await.unwrap();
        if body.get("completed").and_then(|v| v.as_bool()) == Some(true) {
            return body
                .get("new_content")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    panic!("thread {thread_id} did not complete in time");
}

#[tokio::test]
async fn aux_chat_roundtrip_over_http() {
    let dir = tempfile::tempdir().unwrap();
    let (base, server) = spawn_server(test_state_with_agent(
        std::path::PathBuf::from("python3"),
        dir.path(),
    ))
    .await;

    let r = client()
        .post(format!("{base}/v1/chat/async"))
        .json(&serde_json::json!({
            "agent": "aux",
            "message": "hello from http"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let body: serde_json::Value = r.json().await.unwrap();
    let task_id = body["task_id"].as_str().unwrap();
    let thread_id = body["thread_id"].as_str().unwrap();
    assert!(!task_id.is_empty());

    let content = wait_completed(&base, thread_id).await;
    assert_eq!(content, "echo:hello from http");

    let health: serde_json::Value = client()
        .get(format!("{base}/health"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let agents = health["agents"].as_array().unwrap();
    let aux = agents.iter().find(|a| a["name"] == "aux").unwrap();
    assert_eq!(aux["kind"], "ext_cli");
    assert_eq!(aux["ready"], true);
    assert_eq!(aux["capabilities"]["transport"], "cli");
    assert_eq!(aux["capabilities"]["parallel"], true);

    server.abort();
}

#[tokio::test]
async fn aux_cancel_one_parallel_turn_over_http() {
    let dir = tempfile::tempdir().unwrap();
    let (base, server) = spawn_server(test_state_with_agent(
        std::path::PathBuf::from("python3"),
        dir.path(),
    ))
    .await;

    let slow = submit_chat(&base, "slow:one").await;
    let fast = submit_chat(&base, "fast two").await;

    // Cancel only the slow turn by its task id (the fabric request id).
    let r = client()
        .post(format!("{base}/v1/cancel"))
        .json(&serde_json::json!({
            "agent": "aux",
            "request_id": slow["task_id"]
        }))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = r.json().await.unwrap();
    assert_eq!(body["status"], "cancelled");

    let fast_content = wait_completed(&base, fast["thread_id"].as_str().unwrap()).await;
    assert_eq!(fast_content, "echo:fast two");
    let slow_content = wait_completed(&base, slow["thread_id"].as_str().unwrap()).await;
    assert_eq!(slow_content, "[ext-cli] cancelled");

    server.abort();
}

#[tokio::test]
async fn aux_health_reports_launch_probe_failure() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("does-not-exist");
    let (base, server) = spawn_server(test_state_with_agent(missing, dir.path())).await;

    let health: serde_json::Value = client()
        .get(format!("{base}/health"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let agents = health["agents"].as_array().unwrap();
    let aux = agents.iter().find(|a| a["name"] == "aux").unwrap();
    assert_eq!(aux["ready"], false);
    let err = aux["last_error"].as_str().unwrap();
    assert!(err.contains("cannot start"), "unexpected: {err}");

    server.abort();
}

#[tokio::test]
async fn controller_dispatch_denied_without_policy_rule() {
    let dir = tempfile::tempdir().unwrap();
    let (base, server) = spawn_server(test_state_with_agent(
        std::path::PathBuf::from("python3"),
        dir.path(),
    ))
    .await;

    // An agent-originated dispatch (identity header) with an empty
    // policy is denied before any process starts.
    let r = client()
        .post(format!("{base}/v1/chat/async"))
        .header("x-actus-controller", "telos")
        .json(&serde_json::json!({ "agent": "aux", "message": "hi" }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403);

    // A human client without the header keeps working.
    let r = client()
        .post(format!("{base}/v1/chat/async"))
        .json(&serde_json::json!({ "agent": "aux", "message": "hi" }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);

    server.abort();
}

#[tokio::test]
async fn controller_dispatch_allowed_by_policy_rule() {
    let dir = tempfile::tempdir().unwrap();
    let policy = ControlPolicy {
        allow: vec![ControlRule {
            controller: "telos".to_string(),
            targets: vec!["aux".to_string()],
        }],
    };
    let (base, server) = spawn_server(test_state_with_agent_and_policy(
        std::path::PathBuf::from("python3"),
        dir.path(),
        policy,
    ))
    .await;

    let r = client()
        .post(format!("{base}/v1/chat/async"))
        .header("x-actus-controller", "telos")
        .json(&serde_json::json!({ "agent": "aux", "message": "hello meta" }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let body: serde_json::Value = r.json().await.unwrap();
    let thread_id = body["thread_id"].as_str().unwrap();
    let content = wait_completed(&base, thread_id).await;
    assert_eq!(content, "echo:hello meta");

    server.abort();
}

#[tokio::test]
async fn controller_dispatch_records_parent_thread() {
    let dir = tempfile::tempdir().unwrap();
    let policy = ControlPolicy {
        allow: vec![ControlRule {
            controller: "telos".to_string(),
            targets: vec!["aux".to_string()],
        }],
    };
    let (base, server) = spawn_server(test_state_with_agent_and_policy(
        std::path::PathBuf::from("python3"),
        dir.path(),
        policy,
    ))
    .await;

    let r = client()
        .post(format!("{base}/v1/chat/async"))
        .header("x-actus-controller", "telos")
        .json(&serde_json::json!({
            "agent": "aux",
            "message": "child turn",
            "parent_thread_id": "ses_meta-123"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let body: serde_json::Value = r.json().await.unwrap();
    let thread_id = body["thread_id"].as_str().unwrap();
    let content = wait_completed(&base, thread_id).await;
    assert_eq!(content, "echo:child turn");

    // The target thread carries the dispatch origin: controller agent
    // plus the meta thread id.
    let detail: serde_json::Value = client()
        .get(format!("{base}/v1/threads/{thread_id}?agent=aux"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(detail["parent"]["agent"], "telos");
    assert_eq!(detail["parent"]["thread_id"], "ses_meta-123");

    server.abort();
}

#[tokio::test]
async fn human_submit_without_identity_never_records_parent() {
    let dir = tempfile::tempdir().unwrap();
    let (base, server) = spawn_server(test_state_with_agent(
        std::path::PathBuf::from("python3"),
        dir.path(),
    ))
    .await;

    // A human API client has no controller identity, so even a provided
    // parent_thread_id must not fabricate a dispatch origin.
    let r = client()
        .post(format!("{base}/v1/chat/async"))
        .json(&serde_json::json!({
            "agent": "aux",
            "message": "human turn",
            "parent_thread_id": "ses_human-1"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let body: serde_json::Value = r.json().await.unwrap();
    let thread_id = body["thread_id"].as_str().unwrap();
    let detail: serde_json::Value = client()
        .get(format!("{base}/v1/threads/{thread_id}?agent=aux"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(detail.get("parent").is_none(), "unexpected: {detail}");

    server.abort();
}

#[tokio::test]
async fn controller_cancel_denied_by_policy() {
    let dir = tempfile::tempdir().unwrap();
    let (base, server) = spawn_server(test_state_with_agent(
        std::path::PathBuf::from("python3"),
        dir.path(),
    ))
    .await;

    let body = submit_chat(&base, "fast turn").await;
    let thread_id = body["thread_id"].as_str().unwrap().to_string();
    let task_id = body["task_id"].as_str().unwrap().to_string();

    // Agent-originated cancel without a policy rule is refused, and the
    // human can still cancel the same turn afterwards.
    let r = client()
        .post(format!("{base}/v1/cancel"))
        .header("x-actus-controller", "telos")
        .json(&serde_json::json!({
            "agent": "aux",
            "request_id": task_id
        }))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = r.json().await.unwrap();
    assert_eq!(body["status"], "error");
    assert!(
        body["error"].as_str().unwrap().contains("forbidden"),
        "unexpected: {body}"
    );

    let content = wait_completed(&base, &thread_id).await;
    assert_eq!(content, "echo:fast turn");

    server.abort();
}
