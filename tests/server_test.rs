// HTTP API integration tests (issue #9).
//
// These are real end-to-end tests: the production router is served on
// an ephemeral port and exercised through a real HTTP client. The agent
// backend is a disconnected ZedBackend, so endpoints that require a
// live agent exercise the failure paths (503, error payloads) while
// endpoints that only need the workspace (files, git, threads) are
// covered end to end.

use std::net::SocketAddr;
use std::sync::Arc;

use actus::agent::{AgentBackend, AgentRegistry};
use actus::server::{AppState, SharedState, build_router};
use actus::zed::backend::ZedBackend;
use actus::zed::{WsCommandTx, ZedManager};
use tokio::net::TcpListener;
use tokio::sync::RwLock;

/// State with one disconnected Zed backend and an empty temp workdir.
/// The TempDir is returned so it outlives the tests.
fn test_state() -> (SharedState, tempfile::TempDir) {
    let workdir = tempfile::tempdir().expect("tempdir");
    let manager = Arc::new(RwLock::new(ZedManager::new(
        "ses_test".to_string(),
        "127.0.0.1:9999".to_string(),
        workdir.path(),
    )));
    let ws_tx: WsCommandTx = Arc::new(tokio::sync::Mutex::new(None));
    let backend: Arc<dyn AgentBackend> = Arc::new(ZedBackend { manager, ws_tx });

    let mut registry = AgentRegistry::new();
    registry.register(backend, true);
    let state = AppState::new(registry, workdir.path().to_path_buf());
    (Arc::new(state), workdir)
}

/// Bind an ephemeral port, serve the production router, return its URL.
/// The listener stays bound, so the server task never races to claim
/// the port.
async fn spawn_server(state: SharedState) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr: SocketAddr = listener.local_addr().expect("local addr");
    let handle = tokio::spawn(async move {
        axum::serve(listener, build_router(state))
            .await
            .expect("serve");
    });
    (format!("http://{addr}"), handle)
}

fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .expect("client")
}

/// Run a git command in a directory, panicking on failure.
fn git(dir: &std::path::Path, args: &[&str]) {
    let out = std::process::Command::new("git")
        .args(args)
        .current_dir(dir)
        .output()
        .expect("git runs");
    assert!(
        out.status.success(),
        "git {:?} failed: {}",
        args,
        String::from_utf8_lossy(&out.stderr)
    );
}

#[tokio::test]
async fn health_reports_disconnected_agent() {
    let (state, _keep) = test_state();
    let (base, server) = spawn_server(state).await;

    let resp = client().get(format!("{base}/health")).send().await.unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();

    assert_eq!(body["status"], "ok");
    assert_eq!(body["zed_connected"], false);
    assert_eq!(body["agent_ready"], false);
    assert_eq!(body["active_threads"], 0);
    let agents = body["agents"].as_array().expect("agents array");
    assert_eq!(agents.len(), 1);
    assert_eq!(agents[0]["name"], "zed");
    assert_eq!(agents[0]["kind"], "zed");

    server.abort();
}

#[tokio::test]
async fn chat_endpoints_fail_with_503_when_not_connected() {
    let (state, _keep) = test_state();
    let (base, server) = spawn_server(state).await;

    // Streaming endpoint.
    let resp = client()
        .post(format!("{base}/v1/chat"))
        .json(&serde_json::json!({"message": "hello"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);

    // Async endpoint.
    let resp = client()
        .post(format!("{base}/v1/chat/async"))
        .json(&serde_json::json!({"message": "hello"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);

    server.abort();
}

#[tokio::test]
async fn thread_lifecycle_over_http() {
    let (state, _keep) = test_state();
    let (base, server) = spawn_server(state).await;

    // Empty listing first.
    let resp = client().get(format!("{base}/v1/threads")).send().await.unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["threads"].as_array().unwrap().len(), 0);

    // Create a thread without a live agent (create_thread is local).
    let resp = client()
        .post(format!("{base}/v1/threads"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let created: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(created["status"], "created");
    let tid = created["thread_id"].as_str().expect("thread_id").to_string();

    // Listing now shows one thread.
    let resp = client().get(format!("{base}/v1/threads")).send().await.unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    let threads = body["threads"].as_array().unwrap();
    assert_eq!(threads.len(), 1);
    assert_eq!(threads[0]["id"], tid);
    assert_eq!(threads[0]["message_count"], 0);

    // Detail round-trip.
    let resp = client()
        .get(format!("{base}/v1/threads/{tid}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let detail: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(detail["id"], tid);
    assert_eq!(detail["completed"], false);
    assert_eq!(detail["messages"].as_array().unwrap().len(), 0);

    server.abort();
}

#[tokio::test]
async fn missing_thread_returns_404() {
    let (state, _keep) = test_state();
    let (base, server) = spawn_server(state).await;

    for path in ["/v1/threads/does-not-exist", "/v1/threads/does-not-exist/poll"] {
        let resp = client()
            .get(format!("{base}{path}"))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            reqwest::StatusCode::NOT_FOUND,
            "{} must 404",
            path
        );
    }

    server.abort();
}

#[tokio::test]
async fn unknown_agent_returns_404() {
    let (state, _keep) = test_state();
    let (base, server) = spawn_server(state).await;

    // Thread detail routes through agent_for, which yields 404 for an
    // unknown agent name.
    let resp = client()
        .get(format!("{base}/v1/threads/any?agent=ghost"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    server.abort();
}

#[tokio::test]
async fn file_search_and_mention_scope_to_workdir() {
    let (state, workdir) = test_state();
    std::fs::create_dir_all(workdir.path().join("src")).unwrap();
    std::fs::write(workdir.path().join("src/main.rs"), "fn main() {}\n").unwrap();
    std::fs::write(workdir.path().join("README.md"), "# actus\n").unwrap();

    let (base, server) = spawn_server(state).await;

    let resp = client()
        .get(format!("{base}/v1/files"))
        .query(&[("q", "main"), ("max", "10")])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    let files = body["files"].as_array().unwrap();
    assert!(!files.is_empty(), "search for 'main' must match src/main.rs");
    assert!(
        files
            .iter()
            .any(|f| f["relative_path"].as_str().unwrap().ends_with("src/main.rs"))
    );
    assert_eq!(body["count"], files.len() as u64);

    // Mention formatting returns a non-empty string referencing the match.
    let resp = client()
        .get(format!("{base}/v1/files/mention"))
        .query(&[("q", "main")])
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    let mention = body["mention"].as_str().unwrap();
    assert!(mention.contains("main.rs"), "mention: {mention}");

    server.abort();
}

#[tokio::test]
async fn symbols_and_rules_work_on_empty_workdir() {
    let (state, _keep) = test_state();
    let (base, server) = spawn_server(state).await;

    let resp = client()
        .get(format!("{base}/v1/symbols"))
        .query(&[("q", "fn")])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["count"], 0);
    assert!(body["symbols"].is_array());

    let resp = client()
        .get(format!("{base}/v1/rules"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["count"], 0);
    assert!(body["rules"].is_array());

    server.abort();
}

#[tokio::test]
async fn git_status_reports_non_repo() {
    let (state, _keep) = test_state();
    let (base, server) = spawn_server(state).await;

    // The temp workdir is not a git repository.
    let resp = client()
        .get(format!("{base}/v1/git/status"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["ok"], false);
    assert_eq!(body["error"], "Not a git repository");

    server.abort();
}

#[tokio::test]
async fn git_status_diff_log_on_real_repo() {
    // Skip cleanly when git is unavailable.
    if std::process::Command::new("git").arg("--version").output().is_err() {
        eprintln!("skipping: git not available");
        return;
    }

    let (state, workdir) = test_state();
    let repo = workdir.path();
    git(repo, &["init", "--initial-branch=main"]);
    git(repo, &["config", "user.email", "test@test.com"]);
    git(repo, &["config", "user.name", "Test"]);
    std::fs::write(repo.join("readme.md"), "# Test\n").unwrap();
    git(repo, &["add", "."]);
    git(repo, &["commit", "-m", "Initial commit"]);
    // Introduce an unstaged change for the diff endpoint.
    std::fs::write(repo.join("readme.md"), "# Modified\n").unwrap();

    let (base, server) = spawn_server(state).await;

    let resp = client()
        .get(format!("{base}/v1/git/status"))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["ok"], true);
    assert_eq!(body["status"]["branch"], "main");
    assert!(
        body["status"]["modified"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m.as_str().unwrap().contains("readme.md"))
    );

    let resp = client()
        .get(format!("{base}/v1/git/diff"))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["ok"], true);
    assert!(
        body["diff"].as_str().unwrap().contains("readme.md"),
        "diff must reference readme.md"
    );

    let resp = client()
        .get(format!("{base}/v1/git/log"))
        .query(&[("max", "3")])
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["ok"], true);
    let commits = body["commits"].as_array().unwrap();
    assert_eq!(commits.len(), 1);
    assert_eq!(commits[0]["message"], "Initial commit");

    server.abort();
}

#[tokio::test]
async fn fetch_rejects_non_http_url() {
    let (state, _keep) = test_state();
    let (base, server) = spawn_server(state).await;

    // No network is involved: the scheme check fails before any request.
    let resp = client()
        .get(format!("{base}/v1/fetch"))
        .query(&[("url", "ftp://example.com/file")])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["ok"], false);
    assert!(body["error"].as_str().unwrap().contains("http"));

    server.abort();
}

#[tokio::test]
async fn tool_call_endpoints_without_agent_connection() {
    let (state, _keep) = test_state();
    let (base, server) = spawn_server(state).await;

    // Pending list is empty when the agent has no pending authorizations.
    let resp = client()
        .get(format!("{base}/v1/agents/tool-calls/pending"))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["count"], 0);
    assert_eq!(body["pending"].as_array().unwrap().len(), 0);

    // Resolving without a WebSocket reports an error instead of hanging.
    let resp = client()
        .post(format!("{base}/v1/agents/tool-calls/resolve"))
        .json(&serde_json::json!({
            "platform_thread_id": "t",
            "tool_call_id": "c",
            "allow": true
        }))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "error");

    server.abort();
}

#[tokio::test]
async fn cancel_reports_error_when_disconnected() {
    let (state, _keep) = test_state();
    let (base, server) = spawn_server(state).await;

    let resp = client()
        .post(format!("{base}/v1/cancel"))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "error");
    assert!(
        body["error"].as_str().unwrap().contains("not connected"),
        "unexpected error: {body}"
    );

    server.abort();
}

#[tokio::test]
async fn unknown_route_returns_404() {
    let (state, _keep) = test_state();
    let (base, server) = spawn_server(state).await;

    let resp = client()
        .get(format!("{base}/v1/does-not-exist"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    server.abort();
}
