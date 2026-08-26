// Integration tests for the agent execution fabric (issue #9).

use std::sync::Arc;

use actus::agent::{AgentBackend, AgentKind, AgentRegistry};
use actus::zed::backend::ZedBackend;
use actus::zed::{WsCommandTx, ZedManager};
use tokio::sync::RwLock;

#[test]
fn agent_kind_roundtrip() {
    assert_eq!(AgentKind::parse("zed"), Some(AgentKind::Zed));
    assert_eq!(AgentKind::parse("langgraph"), Some(AgentKind::LangGraph));
    assert_eq!(AgentKind::parse("native"), Some(AgentKind::Native));
    assert_eq!(AgentKind::parse("unknown"), None);
    assert_eq!(AgentKind::Zed.as_str(), "zed");
    assert_eq!(serde_json::to_string(&AgentKind::Zed).unwrap(), "\"zed\"");
    assert_eq!(
        serde_json::from_str::<AgentKind>("\"langgraph\"").unwrap(),
        AgentKind::LangGraph
    );
}

fn zed_backend() -> ZedBackend {
    let dir = tempfile::tempdir().expect("tempdir");
    let manager = Arc::new(RwLock::new(ZedManager::new(
        "ses_test".to_string(),
        "127.0.0.1:9999".to_string(),
        dir.path(),
    )));
    let ws_tx: WsCommandTx = Arc::new(tokio::sync::Mutex::new(None));
    ZedBackend { manager, ws_tx }
}

#[tokio::test]
async fn registry_default_agent_status() {
    let mut registry = AgentRegistry::new();
    registry.register(Arc::new(zed_backend()), true);

    let default = registry.default_agent().expect("default agent");
    let status = default.status().await;
    assert_eq!(status.name, "zed");
    assert_eq!(status.kind, AgentKind::Zed);
    assert!(!status.connected);
    assert!(!status.ready);

    let statuses = registry.statuses().await;
    assert_eq!(statuses.len(), 1);
    assert_eq!(statuses[0].name, "zed");
}

#[tokio::test]
async fn registry_get_by_name() {
    let mut registry = AgentRegistry::new();
    registry.register(Arc::new(zed_backend()), true);

    assert!(registry.get("zed").is_some());
    assert!(registry.get("missing").is_none());
}

#[tokio::test]
async fn submit_fails_when_not_connected() {
    let backend = zed_backend();
    let err = backend.submit(None, "hello").await.expect_err("must fail");
    assert!(
        err.contains("not connected") || err.contains("not ready"),
        "unexpected error: {}",
        err
    );
}

#[tokio::test]
async fn threads_empty_when_no_state() {
    let backend = zed_backend();
    assert!(backend.threads().await.is_empty());
    assert!(backend.thread("missing").await.is_none());
}
