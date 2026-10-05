// Integration tests for the native reference adapter (issue #12 follow-up).

use std::sync::Arc;

use actus::agent::native::NativeAgent;
use actus::agent::{AgentBackend, AgentKind, AgentRegistry};

#[tokio::test]
async fn native_agent_echoes_and_owns_threads() {
    let backend = Arc::new(NativeAgent::new("native".to_string()));
    let receipt = backend.submit(None, "hello world").await.unwrap();
    assert!(receipt.is_new);

    let session = backend.thread(&receipt.thread_id).await.unwrap();
    assert_eq!(session.messages.len(), 2);
    assert_eq!(session.messages[0].role, "user");
    assert_eq!(session.messages[0].content, "hello world");
    assert!(session.messages[1]
        .content
        .contains("received: hello world"));
    assert!(session.completed);
    assert_eq!(session.turn_completed, 1);
}

#[tokio::test]
async fn native_agent_resumes_existing_thread() {
    let backend = Arc::new(NativeAgent::new("native".to_string()));
    let first = backend.submit(None, "first").await.unwrap();

    let second = backend
        .submit(Some(&first.thread_id), "second")
        .await
        .unwrap();
    assert_eq!(second.thread_id, first.thread_id);
    assert!(!second.is_new);

    let session = backend.thread(&first.thread_id).await.unwrap();
    assert_eq!(session.messages.len(), 4);
    assert_eq!(session.turn_completed, 2);
}

/// A note is stored, in order, and runs no turn: it is what one person in a shared thread says
/// to the others, so it belongs in the record where they read it and nowhere near the agent.
#[tokio::test]
async fn native_agent_records_a_note_without_running_a_turn() {
    let backend = Arc::new(NativeAgent::new("native".to_string()));
    let tid = backend
        .append_note(None, "brb, restarting the server")
        .await
        .unwrap();

    let session = backend.thread(&tid).await.unwrap();
    assert_eq!(
        session.messages.len(),
        1,
        "a note is the only thing written"
    );
    assert_eq!(session.messages[0].role, actus::agent::NOTE_ROLE);
    assert_eq!(session.messages[0].content, "brb, restarting the server");
    assert_eq!(session.turn_completed, 0, "no turn ran for it");
    assert_eq!(
        session.title.as_deref(),
        Some("brb, restarting the server"),
        "and the first thing said in a thread names it"
    );

    // The agent is where it was: the next message runs one turn, on the same thread.
    let receipt = backend.submit(Some(&tid), "go").await.unwrap();
    assert_eq!(receipt.thread_id, tid);
    assert_eq!(backend.thread(&tid).await.unwrap().turn_completed, 1);
}

#[tokio::test]
async fn native_agent_registers_as_fabric_default() {
    let backend = Arc::new(NativeAgent::new("native".to_string()));
    let mut registry = AgentRegistry::new();
    registry.register(backend.clone(), true);

    assert!(registry.default_agent().is_some());
    let status = backend.status().await;
    assert_eq!(status.kind, AgentKind::Native);
    assert!(status.connected);
    assert!(status.ready);

    let threads = backend.threads().await;
    assert!(threads.is_empty());
    let created = backend.create_thread().await.unwrap();
    assert!(backend.thread(&created).await.is_some());
}
