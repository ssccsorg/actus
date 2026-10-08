//! A turn accepted while another runs.
//!
//! Two people in one room is the premise, and the second person's message must not be refused.
//! What the executor cannot take yet waits, and the wait is in the record so every client reads
//! the same order. What this pins is that the message is recorded, that no command reaches the
//! executor while it is busy, and that the turn goes when the one in flight ends.

use std::sync::Arc;

use actus::acpws::backend::AcpwsBackend;
use actus::acpws::control;
use actus::acpws::types::{Command, SyncEvent};
use actus::acpws::{AcpwsManager, QUEUED_ENTRY};
use actus::agent::AgentBackend;
use tokio::sync::{mpsc, Mutex, RwLock};

type Commands = mpsc::UnboundedReceiver<String>;

fn backend(dir: &std::path::Path) -> (AcpwsBackend, Arc<RwLock<AcpwsManager>>, Commands) {
    let mut manager = AcpwsManager::new("ses_test".to_string(), "127.0.0.1:0".to_string(), dir)
        .expect("a manager over a directory");
    manager.agent_connected = true;
    manager.agent_ready = true;
    let (tx, rx) = mpsc::unbounded_channel::<String>();
    manager.set_ws_tx(tx.clone());
    let manager = Arc::new(RwLock::new(manager));
    let backend = AcpwsBackend {
        name: "test".to_string(),
        manager: manager.clone(),
        ws_tx: Arc::new(Mutex::new(Some(tx))),
        scope: None,
    };
    (backend, manager, rx)
}

/// The one command a test expects, which is a chat message for the given turn.
fn chat_command(sent: &str) -> (String, String) {
    match serde_json::from_str::<Command>(sent).expect("a command") {
        Command::ChatMessage {
            request_id,
            message,
            ..
        } => (request_id, message),
        other => panic!("expected a chat message, got {other:?}"),
    }
}

/// The event that ends a turn, as the executor sends it.
fn completed(request_id: &str) -> String {
    let event = SyncEvent::MessageCompleted {
        acp_thread_id: String::new(),
        message_id: String::new(),
        request_id: request_id.to_string(),
    }
    .into_outgoing_message();
    serde_json::to_string(&event).expect("json")
}

async fn entry_types(manager: &Arc<RwLock<AcpwsManager>>) -> Vec<(String, Option<String>)> {
    manager
        .read()
        .await
        .window("t1", 0, 20)
        .expect("window")
        .into_iter()
        .map(|message| (message.content, message.entry_type))
        .collect()
}

/// The second message of a room is accepted while the agent runs the first, and runs when the
/// first ends. No command reaches the executor in between, and the wait is in the record.
#[tokio::test]
async fn a_second_turn_waits_for_the_one_in_flight() {
    let dir = tempfile::tempdir().expect("dir");
    let (backend, manager, mut commands) = backend(dir.path());

    let first = backend.submit(Some("t1"), "first").await.expect("submit");
    assert!(!first.queued, "the agent was free, so the turn runs now");
    let (request_id, message) = chat_command(&commands.try_recv().expect("the first turn is sent"));
    assert_eq!(request_id, first.request_id);
    assert_eq!(message, "first");

    let second = backend.submit(Some("t1"), "second").await.expect("submit");
    assert!(
        second.queued,
        "the agent is running a turn, so this one waits rather than being refused"
    );
    assert_eq!(second.thread_id, "t1");
    assert!(
        commands.try_recv().is_err(),
        "and nothing reaches the executor while it is busy"
    );

    // The wait is in the record, so a client that reads the thread sees it without asking.
    assert_eq!(
        entry_types(&manager).await,
        vec![
            ("first".to_string(), None),
            ("second".to_string(), Some(QUEUED_ENTRY.to_string())),
        ],
        "the queued message says it is waiting"
    );

    // The turn in flight ends, which is what frees the agent.
    control::handle_agent_event(&manager, &completed(&first.request_id)).await;

    let (request_id, message) =
        chat_command(&commands.try_recv().expect("the queued turn goes now"));
    assert_eq!(
        request_id, second.request_id,
        "and it is the one that waited"
    );
    assert_eq!(message, "second");
    assert!(commands.try_recv().is_err(), "only that one goes");

    assert_eq!(
        entry_types(&manager).await,
        vec![("first".to_string(), None), ("second".to_string(), None)],
        "the message is the turn that runs now, so it stops reading as a wait"
    );
    assert!(
        manager
            .read()
            .await
            .pending_requests
            .get(&second.request_id)
            .is_some_and(|thread_id| thread_id == "t1"),
        "and its request id is the turn in flight"
    );
}

/// The queue is in arrival order, so a room's second and third messages run in the order they
/// were said rather than in whichever order the agent happens to free up.
#[tokio::test]
async fn the_queue_keeps_the_order_the_messages_arrived() {
    let dir = tempfile::tempdir().expect("dir");
    let (backend, manager, mut commands) = backend(dir.path());

    let first = backend.submit(Some("t1"), "one").await.expect("submit");
    let _ = commands.try_recv().expect("the first turn is sent");
    let second = backend.submit(Some("t1"), "two").await.expect("submit");
    let third = backend.submit(Some("t1"), "three").await.expect("submit");
    assert!(second.queued && third.queued);

    control::handle_agent_event(&manager, &completed(&first.request_id)).await;
    let (request_id, message) = chat_command(&commands.try_recv().expect("the second turn goes"));
    assert_eq!(
        request_id, second.request_id,
        "the earlier message goes first"
    );
    assert_eq!(message, "two");

    control::handle_agent_event(&manager, &completed(&second.request_id)).await;
    let (request_id, message) = chat_command(&commands.try_recv().expect("the third turn goes"));
    assert_eq!(request_id, third.request_id);
    assert_eq!(message, "three");
    assert!(commands.try_recv().is_err());
}

/// An empty turn is read as empty against the turn that ended, not against a message that is
/// still waiting: a queued message is not the answer to anything yet.
#[tokio::test]
async fn a_waiting_message_is_not_mistaken_for_the_answer() {
    let dir = tempfile::tempdir().expect("dir");
    let (backend, manager, mut commands) = backend(dir.path());

    let first = backend.submit(Some("t1"), "first").await.expect("submit");
    let _ = commands.try_recv().expect("the first turn is sent");
    let second = backend.submit(Some("t1"), "second").await.expect("submit");
    assert!(second.queued);

    // No assistant message arrived before the completion, and the message behind it is not one.
    control::handle_agent_event(&manager, &completed(&first.request_id)).await;

    assert!(
        entry_types(&manager)
            .await
            .iter()
            .all(|(_, entry)| entry.as_deref() != Some("error")),
        "a turn with no answer is not recorded as an empty answer here"
    );
}
