//! A turn accepted while another runs.
//!
//! Two people in one room is the premise, and the second person's message must not be refused.
//! What the executor cannot take yet waits, and the wait is something the thread itself carries, so
//! every client reads the same order and a restart can find it. What this pins is that the message
//! is recorded, that no command reaches the executor while it is busy, and that the turn goes when
//! the one in flight ends.

use std::sync::Arc;

use actus::acpws::backend::AcpwsBackend;
use actus::acpws::control;
use actus::acpws::types::{Command, SyncEvent};
use actus::acpws::AcpwsManager;
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

/// What a thread's messages are, as content and the state each carries.
async fn entries(
    manager: &Arc<RwLock<AcpwsManager>>,
    thread_id: &str,
) -> Vec<(String, Option<String>)> {
    manager
        .read()
        .await
        .window(thread_id, 0, 20)
        .expect("window")
        .into_iter()
        .map(|message| (message.content, message.entry_type))
        .collect()
}

/// The turns of a thread that have not run, as the content of the messages that carry them.
async fn waits(manager: &Arc<RwLock<AcpwsManager>>, thread_id: &str) -> Vec<String> {
    let mgr = manager.read().await;
    let Some(thread) = mgr.threads.get(thread_id) else {
        return Vec::new();
    };
    thread
        .waiting
        .iter()
        .filter_map(|id| {
            thread
                .messages
                .iter()
                .find(|message| message.message_id.as_ref() == Some(id))
                .map(|message| message.content.clone())
        })
        .collect()
}

/// The second message of a room is accepted while the agent runs the first, and runs when the first
/// ends. No command reaches the executor in between, and the wait is the thread's own state.
#[tokio::test]
async fn a_second_turn_waits_for_the_one_in_flight() {
    let dir = tempfile::tempdir().expect("dir");
    let (backend, manager, mut commands) = backend(dir.path());

    let first = backend.submit(Some("t1"), "first").await.expect("submit");
    assert!(!first.queued, "the agent was free, so the turn runs now");
    let (request_id, message) = chat_command(&commands.try_recv().expect("the first turn is sent"));
    assert_eq!(request_id, first.request_id);
    assert_eq!(message, "first");
    // What the agent wrote while the turn ran.
    {
        let mut mgr = manager.write().await;
        mgr.add_message_full(
            &first.thread_id,
            "assistant",
            "an answer",
            Some("a-1".to_string()),
            Some("text".to_string()),
            None,
            None,
            None,
        );
    }

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
    assert_eq!(
        waits(&manager, "t1").await,
        vec!["second".to_string()],
        "the thread says which of its turns have not run"
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

    assert!(
        waits(&manager, "t1").await.is_empty(),
        "the message is the turn that runs now, so it is no longer a wait"
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

/// The queue is in arrival order, so a room's second and third messages run in the order they were
/// said rather than in whichever order the agent happens to free up.
#[tokio::test]
async fn the_queue_keeps_the_order_the_messages_arrived() {
    let dir = tempfile::tempdir().expect("dir");
    let (backend, manager, mut commands) = backend(dir.path());

    let first = backend.submit(Some("t1"), "one").await.expect("submit");
    let _ = commands.try_recv().expect("the first turn is sent");
    let second = backend.submit(Some("t1"), "two").await.expect("submit");
    let third = backend.submit(Some("t1"), "three").await.expect("submit");
    assert!(second.queued && third.queued);
    assert_eq!(
        waits(&manager, "t1").await,
        vec!["two".to_string(), "three".to_string()],
        "both waits are the thread's, in the order they arrived"
    );

    control::handle_agent_event(&manager, &completed(&first.request_id)).await;
    let (request_id, message) = chat_command(&commands.try_recv().expect("the second turn goes"));
    assert_eq!(
        request_id, second.request_id,
        "the earlier message goes first"
    );
    assert_eq!(message, "two");
    assert_eq!(waits(&manager, "t1").await, vec!["three".to_string()]);

    control::handle_agent_event(&manager, &completed(&second.request_id)).await;
    let (request_id, message) = chat_command(&commands.try_recv().expect("the third turn goes"));
    assert_eq!(request_id, third.request_id);
    assert_eq!(message, "three");
    assert!(commands.try_recv().is_err());
    assert!(waits(&manager, "t1").await.is_empty());
}

/// A turn that answered is not read as empty because a message waits behind it. The empty check
/// reads past the waits to the last message that belongs to a turn, and that message is the
/// agent's.
#[tokio::test]
async fn an_answered_turn_is_not_read_as_empty_because_a_wait_is_behind_it() {
    let dir = tempfile::tempdir().expect("dir");
    let (backend, manager, mut commands) = backend(dir.path());

    let first = backend.submit(Some("t1"), "first").await.expect("submit");
    let _ = commands.try_recv().expect("the first turn is sent");
    {
        let mut mgr = manager.write().await;
        mgr.add_message_full(
            &first.thread_id,
            "assistant",
            "an answer",
            Some("a-1".to_string()),
            Some("text".to_string()),
            None,
            None,
            None,
        );
    }
    let second = backend.submit(Some("t1"), "second").await.expect("submit");
    assert!(second.queued, "the second message waits behind the answer");

    control::handle_agent_event(&manager, &completed(&first.request_id)).await;

    assert!(
        entries(&manager, "t1")
            .await
            .iter()
            .all(|(_, entry)| entry.as_deref() != Some("error")),
        "the empty check read the answer, so no empty answer was recorded"
    );
}

/// A turn that answered nothing is still recorded as empty, with a message waiting behind it: the
/// wait does not stand in for the answer and does not mask the absence of one.
#[tokio::test]
async fn an_empty_turn_is_still_read_as_empty_though_a_wait_is_behind_it() {
    let dir = tempfile::tempdir().expect("dir");
    let (backend, manager, mut commands) = backend(dir.path());

    let first = backend.submit(Some("t1"), "first").await.expect("submit");
    let _ = commands.try_recv().expect("the first turn is sent");
    let second = backend.submit(Some("t1"), "second").await.expect("submit");
    assert!(second.queued);

    // No assistant message arrived before the completion.
    control::handle_agent_event(&manager, &completed(&first.request_id)).await;

    let entries = entries(&manager, "t1").await;
    assert!(
        entries
            .iter()
            .any(|(content, entry)| content == "[error] empty response"
                && entry.as_deref() == Some("error")),
        "a turn with no answer is recorded as one: {entries:?}"
    );
}
