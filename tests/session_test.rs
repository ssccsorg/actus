// Integration tests for the session layer (src/agent/session.rs).
//
// The store is the one definition of a thread and a turn: it allocates the
// thread, titles it from the first message, records the dispatch origin once,
// marks the turn incomplete and then complete, and wakes subscribers. The
// auxiliary adapters are its callers, so these tests are where the contract
// lives that they all used to carry a copy of.

use std::time::Duration;

use actus::agent::session::{ThreadStore, TurnReply};
use actus::agent::{ActOutcome, ThreadParent};

fn parent(agent: &str) -> ThreadParent {
    ThreadParent {
        agent: agent.to_string(),
        thread_id: format!("{agent}-thread"),
    }
}

#[tokio::test]
async fn a_turn_records_its_outcome_and_the_dispatch_it_came_from() {
    let store = ThreadStore::new("probe");
    let (tid, _) = store.get_or_create(None).await;
    let dispatch = parent("meta");

    store
        .begin_turn(&tid, "do the thing", Some(dispatch.clone()))
        .await
        .unwrap();
    store
        .finish_turn(
            &tid,
            TurnReply::message("it broke", Some("req-1".to_string()))
                .dispatched(Some(dispatch.clone()))
                .ended(ActOutcome::Failed),
        )
        .await
        .unwrap();

    let thread = store.thread(&tid).await.expect("the thread");
    let user = &thread.messages[0];
    assert_eq!(
        user.parent.as_ref().map(|p| p.agent.as_str()),
        Some("meta"),
        "the dispatch is on the message that opened the turn, so it survives a turn \
         that never reaches a reply"
    );
    assert_eq!(user.outcome, None, "a user message is not a reply");

    let reply = &thread.messages[1];
    assert_eq!(
        reply.parent.as_ref().map(|p| p.thread_id.as_str()),
        Some("meta-thread")
    );
    assert_eq!(
        reply.outcome,
        Some(ActOutcome::Failed),
        "how the turn ended is a field rather than prose"
    );

    // A turn nobody dispatched carries no parent of its own, and leaves the
    // first turn's dispatch where it was.
    store
        .run_turn(&tid, "again", None, TurnReply::message("done", None))
        .await
        .unwrap();
    let thread = store.thread(&tid).await.expect("the thread");
    let last = thread.messages.last().expect("a reply");
    assert_eq!(last.outcome, Some(ActOutcome::Ok));
    assert!(
        last.parent.is_none(),
        "a turn nobody dispatched carries no parent"
    );
}

#[tokio::test]
async fn a_named_thread_is_allocated_once_and_a_fresh_id_carries_the_kinds_prefix() {
    let store = ThreadStore::new("probe");

    let (named, is_new) = store.get_or_create(Some("thread-1")).await;
    assert_eq!(named, "thread-1");
    assert!(is_new, "an unknown name is allocated");

    let (again, is_new) = store.get_or_create(Some("thread-1")).await;
    assert_eq!(again, "thread-1", "the name is reused, not recreated");
    assert!(
        is_new,
        "the flag reports an empty thread, not a fresh name: a thread nothing has \
         been written to is still new to a caller"
    );

    store
        .run_turn(&named, "hello", None, TurnReply::message("world", None))
        .await
        .unwrap();
    let (_, is_new) = store.get_or_create(Some("thread-1")).await;
    assert!(!is_new, "a thread with a turn in it is not new");

    let (fresh, is_new) = store.get_or_create(None).await;
    assert!(is_new);
    assert!(
        fresh.starts_with("probe-"),
        "a fresh id says which kind minted it: {fresh}"
    );
    assert_ne!(fresh, named);

    let created = store.create_thread().await;
    assert!(store.thread(&created).await.is_some());
}

#[tokio::test]
async fn the_title_comes_from_the_first_message_and_the_origin_is_recorded_once() {
    let store = ThreadStore::new("probe");
    let (tid, _) = store.get_or_create(None).await;

    store
        .begin_turn(&tid, "first message", Some(parent("meta")))
        .await
        .unwrap();
    store
        .finish_turn(&tid, TurnReply::message("reply", Some("req-1".into())))
        .await
        .unwrap();

    store
        .begin_turn(&tid, "second message", Some(parent("other")))
        .await
        .unwrap();
    store
        .finish_turn(&tid, TurnReply::message("reply", Some("req-2".into())))
        .await
        .unwrap();

    let session = store.thread(&tid).await.unwrap();
    assert_eq!(session.title.as_deref(), Some("first message"));
    assert_eq!(
        session.parent.as_ref().map(|p| p.agent.as_str()),
        Some("meta"),
        "the first turn into a thread decides where it came from"
    );
    assert_eq!(session.turn_completed, 2);
    assert!(session.completed);
}

#[tokio::test]
async fn a_turn_opens_incomplete_and_closes_complete() {
    let store = ThreadStore::new("probe");
    let (tid, _) = store.get_or_create(None).await;

    store.begin_turn(&tid, "hello", None).await.unwrap();
    let open = store.thread(&tid).await.unwrap();
    assert!(!open.completed, "a turn in flight is incomplete");
    assert_eq!(open.messages.len(), 1);
    assert_eq!(open.messages[0].role, "user");
    assert_eq!(open.messages[0].content, "hello");
    assert_eq!(open.turn_completed, 0);
    assert!(!store.has_reply("req-1").await);

    store
        .finish_turn(&tid, TurnReply::message("world", Some("req-1".into())))
        .await
        .unwrap();
    let closed = store.thread(&tid).await.unwrap();
    assert!(closed.completed);
    assert_eq!(closed.messages.len(), 2);
    assert_eq!(closed.messages[1].role, "assistant");
    assert_eq!(
        closed.messages[1].entry_type.as_deref(),
        Some("agent_message")
    );
    assert_eq!(closed.messages[1].message_id.as_deref(), Some("req-1"));
    assert_eq!(closed.turn_completed, 1);
    assert!(
        store.has_reply("req-1").await,
        "a finished turn is findable by the request id it ran under"
    );
    assert!(
        !store.has_reply("never-issued").await,
        "an id no thread carries is not a finished turn"
    );
}

/// The in-process adapter answers inside the request, so its turn must not be
/// observable as incomplete: both messages land in one mutation, with the one
/// timestamp its reply shares with the request.
#[tokio::test]
async fn run_turn_writes_the_request_and_the_reply_in_one_step() {
    let store = ThreadStore::new("probe");
    let (tid, _) = store.get_or_create(None).await;

    store
        .run_turn(&tid, "ping", None, TurnReply::message("pong", None))
        .await
        .unwrap();

    let session = store.thread(&tid).await.unwrap();
    assert!(session.completed);
    assert_eq!(session.messages.len(), 2);
    assert_eq!(session.messages[0].timestamp, session.messages[1].timestamp);
    assert_eq!(session.title.as_deref(), Some("ping"));
    assert_eq!(session.turn_completed, 1);
}

#[tokio::test]
async fn closing_a_turn_wakes_a_subscriber_and_opening_one_does_not() {
    let store = ThreadStore::new("probe");
    let (tid, _) = store.get_or_create(None).await;
    let mut rx = store.subscribe();

    store.begin_turn(&tid, "hello", None).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(100), rx.changed())
            .await
            .is_err(),
        "opening a turn does not wake subscribers; the kinds differ and ask for it themselves"
    );

    store
        .finish_turn(&tid, TurnReply::message("world", None))
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(1), rx.changed())
            .await
            .is_ok(),
        "closing a turn wakes a subscriber"
    );

    store.notify();
    assert!(
        tokio::time::timeout(Duration::from_secs(1), rx.changed())
            .await
            .is_ok(),
        "an adapter can ask for the wakeup itself"
    );
}

#[tokio::test]
async fn a_turn_into_a_thread_that_vanished_is_an_error() {
    let store = ThreadStore::new("probe");

    let error = store
        .begin_turn("no-such-thread", "hello", None)
        .await
        .expect_err("a thread the store does not hold cannot be written to");
    assert!(error.contains("vanished"), "{error}");

    let error = store
        .finish_turn("no-such-thread", TurnReply::message("world", None))
        .await
        .expect_err("the same for a reply");
    assert!(error.contains("vanished"), "{error}");
}
