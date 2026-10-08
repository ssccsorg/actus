# A Turn Accepted While Another Runs

date: 2026-10-08
project: Actus
status: designed and landed; the queue is in actus, the gateway still refuses
related:

- kletos `gateway/src/main.rs`, `gateway/tests/multiuser.rs`
- actus `src/acpws/backend.rs`, `src/acpws/control.rs`, `src/agent/mod.rs`

## The defect

Two people in one room, which is the pilot's whole premise. One is running a turn. The other's
message is refused with `HTTP 429: the agent is running a turn; one sandbox runs one turn at a
time`, and what they typed is gone.

The app queues a message locally, so a single user almost never sees this. The moment a second
client shares the folder it is the ordinary case, and the second person is the one who loses the
message.

## What the code does today

The refusal is the gateway's and not actus's. `submit_turn` in `gateway/src/main.rs` takes a lock
keyed by the sandbox name and refuses when `current_turn(owner).is_some()`. The in-flight turn is
cleared by the client's own poll reporting `completed`, or by `turn_stale` when the client went away.

actus neither refuses nor queues. `AcpwsBackend::submit_with_options` adds the user's message to the
thread

```rust
mgr.add_message_full(&tid, "user", message, None, None, None, None, author);
```

and then sends `Command::ChatMessage` to the executor. There is no per-agent admission check in that
path.

So the record already holds the message the refusal prevents, and the only thing standing between
the second person's message and the thread is the gateway.

## Why the serialization is ours

`AgentKind::Acpws` declares `parallel: false`. The contract says the executor does not run two turns
at once, so serializing cannot be delegated to it. `ExtCli` declares `parallel: true`, and a device
turn that mutates nothing can run beside another.

## Where a lock is genuinely needed

The contended thing is the workspace, not the thread. Every thread of one agent shares one folder and
a turn may mutate it. So the lock's scope is the agent, and it is needed when the executor is not
parallel. A parallel executor whose turns touch no shared state needs no lock.

A per-thread lock would be wrong: it would let two turns mutate one workspace at once.

What is per-thread is the queue. A message to a busy agent belongs to its thread and is ordered
there.

## The design

Read this section as the design, not as the state: the mark on the message is the part the storage
refused, and "What landed" below records what replaced it. The reason it looked right is here, and
the reason it was wrong is there.

A message accepted while a turn runs is recorded in the thread and dispatched when the turn ends.
Three properties follow from putting it there rather than beside it.

The queue is derived from the record. A queued message is a user message with a queued state and no
answer, so the pending dispatch is a small table keyed by request id and the queue after a restart
rebuilds by reading the record. Nothing new is persisted and nothing can drift from the thread.

Nothing has to be synchronized. Clients read the thread, so a queued message shows to every client
the way the thread's other messages do.

A receipt says the turn is queued, so a caller reports it rather than guessing.

`ThreadMessage` already carries `entry_type` and `author`, so a queued message needs no new type: it
is a message whose state says queued and whose author is who sent it.

The state on the message is what did not hold. A volume's record is written once, so the state could
not be cleared, and `ThreadSession::waiting` carries it instead.

## The queue's semantics, from upstream Zed

The queue is a mature thing upstream, so its rules are adopted rather than invented. In Zed the
queue is per thread (`thread.message_queue`, `crates/agent_ui/src/conversation_view/message_queue.rs`),
FIFO, and each entry is editable and removable. Its rules:

Release on the generation-stopped event, which sends the front entry.

One entry while a turn runs may `steer`: only the front entry carries it, and it interrupts at the
next turn boundary rather than waiting for the turn to finish.

Explicit actions pace the queue by hand. Send-now pops an entry even while generating, which
cancels the running turn, and the cancellation's stopped event is then absorbed so the queue does
not send twice.

A manual stop pauses the queue, and queueing a message or sending resumes it.

Auto-send waits while the user is editing the next entry.

A turn's end here is the same trigger as the generation-stopped event, so the release rule carries
over unchanged.

## The one place we differ

Zed's queue is client-side because a Zed thread has one client. Ours has several, so the queue has
to be in the record or two clients cannot see the same order. That is the whole of the difference,
and it has one consequence.

The record is append-only, so a queued message cannot be edited or deleted in place the way a Zed
entry can. Its state changes instead: queued, dispatched, answered, withdrawn. That is a lifecycle,
and the format already carries one for intents, so a queued message is an intent rather than a new
kind of message.

## What changes, in order

actus. `submit_with_options` records the message as it does now; when the agent already has a live
request and the kind is not parallel, it records the message as queued and does not send the
command. `live_requests()` is the busy test. The turn's end is where the request id leaves
`pending_requests` in `src/acpws/control.rs`, and that is where the next queued turn is dispatched.
`SubmitReceipt` gains the queued indication.

kletos gateway. The refusal goes, and with it the per-agent turn lock, whose only job was the
refusal; the turn log shrinks to what the client's poll still needs. The four `multiuser.rs`
assertions that expect 429 expect a queued accept instead.

The app. It already queues locally and renders that state. A message queued by the server is rendered
from the thread's own record, so the two read the same to a person.

Order matters. The gateway must not stop refusing before actus queues, or two turns reach a
non-parallel executor.

## Not decided here

The queue's bound, and what a caller sees for a full queue. Zed has none, because a client's queue
is the client's own memory; a shared one needs a stated bound.

Whether a withdrawal needs its own status or is expressed another way, given that the record is
append-only and an intent's status axis is submitted, claimed, and concluded.

Whether `steer` and send-now are in the first unit. Both cancel a running turn, so both need the
absorbing rule Zed found it needed.

Whether the app's local queue stays. It should: it is what makes a one-user session instant, and it
is not the same thing as the room's order.

## What landed, and the design decision the storage forced

The queue is in actus. `AcpwsManager::queued_turns` holds the turns that are waiting, in arrival
order; `submit_with_options` records the message and queues it when `has_live_request()` says the
agent is running one; `dispatch_queued` sends the front turn; the three places a turn can end
(answered, failed, cancelled) call it through `release_queue`, and so does `agent_ready`, which is
what lets a rebuilt queue run. `AgentBackend::request_state` answers a turn by the request id its
receipt carried (`Running`, `Waiting`, `Settled`, `Unknown`), and both the poll and the stream take
that answer, so a client that names the request it submitted is told about its own turn rather than
about the thread's counter. A backend that cannot answer says `Unknown` and the counter answers as
it always did.

One part of the design did not survive contact with the storage, and the reversal is the important
thing in this record.

The design said the record would carry the wait: a queued message is a user message whose state says
it has not run, and dispatch clears that state. That was implemented, and it was wrong. A volume's
record is written once and never rewritten (`src/store/volume.rs`: a name resolves to an address, so
a record never changes), and a store that writes one message at a time persists only the messages a
thread has not deposited yet. So clearing a mark on a message is a write the persistence layer
discards. A probe confirmed it: after the mark was cleared and the state flushed, the record still
held it. Two consequences, both bad: a client reading the record after a restart would see a wait
that had already run, and a queue rebuilt from those marks would run that turn a second time.

The wait therefore lives in actus's own state about the thread, `ThreadSession::waiting`, which is
the list of the ids of its messages that are turns which have not run. It sits beside the message
counts and the title versions, which are carried in the index for the same reason: they are derived,
the volume cannot cheaply answer them, and the head is rewritten whole on every persist, so a change
to them is a write the storage can make. What the record holds is the message, and what the index
holds is which of a thread's messages are still waiting. `is_waiting(&thread.waiting, message)` is
the single question every reader asks, so `read_turn` keeps the running turn whole and the
empty-turn check reads past the waits.

The rebuild is `AcpwsManager::rebuild_queue`, called once at startup: a thread the index marks as
holding a wait is read, and each waiting message becomes a queued turn. The request id is minted
because the record does not carry it, and the reasoning effort the turn asked for is not recoverable,
so a rebuilt turn runs at the thread's own level. Everything else about the wait, the message, its
author and its place in the order, is the record's.

The tests are `tests/queued_turn_test.rs` (the second message waits and then runs, the queue keeps
arrival order, and the empty check reads past a wait in both directions), `tests/server_test.rs`
(`poll_does_not_serve_a_turn_that_is_still_waiting`, `a_queued_submit_is_followed_by_the_stream`, and
`a_poll_by_request_answers_about_that_request`),
`tests/manager_index_test.rs::a_waiting_turn_is_rebuilt_and_runs_once_the_executor_is_ready` (a
restart over a volume, which also pins that the clear reaches that volume's record), and
`kletos/client/tests` (`turn_test` asserts the polls name the request the submit answered with, and
`request_test` asserts the query carries it).

Both are named here because a reader who finds the queue working should know which of its promises
are still owed.

The tests are in `tests/queued_turn_test.rs` (the second message waits and then runs, the queue keeps
arrival order, and the empty check reads past a wait in both directions),
`tests/server_test.rs` (`poll_does_not_serve_a_turn_that_is_still_waiting`,
`a_queued_submit_is_told_so_by_the_stream`, and `a_poll_by_request_answers_about_that_request`), and
`kletos/client/tests` (`turn_test` asserts the polls name the request the submit answered with, and
`request_test` asserts the query carries it).
