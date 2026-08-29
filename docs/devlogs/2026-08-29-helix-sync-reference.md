# Helix Sync Implementation Reference

## Purpose

This devlog records what the Helix platform (github.com/helixml/helix) implements on the agent control side, how it differs from actus, and which ideas are safe to absorb. It exists so the knowledge does not live only in chat history.

The Helix platform is a Go product with a restricted source-available license. Actus is an independent Rust implementation. The boundary is important: ideas, protocol behavior, and bug causes can be studied freely, while concrete code expression must be written independently. This document records behavior and design intent, not Go code.

## Helix Agent Control Architecture

Helix runs headless Zed instances inside Docker containers and controls them over a single bidirectional WebSocket. The Go side lives in `api/pkg/server/websocket_external_agent_sync.go` (4502 lines) with a shared protocol package in `api/pkg/server/wsprotocol/`.

The protocol matches what actus already implements: `chat_message` commands with `request_id` and `acp_thread_id`, then `thread_created`, `message_added*`, `message_completed` events. Helix maintains a map from `acp_thread_id` to Helix session IDs, the same role actus's `thread_id_map` plays.

## MessageAccumulator

Zed emits multiple distinct entries per response turn: an assistant text block, one or more tool calls, and a follow-up message. Each entry has its own `message_id`. Within one entry, Zed streams cumulative content updates (overwrite semantics), not deltas.

The Helix accumulator keeps four structures per interaction:

- `messageOrder`: ordered list of message ids in insertion order
- `messageContent`: map from message id to content
- `messageType`: map from message id to entry type, `text` or `tool_call`
- `messageToolName` and `messageToolStatus`: tool metadata for tool call entries

A known message id replaces its content in place. A new id appends to the order list. The full content string is joined from the ordered parts with a double newline separator, rebuilt lazily because joining multi-megabyte strings on every token is expensive.

Actus reached the same design in `ZedManager::add_message_full`: replace by id anywhere in the thread, append only for new ids. The remaining gap is replay filtering, described next.

## Replay Filtering

The critical Helix discovery: Zed's `flush_streaming_throttle` resends ALL ACP thread entries when a turn completes. The corrected content for earlier message ids arrives after later ids were already seen. Additionally, on a follow-up turn, the wrapper replays entries from previous turns.

Helix handles this in two layers:

- Same interaction replay: the accumulator replaces by message id, so a corrected resend of an earlier id updates in place instead of duplicating.
- Cross interaction replay: `priorMessageContent` stores (message id, content) snapshots from earlier completed interactions. An incoming event whose id and content both match a prior entry is dropped as a wrapper replay.

The content comparison matters. Filtering by id alone was tried and caused an incident: when the wrapper restarts inside Zed, message ids reset and are reused for genuinely new content. Dropping those produced an empty interaction and the agent bounced with an empty response error. Matching on id plus content distinguishes a replay (identical content) from renumbered new content (different content).

Actus currently scopes message ids as `acp_thread_id:message_id`, which prevents collisions across ACP threads. That solves the cross-thread half of the problem. The same-thread replay of a prior turn's entries is not yet filtered.

## Request Id Consumption

Helix routes `message_completed` through a `requestToInteractionMapping`. When a completion is processed, the mapping entry is overwritten with an empty string sentinel rather than deleted. A later duplicate completion for the same request id finds the sentinel and is dropped.

This defends against the interrupt race that sends two completions for one cancelled turn, and against stale wrapper replays tagged with an already consumed request id. The sentinel approach also prevents a stale event from rebinding to a new waiting interaction.

Actus deletes the pending request entry on completion. Deleting loses the ability to detect a duplicate completion. The state transition after a duplicate is idempotent in actus today, but the sentinel pattern is a cheap defense worth adopting.

## Completion State Machine

Helix resolves the target interaction for a completion without timing assumptions, purely from state:

1. Try the request id to interaction mapping.
2. If no mapping, check the streaming context; if the agent is streaming to a different interaction, the completion is stale.
3. If neither, fall back to the database and match the most recent waiting interaction.

Already complete or interrupted interactions are skipped. An empty response marks the interaction as an error and re-queues the prompt. If `chat_response_error` already stored a real error, that error is preserved.

Actus increments `turn_completed` on completion and records `[error]` or `[cancelled]` assistant messages for the error paths. The empty response case is not distinguished from a normal completion.

## Crash Classification

Helix classifies certain error strings as agent crashes, including the `agent turn aborted` message actus has seen. A crash marks the queued prompt as crashed so the queue stops dispatching into the dead process, and triggers an automatic restart on autonomous surfaces.

Actus treats every `chat_response_error` the same way: record the error message and release consumers. There is no crash classification or restart policy.

## Differences Summary

| Mechanism | Helix | Actus |
|---|---|---|
| Message id replacement | map keyed by id | same, plus acp thread scoping |
| Cross turn replay filter | id plus content match | acp thread scoping only |
| Duplicate completion | consumed sentinel | delete mapping |
| Empty response | mark error, re-queue | no distinction |
| Crash classification | detect, mark, restart | none |

## Absorbing the Knowledge Safely

The license boundary is the reason this document records behavior, not code. Reimplementing in Rust from the documented behavior is the safe path and matches actus's existing structure. The concrete next steps are all behavioral, not translational:

- Add an id plus content replay filter to `add_message_full`, keyed by thread, so a prior turn's replayed entries cannot reappear as new messages.
- Replace pending request deletion with a consumed sentinel so duplicate completions are detectable and dropped.
- Distinguish empty completions from normal ones so consumers do not see a silent success.

These changes are already part of the actus design vocabulary; the Helix platform only confirms the failure modes they prevent.

## References

- Helix fork blog post: How We Forked Zed and Added Remote Control for Agent Fleet Orchestration
- github.com/helixml/helix: `api/pkg/server/websocket_external_agent_sync.go`, `api/pkg/server/wsprotocol/accumulator.go`
- actus: `src/zed/control.rs`, `src/zed/mod.rs`, `src/zed/backend.rs`
