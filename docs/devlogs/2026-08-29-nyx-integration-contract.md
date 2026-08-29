# Nyx Integration Contract

## Purpose

This devlog freezes the wire contract between actus and the headless agent binary it drives. It is the reference for the planned nyx binary: actus must accept nyx as a drop-in replacement with no code change beyond the binary path. The contract is recorded in actus's own words, derived from the protocol actus actually exercises, and pinned by the integration tests.

The boundary from the previous devlog applies here too. The contract is a protocol interface, and the behavior actus depends on is documented as behavior, not as code copied from any fork. The upstream authoritative spec lives at `helix/subtree/crates/external_websocket_sync/PROTOCOL_SPEC.md`; this document records the subset actus uses and the semantics actus relies on.

## Roles

Actus runs one WebSocket server per agent on `127.0.0.1:{ws_port}` and launches the agent binary with the session id and that address. The agent connects to actus as a WebSocket client, executes agent turns, emits events, and receives commands. Actus also runs the HTTP API on `127.0.0.1:{http_port}` and persists threads at `~/.actus/threads/{agent_name}/`.

## Wire Format

- Events, agent to actus: `{"event_type": "...", "data": {...}}`
- Commands, actus to agent: `{"type": "...", "data": {...}}`

## Events Actus Consumes

| event_type | data fields | actus behavior |
|---|---|---|
| `agent_ready` | `agent_name`, `thread_id?` | marks the agent ready; submits are rejected until ready |
| `thread_created` | `acp_thread_id`, `request_id` | maps the request id to the thread; stale replays for consumed request ids are ignored |
| `thread_title_changed` | `acp_thread_id`, `title` | parsed by the protocol types; titles currently derive from the first user message, not from this event |
| `message_added` | `acp_thread_id`, `message_id`, `role`, `content`, `request_id?`, `entry_type?`, `tool_name?`, `tool_status?`, `timestamp` | accumulates or replaces by `message_id` scoped as `acp_thread_id:message_id`; `entry_type` distinguishes `text` from `tool_call` |
| `message_completed` | `acp_thread_id`, `message_id`, `request_id` | consumes the request mapping via a sentinel, increments the turn counter; a completion with no assistant content is recorded as an error |
| `chat_response_error` | `request_id`, `error` | records the error, consumes the request mapping, releases consumers |
| `turn_cancelled` | `request_id`, `status` | consumes the request mapping, records a cancelled marker |
| `tool_call_authorization_requested` | `acp_thread_id`, `tool_call_id`, `tool_name` | queues a pending authorization for a human decision |

## Commands Actus Emits

| type | data fields | purpose |
|---|---|---|
| `chat_message` | `message`, `request_id`, `acp_thread_id` | submits a user message; a null `acp_thread_id` creates a new thread |
| `cancel_current_turn` | `{}` | aborts the running turn |
| `resolve_tool_call_authorization` | `acp_thread_id`, `tool_call_id`, `allow` | approves or rejects a pending tool call |

## Behavioral Semantics Actus Depends On

These are the hard-won failure modes from the zed integration work. A replacement binary must preserve them.

- Cumulative content overwrite. `message_added` carries the full content of an entry, not a delta. A repeated `message_id` replaces the content in place.
- Turn-end replay. The agent resends thread entries when a turn completes, and replays prior-turn entries on follow-up turns. Actus filters replays by matching both id and content, so renumbered new content survives while identical replays are dropped.
- Request id lifecycle. Every `chat_message` carries a `request_id`. Exactly one terminal event (`message_completed`, `chat_response_error`, or `turn_cancelled`) per request is expected. Actus consumes the mapping with a sentinel and drops duplicate terminal events.
- Empty response is an error. A completion that produced no assistant content is recorded as an error, not a silent success.
- Stream ordering. One turn emits several assistant entries in order, for example thinking text, tool call, then the answer. Consumers diff by `message_id`.
- Stateless agent. The agent only knows `acp_thread_id`. Actus owns session mapping, thread persistence, and context injection.

## Upstream Spec Differences

The upstream spec additionally defines `user_created_thread`, `thread_load_error`, and `open_thread`. Actus does not use them. Nyx should implement the actus subset above, and nothing else is required for the swap.

## Contract Pins

- `tests/zed_types_test.rs`: wire format and serialization round trips for every event variant.
- `tests/agent_test.rs`: replay filter, sentinel consumption, scoped message ids, error and cancel paths.
- `tests/server_test.rs`: the HTTP API over the fabric, exercised through a real server.
- `src/zed/types.rs`: the `SyncEvent` and `IncomingChatMessage` types.

## Binary Swap Points

- `runner.py`: `--bin` argument and `ZED_BIN` detection.
- `run.sh`: `ZED_BIN` environment variable and `ensure_zed_binary`.
- `src/main.rs`: `launch_zed` receives the binary path per agent config.

## Nyx Implication

A nyx binary that speaks this subset and emits `agent_ready` after connecting can replace the helix headless binary without actus changes. The actus test suite doubles as the conformance suite for nyx: run `run.sh --test` with `ZED_BIN` pointing at the nyx binary, and the existing endpoint, protocol, and behavior tests become the acceptance gate.

## References

- `helix/subtree/crates/external_websocket_sync/PROTOCOL_SPEC.md`: authoritative upstream protocol spec
- `helix/subtree/crates/external_websocket_sync/src/protocol_test.rs`: upstream conformance flows
- actus: `src/zed/types.rs`, `src/zed/control.rs`, `src/zed/backend.rs`, `tests/zed_types_test.rs`, `tests/agent_test.rs`, `tests/server_test.rs`
- `docs/devlogs/2026-08-29-helix-sync-reference.md`: prior license boundary and behavior analysis
