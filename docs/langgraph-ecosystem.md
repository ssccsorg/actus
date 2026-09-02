# Actus and the LangGraph Ecosystem

## Purpose

This document records the relationship between actus, an agent execution runtime, and the LangGraph ecosystem. It serves two readers: a maintainer deciding whether to implement the declared `AgentKind::LangGraph` adapter, and an implementer mapping the `AgentBackend` trait onto the LangGraph Agent Server API.

The analysis is grounded in the official LangGraph documentation and repositories as of this writing. Product naming in this area shifted recently (LangGraph Server became the Agent Server inside the LangSmith Deployment surface), so the document records the names that appear in the current docs and flags the ones that changed.

## Positioning

Actus is an execution fabric. It exposes one `AgentBackend` trait and one REST API, and behind that surface it weaves whatever agent platform a task needs. The telos agent is the first adapter (ACP over WebSocket). LangGraph is the declared second platform kind, but no adapter exists yet.

LangGraph is a graph-based agent orchestration framework with a server runtime. The ecosystem splits into layers:

| Layer | Product | Role |
|---|---|---|
| Orchestration library | `langgraph` (Python), `@langchain/langgraph` (JS) | Build stateful agents with durable execution, human-in-the-loop, memory |
| Local runtime | LangGraph CLI (`langgraph dev`) | Launch an Agent Server locally |
| Server runtime | Agent Server (formerly LangGraph Server) | REST/SSE API over assistants, threads, runs, crons; persistence and task queue |
| Deployment | LangSmith Deployment (formerly LangGraph Platform) | Cloud, self-hosted, hybrid, standalone hosting |
| Observability | LangSmith | Tracing, evaluation, Studio UI |

The relationship is complementary, not competing. Actus owns lifecycle and routing across platforms; LangGraph owns graph state and durable execution inside one platform. A `LangGraphBackend` adapter would let actus orchestrate a LangGraph agent exactly as it orchestrates Telos today.

## Verified API Surface

The Agent Server organizes its API into assistants, threads, thread runs, stateless runs, crons, store, and MCP tags. The endpoint paths below come from the server OpenAPI document.

| Resource | Method and path |
|---|---|
| Create assistant | `POST /assistants` |
| Search assistants | `POST /assistants/search` |
| Create thread | `POST /threads` |
| List threads | `POST /threads/search` |
| Get thread | `GET /threads/{thread_id}` |
| Get thread state | `GET /threads/{thread_id}/state` |
| Update thread state | `POST /threads/{thread_id}/state` |
| Background run | `POST /threads/{thread_id}/runs` |
| Run and wait | `POST /threads/{thread_id}/runs/wait` |
| Run and stream | `POST /threads/{thread_id}/runs/stream` |
| List runs | `GET /threads/{thread_id}/runs` |
| Get run | `GET /threads/{thread_id}/runs/{run_id}` |
| Join run | `GET /threads/{thread_id}/runs/{run_id}/join` |
| Cancel run | `POST /threads/{thread_id}/runs/{run_id}/cancel` |
| Thread cron | `POST /threads/{thread_id}/runs/crons` |
| Stateless runs | `POST /runs`, `POST /runs/stream`, `POST /runs/wait` |
| Protocol v2 events | `POST /threads/{thread_id}/stream/events` |
| Protocol v2 commands | `POST /threads/{thread_id}/commands` |
| MCP server endpoint | `POST /mcp/` |

A run is created with an assistant id, an input, optional interrupt points, a stream mode, a multitask strategy, and a durability setting. Thread status is `idle`, `busy`, `interrupted`, or `error`; run status is `pending`, `running`, `error`, `success`, `timeout`, or `interrupted`.

Streaming runs return a text event stream. The protocol v2 endpoint (`/threads/{thread_id}/stream/events`) is a connection-scoped, thread-scoped subscription over channels such as `values`, `updates`, `messages`, `tools`, and `lifecycle`; frames carry a sequence number and the connection can resume from a sequence with `since`. The same path supports WebSocket.

## Trait Mapping

The `AgentBackend` trait in `src/agent/mod.rs` declares ten methods. The mapping below is analytic: each trait method is paired with the LangGraph call that would implement it. The gaps section records the points where the mapping is not mechanical.

| Trait method | LangGraph call | Notes |
|---|---|---|
| `status()` | Server health plus `GET /threads/{thread_id}` status field | Health path is under the System tag; not re-verified in detail |
| `submit(thread_id, message)` | `POST /threads/{thread_id}/runs` with input `{messages: [message]}` | Receipt is the `run_id`; status starts `pending` |
| `cancel()` | `POST /threads/{thread_id}/runs/{run_id}/cancel` | Cooperative, checkpoint-based, not an immediate kill |
| `thread(thread_id)` | `GET /threads/{thread_id}` and `GET /threads/{thread_id}/state` | State returns values, next, tasks, interrupts |
| `threads()` | `POST /threads/search` | Listing is a POST with limit/offset |
| `subscribe()` | `POST /threads/{thread_id}/stream/events` | Push-based per thread; actus watch channel can wrap it |
| `pending_tool_calls()` | Derive from thread `interrupts` or the `tools` channel | No dedicated approval-list endpoint |
| `resolve_tool_call()` | `POST /threads/{thread_id}/runs` with `command.resume` | Resume value is a graph-level contract |
| `create_thread()` | `POST /threads` | Returns `thread_id` |
| `name()`, `kind()` | Static config | No server call |

## Gaps and Design Notes

Cancel is cooperative. LangGraph cancels by interrupting or rolling back through the checkpointer, so a cancelled run may still persist state. The adapter should treat `cancel()` as best-effort and reconcile thread state afterward.

Tool approval has no first-class endpoint. The idiomatic mechanism is an `interrupt()` inside a tool or an `interrupt_before` breakpoint on a tool node; approval arrives as the resume value of the next run. The adapter therefore maps `pending_tool_calls()` to interrupts observed in thread state or the `tools` channel, and `resolve_tool_call()` to a resume run. The approve/reject payload shape must be agreed with the graph author; actus cannot infer it from the API alone.

Subscription is per thread. A fleet-wide watch, which actus needs for `/v1/threads` and SSE fan-out, requires one event-stream connection per thread or a polling fallback over `GET /threads/{thread_id}/state`.

Concurrency needs an explicit choice. The default `multitask_strategy` is `enqueue`, which queues a second run on a busy thread and may delay `submit` indefinitely. The adapter should let the config select `reject`, `interrupt`, `rollback`, or `enqueue` per agent to match actus semantics.

There is no official Rust client. The official SDKs are Python (`langgraph-sdk`) and JS (`@langchain/langgraph-sdk`). Community crates exist but none implement the Agent Server client with any adoption. The REST surface is plain JSON over HTTP, so a hand-written Rust adapter is feasible and the trait mapping above is the specification for it.

## Ecosystem Relationships

MCP connects in both directions. LangGraph agents consume MCP tools through `langchain-mcp-adapters`, and the Agent Server exposes an assistant as an MCP server at `POST /mcp/`. Actus already injects MCP servers into Telos via `context_servers`; a LangGraph agent would instead declare MCP consumption inside the graph, so the config surface differs.

ACP has no official presence in the LangGraph ecosystem. No LangGraph or Telos ACP integration was found in official docs or repositories; only small community bridges exist. This matters because actus's Telos adapter speaks ACP over WebSocket, so the two adapters would share no wire protocol.

The OpenAI Assistants API is conceptually parallel (assistants, threads, runs, messages) but wire-incompatible, and OpenAI has deprecated it in favor of the Responses API. LangGraph's value over that surface is graph control, interrupts, and checkpointing.

LangSmith is the observability layer. Agent Server traces runs into LangSmith automatically, so a LangGraph agent under actus gains tracing without actus work, which the Telos adapter cannot offer.

## Recommendation

The analysis supports implementing `AgentKind::LangGraph` as the second adapter, with two prerequisites: agree on the interrupt-based approval payload contract, and select a per-agent multitask strategy. The work is a separate task subject; this document is the mapping reference for it.

## References

- docs.langchain.com: Agent Server overview and API reference, LangSmith Deployment overview, local server guide
- docs.langchain.com/oss/python/langgraph: interrupts, streaming, checkpointers
- github.com/langchain-ai/langgraph and github.com/langchain-ai/langgraphjs
- reference.langchain.com/python/langgraph-sdk
- npm package `@langchain/langgraph-sdk` v1.10.0
