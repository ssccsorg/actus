# Meta-Agent Control Surface

## Purpose

This devlog records the architecture analysis and implementation plan for
making Telos, the default sessionful agent, act as a meta-agent: dispatch
independent raw-CLI agents (Ante, AURA, future ones) that actus manages as
`ext_cli` entries, and collect their results. The agent family is one act
category; meta-agent control turns the dispatch act itself into a
first-class, policy-gated surface of the runtime.

## Current Architecture Facts

- Every agent sits behind the `AgentBackend` trait (`src/agent/mod.rs`):
  `submit`, `thread`, `threads`, `cancel`, `cancel_request`, `status`.
  HTTP endpoints route by agent name through `agent_for` in
  `src/server.rs`; chat, thread, poll, and cancel endpoints all accept an
  `agent` field.
- Telos receives MCP context servers declared in its `[[agents.mcp]]`
  config entries: `ensure_telos_settings` in `src/telos/mod.rs` writes
  them into the agent's `context_servers` settings as stdio or HTTP
  servers. Injecting a control surface into telos therefore needs no
  telos-repo change.
- Telos ask-mode approval gates its own tool calls through the ACP
  `tool_call_authorization_requested` event and the actus pending/resolve
  bridge (`src/telos/backend.rs`). MCP tool calls are telos tool calls
  and pass through the same gate.
- `ext_cli` agents expose the dispatch primitive set after issue #18:
  submit, per-request cancel with process-group kill, per-agent workdir,
  launch probe, stdout capture with exit and timeout diagnostics.
- Missing piece: no channel lets an agent call the actus control plane,
  and no policy layer decides which agent may dispatch which other agent.
  actus HTTP is client-facing (CLI, curl); telos only receives settings
  and commands, it never calls actus.

## Required Control Loop

A telos tool call must reach an auxiliary agent and return its result:

```
telos session --tool call--> actus control surface --AgentBackend--> aux agent
        ^                                                        |
        +---------- poll / wait / cancel / result <-------------+
```

The control surface reuses the existing fabric endpoints and adds two
things: identity (which agent is calling) and policy (who may dispatch
whom).

## Design Decision: Stdio MCP Proxy plus Server-Side Policy

Options compared in review: an actus-hosted control MCP server injected
as a telos MCP entry (chosen), native tool hooks in the telos fork
(rejected: fork changes per agent, no central policy point), and an
external orchestrator such as kineTic (parallel option, not a substitute
when telos itself must choose dispatches).

Chosen shape:

- `actus control` runs a stdio MCP server as a child of telos (telos
  already spawns stdio MCP servers). It translates MCP tool calls into
  actus HTTP API calls on the loopback port, carrying the bearer token
  and an identity header `X-Actus-Controller: <agent name>`.
- The actus server enforces an allowlist policy from the config file
  (`[agent-control]`) on header-bearing dispatch and cancel requests.
  Requests without the identity header (the human CLI, curl) keep the
  current unrestricted behavior; agent-originated control is default
  deny.
- The launcher exports `ACTUS_HTTP_PORT`, `ACTUS_API_TOKEN`, and
  `ACTUS_AGENT_NAME` to spawned agents so the control proxy knows where
  to call and who it is.

Tools exposed to the meta agent: `agent_list`, `agent_submit`,
`agent_poll`, `agent_wait` (bounded polling), `agent_cancel`,
`agent_thread`. `agent_wait` returns the finished turn or an explicit
timeout error, so the meta agent sees the same diagnostics a CLI user
sees: cancelled marker, timeout message, exit code with stderr.

## Policy Model

Config:

```toml
[agent-control]
allow = [
  { controller = "telos", targets = ["ante", "aura"] },
]
```

`ControlPolicy::allows(controller, target)` matches exact names; `*`
matches any. Empty allow list means no agent may control another. A
controller cannot dispatch to itself regardless of the list (recursion
guard). Human clients are not controllers and are not gated.

## Guardrails

- Default deny allowlist: prompt injection inside a meta agent cannot
  reach an unlisted auxiliary agent.
- Ask-mode approval: telos ask mode already gates MCP tool calls through
  the HITL bridge; in `always` mode the allowlist is the gate.
- Self-dispatch rejection; deeper recursion is prevented by the allowlist
  (only listed leaf agents are reachable) until a depth rule is needed.
- Result diagnostics: `agent_wait` surfaces cancelled, timeout, and exit
  failure markers verbatim.

## Contract Pins

- `src/control.rs`: stdio MCP framing, tool definitions, HTTP translation
  with identity header.
- `src/server.rs`: policy gate on dispatch and cancel.
- `src/agent/config.rs`: `ControlPolicy` parsing.
- `src/main.rs` + `src/telos/mod.rs`: launcher exports and the `actus
  control` subcommand.
- Tests: MCP framing unit tests, HTTP policy tests, and the live
  telos x Ante measurement in issue #19-3.
