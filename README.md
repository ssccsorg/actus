# Actus: the act runtime of the SSCCS stack

Actus is a headless execution runtime that performs acts over a shared
knowledge space. The name joins act and us: the runtime exists for acts.
An act is any unit of execution, from reading workspace context, editing
files, running commands, resolving symbols, and fetching resources, to
driving an autonomous agent through a task. The agent is the first type
of act and only one type among several.

Actus plans with the LLM, holds conversation state and workspace context,
and commands the execution agents that convert plans into system effects.
The default execution agent is Telos. If neXus is the knowledge fabric
(FIH blackboard, state space, storage), actus is the execution fabric:
the runtime that spawns agents, routes messages, persists threads, and
exposes a uniform HTTP API regardless of which agent or act type is
underneath.

## Architecture

```
External Client (CLI / HTTP)
        │
        ▼
  ┌─────────────────────────────────────┐
  │          Actus Server (REST)         │
  │  ┌─────────┐  ┌──────────┐          │
  │  │ Session  │  │  Agent   │          │
  │  │ Manager  │  │  Bridge  │          │
  │  └─────────┘  └────┬─────┘          │
  │  ┌────────────────────────────────┐  │
  │  │   Direct acts (no agent)       │  │
  │  │  File + Git · Rules · Fetch    │  │
  │  └────────────────────────────────┘  │
  └───────────────┬─────────────────────┘
                  │
        ┌─────────┴─────────┐
        ▼                   ▼
  ┌──────────┐      ┌──────────────┐
  │ Telos    │      │   Future     │
  │ (General │      │  Agent Types │
  │ Execution)│      │ (Research,   │
  │          │      │  Review,     │
  │          │      │  Deploy...)  │
  └──────────┘      └──────────────┘
```

Direct acts run inside the server: file search and mention, symbol and
rule lookup, URL fetch behind an SSRF guard, and git status, diff, and
log. Delegated acts run through the agent bridge. Telos is the first
default agent type: a general agent execution layer that converts plans
into system effects without binding to a language or a domain. It carries
file-system and git awareness, which suits code work, and the same
surface reaches processes and network effects. The architecture accepts
any act or agent that communicates over a contract, making actus a
universal gateway for execution.

## Execution Fabric

Like neXus weaves heterogeneous FIH storage types behind one thin
knowledge fabric, actus weaves heterogeneous act types behind one thin
execution fabric. Agents are the first implemented family of acts. Any
agent platform can be orchestrated through the same actus surface; Telos
is the default agent.

- `agent::AgentKind`: platform kinds (`telos`, `langgraph`, `native`),
  extensible by adding a kind and an adapter.
- `agent::AgentBackend`: uniform async trait (`status`, `submit`,
  `cancel`, `thread`, `threads`, `subscribe`) implemented by every
  platform adapter.
- `agent::AgentRegistry`: name to running adapter map with a default
  agent.
- `telos::backend::TelosBackend`: first adapter, wrapping `TelosManager`.
  ACP-over-WebSocket details (reconnect, event dispatch) stay inside
  `telos::control`; the adapter owns thread state and command submission.
- Direct acts: the server performs file, git, rules, fetch, and symbol
  services in-process; they are acts with no agent attached.

HTTP handlers talk only to the `AgentBackend` trait, so a new platform
(LangGraph Server over REST/SSE, an in-process Rust agent, a lightweight
auxiliary agent binary) plugs in by implementing the trait and
registering it. `/v1/health` reports per-agent status in the `agents`
map.

## Configuration

Agents are declared in `~/.actus/config.toml` (or `ACTUS_CONFIG`). When
the file is absent, a single default `telos` agent is derived from the CLI
flags and environment (`LLM_API_KEY`, `LLM_PROVIDER`, `LLM_BASE_URL`,
`LLM_MODEL`). The LLM layer belongs to the agent processes: actus treats
the endpoint as an OpenAI-compatible API and ships no LLM-specific
provider, model name, or API host of its own. The provider label, model,
and base URL always come from the local environment or the agent's config
entry.

```toml
[[agents]]
name = "telos"             # default agent; routed when no agent is named
kind = "telos"             # telos | langgraph | native (telos default; native is in-process)
provider = "openai-compatible"          # label of the OpenAI-compatible endpoint
model = "example-model"                 # model served by that endpoint (LLM_MODEL)
base_url = "https://api.example.com/v1" # base URL of that endpoint (LLM_BASE_URL)
api_key = "sk-..."                      # key for that endpoint (LLM_API_KEY)
bin = "../telos/target/telos-release/tel"
ws_port = 8080
tool_approval = "always"  # always | ask | never (drives the agent's approval policy)

  # MCP servers attached to this agent (stdio or http)
  [[agents.mcp]]
  name = "filesystem"
  command = "npx"
  args = ["-y", "@modelcontextprotocol/server-filesystem", "./"]

  [[agents.mcp]]
  name = "cloudflare-api"
  url = "https://mcp.cloudflare.com/mcp"

[[agents]]
name = "research"
kind = "telos"
provider = "anthropic"
model = "claude-sonnet-4"
ws_port = 8081
```

Each agent inherits any omitted field from the defaults. Every `telos`
agent needs a unique `ws_port`; thread state is persisted per agent under
`~/.actus/threads/{name}/`. Chat and thread endpoints accept an `agent`
field to route to a specific agent. MCP servers declared under an agent
are injected into the agent's `context_servers` settings and started by
the headless agent, exposing their tools to the model.

`tool_approval` sets the tool call approval policy. `always` auto-approves
tool calls (headless task execution); `ask` waits for a human or approval
bridge; `never` rejects them. The mode is carried to the agent via the
`TELOS_TOOL_APPROVAL` environment variable.

### Auxiliary Raw-CLI Agents

An `ext_cli` agent attaches any external binary that answers one prompt
per process invocation. Specialized agents attach without code changes:
Ante over its headless `-p` mode, AURA over its standalone `--query`
mode. Declare the agent like any other, with the CLI profile fields
`cli_args`, `cli_env`, `cli_prompt`, and `cli_timeout_secs`.

```toml
[[agents]]
name = "ante"
kind = "ext_cli"
bin = "ante"
cli_args = ["-p", "{prompt}"]   # {prompt} is replaced by the message

[[agents]]
name = "aura"
kind = "ext_cli"
bin = "aura"
cli_args = ["--config", "/path/to/agent.toml", "--query", "{prompt}"]
cli_timeout_secs = 600
```

Per turn actus spawns `bin` with the declared arguments plus the message
(either at the `{prompt}` marker or appended as the final argument), runs
it in the server working directory with the server environment plus
`cli_env`, and records stdout as the assistant reply. A non-zero exit
records the exit code with stderr; a timeout kills the process. A
`cli_env` value of the form `$NAME` is resolved from the server
environment at spawn time, so secrets stay out of `config.toml`.
`cli_prompt = "stdin"` writes the message to the child's stdin instead of
passing an argument. `workdir` overrides the server working directory per
agent for CLIs that resolve project scope from the current directory (for
example AURA locating `.aura/permissions.json`); a relative path resolves
against the directory actus was started from, and an unresolvable entry
fails the launch with the agent name.

Raw-CLI agents are one-shot and parallel. Threads stay in memory for the
server lifetime and do not survive a restart. The asymmetry to sessionful
telos agents is deliberate: a raw-CLI turn is a stateless one-shot
process that cannot resume, while telos threads persist to disk under
`~/.actus/threads/{name}/`. Thread management parity (issue #3) decides
whether auxiliary threads persist later. There is no streaming or tool
approval surface, and the model settings live inside the agent's own
configuration, which actus does not read. The profile contract and
integration notes are recorded in
`docs/devlogs/2026-09-06-aux-agent-profiles.md`.

## `@` Mention Context

Typing `@` in the CLI injects context into the message before it is sent,
mirroring Telos's mention picker:

| Form | Source | Example |
|---|---|---|
| `@path/to/file` | file paths auto-injected (top matches) | `@src/server.rs` |
| `@?query` | interactive picker across files, symbols, threads | `@?server` |
| `@rules` | project rule files (AGENTS.md, *.mdc) | `@rules` |
| `@symbol:query` | definition-pattern symbol search | `@symbol:search_symbols` |
| `@thread:query` | conversation thread content | `@thread:thread-title` |
| `@fetch:URL` | fetched URL text | `@fetch:https://example.com` |

Server endpoints: `/v1/symbols?q=`, `/v1/rules`, `/v1/fetch?url=`.
Diagnostics mention is deferred (requires a language server).
Plain `@` mentions stay non-interactive so chat flows smoothly; the
picker opens only for explicit `@?query`.

## Agent Types

The agent family is the first implemented type of act. Each agent is an
external process behind the registry.

| Agent | Role | Protocol |
|---|---|---|
| Telos | General execution: converts plans into system effects across files, commands, and network | ACP over WebSocket |
| (planned) Research Agent | Literature search, experiment design | TBD |
| (planned) Review Agent | Code review, compliance checking | TBD |
| (planned) Deploy Agent | CI/CD, infrastructure management | TBD |
| (trial) Auxiliary Agent | Lightweight input/output tasks alongside the professional core, attached over a raw-CLI profile; first candidates are Ante (`-p` one-shot) and AURA (`--query` one-shot) | CLI (one process per turn) |

## Getting Started

### Prerequisites

- Rust toolchain
- Python 3.12+
- A built telos binary (build the sibling `telos` repo; default path
  `../telos/target/telos-release/tel`)

### Quick Start

```bash
# Build and start server with interactive CLI
./run.sh

# Run the test suite (static checks, unit and integration tests, HTTP smoke tests)
./run.sh --test

# Start server only (background)
./run.sh --server-only

# Connect CLI to existing server
./run.sh --cli
```

### Docker

```bash
# Build the slim runtime image (default target: actus binary and git only)
docker build -t actus .

# Build the test image used by CI (adds python3, curl, git for run.sh --test)
docker build --target test -t actus:test .
```

Published images are cut on version tags (a versioned image plus `latest`)
or as manual dev snapshots. Development commits publish nothing.

### API Endpoints

| Endpoint | Method | Description |
|---|---|---|
| `/health` | GET | Server status, agent connection state |
| `/v1/chat` | POST | Send message, SSE stream response |
| `/v1/chat/async` | POST | Send message, return task ID |
| `/v1/threads` | GET | List conversation threads |
| `/v1/threads/{id}` | GET | Thread messages and metadata |
| `/v1/files` | GET | Search workspace files (direct act) |
| `/v1/files/mention` | GET | File mention for prompt injection |
| `/v1/symbols` | GET | Symbol search (direct act) |
| `/v1/rules` | GET | Project rules (direct act) |
| `/v1/fetch` | GET | Fetch a public URL (direct act, SSRF-guarded) |
| `/v1/git/status` | GET | Git working tree status |
| `/v1/git/diff` | GET | Git diff (unstaged/staged) |
| `/v1/git/log` | GET | Recent commit history |
| `/v1/cancel` | POST | Cancel a turn; optional body `{ agent?, request_id? }` scopes the cancel (no body cancels the default agent's whole turn) |

### API Authentication

Every endpoint except `/health` requires a bearer token sent as
`Authorization: Bearer <token>`. The server resolves the effective token
in this order: the `--api-token` CLI argument, the `ACTUS_API_TOKEN`
environment variable, the persisted token file `~/.actus/api_token`, or a
freshly generated token. The effective token is written to
`~/.actus/api_token` (mode 0600) at startup, so the CLI and shell
consumers read the same value the server enforces. Authentication is
enabled by default and has no disable switch yet; `/health` stays open so
readiness probes work before a token exists.

Browser access is governed separately. Pass `--cors-origins` with a
comma-separated origin list to allow cross-origin requests from those
origins only. The default (empty) sends no CORS headers, so browsers
enforce same-origin policy.

Consumers on the same machine share the token file: when a second server
instance starts, it overwrites `~/.actus/api_token`, so file-based
consumers may then hold a token valid only for the later instance. The
runtime targets a single local instance.

## Project Structure

```
actus/
├── src/
│   ├── main.rs          Server entry point
│   ├── agent/           Execution fabric: AgentKind, AgentBackend, AgentRegistry, native reference adapter
│   ├── server.rs        REST API routes and handlers
│   ├── files.rs         File search and mention (direct act)
│   ├── git.rs           Git operations (direct act)
│   └── telos/
│       ├── mod.rs       Telos lifecycle and session management
│       ├── backend.rs   TelosBackend adapter (AgentBackend impl)
│       ├── control.rs   WebSocket bridge and event dispatch
│       └── types.rs     Protocol type definitions
├── runner.py            Server launcher (build + run)
├── terminal.py          Interactive chat CLI
├── run.sh               Gateway: build, test, launch
├── Dockerfile           Multi-stage: toolchain, release, test, runtime targets
└── .github/workflows/
    ├── ci.yml           Test workflow (test target)
    └── publish-actus-image.yml  Publish runtime image on version tags or manual dispatch
```

## Relationship to neXus

| Layer | neXus | actus |
|---|---|---|
| Domain | Knowledge, state, storage | Execution: acts and agents |
| Primitives | Fact, Intent, Hint | Act, Agent, Thread |
| Interface | FIH Blackboard API | REST + WebSocket |
| Role | Accumulate verified knowledge | Execute actions from knowledge |

Actus agents read knowledge from neXus to make decisions and write
execution results back as Facts, forming a stigmergic loop between
knowledge and action.

## License

Apache-2.0 (see `LICENSE`). Third-party licenses are reported via
cargo-about; see NOTICE.md. Actus is the execution fabric; it launches
and talks to agent processes over a contract. Agent binaries are separate
projects with their own licenses (Telos is GPL-3.0-or-later) and are not
packaged with actus.
