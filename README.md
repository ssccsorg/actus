# Actus — Agent Execution Runtime

Actus is an agent execution runtime that manages the lifecycle,
communication, and coordination of autonomous agents operating across
a shared knowledge space.

If neXus is the knowledge fabric (FIH blackboard, state space, storage),
actus is the execution fabric — the runtime that spawns agents, routes
messages, persists threads, and exposes a uniform HTTP API regardless of
which agent type is underneath.

## Architecture

```
External Client (CLI / HTTP)
        │
        ▼
  ┌─────────────────────────────────────┐
  │         Actus Server (REST API)      │
  │  ┌─────────┐  ┌──────────┐          │
  │  │ Session  │  │  Agent   │          │
  │  │ Manager  │  │  Bridge  │          │
  │  └─────────┘  └────┬─────┘          │
  │  ┌─────────┐  ┌────┴─────┐          │
  │  │ File +  │  │ Workspace│          │
  │  │  Git    │  │ Context  │          │
  │  └─────────┘  └──────────┘          │
  └───────────────┬─────────────────────┘
                  │
        ┌─────────┴─────────┐
        ▼                   ▼
  ┌──────────┐      ┌──────────────┐
  │ Telos    │      │   Future     │
  │ (Coding) │      │  Agent Types │
  │          │      │ (Research,   │
  │          │      │  Review,     │
  │          │      │  Deploy...)  │
  └──────────┘      └──────────────┘
```

Telos is the first default agent type — a general-purpose coding
agent with file-system and git awareness. The architecture is designed to
accept any agent that communicates via WebSocket, making actus a universal
gateway for agent execution.

## Agent Execution Fabric

Like neXus weaves heterogeneous FIH storage types behind one thin knowledge
fabric, actus weaves heterogeneous agent platforms behind one thin execution
fabric. Any platform can be orchestrated through the same actus surface;
Telos is the default agent.

- `agent::AgentKind` — platform kinds (`telos`, `langgraph`, `native`),
  extensible by adding a kind and an adapter.
- `agent::AgentBackend` — uniform async trait (`status`, `submit`, `cancel`,
  `thread`, `threads`, `subscribe`) implemented by every platform adapter.
- `agent::AgentRegistry` — name to running adapter map with a default agent.
- `telos::backend::TelosBackend` — first adapter, wrapping `TelosManager`.
  ACP-over-WebSocket details (reconnect, event dispatch) stay inside
  `telos::control`; the adapter owns thread state and command submission.

HTTP handlers talk only to the `AgentBackend` trait, so a new platform
(LangGraph Server over REST/SSE, an in-process Rust agent) plugs in by
implementing the trait and registering it. `/v1/health` reports per-agent
status in the `agents` map.

## Configuration

Agents are declared in `~/.actus/config.toml` (or `ACTUS_CONFIG`). When
the file is absent, a single default `telos` agent is derived from the CLI
flags and environment (`LLM_API_KEY`, `LLM_PROVIDER`, `LLM_BASE_URL`,
`LLM_MODEL`).

```toml
[[agents]]
name = "telos"             # default agent; routed when no agent is named
kind = "telos"             # telos | langgraph | native (only telos has an adapter yet)
provider = "deepseek"
model = "deepseek-chat"
base_url = "https://api.deepseek.com/v1"
api_key = "sk-..."
bin = "../telos/target/telos-release/tel"
ws_port = 8080
tool_approval = "always"  # always | ask | never (drives the fork's approval policy)

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
bridge; `never` rejects them. The mode is carried to the fork via the
`TELOS_TOOL_APPROVAL` environment variable.

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

| Agent | Role | Protocol |
|---|---|---|
| Telos | Code generation, editing, file operations | ACP over WebSocket |
| (future) Research Agent | Literature search, experiment design | TBD |
| (future) Review Agent | Code review, compliance checking | TBD |
| (future) Deploy Agent | CI/CD, infrastructure management | TBD |

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

# Run full test suite (static checks + HTTP smoke tests)
./run.sh --test

# Start server only (background)
./run.sh --server-only

# Connect CLI to existing server
./run.sh --cli
```

### Docker

```bash
# Build base image (actus binary only)
docker build .

# Build full integration image (includes Telos bootstrapping)
docker build --target full .
```

### API Endpoints

| Endpoint | Method | Description |
|---|---|---|
| `/health` | GET | Server status, agent connection state |
| `/v1/chat` | POST | Send message, SSE stream response |
| `/v1/chat/async` | POST | Send message, return task ID |
| `/v1/threads` | GET | List conversation threads |
| `/v1/threads/{id}` | GET | Thread messages and metadata |
| `/v1/files` | GET | Search workspace files |
| `/v1/files/mention` | GET | File mention for prompt injection |
| `/v1/git/status` | GET | Git working tree status |
| `/v1/git/diff` | GET | Git diff (unstaged/staged) |
| `/v1/git/log` | GET | Recent commit history |
| `/v1/cancel` | POST | Cancel current agent turn |

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
│   ├── agent/           Execution fabric: AgentKind, AgentBackend, AgentRegistry
│   ├── server.rs        REST API routes and handlers
│   ├── files.rs         File search and mention
│   ├── git.rs           Git operations
│   └── telos/
│       ├── mod.rs       Telos lifecycle and session management
│       ├── backend.rs   TelosBackend adapter (AgentBackend impl)
│       ├── control.rs   WebSocket bridge and event dispatch
│       └── types.rs     Protocol type definitions
├── runner.py            Server launcher (build + run)
├── terminal.py          Interactive chat CLI
├── run.sh               Gateway: build, test, launch
├── Dockerfile           Multi-stage image builder
└── .github/workflows/
    ├── ci.yml           Test workflow (base target)
    └── publish-actus-image.yml  Publish to GHCR
```

## Relationship to neXus

| Layer | neXus | actus |
|---|---|---|
| Domain | Knowledge, state, storage | Agent execution, lifecycle |
| Primitives | Fact, Intent, Hint | Agent, Thread, Task |
| Interface | FIH Blackboard API | REST + WebSocket |
| Role | Accumulate verified knowledge | Execute actions from knowledge |

Actus agents read knowledge from neXus to make decisions and write
execution results back as Facts, forming a stigmergic loop between
knowledge and action.

## License

Apache-2.0 (see `LICENSE`). Actus is the execution fabric; it
launches and talks to agent processes over WebSocket. Agent binaries are
separate projects with their own licenses and are not packaged with
actus.
