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
  │  Zed     │      │   Future     │
  │ Headless │      │  Agent Types │
  │ (Coding) │      │ (Research,   │
  │          │      │  Review,     │
  │          │      │  Deploy...)  │
  └──────────┘      └──────────────┘
```

Zed headless is the first default agent type — a general-purpose coding
agent with file-system and git awareness. The architecture is designed to
accept any agent that communicates via WebSocket, making actus a universal
gateway for agent execution.

## Agent Execution Fabric

Like neXus weaves heterogeneous FIH storage types behind one thin knowledge
fabric, actus weaves heterogeneous agent platforms behind one thin execution
fabric. Any platform can be orchestrated through the same actus surface;
Zed headless is the default agent.

- `agent::AgentKind` — platform kinds (`zed`, `langgraph`, `native`),
  extensible by adding a kind and an adapter.
- `agent::AgentBackend` — uniform async trait (`status`, `submit`, `cancel`,
  `thread`, `threads`, `subscribe`) implemented by every platform adapter.
- `agent::AgentRegistry` — name to running adapter map with a default agent.
- `zed::backend::ZedBackend` — first adapter, wrapping `ZedManager`.
  ACP-over-WebSocket details (reconnect, event dispatch) stay inside
  `zed::control`; the adapter owns thread state and command submission.

HTTP handlers talk only to the `AgentBackend` trait, so a new platform
(LangGraph Server over REST/SSE, an in-process Rust agent) plugs in by
implementing the trait and registering it. `/v1/health` reports per-agent
status in the `agents` map.

## Configuration

Agents are declared in `~/.actus/config.toml` (or `ACTUS_CONFIG`). When
the file is absent, a single default `zed` agent is derived from the CLI
flags and environment (`LLM_API_KEY`, `LLM_PROVIDER`, `LLM_BASE_URL`,
`LLM_MODEL`).

```toml
[[agents]]
name = "zed"             # default agent; routed when no agent is named
kind = "zed"             # zed | langgraph | native (only zed has an adapter yet)
provider = "deepseek"
model = "deepseek-chat"
base_url = "https://api.deepseek.com/v1"
api_key = "sk-..."
bin = "helix/.bin/helix-zed-headless-arm64"
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
kind = "zed"
provider = "anthropic"
model = "claude-sonnet-4"
ws_port = 8081
```

Each agent inherits any omitted field from the defaults. Every `zed`
agent needs a unique `ws_port`; thread state is persisted per agent under
`~/.actus/threads/{name}/`. Chat and thread endpoints accept an `agent`
field to route to a specific agent. MCP servers declared under an agent
are injected into the agent's `context_servers` settings and started by
the headless agent, exposing their tools to the model.

`tool_approval` sets the tool call approval policy. `always` auto-approves
tool calls (headless task execution); `ask` waits for a human or approval
bridge; `never` rejects them. The mode is carried to the fork via the
`ZED_TOOL_APPROVAL` environment variable.

## `@` Mention Context

Typing `@` in the CLI injects context into the message before it is sent,
mirroring Zed's mention picker:

| Form | Source | Example |
|---|---|---|
| `@path/to/file` | file search, paths injected | `@src/server.rs` |
| `@rules` | project rule files (AGENTS.md, *.mdc) | `@rules` |
| `@symbol:query` | definition-pattern symbol search | `@symbol:search_symbols` |
| `@thread:query` | conversation thread content | `@thread:thread-title` |
| `@fetch:URL` | fetched URL text | `@fetch:https://example.com` |

Server endpoints: `/v1/symbols?q=`, `/v1/rules`, `/v1/fetch?url=`.
Diagnostics mention is deferred (requires a language server).
When a mention matches several candidates, the CLI opens a numbered
picker (files and symbols grouped, `a` for all, `0` to skip).

## Agent Types

| Agent | Role | Protocol |
|---|---|---|
| Zed Headless | Code generation, editing, file operations | ACP over WebSocket |
| (future) Research Agent | Literature search, experiment design | TBD |
| (future) Review Agent | Code review, compliance checking | TBD |
| (future) Deploy Agent | CI/CD, infrastructure management | TBD |

## Getting Started

### Prerequisites

- Rust toolchain
- Python 3.12+
- A pre-built headless Zed binary (or build with `helix/build.sh`)

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

# Build full integration image (includes Zed bootstrapping)
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

## Project Structure

```
actus/
├── src/
│   ├── main.rs          Server entry point
│   ├── agent/           Execution fabric: AgentKind, AgentBackend, AgentRegistry
│   ├── server.rs        REST API routes and handlers
│   ├── files.rs         File search and mention
│   ├── git.rs           Git operations
│   └── zed/
│       ├── mod.rs       Zed lifecycle and session management
│       ├── backend.rs   ZedBackend adapter (AgentBackend impl)
│       ├── control.rs   WebSocket bridge and event dispatch
│       └── types.rs     Protocol type definitions
├── helix/
│   ├── build.sh         Clone → patch → build Zed headless
│   ├── patch/           Patches for Helix Zed fork
│   └── subtree/         Helix Zed fork (git subtree)
├── runner.py            Server launcher (build + run)
├── terminal.py          Interactive chat CLI
├── actus-server.py      Python reference server
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

BUSL-1.1
