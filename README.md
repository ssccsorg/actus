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
│   ├── server.rs        REST API routes and handlers
│   ├── files.rs         File search and mention
│   ├── git.rs           Git operations
│   └── zed/
│       ├── mod.rs       Zed lifecycle and session management
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
