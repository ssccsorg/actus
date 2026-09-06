# Auxiliary Agent Profiles and Integration Review

## Purpose

This devlog records the review of the issue #15 implementation now on the
default branch: the generalized `ext_cli` agent kind that attaches any raw
CLI binary as an auxiliary agent behind the fabric. It fixes the profile
contract as shipped, lists the gaps found in the review with file anchors,
and records the integration surface of AURA as the second specialized
agent candidate after Ante. It is the reference for attaching a new
raw-CLI agent to actus without code changes.

The LLM-layer boundary from issue #16 applies: actus owns execution and
routing, agents own their LLM. A raw-CLI agent profile only declares how
to spawn the binary and where the prompt goes; credentials and model
settings live inside the agent's own configuration or environment.

## Shipped Implementation

Four commits under issue #15 landed on the default branch:

- add the `ext_cli` agent kind for any external CLI binary
- make `ext_cli` a declarative one-shot profile
- advertise agent capabilities and add terminal agent selection
- resolve `cli_env` from the server env and export `.env` in `run.sh`

The Ante-specific kind from the issue draft was replaced by a generic
`ext_cli` kind plus a declarative profile. Attaching a new CLI therefore
means adding one `[[agents]]` config entry, not code.

### Profile Contract

Fields on an `[[agents]]` entry with `kind = "ext_cli"`:

| Field | Meaning | Default |
|---|---|---|
| `bin` | binary path or PATH name | none, required |
| `cli_args` | fixed arguments; a `{prompt}` marker is replaced by the message, without a marker the message is appended as the final argument | empty |
| `cli_prompt` | `arg` passes the prompt as an argument, `stdin` writes it to the child's stdin | `arg` |
| `cli_env` | extra environment for the child; a value of the form `$NAME` is replaced by the server process env var NAME | empty |
| `cli_timeout_secs` | per-turn timeout; the child is killed on expiry | `300` |
| `tool_approval` | accepted for schema uniformity; raw-CLI agents have no approval surface | `always` |

Behavioral contract per turn: actus spawns one child process in the server
workdir with the server environment inherited plus `cli_env`. Stdout is
the assistant reply, recorded up to a 200,000 character cap. A non-zero
exit records the exit code plus trimmed stderr as the reply; an empty
successful stdout records a fixed notice; a timeout kills the child and
records the timeout. Threads live in memory only. Turns on different
threads run in parallel. See `src/agent/ext_cli.rs` and
`src/agent/config.rs`.

### Capabilities

`AgentKind::capabilities()` in `src/agent/mod.rs` classifies a raw-CLI
agent as `sessionful: false, streaming: false, tools: false, approval:
false, parallel: true, transport: "cli"`. The health endpoint exposes the
list, and the terminal shows it through `/agents`. Upper layers can
distinguish a one-shot act from a sessionful agent instead of assuming
every agent is a full session.

## Review Findings

### Strengths

- Generic profile instead of a per-agent kind: Ante, AURA, or any future
  single-shot CLI attaches by config alone.
- Failure diagnostics are recorded in the thread: exit code, stderr, and
  timeout are distinguishable in the reply text.
- `$NAME` env mapping keeps secrets out of `config.toml`; `run.sh` exports
  `.env` so the server environment carries the declared values.
- Cancellation and timeout kill the child; pipes are drained concurrently
  so a chatty child cannot deadlock the wait.

### Gaps

- Server workdir is global. `ExtCliAgent` spawns in the canonicalized
  server workdir and there is no per-agent `workdir` field. Agents that
  resolve project scope from the current directory (AURA permission and
  `cli.toml` walk-up, Ante workspace detection) need a per-agent working
  directory to behave correctly.
- `AgentBackend::cancel` is agent-wide. On a parallel raw-CLI agent one
  cancel kills every running child; the trait has no request or thread
  scope. In addition the running-children map is keyed by thread id, so a
  second submit on the same thread can overwrite the handle of a still
  starting first turn, a small race window on the same thread.
- Readiness is `bin.exists()` only. Permission, launch, and argument
  problems surface on the first submit, not at registration.
- Raw-CLI threads are in-memory. A server restart drops auxiliary
  threads, unlike the per-agent persisted threads of telos agents.
- Config-level coverage is thin. The adapter tests construct
  `ExtCliAgent` directly; no test parses the `cli_*` TOML fields, and the
  scenario suite has no raw-CLI case from config to `/v1/chat`.
- The default agent is the first entry in the config array. Placing an
  auxiliary agent first silently changes the default routing target.
- Whole-value `$NAME` env substitution only. A missing variable resolves
  to an empty string without a warning, and combined forms such as
  `$A/suffix` pass through literally.

## Ante Profile

The first candidate is Ante over its raw headless transport. The profile
mirrors the integration test in `tests/ext_cli_test.rs`:

```toml
[[agents]]
name = "ante"
kind = "ext_cli"
bin = "ante"
cli_args = ["-p", "{prompt}"]
```

Ante does not expose ACP yet. When `ante serve` (JSONL) or ACP lands, the
transport decision is revisited; a long-lived protocol needs a new
adapter kind, not an `ext_cli` profile.

## AURA Integration Surface

Facts below are read from the AURA clone on this machine at commit time
(crate `aura-cli`, binary `aura`; `crates/aura-cli/src/cli.rs` and
`oneshot.rs`). The upstream repository stays authoritative.

AURA is a TOML-configured agent runtime. The same `aura` binary serves
two topologies:

- Standalone one-shot: `aura --config <agent.toml> --query "<prompt>"`
  builds the agent in-process from the TOML config and exits after one
  turn. Stdout carries only the raw assistant reply; exit code zero means
  the reply is complete, a non-zero exit means an error explained on
  stderr. Logging goes to stderr and optionally to `--log-file`.
- Web server: `aura webserver` runs a long-lived OpenAI-compatible
  endpoint with its own host and port arguments. This topology is
  operator-managed and speaks the LLM API protocol, so it stays outside
  the actus execution surface per the issue #16 boundary. A telos agent
  could point its LLM environment at such an endpoint as a pure
  operator-side LLM choice.

Relevant environment: `AURA_API_URL`, `AURA_API_KEY`, `AURA_MODEL`,
`AURA_CONFIG`, `AURA_LOG_FILE`. Standalone mode is the default when no
API URL is given; `--config` selects the agent TOML. API keys and model
settings live in the agent TOML, so actus does not carry them.

Client-side tools are opt-in on both sides: the CLI flag
`--enable-client-tools` plus `[agent].enable_client_tools = true` in the
agent TOML. Tool permission rules resolve from `.aura/permissions.json`
and per-project `cli.toml` by walking up from the current directory,
which makes the per-agent workdir gap above directly relevant to AURA.

Draft profile, subject to verification against a built AURA binary:

```toml
[[agents]]
name = "aura"
kind = "ext_cli"
bin = "aura"
cli_args = ["--config", "/absolute/path/to/agent.toml", "--query", "{prompt}"]
cli_timeout_secs = 600
```

The agent TOML path is absolute because child working directories are
currently fixed to the server workdir. The timeout is a starting value;
agent turns routinely exceed the default 300 seconds.

## Remaining Work

- Per-agent `workdir` field on `AgentSpec`, passed to every adapter
  (blocker for cwd-sensitive raw-CLI agents such as AURA with client
  tools).
- Request-scoped cancellation and a running-children map keyed by request
  id instead of thread id.
- Spawn preflight at registration: probe the binary so permission and
  argument errors surface in the health status, not on first submit.
- Decision on raw-CLI thread persistence across server restarts.
- TOML-level parse tests for the `cli_*` fields and a config-driven
  scenario from config to `/v1/chat` with a stub CLI.
- Documentation of the default-agent rule (first array entry) and the
  whole-value `$NAME` env rule, including the empty-missing-var behavior.
- Live trials with real Ante and AURA binaries once available; verify the
  draft profiles above.

## Contract Pins

- `src/agent/ext_cli.rs`: spawn, capture, timeout, and cancellation
  behavior.
- `src/agent/config.rs`: `cli_args`, `cli_env`, `cli_prompt`,
  `cli_timeout_secs` parsing and defaults.
- `src/agent/mod.rs`: `AgentKind::capabilities`.
- `tests/ext_cli_test.rs`: marker, append, stdin, parallel, env, failure,
  and missing-binary adapter tests.
