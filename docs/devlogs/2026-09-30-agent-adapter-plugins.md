# Agent Adapters as Registered Plugins

## Purpose

This devlog records issue #36: the agent kind stops being a closed enum in
the fabric's types, and telos stops being the core's special case. A kind is
now a name a declaration resolves against a registry of factories, and a
platform's options, launch path, and lifecycle live in its own module. It is
the reference for attaching a new agent platform to actus.

## What the seam is

- `AgentSpec` (`src/agent/config.rs`) is what the fabric understands:
  `name`, `kind`, `workdir`, and the rest of the entry as an options table.
  `AgentSpec::options::<T>()` reads that table into one adapter's typed
  options; keys the adapter does not declare are ignored, so several
  adapters can read the same table.
- `AgentFactory` (`src/agent/adapter.rs`): `kind`, `validate(spec, all)`,
  `launch(spec, ctx)`. `all` carries every spec, so a constraint across one
  kind's agents (the telos WebSocket-port collision check) is the factory's.
- `FactoryRegistry` maps kind names to factories, sorted by name, and
  `load_config` resolves every spec against it. An unknown kind is refused
  before anything launches, and the error names the registered kinds.
- `LaunchContext` carries the fabric's own state into a launch: the server
  workdir, the threads root, the HTTP port, and the API token.
- `LaunchedAgent` is what a launch returns: the `AgentBackend`, and the
  child processes actus kills when it exits.
- `AgentBackend` gained `capabilities()` (the backend declares its own) and
  a `shutdown()` hook. `AgentStatus.kind` carries the registered kind name.

## What moved where

| Was | Is |
|---|---|
| `AgentKind` enum, `ALL`, `parse`, `capabilities()` match | Deleted. Kinds are registered names; capabilities come from the backend. |
| `AgentSpec` fields `bin`, `ws_port`, `tool_approval`, `provider`, `model`, `model_display`, `base_url`, `api_key`, `reasoning_effort`, `mcp`, `cli_*` | Each adapter's own options type: `telos::options::TelosOptions`, `ext_cli::ExtCliOptions`. |
| `AgentDefaults`, `resolve_llm_settings`, reasoning-effort constants | `telos::options::TelosDefaults` and the LLM helpers beside it. The launcher (`main.rs`) resolves them from the CLI and environment. |
| `main.rs` `match spec.kind` launch arms | `TelosFactory::launch`, `ExtCliFactory::launch`, `NativeFactory::launch`. `main.rs` registers the factories and runs one generic loop. |
| Telos WebSocket server, settings bootstrap, thread saver, child process, reconnect monitor, shutdown flush in `main.rs` | `telos::adapter::TelosFactory`, `TelosManager::spawn_health_monitor`, `TelosBackend::shutdown`. |
| Per-agent `tempfile::TempDir` kept alive in `main.rs` | Held by `TelosBackend` (`new(...)` takes it), which is what reads the settings it holds. |
| `AgentKind`-based port check in the config loader | `TelosFactory::validate`, which reads telos specs and only telos specs. |
| `server.rs` re-export of `telos::WsCommandTx` | Deleted; `telos::control` imports the type from `telos`. |

The `langgraph` kind disappears with the enum. It is kept as a reserved kind
(`FactoryRegistry::reserve` in `main.rs`): a config that declares it still
loads, and the launch loop skips that agent with the warning actus emitted
when the kind was an enum variant, so an existing config keeps the outcome it
had. A kind that was never declared is refused at config load instead, which
is what a typo needs. Registering a `langgraph` factory later takes
precedence over the reservation.

## Configuration

The file format is unchanged. The flat fields are what a factory parses out
of its options table, so an existing `config.toml` stays valid. Two rules
are worth stating because they are now the loader's:

- A spec that omits `kind` resolves to the registry's default kind, which
  `main.rs` sets to `telos` with `register_default`.
- A config-less launch yields one agent of the default kind with an empty
  options table; the factory fills everything from its defaults.

## Health

`/health` still answers `telos_connected`, which is the default agent's
transport state under its historical name. The kletos client pins the name
(`kletos/client/src/api.rs`), so the rename travels with that client.

## Tests

- `tests/adapter_test.rs`: a factory and a backend declared in the test
  crate launch through the generic path; a factory validates its own options
  and names the agent; an unregistered kind is refused and the registered
  ones are named.
- `tests/config_test.rs`: the loader's own rules (default agent, default
  kind, duplicate names, empty and malformed files, the control policy) and
  the options table reaching the factory unchanged.
- `tests/telos_options_test.rs`: telos options, defaults inheritance, the
  reasoning-effort validation, the port collision, and the LLM endpoint
  resolution.
- `tests/settings_test.rs` and `tests/mcp_test.rs` construct
  `TelosSettings` (or resolve a config through `TelosFactory`) instead of
  reading fields off `AgentSpec`.

## Contract Pins

- `src/agent/adapter.rs`: the seam (`AgentFactory`, `FactoryRegistry`,
  `LaunchContext`, `LaunchedAgent`).
- `src/agent/config.rs`: `AgentSpec`, `load_config`, the control policy.
- `src/agent/mod.rs`: `AgentBackend` (including `capabilities` and
  `shutdown`), `AgentRegistry`.
- `src/telos/adapter.rs`, `src/telos/options.rs`: the telos kind.
- `src/main.rs`: the composition point and the default kind.
- `tests/adapter_test.rs`: the seam exercised from outside the fabric.

## Remaining Work

- Rename `/health`'s `telos_connected` together with the kletos client.
- A browser agent attaches over this seam (#37).
- kineTics executor kinds (transductive, materialization, HMI) are a
  separate family; the fabric's act taxonomy is where they attach, not the
  agent registry.
