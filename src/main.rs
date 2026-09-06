// Actus — agent execution runtime (REST API server)
//
// Launches and supervises agent adapters (Telos over WebSocket, the
// in-process native reference adapter, future platforms) and exposes a
// REST API for multi-thread chat with an async task queue.
//
// Usage:
//    --workdir /path/to/project

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

use actus::agent::config::{load_config, AgentDefaults};
use actus::agent::ext_cli::ExtCliAgent;
use actus::agent::native::NativeAgent;
use actus::agent::AgentKind;
use actus::agent::AgentRegistry;
use actus::server::{run_http_server, AppState};
use actus::telos::backend::TelosBackend;
use actus::telos::control::run_ws_server;
use actus::telos::{ensure_telos_settings, launch_telos, TelosManager, WsCommandTx};

#[derive(clap::Parser, Debug, Clone)]
#[command(name = "", version, about = "Actus agent runtime server")]
struct Args {
    /// Telos binary path
    #[arg(long)]
    bin: Option<PathBuf>,

    /// Working directory for agents
    #[arg(long, default_value = ".")]
    workdir: PathBuf,

    /// HTTP API port
    #[arg(long, default_value = "9090")]
    http_port: u16,

    /// WebSocket port a spawned agent process connects back to
    #[arg(long, default_value = "8080")]
    ws_port: u16,

    /// LLM API key (default: LLM_API_KEY env var)
    #[arg(long)]
    api_key: Option<String>,

    /// Provider label for the OpenAI-compatible API
    /// (default: LLM_PROVIDER env or "openai-compatible")
    #[arg(long, default_value = "openai-compatible")]
    provider: String,

    /// API base URL of the OpenAI-compatible endpoint (default: LLM_BASE_URL env)
    #[arg(long)]
    base_url: Option<String>,

    /// Path to terminal.py (auto-detected if not set)
    #[arg(long)]
    cli: Option<PathBuf>,

    /// Server-only mode: don't auto-start CLI
    #[arg(long, default_value_t = false)]
    server_only: bool,

    /// Bearer token required by the HTTP API. Falls back to the
    /// ACTUS_API_TOKEN env var, then to ~/.actus/api_token, then to a
    /// freshly generated token persisted to that file.
    #[arg(long)]
    api_token: Option<String>,

    /// Comma-separated list of CORS origins allowed to call the HTTP API
    /// from a browser. Empty (default) sends no CORS headers, so browsers
    /// enforce same-origin and cross-origin reads are blocked.
    #[arg(long, default_value = "")]
    cors_origins: String,
}

/// Location of the persisted API token shared with the CLI and scripts.
fn api_token_file() -> PathBuf {
    dirs::home_dir()
        .map(|h| h.join(".actus").join("api_token"))
        .unwrap_or_else(|| PathBuf::from(".actus/api_token"))
}

/// Write the effective token to ~/.actus/api_token with mode 0600 so the
/// CLI, run.sh, and curl can read the same value the server enforces.
fn persist_api_token(token: &str) -> anyhow::Result<()> {
    let file = api_token_file();
    if let Some(parent) = file.parent() {
        std::fs::create_dir_all(parent)?;
    }
    use std::os::unix::fs::OpenOptionsExt;
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create(true).truncate(true).mode(0o600);
    let mut f = opts.open(&file)?;
    std::io::Write::write_all(&mut f, token.as_bytes())?;
    Ok(())
}

/// Resolve the HTTP API bearer token: CLI arg, then ACTUS_API_TOKEN env,
/// then the persisted token file, then a freshly generated token. The
/// effective token is persisted so every consumer reads the same value.
fn resolve_api_token(arg: Option<String>) -> anyhow::Result<String> {
    if let Some(t) = arg {
        let t = t.trim().to_string();
        if !t.is_empty() {
            persist_api_token(&t)?;
            return Ok(t);
        }
    }
    if let Ok(t) = std::env::var("ACTUS_API_TOKEN") {
        let t = t.trim().to_string();
        if !t.is_empty() {
            persist_api_token(&t)?;
            return Ok(t);
        }
    }
    if let Ok(t) = std::fs::read_to_string(api_token_file()) {
        let t = t.trim().to_string();
        if !t.is_empty() {
            return Ok(t);
        }
    }
    let token = format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    );
    persist_api_token(&token)?;
    tracing::warn!(
        "Generated API token starting {}...; full value written to {}",
        &token[..4],
        api_token_file().display()
    );
    Ok(token)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Args = clap::Parser::parse();

    // Init logging — always to stderr. The filter falls back to
    // `actus=info` when RUST_LOG is unset or invalid.
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("actus=info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();

    // Environment values from a `.env` file may carry surrounding quotes
    // (`LLM_MODEL="example-model"`). Strip them defensively here so a
    // quoted value never leaks into the agent's settings.json or
    // credentials, where a `"example-model"` model name or quoted key
    // fails the lookup and aborts every turn.
    let unquote = |s: &str| -> String {
        let t = s.trim();
        if t.len() >= 2
            && ((t.starts_with('"') && t.ends_with('"'))
                || (t.starts_with('\'') && t.ends_with('\'')))
        {
            t[1..t.len() - 1].to_string()
        } else {
            t.to_string()
        }
    };

    // Resolve the LLM API key. Optional at the fabric level; the Telos
    // adapter requires one when it launches an agent.
    let api_key = args
        .api_key
        .or_else(|| std::env::var("LLM_API_KEY").ok())
        .map(|k| unquote(&k));

    // Resolve the Telos binary path: --bin, TELOS_BIN, or the sibling
    // build. Used by the Telos adapter only; existence is checked at
    // launch so non-Telos agents do not require it.
    let bin_path = if let Some(p) = args.bin {
        p
    } else if let Ok(p) = std::env::var("TELOS_BIN") {
        PathBuf::from(p)
    } else {
        PathBuf::from("../telos/target/telos-release/tel")
    };

    let workdir = std::fs::canonicalize(&args.workdir)?;

    // The HTTP API requires a bearer token. Resolution order: CLI arg,
    // ACTUS_API_TOKEN env, the persisted token file, then a freshly
    // generated token. The effective token is always persisted to
    // ~/.actus/api_token (mode 0600) so the CLI, run.sh, and curl can
    // read the same value.
    let api_token = resolve_api_token(args.api_token.clone())?;
    let cors_origins: Vec<String> = args
        .cors_origins
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    // Resolve the OpenAI-compatible endpoint from args or the local
    // environment. Actus ships no LLM-specific provider, model name, or
    // API host: the operator supplies them through LLM_PROVIDER,
    // LLM_MODEL, and LLM_BASE_URL (or the agent's config.toml entry).
    let provider = unquote(&std::env::var("LLM_PROVIDER").unwrap_or(args.provider));
    let base_url = std::env::var("LLM_BASE_URL")
        .ok()
        .map(|s| unquote(&s))
        .or_else(|| args.base_url.as_deref().map(|s| unquote(s)))
        .unwrap_or_default();
    let model_name = unquote(&std::env::var("LLM_MODEL").unwrap_or_default());
    let model_display =
        unquote(&std::env::var("LLM_MODEL_DISPLAY").unwrap_or_else(|_| model_name.clone()));

    // Resolve agent config: ACTUS_CONFIG overrides ~/.actus/config.toml.
    // Missing file (or no override) means a single default telos agent.
    let config_path = std::env::var("ACTUS_CONFIG")
        .ok()
        .map(PathBuf::from)
        .or_else(|| dirs::home_dir().map(|h| h.join(".actus").join("config.toml")));
    let config_file = match config_path {
        Some(p) if p.exists() => Some(p),
        _ => None,
    };

    let defaults = AgentDefaults {
        provider: provider.clone(),
        model: model_name.clone(),
        model_display: model_display.clone(),
        base_url: base_url.clone(),
        api_key: api_key.clone(),
        bin: bin_path.clone(),
        ws_port: args.ws_port,
    };
    let specs = load_config(config_file.as_deref(), &defaults).map_err(anyhow::Error::msg)?;
    tracing::info!(
        "Config: {} agent(s) from {}",
        specs.len(),
        config_file
            .as_ref()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| "defaults".to_string())
    );

    // Threads root: ~/.actus/threads/{agent_name}/
    let threads_root = dirs::home_dir()
        .ok_or_else(|| anyhow::anyhow!("Cannot determine home directory"))?
        .join(".actus")
        .join("threads");
    std::fs::create_dir_all(&threads_root)?;

    // Launch every configured agent and register it in the fabric.
    let mut registry = AgentRegistry::new();
    let mut children: Vec<std::process::Child> = Vec::new();
    // (manager, ws_tx) pairs drive the shutdown handler and health monitor.
    let mut monitors: Vec<(Arc<RwLock<TelosManager>>, WsCommandTx)> = Vec::new();
    // Per-agent user data dirs stay alive for the process lifetime; Telos
    // reads settings at startup and watches them while running.
    let mut _user_data_dirs: Vec<tempfile::TempDir> = Vec::new();

    // The first configured agent is the fabric default.
    let default_name = specs[0].name.clone();

    for spec in &specs {
        match spec.kind {
            AgentKind::Telos => {
                let api_key = spec.api_key.as_deref().ok_or_else(|| {
                    anyhow::anyhow!(
                        "agent '{}': LLM API key required (LLM_API_KEY or --api-key)",
                        spec.name
                    )
                })?;
                if !spec.bin.exists() {
                    return Err(anyhow::anyhow!(
                        "agent '{}': telos binary not found at {}",
                        spec.name,
                        spec.bin.display()
                    ));
                }
                let ws_host = format!("127.0.0.1:{}", spec.ws_port);
                let user_data_dir = tempfile::tempdir()?;
                ensure_telos_settings(
                    user_data_dir.path(),
                    Some(api_key),
                    &spec.provider,
                    &spec.base_url,
                    &spec.model,
                    &spec.model_display,
                    &spec.mcp,
                )?;
                let threads_dir = threads_root.join(&spec.name);
                std::fs::create_dir_all(&threads_dir)?;
                let session_id = format!(
                    "ses_actus-{}-{}",
                    spec.name,
                    &uuid::Uuid::new_v4().to_string()[..8]
                );
                let manager = Arc::new(RwLock::new(TelosManager::new(
                    session_id.clone(),
                    ws_host.clone(),
                    &threads_dir,
                )));
                let ws_tx: WsCommandTx = Arc::new(tokio::sync::Mutex::new(None));
                monitors.push((manager.clone(), ws_tx.clone()));

                // Per-agent WebSocket server (Telos connects back here).
                tokio::spawn({
                    let host = ws_host.clone();
                    let mgr = manager.clone();
                    let tx = ws_tx.clone();
                    async move {
                        if let Err(e) = run_ws_server(&host, mgr, tx).await {
                            tracing::error!("WS server for agent failed: {}", e);
                        }
                    }
                });
                // Debounced thread persistence (see TelosManager::save_threads).
                TelosManager::spawn_thread_saver(manager.clone());
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;

                let child = launch_telos(
                    &spec.bin,
                    &workdir,
                    user_data_dir.path(),
                    &session_id,
                    &ws_host,
                    spec.tool_approval.as_str(),
                    &threads_dir.join("telos.log"),
                )
                .await?;
                _user_data_dirs.push(user_data_dir);
                tracing::info!(
                    "Agent '{}' launched (PID {:?}, WS ws://{}, threads {})",
                    spec.name,
                    child.id(),
                    ws_host,
                    threads_dir.display()
                );
                children.push(child);

                let backend = Arc::new(TelosBackend {
                    manager: manager.clone(),
                    ws_tx: ws_tx.clone(),
                });
                registry.register(backend, spec.name == default_name);
            }
            AgentKind::Native => {
                let backend = Arc::new(NativeAgent::new(spec.name.clone()));
                registry.register(backend, spec.name == default_name);
                tracing::info!(
                    "Agent '{}' running (native reference adapter, in-process)",
                    spec.name
                );
            }
            AgentKind::ExtCli => {
                // Per-agent working directory overrides the server
                // workdir for cwd-sensitive CLIs; None falls back to the
                // server workdir. The path is canonicalized at launch so
                // a bad entry fails fast with the agent name.
                let agent_workdir = match &spec.workdir {
                    Some(p) => std::fs::canonicalize(p).map_err(|e| {
                        anyhow::anyhow!(
                            "agent '{}': cannot resolve workdir {}: {}",
                            spec.name,
                            p.display(),
                            e
                        )
                    })?,
                    None => workdir.clone(),
                };
                let backend = Arc::new(ExtCliAgent::new(
                    spec.name.clone(),
                    spec.bin.clone(),
                    spec.cli_args.clone(),
                    spec.cli_env.clone(),
                    spec.cli_prompt,
                    spec.cli_timeout_secs,
                    agent_workdir.clone(),
                ));
                registry.register(backend, spec.name == default_name);
                tracing::info!(
                    "Agent '{}' running (ext_cli adapter, raw transport, bin {}, workdir {})",
                    spec.name,
                    spec.bin.display(),
                    agent_workdir.display()
                );
            }
            AgentKind::LangGraph => {
                tracing::warn!(
                    "Agent '{}': kind langgraph has no adapter yet, skipping",
                    spec.name
                );
            }
        }
    }

    if registry.default_agent().is_none() {
        anyhow::bail!("no agent could be launched from config");
    }

    tracing::info!("Starting actus server");
    tracing::info!("  HTTP API:   http://127.0.0.1:{}", args.http_port);
    tracing::info!("  Workdir:    {}", workdir.display());
    tracing::info!("  Threads:    {}", threads_root.display());

    // Build app state and start HTTP server
    let state = Arc::new(AppState::new(
        registry,
        workdir.clone(),
        Some(api_token.clone()),
    ));

    let http_server = tokio::spawn({
        let state = state.clone();
        let addr = format!("127.0.0.1:{}", args.http_port);
        async move { run_http_server(&addr, state, cors_origins).await }
    });

    // Resolve CLI path (terminal.py) — skip if --server-only
    let cli_path = if args.server_only {
        None
    } else {
        args.cli.or_else(|| {
            // Auto-detect relative to binary or CWD
            let candidates = vec![
                std::env::current_exe()
                    .ok()
                    .and_then(|p| p.parent().map(|p| p.join("terminal.py"))),
                Some(PathBuf::from("terminal.py")),
            ];
            candidates.into_iter().flatten().find(|p| p.exists())
        })
    };

    // Wait for the default agent to be ready before starting the CLI.
    let default_backend = state.agents.default_agent();
    for i in 0..30 {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        let ready = match &default_backend {
            Some(agent) => agent.status().await.ready,
            None => false,
        };
        if ready {
            tracing::info!("Agent ready after {}s", i + 1);
            break;
        }
        if i == 29 {
            tracing::warn!("Agent not ready after 30s");
        }
    }

    // ── Graceful shutdown ────────────────────────────────────────────
    // Handle SIGTERM/SIGINT: save threads per agent, exit cleanly.
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    {
        let monitors = monitors.clone();

        tokio::spawn(async move {
            let mut sigterm =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("Failed to register SIGTERM handler");
            let mut sigint =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
                    .expect("Failed to register SIGINT handler");

            tokio::select! {
                _ = sigterm.recv() => {}
                _ = sigint.recv() => {}
            }

            tracing::info!("Shutdown signal received, cleaning up...");

            // Cancel active turns and persist threads for every agent.
            for (mgr, _tx) in &monitors {
                {
                    let g = mgr.read().await;
                    g.cancel_current_turn().ok();
                }
                {
                    let g = mgr.read().await;
                    g.flush_threads();
                }
            }

            let _ = shutdown_tx.send(());
        });
    }

    // ── WebSocket health monitor ─────────────────────────────────────
    // Detects a half-open WebSocket where the read loop would otherwise
    // block forever. Reconnection is forced only when a turn is actively
    // in flight and no events arrive for a long window: LLM responses
    // routinely pause for tens of seconds (thinking, slow providers) and
    // long turns (code review, multi-tool research) pause for minutes, so
    // the timeout must be generous or the monitor kills live turns. The
    // 30-minute ceiling matches the CLI poll and SSE stream.
    {
        let monitors = monitors.clone();

        tokio::spawn(async move {
            tracing::info!("Health monitor started (check every 30s, timeout 1800s)");
            let check_interval = Duration::from_secs(30);
            let timeout = Duration::from_secs(1800);

            loop {
                tokio::time::sleep(check_interval).await;

                for (mgr, ws_tx) in &monitors {
                    let (connected, elapsed, active_turn) = {
                        let g = mgr.read().await;
                        (
                            g.telos_connected,
                            g.last_sse_event_time.elapsed(),
                            !g.pending_chat_queue.is_empty(),
                        )
                    };

                    if connected && active_turn && elapsed > timeout {
                        tracing::warn!(
                            "Health monitor: no events for {}s during an active turn, forcing reconnection",
                            elapsed.as_secs()
                        );
                        // Force reconnection: clear telos_connected and the
                        // shared command channel. The WS read loop's
                        // periodic check breaks, and the connection loop
                        // accepts a new connection (Telos auto-reconnects).
                        {
                            let mut g = mgr.write().await;
                            g.telos_connected = false;
                        }
                        {
                            let mut guard = ws_tx.lock().await;
                            *guard = None;
                        }
                    }
                }
            }
        });
    }

    if let Some(cli_path) = cli_path {
        tracing::info!("Starting CLI: {}", cli_path.display());
        let mut cli = tokio::process::Command::new("python3")
            .arg(&cli_path)
            .arg("--port")
            .arg(args.http_port.to_string())
            .env("ACTUS_API_TOKEN", &api_token)
            .stdin(std::process::Stdio::inherit())
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::inherit())
            .spawn()
            .map_err(|e| anyhow::anyhow!("Failed to spawn CLI: {}", e))?;

        // Wait for CLI, server, or shutdown signal
        tokio::select! {
            r = http_server => {
                cli.kill().await.ok();
                for c in children.iter_mut() { c.kill().ok(); }
                r.unwrap()?
            },
            result = cli.wait() => {
                match result {
                    Ok(status) => tracing::info!("CLI exited with status: {}", status),
                    Err(e) => tracing::error!("CLI error: {}", e),
                }
            },
            _ = shutdown_rx => {
                cli.kill().await.ok();
                for c in children.iter_mut() { c.kill().ok(); }
                tracing::info!("Shutdown complete");
            },
        }
    } else {
        tracing::warn!("No CLI found, running server only");
        // Wait for servers or shutdown signal
        tokio::select! {
            r = http_server => {
                for c in children.iter_mut() { c.kill().ok(); }
                r.unwrap()?
            },
            _ = shutdown_rx => {
                for c in children.iter_mut() { c.kill().ok(); }
                tracing::info!("Shutdown complete");
            },
        }
    }

    Ok(())
}
