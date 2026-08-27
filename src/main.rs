// : Headless Zed AI agent — REST API server
//
// Launches Zed in --headless mode, connects via WebSocket,
// and exposes a REST API for multi-thread chat with async task queue.
//
// Usage:
//    --workdir /path/to/project

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

use actus::agent::config::{load_config, AgentDefaults};
use actus::agent::AgentKind;
use actus::agent::AgentRegistry;
use actus::server::run_http_server;
use actus::server::{AppState, WsCommandTx};
use actus::zed::backend::ZedBackend;
use actus::zed::control::run_ws_server;
use actus::zed::{ensure_zed_settings, launch_zed, ZedManager};

#[derive(clap::Parser, Debug, Clone)]
#[command(name = "", version, about = "Headless Zed AI agent server")]
struct Args {
    /// Helix headless Zed binary path
    #[arg(long)]
    bin: Option<PathBuf>,

    /// Working directory for Zed
    #[arg(long, default_value = ".")]
    workdir: PathBuf,

    /// HTTP API port
    #[arg(long, default_value = "9090")]
    http_port: u16,

    /// WebSocket port for Zed to connect to
    #[arg(long, default_value = "8080")]
    ws_port: u16,

    /// LLM API key (default: LLM_API_KEY env var)
    #[arg(long)]
    api_key: Option<String>,

    /// LLM provider name (default: LLM_PROVIDER env or "deepseek")
    #[arg(long, default_value = "deepseek")]
    provider: String,

    /// LLM API base URL (default: LLM_BASE_URL env or "https://api.deepseek.com/v1")
    #[arg(long, default_value = "https://api.deepseek.com/v1")]
    base_url: String,

    /// Path to terminal.py (auto-detected if not set)
    #[arg(long)]
    cli: Option<PathBuf>,

    /// Server-only mode: don't auto-start CLI
    #[arg(long, default_value_t = false)]
    server_only: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Args = clap::Parser::parse();

    // Init logging — always to stderr
    if std::env::var("RUST_LOG").is_err() {
        unsafe { std::env::set_var("RUST_LOG", "actus=info"); }
    }
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env()?)
        .with_writer(std::io::stderr)
        .init();

    // Resolve API key
    let api_key = args
        .api_key
        .or_else(|| std::env::var("LLM_API_KEY").ok())
        .ok_or_else(|| anyhow::anyhow!("API key required: set LLM_API_KEY or --api-key"))?;

    // Resolve binary path
    let bin_path = if let Some(p) = args.bin {
        p
    } else {
        let arch_suffix = match std::env::consts::ARCH {
            "aarch64" => "arm64",
            "x86_64" => "amd64",
            other => other,
        };
        let bin_name = format!("helix-zed-headless-{}", arch_suffix);
        let candidates = vec![
            dirs::home_dir()
                .map(|h| h.join(format!(".bin/{}", bin_name)))
                .unwrap_or_default(),
            PathBuf::from(format!("../.bin/{}", bin_name)),
            PathBuf::from(format!(".bin/{}", bin_name)),
            PathBuf::from(format!("helix/.bin/{}", bin_name)),
        ];
        candidates
            .into_iter()
            .find(|p| p.exists())
            .ok_or_else(|| anyhow::anyhow!("helix-zed-headless binary not found"))?
    };

    let workdir = std::fs::canonicalize(&args.workdir)?;

    // Read model/provider config from args or env
    let provider = std::env::var("LLM_PROVIDER").unwrap_or(args.provider);
    let base_url = std::env::var("LLM_BASE_URL").unwrap_or(args.base_url);
    let model_name = std::env::var("LLM_MODEL").unwrap_or_else(|_| format!("{}-chat", provider));
    let model_display = std::env::var("LLM_MODEL_DISPLAY").unwrap_or_else(|_| model_name.clone());

    // Resolve agent config: ACTUS_CONFIG overrides ~/.actus/config.toml.
    // Missing file (or no override) means a single default zed agent.
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
    let mut children: Vec<tokio::process::Child> = Vec::new();
    // (manager, ws_tx) pairs drive the shutdown handler and health monitor.
    let mut monitors: Vec<(Arc<RwLock<ZedManager>>, WsCommandTx)> = Vec::new();

    let default_name = if specs.iter().any(|s| s.name == "zed") {
        "zed".to_string()
    } else {
        specs[0].name.clone()
    };

    for spec in &specs {
        match spec.kind {
            AgentKind::Zed => {
                let ws_host = format!("127.0.0.1:{}", spec.ws_port);
                let user_data_dir = tempfile::tempdir()?;
                ensure_zed_settings(
                    user_data_dir.path(),
                    &spec.api_key,
                    &spec.provider,
                    &spec.base_url,
                    &spec.model,
                    &spec.model_display,
                )?;
                let threads_dir = threads_root.join(&spec.name);
                std::fs::create_dir_all(&threads_dir)?;
                let session_id = format!(
                    "ses_actus-{}-{}",
                    spec.name,
                    &uuid::Uuid::new_v4().to_string()[..8]
                );
                let manager = Arc::new(RwLock::new(ZedManager::new(
                    session_id.clone(),
                    ws_host.clone(),
                    &threads_dir,
                )));
                let ws_tx: WsCommandTx = Arc::new(tokio::sync::Mutex::new(None));
                monitors.push((manager.clone(), ws_tx.clone()));

                // Per-agent WebSocket server (Zed connects back here).
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
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;

                let child = launch_zed(
                    &spec.bin,
                    &workdir,
                    user_data_dir.path(),
                    &session_id,
                    &ws_host,
                )
                .await?;
                tracing::info!(
                    "Agent '{}' launched (PID {:?}, WS ws://{}, threads {})",
                    spec.name,
                    child.id(),
                    ws_host,
                    threads_dir.display()
                );
                children.push(child);

                let backend = Arc::new(ZedBackend {
                    manager: manager.clone(),
                    ws_tx: ws_tx.clone(),
                });
                registry.register(backend, spec.name == default_name);
            }
            AgentKind::LangGraph | AgentKind::Native => {
                tracing::warn!(
                    "Agent '{}': kind {:?} has no adapter yet, skipping",
                    spec.name,
                    spec.kind
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
    let state = Arc::new(AppState::new(registry, workdir.clone()));

    let http_server = tokio::spawn({
        let state = state.clone();
        let addr = format!("127.0.0.1:{}", args.http_port);
        async move { run_http_server(&addr, state).await }
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
            Some(PathBuf::from("apps//terminal.py")),
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
            let mut sigterm = tokio::signal::unix::signal(
                tokio::signal::unix::SignalKind::terminate(),
            ).expect("Failed to register SIGTERM handler");
            let mut sigint = tokio::signal::unix::signal(
                tokio::signal::unix::SignalKind::interrupt(),
            ).expect("Failed to register SIGINT handler");

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
                    g.save_threads();
                }
            }

            let _ = shutdown_tx.send(());
        });
    }

    // ── WebSocket health monitor ─────────────────────────────────────
    // Periodically checks if events are still arriving from each Zed via
    // its WebSocket. If no events arrive within 15 seconds, assumes the
    // Zed side is stuck and forces reconnection by setting
    // zed_connected = false. The WS read loop's health check detects
    // this, breaks, and the connection loop accepts a new connection
    // (Zed auto-reconnects).
    {
        let monitors = monitors.clone();

        tokio::spawn(async move {
            tracing::info!("Health monitor started (check every 10s, timeout 15s)");
            let check_interval = Duration::from_secs(10);
            let timeout = Duration::from_secs(15);

            loop {
                tokio::time::sleep(check_interval).await;

                for (mgr, ws_tx) in &monitors {
                    let (connected, elapsed) = {
                        let g = mgr.read().await;
                        (g.zed_connected, g.last_sse_event_time.elapsed())
                    };

                    if connected && elapsed > timeout {
                        tracing::warn!(
                            "Health monitor: no events for {}s, forcing reconnection",
                            elapsed.as_secs()
                        );
                        // Force reconnection: clear zed_connected and the
                        // shared command channel. The WS read loop's
                        // periodic check breaks, and the connection loop
                        // accepts a new connection (Zed auto-reconnects).
                        {
                            let mut g = mgr.write().await;
                            g.zed_connected = false;
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
            .stdin(std::process::Stdio::inherit())
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::inherit())
            .spawn()
            .map_err(|e| anyhow::anyhow!("Failed to spawn CLI: {}", e))?;

        // Wait for CLI, server, or shutdown signal
        tokio::select! {
            r = http_server => {
                cli.kill().await.ok();
                for c in children.iter_mut() { c.kill().await.ok(); }
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
                for c in children.iter_mut() { c.kill().await.ok(); }
                tracing::info!("Shutdown complete");
            },
        }
    } else {
        tracing::warn!("No CLI found, running server only");
        // Wait for servers or shutdown signal
        tokio::select! {
            r = http_server => {
                for c in children.iter_mut() { c.kill().await.ok(); }
                r.unwrap()?
            },
            _ = shutdown_rx => {
                for c in children.iter_mut() { c.kill().await.ok(); }
                tracing::info!("Shutdown complete");
            },
        }
    }

    Ok(())
}
