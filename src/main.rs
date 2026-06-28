// : Headless Zed AI agent — REST API server
//
// Launches Zed in --headless mode, connects via WebSocket,
// and exposes a REST API for multi-thread chat with async task queue.
//
// Usage:
//    --workdir /path/to/project

mod files;
mod git;
mod server;
mod zed;

use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::server::AppState;
use crate::zed::ZedManager;

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

    /// LLM API key (default: DEEPSEEK_API_KEY or LLM_API_KEY env var)
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
        .or_else(|| std::env::var("DEEPSEEK_API_KEY").ok())
        .or_else(|| std::env::var("LLM_API_KEY").ok())
        .ok_or_else(|| anyhow::anyhow!("API key required: set DEEPSEEK_API_KEY or --api-key"))?;

    // Resolve binary path
    let bin_path = if let Some(p) = args.bin {
        p
    } else {
        // Search common locations
        let candidates = vec![
            dirs::home_dir()
                .map(|h| h.join(".bin/helix-zed-headless-arm64"))
                .unwrap_or_default(),
            PathBuf::from("../.bin/helix-zed-headless-arm64"),
            PathBuf::from(".bin/helix-zed-headless-arm64"),
            PathBuf::from("helix/.bin/helix-zed-headless-arm64"),
        ];
        candidates
            .into_iter()
            .find(|p| p.exists())
            .ok_or_else(|| anyhow::anyhow!("helix-zed-headless binary not found"))?
    };

    let workdir = std::fs::canonicalize(&args.workdir)?;
    let ws_host = format!("127.0.0.1:{}", args.ws_port);

    // Read model/provider config from args or env
    let provider = std::env::var("LLM_PROVIDER").unwrap_or(args.provider);
    let base_url = std::env::var("LLM_BASE_URL").unwrap_or(args.base_url);
    let model_name = std::env::var("LLM_MODEL").unwrap_or_else(|_| format!("{}-chat", provider));
    let model_display = std::env::var("LLM_MODEL_DISPLAY").unwrap_or_else(|_| model_name.clone());

    // Bootstrap Zed user data dir with LLM settings
    let user_data_dir = tempfile::tempdir()?;
    zed::ensure_zed_settings(
        user_data_dir.path(),
        &api_key,
        &provider,
        &base_url,
        &model_name,
        &model_display,
    )?;

    // Create threads directory for persistence
    let threads_dir = dirs::home_dir()
        .ok_or_else(|| anyhow::anyhow!("Cannot determine home directory"))?
        .join(".")
        .join("threads");
    std::fs::create_dir_all(&threads_dir)?;

    let session_id = format!(
        "ses_actus-{}",
        uuid::Uuid::new_v4().to_string().split('-').next().unwrap()
    );

    tracing::info!("Starting  server");
    tracing::info!("  Binary:     {}", bin_path.display());
    tracing::info!("  Workdir:    {}", workdir.display());
    tracing::info!("  User data:  {}", user_data_dir.path().display());
    tracing::info!("  Session:    {}", session_id);
    tracing::info!("  Provider:   {} ({})", provider, base_url);
    tracing::info!("  Model:      {} ({})", model_name, model_display);
    tracing::info!("  HTTP API:   http://127.0.0.1:{}", args.http_port);
    tracing::info!("  WebSocket:  ws://{}", ws_host);

    // Start WebSocket server (Zed connects to us)
    let zed_manager = Arc::new(RwLock::new(ZedManager::new(
        session_id.clone(),
        ws_host.clone(),
        &threads_dir,
    )));

    let ws_zed_manager = zed_manager.clone();
    let ws_host_clone = ws_host.clone();
    let ws_server =
        tokio::spawn(async move { zed::run_ws_server(&ws_host_clone, ws_zed_manager).await });

    // Wait for WebSocket server to be ready
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Launch Zed headless
    zed::launch_zed(
        &bin_path,
        &workdir,
        user_data_dir.path(),
        &session_id,
        &ws_host,
    )
    .await?;

    // Build app state and start HTTP server
    let state = Arc::new(AppState::new(zed_manager.clone(), workdir.clone()));

    let http_server = tokio::spawn({
        let state = state.clone();
        let addr = format!("127.0.0.1:{}", args.http_port);
        async move { server::run_http_server(&addr, state).await }
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

    // Wait for agent to be ready before starting CLI
    for i in 0..30 {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        let ready = {
            let mgr = zed_manager.read().await;
            mgr.agent_ready
        };
        if ready {
            tracing::info!("Agent ready after {}s, starting CLI", i + 1);
            break;
        }
        if i == 29 {
            tracing::warn!("Agent not ready after 30s, starting CLI anyway");
        }
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

        // Wait for CLI or server to finish
        tokio::select! {
            r = ws_server => {
                cli.kill().await.ok();
                r.unwrap()?
            },
            r = http_server => {
                cli.kill().await.ok();
                r.unwrap()?
            },
            result = cli.wait() => {
                match result {
                    Ok(status) => tracing::info!("CLI exited with status: {}", status),
                    Err(e) => tracing::error!("CLI error: {}", e),
                }
            },
        }
    } else {
        tracing::warn!("No CLI found, running server only");
        // Wait for servers
        tokio::select! {
            r = ws_server => r.unwrap()?,
            r = http_server => r.unwrap()?,
        }
    }

    Ok(())
}
