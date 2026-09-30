// Actus — agent execution runtime (REST API server)
//
// Launches and supervises agent adapters and exposes a REST API for
// multi-thread chat with an async task queue. This binary is the
// composition point: the fabric names no platform, and the kinds actus
// ships are registered here.
//
// Usage:
//    --workdir /path/to/project

use std::path::PathBuf;
use std::sync::Arc;

use actus::agent::adapter::{FactoryRegistry, LaunchContext};
use actus::agent::browser::BrowserFactory;
use actus::agent::config::{load_config, load_control_policy, unquote_env_value};
use actus::agent::ext_cli::ExtCliFactory;
use actus::agent::native::NativeFactory;
use actus::agent::AgentRegistry;
use actus::control;
use actus::server::{run_http_server, AppState};
use actus::telos::adapter::TelosFactory;
use actus::telos::options::{resolve_llm_settings, TelosDefaults};

/// The `actus control` stdio MCP proxy is a subcommand so the same binary
/// can serve as a context server of a sessionful agent such as telos.
#[derive(clap::Subcommand, Debug, Clone)]
enum Command {
    /// Run the meta-agent control MCP proxy over stdio.
    Control,
}

#[derive(clap::Parser, Debug, Clone)]
#[command(name = "", version, about = "Actus agent runtime server")]
struct Args {
    #[command(subcommand)]
    command: Option<Command>,

    /// Telos binary path (the default agent kind)
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

    /// Reasoning effort asked of the model: none, minimal, low, medium, high,
    /// xhigh or max (default: LLM_REASONING_EFFORT env or "high")
    #[arg(long)]
    reasoning_effort: Option<String>,

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
///
/// The write is announced when it changes an existing value. Every consumer
/// reads this one file, so a second actus started with an explicit token
/// silently invalidates the clients and services already using the old one,
/// and the mismatch then looks like a rejected credential everywhere else.
fn persist_api_token(token: &str) -> anyhow::Result<()> {
    let file = api_token_file();
    if let Ok(existing) = std::fs::read_to_string(&file) {
        let existing = existing.trim();
        if !existing.is_empty() && existing != token {
            tracing::warn!(
                "Replacing the API token at {}: {}... becomes {}...",
                file.display(),
                token_prefix(existing),
                token_prefix(token)
            );
        }
    }
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

/// Enough of a token to tell two apart, and never the whole value.
fn token_prefix(token: &str) -> String {
    token.chars().take(8).collect()
}

/// The agent processes this run started, taken down when it leaves however it leaves.
///
/// The launch loop spawns an agent per project, and the HTTP API binds after it, so every
/// step past the first spawn can fail. A `?` there used to return from `main` and leave the
/// agents running with no actus serving them: nothing holds a port, so nothing else
/// notices, and they reconnect to the next actus's WebSocket ports in place of the agents
/// it started, which is one actus's leftovers answering another actus's roster. A guard
/// covers every exit, including the ones no code here writes.
struct AgentChildren(Vec<std::process::Child>);

impl AgentChildren {
    fn new() -> Self {
        Self(Vec::new())
    }

    fn extend(&mut self, children: Vec<std::process::Child>) {
        self.0.extend(children);
    }

    /// Signal and reap nothing: the agents are killed, and actus is on its way out, so a
    /// zombie holds the pid until this process exits. Running twice is harmless for the
    /// same reason, which is what lets the graceful paths call it and the guard repeat it.
    fn take_down(&mut self) {
        for child in &mut self.0 {
            child.kill().ok();
        }
    }
}

impl Drop for AgentChildren {
    fn drop(&mut self) {
        self.take_down();
    }
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

    // The control MCP proxy runs as its own process under a sessionful
    // agent; it never starts the server.
    if let Some(Command::Control) = args.command {
        return control::run_control_proxy().await;
    }

    // Init logging — always to stderr. The filter falls back to
    // `actus=info` when RUST_LOG is unset or invalid.
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("actus=info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();

    // Environment values from a `.env` file may carry surrounding quotes
    // (`LLM_MODEL="example-model"`). Strip them defensively so a quoted
    // value never leaks into the agent's settings.json or credentials,
    // where a quoted model name or key fails the lookup.
    let api_key = args
        .api_key
        .or_else(|| std::env::var("LLM_API_KEY").ok())
        .map(|k| unquote_env_value(&k));

    // Resolve the telos kind's binary: --bin, TELOS_BIN, or the sibling
    // build. The factory holds it as a default; a spec that declares its own
    // `bin` overrides it.
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
    let llm = resolve_llm_settings(
        |key| std::env::var(key).ok(),
        args.provider.clone(),
        args.base_url.clone(),
        args.reasoning_effort.clone(),
    );
    let provider = llm.provider;
    let base_url = llm.base_url;
    let model_name = llm.model;
    let model_display = llm.model_display;
    let reasoning_effort = llm.reasoning_effort;

    // The built-in factories, each holding its own platform's defaults.
    // This is the composition point: the fabric never names a platform, and
    // this binary is where the ones it ships are registered.
    let telos_defaults = TelosDefaults {
        bin: bin_path.clone(),
        ws_port: args.ws_port,
        provider,
        model: model_name,
        model_display,
        base_url,
        api_key: api_key.clone(),
        reasoning_effort,
    };
    let mut factories = FactoryRegistry::new();
    factories.register_default(Arc::new(TelosFactory::new(telos_defaults)));
    factories.register(Arc::new(ExtCliFactory));
    factories.register(Arc::new(NativeFactory));
    factories.register(Arc::new(BrowserFactory));
    // `langgraph` was declared as a platform before the factory seam and has
    // no adapter. A config that names it starts and skips that agent with a
    // warning, which is what actus did when the kind was an enum variant; a
    // name that was never declared is refused instead.
    factories.reserve("langgraph");

    // Resolve agent config: ACTUS_CONFIG overrides ~/.actus/config.toml.
    // A missing file (or no override) means one agent of the registry's
    // default kind.
    let config_path = std::env::var("ACTUS_CONFIG")
        .ok()
        .map(PathBuf::from)
        .or_else(|| dirs::home_dir().map(|h| h.join(".actus").join("config.toml")));
    let config_file = match config_path {
        Some(p) if p.exists() => Some(p),
        _ => None,
    };

    let specs = load_config(config_file.as_deref(), &factories).map_err(anyhow::Error::msg)?;
    let control_policy = load_control_policy(config_file.as_deref()).map_err(anyhow::Error::msg)?;
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

    let launch_ctx = LaunchContext {
        workdir: workdir.clone(),
        threads_root: threads_root.clone(),
        http_port: args.http_port,
        api_token: api_token.clone(),
    };

    // Launch every configured agent and register it in the fabric.
    let mut registry = AgentRegistry::new();
    let mut children = AgentChildren::new();

    // The first configured agent is the fabric default.
    let default_name = specs[0].name.clone();

    for spec in &specs {
        if factories.is_reserved(&spec.kind) {
            tracing::warn!(
                "Agent '{}': kind {} has no adapter yet, skipping",
                spec.name,
                spec.kind
            );
            continue;
        }
        let factory = factories.get(&spec.kind).ok_or_else(|| {
            anyhow::anyhow!("agent '{}': unknown kind '{}'", spec.name, spec.kind)
        })?;
        let launched = factory.launch(spec, &launch_ctx).await?;
        children.extend(launched.children);
        registry.register(launched.backend, spec.name == default_name);
    }

    if registry.default_agent().is_none() {
        anyhow::bail!("no agent could be launched from config");
    }

    tracing::info!("Starting actus server");
    tracing::info!("  HTTP API:   http://127.0.0.1:{}", args.http_port);
    tracing::info!("  Workdir:    {}", workdir.display());
    tracing::info!("  Threads:    {}", threads_root.display());

    // Build app state and start HTTP server
    let state = Arc::new(AppState::new_with_policy(
        registry,
        workdir.clone(),
        Some(api_token.clone()),
        control_policy,
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
    // Handle SIGTERM/SIGINT: ask every registered agent to persist what it
    // holds, then exit cleanly.
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    {
        let state = state.clone();

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

            for (_, backend) in state.agents.agents() {
                backend.shutdown().await;
            }

            let _ = shutdown_tx.send(());
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
                children.take_down();
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
                children.take_down();
                tracing::info!("Shutdown complete");
            },
        }
    } else {
        tracing::warn!("No CLI found, running server only");
        // Wait for servers or shutdown signal
        tokio::select! {
            r = http_server => {
                children.take_down();
                r.unwrap()?
            },
            _ = shutdown_rx => {
                children.take_down();
                tracing::info!("Shutdown complete");
            },
        }
    }

    Ok(())
}
