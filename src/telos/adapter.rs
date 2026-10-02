// TelosFactory — the telos kind's launch path.
//
// Everything telos-specific about running an agent lives here: the option
// table the kind reads, the WebSocket server the agent connects back to,
// the settings and credentials it starts from, the reconnect monitor, and
// the flush on shutdown. The composition root registers this factory and
// never sees a telos field.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::RwLock;

use crate::agent::adapter::{AgentFactory, LaunchContext, LaunchedAgent};
use crate::agent::config::AgentSpec;
use crate::telos::backend::TelosBackend;
use crate::telos::control::run_ws_server;
use crate::telos::options::{TelosDefaults, TelosOptions, TelosSettings};
use crate::telos::{ensure_telos_settings, launch_telos, TelosManager, WsCommandTx};

/// Factory for the `telos` kind: a sessionful agent over ACP/WebSocket.
pub struct TelosFactory {
    defaults: TelosDefaults,
}

impl TelosFactory {
    pub fn new(defaults: TelosDefaults) -> Self {
        Self { defaults }
    }

    /// Read one spec's declaration into the settings a launch uses.
    pub fn resolve(&self, spec: &AgentSpec) -> Result<TelosSettings, String> {
        let options: TelosOptions = spec.options()?;
        options.resolve(&spec.name, &self.defaults)
    }
}

#[async_trait::async_trait]
impl AgentFactory for TelosFactory {
    fn kind(&self) -> &'static str {
        "telos"
    }

    /// A WebSocket port is this adapter's own resource: two telos agents
    /// cannot share one, because both would race for the same connect-back
    /// port. The check reads the telos specs and only the telos specs.
    fn validate(&self, spec: &AgentSpec, all: &[AgentSpec]) -> Result<(), String> {
        let resolved = self.resolve(spec)?;
        for other in all {
            if other.name == spec.name || other.kind != spec.kind {
                continue;
            }
            let other_resolved = self.resolve(other)?;
            if other_resolved.ws_port == resolved.ws_port {
                return Err(format!(
                    "config: agents '{}' and '{}' share WebSocket port {}",
                    other.name, spec.name, resolved.ws_port
                ));
            }
        }
        Ok(())
    }

    async fn launch(&self, spec: &AgentSpec, ctx: &LaunchContext) -> anyhow::Result<LaunchedAgent> {
        let settings = self.resolve(spec).map_err(anyhow::Error::msg)?;
        if !settings.bin.exists() {
            return Err(anyhow::anyhow!(
                "agent '{}': telos binary not found at {}",
                spec.name,
                settings.bin.display()
            ));
        }

        // A per-agent workdir scopes this agent to one project. Without one
        // the agent opens the fabric-wide workdir. A missing directory is an
        // error rather than a silent fallback: the agent only opens a
        // worktree for a path that exists, so a typo would otherwise yield
        // an agent with no project at all.
        let workdir = match &spec.workdir {
            Some(dir) => {
                if !dir.is_dir() {
                    return Err(anyhow::anyhow!(
                        "agent '{}': workdir not found at {}",
                        spec.name,
                        dir.display()
                    ));
                }
                dir.clone()
            }
            None => ctx.workdir.clone(),
        };
        let ws_host = format!("127.0.0.1:{}", settings.ws_port);

        // The agent reads its settings at startup and watches them while it
        // runs, so the user data directory lives as long as the backend.
        let user_data_dir = tempfile::tempdir()?;
        ensure_telos_settings(user_data_dir.path(), &settings)?;

        let threads_dir = ctx.threads_root.join(&spec.name);
        std::fs::create_dir_all(&threads_dir)?;
        let session_id = format!(
            "ses_actus-{}-{}",
            spec.name,
            &uuid::Uuid::new_v4().to_string()[..8]
        );
        // The record goes to the store the composition root supplied for this
        // agent's own directory, so a deployment that links an engine of its
        // own reaches it and the plain binary keeps the document it had.
        let store = (ctx.store)(&threads_dir).map_err(anyhow::Error::msg)?;
        let manager = Arc::new(RwLock::new(
            TelosManager::with_store(session_id.clone(), ws_host.clone(), store)
                .map_err(anyhow::Error::msg)?,
        ));
        let ws_tx: WsCommandTx = Arc::new(tokio::sync::Mutex::new(None));

        // Per-agent WebSocket server (the agent connects back here).
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
        // Let the listener bind before the agent is told to connect.
        tokio::time::sleep(Duration::from_millis(200)).await;

        let child = launch_telos(
            &settings.bin,
            &workdir,
            user_data_dir.path(),
            &session_id,
            &ws_host,
            settings.tool_approval,
            &spec.name,
            ctx.http_port,
            &ctx.api_token,
            &threads_dir.join("telos.log"),
        )
        .await?;
        tracing::info!(
            "Agent '{}' launched (PID {:?}, WS ws://{}, threads {})",
            spec.name,
            child.id(),
            ws_host,
            threads_dir.display()
        );

        TelosManager::spawn_health_monitor(manager.clone(), ws_tx.clone());

        let backend = TelosBackend::new(
            spec.name.clone(),
            manager,
            ws_tx,
            Some(workdir.display().to_string()),
            user_data_dir,
        );
        Ok(LaunchedAgent {
            backend: Arc::new(backend),
            children: vec![child],
        })
    }
}
