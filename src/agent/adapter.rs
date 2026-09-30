// The adapter seam: what a new agent platform implements and registers.
//
// A kind is a name a config declaration resolves against this registry.
// The fabric reads only `AgentSpec`, `AgentFactory`, and `AgentBackend`; a
// platform's fields, its launch path, and its lifecycle live inside its own
// factory module. Adding a platform is one factory and one registration.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use crate::agent::config::AgentSpec;
use crate::agent::AgentBackend;

/// What a factory needs from actus to launch an agent. Everything here is
/// fabric state, not platform state: the server's own directories, port,
/// and token.
pub struct LaunchContext {
    /// The server working directory. A spec that names no workdir inherits
    /// this one.
    pub workdir: PathBuf,
    /// Where per-agent thread state is kept (`~/.actus/threads`).
    pub threads_root: PathBuf,
    /// The actus HTTP port, for agents that call back into actus.
    pub http_port: u16,
    /// The bearer token such an agent presents.
    pub api_token: String,
}

/// One running agent and the process hands actus holds for it.
pub struct LaunchedAgent {
    pub backend: Arc<dyn AgentBackend>,
    /// Child processes killed when actus exits.
    pub children: Vec<std::process::Child>,
}

/// A platform factory: one config declaration in, one running agent out.
///
/// A factory owns everything about its platform: which option fields its
/// declarations read, what its defaults are, what a valid declaration looks
/// like, and how a process (or nothing at all) is started for it.
#[async_trait::async_trait]
pub trait AgentFactory: Send + Sync {
    /// The kind name this factory answers to in the config file.
    fn kind(&self) -> &'static str;

    /// Check one spec before anything launches. `all` carries every spec,
    /// so a factory can enforce a constraint across its own agents (a
    /// port collision, a shared resource). An error names the agent it
    /// concerns, because the caller only sees the string.
    fn validate(&self, spec: &AgentSpec, all: &[AgentSpec]) -> Result<(), String> {
        let _ = (spec, all);
        Ok(())
    }

    /// Launch one agent.
    async fn launch(&self, spec: &AgentSpec, ctx: &LaunchContext) -> anyhow::Result<LaunchedAgent>;
}

/// Registered factories, by kind name. Names are sorted so the kind list an
/// error names is the same from one failure to the next.
#[derive(Default)]
pub struct FactoryRegistry {
    factories: BTreeMap<String, Arc<dyn AgentFactory>>,
    default_kind: Option<&'static str>,
}

impl FactoryRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a factory under its kind name.
    pub fn register(&mut self, factory: Arc<dyn AgentFactory>) {
        self.factories.insert(factory.kind().to_string(), factory);
    }

    /// Register the kind a config-less launch resolves to, and register it.
    pub fn register_default(&mut self, factory: Arc<dyn AgentFactory>) {
        self.default_kind = Some(factory.kind());
        self.register(factory);
    }

    pub fn get(&self, kind: &str) -> Option<Arc<dyn AgentFactory>> {
        self.factories.get(kind).cloned()
    }

    /// The kind a config without a file resolves to. None when no factory
    /// was registered as the default, which leaves a config-less launch an
    /// error rather than a guess.
    pub fn default_kind(&self) -> Option<&'static str> {
        self.default_kind
    }

    /// The registered kinds, in name order, for diagnostics.
    pub fn kinds(&self) -> Vec<&str> {
        self.factories.keys().map(String::as_str).collect()
    }

    /// Validate one spec against its factory. An unregistered kind is
    /// refused here, naming the kinds that are registered.
    pub fn validate(&self, spec: &AgentSpec, all: &[AgentSpec]) -> Result<(), String> {
        let factory = self.get(&spec.kind).ok_or_else(|| {
            format!(
                "config: agent '{}': unknown kind '{}'; registered kinds: {}",
                spec.name,
                spec.kind,
                self.kinds().join(", ")
            )
        })?;
        factory.validate(spec, all)
    }
}
