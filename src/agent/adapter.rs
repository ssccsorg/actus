// The adapter seam: what a new agent platform implements and registers.
//
// A kind is a name a config declaration resolves against this registry.
// The fabric reads only `AgentSpec`, `AgentFactory`, and `AgentBackend`; a
// platform's fields, its launch path, and its lifecycle live inside its own
// factory module. Adding a platform is one factory and one registration.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;

use crate::agent::config::AgentSpec;
use crate::agent::AgentBackend;
use crate::store::StoreFactory;

/// What a factory needs from actus to launch an agent. Everything here is
/// fabric state, not platform state: the server's own directories, port,
/// token, and the store an agent's record goes to.
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
    /// Where this agent's record lives. The composition root decides, so a
    /// build that links an engine of its own reaches it.
    pub store: StoreFactory,
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
    reserved: BTreeSet<String>,
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

    /// Name a platform actus knows and has no factory for. A spec of such a
    /// kind loads and is skipped at launch with a warning, which is what
    /// actus did when the platform was an enum variant without an adapter.
    /// Registering a factory for the same name takes precedence.
    pub fn reserve(&mut self, kind: &str) {
        self.reserved.insert(kind.to_string());
    }

    /// Whether `kind` is reserved and still has no factory.
    pub fn is_reserved(&self, kind: &str) -> bool {
        self.reserved.contains(kind) && !self.factories.contains_key(kind)
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
    /// refused here, naming the kinds that are registered, unless it is a
    /// reserved kind: a platform actus knows and has no adapter for is
    /// skipped at launch rather than failing the whole config.
    pub fn validate(&self, spec: &AgentSpec, all: &[AgentSpec]) -> Result<(), String> {
        let Some(factory) = self.get(&spec.kind) else {
            if self.is_reserved(&spec.kind) {
                return Ok(());
            }
            return Err(format!(
                "config: agent '{}': unknown kind '{}'; registered kinds: {}",
                spec.name,
                spec.kind,
                self.kinds().join(", ")
            ));
        };
        factory.validate(spec, all)
    }
}
