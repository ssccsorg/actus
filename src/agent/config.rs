// Agent configuration: the declaration the fabric understands.
//
// An agent entry names the agent, its kind, and its working directory, and
// carries the rest of its declaration as an options table that the kind's
// factory reads. The fabric never parses a platform field; a factory parses
// its own, with its own defaults and its own validation.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::agent::adapter::FactoryRegistry;

/// Resolved declaration of one agent platform instance.
#[derive(Clone, Debug)]
pub struct AgentSpec {
    /// Unique agent name used in routing (e.g. "telos", "browser").
    pub name: String,
    /// Registered kind name, resolved against the factory registry.
    pub kind: String,
    /// Working directory for spawned agent processes. None means the
    /// server working directory.
    pub workdir: Option<PathBuf>,
    /// The declaration table as written, minus the fields above. The kind's
    /// factory deserializes it into its own options type; the fabric never
    /// reads it.
    pub options: toml::Table,
}

impl AgentSpec {
    /// Read the declaration as one adapter's options. Keys the adapter does
    /// not declare are ignored, so every adapter can read the same table.
    pub fn options<T: serde::de::DeserializeOwned>(&self) -> Result<T, String> {
        toml::Value::Table(self.options.clone())
            .try_into()
            .map_err(|e| format!("agent '{}': cannot read its options: {e}", self.name))
    }
}

/// One allow rule: `controller` may dispatch to every name in
/// `targets`. Either side accepts `"*"` for any agent.
#[derive(Clone, Debug, Deserialize)]
pub struct ControlRule {
    pub controller: String,
    #[serde(default)]
    pub targets: Vec<String>,
}

/// Meta-agent control policy from the `[agent-control]` section of the
/// config file. Empty `allow` means no agent may control another.
/// Human API clients carry no controller identity and stay ungated.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct ControlPolicy {
    #[serde(default)]
    pub allow: Vec<ControlRule>,
}

impl ControlPolicy {
    /// Whether `controller` may dispatch to `target`. A controller can
    /// never dispatch to itself, regardless of the list.
    pub fn allows(&self, controller: &str, target: &str) -> bool {
        if controller == target {
            return false;
        }
        self.allow.iter().any(|rule| {
            (rule.controller == "*" || rule.controller == controller)
                && rule.targets.iter().any(|t| t == "*" || t == target)
        })
    }
}

/// Load the meta-agent control policy from the config file. A missing
/// file or a missing `[agent-control]` section yields an empty policy
/// (default deny for agent-originated dispatch).
#[derive(Deserialize)]
struct ControlFile {
    #[serde(rename = "agent-control", default)]
    policy: ControlPolicy,
}

pub fn load_control_policy(file: Option<&Path>) -> Result<ControlPolicy, String> {
    match file {
        Some(p) => {
            let text = std::fs::read_to_string(p)
                .map_err(|e| format!("cannot read {}: {}", p.display(), e))?;
            let cfg: ControlFile = toml::from_str(&text)
                .map_err(|e| format!("cannot parse {}: {}", p.display(), e))?;
            Ok(cfg.policy)
        }
        None => Ok(ControlPolicy::default()),
    }
}

/// Trim a value and strip one pair of surrounding quotes. `.env` files
/// often carry `KEY="value"`; a quoted model name or key would fail the
/// agent-side lookup.
pub fn unquote_env_value(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.len() >= 2
        && ((trimmed.starts_with('"') && trimmed.ends_with('"'))
            || (trimmed.starts_with('\'') && trimmed.ends_with('\'')))
    {
        trimmed[1..trimmed.len() - 1].to_string()
    } else {
        trimmed.to_string()
    }
}

/// File-format variant of `AgentSpec`: the fabric fields typed, everything
/// else kept as written for the factory.
#[derive(Deserialize)]
struct AgentSpecFile {
    name: String,
    #[serde(default)]
    kind: Option<String>,
    #[serde(default)]
    workdir: Option<PathBuf>,
    #[serde(flatten)]
    options: toml::Table,
}

#[derive(Deserialize)]
struct ConfigFile {
    #[serde(default)]
    agents: Vec<AgentSpecFile>,
}

/// Load and resolve agent specs against the registered factories.
///
/// `file = None` yields one default agent of the registry's default kind.
/// A file must parse, name at least one agent, and name each agent once.
/// Every kind is resolved against the registry, and every spec is validated
/// by its factory, before anything launches.
pub fn load_config(
    file: Option<&Path>,
    registry: &FactoryRegistry,
) -> Result<Vec<AgentSpec>, String> {
    let files: Vec<AgentSpecFile> = match file {
        Some(p) => {
            let text = std::fs::read_to_string(p)
                .map_err(|e| format!("cannot read {}: {}", p.display(), e))?;
            let cfg: ConfigFile = toml::from_str(&text)
                .map_err(|e| format!("cannot parse {}: {}", p.display(), e))?;
            cfg.agents
        }
        None => vec![AgentSpecFile {
            name: registry
                .default_kind()
                .ok_or_else(|| {
                    "config: no agents defined and no default kind registered".to_string()
                })?
                .to_string(),
            kind: None,
            workdir: None,
            options: toml::Table::new(),
        }],
    };

    if files.is_empty() {
        return Err("config: no agents defined".to_string());
    }

    let mut specs = Vec::with_capacity(files.len());
    let mut names = HashSet::new();
    for f in files {
        if !names.insert(f.name.clone()) {
            return Err(format!("config: duplicate agent name '{}'", f.name));
        }
        let kind = match f.kind {
            Some(kind) => kind,
            None => registry
                .default_kind()
                .ok_or_else(|| {
                    format!(
                        "config: agent '{}': no kind declared and no default kind registered",
                        f.name
                    )
                })?
                .to_string(),
        };
        specs.push(AgentSpec {
            name: f.name,
            kind,
            workdir: f.workdir,
            options: f.options,
        });
    }

    // Factories validate their own specs, with the whole list in hand so a
    // constraint across a kind's agents (a shared port, say) is theirs too.
    for spec in &specs {
        registry.validate(spec, &specs)?;
    }
    Ok(specs)
}
