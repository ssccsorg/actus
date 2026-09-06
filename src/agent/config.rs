// Agent configuration — static declaration of the platforms the fabric
// weaves. Agents live in `~/.actus/config.toml` (or `ACTUS_CONFIG`); when
// the file is absent a single default "telos" agent is derived from the CLI
// and environment.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::agent::AgentKind;

/// How a cli-kind agent receives the prompt message.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum PromptMode {
    /// Pass the prompt as a command argument. A `{prompt}` marker in
    /// `cli_args` is replaced; without a marker the prompt is appended
    /// as the final argument.
    Arg,
    /// Write the prompt to the child's stdin instead of passing it as an
    /// argument (for CLIs that read the prompt from stdin).
    Stdin,
}

fn default_prompt_mode() -> PromptMode {
    PromptMode::Arg
}

/// Tool call approval policy for an agent. `Always` auto-approves every
/// tool call (headless task execution); `Ask` waits for a human or a
/// future approval bridge; `Never` rejects tool calls.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ToolApproval {
    Always,
    Ask,
    Never,
}

impl ToolApproval {
    pub fn as_str(self) -> &'static str {
        match self {
            ToolApproval::Always => "always",
            ToolApproval::Ask => "ask",
            ToolApproval::Never => "never",
        }
    }
}

/// One MCP (Model Context Protocol) server attached to an agent. Matches
/// Telos's `context_servers` settings entries: either a local stdio process
/// (`command`/`args`/`env`) or a remote HTTP endpoint (`url`/`headers`).
#[derive(Clone, Debug, Deserialize)]
pub struct McpServer {
    pub name: String,
    /// Stdio transport: executable path.
    #[serde(default)]
    pub command: Option<String>,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub env: HashMap<String, String>,
    /// HTTP transport: remote MCP endpoint.
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub headers: HashMap<String, String>,
    /// Tool call timeout in seconds (stdio only).
    #[serde(default)]
    pub timeout: Option<u64>,
}

/// Resolved declaration of one agent platform instance. All fields are
/// concrete: `load_config` fills anything the file omits from defaults.
#[derive(Clone, Debug)]
pub struct AgentSpec {
    /// Unique agent name used in routing (e.g. "telos", "claude").
    pub name: String,
    pub kind: AgentKind,
    pub provider: String,
    pub model: String,
    pub model_display: String,
    pub base_url: String,
    /// LLM API key. Optional at the fabric level; the Telos adapter
    /// requires one when it launches an agent.
    pub api_key: Option<String>,
    /// Telos binary path.
    pub bin: PathBuf,
    /// WebSocket port the agent process connects back to.
    pub ws_port: u16,
    /// Tool call approval policy; drives the fork's TELOS_TOOL_APPROVAL env.
    pub tool_approval: ToolApproval,
    /// Working directory for spawned agent processes. None means the
    /// server working directory; raw-CLI agents that resolve project
    /// scope from the current directory use this field.
    pub workdir: Option<PathBuf>,
    /// MCP servers attached to this agent.
    pub mcp: Vec<McpServer>,
    /// Fixed CLI arguments prepended before the prompt for cli-kind agents.
    /// A `{prompt}` marker is replaced by the message; without a marker
    /// the message is appended as the final argument.
    pub cli_args: Vec<String>,
    /// Extra environment variables for cli-kind agents, merged over the
    /// inherited server environment.
    pub cli_env: HashMap<String, String>,
    /// Prompt injection mode for cli-kind agents.
    pub cli_prompt: PromptMode,
    /// Per-turn timeout in seconds for cli-kind agents.
    pub cli_timeout_secs: u64,
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

/// Resolved LLM endpoint settings. Actus ships no vendor defaults: the
/// provider label, model, and base URL come from the local environment
/// or the CLI flags only (issue #16).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LlmSettings {
    pub provider: String,
    pub base_url: String,
    pub model: String,
    pub model_display: String,
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

/// Resolve the OpenAI-compatible endpoint from the local environment and
/// CLI flags. Precedence matches the original main.rs behavior: env wins
/// over flags for provider and base URL; model and display name come from
/// the env only, with display falling back to the model. Missing values
/// stay empty so a later launch step can warn instead of inventing an
/// endpoint.
pub fn resolve_llm_settings(
    env_get: impl Fn(&str) -> Option<String>,
    provider_default: String,
    base_url_flag: Option<String>,
) -> LlmSettings {
    let provider = unquote_env_value(&env_get("LLM_PROVIDER").unwrap_or(provider_default));
    let base_url = env_get("LLM_BASE_URL")
        .map(|v| unquote_env_value(&v))
        .or_else(|| base_url_flag.map(|v| unquote_env_value(&v)))
        .unwrap_or_default();
    let model = unquote_env_value(&env_get("LLM_MODEL").unwrap_or_default());
    let model_display =
        unquote_env_value(&env_get("LLM_MODEL_DISPLAY").unwrap_or_else(|| model.clone()));
    LlmSettings {
        provider,
        base_url,
        model,
        model_display,
    }
}

/// Values every agent inherits when the config file omits them.
#[derive(Clone, Debug)]
pub struct AgentDefaults {
    pub provider: String,
    pub model: String,
    pub model_display: String,
    pub base_url: String,
    pub api_key: Option<String>,
    pub bin: PathBuf,
    pub ws_port: u16,
}

/// File-format variant of `AgentSpec`: every field optional so a minimal
/// TOML entry can inherit the rest from defaults.
#[derive(Deserialize)]
struct AgentSpecFile {
    name: String,
    #[serde(default)]
    kind: Option<AgentKind>,
    #[serde(default)]
    provider: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    model_display: Option<String>,
    #[serde(default)]
    base_url: Option<String>,
    #[serde(default)]
    api_key: Option<String>,
    #[serde(default)]
    bin: Option<PathBuf>,
    #[serde(default)]
    ws_port: Option<u16>,
    #[serde(default)]
    tool_approval: Option<ToolApproval>,
    #[serde(default)]
    workdir: Option<PathBuf>,
    #[serde(default)]
    mcp: Vec<McpServer>,
    #[serde(default)]
    cli_args: Vec<String>,
    #[serde(default)]
    cli_env: HashMap<String, String>,
    #[serde(default = "default_prompt_mode")]
    cli_prompt: PromptMode,
    #[serde(default = "default_cli_timeout")]
    cli_timeout_secs: u64,
}

#[derive(Deserialize)]
struct ConfigFile {
    #[serde(default)]
    agents: Vec<AgentSpecFile>,
}

/// Load and resolve agent specs.
///
/// `file = None` yields a single default "telos" spec. When `file` is Some
/// the TOML must parse and contain at least one agent. Resolved specs have
/// unique names and unique WebSocket ports among `telos`-kind agents.
pub fn load_config(
    file: Option<&Path>,
    defaults: &AgentDefaults,
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
            name: "telos".to_string(),
            kind: None,
            provider: None,
            model: None,
            model_display: None,
            base_url: None,
            api_key: None,
            bin: None,
            ws_port: None,
            tool_approval: None,
            workdir: None,
            mcp: Vec::new(),
            cli_args: Vec::new(),
            cli_env: HashMap::new(),
            cli_prompt: default_prompt_mode(),
            cli_timeout_secs: default_cli_timeout(),
        }],
    };

    if files.is_empty() {
        return Err("config: no agents defined".to_string());
    }

    let mut specs = Vec::with_capacity(files.len());
    let mut names = HashSet::new();
    let mut ports = HashMap::new();
    for f in files {
        if !names.insert(f.name.clone()) {
            return Err(format!("config: duplicate agent name '{}'", f.name));
        }
        let kind = f.kind.unwrap_or(AgentKind::Telos);
        let ws_port = f.ws_port.unwrap_or(defaults.ws_port);
        if kind == AgentKind::Telos {
            if let Some(prev) = ports.insert(ws_port, f.name.clone()) {
                return Err(format!(
                    "config: agents '{}' and '{}' share WebSocket port {}",
                    prev, f.name, ws_port
                ));
            }
        }
        let model_display = f.model_display.unwrap_or_else(|| {
            f.model
                .clone()
                .unwrap_or_else(|| defaults.model_display.clone())
        });
        specs.push(AgentSpec {
            name: f.name,
            kind,
            provider: f.provider.unwrap_or_else(|| defaults.provider.clone()),
            model: f.model.unwrap_or_else(|| defaults.model.clone()),
            model_display,
            base_url: f.base_url.unwrap_or_else(|| defaults.base_url.clone()),
            api_key: f.api_key.or_else(|| defaults.api_key.clone()),
            bin: f.bin.unwrap_or_else(|| defaults.bin.clone()),
            ws_port,
            tool_approval: f.tool_approval.unwrap_or(ToolApproval::Always),
            workdir: f.workdir,
            mcp: f.mcp,
            cli_args: f.cli_args,
            cli_env: f.cli_env,
            cli_prompt: f.cli_prompt,
            cli_timeout_secs: f.cli_timeout_secs,
        });
    }
    Ok(specs)
}

fn default_cli_timeout() -> u64 {
    300
}
