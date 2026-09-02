// Agent configuration — static declaration of the platforms the fabric
// weaves. Agents live in `~/.actus/config.toml` (or `ACTUS_CONFIG`); when
// the file is absent a single default "telos" agent is derived from the CLI
// and environment.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::agent::AgentKind;

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
    pub api_key: String,
    /// Telos binary path.
    pub bin: PathBuf,
    /// WebSocket port the agent process connects back to.
    pub ws_port: u16,
    /// Tool call approval policy; drives the fork's TELOS_TOOL_APPROVAL env.
    pub tool_approval: ToolApproval,
    /// MCP servers attached to this agent.
    pub mcp: Vec<McpServer>,
}

/// Values every agent inherits when the config file omits them.
#[derive(Clone, Debug)]
pub struct AgentDefaults {
    pub provider: String,
    pub model: String,
    pub model_display: String,
    pub base_url: String,
    pub api_key: String,
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
    mcp: Vec<McpServer>,
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
pub fn load_config(file: Option<&Path>, defaults: &AgentDefaults) -> Result<Vec<AgentSpec>, String> {
    let files: Vec<AgentSpecFile> = match file {
        Some(p) => {
            let text = std::fs::read_to_string(p)
                .map_err(|e| format!("cannot read {}: {}", p.display(), e))?;
            let cfg: ConfigFile =
                toml::from_str(&text).map_err(|e| format!("cannot parse {}: {}", p.display(), e))?;
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
            mcp: Vec::new(),
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
            api_key: f.api_key.unwrap_or_else(|| defaults.api_key.clone()),
            bin: f.bin.unwrap_or_else(|| defaults.bin.clone()),
            ws_port,
            tool_approval: f.tool_approval.unwrap_or(ToolApproval::Always),
            mcp: f.mcp,
        });
    }
    Ok(specs)
}
