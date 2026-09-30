// The telos adapter's declaration: what an `[[agents]]` entry of kind
// `telos` says, resolved against the launcher's defaults.
//
// Everything here is one platform's business. The fabric carries an agent
// declaration as an unread options table (`AgentSpec`); this module is what
// turns that table into the concrete settings a launched telos agent starts
// from.

use std::collections::HashMap;
use std::path::PathBuf;

use serde::Deserialize;

use crate::agent::config::unquote_env_value;

/// Tool call approval policy for an agent. `Always` auto-approves every
/// tool call (headless task execution); `Ask` waits for a human or a
/// future approval bridge; `Never` rejects tool calls.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, serde::Serialize)]
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

/// One MCP (Model Context Protocol) server attached to a launched agent:
/// either a local stdio process (`command`/`args`/`env`) or a remote HTTP
/// endpoint (`url`/`headers`). It becomes a `context_servers` entry in the
/// agent's settings.
#[derive(Clone, Debug, Deserialize)]
pub struct McpServer {
    pub name: String,
    /// Whether the server starts with the agent. A declared server is on
    /// unless the declaration says otherwise, and a server that is off is
    /// still written to the agent's settings: it is in the agent's catalog,
    /// where the agent can turn it on itself.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Stdio transport: executable path.
    #[serde(default)]
    pub command: Option<String>,
    #[serde(default)]
    pub args: Vec<String>,
    /// Stdio transport: environment for the server process. A value of the
    /// form `$NAME` is resolved from the actus environment when the agent
    /// starts, so a token stays out of the config file.
    #[serde(default)]
    pub env: HashMap<String, String>,
    /// HTTP transport: remote MCP endpoint.
    #[serde(default)]
    pub url: Option<String>,
    /// HTTP transport: request headers. Values resolve like `env` above.
    #[serde(default)]
    pub headers: HashMap<String, String>,
    /// Tool call timeout in seconds (stdio only).
    #[serde(default)]
    pub timeout: Option<u64>,
}

fn default_true() -> bool {
    true
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
    /// Reasoning effort the model is asked for, already normalized.
    pub reasoning_effort: String,
}

/// Resolve the OpenAI-compatible endpoint from the local environment and
/// CLI flags. Precedence matches the original behavior: env wins over flags
/// for provider, base URL and reasoning effort; model and display name come
/// from the env only, with display falling back to the model. Missing values
/// stay empty so a later launch step can warn instead of inventing an
/// endpoint.
pub fn resolve_llm_settings(
    env_get: impl Fn(&str) -> Option<String>,
    provider_default: String,
    base_url_flag: Option<String>,
    reasoning_effort_flag: Option<String>,
) -> LlmSettings {
    let provider = unquote_env_value(&env_get("LLM_PROVIDER").unwrap_or(provider_default));
    let base_url = env_get("LLM_BASE_URL")
        .map(|v| unquote_env_value(&v))
        .or_else(|| base_url_flag.map(|v| unquote_env_value(&v)))
        .unwrap_or_default();
    let model = unquote_env_value(&env_get("LLM_MODEL").unwrap_or_default());
    let model_display =
        unquote_env_value(&env_get("LLM_MODEL_DISPLAY").unwrap_or_else(|| model.clone()));
    let reasoning_effort = env_get("LLM_REASONING_EFFORT")
        .map(|v| unquote_env_value(&v))
        .or_else(|| reasoning_effort_flag.map(|v| unquote_env_value(&v)))
        .unwrap_or_else(|| DEFAULT_REASONING_EFFORT.to_string());
    LlmSettings {
        provider,
        base_url,
        model,
        model_display,
        reasoning_effort,
    }
}

/// The reasoning effort an agent's model is asked for when nothing sets one.
///
/// An agent is more useful thinking, and the value is a default rather than a
/// claim about any model: `LLM_REASONING_EFFORT=none` (or the equivalent agent
/// config field) turns thinking off explicitly, and any other level moves it.
/// A deployment whose model rejects the parameter has to say `none`, which is
/// why the level is validated loudly rather than dropped.
pub const DEFAULT_REASONING_EFFORT: &str = "high";

/// The reasoning-effort levels a model entry may declare, lowercased as the
/// agent parses them.
pub const REASONING_EFFORT_LEVELS: [&str; 7] =
    ["none", "minimal", "low", "medium", "high", "xhigh", "max"];

/// Normalize a declared reasoning effort, or explain what is accepted.
///
/// The agent parses the value case-sensitively into its own enum and drops one
/// it cannot parse without a word, which would leave an agent silently thinking
/// at the provider's default. A typo here fails the launch instead.
pub fn normalize_reasoning_effort(value: &str) -> Result<String, String> {
    let normalized = unquote_env_value(value).to_lowercase();
    if REASONING_EFFORT_LEVELS.contains(&normalized.as_str()) {
        Ok(normalized)
    } else {
        Err(format!(
            "reasoning effort '{value}' is not one of: {}",
            REASONING_EFFORT_LEVELS.join(", ")
        ))
    }
}

/// Values every telos agent inherits when its declaration omits them. The
/// launcher resolves these from the CLI and the environment, which is where
/// the agent binary path and the LLM endpoint live.
#[derive(Clone, Debug)]
pub struct TelosDefaults {
    pub bin: PathBuf,
    pub ws_port: u16,
    pub provider: String,
    pub model: String,
    pub model_display: String,
    pub base_url: String,
    pub api_key: Option<String>,
    pub reasoning_effort: String,
}

/// The declaration of one telos agent, as written.
#[derive(Deserialize)]
pub struct TelosOptions {
    /// Telos binary path or PATH name.
    #[serde(default)]
    pub bin: Option<PathBuf>,
    /// WebSocket port the agent process connects back to.
    #[serde(default)]
    pub ws_port: Option<u16>,
    #[serde(default)]
    pub tool_approval: Option<ToolApproval>,
    #[serde(default)]
    pub provider: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub model_display: Option<String>,
    #[serde(default)]
    pub base_url: Option<String>,
    #[serde(default)]
    pub api_key: Option<String>,
    /// Reasoning effort this agent's model is asked for.
    #[serde(default)]
    pub reasoning_effort: Option<String>,
    #[serde(default)]
    pub mcp: Vec<McpServer>,
}

/// A telos declaration with every omitted field filled from the defaults and
/// every declared value validated.
#[derive(Clone, Debug)]
pub struct TelosSettings {
    pub name: String,
    pub bin: PathBuf,
    pub ws_port: u16,
    pub tool_approval: ToolApproval,
    pub provider: String,
    pub model: String,
    pub model_display: String,
    pub base_url: String,
    pub api_key: Option<String>,
    pub reasoning_effort: String,
    pub mcp: Vec<McpServer>,
}

impl TelosOptions {
    /// Fill every omitted field from the defaults and validate what is
    /// declared. A typo in the reasoning effort fails the launch here rather
    /// than leaving the agent thinking at the provider's default without a
    /// word.
    pub fn resolve(&self, name: &str, defaults: &TelosDefaults) -> Result<TelosSettings, String> {
        let declared_effort = self
            .reasoning_effort
            .clone()
            .unwrap_or_else(|| defaults.reasoning_effort.clone());
        let reasoning_effort = normalize_reasoning_effort(&declared_effort)
            .map_err(|e| format!("agent '{name}': {e}"))?;
        let model = self.model.clone().unwrap_or_else(|| defaults.model.clone());
        let model_display = self.model_display.clone().unwrap_or_else(|| {
            self.model
                .clone()
                .unwrap_or_else(|| defaults.model_display.clone())
        });
        Ok(TelosSettings {
            name: name.to_string(),
            bin: self.bin.clone().unwrap_or_else(|| defaults.bin.clone()),
            ws_port: self.ws_port.unwrap_or(defaults.ws_port),
            tool_approval: self.tool_approval.unwrap_or(ToolApproval::Always),
            provider: self
                .provider
                .clone()
                .unwrap_or_else(|| defaults.provider.clone()),
            model,
            model_display,
            base_url: self
                .base_url
                .clone()
                .unwrap_or_else(|| defaults.base_url.clone()),
            api_key: self.api_key.clone().or_else(|| defaults.api_key.clone()),
            reasoning_effort,
            mcp: self.mcp.clone(),
        })
    }
}
