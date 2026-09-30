// The telos adapter's declaration surface (issue #36): the options a
// declaration carries, the defaults they resolve against, and the
// validation its factory runs before anything launches.

use std::path::PathBuf;
use std::sync::Arc;

use actus::agent::adapter::FactoryRegistry;
use actus::agent::config::load_config;
use actus::agent::ext_cli::ExtCliFactory;
use actus::telos::adapter::TelosFactory;
use actus::telos::options::{
    normalize_reasoning_effort, resolve_llm_settings, TelosDefaults, TelosSettings, ToolApproval,
    DEFAULT_REASONING_EFFORT, REASONING_EFFORT_LEVELS,
};

fn defaults() -> TelosDefaults {
    TelosDefaults {
        bin: PathBuf::from("/bin/telos"),
        ws_port: 8080,
        provider: "openai-compatible".to_string(),
        model: "example-model".to_string(),
        model_display: "example-model".to_string(),
        base_url: "https://api.example.com/v1".to_string(),
        api_key: Some("sk-test".to_string()),
        reasoning_effort: DEFAULT_REASONING_EFFORT.to_string(),
    }
}

/// The telos kind plus a second kind, so a test can prove the port check is
/// the telos adapter's own and not a fabric-wide rule.
fn registry() -> FactoryRegistry {
    let mut factories = FactoryRegistry::new();
    factories.register_default(Arc::new(TelosFactory::new(defaults())));
    factories.register(Arc::new(ExtCliFactory));
    factories
}

fn write_config(dir: &std::path::Path, body: &str) -> std::path::PathBuf {
    let path = dir.join("config.toml");
    std::fs::write(&path, body).expect("write config");
    path
}

/// Load a config file and resolve every spec the way a launch does.
fn resolve_all(cfg: &std::path::Path) -> Result<Vec<TelosSettings>, String> {
    let specs = load_config(Some(cfg), &registry())?;
    let factory = TelosFactory::new(defaults());
    specs.iter().map(|spec| factory.resolve(spec)).collect()
}

#[test]
fn a_default_agent_inherits_every_default() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(dir.path(), "[[agents]]\nname = \"telos\"\n");

    let resolved = resolve_all(&cfg).unwrap();
    assert_eq!(resolved.len(), 1);
    let s = &resolved[0];
    assert_eq!(s.name, "telos");
    assert_eq!(s.bin, PathBuf::from("/bin/telos"));
    assert_eq!(s.ws_port, 8080);
    assert_eq!(s.tool_approval, ToolApproval::Always);
    assert_eq!(s.provider, "openai-compatible");
    assert_eq!(s.model, "example-model");
    assert_eq!(s.model_display, "example-model");
    assert_eq!(s.base_url, "https://api.example.com/v1");
    assert_eq!(s.api_key.as_deref(), Some("sk-test"));
    assert_eq!(s.reasoning_effort, DEFAULT_REASONING_EFFORT);
    assert!(s.mcp.is_empty());
}

#[test]
fn a_declared_field_wins_over_the_default() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(
        dir.path(),
        r#"
[[agents]]
name = "telos"
ws_port = 8081
tool_approval = "ask"
provider = "anthropic"
model = "claude-sonnet-4"
model_display = "Claude Sonnet 4"
base_url = "https://api.anthropic.com/v1"
api_key = "sk-own"
reasoning_effort = "max"
"#,
    );

    let resolved = resolve_all(&cfg).unwrap();
    let s = &resolved[0];
    assert_eq!(s.ws_port, 8081);
    assert_eq!(s.tool_approval, ToolApproval::Ask);
    assert_eq!(s.provider, "anthropic");
    assert_eq!(s.model, "claude-sonnet-4");
    assert_eq!(s.model_display, "Claude Sonnet 4");
    assert_eq!(s.base_url, "https://api.anthropic.com/v1");
    assert_eq!(s.api_key.as_deref(), Some("sk-own"));
    assert_eq!(s.reasoning_effort, "max");
}

#[test]
fn a_declared_model_is_its_own_display_name_when_no_display_is_given() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(
        dir.path(),
        "[[agents]]\nname = \"telos\"\nmodel = \"model-b\"\n",
    );
    let resolved = resolve_all(&cfg).unwrap();
    assert_eq!(resolved[0].model_display, "model-b");
}

#[test]
fn tool_approval_modes_are_read_per_agent() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(
        dir.path(),
        r#"
[[agents]]
name = "never"
tool_approval = "never"
ws_port = 8080

[[agents]]
name = "asker"
tool_approval = "ask"
ws_port = 8081
"#,
    );
    let resolved = resolve_all(&cfg).unwrap();
    assert_eq!(resolved[0].tool_approval, ToolApproval::Never);
    assert_eq!(resolved[1].tool_approval, ToolApproval::Ask);
}

#[test]
fn mcp_servers_parse_stdio_and_http() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(
        dir.path(),
        r#"
[[agents]]
name = "telos"

[[agents.mcp]]
name = "filesystem"
command = "npx"
args = ["-y", "@modelcontextprotocol/server-filesystem", "./"]
env = { "FOO" = "bar" }

[[agents.mcp]]
name = "cloudflare-api"
url = "https://mcp.cloudflare.com/mcp"
"#,
    );
    let resolved = resolve_all(&cfg).unwrap();
    let mcp = &resolved[0].mcp;
    assert_eq!(mcp.len(), 2);

    let fs = &mcp[0];
    assert_eq!(fs.name, "filesystem");
    assert_eq!(fs.command.as_deref(), Some("npx"));
    assert_eq!(fs.args.len(), 3);
    assert_eq!(fs.env.get("FOO").map(String::as_str), Some("bar"));
    assert!(fs.url.is_none());
    assert!(
        fs.enabled,
        "a declared server is on unless it says otherwise"
    );

    let cf = &mcp[1];
    assert_eq!(cf.name, "cloudflare-api");
    assert_eq!(cf.url.as_deref(), Some("https://mcp.cloudflare.com/mcp"));
    assert!(cf.command.is_none());
}

/// Every level an operator may declare is accepted, normalized, and survives
/// the round trip the agent parses.
#[test]
fn reasoning_effort_levels_are_accepted_and_normalized() {
    for level in REASONING_EFFORT_LEVELS {
        assert_eq!(normalize_reasoning_effort(level).unwrap(), level);
        assert_eq!(
            normalize_reasoning_effort(&level.to_uppercase()).unwrap(),
            level,
            "a level typed in upper case is the same level"
        );
        assert_eq!(
            normalize_reasoning_effort(&format!("\"{level}\"")).unwrap(),
            level,
            "a quoted value from a .env file is the same level"
        );
    }
}

/// A typo fails the load rather than leaving the agent thinking at the
/// provider's default without a word.
#[test]
fn an_unknown_reasoning_effort_is_refused() {
    let error = normalize_reasoning_effort("highest").unwrap_err();
    assert!(error.contains("highest"), "{error}");
    assert!(
        error.contains("high"),
        "the accepted levels are named: {error}"
    );

    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(
        dir.path(),
        "[[agents]]\nname = \"telos\"\nreasoning_effort = \"fast\"\n",
    );
    let error = load_config(Some(&cfg), &registry()).unwrap_err();
    assert!(error.contains("agent 'telos'"), "{error}");
    assert!(error.contains("fast"), "{error}");
}

/// A per-agent level wins over the default, and the default reaches every
/// agent that does not declare one.
#[test]
fn reasoning_effort_falls_back_to_the_default_and_is_overridable() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(
        dir.path(),
        "[[agents]]\nname = \"plain\"\n\n\
         [[agents]]\nname = \"raising\"\nws_port = 8081\nreasoning_effort = \"max\"\n",
    );
    let resolved = resolve_all(&cfg).unwrap();
    assert_eq!(resolved[0].reasoning_effort, DEFAULT_REASONING_EFFORT);
    assert_eq!(resolved[1].reasoning_effort, "max");
}

#[test]
fn two_telos_agents_on_one_port_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(
        dir.path(),
        r#"
[[agents]]
name = "a"
ws_port = 8080

[[agents]]
name = "b"
ws_port = 8080
"#,
    );
    let err = load_config(Some(&cfg), &registry()).unwrap_err();
    assert!(err.contains("share WebSocket port"), "unexpected: {}", err);
}

/// The port is the telos adapter's resource. A declaration of another kind
/// carrying the same number is not a collision, because nothing else binds
/// it.
#[test]
fn another_kind_is_not_checked_against_the_telos_port() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(
        dir.path(),
        r#"
[[agents]]
name = "aux"
kind = "ext_cli"
bin = "ante"
ws_port = 8080

[[agents]]
name = "telos"
ws_port = 8080
"#,
    );
    let specs = load_config(Some(&cfg), &registry()).unwrap();
    assert_eq!(specs.len(), 2);
}

#[test]
fn llm_settings_resolution_env_wins_and_unquotes() {
    let env = |key: &str| -> Option<String> {
        match key {
            "LLM_PROVIDER" => Some("\"openai\"".to_string()),
            "LLM_BASE_URL" => Some("'https://api.openai.com'".to_string()),
            "LLM_MODEL" => Some("\"llm_model\"".to_string()),
            _ => None,
        }
    };
    let resolved = resolve_llm_settings(
        env,
        "openai-compatible".to_string(),
        Some("https://flag.example/v1".to_string()),
        None,
    );
    // The env values win over the flag and the default, and surrounding
    // quotes are stripped (issue #16 parity: local env drives the LLM).
    assert_eq!(resolved.provider, "openai");
    assert_eq!(resolved.base_url, "https://api.openai.com");
    assert_eq!(resolved.model, "llm_model");
    assert_eq!(resolved.model_display, "llm_model");
}

#[test]
fn llm_settings_resolution_defaults_and_display_fallback() {
    // No env: the neutral provider default stays, the flag base URL is
    // used, and model values stay empty (no vendor model invented).
    let resolved = resolve_llm_settings(
        |_| None,
        "openai-compatible".to_string(),
        Some("https://flag.example/v1".to_string()),
        None,
    );
    assert_eq!(resolved.provider, "openai-compatible");
    assert_eq!(resolved.base_url, "https://flag.example/v1");
    assert_eq!(resolved.model, "");
    assert_eq!(resolved.model_display, "");
    assert_eq!(resolved.reasoning_effort, DEFAULT_REASONING_EFFORT);

    // An explicit display name wins over the model fallback.
    let env = |key: &str| -> Option<String> {
        match key {
            "LLM_MODEL" => Some("model-a".to_string()),
            "LLM_MODEL_DISPLAY" => Some("Model A".to_string()),
            _ => None,
        }
    };
    let resolved = resolve_llm_settings(env, "openai-compatible".to_string(), None, None);
    assert_eq!(resolved.model, "model-a");
    assert_eq!(resolved.model_display, "Model A");
}
