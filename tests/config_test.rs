// Integration tests for agent config loading (issue #7).

use actus::agent::config::{
    load_config, load_control_policy, resolve_llm_settings, AgentDefaults, PromptMode, ToolApproval,
};
use actus::agent::AgentKind;
use std::path::PathBuf;

fn defaults() -> AgentDefaults {
    AgentDefaults {
        provider: "openai-compatible".to_string(),
        model: "example-model".to_string(),
        model_display: "example-model".to_string(),
        base_url: "https://api.example.com/v1".to_string(),
        api_key: Some("sk-test".to_string()),
        bin: PathBuf::from("/bin/telos"),
        ws_port: 8080,
    }
}

fn write_config(dir: &std::path::Path, body: &str) -> std::path::PathBuf {
    let path = dir.join("config.toml");
    std::fs::write(&path, body).expect("write config");
    path
}

#[test]
fn no_file_yields_single_default_telos() {
    let specs = load_config(None, &defaults()).unwrap();
    assert_eq!(specs.len(), 1);
    assert_eq!(specs[0].name, "telos");
    assert_eq!(specs[0].kind, AgentKind::Telos);
    assert_eq!(specs[0].provider, "openai-compatible");
    assert_eq!(specs[0].model, "example-model");
    assert_eq!(specs[0].api_key.as_deref(), Some("sk-test"));
    assert_eq!(specs[0].bin, PathBuf::from("/bin/telos"));
    assert_eq!(specs[0].ws_port, 8080);
    assert_eq!(specs[0].tool_approval, ToolApproval::Always);
    assert!(specs[0].workdir.is_none());
}

#[test]
fn llm_settings_resolution_env_wins_and_unquotes() {
    let env = |key: &str| -> Option<String> {
        match key {
            "LLM_PROVIDER" => Some("\"deepseek\"".to_string()),
            "LLM_BASE_URL" => Some("'https://api.deepseek.com'".to_string()),
            "LLM_MODEL" => Some("\"deepseek-v4-flash\"".to_string()),
            _ => None,
        }
    };
    let resolved = resolve_llm_settings(
        env,
        "openai-compatible".to_string(),
        Some("https://flag.example/v1".to_string()),
    );
    // The env values win over the flag and the default, and surrounding
    // quotes are stripped (issue #16 parity: local env drives the LLM).
    assert_eq!(resolved.provider, "deepseek");
    assert_eq!(resolved.base_url, "https://api.deepseek.com");
    assert_eq!(resolved.model, "deepseek-v4-flash");
    assert_eq!(resolved.model_display, "deepseek-v4-flash");
}

#[test]
fn llm_settings_resolution_defaults_and_display_fallback() {
    // No env: the neutral provider default stays, the flag base URL is
    // used, and model values stay empty (no vendor model invented).
    let resolved = resolve_llm_settings(
        |_| None,
        "openai-compatible".to_string(),
        Some("https://flag.example/v1".to_string()),
    );
    assert_eq!(resolved.provider, "openai-compatible");
    assert_eq!(resolved.base_url, "https://flag.example/v1");
    assert_eq!(resolved.model, "");
    assert_eq!(resolved.model_display, "");

    // An explicit display name wins over the model fallback.
    let env = |key: &str| -> Option<String> {
        match key {
            "LLM_MODEL" => Some("model-a".to_string()),
            "LLM_MODEL_DISPLAY" => Some("Model A".to_string()),
            _ => None,
        }
    };
    let resolved = resolve_llm_settings(env, "openai-compatible".to_string(), None);
    assert_eq!(resolved.model, "model-a");
    assert_eq!(resolved.model_display, "Model A");
}

#[test]
fn control_policy_parsed_and_applied() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_config(
        dir.path(),
        r#"
[agent-control]
allow = [
  { controller = "telos", targets = ["ante", "aura"] },
  { controller = "meta", targets = ["*"] },
  { controller = "*", targets = ["audit"] },
]
"#,
    );
    let policy = load_control_policy(Some(&path)).unwrap();
    assert!(policy.allows("telos", "ante"));
    assert!(policy.allows("telos", "aura"));
    assert!(!policy.allows("telos", "aux"));
    assert!(policy.allows("meta", "anything"));
    assert!(policy.allows("other", "audit"));
    // A controller can never dispatch to itself, even when listed.
    assert!(!policy.allows("telos", "telos"));
}

#[test]
fn control_policy_defaults_to_deny() {
    let dir = tempfile::tempdir().unwrap();
    // A config file without an [agent-control] section denies everything.
    let path = write_config(dir.path(), "");
    let policy = load_control_policy(Some(&path)).unwrap();
    assert!(!policy.allows("telos", "ante"));
    // No config file at all denies everything too.
    let policy = load_control_policy(None).unwrap();
    assert!(!policy.allows("any", "any"));
}

#[test]
fn ext_cli_profile_fields_parsed() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_config(
        dir.path(),
        r#"
[[agents]]
name = "aux"
kind = "ext_cli"
bin = "ante"
cli_args = ["-p", "{prompt}"]
cli_env = { "FOO" = "bar", "KEY" = "$LLM_API_KEY" }
cli_prompt = "stdin"
cli_timeout_secs = 42
workdir = "/tmp/aux-work"
"#,
    );
    let specs = load_config(Some(&path), &defaults()).unwrap();
    assert_eq!(specs.len(), 1);
    let s = &specs[0];
    assert_eq!(s.name, "aux");
    assert_eq!(s.kind, AgentKind::ExtCli);
    assert_eq!(s.bin, PathBuf::from("ante"));
    assert_eq!(s.cli_args, vec!["-p".to_string(), "{prompt}".to_string()]);
    assert_eq!(s.cli_env.get("FOO").map(String::as_str), Some("bar"));
    assert_eq!(
        s.cli_env.get("KEY").map(String::as_str),
        Some("$LLM_API_KEY")
    );
    assert_eq!(s.cli_prompt, PromptMode::Stdin);
    assert_eq!(s.cli_timeout_secs, 42);
    assert_eq!(
        s.workdir.as_deref(),
        Some(std::path::Path::new("/tmp/aux-work"))
    );
}

#[test]
fn ext_cli_profile_defaults_when_fields_omitted() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_config(
        dir.path(),
        r#"
[[agents]]
name = "aux"
kind = "ext_cli"
bin = "some-cli"
"#,
    );
    let specs = load_config(Some(&path), &defaults()).unwrap();
    let s = &specs[0];
    assert!(s.cli_args.is_empty());
    assert!(s.cli_env.is_empty());
    assert_eq!(s.cli_prompt, PromptMode::Arg);
    assert_eq!(s.cli_timeout_secs, 300);
    assert!(s.workdir.is_none());
}

#[test]
fn tool_approval_modes_parsed() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_config(
        dir.path(),
        r#"
[[agents]]
name = "telos"
tool_approval = "never"
ws_port = 8080

[[agents]]
name = "asker"
tool_approval = "ask"
ws_port = 8081
"#,
    );
    let specs = load_config(Some(&path), &defaults()).unwrap();
    assert_eq!(specs[0].tool_approval, ToolApproval::Never);
    assert_eq!(specs[1].tool_approval, ToolApproval::Ask);
}

#[test]
fn toml_fills_missing_fields_from_defaults() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_config(
        dir.path(),
        r#"
[[agents]]
name = "telos"
ws_port = 8080

[[agents]]
name = "claude"
kind = "langgraph"
provider = "anthropic"
model = "claude-sonnet-4"
base_url = "https://api.anthropic.com/v1"
"#,
    );
    let specs = load_config(Some(&path), &defaults()).unwrap();
    assert_eq!(specs.len(), 2);

    // telos inherits everything from defaults
    assert_eq!(specs[0].kind, AgentKind::Telos);
    assert_eq!(specs[0].api_key.as_deref(), Some("sk-test"));
    assert_eq!(specs[0].bin, PathBuf::from("/bin/telos"));

    // claude overrides provider/model/base_url, inherits api_key
    assert_eq!(specs[1].kind, AgentKind::LangGraph);
    assert_eq!(specs[1].provider, "anthropic");
    assert_eq!(specs[1].model, "claude-sonnet-4");
    assert_eq!(specs[1].model_display, "claude-sonnet-4");
    assert_eq!(specs[1].base_url, "https://api.anthropic.com/v1");
    assert_eq!(specs[1].api_key.as_deref(), Some("sk-test"));
    assert_eq!(specs[1].ws_port, 8080);
}

#[test]
fn duplicate_ports_rejected_for_telos_kind() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_config(
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
    let err = load_config(Some(&path), &defaults()).unwrap_err();
    assert!(err.contains("share WebSocket port"), "unexpected: {}", err);
}

#[test]
fn duplicate_names_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_config(
        dir.path(),
        r#"
[[agents]]
name = "telos"

[[agents]]
name = "telos"
"#,
    );
    let err = load_config(Some(&path), &defaults()).unwrap_err();
    assert!(err.contains("duplicate agent name"), "unexpected: {}", err);
}

#[test]
fn empty_agents_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_config(dir.path(), "");
    let err = load_config(Some(&path), &defaults()).unwrap_err();
    assert!(err.contains("no agents"), "unexpected: {}", err);
}

#[test]
fn malformed_toml_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_config(dir.path(), "not [ valid toml");
    let err = load_config(Some(&path), &defaults()).unwrap_err();
    assert!(err.contains("cannot parse"), "unexpected: {}", err);
}

#[test]
fn mcp_servers_parsed_stdio_and_http() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_config(
        dir.path(),
        r#"
[[agents]]
name = "telos"
ws_port = 8080

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
    let specs = load_config(Some(&path), &defaults()).unwrap();
    assert_eq!(specs.len(), 1);
    assert_eq!(specs[0].mcp.len(), 2);

    let fs = &specs[0].mcp[0];
    assert_eq!(fs.name, "filesystem");
    assert_eq!(fs.command.as_deref(), Some("npx"));
    assert_eq!(fs.args.len(), 3);
    assert_eq!(fs.env.get("FOO").map(String::as_str), Some("bar"));
    assert!(fs.url.is_none());

    let cf = &specs[0].mcp[1];
    assert_eq!(cf.name, "cloudflare-api");
    assert_eq!(cf.url.as_deref(), Some("https://mcp.cloudflare.com/mcp"));
    assert!(cf.command.is_none());
}

#[test]
fn no_mcp_by_default() {
    let specs = load_config(None, &defaults()).unwrap();
    assert!(specs[0].mcp.is_empty());
}
