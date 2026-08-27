// Integration tests for agent config loading (issue #7).

use actus::agent::config::{load_config, AgentDefaults};
use actus::agent::AgentKind;
use std::path::PathBuf;

fn defaults() -> AgentDefaults {
    AgentDefaults {
        provider: "deepseek".to_string(),
        model: "deepseek-chat".to_string(),
        model_display: "deepseek-chat".to_string(),
        base_url: "https://api.deepseek.com/v1".to_string(),
        api_key: "sk-test".to_string(),
        bin: PathBuf::from("/bin/zed"),
        ws_port: 8080,
    }
}

fn write_config(dir: &std::path::Path, body: &str) -> std::path::PathBuf {
    let path = dir.join("config.toml");
    std::fs::write(&path, body).expect("write config");
    path
}

#[test]
fn no_file_yields_single_default_zed() {
    let specs = load_config(None, &defaults()).unwrap();
    assert_eq!(specs.len(), 1);
    assert_eq!(specs[0].name, "zed");
    assert_eq!(specs[0].kind, AgentKind::Zed);
    assert_eq!(specs[0].provider, "deepseek");
    assert_eq!(specs[0].model, "deepseek-chat");
    assert_eq!(specs[0].api_key, "sk-test");
    assert_eq!(specs[0].bin, PathBuf::from("/bin/zed"));
    assert_eq!(specs[0].ws_port, 8080);
}

#[test]
fn toml_fills_missing_fields_from_defaults() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_config(
        dir.path(),
        r#"
[[agents]]
name = "zed"
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

    // zed inherits everything from defaults
    assert_eq!(specs[0].kind, AgentKind::Zed);
    assert_eq!(specs[0].api_key, "sk-test");
    assert_eq!(specs[0].bin, PathBuf::from("/bin/zed"));

    // claude overrides provider/model/base_url, inherits api_key
    assert_eq!(specs[1].kind, AgentKind::LangGraph);
    assert_eq!(specs[1].provider, "anthropic");
    assert_eq!(specs[1].model, "claude-sonnet-4");
    assert_eq!(specs[1].model_display, "claude-sonnet-4");
    assert_eq!(specs[1].base_url, "https://api.anthropic.com/v1");
    assert_eq!(specs[1].api_key, "sk-test");
    assert_eq!(specs[1].ws_port, 8080);
}

#[test]
fn duplicate_ports_rejected_for_zed_kind() {
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
name = "zed"

[[agents]]
name = "zed"
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
