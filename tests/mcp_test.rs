// The MCP declaration surface actus owns (issue #7, re-scoped in #56).
//
// actus parses the declaration and decides what the absence of an `enabled` means. What the
// executor's settings file looks like from there belongs to the deployment that runs that
// executor, and it is tested where that writer lives; what actus passes to the writer, and
// the resolution of a name a declaration reads from the environment, are in
// `settings_test.rs`.

use actus::agent::config::{load_config, AgentDefaults, DEFAULT_REASONING_EFFORT};
use std::path::PathBuf;

fn defaults() -> AgentDefaults {
    AgentDefaults {
        provider: "openai-compatible".to_string(),
        model: "example-model".to_string(),
        model_display: "example-model".to_string(),
        base_url: "https://api.example.com/v1".to_string(),
        api_key: Some("sk-test".to_string()),
        bin: Some(PathBuf::from("/bin/telos")),
        ws_port: 8080,
        reasoning_effort: DEFAULT_REASONING_EFFORT.to_string(),
    }
}

/// The production six-server TOML, token redacted.
const SIX_SERVER_TOML: &str = r#"
[[agents]]
name = "telos"
ws_port = 8080

[[agents.mcp]]
name = "memory"
command = "npx"
args = ["-y", "@modelcontextprotocol/server-memory"]

[[agents.mcp]]
name = "sequentialthinking"
command = "npx"
args = ["-y", "@modelcontextprotocol/server-sequential-thinking"]

[[agents.mcp]]
name = "filesystem"
command = "npx"
args = ["-y", "@modelcontextprotocol/server-filesystem", "./"]

[[agents.mcp]]
name = "context7"
command = "npx"
args = ["-y", "@upstash/context7-mcp"]
env = { "DEFAULT_MINIMUM_TOKENS" = "" }

[[agents.mcp]]
name = "cloudflare-api"
url = "https://mcp.cloudflare.com/mcp"

[[agents.mcp]]
name = "mcp-server-github"
url = "https://api.githubcopilot.com/mcp/"
headers = { "Authorization" = "Bearer <PAT>" }
"#;

#[test]
fn six_server_config_parses() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.toml");
    std::fs::write(&path, SIX_SERVER_TOML).unwrap();

    let specs = load_config(Some(&path), &defaults()).unwrap();
    assert_eq!(specs.len(), 1);
    let mcp = &specs[0].mcp;
    assert_eq!(mcp.len(), 6);

    let by_name = |n: &str| mcp.iter().find(|m| m.name == n).expect(n);

    // stdio servers
    let memory = by_name("memory");
    assert_eq!(memory.command.as_deref(), Some("npx"));
    assert!(memory.url.is_none());
    assert!(memory.args.iter().any(|a| a.contains("server-memory")));

    let seq = by_name("sequentialthinking");
    assert!(seq
        .args
        .iter()
        .any(|a| a.contains("server-sequential-thinking")));

    let fs = by_name("filesystem");
    assert!(fs.args.iter().any(|a| a == "./"));

    let ctx7 = by_name("context7");
    assert_eq!(
        ctx7.env.get("DEFAULT_MINIMUM_TOKENS").map(String::as_str),
        Some("")
    );

    // http servers
    let cf = by_name("cloudflare-api");
    assert_eq!(cf.url.as_deref(), Some("https://mcp.cloudflare.com/mcp"));
    assert!(cf.command.is_none());

    let gh = by_name("mcp-server-github");
    assert_eq!(
        gh.url.as_deref(),
        Some("https://api.githubcopilot.com/mcp/")
    );
    assert_eq!(
        gh.headers.get("Authorization").map(String::as_str),
        Some("Bearer <PAT>")
    );
}

/// The catalogue an agent can turn on is what the operator declared, and a server the
/// declaration marks off is still in it: the agent's own `enable_context_server` tool flips
/// an entry that is already there, so an entry that is absent is a capability the agent
/// cannot reach.
#[test]
fn a_declared_server_is_on_unless_the_declaration_says_otherwise() {
    const TOML: &str = r#"
[[agents]]
name = "telos"
ws_port = 8080

[[agents.mcp]]
name = "memory"
command = "npx"
args = ["-y", "@modelcontextprotocol/server-memory"]

[[agents.mcp]]
name = "mcp-server-github"
enabled = false
command = "npx"
args = ["-y", "@modelcontextprotocol/server-github"]
"#;

    let dir = tempfile::tempdir().unwrap();
    let cfg = dir.path().join("config.toml");
    std::fs::write(&cfg, TOML).unwrap();
    let spec = load_config(Some(&cfg), &defaults()).unwrap().remove(0);
    assert!(
        spec.mcp.iter().find(|s| s.name == "memory").unwrap().enabled,
        "a server the declaration does not mention is on"
    );
    assert!(
        !spec
            .mcp
            .iter()
            .find(|s| s.name == "mcp-server-github")
            .unwrap()
            .enabled,
        "a declaration may put a server in the catalogue without starting it"
    );
}
