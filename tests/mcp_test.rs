// MCP coverage for the production six-server set (issue #7).
//
// Mirrors the servers configured in the user's Telos IDE:
//   stdio: memory, sequentialthinking, filesystem, context7
//   http:  cloudflare-api, mcp-server-github
// Unit tests cover config parsing and settings injection; the scenario
// test proves an injected stdio entry spawns a working MCP server.

use actus::agent::config::{load_config, AgentDefaults, McpServer};
use actus::telos::ensure_telos_settings;
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

#[test]
fn six_server_settings_injection_schema() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path();

    let specs = {
        let cfg = dir.path().join("config.toml");
        std::fs::write(&cfg, SIX_SERVER_TOML).unwrap();
        load_config(Some(&cfg), &defaults()).unwrap()
    };
    ensure_telos_settings(
        data_dir,
        Some("sk-test"),
        "openai-compatible",
        "https://api.example.com/v1",
        "example-model",
        "example-model",
        &specs[0].mcp,
    )
    .unwrap();

    let settings: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(data_dir.join("config/settings.json")).unwrap(),
    )
    .unwrap();
    let servers = settings
        .get("context_servers")
        .unwrap()
        .as_object()
        .unwrap();
    assert_eq!(servers.len(), 6);

    // stdio entries carry command/args; context7 also carries env
    for name in ["memory", "sequentialthinking", "filesystem"] {
        let e = servers.get(name).unwrap();
        assert!(e.get("command").is_some(), "{name} must be stdio");
        assert!(e.get("url").is_none(), "{name} must not be http");
    }
    assert!(servers.get("context7").unwrap().get("env").is_some());

    // http entries carry url; github also carries headers
    let cf = servers.get("cloudflare-api").unwrap();
    assert!(cf.get("url").is_some());
    assert!(cf.get("command").is_none());
    let gh = servers.get("mcp-server-github").unwrap();
    assert_eq!(
        gh.get("headers").unwrap().get("Authorization").unwrap(),
        "Bearer <PAT>"
    );

    // The OpenAI-compatible endpoint entry carries the resolved label,
    // base URL, and model (issue #16 fixtures: no vendor names).
    let entry = &settings["language_models"]["openai_compatible"]["openai-compatible"];
    assert_eq!(entry["api_url"], "https://api.example.com/v1");
    let model = &entry["available_models"][0];
    assert_eq!(model["name"], "example-model");
    assert_eq!(model["display_name"], "example-model");
    assert_eq!(model["tool_use"], true);
}

#[test]
fn settings_injection_skipped_without_endpoint_config() {
    // With no base URL or model (nothing set in the local environment or
    // the agent config), actus writes no language_models entry: it ships
    // no LLM endpoint of its own. Credentials stay written so an agent
    // whose own built-in provider matches the label can still resolve the
    // key.
    let dir = tempfile::tempdir().unwrap();
    ensure_telos_settings(
        dir.path(),
        Some("sk-test"),
        "openai-compatible",
        "",
        "",
        "",
        &[],
    )
    .unwrap();

    let settings: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(dir.path().join("config/settings.json")).unwrap(),
    )
    .unwrap();
    assert!(
        settings
            .get("language_models")
            .and_then(|lm| lm.get("openai_compatible"))
            .is_none(),
        "no provider entry expected without an endpoint: {settings}"
    );

    let creds: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(dir.path().join("credentials/credentials.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(creds["provider/openai-compatible"]["api_key"], "sk-test");
}

/// A minimal stdio MCP server used to prove injected configs spawn
/// working servers without the fork binary.
const STUB_MCP_SERVER: &str = r#"
import json, sys
for line in sys.stdin:
    try:
        req = json.loads(line)
    except Exception:
        continue
    method = req.get("method")
    if method == "initialize":
        result = {"protocolVersion": "2024-11-05", "capabilities": {"tools": {}}, "serverInfo": {"name": "stub", "version": "1.0"}}
    elif method == "tools/list":
        result = {"tools": [{"name": "echo", "description": "echo", "inputSchema": {"type": "object", "properties": {}}}]}
    elif method == "tools/call":
        result = {"content": [{"type": "text", "text": "pong"}]}
    else:
        result = {}
    sys.stdout.write(json.dumps({"jsonrpc": "2.0", "id": req.get("id"), "result": result}) + "\n")
    sys.stdout.flush()
"#;

fn mcp_rpc(
    server: &mut std::process::Child,
    method: &str,
    params: Option<serde_json::Value>,
) -> serde_json::Value {
    use std::io::{BufRead, Write};
    let stdin = server.stdin.as_mut().unwrap();
    let stdout = server.stdout.as_mut().unwrap();
    let mut reader = std::io::BufReader::new(stdout);

    writeln!(
        stdin,
        "{}",
        serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params.unwrap_or(serde_json::json!({}))})
    )
    .unwrap();
    stdin.flush().unwrap();

    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    serde_json::from_str(&line).unwrap()
}

#[test]
fn scenario_injected_stdio_server_is_spawnable() {
    // A python3 interpreter is required for the scenario.
    if std::process::Command::new("python3")
        .arg("--version")
        .output()
        .is_err()
    {
        eprintln!("skipping: python3 not available");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let server_script = dir.path().join("stub_mcp.py");
    std::fs::write(&server_script, STUB_MCP_SERVER).unwrap();

    // Inject a stdio MCP entry pointing at the stub server.
    let mcp = vec![McpServer {
        name: "stub".to_string(),
        command: Some("python3".to_string()),
        args: vec![server_script.to_string_lossy().to_string()],
        env: Default::default(),
        url: None,
        headers: Default::default(),
        timeout: None,
    }];
    ensure_telos_settings(
        dir.path(),
        Some("sk-test"),
        "openai-compatible",
        "https://api.example.com/v1",
        "example-model",
        "example-model",
        &mcp,
    )
    .unwrap();

    // Read the injected entry back and spawn it exactly as configured.
    let settings: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(dir.path().join("config/settings.json")).unwrap(),
    )
    .unwrap();
    let entry = &settings["context_servers"]["stub"];
    let command = entry["command"].as_str().unwrap();
    let args: Vec<String> = entry["args"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a.as_str().unwrap().to_string())
        .collect();

    let mut server = std::process::Command::new(command)
        .args(&args)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("injected command must spawn");

    let init = mcp_rpc(&mut server, "initialize", None);
    assert_eq!(init["result"]["serverInfo"]["name"], "stub");

    let tools = mcp_rpc(&mut server, "tools/list", None);
    assert_eq!(tools["result"]["tools"][0]["name"], "echo");

    let call = mcp_rpc(
        &mut server,
        "tools/call",
        Some(serde_json::json!({"name": "echo", "arguments": {}})),
    );
    assert_eq!(call["result"]["content"][0]["text"], "pong");

    server.kill().ok();
}
