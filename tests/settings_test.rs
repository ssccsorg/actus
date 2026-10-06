// The settings step between a parsed declaration and a launched executor (issue #56).
//
// The settings file is the executor's format, so the writer belongs to the deployment that
// runs that executor and actus carries none. What actus owns is the value: a `$NAME` in a
// declared context server names something this process holds, so every reference is
// resolved before the writer runs, a name that is not set is refused at the launch rather
// than written through as text, and a composition that names no writer is refused by name
// rather than starting a program whose settings nobody wrote.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use actus::acpws::{write_settings, SettingsWriter};
use actus::agent::config::{load_config, AgentDefaults, AgentSpec, McpServer};

/// The name a declaration below resolves, so the resolution has something to find. The
/// other name is set by nothing, which is the case a server that is off keeps.
const TOKEN_VAR: &str = "ACTUS_SETTINGS_TEST_TOKEN";
const UNSET_VAR: &str = "ACTUS_SETTINGS_TEST_UNSET";

fn defaults() -> AgentDefaults {
    AgentDefaults {
        provider: "openai-compatible".to_string(),
        model: "example-model".to_string(),
        model_display: "example-model".to_string(),
        base_url: "https://api.example.com/v1".to_string(),
        api_key: Some("sk-test".to_string()),
        bin: Some(PathBuf::from("/bin/tel")),
        ws_port: 8080,
        reasoning_effort: "high".to_string(),
    }
}

fn spec_from(toml: &str) -> AgentSpec {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let cfg = dir.path().join("config.toml");
    std::fs::write(&cfg, toml).expect("the config is written");
    load_config(Some(&cfg), &defaults())
        .expect("the config loads")
        .remove(0)
}

fn declared(name: &str, enabled: bool, env: &[(&str, &str)]) -> McpServer {
    McpServer {
        name: name.to_string(),
        enabled,
        command: Some("npx".to_string()),
        args: vec!["-y".to_string(), format!("server-{name}")],
        env: env
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect(),
        url: None,
        headers: HashMap::new(),
        timeout: None,
    }
}

fn spec_with(mcp: Vec<McpServer>) -> AgentSpec {
    let mut spec = load_config(None, &defaults())
        .expect("the default spec loads")
        .remove(0);
    spec.mcp = mcp;
    spec
}

/// The writer a deployment supplies, recording what it was handed. It renders no format:
/// what the settings file looks like is that deployment's test, and what actus passes to
/// the writer is this one.
struct Recorder(std::sync::Mutex<Option<AgentSpec>>);

impl Recorder {
    fn new() -> Self {
        Self(std::sync::Mutex::new(None))
    }

    fn recorded(&self) -> AgentSpec {
        self.0
            .lock()
            .expect("the recorder is not poisoned")
            .clone()
            .expect("the writer was handed a spec")
    }
}

impl SettingsWriter for Recorder {
    fn write(&self, _data_dir: &Path, spec: &AgentSpec) -> anyhow::Result<()> {
        *self.0.lock().expect("the recorder is not poisoned") = Some(spec.clone());
        Ok(())
    }
}

fn unset(name: &str) -> String {
    format!("${name}")
}

/// A token declared as `$NAME` is read from this process's environment, so it reaches the
/// executor without being written to the config file that names the server. A header may
/// mix its own text with a resolved value.
#[test]
fn a_declared_value_is_resolved_from_the_environment() {
    let previous = std::env::var_os(TOKEN_VAR);
    // SAFETY: the name belongs to this test alone.
    unsafe { std::env::set_var(TOKEN_VAR, "tok-123") };

    let spec = spec_from(&format!(
        r#"
[[agents]]
name = "telos"
ws_port = 8080

[[agents.mcp]]
name = "mcp-server-github"
command = "npx"
args = ["-y", "@modelcontextprotocol/server-github"]
env = {{ "GITHUB_PERSONAL_ACCESS_TOKEN" = "${TOKEN_VAR}" }}

[[agents.mcp]]
name = "cloudflare-api"
url = "https://mcp.cloudflare.com/mcp"
headers = {{ "Authorization" = "Bearer ${TOKEN_VAR}" }}
"#
    ));
    let recorder = Recorder::new();
    let result = write_settings(Some(&recorder), tempfile::tempdir().unwrap().path(), &spec);

    match previous {
        Some(value) => unsafe { std::env::set_var(TOKEN_VAR, value) },
        None => unsafe { std::env::remove_var(TOKEN_VAR) },
    }
    result.expect("a name that is set resolves");

    let recorded = recorder.recorded();
    let by_name = |n: &str| recorded.mcp.iter().find(|m| m.name == n).expect(n);
    assert_eq!(
        by_name("mcp-server-github")
            .env
            .get("GITHUB_PERSONAL_ACCESS_TOKEN")
            .map(String::as_str),
        Some("tok-123"),
        "an env value resolves before the writer sees it"
    );
    assert_eq!(
        by_name("cloudflare-api")
            .headers
            .get("Authorization")
            .map(String::as_str),
        Some("Bearer tok-123"),
        "a header may mix its own text with a resolved value"
    );
}

/// An unset `$NAME` fails the launch of a server that starts, and names the variable. An
/// empty token would leave the server starting and failing on every call, which reads as a
/// broken tool rather than as a missing setting.
#[test]
fn an_unset_declared_value_fails_the_launch() {
    let spec = spec_with(vec![declared(
        "mcp-server-github",
        true,
        &[("GITHUB_PERSONAL_ACCESS_TOKEN", &unset(UNSET_VAR))],
    )]);
    let error = write_settings(Some(&Recorder::new()), tempfile::tempdir().unwrap().path(), &spec)
        .expect_err("an unset declared variable must fail the launch of a server that starts")
        .to_string();
    assert!(error.contains("mcp-server-github"), "{error}");
    assert!(error.contains(UNSET_VAR), "{error}");
}

/// A server that is off is in the catalogue and not running, and its credential is not
/// needed until it is turned on. Requiring one here would make every launch depend on a
/// credential for a server nobody runs, so the reference is left as written and the missing
/// name is reported instead.
#[test]
fn an_unset_declared_value_does_not_fail_the_launch_of_a_server_that_is_off() {
    let spec = spec_with(vec![declared(
        "mcp-server-github",
        false,
        &[("GITHUB_PERSONAL_ACCESS_TOKEN", &unset(UNSET_VAR))],
    )]);
    let recorder = Recorder::new();
    write_settings(Some(&recorder), tempfile::tempdir().unwrap().path(), &spec)
        .expect("a server that is off must not hold up the launch");

    let recorded = recorder.recorded();
    let server = recorded
        .mcp
        .iter()
        .find(|m| m.name == "mcp-server-github")
        .expect("the declared server");
    assert!(!server.enabled);
    assert_eq!(
        server
            .env
            .get("GITHUB_PERSONAL_ACCESS_TOKEN")
            .map(String::as_str),
        Some(unset(UNSET_VAR).as_str()),
        "the reference is left as written rather than resolved to nothing"
    );
}

/// A composition that names no writer runs no executor whose format actus carries, and the
/// launch says so by name: a version that started the program anyway would leave it failing
/// on its own first read of a file nobody wrote.
#[test]
fn a_composition_with_no_writer_is_refused_by_name() {
    let spec = spec_with(Vec::new());
    let error = write_settings(None, tempfile::tempdir().unwrap().path(), &spec)
        .expect_err("a launch with no writer must be refused")
        .to_string();
    assert!(error.contains("agent 'telos'"), "{error}");
    assert!(error.contains("names no writer"), "{error}");
}
