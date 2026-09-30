// Integration tests for agent config loading (issue #7). The spec the loader
// produces is the fabric's own shape (issue #36): name, kind, workdir, and an
// options table the kind's factory reads.

use std::path::PathBuf;
use std::sync::Arc;

use actus::agent::adapter::FactoryRegistry;
use actus::agent::config::{load_config, load_control_policy};
use actus::agent::ext_cli::{ExtCliFactory, ExtCliOptions, PromptMode};
use actus::agent::native::NativeFactory;
use actus::telos::adapter::TelosFactory;
use actus::telos::options::{TelosDefaults, DEFAULT_REASONING_EFFORT};

fn telos_defaults() -> TelosDefaults {
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

/// The built-in kinds, registered the way main registers them.
fn registry() -> FactoryRegistry {
    let mut factories = FactoryRegistry::new();
    factories.register_default(Arc::new(TelosFactory::new(telos_defaults())));
    factories.register(Arc::new(ExtCliFactory));
    factories.register(Arc::new(NativeFactory));
    factories
}

fn write_config(dir: &std::path::Path, body: &str) -> std::path::PathBuf {
    let path = dir.join("config.toml");
    std::fs::write(&path, body).expect("write config");
    path
}

#[test]
fn no_file_yields_single_default_agent() {
    let specs = load_config(None, &registry()).unwrap();
    assert_eq!(specs.len(), 1);
    assert_eq!(specs[0].name, "telos");
    assert_eq!(specs[0].kind, "telos");
    assert!(specs[0].workdir.is_none());
    assert!(
        specs[0].options.is_empty(),
        "a default agent declares no options; the factory supplies its defaults"
    );
}

#[test]
fn a_kind_omitted_is_the_registry_default() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_config(dir.path(), "[[agents]]\nname = \"plain\"\n");
    let specs = load_config(Some(&path), &registry()).unwrap();
    assert_eq!(specs[0].kind, "telos");
}

#[test]
fn an_unknown_kind_is_refused_and_names_the_registered_ones() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_config(
        dir.path(),
        "[[agents]]\nname = \"claude\"\nkind = \"langgraph\"\n",
    );
    let error = load_config(Some(&path), &registry()).unwrap_err();
    assert!(error.contains("agent 'claude'"), "{error}");
    assert!(error.contains("langgraph"), "{error}");
    for kind in ["ext_cli", "native", "telos"] {
        assert!(
            error.contains(kind),
            "the registered kinds are named: {error}"
        );
    }
}

#[test]
fn the_options_table_keeps_the_declaration_for_the_factory() {
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
    let specs = load_config(Some(&path), &registry()).unwrap();
    assert_eq!(specs.len(), 1);
    let s = &specs[0];
    assert_eq!(s.name, "aux");
    assert_eq!(s.kind, "ext_cli");
    assert_eq!(
        s.workdir.as_deref(),
        Some(std::path::Path::new("/tmp/aux-work"))
    );

    // The fabric reads none of these; the kind's factory reads them out of
    // the same table.
    let options: ExtCliOptions = s.options().unwrap();
    assert_eq!(options.bin, PathBuf::from("ante"));
    assert_eq!(
        options.cli_args,
        vec!["-p".to_string(), "{prompt}".to_string()]
    );
    assert_eq!(options.cli_env.get("FOO").map(String::as_str), Some("bar"));
    assert_eq!(
        options.cli_env.get("KEY").map(String::as_str),
        Some("$LLM_API_KEY")
    );
    assert_eq!(options.cli_prompt, PromptMode::Stdin);
    assert_eq!(options.cli_timeout_secs, 42);
}

#[test]
fn ext_cli_profile_defaults_when_fields_are_omitted() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_config(
        dir.path(),
        "[[agents]]\nname = \"aux\"\nkind = \"ext_cli\"\nbin = \"some-cli\"\n",
    );
    let specs = load_config(Some(&path), &registry()).unwrap();
    let options: ExtCliOptions = specs[0].options().unwrap();
    assert_eq!(options.bin, PathBuf::from("some-cli"));
    assert!(options.cli_args.is_empty());
    assert!(options.cli_env.is_empty());
    assert_eq!(options.cli_prompt, PromptMode::Arg);
    assert_eq!(options.cli_timeout_secs, 300);
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
    let err = load_config(Some(&path), &registry()).unwrap_err();
    assert!(err.contains("duplicate agent name"), "unexpected: {}", err);
}

#[test]
fn empty_agents_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_config(dir.path(), "");
    let err = load_config(Some(&path), &registry()).unwrap_err();
    assert!(err.contains("no agents"), "unexpected: {}", err);
}

#[test]
fn malformed_toml_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_config(dir.path(), "not [ valid toml");
    let err = load_config(Some(&path), &registry()).unwrap_err();
    assert!(err.contains("cannot parse"), "unexpected: {}", err);
}
