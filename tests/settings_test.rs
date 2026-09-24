// Integration tests for the settings an actus-launched agent starts from
// (issue #25: an agent thinks at the level the operator chose, or not at all).

use actus::agent::config::{load_config, AgentDefaults, AgentSpec, ToolApproval};
use actus::telos::ensure_telos_settings;
use std::path::PathBuf;

fn spec_with_effort(level: &str) -> AgentSpec {
    let defaults = AgentDefaults {
        provider: "openai-compatible".to_string(),
        model: "example-model".to_string(),
        model_display: "example-model".to_string(),
        base_url: "https://api.example.com/v1".to_string(),
        api_key: Some("sk-test".to_string()),
        bin: PathBuf::from("/bin/telos"),
        ws_port: 8080,
        reasoning_effort: level.to_string(),
    };
    let mut spec = load_config(None, &defaults).unwrap().remove(0);
    spec.tool_approval = ToolApproval::Always;
    spec
}

fn settings_written(spec: &AgentSpec) -> serde_json::Value {
    let dir = tempfile::tempdir().unwrap();
    ensure_telos_settings(dir.path(), spec).unwrap();
    serde_json::from_str(&std::fs::read_to_string(dir.path().join("config/settings.json")).unwrap())
        .unwrap()
}

/// A level the operator declares reaches both places the agent reads it from:
/// the model entry, which is where the agent's OpenAI-compatible provider
/// decides a model can think at all, and `agent.default_model`, which is the
/// only thing a new thread reads its thinking state from.
#[test]
fn a_declared_effort_reaches_the_model_entry_and_the_default_model() {
    for level in ["minimal", "low", "medium", "high", "xhigh", "max"] {
        let settings = settings_written(&spec_with_effort(level));

        let model = &settings["language_models"]["openai_compatible"]["openai-compatible"]
            ["available_models"][0];
        assert_eq!(
            model["reasoning_effort"], level,
            "{level} must be declared on the model entry"
        );

        let default_model = &settings["agent"]["default_model"];
        assert_eq!(default_model["provider"], "openai-compatible");
        assert_eq!(default_model["model"], "example-model");
        assert_eq!(
            default_model["enable_thinking"], true,
            "a declared effort is a thinking agent"
        );
        assert_eq!(
            default_model["effort"], level,
            "{level} must be the thread's own effort"
        );
    }
}

/// `none` is the escape hatch: it is written as no reasoning field at all,
/// which is the one shape that asks the provider for no reasoning parameter,
/// and as a thread that does not think. A deployment whose model rejects the
/// parameter has nothing else to reach for.
#[test]
fn none_sends_no_reasoning_parameter_and_starts_without_thinking() {
    let settings = settings_written(&spec_with_effort("none"));

    let model = &settings["language_models"]["openai_compatible"]["openai-compatible"]
        ["available_models"][0];
    assert!(
        model.get("reasoning_effort").is_none(),
        "none means the entry declares nothing: {model}"
    );

    let default_model = &settings["agent"]["default_model"];
    assert_eq!(default_model["enable_thinking"], false);
    assert!(default_model["effort"].is_null());
}

/// An endpoint that is not configured writes neither, so a deployment that
/// named no model is not given a thinking stance for one.
#[test]
fn no_endpoint_writes_no_model_or_thinking_settings() {
    let mut spec = spec_with_effort("high");
    spec.base_url = String::new();
    spec.model = String::new();
    let settings = settings_written(&spec);

    assert!(settings.get("language_models").is_none(), "{settings}");
    assert!(
        settings["agent"].get("default_model").is_none(),
        "no endpoint means no default model: {settings}"
    );
}

/// The file actus writes is the operator's when it already says something, so
/// a hand-written default model survives a launch.
#[test]
fn an_existing_default_model_is_left_alone() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("config")).unwrap();
    std::fs::write(
        dir.path().join("config/settings.json"),
        r#"{"agent": {"default_model": {"provider": "other", "model": "other-model"}}}"#,
    )
    .unwrap();

    ensure_telos_settings(dir.path(), &spec_with_effort("high")).unwrap();
    let settings: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(dir.path().join("config/settings.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(settings["agent"]["default_model"]["model"], "other-model");
}
