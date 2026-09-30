// The adapter seam (issue #36). A kind is a name a config resolves against
// registered factories. These tests declare a factory and a backend in the
// test crate itself, which is the claim the seam makes: a platform that is
// not in the fabric's source attaches without touching it.

use std::collections::HashMap;
use std::sync::Arc;

use actus::agent::adapter::{AgentFactory, FactoryRegistry, LaunchContext, LaunchedAgent};
use actus::agent::config::{load_config, AgentSpec};
use actus::agent::{
    truncate_title, AgentBackend, AgentCapabilities, AgentRegistry, AgentStatus,
    PendingAuthorization, SubmitReceipt, ThreadMessage, ThreadSession,
};
use serde::Deserialize;
use tokio::sync::{watch, RwLock};

/// The option type this kind reads out of its declaration. The fabric never
/// sees it.
#[derive(Deserialize)]
struct StubOptions {
    /// Names the launched agent, so a test can see the option travel.
    #[serde(default)]
    label: Option<String>,
    /// The reply the stub backend answers with.
    #[serde(default = "default_reply")]
    reply: String,
}

fn default_reply() -> String {
    "stub reply".to_string()
}

/// A factory declared outside the fabric's source. It is the whole of what a
/// platform has to provide: a kind name, a validation, and a launch.
struct StubFactory;

#[async_trait::async_trait]
impl AgentFactory for StubFactory {
    fn kind(&self) -> &'static str {
        "stub"
    }

    fn validate(&self, spec: &AgentSpec, _all: &[AgentSpec]) -> Result<(), String> {
        let options: StubOptions = spec.options()?;
        if options.label.as_deref() == Some("") {
            return Err(format!("agent '{}': label must not be empty", spec.name));
        }
        Ok(())
    }

    async fn launch(
        &self,
        spec: &AgentSpec,
        _ctx: &LaunchContext,
    ) -> anyhow::Result<LaunchedAgent> {
        let options: StubOptions = spec.options().map_err(anyhow::Error::msg)?;
        let name = match options.label {
            Some(label) => format!("{}-{}", spec.name, label),
            None => spec.name.clone(),
        };
        Ok(LaunchedAgent {
            backend: Arc::new(StubAgent::new(name, options.reply)),
            children: Vec::new(),
        })
    }
}

struct StubAgent {
    name: String,
    reply: String,
    threads: RwLock<HashMap<String, ThreadSession>>,
    notify: watch::Sender<u64>,
}

impl StubAgent {
    fn new(name: String, reply: String) -> Self {
        let (notify, _) = watch::channel(0u64);
        Self {
            name,
            reply,
            threads: RwLock::new(HashMap::new()),
            notify,
        }
    }
}

#[async_trait::async_trait]
impl AgentBackend for StubAgent {
    fn name(&self) -> &str {
        &self.name
    }

    fn kind(&self) -> &'static str {
        "stub"
    }

    fn capabilities(&self) -> AgentCapabilities {
        AgentCapabilities {
            sessionful: false,
            streaming: false,
            tools: false,
            approval: false,
            parallel: true,
            transport: "stub",
        }
    }

    async fn status(&self) -> AgentStatus {
        AgentStatus {
            name: self.name.clone(),
            kind: self.kind().to_string(),
            connected: true,
            ready: true,
            capabilities: self.capabilities(),
            last_error: None,
        }
    }

    async fn submit(
        &self,
        thread_id: Option<&str>,
        message: &str,
    ) -> Result<SubmitReceipt, String> {
        let tid = thread_id
            .map(str::to_string)
            .unwrap_or_else(|| format!("stub-{}", uuid::Uuid::new_v4()));
        let request_id = uuid::Uuid::new_v4().to_string();
        let now = chrono::Utc::now();
        let mut threads = self.threads.write().await;
        let session = threads.entry(tid.clone()).or_insert_with(|| ThreadSession {
            id: tid.clone(),
            title: None,
            messages: Vec::new(),
            created_at: now,
            updated_at: None,
            completed: true,
            acp_thread_id: None,
            turn_completed: 0,
            parent: None,
        });
        if session.title.is_none() {
            session.title = Some(truncate_title(message));
        }
        session.messages.push(ThreadMessage {
            role: "user".to_string(),
            content: message.to_string(),
            message_id: None,
            entry_type: None,
            tool_name: None,
            tool_status: None,
            timestamp: now,
        });
        session.messages.push(ThreadMessage {
            role: "assistant".to_string(),
            content: self.reply.clone(),
            message_id: Some(request_id.clone()),
            entry_type: Some("agent_message".to_string()),
            tool_name: None,
            tool_status: None,
            timestamp: now,
        });
        session.completed = true;
        session.turn_completed += 1;
        let is_new = session.messages.len() == 2;
        drop(threads);
        let _ = self.notify.send(now.timestamp_millis() as u64);
        Ok(SubmitReceipt {
            thread_id: tid,
            request_id,
            is_new,
        })
    }

    async fn cancel(&self) -> Result<(), String> {
        Ok(())
    }

    async fn thread(&self, thread_id: &str) -> Option<ThreadSession> {
        self.threads.read().await.get(thread_id).cloned()
    }

    async fn threads(&self) -> Vec<ThreadSession> {
        self.threads.read().await.values().cloned().collect()
    }

    async fn subscribe(&self) -> watch::Receiver<u64> {
        self.notify.subscribe()
    }

    async fn pending_tool_calls(&self) -> Vec<PendingAuthorization> {
        Vec::new()
    }

    async fn resolve_tool_call(
        &self,
        _platform_thread_id: &str,
        _tool_call_id: &str,
        _allow: bool,
    ) -> Result<(), String> {
        Ok(())
    }

    async fn create_thread(&self) -> Result<String, String> {
        Ok(format!("stub-{}", uuid::Uuid::new_v4()))
    }
}

fn registry() -> FactoryRegistry {
    let mut factories = FactoryRegistry::new();
    factories.register(Arc::new(StubFactory));
    factories
}

fn write_config(dir: &std::path::Path, body: &str) -> std::path::PathBuf {
    let path = dir.join("config.toml");
    std::fs::write(&path, body).expect("write config");
    path
}

fn launch_context(dir: &std::path::Path) -> LaunchContext {
    LaunchContext {
        workdir: dir.to_path_buf(),
        threads_root: dir.to_path_buf(),
        http_port: 0,
        api_token: "test-token".to_string(),
    }
}

#[tokio::test]
async fn a_factory_declared_outside_the_fabric_launches_through_the_registry() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(
        dir.path(),
        "[[agents]]\nname = \"alpha\"\nkind = \"stub\"\nlabel = \"one\"\nreply = \"hi\"\n",
    );

    let factories = registry();
    let specs = load_config(Some(&cfg), &factories).unwrap();
    assert_eq!(specs.len(), 1);
    assert_eq!(specs[0].kind, "stub");

    let factory = factories.get(&specs[0].kind).expect("registered factory");
    let launched = factory
        .launch(&specs[0], &launch_context(dir.path()))
        .await
        .unwrap();

    let mut agents = AgentRegistry::new();
    agents.register(launched.backend, true);

    let agent = agents.get("alpha-one").expect("the option named the agent");
    let status = agent.status().await;
    assert_eq!(status.kind, "stub");
    assert_eq!(status.capabilities.transport, "stub");
    assert!(status.ready);

    let receipt = agent.submit(None, "hello").await.unwrap();
    let thread = agent.thread(&receipt.thread_id).await.unwrap();
    assert_eq!(thread.messages[0].content, "hello");
    assert_eq!(thread.messages[1].content, "hi");
}

#[test]
fn a_factory_validates_its_own_options_and_names_the_agent() {
    let dir = tempfile::tempdir().unwrap();

    let cfg = write_config(
        dir.path(),
        "[[agents]]\nname = \"empty\"\nkind = \"stub\"\nlabel = \"\"\n",
    );
    let error = load_config(Some(&cfg), &registry()).unwrap_err();
    assert!(error.contains("agent 'empty'"), "{error}");
    assert!(error.contains("label must not be empty"), "{error}");

    let cfg = write_config(
        dir.path(),
        "[[agents]]\nname = \"typed\"\nkind = \"stub\"\nlabel = 3\n",
    );
    let error = load_config(Some(&cfg), &registry()).unwrap_err();
    assert!(error.contains("agent 'typed'"), "{error}");
    assert!(error.contains("options"), "{error}");
}

#[test]
fn an_unregistered_kind_is_refused_and_the_registered_ones_are_named() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(dir.path(), "[[agents]]\nname = \"t\"\nkind = \"telos\"\n");

    let error = load_config(Some(&cfg), &registry()).unwrap_err();
    assert!(error.contains("agent 't'"), "{error}");
    assert!(error.contains("unknown kind 'telos'"), "{error}");
    assert!(
        error.contains("stub"),
        "the registered kinds are named: {error}"
    );
}
