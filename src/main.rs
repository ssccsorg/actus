// Actus — agent execution runtime (REST API server)
//
// The server is `run::run`, which the library owns so that a build which links a record
// store the public tree cannot carry can compose it. This binary passes the store the
// environment selects, which is the behavior a host had before the record became a port.

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    actus::run::main_with_store(actus::run::environment_store()).await
}
