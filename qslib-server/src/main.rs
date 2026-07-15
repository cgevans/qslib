use clap::Parser;
use tracing::warn;

use qslib_server::config::Config;
use qslib_server::state::AppState;
use qslib_server::{init_logging, run};

fn main() -> anyhow::Result<()> {
    let config = Config::parse();
    init_logging(&config)?;
    let auth = config.resolve_auth()?;
    if let Some(role) = auth.unauthenticated_role() {
        warn!(?role, "unauthenticated HTTP requests are enabled");
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async move {
        let state = AppState::new(&config, auth)?;
        run(config, state).await
    })
}
