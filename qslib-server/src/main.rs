//! qslib-server binary entry point. See the crate docs (`lib.rs`) for the API.

use clap::Parser;
use tracing::warn;

use qslib_server::config::Config;
use qslib_server::state::AppState;
use qslib_server::{init_logging, run};

fn main() -> anyhow::Result<()> {
    let config = Config::parse();
    init_logging(&config)?;

    let token = config.resolve_token()?;
    if token.is_none() {
        warn!("authentication is DISABLED (--no-auth); serving without a bearer token");
    }
    if config.pool_size != 0 {
        warn!(
            "--pool-size {} ignored: the warm connection pool is not yet implemented; \
             /scpi uses connect-per-request",
            config.pool_size
        );
    }
    let state = AppState::new(&config, token)?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(run(config, state))
}
