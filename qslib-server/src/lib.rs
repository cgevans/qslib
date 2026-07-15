//! qslib-server: on-instrument HTTP transport/command service.
//!
//! Binds one port on the private link and serves, over plain HTTP: bulk file
//! transfer off disk (`/file` GET, and PUT to write unless `--read-only`), a
//! one-shot SCPI command (`/scpi`), a streaming SCPI tunnel, and `/health`. It
//! is a client of the localhost plaintext SCPI server and reads and writes
//! on-disk experiment files; it never drives the InstrumentServer's hardware
//! control directly.

pub mod auth;
pub mod config;
pub mod error;
pub mod file;
pub mod health;
pub mod scpi;
pub mod scpi_http;
pub mod state;
pub mod tunnel;
pub mod upgrade;

use std::io::ErrorKind;

use axum::extract::DefaultBodyLimit;
use axum::routing::{get, post};
use axum::Router;
use tokio::net::TcpListener;
use tracing::{info, warn};

use crate::config::Config;
use crate::state::AppState;

/// Build the qslib-server HTTP router with bearer-token auth applied to every route.
pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health::health))
        .route(
            "/scpi",
            post(scpi_http::post_scpi)
                .get(tunnel::tunnel)
                .connect(tunnel::tunnel),
        )
        .route(
            "/file/{*path}",
            get(file::serve_file)
                .head(file::serve_file)
                .put(file::put_file)
                // An uploaded experiment file (or `.eds`) can be several MB —
                // well over axum's 2 MB default. GET/HEAD carry no body.
                .layer(DefaultBodyLimit::max(128 * 1024 * 1024)),
        )
        .route("/list/{*path}", get(file::list_dir))
        // The uploaded binary is several MB — well over axum's 2 MB default.
        .route(
            "/upgrade",
            post(upgrade::upgrade).layer(DefaultBodyLimit::max(128 * 1024 * 1024)),
        )
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth::require_bearer,
        ))
        .with_state(state)
}

/// Bind the configured address and serve until a shutdown signal. Exits cleanly
/// (idempotent start) if the port is already in use.
pub async fn run(config: Config, state: AppState) -> anyhow::Result<()> {
    let app = build_router(state);

    let listener = match bind_reuseaddr(config.listen).await {
        Ok(l) => l,
        Err(e) if e.kind() == ErrorKind::AddrInUse => {
            info!(
                "address {} already in use; assuming qslib-server is already running",
                config.listen
            );
            return Ok(());
        }
        Err(e) => return Err(e.into()),
    };

    info!(
        "qslib-server {} listening on {} (scpi target {}, file root {:?})",
        env!("CARGO_PKG_VERSION"),
        config.listen,
        config.scpi_target,
        config.file_root,
    );

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

/// Bind a TCP listener with `SO_REUSEADDR` so an `/upgrade` restart can rebind
/// the port immediately after the old process exits (avoiding a `TIME_WAIT`
/// `EADDRINUSE`), rather than the plain `TcpListener::bind` default.
async fn bind_reuseaddr(addr: std::net::SocketAddr) -> std::io::Result<TcpListener> {
    let socket = if addr.is_ipv4() {
        tokio::net::TcpSocket::new_v4()?
    } else {
        tokio::net::TcpSocket::new_v6()?
    };
    socket.set_reuseaddr(true)?;
    socket.bind(addr)?;
    socket.listen(1024)
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        if let Ok(mut sig) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            sig.recv().await;
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    info!("shutdown signal received");
}

/// Initialize tracing to stderr, or to `--log` when configured.
pub fn init_logging(config: &Config) -> anyhow::Result<()> {
    use tracing_subscriber::fmt::writer::BoxMakeWriter;
    use tracing_subscriber::EnvFilter;

    let filter =
        EnvFilter::try_from_env("QSLIB_SERVER_LOG").unwrap_or_else(|_| EnvFilter::new("info"));

    let writer: BoxMakeWriter = match &config.log {
        Some(path) => {
            let file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .map_err(|e| anyhow::anyhow!("failed to open --log file {:?}: {e}", path))?;
            BoxMakeWriter::new(std::sync::Mutex::new(file))
        }
        None => BoxMakeWriter::new(std::io::stderr),
    };

    // No ANSI: the `ansi` crate feature is intentionally excluded to keep the
    // qslib-server binary small; logs go to stderr or a file, where colors are noise.
    if tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(writer)
        .with_ansi(false)
        .try_init()
        .is_err()
    {
        warn!("tracing subscriber already initialized");
    }
    Ok(())
}
