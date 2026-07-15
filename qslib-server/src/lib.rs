//! Optional semantic API and managed SCPI connection for QSLib.

pub mod api;
pub mod auth;
pub mod config;
pub mod dto;
pub mod error;
pub mod events;
pub mod file;
pub mod health;
pub mod operation;
pub mod package;
pub mod scpi;
pub mod scpi_http;
pub mod service;
pub mod state;
pub mod tunnel;
pub mod upgrade;

use std::io::ErrorKind;

use axum::body::{to_bytes, Body};
use axum::extract::DefaultBodyLimit;
use axum::http::{Request, Response, StatusCode};
use axum::middleware::Next;
use axum::routing::{get, post, put};
use axum::Router;
use tokio::net::TcpListener;
use tracing::{info, warn};
use uuid::Uuid;

use crate::config::Config;
use crate::state::AppState;

/// Build only the current versioned application surface. qslib-server has not
/// been released, so the prior experimental transport routes intentionally do
/// not remain as compatibility aliases.
pub fn build_router(state: AppState) -> Router {
    let mut router = Router::new()
        .route("/health", get(health::health))
        .route("/api/v1/capabilities", get(api::capabilities))
        .route("/api/v1/operations/{id}", get(api::get_operation))
        .route("/api/v1/events", get(api::events))
        .route("/api/v1/instrument/status", get(api::instrument_status))
        .route("/api/v1/instrument/power", put(api::set_power))
        .route("/api/v1/instrument/block", put(api::set_block))
        .route("/api/v1/instrument/indicator", put(api::set_indicator))
        .route("/api/v1/instrument/drawer", put(api::set_drawer))
        .route("/api/v1/instrument/cover", put(api::set_cover))
        .route("/api/v1/instrument/access-keys", post(api::access_key))
        .route("/api/v1/instrument/actions/restart", post(api::restart))
        .route(
            "/api/v1/files/{context}/{*path}",
            get(file::serve_file)
                .head(file::serve_file)
                .put(file::put_file)
                .layer(DefaultBodyLimit::max(128 * 1024 * 1024)),
        )
        .route("/api/v1/directories/{context}/{*path}", get(file::list_dir))
        .route(
            "/api/v1/directories/{context}",
            get(file::list_context_root),
        )
        .route("/api/v1/experiments", get(api::list_experiments))
        .route(
            "/api/v1/experiments/{name}",
            get(api::get_experiment).delete(api::delete_experiment),
        )
        .route(
            "/api/v1/experiments/{name}/package",
            get(api::get_package)
                .put(api::put_package)
                .layer(DefaultBodyLimit::max(128 * 1024 * 1024)),
        )
        .route("/api/v1/runs", get(api::list_runs).post(api::start_run))
        .route("/api/v1/runs/current", get(api::current_run))
        .route("/api/v1/runs/current/protocol", get(api::current_protocol))
        .route("/api/v1/runs/{name}", get(api::get_run))
        .route(
            "/api/v1/runs/{name}/actions/{action}",
            post(api::run_action),
        )
        .route(
            "/api/v1/runs/{name}/protocol",
            put(api::put_protocol).layer(DefaultBodyLimit::max(16 * 1024 * 1024)),
        )
        .route("/api/v1/runs/{name}/eds", get(api::get_eds))
        .route(
            "/api/v1/server/upgrade",
            post(upgrade::upgrade).layer(DefaultBodyLimit::max(128 * 1024 * 1024)),
        );

    if state.enable_raw_scpi {
        router = router.route(
            "/api/v1/scpi",
            post(scpi_http::post_scpi).layer(DefaultBodyLimit::max(1024 * 1024)),
        );
    }
    if state.enable_scpi_tunnel {
        router = router.route(
            "/api/v1/scpi/tunnel",
            get(tunnel::tunnel).connect(tunnel::tunnel),
        );
    }

    router
        .fallback(route_not_found)
        .method_not_allowed_fallback(method_not_allowed)
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth::require_bearer,
        ))
        .layer(axum::middleware::from_fn(request_id))
        .with_state(state)
}

async fn route_not_found() -> crate::error::ServerError {
    crate::error::ServerError::not_found("API resource not found")
}

async fn method_not_allowed() -> crate::error::ServerError {
    crate::error::ServerError::new(
        StatusCode::METHOD_NOT_ALLOWED,
        "method_not_allowed",
        "method not allowed for this API resource",
    )
}

async fn request_id(
    mut request: Request<axum::body::Body>,
    next: Next,
) -> Response<axum::body::Body> {
    let request_id = request
        .headers()
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty() && value.len() <= 128)
        .map(str::to_string)
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    request.extensions_mut().insert(request_id.clone());
    let mut response = next.run(request).await;
    if response.status().is_client_error() || response.status().is_server_error() {
        let is_json = response
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("application/json"));
        if is_json {
            let (mut parts, body) = response.into_parts();
            match to_bytes(body, 1024 * 1024).await {
                Ok(bytes) => {
                    let replacement = serde_json::from_slice::<serde_json::Value>(&bytes)
                        .ok()
                        .and_then(|mut value| {
                            value["request_id"] = serde_json::Value::String(request_id.clone());
                            serde_json::to_vec(&value).ok()
                        })
                        .unwrap_or_else(|| bytes.to_vec());
                    parts.headers.remove(axum::http::header::CONTENT_LENGTH);
                    response = Response::from_parts(parts, Body::from(replacement));
                }
                Err(error) => {
                    warn!("could not attach request ID to error response: {error}");
                    response = Response::from_parts(parts, Body::empty());
                }
            }
        }
    }
    if let Ok(value) = request_id.parse() {
        response.headers_mut().insert("x-request-id", value);
    }
    response
}

pub async fn run(config: Config, state: AppState) -> anyhow::Result<()> {
    let app = build_router(state.clone());
    let listener = match bind_reuseaddr(config.listen).await {
        Ok(listener) => listener,
        Err(error) if error.kind() == ErrorKind::AddrInUse => {
            info!(
                "address {} already in use; assuming qslib-server is already running",
                config.listen
            );
            return Ok(());
        }
        Err(error) => return Err(error.into()),
    };
    info!(
        "qslib-server {} listening on {} (managed SCPI target {})",
        env!("CARGO_PKG_VERSION"),
        config.listen,
        config.scpi_target,
    );
    let shutdown_state = state.clone();
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown_signal().await;
            // Stop semantic intake and return the managed connection to
            // Observer before HTTP stops accepting/draining requests.
            shutdown_state.service.shutdown().await;
        })
        .await?;
    Ok(())
}

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
        if let Ok(mut signal) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            signal.recv().await;
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
                .map_err(|error| anyhow::anyhow!("failed to open log file {path:?}: {error}"))?;
            BoxMakeWriter::new(std::sync::Mutex::new(file))
        }
        None => BoxMakeWriter::new(std::io::stderr),
    };
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
