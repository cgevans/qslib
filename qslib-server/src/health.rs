//! `GET /health` — liveness plus a live probe of the SCPI server.

use std::time::Duration;

use axum::extract::State;
use axum::Json;
use serde::Serialize;
use tokio::net::TcpStream;

use crate::state::AppState;

#[derive(Serialize)]
pub struct Health {
    name: &'static str,
    version: &'static str,
    uptime_s: u64,
    scpi_ok: bool,
    /// Canonicalized `--file-root`. Clients use it to decide whether an
    /// absolute filesystem path is reachable over `/file` (and how to make it
    /// root-relative) before falling back to SCPI.
    file_root: String,
}

/// Probe the SCPI target with a short-timeout TCP connect.
async fn probe_scpi(state: &AppState) -> bool {
    matches!(
        tokio::time::timeout(Duration::from_secs(2), TcpStream::connect(state.scpi_target)).await,
        Ok(Ok(_))
    )
}

pub async fn health(State(state): State<AppState>) -> Json<Health> {
    Json(Health {
        name: "qslib-server",
        version: env!("CARGO_PKG_VERSION"),
        uptime_s: state.started.elapsed().as_secs(),
        scpi_ok: probe_scpi(&state).await,
        file_root: state.file_root.to_string_lossy().into_owned(),
    })
}
