//! Stable process and managed-actor health resource.

use axum::extract::State;
use axum::Json;
use serde::Serialize;

use crate::dto::AccessDto;
use crate::state::AppState;

#[derive(Serialize)]
pub struct Health {
    name: &'static str,
    version: &'static str,
    executable_sha256: String,
    uptime_s: u64,
    ready: bool,
    generation: u64,
    current_access: Option<AccessDto>,
    last_successful_command: Option<chrono::DateTime<chrono::Utc>>,
    reconnect_count: u64,
    queue_depth: usize,
}

/// This reads actor state only. It must never create a probe SCPI connection.
pub async fn health(State(state): State<AppState>) -> Json<Health> {
    let actor = state.service.health();
    Json(Health {
        name: "qslib-server",
        version: env!("CARGO_PKG_VERSION"),
        executable_sha256: state.exe_sha256.clone(),
        uptime_s: state.started.elapsed().as_secs(),
        ready: actor.ready,
        generation: actor.generation,
        current_access: actor.current_access,
        last_successful_command: actor.last_successful_command,
        reconnect_count: actor.reconnect_count,
        queue_depth: state.service.queue_depth(),
    })
}
