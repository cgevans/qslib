//! Shared application state.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use qslib_core::commands::AccessLevel;

use crate::config::Config;

/// Immutable, shared agent state, cheaply cloneable via `Arc`.
#[derive(Clone)]
pub struct AppState(pub Arc<AppStateInner>);

pub struct AppStateInner {
    /// Localhost plaintext SCPI endpoint.
    pub scpi_target: SocketAddr,
    /// Canonicalized root for `/file` resolution.
    pub file_root: PathBuf,
    pub default_access: AccessLevel,
    pub max_access: AccessLevel,
    /// Bearer token; `None` means authentication is disabled.
    pub token: Option<String>,
    /// Password for password-gated SCPI access levels.
    pub scpi_password: Option<String>,
    pub scpi_timeout_ms: u64,
    pub started: Instant,
    /// Bounds concurrent SCPI tunnels.
    pub tunnels: Arc<tokio::sync::Semaphore>,
}

impl AppState {
    /// Build the shared state from a parsed [`Config`] and its resolved token.
    /// The file root is canonicalized so path-safety checks compare against the
    /// real directory.
    pub fn new(config: &Config, token: Option<String>) -> anyhow::Result<Self> {
        let file_root = std::fs::canonicalize(&config.file_root).map_err(|e| {
            anyhow::anyhow!(
                "failed to canonicalize --file-root {:?}: {e}",
                config.file_root
            )
        })?;
        Ok(AppState(Arc::new(AppStateInner {
            scpi_target: config.scpi_target,
            file_root,
            default_access: config.default_access.clone(),
            max_access: config.max_access.clone(),
            token,
            scpi_password: config.scpi_password.clone(),
            scpi_timeout_ms: config.scpi_timeout_ms,
            started: Instant::now(),
            tunnels: Arc::new(tokio::sync::Semaphore::new(config.max_tunnels.max(1))),
        })))
    }
}

impl std::ops::Deref for AppState {
    type Target = AppStateInner;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
