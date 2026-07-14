//! Shared application state.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use qslib_core::commands::AccessLevel;

use crate::config::Config;

/// Immutable, shared qslib-server state, cheaply cloneable via `Arc`.
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
    /// Absolute path of the running executable (for in-place `/upgrade`).
    pub exe_path: PathBuf,
    /// SHA-256 (lowercase hex) of the running executable, computed at startup.
    /// Reported by `/health` so a client can confirm which build is live.
    pub exe_sha256: String,
    /// The argv (excluding argv[0]) this process was launched with, replayed
    /// verbatim when `/upgrade` restarts into the new binary.
    pub restart_args: Vec<String>,
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

        // Resolve the running executable and hash it so /upgrade can verify and
        // /health can report the live build. A failure here is non-fatal for
        // serving files/SCPI; /upgrade will just be unavailable.
        let exe_path = std::env::current_exe().unwrap_or_default();
        let exe_sha256 = match std::fs::read(&exe_path) {
            Ok(bytes) => sha256_hex(&bytes),
            Err(_) => String::new(),
        };
        let restart_args: Vec<String> = std::env::args().skip(1).collect();

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
            exe_path,
            exe_sha256,
            restart_args,
        })))
    }
}

/// Lowercase-hex SHA-256 of `bytes`.
pub fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(bytes);
    let mut s = String::with_capacity(64);
    for b in digest {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

impl std::ops::Deref for AppState {
    type Target = AppStateInner;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
