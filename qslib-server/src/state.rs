//! Shared immutable application state and named filesystem contexts.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Instant;

use axum::http::StatusCode;

use crate::auth::AuthPolicy;
use crate::config::Config;
use crate::error::ServerError;
use crate::events::EventHub;
use crate::operation::OperationStore;
use crate::service::{InstrumentService, ServiceConfig};

#[derive(Clone)]
pub struct AppState(pub Arc<AppStateInner>);

pub struct AppStateInner {
    pub auth: AuthPolicy,
    pub max_access: qslib_core::commands::AccessLevel,
    pub scpi_target: std::net::SocketAddr,
    pub scpi_password: Option<String>,
    pub contexts: BTreeMap<String, PathBuf>,
    pub allow_file_writes: bool,
    pub allow_controls: bool,
    pub enable_raw_scpi: bool,
    pub enable_scpi_tunnel: bool,
    pub tunnels: Arc<tokio::sync::Semaphore>,
    pub service: InstrumentService,
    pub events: EventHub,
    pub operations: OperationStore,
    pub started: Instant,
    pub upgrade_in_progress: AtomicBool,
    pub exe_path: PathBuf,
    pub exe_sha256: String,
    pub restart_args: Vec<String>,
}

impl AppState {
    pub fn new(config: &Config, auth: AuthPolicy) -> anyhow::Result<Self> {
        let default_root = std::fs::canonicalize(&config.file_root).map_err(|error| {
            anyhow::anyhow!(
                "failed to canonicalize --file-root {:?}: {error}",
                config.file_root
            )
        })?;
        let contexts = build_contexts(&default_root);
        let events = EventHub::new();
        let operations = OperationStore::new(events.clone());
        let service = InstrumentService::spawn(
            ServiceConfig {
                target: config.scpi_target,
                password: config.scpi_password.clone(),
                max_access: config.max_access.clone(),
                queue_capacity: config.queue_capacity,
            },
            events.clone(),
        );

        let exe_path = std::env::current_exe().unwrap_or_default();
        let exe_sha256 = std::fs::read(&exe_path)
            .map(|bytes| sha256_hex(&bytes))
            .unwrap_or_default();

        Ok(Self(Arc::new(AppStateInner {
            auth,
            max_access: config.max_access.clone(),
            scpi_target: config.scpi_target,
            scpi_password: config.scpi_password.clone(),
            contexts,
            allow_file_writes: config.allow_file_writes,
            allow_controls: config.allow_controls,
            enable_raw_scpi: config.enable_raw_scpi,
            enable_scpi_tunnel: config.enable_scpi_tunnel,
            tunnels: Arc::new(tokio::sync::Semaphore::new(config.max_tunnels.max(1))),
            service,
            events,
            operations,
            started: Instant::now(),
            upgrade_in_progress: AtomicBool::new(false),
            exe_path,
            exe_sha256,
            restart_args: std::env::args().skip(1).collect(),
        })))
    }

    pub fn context_root(&self, context: &str) -> Result<&Path, ServerError> {
        self.contexts
            .get(&context.to_ascii_lowercase())
            .map(PathBuf::as_path)
            .ok_or_else(|| {
                ServerError::coded(
                    StatusCode::NOT_FOUND,
                    "unknown_context",
                    format!("unknown file context {context:?}"),
                )
            })
    }
}

fn build_contexts(default_root: &Path) -> BTreeMap<String, PathBuf> {
    let mut contexts = BTreeMap::new();
    contexts.insert("default".to_string(), default_root.to_path_buf());
    for name in ["experiments", "runs", "logs", "templates", "calibrations"] {
        contexts.insert(name.to_string(), context_path(default_root.join(name)));
    }

    // Production InstrumentServer places completion contexts on /sdcard. For
    // alternate/test roots, keep every named context beneath that root unless
    // those production directories actually exist.
    let production_layout = default_root == Path::new("/data/vendor/IS");
    for name in ["public_run_complete", "private_run_complete"] {
        let path = if production_layout {
            PathBuf::from("/sdcard").join(name)
        } else {
            default_root.join(name)
        };
        contexts.insert(name.to_string(), context_path(path));
    }
    contexts
}

fn context_path(path: PathBuf) -> PathBuf {
    std::fs::canonicalize(&path).unwrap_or(path)
}

/// Lowercase hexadecimal SHA-256 of `bytes`.
pub fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write;
        let _ = write!(output, "{byte:02x}");
    }
    output
}

impl std::ops::Deref for AppState {
    type Target = AppStateInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
