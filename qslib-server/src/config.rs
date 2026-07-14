//! qslib-server configuration and command-line parsing.

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;
use qslib_core::commands::AccessLevel;

/// On-instrument HTTP transport/command service for QuantStudio machines.
///
/// Serves, over plain HTTP on a single private-interface port: bulk file
/// transfer straight off disk (`/file`), a one-shot SCPI command call
/// (`/scpi`), and a streaming SCPI tunnel. It is a client of the existing
/// localhost plaintext SCPI server and a reader of on-disk experiment files;
/// it does not modify the InstrumentServer.
#[derive(Debug, Clone, Parser)]
#[command(name = "qslib-server", version, about)]
pub struct Config {
    /// Address to bind, e.g. `169.254.217.190:7500`. Bind the private eth0 IP
    /// only, never `0.0.0.0`.
    #[arg(long, default_value = "127.0.0.1:7500")]
    pub listen: SocketAddr,

    /// Localhost plaintext SCPI endpoint of the InstrumentServer.
    #[arg(long, default_value = "127.0.0.1:7000")]
    pub scpi_target: SocketAddr,

    /// Root directory under which `/file` paths are resolved. Requests cannot
    /// escape this directory.
    #[arg(long, default_value = "/data/vendor/IS")]
    pub file_root: PathBuf,

    /// Default SCPI access level for `/scpi` calls that do not request one.
    #[arg(long, default_value = "Observer", value_parser = parse_access_level)]
    pub default_access: AccessLevel,

    /// Maximum access level `/scpi` will ever elevate to (a safety cap).
    #[arg(long, default_value = "Controller", value_parser = parse_access_level)]
    pub max_access: AccessLevel,

    /// Bearer token required on every request. If omitted, the token is read
    /// from the `QSLIB_SERVER_TOKEN` environment variable, or from
    /// `--token-file`. Auth is on by default; use `--no-auth` to disable.
    #[arg(long, env = "QSLIB_SERVER_TOKEN", hide_env_values = true)]
    pub token: Option<String>,

    /// Read the bearer token from this file (first line, trimmed).
    #[arg(long)]
    pub token_file: Option<PathBuf>,

    /// Disable bearer-token authentication entirely. Only appropriate on a
    /// fully trusted private link.
    #[arg(long)]
    pub no_auth: bool,

    /// Password used to authenticate for password-gated SCPI access levels.
    #[arg(long, env = "QSLIB_SERVER_SCPI_PASSWORD", hide_env_values = true)]
    pub scpi_password: Option<String>,

    /// Reserved: warm-connection pool size for `/scpi`. Not yet implemented;
    /// `/scpi` uses connect-per-request (0 = default). A non-zero value is
    /// accepted but ignored (with a warning).
    #[arg(long, default_value_t = 0)]
    pub pool_size: usize,

    /// Write structured logs to this file instead of stderr.
    #[arg(long)]
    pub log: Option<PathBuf>,

    /// Default per-request SCPI timeout, in milliseconds.
    #[arg(long, default_value_t = 30_000)]
    pub scpi_timeout_ms: u64,

    /// Maximum number of concurrent SCPI tunnels. Bounds how many localhost
    /// SCPI connections abandoned/idle tunnels can pin open.
    #[arg(long, default_value_t = 16)]
    pub max_tunnels: usize,
}

fn parse_access_level(s: &str) -> Result<AccessLevel, String> {
    AccessLevel::try_from(s.to_string()).map_err(|_| {
        format!("invalid access level `{s}` (expected Guest|Observer|Controller|Administrator|Full)")
    })
}

impl Config {
    /// Resolve the effective bearer token, honouring `--token`, `--token-file`,
    /// the environment, and `--no-auth`. Returns an error if auth is required
    /// but no token was supplied.
    pub fn resolve_token(&self) -> anyhow::Result<Option<String>> {
        if self.no_auth {
            return Ok(None);
        }
        if let Some(t) = &self.token {
            let t = t.trim();
            if !t.is_empty() {
                return Ok(Some(t.to_string()));
            }
        }
        if let Some(path) = &self.token_file {
            let contents = std::fs::read_to_string(path)
                .map_err(|e| anyhow::anyhow!("failed to read --token-file {:?}: {e}", path))?;
            let t = contents.lines().next().unwrap_or("").trim().to_string();
            if !t.is_empty() {
                return Ok(Some(t));
            }
        }
        anyhow::bail!(
            "no bearer token configured. Pass --token, --token-file, set QSLIB_SERVER_TOKEN, \
             or explicitly disable authentication with --no-auth."
        )
    }
}
