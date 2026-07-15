//! qslib-server configuration and command-line parsing.

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;
use qslib_core::commands::AccessLevel;

use crate::auth::{AuthPolicy, Role};

/// Optional semantic API and connection manager for QuantStudio machines.
#[derive(Debug, Clone, Parser)]
#[command(name = "qslib-server", version, about)]
pub struct Config {
    /// Private interface to bind. Avoid wildcard addresses on instruments.
    #[arg(long, default_value = "127.0.0.1:7500")]
    pub listen: SocketAddr,

    /// Local plaintext InstrumentServer SCPI endpoint.
    #[arg(long, default_value = "127.0.0.1:7000")]
    pub scpi_target: SocketAddr,

    /// Base for the default, experiments, runs, logs, templates, and
    /// calibrations named file contexts.
    #[arg(long, default_value = "/data/vendor/IS")]
    pub file_root: PathBuf,

    /// Root-readable TOML file containing role-bearing token hashes.
    #[arg(long, env = "QSLIB_SERVER_AUTH_CONFIG")]
    pub auth_config: Option<PathBuf>,

    /// Disable token authentication. Requires an explicit role cap.
    #[arg(long)]
    pub no_auth: bool,

    /// Role assigned when --no-auth is used.
    #[arg(long, default_value = "observer", value_parser = parse_role)]
    pub unauthenticated_role: Role,

    /// Absolute cap on access the managed actor may request.
    #[arg(long, default_value = "Controller", value_parser = parse_access_level)]
    pub max_access: AccessLevel,

    /// Instrument password used once on every managed connection.
    #[arg(long, env = "QSLIB_SERVER_SCPI_PASSWORD", hide_env_values = true)]
    pub scpi_password: Option<String>,

    /// Bounded semantic-operation queue size.
    #[arg(long, default_value_t = 64)]
    pub queue_capacity: usize,

    /// Permit Controller-role writes to named file contexts.
    #[arg(long)]
    pub allow_file_writes: bool,

    /// Permit Controller-role hardware and run controls.
    #[arg(long)]
    pub allow_controls: bool,

    /// Enable Administrator-only isolated one-shot SCPI.
    #[arg(long)]
    pub enable_raw_scpi: bool,

    /// Enable Administrator-only separately connected SCPI tunnels.
    #[arg(long)]
    pub enable_scpi_tunnel: bool,

    /// Maximum number of separately connected SCPI tunnels.
    #[arg(long, default_value_t = 16)]
    pub max_tunnels: usize,

    /// Write structured logs to this file instead of stderr.
    #[arg(long)]
    pub log: Option<PathBuf>,
}

fn parse_access_level(value: &str) -> Result<AccessLevel, String> {
    AccessLevel::try_from(value.to_string()).map_err(|_| {
        format!("invalid access level `{value}` (expected Guest|Observer|Controller|Administrator|Full)")
    })
}

fn parse_role(value: &str) -> Result<Role, String> {
    value.parse()
}

impl Config {
    pub fn resolve_auth(&self) -> anyhow::Result<AuthPolicy> {
        match (self.no_auth, self.auth_config.as_deref()) {
            (true, Some(_)) => anyhow::bail!("--no-auth and --auth-config are mutually exclusive"),
            (true, None) => Ok(AuthPolicy::unauthenticated(self.unauthenticated_role)),
            (false, Some(path)) => AuthPolicy::from_file(path),
            (false, None) => anyhow::bail!(
                "authentication is required: pass --auth-config (or QSLIB_SERVER_AUTH_CONFIG), or explicitly use --no-auth --unauthenticated-role observer"
            ),
        }
    }
}
