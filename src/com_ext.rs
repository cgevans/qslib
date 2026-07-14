//! Extension methods on the core [`QSConnection`] that return qslib domain
//! objects (plate setup, filter data, parsed protocol).
//!
//! These live in the `qslib` crate rather than `qslib-core` so the core stays
//! free of Polars/EDS types; the `qslib-server` binary does not pull them
//! in. Each method uses only the public core connection API (send a command,
//! read a response, fetch an experiment file) and then parses the result into a
//! qslib type.

use anyhow::Context;
use bstr::ByteSlice;

use crate::agent_client::AgentClient;
use crate::com::{CommandError, FilterDataFilename, QSConnection};
use crate::data::{FilterDataCollection, PlateData};
use crate::parser::{Command, ErrorResponse};
use crate::plate_setup::PlateSetup;
use crate::protocol::Protocol;

/// Absolute on-instrument root of the experiments tree (the `experiments` SCPI
/// file context; see the Python `_SCPI_CONTEXT_ROOTS`). Used to turn a
/// run-relative experiment path into the absolute path qslib-server serves.
pub const INSTRUMENT_EXPERIMENTS_ROOT: &str = "/data/vendor/IS/experiments";

/// Domain-object convenience methods on a core SCPI [`QSConnection`].
///
/// Bring this trait into scope to call the methods on a connection value.
///
/// The `_via` variants take an optional [`AgentClient`]: when present, the
/// underlying experiment file is fetched over qslib-server's `/file` (raw off
/// disk, no base64+TLS overhead on the instrument), falling back to SCPI on any
/// agent error. The plain methods are the SCPI-only path (`agent = None`).
#[allow(async_fn_in_trait)]
pub trait QSConnectionExt {
    /// Fetch and parse the plate setup for a run (or the current run).
    async fn get_plate_setup(
        &self,
        run: Option<String>,
    ) -> Result<PlateSetup, CommandError<ErrorResponse>>;

    /// Like [`get_plate_setup`](Self::get_plate_setup), preferring qslib-server
    /// when `agent` is provided.
    async fn get_plate_setup_via(
        &self,
        agent: Option<&AgentClient>,
        run: Option<String>,
    ) -> Result<PlateSetup, CommandError<ErrorResponse>>;

    /// Fetch the currently running protocol and parse it into a [`Protocol`].
    async fn get_running_protocol(&self) -> Result<Protocol, CommandError<ErrorResponse>>;

    /// Fetch and parse a single filter-data file into a [`PlateData`].
    async fn get_filterdata_one(
        &self,
        fref: FilterDataFilename,
        run: Option<String>,
    ) -> Result<PlateData, CommandError<ErrorResponse>>;

    /// Like [`get_filterdata_one`](Self::get_filterdata_one), preferring
    /// qslib-server when `agent` is provided.
    async fn get_filterdata_one_via(
        &self,
        agent: Option<&AgentClient>,
        fref: FilterDataFilename,
        run: Option<String>,
    ) -> Result<PlateData, CommandError<ErrorResponse>>;
}

/// Fetch an experiment file under a run's `apldbio/sds/` directory, preferring
/// qslib-server's `/file` when `agent` is set and falling back to SCPI.
///
/// `sds_subpath` is relative to `apldbio/sds/` (e.g. `"plate_setup.xml"` or
/// `"filter/<name>"`). `scpi_var_path` is the SCPI-side path used when there is
/// no agent or the agent fetch fails (using the server's `${LogFolder}` /
/// `${FilterFolder}` variables for the current run).
async fn fetch_sds_file(
    con: &QSConnection,
    agent: Option<&AgentClient>,
    run: &Option<String>,
    sds_subpath: &str,
    scpi_var_path: &str,
) -> Result<Vec<u8>, CommandError<ErrorResponse>> {
    if let Some(agent) = agent {
        // The agent serves absolute paths, so resolve the run title (the
        // experiment folder name) for the current run.
        let runtitle = match run {
            Some(r) => Some(r.clone()),
            None => con.get_run_title().await.ok(),
        };
        if let Some(rt) = runtitle {
            let abspath = format!("{INSTRUMENT_EXPERIMENTS_ROOT}/{rt}/apldbio/sds/{sds_subpath}");
            match agent.get_abs_file(&abspath).await {
                Ok(bytes) => return Ok(bytes),
                Err(e) => {
                    log::debug!("qslib-server fetch of {abspath} failed ({e}); falling back to SCPI");
                }
            }
        }
    }
    let path = match run {
        Some(r) => format!("{r}/apldbio/sds/{sds_subpath}"),
        None => scpi_var_path.to_string(),
    };
    con.get_exp_file(&path).await
}

impl QSConnectionExt for QSConnection {
    async fn get_plate_setup(
        &self,
        run: Option<String>,
    ) -> Result<PlateSetup, CommandError<ErrorResponse>> {
        self.get_plate_setup_via(None, run).await
    }

    async fn get_plate_setup_via(
        &self,
        agent: Option<&AgentClient>,
        run: Option<String>,
    ) -> Result<PlateSetup, CommandError<ErrorResponse>> {
        let x = fetch_sds_file(
            self,
            agent,
            &run,
            "plate_setup.xml",
            "${LogFolder}/plate_setup.xml",
        )
        .await?;
        let plate_setup: PlateSetup = quick_xml::de::from_str(&x.to_str_lossy())
            .with_context(|| "PlateSetup deserialization error")
            .map_err(CommandError::InternalError)?;

        Ok(plate_setup)
    }

    async fn get_running_protocol(&self) -> Result<Protocol, CommandError<ErrorResponse>> {
        let prot_command = self.get_running_protocol_string().await?;

        // Parse into Command and then Protocol
        let cmd = Command::try_from(prot_command.clone()).map_err(|e| {
            CommandError::InternalError(anyhow::anyhow!("Failed to parse protocol command: {}", e))
        })?;

        Protocol::from_scpicommand(&cmd).map_err(|e| {
            CommandError::InternalError(anyhow::anyhow!("Failed to parse protocol: {}", e))
        })
    }

    async fn get_filterdata_one(
        &self,
        fref: FilterDataFilename,
        run: Option<String>,
    ) -> Result<PlateData, CommandError<ErrorResponse>> {
        self.get_filterdata_one_via(None, fref, run).await
    }

    async fn get_filterdata_one_via(
        &self,
        agent: Option<&AgentClient>,
        fref: FilterDataFilename,
        run: Option<String>,
    ) -> Result<PlateData, CommandError<ErrorResponse>> {
        let x = fetch_sds_file(
            self,
            agent,
            &run,
            &format!("filter/{fref}"),
            &format!("${{FilterFolder}}/{fref}"),
        )
        .await?;

        let filter_data_collection: FilterDataCollection =
            quick_xml::de::from_str(&x.to_str_lossy())
                .with_context(|| "PlatePointData deserialization error")
                .map_err(CommandError::InternalError)?;

        let plate_point_data = filter_data_collection
            .plate_point_data
            .into_iter()
            .next()
            .ok_or_else(|| CommandError::InternalError(anyhow::anyhow!("No PlatePointData found")))?;
        let plate_data = plate_point_data
            .plate_data
            .into_iter()
            .next()
            .ok_or_else(|| CommandError::InternalError(anyhow::anyhow!("No PlateData found")))?;
        Ok(plate_data)
    }
}
