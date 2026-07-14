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

use crate::com::{CommandError, FilterDataFilename, QSConnection};
use crate::data::{FilterDataCollection, PlateData};
use crate::parser::{Command, ErrorResponse};
use crate::plate_setup::PlateSetup;
use crate::protocol::Protocol;

/// Domain-object convenience methods on a core SCPI [`QSConnection`].
///
/// Bring this trait into scope to call the methods on a connection value.
#[allow(async_fn_in_trait)]
pub trait QSConnectionExt {
    /// Fetch and parse the plate setup for a run (or the current run).
    async fn get_plate_setup(
        &self,
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
}

impl QSConnectionExt for QSConnection {
    async fn get_plate_setup(
        &self,
        run: Option<String>,
    ) -> Result<PlateSetup, CommandError<ErrorResponse>> {
        let path = match run {
            Some(r) => format!("{}/apldbio/sds/plate_setup.xml", r),
            None => "${LogFolder}/plate_setup.xml".to_string(),
        };
        let x = self.get_exp_file(&path).await?;
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
        let path = match run {
            Some(r) => format!("{}/apldbio/sds/filter/{}", r, fref),
            None => format!("${{FilterFolder}}/{}", fref),
        };
        let x = self.get_exp_file(&path).await?;

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
