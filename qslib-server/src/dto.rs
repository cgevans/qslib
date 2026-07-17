//! Stable `/api/v1` data-transfer types.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use qslib_core::commands::{
    AccessState, BlockStatus, MachineStatus, PowerStatus, RunStatus, StatusLedState,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessDto {
    pub level: String,
    pub exclusive: bool,
    pub stealth: bool,
}

impl From<AccessState> for AccessDto {
    fn from(value: AccessState) -> Self {
        Self {
            level: String::from(value.level).to_ascii_lowercase(),
            exclusive: value.exclusive,
            stealth: value.stealth,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndicatorDto {
    pub color: Option<String>,
    pub mode: String,
}

impl From<StatusLedState> for IndicatorDto {
    fn from(value: StatusLedState) -> Self {
        Self {
            color: value.color.map(|color| color.name().to_ascii_lowercase()),
            mode: value.mode.name().to_ascii_lowercase(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockDto {
    pub enabled: bool,
    pub target_c: f64,
}

impl From<BlockStatus> for BlockDto {
    fn from(value: BlockStatus) -> Self {
        Self {
            enabled: value.enabled,
            target_c: value.target_c,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunStatusDto {
    pub name: String,
    pub stage: i64,
    pub stage_name: String,
    pub num_stages: i64,
    pub cycle: i64,
    pub num_cycles: i64,
    pub step: i64,
    pub point: i64,
    pub state: String,
    pub remaining_time_s: Option<i64>,
}

/// Exact protocol currently being executed by the instrument.
///
/// `scpi` is authoritative. The remaining fields let API clients identify the
/// protocol without partially parsing the command.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunningProtocolDto {
    pub name: String,
    pub sample_volume: f64,
    pub run_mode: String,
    pub scpi: String,
}

impl RunStatusDto {
    pub fn from_parts(value: RunStatus, remaining_time_s: Option<i64>) -> Self {
        Self {
            name: value.name,
            stage: value.stage,
            stage_name: value.stage_name,
            num_stages: value.num_stages,
            cycle: value.cycle,
            num_cycles: value.num_cycles,
            step: value.step,
            point: value.point,
            state: value.state,
            remaining_time_s,
        }
    }
}

impl From<RunStatus> for RunStatusDto {
    fn from(value: RunStatus) -> Self {
        Self::from_parts(value, None)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstrumentStatusDto {
    pub observed_at: DateTime<Utc>,
    pub power_enabled: bool,
    pub block: BlockDto,
    pub zone_count: usize,
    pub drawer: String,
    pub cover: String,
    pub lamp_status: String,
    pub sample_temperatures_c: Vec<f64>,
    pub block_temperatures_c: Vec<f64>,
    pub cover_temperature_c: f64,
    pub target_temperatures_c: HashMap<String, f64>,
    pub target_controlled: HashMap<String, bool>,
    pub led_temperature_c: f64,
    pub indicator: IndicatorDto,
    pub run: RunStatusDto,
}

impl InstrumentStatusDto {
    pub fn from_parts(
        machine: MachineStatus,
        power: PowerStatus,
        block: BlockStatus,
        zone_count: usize,
        indicator: StatusLedState,
        run: RunStatus,
        remaining_time_s: Option<i64>,
    ) -> Self {
        Self {
            observed_at: Utc::now(),
            power_enabled: matches!(power, PowerStatus::On),
            block: block.into(),
            zone_count,
            drawer: machine.drawer,
            cover: machine.cover,
            lamp_status: machine.lamp_status,
            sample_temperatures_c: machine.sample_temperatures,
            block_temperatures_c: machine.block_temperatures,
            cover_temperature_c: machine.cover_temperature,
            target_temperatures_c: machine.target_temperatures,
            target_controlled: machine.target_controlled,
            led_temperature_c: machine.led_temperature,
            indicator: indicator.into(),
            run: RunStatusDto::from_parts(run, remaining_time_s),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapabilitiesDto {
    pub api_version: &'static str,
    pub resources: Vec<&'static str>,
    pub file_contexts: Vec<String>,
    pub max_access: String,
    pub sse: bool,
    pub sse_cursor_format: &'static str,
    pub raw_scpi: bool,
    pub scpi_tunnel: bool,
    pub file_writes: bool,
    pub controls: bool,
}
