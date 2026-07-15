// Raised for trait-solver recursion when proving `Send` of matrix-sdk's deeply
// nested async sync() future passed to tokio::spawn. See rust-lang/rust#152942.
#![recursion_limit = "512"]

use anyhow::Result;
use clap::Parser;
use dashmap::DashMap;
use env_logger::Env;
use futures::stream;
use influxdb2::Client;
use influxdb2::models::DataPoint;
use log::{debug, error, info, warn};
use qslib::com_ext::QSConnectionExt;
use qslib::data::FilterDataCollection;
use qslib::parser::OkResponse;
use qslib::server_client::{InstrumentStatus, ServerClient};
use qslib::{
    com::FilterDataFilename,
    com::QSConnection,
    commands::{
        AccessLevel, AccessLevelSet, CommandBuilder, ControlZonesQuery, PossibleRunProgress,
        QuickStatusQuery,
    },
    parser::LogMessage,
    plate_setup::PlateSetup,
};
use serde_derive::Deserialize;
use std::path::PathBuf;
use std::sync::Arc;
use thiserror::Error;
use tokio::select;
use tokio::sync::mpsc;
use tokio::task::{Id, JoinSet};
use tokio::time::{Duration, interval};
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::{StreamExt, StreamMap, wrappers::BroadcastStream};

mod matrix;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value = "config.toml")]
    config: PathBuf,

    #[arg(short, long, default_value = "info")]
    #[arg(value_enum)]
    log_level: log::LevelFilter,
}

#[derive(Debug, Deserialize)]
struct Config {
    global: Option<GlobalConfig>,
    machines: Vec<MachineConfig>,
    matrix: Option<matrix::MatrixSettings>,
    influxdb: Option<InfluxDBConfig>,
    #[allow(dead_code)] // currently ignoring this option, but may exist
    stdout: Option<()>,
}

#[derive(Debug, Deserialize)]
struct GlobalConfig {
    reconnect_wait_seconds: Option<u64>,
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct MachineConfig {
    pub(crate) name: String,
    pub(crate) host: String,
    /// Port of the optional qslib-server semantic API. When absent, qs-monitor
    /// uses its permanent direct-SCPI subscription mode.
    #[serde(default, alias = "agent_port")]
    pub(crate) server_port: Option<u16>,
    /// Bearer token for qslib-server. May be omitted when the server ACL grants
    /// an unauthenticated role sufficient for monitoring.
    pub(crate) server_token: Option<String>,
}

/// Build a qslib-server client only when server mode was explicitly selected.
fn build_server_client(config: &MachineConfig) -> Option<Arc<ServerClient>> {
    match config.server_port {
        None | Some(0) => None,
        Some(port) => Some(Arc::new(ServerClient::new(
            &config.host,
            port,
            config.server_token.clone(),
        ))),
    }
}

#[derive(Debug, Deserialize, Clone)]
struct InfluxDBConfig {
    url: String,
    org: String,
    bucket: String,
    token: String,
    batch_size: Option<usize>,
    flush_interval_ms: Option<u64>,
}

#[derive(Debug, Error)]
pub enum QSConnectionError {
    #[error("Missing required argument: {0}")]
    MissingArgument(String),

    #[error("Invalid argument {0}: {1}")]
    InvalidArgument(String, String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("InfluxDB error: {0}")]
    InfluxError(#[from] influxdb2::RequestError),

    #[error("QS connection error: {0}")]
    QSError(#[from] qslib::com::QSConnectionError),

    #[error("Data error: {0}")]
    DataError(#[from] qslib::data::DataError),

    #[error("Path strip prefix error: {0}")]
    StripPrefixError(#[from] std::path::StripPrefixError),
}

struct MachineState {
    zone_targets: Vec<f64>,
    run_name: Option<String>,
    stage: Option<i64>,
    cycle: Option<i64>,
    step: Option<i64>,
    plate_setup: Option<PlateSetup>,
    /// The run name that `plate_setup` was fetched for. The plate setup only
    /// changes when the run does, so it is refetched only when this no longer
    /// matches `run_name` — not on every Run message.
    plate_setup_run: Option<String>,
}

impl MachineState {
    fn new(num_zones: usize) -> Self {
        Self {
            zone_targets: vec![25.0; num_zones],
            run_name: None,
            stage: None,
            cycle: None,
            step: None,
            plate_setup: None,
            plate_setup_run: None,
        }
    }

    #[cfg(test)]
    fn default_idle() -> Self {
        Self::new(6)
    }

    fn update_from_quick_status(&mut self, qs: &qslib::commands::QuickStatus) {
        self.zone_targets = qs.set_temperatures.zones.clone();
        match &qs.runprogress {
            PossibleRunProgress::Running(rp) => {
                self.stage = rp.stage.parse::<i64>().ok();
                self.cycle = rp.cycle.parse::<i64>().ok();
                self.step = rp.step.parse::<i64>().ok();
            }
            PossibleRunProgress::NotRunning(_) => {
                self.stage = None;
                self.cycle = None;
                self.step = None;
            }
        }
    }

    fn update_zone_targets_from_ramping(&mut self, targets: &[f64]) {
        self.zone_targets = targets.to_vec();
    }
}

fn load_config(path: PathBuf) -> Result<Config> {
    let settings = config::Config::builder()
        .add_source(config::File::from(path))
        .build()?;

    Ok(settings.try_deserialize()?)
}

async fn refresh_state(
    con: &QSConnection,
    state: &mut MachineState,
    machine_name: &str,
    timestamp: chrono::DateTime<chrono::Utc>,
    server: Option<&ServerClient>,
) -> Vec<DataPoint> {
    let mut points = Vec::new();
    let ts = match timestamp.timestamp_nanos_opt() {
        Some(ts) => ts,
        None => {
            warn!("Timestamp out of range in refresh_state");
            return points;
        }
    };

    // 1. QuickStatusQuery → update state
    match QuickStatusQuery.send(con).await {
        Ok(mut v) => match v.receive_response().await {
            Ok(Ok(qs)) => {
                state.update_from_quick_status(&qs);
            }
            Ok(Err(e)) => {
                warn!("QuickStatus returned error: {}", e);
            }
            Err(e) => {
                warn!("Error receiving QuickStatus response: {}", e);
            }
        },
        Err(e) => {
            warn!("Error sending QuickStatusQuery: {}", e);
        }
    }

    // 2. Get current run name
    match con.get_current_run_name().await {
        Ok(name) => {
            state.run_name = name;
        }
        Err(e) => {
            warn!("Error getting current run name: {}", e);
        }
    }

    // 3. If run active, get plate setup — over qslib-server when available, and
    // only when the run changed (it is otherwise stable for the whole run).
    match &state.run_name {
        Some(run) => {
            let have_current = state.plate_setup.is_some()
                && state.plate_setup_run.as_deref() == Some(run.as_str());
            if !have_current {
                match con.get_plate_setup_via(server, Some(run.clone())).await {
                    Ok(ps) => {
                        state.plate_setup = Some(ps);
                        state.plate_setup_run = Some(run.clone());
                    }
                    Err(e) => {
                        warn!("Error getting plate setup: {}", e);
                    }
                }
            }
        }
        None => {
            state.plate_setup = None;
            state.plate_setup_run = None;
        }
    }

    // 4. Build run_state DataPoint
    let mut run_state_builder = DataPoint::builder("run_state").tag("machine", machine_name);
    if let Some(ref name) = state.run_name {
        run_state_builder = run_state_builder.field("name", name.clone());
    } else {
        run_state_builder = run_state_builder.field("name", "");
    }
    if let Some(stage) = state.stage {
        run_state_builder = run_state_builder.field("stage", stage);
    }
    if let Some(cycle) = state.cycle {
        run_state_builder = run_state_builder.field("cycle", cycle);
    }
    if let Some(step) = state.step {
        run_state_builder = run_state_builder.field("step", step);
    }
    match run_state_builder.timestamp(ts).build() {
        Ok(point) => points.push(point),
        Err(e) => {
            warn!("Error building run_state point: {}", e);
        }
    }

    // 5. If plate_setup exists, convert to line protocol and parse
    if let Some(ref ps) = state.plate_setup {
        let lp_lines = ps.to_lineprotocol(ts, state.run_name.as_deref(), None);
        for line in lp_lines {
            match parse_line_protocol_to_datapoint(&line, machine_name) {
                Ok(point) => points.push(point),
                Err(e) => {
                    warn!("Error parsing plate setup line protocol: {}", e);
                }
            }
        }
    }

    points
}

fn state_snapshot_points(
    state: &MachineState,
    machine_name: &str,
    timestamp: chrono::DateTime<chrono::Utc>,
) -> Vec<DataPoint> {
    let mut points = Vec::new();
    let Some(ts) = timestamp.timestamp_nanos_opt() else {
        warn!("Timestamp out of range while building state snapshot");
        return points;
    };
    let mut run_state = DataPoint::builder("run_state").tag("machine", machine_name);
    run_state = run_state.field("name", state.run_name.clone().unwrap_or_default());
    if let Some(stage) = state.stage {
        run_state = run_state.field("stage", stage);
    }
    if let Some(cycle) = state.cycle {
        run_state = run_state.field("cycle", cycle);
    }
    if let Some(step) = state.step {
        run_state = run_state.field("step", step);
    }
    match run_state.timestamp(ts).build() {
        Ok(point) => points.push(point),
        Err(error) => warn!("Error building run_state point: {error}"),
    }
    if let Some(plate) = &state.plate_setup {
        for line in plate.to_lineprotocol(ts, state.run_name.as_deref(), None) {
            match parse_line_protocol_to_datapoint(&line, machine_name) {
                Ok(point) => points.push(point),
                Err(error) => warn!("Error parsing plate setup line protocol: {error}"),
            }
        }
    }
    points
}

async fn write_points_to_influx(
    mut rx: mpsc::Receiver<(String, DataPoint)>,
    client: Client,
    bucket: String,
    batch_size: usize,
    flush_interval: Duration,
) -> Result<()> {
    let mut interval = interval(flush_interval);
    let mut points: Vec<DataPoint> = Vec::new();
    let mut last_flush = tokio::time::Instant::now();
    let mut batched = 0;
    let mut to_retry = Vec::new();

    info!("InfluxDB write task started.");

    loop {
        tokio::select! {
            // Check for new points
            point = rx.recv() => {
                match point {
                    Some((_machine, point)) => {
                        points.push(point);
                        batched += 1;
                        if batched >= batch_size {
                            debug!("Flushing {} points to InfluxDB (batch size reached)", points.len());
                            match client.write(&bucket, stream::iter(points.clone())).await { // FIXME
                                Ok(_) => {
                                    points.clear();
                                    last_flush = tokio::time::Instant::now();
                                    batched = 0;
                                }
                                Err(e) => {
                                    warn!("Error writing points to InfluxDB, will retry: {}", e);
                                    to_retry.append(&mut points);
                                    batched = 0;
                                }
                            }
                        }
                    }
                    None => break, // Channel closed
                }
            }
            // Flush on interval only if enough time has passed since last flush
            _ = interval.tick() => {
                if !points.is_empty() && last_flush.elapsed() >= flush_interval {
                    debug!("Flushing {} points to InfluxDB (interval reached)", points.len());
                    match client.write(&bucket, stream::iter(points.clone())).await { // FIXME
                        Ok(_) => {
                            points.clear();
                            last_flush = tokio::time::Instant::now();
                        }
                        Err(e) => {
                            warn!("Error writing points to InfluxDB, will retry: {}", e);
                            to_retry.append(&mut points);
                        }
                    }
                }
                if !to_retry.is_empty() {
                    debug!("Retrying {} points to InfluxDB", to_retry.len());
                    match client.write(&bucket, stream::iter(to_retry.clone())).await {
                        Ok(_) => {
                            to_retry.clear();
                        }
                        Err(e) => {
                            warn!("Error writing points to InfluxDB ({}), lost {} points", e, to_retry.len());
                            to_retry.clear();
                        }
                    }
                }
            }
        }
    }

    // Final flush of any remaining points
    if !points.is_empty() {
        debug!("Flushing {} points to InfluxDB (final flush)", points.len());
        let tosend = std::mem::take(&mut points);
        client.write(&bucket, stream::iter(tosend)).await?;
    }

    info!("InfluxDB write task completed.");

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Initialize logging
    env_logger::Builder::from_env(Env::default().default_filter_or(args.log_level.as_str())).init();

    let config = load_config(args.config)?;

    // Set up InfluxDB if configured
    let (tx, rx) = mpsc::channel(1000);
    let _influx_task = if let Some(influx_config) = config.influxdb.as_ref() {
        let client = Client::new(&influx_config.url, &influx_config.org, &influx_config.token);
        let batch_size = influx_config.batch_size.unwrap_or(100);
        let flush_interval =
            Duration::from_millis(influx_config.flush_interval_ms.unwrap_or(10000));
        let bucket = influx_config.bucket.clone();

        Some(tokio::spawn(async move {
            if let Err(e) =
                write_points_to_influx(rx, client, bucket, batch_size, flush_interval).await
            {
                error!("Error writing points to InfluxDB: {}", e);
            }
        }))
    } else {
        None
    };

    let conns = Arc::new(DashMap::new());

    let reconnect_wait = Duration::from_secs(
        config
            .global
            .as_ref()
            .and_then(|g| g.reconnect_wait_seconds)
            .unwrap_or(60),
    );

    for machine_config in config.machines.iter() {
        let machine_config = machine_config.clone();
        let conns_clone = conns.clone();
        let tx_clone = tx.clone();
        let _reconnect_wait_clone = reconnect_wait;

        tokio::spawn(async move {
            let mut backoff_secs = 1u64;
            const MAX_BACKOFF_SECS: u64 = 300;
            loop {
                if let Some(server) = build_server_client(&machine_config) {
                    match log_server_machine(server, &machine_config, tx_clone.clone()).await {
                        Ok(()) => warn!(
                            "Server event stream for {} ended; reconnecting",
                            machine_config.name
                        ),
                        Err(error) => error!(
                            "Server mode for {} failed: {}; retrying in {} seconds",
                            machine_config.name, error, backoff_secs
                        ),
                    }
                    tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                    backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF_SECS);
                    continue;
                }
                match QSConnection::connect_with_timeout(
                    &machine_config.host,
                    7443,
                    qslib::com::ConnectionType::SSL,
                    Duration::from_secs(10),
                )
                .await
                {
                    Ok(con) => {
                        backoff_secs = 1;
                        let con = Arc::new(con);
                        let mut log_tasks = JoinSet::new();
                        match log_machine(
                            con.clone(),
                            &machine_config,
                            tx_clone.clone(),
                            &mut log_tasks,
                        )
                        .await
                        {
                            Ok(_id) => {
                                conns_clone.insert(
                                    machine_config.name.clone(),
                                    (con, machine_config.clone()),
                                );
                                info!("Successfully connected to {}", machine_config.name);

                                // Wait for the logging task to complete (connection dropped)
                                if let Some(result) = log_tasks.join_next().await
                                    && let Err(e) = result
                                {
                                    error!(
                                        "Logging task for {} ended with error: {}",
                                        machine_config.name, e
                                    );
                                }

                                warn!(
                                    "Connection to {} dropped, attempting to reconnect",
                                    machine_config.name
                                );
                                if let Some((_, (old_con, _))) =
                                    conns_clone.remove(&machine_config.name)
                                {
                                    old_con.disconnect().await;
                                }
                            }
                            Err(e) => {
                                error!(
                                    "Error setting up logging for {}: {}, retrying in {} seconds",
                                    machine_config.name, e, backoff_secs
                                );
                                tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                                backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF_SECS);
                            }
                        }
                    }
                    Err(e) => {
                        error!(
                            "Error connecting to {}: {}, retrying in {} seconds",
                            machine_config.name, e, backoff_secs
                        );
                        tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                        backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF_SECS);
                    }
                }
            }
        });
    }

    let conns_clone = conns.clone();
    if let Some(matrix_config) = config.matrix.clone() {
        let _reconnect_wait_matrix = reconnect_wait;
        let machines_for_matrix = config.machines.clone();
        tokio::spawn(async move {
            let mut backoff_secs = 1u64;
            const MAX_BACKOFF_SECS: u64 = 300;
            loop {
                match matrix::setup_matrix(
                    &matrix_config,
                    conns_clone.clone(),
                    machines_for_matrix.clone(),
                )
                .await
                {
                    Ok(()) => {
                        backoff_secs = 1;
                        warn!("Matrix connection ended, attempting to reconnect");
                    }
                    Err(e) => {
                        error!(
                            "Error setting up Matrix: {}, retrying in {} seconds",
                            e, backoff_secs
                        );
                    }
                }
                tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF_SECS);
            }
        });
    }

    // Keep the main task alive (all other tasks run in background)
    // Sleep indefinitely - connections are handled in spawned tasks
    loop {
        tokio::time::sleep(Duration::from_secs(3600)).await;
    }
}

async fn log_machine(
    con: Arc<QSConnection>,
    config: &MachineConfig,
    tx: mpsc::Sender<(String, DataPoint)>,
    log_tasks: &mut JoinSet<()>,
) -> Result<Id> {
    let access = AccessLevelSet::new(AccessLevel::Observer);
    access.send(&con).await?.receive_response().await??;

    // Query actual zone count from the machine
    let num_zones = match ControlZonesQuery.send(&con).await {
        Ok(mut v) => match v.receive_response().await {
            Ok(Ok(n)) => n,
            _ => 6,
        },
        _ => 6,
    };

    let mut log_sub = con
        .subscribe_log_with_options(&["Temperature", "Time", "Run", "LEDStatus"], true)
        .await;

    // Initialize machine state
    let mut state = MachineState::new(num_zones);
    let timestamp = chrono::Utc::now();
    let server = build_server_client(config);
    let initial_points =
        refresh_state(&con, &mut state, &config.name, timestamp, server.as_deref()).await;
    for point in initial_points {
        if let Err(e) = tx.send((config.name.clone(), point)).await {
            warn!("Failed to send initial state point: {}", e);
        }
    }

    let config_clone = config.clone();

    let aborthandle = log_tasks.spawn(async move {
        if let Err(e) = influx_log_loop(
            &mut log_sub,
            tx,
            &config_clone,
            None,
            con.clone(),
            state,
            server,
        )
        .await
        {
            error!("Logging loop error: {}", e);
        }
    });
    let id = aborthandle.id();

    info!("Logging task started for {}", config.name);

    Ok(id)
}

/// qslib-server mode owns no normal SCPI connection. Status snapshots and the
/// server's resumable event stream feed the same conversion functions used by
/// direct subscriptions, keeping the Influx schema identical.
async fn log_server_machine(
    server: Arc<ServerClient>,
    config: &MachineConfig,
    tx: mpsc::Sender<(String, DataPoint)>,
) -> Result<()> {
    let capabilities = server.capabilities().await?;
    if !capabilities.sse || !capabilities.supports("instrument") {
        anyhow::bail!("qslib-server does not provide instrument status and SSE");
    }

    let status = server.instrument_status().await?;
    let mut state = MachineState::new(status.zone_count);
    let initial = refresh_state_server(&server, &status, &mut state, &config.name).await;
    for point in initial {
        tx.send((config.name.clone(), point)).await?;
    }

    info!("Server logging task started for {}", config.name);
    let mut events = server.event_stream(None).await;
    loop {
        let event = events.next().await?;
        if matches!(event.event.as_str(), "reset" | "connection") {
            if let Ok(status) = server.instrument_status().await {
                for point in refresh_state_server(&server, &status, &mut state, &config.name).await
                {
                    tx.send((config.name.clone(), point)).await?;
                }
            }
            continue;
        }
        if event.event == "operation" {
            continue;
        }

        let payload = event.data.get("data").unwrap_or(&event.data);
        let message = match payload.get("message").and_then(|value| value.as_str()) {
            Some(message) => message.to_string(),
            None => continue,
        };
        let instrument_timestamp = payload
            .get("instrument_timestamp")
            .and_then(|value| value.as_f64());
        let timestamp = instrument_timestamp
            .and_then(|seconds| {
                let whole = seconds.trunc() as i64;
                let nanos = ((seconds.fract().max(0.0)) * 1e9) as u32;
                chrono::DateTime::from_timestamp(whole, nanos)
            })
            .unwrap_or_else(chrono::Utc::now);
        let topic = match event.event.as_str() {
            "temperature" => "Temperature",
            "time" => "Time",
            "run" => "Run",
            "ledstatus" => "LEDStatus",
            _ => continue,
        };
        let message = LogMessage {
            topic: topic.to_string(),
            timestamp: instrument_timestamp,
            message,
        };

        let points = match topic {
            "Temperature" => temperature_to_lineprotocol(&message, &config.name, timestamp, &state),
            "Time" => time_to_lineprotocol(&message, &config.name, timestamp),
            "LEDStatus" => ledstatus_to_lineprotocol(&message, &config.name, timestamp),
            "Run" => {
                // Keep the snapshot-derived run title and targets current; the
                // Run event itself contains the fine-grained action.
                if let Ok(status) = server.instrument_status().await {
                    update_state_from_server_status(&mut state, &status);
                }
                run_to_lineprotocol(
                    &message,
                    &config.name,
                    timestamp,
                    None,
                    &mut state,
                    Some(&server),
                )
                .await
            }
            _ => unreachable!(),
        };
        match points {
            Ok(points) => {
                for point in points {
                    tx.send((config.name.clone(), point)).await?;
                }
            }
            Err(error) => error!(
                "Error converting server event for {}: {}",
                config.name, error
            ),
        }
    }
}

fn update_state_from_server_status(state: &mut MachineState, status: &InstrumentStatus) {
    state.run_name = (status.run.name != "-").then(|| status.run.name.clone());
    state.stage = (status.run.stage >= 0).then_some(status.run.stage);
    state.cycle = (status.run.cycle >= 0).then_some(status.run.cycle);
    state.step = (status.run.step >= 0).then_some(status.run.step);
    state.zone_targets = (1..=status.zone_count)
        .map(|zone| {
            status
                .target_temperatures_c
                .get(&format!("Zone{zone}"))
                .copied()
                .unwrap_or_default()
        })
        .collect();
}

async fn refresh_state_server(
    server: &ServerClient,
    status: &InstrumentStatus,
    state: &mut MachineState,
    machine_name: &str,
) -> Vec<DataPoint> {
    update_state_from_server_status(state, status);
    if let Some(run) = state.run_name.clone() {
        if state.plate_setup_run.as_deref() != Some(run.as_str()) {
            let path = format!("/data/vendor/IS/experiments/{run}/apldbio/sds/plate_setup.xml");
            match server.get_abs_file(&path).await {
                Ok(bytes) => match std::str::from_utf8(&bytes)
                    .ok()
                    .and_then(|xml| PlateSetup::from_xml(xml).ok())
                {
                    Some(plate) => {
                        state.plate_setup = Some(plate);
                        state.plate_setup_run = Some(run);
                    }
                    None => warn!("Could not parse plate setup returned by qslib-server"),
                },
                Err(error) => warn!("Could not fetch plate setup through qslib-server: {error}"),
            }
        }
    } else {
        state.plate_setup = None;
        state.plate_setup_run = None;
    }
    state_snapshot_points(state, machine_name, chrono::Utc::now())
}

async fn influx_log_loop(
    log_sub: &mut StreamMap<String, BroadcastStream<LogMessage>>,
    tx: mpsc::Sender<(String, DataPoint)>,
    config: &MachineConfig,
    timeout_secs: Option<u64>,
    con: Arc<QSConnection>,
    mut state: MachineState,
    server: Option<Arc<ServerClient>>,
) -> Result<()> {
    let machine_name = config.name.as_ref();
    let mut last_message = tokio::time::Instant::now();
    let timeout = Duration::from_secs(timeout_secs.unwrap_or(60));
    let mut check_interval = tokio::time::interval(Duration::from_secs(5));
    loop {
        select! {
            msg = log_sub.next() => {
                let (_, msg) = match msg {
                    Some(msg) => msg,
                    None => {
                        warn!("Machine {} disconnected", config.name);
                        return Ok(());
                    }
                };
                let msg = match msg {
                    Ok(msg) => msg,
                    Err(BroadcastStreamRecvError::Lagged(n)) => {
                        warn!("Machine {} connection lagged by {} messages", config.name, n);
                        continue;
                    }
                };

                debug!("Message: {:?}", msg);

                // Use server timestamp if available, otherwise fall back to local time
                let timestamp = msg.timestamp
                    .and_then(|ts| chrono::DateTime::from_timestamp(ts as i64, ((ts % 1.0) * 1e9) as u32))
                    .unwrap_or_else(chrono::Utc::now);

                // Safely convert points, logging errors instead of propagating
                let points = match msg.topic.as_str() {
                    "Temperature" => match temperature_to_lineprotocol(&msg, machine_name, timestamp, &state) {
                        Ok(points) => points,
                        Err(e) => {
                            error!("Error converting temperature data for {}: {}", config.name, e);
                            continue;
                        }
                    },
                    "Time" => match time_to_lineprotocol(&msg, machine_name, timestamp) {
                        Ok(points) => points,
                        Err(e) => {
                            error!("Error converting time data for {}: {}", config.name, e);
                            continue;
                        }
                    },
                    "Run" => match run_to_lineprotocol(&msg, machine_name, timestamp, Some(con.clone()), &mut state, server.as_deref()).await {
                        Ok(points) => points,
                        Err(e) => {
                            error!("Error converting run data for {}: {}", config.name, e);
                            continue;
                        }
                    },
                    "LEDStatus" => match ledstatus_to_lineprotocol(&msg, machine_name, timestamp) {
                        Ok(points) => points,
                        Err(e) => {
                            error!("Error converting LED status data for {}: {}", config.name, e);
                            continue;
                        }
                    },
                    _ => continue,
                };

                for point in points {
                    if let Err(e) = tx.send((config.name.clone(), point)).await {
                        error!("Failed to send point to InfluxDB for {}: {}", config.name, e);
                    }
                }

                last_message = tokio::time::Instant::now();
            }
            _ = check_interval.tick() => {
                if last_message.elapsed() > timeout {
                    warn!("No messages received from {} in {} seconds, disconnecting", config.name, timeout.as_secs_f32());
                    return Ok(());
                }
            }
        }
    }
}

fn ledstatus_to_lineprotocol(
    msg: &LogMessage,
    machine_name: &str,
    timestamp: chrono::DateTime<chrono::Utc>,
) -> Result<Vec<DataPoint>> {
    debug_assert!(msg.topic == "LEDStatus");
    // Example: "MESSage LEDStatus Temperature:56.1791 Current:9.18727 Voltage:3.41406 JuncTemp:72.8079"
    // Note this doesn't follow standard -key=value format, so each field will be an arg in the message
    // and will need to be parsed manually.

    let mut fields = Vec::with_capacity(4);
    let args = msg.message.split_ascii_whitespace();
    for arg in args {
        let (key, value) = arg
            .split_once(':')
            .ok_or(anyhow::anyhow!("Invalid format"))?;
        let val = value.parse::<f64>()?;
        fields.push((key.to_lowercase(), val));
    }
    Ok(vec![
        fields
            .into_iter()
            .fold(
                DataPoint::builder("lamp").tag("machine", machine_name),
                |builder, (key, value)| builder.field(key, value),
            )
            .timestamp(
                timestamp
                    .timestamp_nanos_opt()
                    .ok_or(anyhow::anyhow!("Timestamp out of range"))? as i64,
            )
            .build()?,
    ])
}

async fn run_to_lineprotocol(
    msg: &LogMessage,
    machine_name: &str,
    timestamp: chrono::DateTime<chrono::Utc>,
    con: Option<Arc<QSConnection>>,
    state: &mut MachineState,
    server: Option<&ServerClient>,
) -> Result<Vec<DataPoint>> {
    let mut points = Vec::new();
    let mut parts = msg.message.splitn(2, ' ');

    let action = parts.next().ok_or(anyhow::anyhow!("Missing action"))?;
    let remaining = parts.next().unwrap_or(""); // Get rest as single string
    let content = OkResponse::parse(&mut remaining.as_bytes())
        .map_err(|e| anyhow::anyhow!("Invalid message: {}", e))?;

    // Create base point for run_action
    let ts = timestamp
        .timestamp_nanos_opt()
        .ok_or(anyhow::anyhow!("Timestamp out of range"))? as i64;
    let mut point = DataPoint::builder("run_action")
        .tag("machine", machine_name)
        .tag("type", action.to_lowercase())
        .timestamp(ts);
    let run_name = match con.as_ref() {
        Some(con) => con
            .get_current_run_name()
            .await
            .map_err(|e| anyhow::anyhow!("Error getting current run name: {}", e))?,
        None => state.run_name.clone(),
    };
    if let Some(run_name) = run_name {
        point = point.field("run_name", run_name);
    }

    match action {
        "Stage" | "Cycle" | "Step" => {
            let value = content
                .args
                .first()
                .ok_or(anyhow::anyhow!("Missing value"))?
                .clone()
                .try_into_i64()
                .map_err(|e| anyhow::anyhow!("Missing value: {}", e))?;

            match action {
                "Stage" => state.stage = Some(value),
                "Cycle" => state.cycle = Some(value),
                "Step" => state.step = Some(value),
                _ => unreachable!(),
            }

            point = point.field(action.to_lowercase(), value);

            points.push(point.build()?);

            // Also create run_status point
            points.push(
                DataPoint::builder("run_status")
                    .tag("machine", machine_name)
                    .tag("type", action.to_lowercase())
                    .field(action.to_lowercase(), value)
                    .timestamp(ts)
                    .build()?,
            );
        }
        "Holding" => {
            let time = content
                .options
                .get("time")
                .ok_or(anyhow::anyhow!("Missing time"))?
                .clone()
                .try_into_f64()
                .map_err(|e| anyhow::anyhow!("Missing time: {}", e))?;
            point = point.field("holdtime", time);
            points.push(point.build()?);
        }
        "Ramping" => {
            let rates_str = content
                .options
                .get("rates")
                .ok_or(anyhow::anyhow!("Missing rates in Ramping message"))?
                .to_string();
            let rates: Result<Vec<f64>, _> = rates_str
                .split(',')
                .map(|s| {
                    s.parse::<f64>()
                        .map_err(|e| anyhow::anyhow!("Invalid rate value '{}': {}", s, e))
                })
                .collect();
            let rates = rates?;

            let zones_str = content
                .options
                .get("zones")
                .ok_or(anyhow::anyhow!("Missing zones in Ramping message"))?
                .to_string();
            let zones: Vec<String> = zones_str.split(',').map(|s| s.to_string()).collect();

            let targets_str = content
                .options
                .get("targets")
                .ok_or(anyhow::anyhow!("Missing targets in Ramping message"))?
                .to_string();
            let targets: Result<Vec<f64>, _> = targets_str
                .split(',')
                .map(|s| {
                    s.parse::<f64>()
                        .map_err(|e| anyhow::anyhow!("Invalid target value '{}': {}", s, e))
                })
                .collect();
            let targets = targets?;

            if rates.len() != zones.len() || rates.len() != targets.len() {
                return Err(anyhow::anyhow!(
                    "Mismatched lengths: rates={}, zones={}, targets={}",
                    rates.len(),
                    zones.len(),
                    targets.len()
                ));
            }

            state.update_zone_targets_from_ramping(&targets);

            for ((zone, rate), target) in zones.iter().zip(rates.iter()).zip(targets.iter()) {
                point = point.field(format!("rate_{}", zone), *rate);
                point = point.field(format!("target_{}", zone), *target);
            }
            points.push(point.build()?);
        }
        "Collected" => {
            let stage = content
                .options
                .get("stage")
                .ok_or(anyhow::anyhow!("Missing stage in Collected message"))?
                .to_string()
                .parse::<i64>()
                .map_err(|e| anyhow::anyhow!("Invalid stage value: {}", e))?;
            let cycle = content
                .options
                .get("cycle")
                .ok_or(anyhow::anyhow!("Missing cycle in Collected message"))?
                .to_string()
                .parse::<i64>()
                .map_err(|e| anyhow::anyhow!("Invalid cycle value: {}", e))?;
            let step = content
                .options
                .get("step")
                .ok_or(anyhow::anyhow!("Missing step in Collected message"))?
                .to_string()
                .parse::<i64>()
                .map_err(|e| anyhow::anyhow!("Invalid step value: {}", e))?;
            let run_point = content
                .options
                .get("point")
                .ok_or(anyhow::anyhow!("Missing point in Collected message"))?
                .to_string()
                .parse::<i64>()
                .map_err(|e| anyhow::anyhow!("Invalid point value: {}", e))?;
            point = point
                .field("stage", stage)
                .field("cycle", cycle)
                .field("step", step)
                .field("point", run_point);
            points.push(point.build()?);
            if let Some(con) = con.as_ref() {
                match docollect(
                    stage,
                    cycle,
                    step,
                    run_point,
                    con.clone(),
                    timestamp,
                    machine_name,
                    state,
                    server,
                )
                .await
                {
                    Ok(collected_points) => {
                        points.extend(collected_points);
                    }
                    Err(e) => {
                        error!("Error collecting data: {}", e);
                    }
                }
            } else if let Some(server) = server {
                match docollect_server(
                    stage,
                    cycle,
                    step,
                    run_point,
                    server,
                    timestamp,
                    machine_name,
                    state,
                )
                .await
                {
                    Ok(collected_points) => points.extend(collected_points),
                    Err(error) => error!("Error collecting data through qslib-server: {error}"),
                }
            }
        }
        "Acquiring" => {
            let stage = content
                .options
                .get("stage")
                .ok_or(anyhow::anyhow!("Missing stage in Acquiring message"))?
                .to_string()
                .parse::<i64>()
                .map_err(|e| anyhow::anyhow!("Invalid stage value: {}", e))?;
            let cycle = content
                .options
                .get("cycle")
                .ok_or(anyhow::anyhow!("Missing cycle in Acquiring message"))?
                .to_string()
                .parse::<i64>()
                .map_err(|e| anyhow::anyhow!("Invalid cycle value: {}", e))?;
            let run_point = content
                .options
                .get("point")
                .ok_or(anyhow::anyhow!("Missing point in Acquiring message"))?
                .to_string()
                .parse::<i64>()
                .map_err(|e| anyhow::anyhow!("Invalid point value: {}", e))?;

            point = point
                .field("stage", stage)
                .field("cycle", cycle)
                .field("point", run_point);

            if let Some(temperature_str) = content.options.get("Temperature") {
                let temperatures: Result<Vec<f64>, _> = temperature_str
                    .to_string()
                    .split(',')
                    .map(|s| {
                        s.parse::<f64>().map_err(|e| {
                            anyhow::anyhow!("Invalid temperature value '{}': {}", s, e)
                        })
                    })
                    .collect();
                let temperatures = temperatures?;
                for (i, temp) in temperatures.iter().enumerate() {
                    point = point.field(format!("temperature_zone_{}", i), *temp);
                }
            }

            points.push(point.build()?);
        }
        "Error" | "Ended" | "Aborted" | "Stopped" | "Starting" => {
            // Collect remaining message
            let remaining = content.to_string();
            if !remaining.is_empty() {
                point = point.field("message", remaining);
            }
            points.push(point.build()?);
        }
        _ => {
            // Handle other cases
            let message = format!("{} {}", action, content);
            point = point.tag("type", "Other").field("message", message);
            points.push(point.build()?);
        }
    }
    // After processing any Run message, refresh state if we have a connection
    if let Some(con) = con.as_ref() {
        let refresh_points = refresh_state(con, state, machine_name, timestamp, server).await;
        points.extend(refresh_points);
    }

    debug!("Points: {:?}", points);
    Ok(points)
}

fn temperature_to_lineprotocol(
    msg: &LogMessage,
    machine_name: &str,
    timestamp: chrono::DateTime<chrono::Utc>,
    state: &MachineState,
) -> Result<Vec<DataPoint>> {
    let mut points = Vec::new();
    let ts = timestamp
        .timestamp_nanos_opt()
        .ok_or(anyhow::anyhow!("Timestamp out of range"))? as i64;

    // Parse the message into key-value pairs
    let mut args = msg
        .message
        .split_ascii_whitespace()
        .filter(|s| s.contains('='))
        .map(|s| {
            let parts: Vec<&str> = s.trim_start_matches('-').split('=').collect();
            (parts[0].to_lowercase(), parts[1].to_string())
        });

    // Handle sample and block temperatures for each zone
    if let (Some((_, sample_str)), Some((_, block_str))) = (
        args.find(|(key, _)| key == "sample"),
        args.find(|(key, _)| key == "block"),
    ) {
        // Parse comma-separated values
        let sample_temps: Vec<f64> = sample_str
            .split(',')
            .filter_map(|s| s.parse().ok())
            .collect();

        let block_temps: Vec<f64> = block_str
            .split(',')
            .filter_map(|s| s.parse().ok())
            .collect();

        // Create points for each zone
        for (i, (sample, block)) in sample_temps.iter().zip(block_temps.iter()).enumerate() {
            let mut builder = DataPoint::builder("temperature")
                .tag("machine", machine_name)
                .tag("loc", "zones")
                .tag("zone", i.to_string())
                .field("sample", *sample)
                .field("block", *block);
            if let Some(target) = state.zone_targets.get(i) {
                builder = builder.field("target", *target);
            }
            points.push(builder.timestamp(ts).build()?);
        }
    }

    // Parse remaining args again since we consumed the iterator
    let args = msg
        .message
        .split_ascii_whitespace()
        .filter(|s| s.contains('='))
        .map(|s| {
            let parts: Vec<&str> = s.trim_start_matches('-').split('=').collect();
            (parts[0].to_lowercase(), parts[1].to_string())
        });

    // Handle cover and heatsink temperatures
    for (key, value) in args {
        match key.as_str() {
            "cover" => {
                points.push(
                    DataPoint::builder("temperature")
                        .tag("machine", machine_name)
                        .tag("loc", "cover")
                        .field("cover", value.parse::<f64>()?)
                        .timestamp(ts)
                        .build()?,
                );
            }
            "heatsink" => {
                points.push(
                    DataPoint::builder("temperature")
                        .tag("machine", machine_name)
                        .tag("loc", "heatsink")
                        .field("heatsink", value.parse::<f64>()?)
                        .timestamp(ts)
                        .build()?,
                );
            }
            _ => {} // Ignore other fields
        }
    }

    Ok(points)
}

fn time_to_lineprotocol(
    msg: &LogMessage,
    machine_name: &str,
    timestamp: chrono::DateTime<chrono::Utc>,
) -> Result<Vec<DataPoint>> {
    let ts = timestamp
        .timestamp_nanos_opt()
        .ok_or(anyhow::anyhow!("Timestamp out of range"))? as i64;
    let mut point = DataPoint::builder("run_time").tag("machine", machine_name);

    // Parse the message into key-value pairs
    for pair in msg.message.split_ascii_whitespace() {
        if let Some((key, value)) = pair.trim_start_matches('-').split_once('=') {
            match key.to_lowercase().as_str() {
                "elapsed" | "remaining" | "active" => {
                    point = point.field(key.to_lowercase(), value.parse::<f64>()?);
                }
                _ => {} // Ignore other fields
            }
        }
    }

    Ok(vec![point.timestamp(ts).build()?])
}

fn parse_line_protocol_to_datapoint(line: &str, machine_name: &str) -> Result<DataPoint> {
    let line = line.trim();
    if line.is_empty() {
        return Err(anyhow::anyhow!("Empty line protocol string"));
    }

    let parts: Vec<&str> = line.splitn(3, ' ').collect();
    if parts.len() < 2 {
        return Err(anyhow::anyhow!(
            "Invalid line protocol format: missing fields"
        ));
    }

    let measurement_and_tags = parts[0];
    let fields_and_timestamp = parts[1];

    let mut measurement_parts = measurement_and_tags.splitn(2, ',');
    let measurement = measurement_parts
        .next()
        .ok_or(anyhow::anyhow!("Missing measurement"))?;
    let tags_str = measurement_parts.next().unwrap_or("");

    let mut builder = DataPoint::builder(measurement).tag("machine", machine_name);

    for tag_pair in tags_str.split(',') {
        if let Some((key, value)) = tag_pair.split_once('=') {
            let value = value.trim_matches('"');
            builder = builder.tag(key, value);
        }
    }

    let (fields_str, timestamp_str) = if let Some(space_idx) = fields_and_timestamp.rfind(' ') {
        let (fields, ts) = fields_and_timestamp.split_at(space_idx);
        (fields, Some(ts.trim()))
    } else {
        (fields_and_timestamp, None)
    };

    for field_pair in fields_str.split(',') {
        if let Some((key, value)) = field_pair.split_once('=') {
            let value = value.trim();
            if value.starts_with('"') && value.ends_with('"') {
                let str_value = value.trim_matches('"');
                builder = builder.field(key, str_value);
            } else if value.ends_with('i') {
                let int_value = value.trim_end_matches('i').parse::<i64>()?;
                builder = builder.field(key, int_value);
            } else if let Ok(float_value) = value.parse::<f64>() {
                builder = builder.field(key, float_value);
            } else if let Ok(int_value) = value.parse::<i64>() {
                builder = builder.field(key, int_value);
            } else if value == "true" {
                builder = builder.field(key, true);
            } else if value == "false" {
                builder = builder.field(key, false);
            } else {
                builder = builder.field(key, value);
            }
        }
    }

    let timestamp = if let Some(ts_str) = timestamp_str {
        ts_str.parse::<i64>()?
    } else {
        chrono::Utc::now()
            .timestamp_nanos_opt()
            .ok_or(anyhow::anyhow!("Current timestamp out of range"))? as i64
    };

    Ok(builder.timestamp(timestamp).build()?)
}

#[allow(clippy::too_many_arguments)]
async fn docollect(
    stage: i64,
    cycle: i64,
    step: i64,
    point: i64,
    con: Arc<QSConnection>,
    timestamp: chrono::DateTime<chrono::Utc>,
    machine_name: &str,
    state: &MachineState,
    server: Option<&ServerClient>,
) -> Result<Vec<DataPoint>> {
    // Get plate setup samples if available
    // let sample_array = plate_setup.map(|ps| ps.well_samples_as_array());
    info!(
        "Collecting data for stage {}, cycle {}, step {}, point {}",
        stage, cycle, step, point
    );
    // Get list of filter data files
    let pattern = format!(
        "${{FilterFolder}}/S{:02}_C{:03}_T{:02}_P{:04}_*_filterdata.xml",
        stage, cycle, step, point
    );
    let files = con
        .get_expfile_list(&pattern)
        .await
        .map_err(|e| anyhow::anyhow!("Error getting file list: {}", e))?;
    // info!("Found {} files", files.len());
    let filter_files: Vec<FilterDataFilename> = files
        .iter()
        .filter_map(|f| FilterDataFilename::from_string(f).ok())
        .collect();
    // filter_files.sort(); FIXME

    let mut points = Vec::new();

    // Process filter data
    // info!("Getting filter data for {:?}", filter_files);

    // Resolve the run name first so it can be reused for the server fast-path of
    // both the plate setup and every filter file (avoiding a per-fetch
    // RUNTitle? query).
    let current_run_name = match con.get_current_run_name().await {
        Ok(run_name) => run_name,
        Err(e) => {
            error!("Error getting current run name: {:?}", e);
            return Err(anyhow::anyhow!("Error getting current run name: {:?}", e));
        }
    };

    // Reuse the run-scoped plate setup that refresh_state cached for this run
    // (it is stable for the whole run); fetch only on a cache miss.
    let fetched_plate_setup;
    let plate_setup: Option<&PlateSetup> = if state.plate_setup.is_some()
        && state.plate_setup_run.as_deref() == current_run_name.as_deref()
    {
        state.plate_setup.as_ref()
    } else {
        fetched_plate_setup = match con
            .get_plate_setup_via(server, current_run_name.clone())
            .await
        {
            Ok(plate_setup) => Some(plate_setup),
            Err(e) => {
                error!("Error getting plate setup: {:?}", e);
                None
            }
        };
        fetched_plate_setup.as_ref()
    };

    let sample_array = plate_setup.map(|ps| ps.well_samples_as_array());

    let current_temperature_setpoints = match con.get_current_temperature_setpoints().await {
        Ok(setpoints) => setpoints,
        Err(e) => {
            error!("Error getting current temperature setpoints: {:?}", e);
            return Err(anyhow::anyhow!(
                "Error getting current temperature setpoints: {:?}",
                e
            ));
        }
    };

    for fdf in filter_files {
        // Pass the resolved run name so the server fast-path does not re-query
        // the run title for every filter file.
        let filter_data_t = con
            .get_filterdata_one_via(server, fdf, current_run_name.clone())
            .await;
        let filter_data = match filter_data_t {
            Ok(filter_data) => filter_data,
            Err(e) => {
                error!("Error getting filter data for {:?}: {:?}", fdf, e);
                continue;
            }
        };
        let mut line_protocols = filter_data
            .to_lineprotocol(
                current_run_name.as_deref(),
                sample_array.as_deref(),
                Some(&current_temperature_setpoints.0),
                None,
            )
            .map_err(|e| anyhow::anyhow!("Error converting to line protocol: {}", e))?;

        if let Some(plate_setup) = plate_setup {
            let plate_setup_ts = timestamp
                .timestamp_nanos_opt()
                .ok_or(anyhow::anyhow!("Timestamp out of range"))?;
            let plate_setup_line_protocols =
                plate_setup.to_lineprotocol(plate_setup_ts, current_run_name.as_deref(), None);
            line_protocols.extend(plate_setup_line_protocols);
        }

        for lp in line_protocols {
            match parse_line_protocol_to_datapoint(&lp, machine_name) {
                Ok(point) => points.push(point),
                Err(e) => {
                    error!("Error parsing line protocol '{}': {}", lp, e);
                }
            }
        }
    }

    Ok(points)
}

#[allow(clippy::too_many_arguments)]
async fn docollect_server(
    stage: i64,
    cycle: i64,
    step: i64,
    point: i64,
    server: &ServerClient,
    timestamp: chrono::DateTime<chrono::Utc>,
    machine_name: &str,
    state: &MachineState,
) -> Result<Vec<DataPoint>> {
    let run = state
        .run_name
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("collected event has no current run title"))?;
    let filter_root = format!("{run}/apldbio/sds/filter");
    let entries = server.list_context_dir("experiments", &filter_root).await?;
    let sample_array = state
        .plate_setup
        .as_ref()
        .map(|plate| plate.well_samples_as_array());
    let mut points = Vec::new();

    for entry in entries {
        let Some(filename) = entry.path.rsplit('/').next() else {
            continue;
        };
        let Ok(reference) = FilterDataFilename::from_string(filename) else {
            continue;
        };
        if reference.stage as i64 != stage
            || reference.cycle as i64 != cycle
            || reference.step as i64 != step
            || reference.point as i64 != point
        {
            continue;
        }
        let path = format!("{filter_root}/{}", entry.path);
        let bytes = server.get_file("experiments", &path).await?;
        let collection: FilterDataCollection =
            quick_xml::de::from_str(&String::from_utf8_lossy(&bytes))?;
        let mut plate_data = collection
            .plate_point_data
            .into_iter()
            .next()
            .and_then(|point| point.plate_data.into_iter().next())
            .ok_or_else(|| anyhow::anyhow!("filter data contains no PlateData"))?;
        plate_data.timestamp = Some(
            timestamp.timestamp() as f64 + f64::from(timestamp.timestamp_subsec_nanos()) / 1e9,
        );
        for line in plate_data.to_lineprotocol(
            Some(run),
            sample_array.as_deref(),
            Some(&state.zone_targets),
            None,
        )? {
            points.push(parse_line_protocol_to_datapoint(&line, machine_name)?);
        }
    }

    if let Some(plate) = &state.plate_setup {
        let ts = timestamp
            .timestamp_nanos_opt()
            .ok_or_else(|| anyhow::anyhow!("Timestamp out of range"))?;
        for line in plate.to_lineprotocol(ts, Some(run), None) {
            points.push(parse_line_protocol_to_datapoint(&line, machine_name)?);
        }
    }
    Ok(points)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use influxdb2::models::WriteDataPoint;

    #[test]
    fn test_ledparse_valid_message() {
        let msg = LogMessage {
            topic: "LEDStatus".to_string(),
            timestamp: None,
            message: "Temperature:56.1791 Current:9.18727 Voltage:3.41406 JuncTemp:72.8079"
                .to_string(),
        };
        let timestamp = chrono::Utc.timestamp_nanos(1_000_000_000);

        let points = ledstatus_to_lineprotocol(&msg, "qpcr1", timestamp).unwrap();
        assert_eq!(points.len(), 1);

        let mut buf = Vec::new();
        points[0].write_data_point_to(&mut buf).unwrap();
        let line = String::from_utf8(buf).unwrap();
        assert!(line.contains("lamp,machine=qpcr1"));
        assert!(line.contains("current=9.18727"));
        assert!(line.contains("junctemp=72.8079"));
        assert!(line.contains("temperature=56.1791"));
        assert!(line.contains("voltage=3.41406"));
    }

    #[test]
    fn test_ledparse_invalid_format() {
        let msg = LogMessage {
            topic: "LEDStatus".to_string(),
            timestamp: None,
            message: "Invalid message format".to_string(),
        };
        let timestamp = chrono::Utc.timestamp_nanos(1_000_000_000);

        assert!(ledstatus_to_lineprotocol(&msg, "qpcr1", timestamp).is_err());
    }

    #[test]
    fn test_ledparse_invalid_number() {
        let msg = LogMessage {
            topic: "LEDStatus".to_string(),
            timestamp: None,
            message: "Temperature:not_a_number".to_string(),
        };
        let timestamp = chrono::Utc.timestamp_nanos(1_000_000_000);

        assert!(ledstatus_to_lineprotocol(&msg, "qpcr1", timestamp).is_err());
    }

    #[test]
    fn test_run_stage_message() {
        let msg = LogMessage {
            topic: "Run".to_string(),
            timestamp: None,
            message: "Stage 2".to_string(),
        };
        let timestamp = chrono::Utc.timestamp_nanos(1_000_000_000);

        let mut state = MachineState::default_idle();
        let points = futures::executor::block_on(run_to_lineprotocol(
            &msg, "qpcr1", timestamp, None, &mut state, None,
        ))
        .unwrap();
        assert_eq!(points.len(), 2);
        assert_eq!(state.stage, Some(2));

        let mut buf = Vec::new();
        points[0].write_data_point_to(&mut buf).unwrap();
        let line = String::from_utf8(buf).unwrap();
        assert!(line.contains("run_action,"));
        assert!(line.contains("type=stage"));
        assert!(line.contains("stage=2"));

        let mut buf = Vec::new();
        points[1].write_data_point_to(&mut buf).unwrap();
        let line = String::from_utf8(buf).unwrap();
        assert!(line.contains("run_status,"));
        assert!(line.contains("type=stage"));
        assert!(line.contains("stage=2"));
    }

    #[test]
    fn test_run_error_message() {
        let msg = LogMessage {
            topic: "Run".to_string(),
            timestamp: None,
            message: "Error Something went wrong".to_string(),
        };
        let timestamp = chrono::Utc.timestamp_nanos(1_000_000_000);

        let mut state = MachineState::default_idle();
        let points = futures::executor::block_on(run_to_lineprotocol(
            &msg, "qpcr1", timestamp, None, &mut state, None,
        ))
        .unwrap();
        assert_eq!(points.len(), 1);

        let mut buf = Vec::new();
        points[0].write_data_point_to(&mut buf).unwrap();
        let line = String::from_utf8(buf).unwrap();
        assert!(line.contains("run_action,"));
        assert!(line.contains("type=error"));
        assert!(line.contains("message=\"Something went wrong\""));
    }

    #[test]
    fn test_temperature_message() {
        let msg = LogMessage {
            topic: "Temperature".to_string(),
            timestamp: None,
            message: "-sample=22.3,22.3,22.3,22.3,22.3,22.3 -heatsink=23.4 -cover=18.2 -block=22.3,22.3,22.3,22.3,22.3,22.3".to_string(),
        };
        let timestamp = chrono::Utc.timestamp_nanos(1_000_000_000);
        let state = MachineState::default_idle();

        let points = temperature_to_lineprotocol(&msg, "qpcr1", timestamp, &state).unwrap();

        // Should have 8 points: 6 zones + cover + heatsink
        assert_eq!(points.len(), 8);

        // Test zone point
        let mut buf = Vec::new();
        points[0].write_data_point_to(&mut buf).unwrap();
        let line = String::from_utf8(buf).unwrap();
        assert!(line.contains("temperature,"));
        assert!(line.contains("loc=zones"));
        assert!(line.contains("zone=0"));
        assert!(line.contains("sample=22.3"));
        assert!(line.contains("block=22.3"));
        assert!(line.contains("target=25"));

        // Test heatsink point
        let mut buf = Vec::new();
        points[6].write_data_point_to(&mut buf).unwrap();
        let line = String::from_utf8(buf).unwrap();
        assert!(line.contains("temperature,"));
        assert!(line.contains("loc=heatsink"));
        assert!(line.contains("heatsink=23.4"));

        // Test cover point
        let mut buf = Vec::new();
        points[7].write_data_point_to(&mut buf).unwrap();
        let line = String::from_utf8(buf).unwrap();
        assert!(line.contains("temperature,"));
        assert!(line.contains("loc=cover"));
        assert!(line.contains("cover=18.2"));
    }

    #[test]
    fn test_time_message() {
        let msg = LogMessage {
            topic: "Time".to_string(),
            timestamp: None,
            message: "-elapsed=120.5 -Remaining=600.0 -active=3552".to_string(),
        };
        let timestamp = chrono::Utc.timestamp_nanos(1_000_000_000);

        let point = time_to_lineprotocol(&msg, "qpcr1", timestamp).unwrap();

        let mut buf = Vec::new();
        assert_eq!(point.len(), 1);
        point[0].write_data_point_to(&mut buf).unwrap();
        let line = String::from_utf8(buf).unwrap();

        assert!(line.contains("run_time"));
        assert!(line.contains("elapsed=120.5"));
        assert!(line.contains("remaining=600"));
        assert!(line.contains("active=3552"));
    }

    #[test]
    fn test_ramping_updates_zone_targets() {
        let msg = LogMessage {
            topic: "Run".to_string(),
            timestamp: None,
            message: "Ramping -rates=1.6,1.6,1.6,1.6,1.6,1.6 -zones=1,2,3,4,5,6 -targets=95.0,95.0,95.0,95.0,95.0,95.0".to_string(),
        };
        let timestamp = chrono::Utc.timestamp_nanos(1_000_000_000);

        let mut state = MachineState::default_idle();
        assert_eq!(state.zone_targets, vec![25.0; 6]);

        let points = futures::executor::block_on(run_to_lineprotocol(
            &msg, "qpcr1", timestamp, None, &mut state, None,
        ))
        .unwrap();

        assert_eq!(state.zone_targets, vec![95.0; 6]);
        assert_eq!(points.len(), 1); // Just the ramping point (no refresh without con)
    }
}
