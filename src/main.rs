use anyhow::Result;
use clap::Parser;
use dashmap::DashMap;
use env_logger::Env;
use futures::stream;
use influxdb2::models::{DataPoint, FieldValue};
use influxdb2::{Client, FromDataPoint, models::WriteDataPoint};
use log::{debug, error, info, warn};
use qslib::com::{CommandError, ConnectionError};
use qslib::parser::{ErrorResponse, MessageResponse, OkResponse, Value};
use qslib::plate_setup::PlateSetup;
use qslib::{
    com::FilterDataFilename,
    com::QSConnection,
    commands::{AccessLevel, AccessLevelSet, CommandBuilder, Subscribe},
    data::{FilterDataCollection, PlateData},
    parser::LogMessage,
};
use serde_derive::Deserialize;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use thiserror::Error;
use tokio::select;
use tokio::sync::mpsc;
use tokio::task::{Id, JoinHandle, JoinSet};
use tokio::time::{Duration, interval};
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::{Stream, StreamExt, StreamMap, wrappers::BroadcastStream};
use walkdir;

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
    stdout: Option<()>,
}

#[derive(Debug, Deserialize)]
struct GlobalConfig {
    // Add global settings as needed
}

#[derive(Debug, Deserialize, Clone)]
struct MachineConfig {
    name: String,
    host: String,
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

fn load_config(path: PathBuf) -> Result<Config> {
    let settings = config::Config::builder()
        .add_source(config::File::from(path))
        .build()?;

    Ok(settings.try_deserialize()?)
}

fn value_to_influxvalue(value: Value) -> FieldValue {
    match value {
        Value::String(s) => FieldValue::String(s),
        Value::Int(i) => FieldValue::I64(i),
        Value::Float(f) => FieldValue::F64(f),
        Value::Bool(b) => FieldValue::Bool(b),
        Value::QuotedString(s) => FieldValue::String(s),
        Value::XmlString { value, tag: _ } => FieldValue::String(value.to_string()),
    }
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
                    Some((machine, point)) => {
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
                                    to_retry.extend(points.drain(..));
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
                            to_retry.extend(points.drain(..));
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
        let tosend = points.drain(..).collect::<Vec<_>>();
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
    let influx_task = if let Some(influx_config) = config.influxdb.as_ref() {
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

    let mut log_tasks = JoinSet::new();
    let conns = Arc::new(DashMap::new());
    let mut ids = HashMap::new();

    for config in config.machines.iter() {
        let con = Arc::new(
            QSConnection::connect(&config.host, 7443, qslib::com::ConnectionType::SSL).await?,
        );
        let id = log_machine(con.clone(), config, tx.clone(), &mut log_tasks).await?;
        conns.insert(config.name.clone(), (con, config.clone()));
        ids.insert(id, config.clone());
    }

    let conns_clone = conns.clone();
    let matrix_task = config.matrix.clone().map(|x| {
        tokio::spawn(async move {
            if let Err(e) = matrix::setup_matrix(&x, conns_clone).await {
                error!("Error setting up Matrix: {}", e);
            }
        })
    });

    while let Some((x)) = log_tasks.join_next_with_id().await {
        let (id, _) = x.unwrap();
        let config = ids.remove(&id).unwrap();
        warn!("Reconnecting to {}", config.name);
        conns.remove(&config.name);
        ids.remove(&id);
        let new_con = Arc::new(
            QSConnection::connect(&config.host, 7443, qslib::com::ConnectionType::SSL).await?,
        );
        let id = log_machine(new_con.clone(), &config, tx.clone(), &mut log_tasks).await?;
        conns.insert(config.name.clone(), (new_con, config.clone()));
        ids.insert(id, config.clone());
    }

    // Wait for influx task to complete if it exists
    if let Some(task) = influx_task {
        task.await.unwrap();
    }

    Ok(())
}

// async fn machine_to_lineprotocol_loop(logchannel: &StreamMap<String, BroadcastStream<LogMessage>>, lpchannel: &mut mpsc::Sender<Point>, config: &InstrumentConfig) {

// }

async fn log_machine(
    mut con: Arc<QSConnection>,
    config: &MachineConfig,
    tx: mpsc::Sender<(String, DataPoint)>,
    log_tasks: &mut JoinSet<()>,
) -> Result<Id> {
    let access = AccessLevelSet::level(AccessLevel::Observer);
    access.send(&mut con).await?.receive_response().await?;
    Subscribe::topic("Temperature").send(&mut con).await?;
    Subscribe::topic("Time").send(&mut con).await?;
    Subscribe::topic("Run").send(&mut con).await?;
    Subscribe::topic("LEDStatus").send(&mut con).await?;

    let mut log_sub = con
        .subscribe_log(&["Temperature", "Time", "Run", "LEDStatus"])
        .await;

    let config_clone = config.clone();

    let aborthandle = log_tasks.spawn(async move {
        influx_log_loop(&mut log_sub, tx, &config_clone, None, con.clone())
            .await
            .unwrap();
    });
    let id = aborthandle.id();

    info!("Logging task started for {}", config.name);

    Ok(id)
}

async fn influx_log_loop(
    log_sub: &mut StreamMap<String, BroadcastStream<LogMessage>>,
    tx: mpsc::Sender<(String, DataPoint)>,
    config: &MachineConfig,
    timeout_secs: Option<u64>,
    con: Arc<QSConnection>,
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
                let timestamp = chrono::Utc::now();
                let msg = match msg {
                    Ok(msg) => msg,
                    Err(BroadcastStreamRecvError::Lagged(n)) => {
                        warn!("Machine {} connection lagged by {} messages", config.name, n);
                        continue;
                    }
                };

                info!("Message: {:?}", msg);

                // Safely convert points, logging errors instead of propagating
                let points = match msg.topic.as_str() {
                    "Temperature" => match temperature_to_lineprotocol(&msg, machine_name, timestamp) {
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
                    "Run" => match run_to_lineprotocol(&msg, machine_name, timestamp, con.clone()).await {
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
    let mut args = msg.message.split_ascii_whitespace();
    while let Some(arg) = args.next() {
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
            .timestamp(timestamp.timestamp_nanos_opt().unwrap() as i64)
            .build()?,
    ])
}

async fn run_to_lineprotocol(
    msg: &LogMessage,
    machine_name: &str,
    timestamp: chrono::DateTime<chrono::Utc>,
    con: Arc<QSConnection>,
) -> Result<Vec<DataPoint>> {
    let mut points = Vec::new();
    let mut parts = msg.message.splitn(2, ' ');

    let action = parts.next().ok_or(anyhow::anyhow!("Missing action"))?;
    let remaining = parts.next().unwrap_or(""); // Get rest as single string
    let content = OkResponse::parse(&mut remaining.as_bytes())
        .map_err(|e| anyhow::anyhow!("Invalid message: {}", e))?;

    // Create base point for run_action
    let mut point = DataPoint::builder("run_action")
        .tag("machine", machine_name)
        .tag("type", action.to_lowercase())
        .timestamp(timestamp.timestamp_nanos_opt().unwrap() as i64);

    match action {
        "Stage" | "Cycle" | "Step" => {
            let value = content
                .args
                .get(0)
                .ok_or(anyhow::anyhow!("Missing value"))?
                .clone()
                .try_into_i64()
                .map_err(|e| anyhow::anyhow!("Missing value: {}", e))?;

            point = point.field(action.to_lowercase(), value);
            points.push(point.build()?);

            // Also create run_status point
            points.push(
                DataPoint::builder("run_status")
                    .tag("machine", machine_name)
                    .tag("type", action.to_lowercase())
                    .field(action.to_lowercase(), value)
                    .timestamp(timestamp.timestamp_nanos_opt().unwrap() as i64)
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
            let rates = content
                .options
                .get("rates")
                .unwrap()
                .to_string()
                .split(',')
                .map(|s| s.parse::<f64>().unwrap())
                .collect::<Vec<f64>>();
            let zones = content
                .options
                .get("zones")
                .unwrap()
                .to_string()
                .split(',')
                .map(|s| s.to_string())
                .collect::<Vec<_>>();
            let targets = content
                .options
                .get("targets")
                .unwrap()
                .to_string()
                .split(',')
                .map(|s| s.parse::<f64>().unwrap())
                .collect::<Vec<f64>>();
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
                .unwrap()
                .to_string()
                .parse::<i64>()
                .unwrap();
            let cycle = content
                .options
                .get("cycle")
                .unwrap()
                .to_string()
                .parse::<i64>()
                .unwrap();
            let step = content
                .options
                .get("step")
                .unwrap()
                .to_string()
                .parse::<i64>()
                .unwrap();
            let run_point = content
                .options
                .get("point")
                .unwrap()
                .to_string()
                .parse::<i64>()
                .unwrap();
            point = point
                .field("stage", stage)
                .field("cycle", cycle)
                .field("step", step)
                .field("point", run_point);
            points.push(point.build()?);
            tokio::spawn(docollect(
                None,
                stage,
                cycle,
                step,
                run_point,
                None,
                con.clone(),
                timestamp,
            ));
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
            let message = format!("{} {}", action, content.to_string());
            point = point.tag("type", "Other").field("message", message);
            points.push(point.build()?);
        }
    }
    info!("Points: {:?}", points);
    Ok(points)
}

fn temperature_to_lineprotocol(
    msg: &LogMessage,
    machine_name: &str,
    timestamp: chrono::DateTime<chrono::Utc>,
) -> Result<Vec<DataPoint>> {
    let mut points = Vec::new();
    let ts = timestamp.timestamp_nanos_opt().unwrap() as i64;

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
            points.push(
                DataPoint::builder("temperature")
                    .tag("machine", machine_name)
                    .tag("loc", "zones")
                    .tag("zone", i.to_string())
                    .field("sample", *sample)
                    .field("block", *block)
                    .timestamp(ts)
                    .build()?,
            );
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
    let ts = timestamp.timestamp_nanos_opt().unwrap() as i64;
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

async fn docollect(
    run: Option<&str>,
    stage: i64,
    cycle: i64,
    step: i64,
    point: i64,
    plate_setup: Option<&PlateSetup>,
    con: Arc<QSConnection>,
    timestamp: chrono::DateTime<chrono::Utc>,
) -> Result<(), CommandError<ErrorResponse>> {
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
    let files = con.get_expfile_list(&pattern).await?;
    info!("Found {} files", files.len());
    let mut filter_files: Vec<FilterDataFilename> = files
        .iter()
        .filter_map(|f| FilterDataFilename::from_string(f).ok())
        .collect();
    // filter_files.sort(); FIXME

    let mut line_protocols = Vec::new();

    // Process filter data
    info!("Getting filter data for {:?}", filter_files);

    for fdf in filter_files {
        info!("Getting filter data for {:?}", fdf);
        let filter_data_t = con.get_filterdata_one(fdf, None).await;
        let filter_data = match filter_data_t {
            Ok(filter_data) => filter_data,
            Err(e) => {
                error!("Error getting filter data for {:?}: {:?}", fdf, e);
                continue;
            }
        };
        info!("Filter data: {:?}", filter_data);
        line_protocols.extend(
            filter_data
                .to_lineprotocol(
                    None, None, // FIXME  sample_array,
                    None,
                )
                .unwrap(),
        );
    }

    for lp in line_protocols {
        // Just print to stdout for now
        info!("{:?}", lp);
    }

    // // Write to InfluxDB
    // if let Some(influx) = &config.influxdb {
    //     let client = Client::new(&influx.url, &influx.org, &influx.token);

    //     for lp in line_protocols {
    //         write_api.write(&influx.bucket, None, lp.as_bytes()).await?;
    //     }
    //     write_api.flush().await?;
    // }

    Ok(())
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
            message: "Temperature:56.1791 Current:9.18727 Voltage:3.41406 JuncTemp:72.8079"
                .to_string(),
        };
        let timestamp = chrono::Utc.timestamp_nanos(1_000_000_000);

        let points = ledstatus_to_lineprotocol(&msg, "qpcr1", timestamp).unwrap();
        assert_eq!(points.len(), 1);

        let mut buf = Vec::new();
        points[0].write_data_point_to(&mut buf).unwrap();
        let line = String::from_utf8(buf).unwrap();
        assert_eq!(
            line,
            "lamp current=9.18727,junctemp=72.8079,temperature=56.1791,voltage=3.41406 1000000000\n"
        );
    }

    #[test]
    fn test_ledparse_invalid_format() {
        let msg = LogMessage {
            topic: "LEDStatus".to_string(),
            message: "Invalid message format".to_string(),
        };
        let timestamp = chrono::Utc.timestamp_nanos(1_000_000_000);

        assert!(ledstatus_to_lineprotocol(&msg, "qpcr1", timestamp).is_err());
    }

    #[test]
    fn test_ledparse_invalid_number() {
        let msg = LogMessage {
            topic: "LEDStatus".to_string(),
            message: "Temperature:not_a_number".to_string(),
        };
        let timestamp = chrono::Utc.timestamp_nanos(1_000_000_000);

        assert!(ledstatus_to_lineprotocol(&msg, "qpcr1", timestamp).is_err());
    }

    #[test]
    fn test_run_stage_message() {
        let msg = LogMessage {
            topic: "Run".to_string(),
            message: "Stage 2".to_string(),
        };
        let timestamp = chrono::Utc.timestamp_nanos(1_000_000_000);

        let points =
            futures::executor::block_on(run_to_lineprotocol(&msg, "qpcr1", timestamp, None))
                .unwrap();
        assert_eq!(points.len(), 2);

        let mut buf = Vec::new();
        points[0].write_data_point_to(&mut buf).unwrap();
        let line = String::from_utf8(buf).unwrap();
        assert!(line.contains("run_action,type=stage stage=2"));

        let mut buf = Vec::new();
        points[1].write_data_point_to(&mut buf).unwrap();
        let line = String::from_utf8(buf).unwrap();
        assert!(line.contains("run_status,type=stage stage=2"));
    }

    #[test]
    fn test_run_error_message() {
        let msg = LogMessage {
            topic: "Run".to_string(),
            message: "Error Something went wrong".to_string(),
        };
        let timestamp = chrono::Utc.timestamp_nanos(1_000_000_000);

        let points =
            futures::executor::block_on(run_to_lineprotocol(&msg, "qpcr1", timestamp, None))
                .unwrap();
        assert_eq!(points.len(), 1);

        let mut buf = Vec::new();
        points[0].write_data_point_to(&mut buf).unwrap();
        let line = String::from_utf8(buf).unwrap();
        assert!(line.contains("run_action,type=error"));
        assert!(line.contains("message=\"Something went wrong\""));
    }

    #[test]
    fn test_temperature_message() {
        let msg = LogMessage {
            topic: "Temperature".to_string(),
            message: "-sample=22.3,22.3,22.3,22.3,22.3,22.3 -heatsink=23.4 -cover=18.2 -block=22.3,22.3,22.3,22.3,22.3,22.3".to_string(),
        };
        let timestamp = chrono::Utc.timestamp_nanos(1_000_000_000);

        let points = temperature_to_lineprotocol(&msg, "qpcr1", timestamp).unwrap();

        // Should have 8 points: 6 zones + cover + heatsink
        assert_eq!(points.len(), 8);

        // Test zone point
        let mut buf = Vec::new();
        points[0].write_data_point_to(&mut buf).unwrap();
        let line = String::from_utf8(buf).unwrap();
        assert!(line.contains("temperature,loc=zones,zone=0"));
        assert!(line.contains("sample=22.3"));
        assert!(line.contains("block=22.3"));

        // Test heatsink point
        let mut buf = Vec::new();
        points[6].write_data_point_to(&mut buf).unwrap();
        let line = String::from_utf8(buf).unwrap();
        assert!(line.contains("temperature,loc=heatsink"));
        assert!(line.contains("heatsink=23.4"));

        // Test cover point
        let mut buf = Vec::new();
        points[7].write_data_point_to(&mut buf).unwrap();
        let line = String::from_utf8(buf).unwrap();
        assert!(line.contains("temperature,loc=cover"));
        assert!(line.contains("cover=18.2"));
    }

    #[test]
    fn test_time_message() {
        let msg = LogMessage {
            topic: "Time".to_string(),
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
}
