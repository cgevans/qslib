use anyhow::Result;
use clap::Parser;
use dashmap::DashMap;
use env_logger::Env;
use futures::stream;
use influxdb2::models::DataPoint;
use influxdb2::{Client, FromDataPoint, models::WriteDataPoint};
use log::{debug, error, info, warn};
use qslib_rs::com::ConnectionError;
use qslib_rs::plate_setup::PlateSetup;
use qslib_rs::{
    com::QSConnection,
    commands::{AccessLevel, AccessSet, CommandBuilder, Subscribe},
    data::{FilterDataCollection, PlateData},
    com::FilterDataFilename,
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
    QSError(#[from] qslib_rs::com::QSConnectionError),

    #[error("Data error: {0}")]
    DataError(#[from] qslib_rs::data::DataError),

    #[error("Path strip prefix error: {0}")]
    StripPrefixError(#[from] std::path::StripPrefixError),
}

fn load_config(path: PathBuf) -> Result<Config> {
    let settings = config::Config::builder()
        .add_source(config::File::from(path))
        .build()?;

    Ok(settings.try_deserialize()?)
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

    info!("InfluxDB write task started.");

    loop {
        tokio::select! {
            // Check for new points
            point = rx.recv() => {
                match point {
                    Some((machine, mut point)) => {
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
    let mut conns = Arc::new(DashMap::new());
    let mut ids = HashMap::new();

    for config in config.machines.iter() {
        let (conn, id) = log_machine(config, tx.clone(), &mut log_tasks).await?;
        conns.insert(config.name.clone(), (Arc::new(conn), config.clone()));
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
        let (conn, id) = log_machine(&config, tx.clone(), &mut log_tasks).await?;
        conns.insert(config.name.clone(), (Arc::new(conn), config.clone()));
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
    config: &MachineConfig,
    tx: mpsc::Sender<(String, DataPoint)>,
    log_tasks: &mut JoinSet<()>,
) -> Result<(QSConnection, Id)> {
    let mut con =
        QSConnection::connect(&config.host, 7443, qslib_rs::com::ConnectionType::SSL).await?;

    let access = AccessSet::level(AccessLevel::Observer);
    access.send(&mut con).await?.recv_response().await?;
    Subscribe::topic("Temperature").send(&mut con).await?;
    Subscribe::topic("Time").send(&mut con).await?;
    Subscribe::topic("Run").send(&mut con).await?;
    Subscribe::topic("LEDStatus").send(&mut con).await?;

    let mut log_sub = con
        .subscribe_log(&["Temperature", "Time", "Run", "LEDStatus"])
        .await;

    let config_clone = config.clone();

    let aborthandle = log_tasks
        .spawn(async move {
            influx_log_loop(&mut log_sub, tx, &config_clone, None)
                .await
                .unwrap();
        });
    let id = aborthandle.id();

    info!("Logging task started for {}", config.name);

    Ok((con, id))
}

async fn influx_log_loop(
    log_sub: &mut StreamMap<String, BroadcastStream<LogMessage>>,
    tx: mpsc::Sender<(String, DataPoint)>,
    config: &MachineConfig,
    timeout_secs: Option<u64>,
) -> Result<()> {
    let machine_name = config.name.as_ref();
    let mut last_message = tokio::time::Instant::now();
    let timeout = Duration::from_secs(timeout_secs.unwrap_or(60));
    let mut check_interval = tokio::time::interval(Duration::from_secs(5));

    loop {
        select! {
            msg = log_sub.next() => {
                let (_, msg) = log_sub.next().await.unwrap();
                let timestamp = chrono::Utc::now();
                let msg = msg.unwrap();
                let points = match msg.topic.as_str() {
                    "Temperature" => temperature_to_lineprotocol(&msg, machine_name, timestamp)?,
                    "Time" => time_to_lineprotocol(&msg, machine_name, timestamp)?,
                    "Run" => run_to_lineprotocol(&msg, machine_name, timestamp)?,
                    "LEDStatus" => ledstatus_to_lineprotocol(&msg, machine_name, timestamp)?,
                    _ => continue,
                };

                for point in points {
                    // If InfluxDB is configured, send points to the channel
                    if let Err(e) = tx.send((config.name.clone(), point)).await {
                        error!("Failed to send point to InfluxDB: {}", e);
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

fn run_to_lineprotocol(
    msg: &LogMessage,
    machine_name: &str,
    timestamp: chrono::DateTime<chrono::Utc>,
) -> Result<Vec<DataPoint>> {
    let mut points = Vec::new();
    let mut args = msg.message.split_ascii_whitespace();

    let action = args.next().ok_or(anyhow::anyhow!("Missing action"))?;

    // Create base point for run_action
    let mut point = DataPoint::builder("run_action")
        .tag("machine", machine_name)
        .tag("type", action.to_lowercase())
        .timestamp(timestamp.timestamp_nanos_opt().unwrap() as i64);

    match action {
        "Stage" | "Cycle" | "Step" => {
            let value = args
                .next()
                .ok_or(anyhow::anyhow!("Missing value"))?
                .parse::<i64>()?;

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
            if let Some(time_str) = args.next() {
                let time = time_str.parse::<f64>()?;
                point = point.field("holdtime", time);
            }
            points.push(point.build()?);
        }
        "Ramping" | "Acquiring" => {
            // These just need the basic point with type tag
            points.push(point.build()?);
        }
        "Error" | "Ended" | "Aborted" | "Stopped" | "Starting" => {
            // Collect remaining message
            let remaining = args.collect::<Vec<_>>().join(" ");
            if !remaining.is_empty() {
                point = point.field("message", remaining);
            }
            points.push(point.build()?);
        }
        "Collected" => {
            points.push(point.build()?);
        }
        _ => {
            // Handle other cases
            let message = format!("{} {}", action, args.collect::<Vec<_>>().join(" "));
            point = point.tag("type", "Other").field("message", message);
            points.push(point.build()?);
        }
    }

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
    args: &HashMap<String, String>,
    machine_name: &str,
    plate_setup: Option<&PlateSetup>,
    connection: &QSConnection,
    config: &Config,
    timestamp: chrono::DateTime<chrono::Utc>,
    tx: mpsc::Sender<(String, DataPoint)>,
) -> Result<(), QSConnectionError> {
    let run = args
        .get("run")
        .ok_or_else(|| QSConnectionError::MissingArgument("run".to_string()))?
        .trim_matches('"')
        .to_string();

    // Convert args to proper format for filter data filename
    let stage = args
        .get("stage")
        .and_then(|s| s.parse::<i32>().ok())
        .ok_or_else(|| {
            QSConnectionError::InvalidArgument("stage".to_string(), "not a valid integer".to_string())
        })?;
    let cycle = args
        .get("cycle")
        .and_then(|s| s.parse::<i32>().ok())
        .ok_or_else(|| {
            QSConnectionError::InvalidArgument("cycle".to_string(), "not a valid integer".to_string())
        })?;
    let step = args
        .get("step")
        .and_then(|s| s.parse::<i32>().ok())
        .ok_or_else(|| {
            QSConnectionError::InvalidArgument("step".to_string(), "not a valid integer".to_string())
        })?;
    let point = args
        .get("point")
        .and_then(|s| s.parse::<i32>().ok())
        .ok_or_else(|| {
            QSConnectionError::InvalidArgument("point".to_string(), "not a valid integer".to_string())
        })?;

    // Get plate setup samples if available
    // let sample_array = plate_setup.map(|ps| ps.well_samples_as_array());

    // Get list of filter data files
    let pattern = format!(
        "{}/apldbio/sds/filter/S{:02}_C{:03}_T{:02}_P{:04}_*_filterdata.xml",
        run, stage, cycle, step, point
    );
    let files = connection.get_expfile_list(&pattern).await?;

    let mut filter_files: Vec<FilterDataFilename> = files
        .iter()
        .filter_map(|f| FilterDataFilename::from_string(f).ok())
        .collect();
    // filter_files.sort(); FIXME

    // Get latest point's files
    let latest_files: Vec<_> = filter_files
        .iter()
        .filter(|x| x.is_same_point(&filter_files[filter_files.len() - 1]))
        .collect();

    let mut line_protocols = Vec::new();

    // Process filter data 
    if let Some(ipdir) = &config.sync.in_progress_directory {
        let filter_path = Path::new(ipdir).join(&run).join("apldbio/sds/filter");
        if filter_path.exists() {
            for fdf in latest_files {
                let filter_data = connection
                    .get_filterdata_one(*fdf, Some(run.clone()))
                    .await?;
                line_protocols.extend(filter_data.to_lineprotocol(
                    Some(&run),
                    None, // FIXME  sample_array,
                    None,
                )?);
            }
        } else {
            for fdf in latest_files {
                let filter_data = connection
                    .get_filterdata_one(*fdf, Some(run.clone()))
                    .await?;
                line_protocols.extend(filter_data.to_lineprotocol(
                    Some(&run),
                    None, // FIXME  sample_array,
                    None,
                )?);
            }
        }
    }

    for lp in line_protocols {
        // Just print to stdout for now
        println!("{}", lp);
    }

    // Write to InfluxDB
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

        let points = run_to_lineprotocol(&msg, "qpcr1", timestamp).unwrap();
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

        let points = run_to_lineprotocol(&msg, "qpcr1", timestamp).unwrap();
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
