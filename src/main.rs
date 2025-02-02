use anyhow::Result;
use qslib_rs::{com::QSConnection, commands::{Access, AccessLevel, CommandBuilder, Subscribe}, parser::LogMessage};
use serde_derive::Deserialize;
use std::path::PathBuf;
use tokio_stream::{Stream, StreamExt};
use influxdb2::{models::WriteDataPoint, Client, FromDataPoint};
use influxdb2::models::DataPoint;
use tokio::sync::mpsc;
use tokio::time::{interval, Duration};
use futures::stream;
// use crate::influx::InfluxDBConfig;

#[derive(Debug, Deserialize)]
struct Config {
    global: Option<GlobalConfig>,
    instruments: Vec<InstrumentConfig>,
    matrix: Option<MatrixConfig>,
    influxdb: Option<InfluxDBConfig>,
    stdout: Option<()>,
}

#[derive(Debug, Deserialize)]
struct GlobalConfig {
    // Add global settings as needed
}

#[derive(Debug, Deserialize)]
struct InstrumentConfig {
    name: String,
    host: String,
}

#[derive(Debug, Deserialize)]
struct MatrixConfig {
    host: String,
    user: String,
    password: String,
    room: String,
    session_file: String,
    allow_verification: bool,
    allow_commands: bool,
    allow_control: bool,
}

#[derive(Debug, Deserialize)]
struct InfluxDBConfig {
    url: String,
    org: String,
    bucket: String,
    token: String,
    batch_size: Option<usize>,
    flush_interval_ms: Option<u64>,
}

fn load_config(path: PathBuf) -> Result<Config> {
    let settings = config::Config::builder()
        .add_source(config::File::from(path))
        .build()?;
    
    Ok(settings.try_deserialize()?)
}

async fn write_points_to_influx(
    mut rx: mpsc::Receiver<DataPoint>,
    client: Client,
    bucket: String,
    batch_size: usize,
    flush_interval: Duration,
) -> Result<()> {
    let mut interval = interval(flush_interval);
    let mut points = Vec::with_capacity(batch_size);

    eprintln!("InfluxDB write task started");

    loop {
        tokio::select! {
            // Check for new points
            point = rx.recv() => {
                match point {
                    Some(point) => {
                        points.push(point);
                        if points.len() >= batch_size {
                            eprintln!("Flushing {} points to InfluxDB (batch size reached)", points.len());
                            let tosend = points.drain(..).collect::<Vec<_>>();
                            client.write(&bucket, stream::iter(tosend)).await?;
                        }
                    }
                    None => break, // Channel closed
                }
            }
            // Flush on interval
            _ = interval.tick() => {
                if !points.is_empty() {
                    eprintln!("Flushing {} points to InfluxDB (interval reached)", points.len());
                    let tosend = points.drain(..).collect::<Vec<_>>();
                    client.write(&bucket, stream::iter(tosend)).await?;
                }
            }
        }
    }

    // Final flush of any remaining points
    if !points.is_empty() {
        let tosend = points.drain(..).collect::<Vec<_>>();
        client.write(&bucket, stream::iter(tosend)).await?;
    }


    eprintln!("InfluxDB write task completed");

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = load_config(PathBuf::from("config.toml"))?;
    
    // Set up InfluxDB if configured
    let (tx, rx) = mpsc::channel(1000);
    let influx_task = if let Some(influx_config) = config.influxdb.as_ref() {
        let client = Client::new(&influx_config.url, &influx_config.org, &influx_config.token);
        let batch_size = influx_config.batch_size.unwrap_or(100);
        let flush_interval = Duration::from_millis(influx_config.flush_interval_ms.unwrap_or(10000));
        let bucket = influx_config.bucket.clone();
        
        Some(tokio::spawn(async move {
            if let Err(e) = write_points_to_influx(
                rx,
                client,
                bucket,
                batch_size,
                flush_interval,
            ).await {
                eprintln!("Error writing points to InfluxDB: {}", e);
            }
        }))
    } else {
        None
    };

    let mut con = QSConnection::connect("qpcr1", 7443).await?;
    let mut log_sub = con.subscribe_log(&["Temperature", "Time", "Run", "LEDStatus"]).await;

    let access = Access::level(AccessLevel::Observer);
    con.send_command(access).await?;

    Subscribe::topic("*").send(&mut con).await?;

    while let (_, msg) = log_sub.next().await.unwrap() {
        let timestamp = chrono::Utc::now();
        let msg = msg.unwrap();
        let points = match msg.topic.as_str() {
            "Temperature" => temperature_to_lineprotocol(&msg, timestamp)?,
            "Time" => time_to_lineprotocol(&msg, timestamp)?,
            "Run" => run_to_lineprotocol(&msg, timestamp)?,
            "LEDStatus" => ledstatus_to_lineprotocol(&msg, timestamp)?,
            _ => continue,
        };

        for point in points {
            // If InfluxDB is configured, send points to the channel
            if let Some(_) = config.influxdb.as_ref() {
                if let Err(e) = tx.send(point.clone()).await {
                    eprintln!("Failed to send point to InfluxDB channel: {:?}, error: {}", point, e);
                }
            }

            // If stdout is enabled, print the points
            if config.stdout.is_some() {
                let mut buf = Vec::new();
                point.write_data_point_to(&mut buf)?;
                println!("{}", String::from_utf8(buf)?);
            }
        }
    }

    // Drop sender to close channel
    drop(tx);

    // Wait for influx task to complete if it exists
    if let Some(task) = influx_task {
        task.await.unwrap();
    }

    Ok(())
}

// async fn machine_to_lineprotocol_loop(logchannel: &StreamMap<String, BroadcastStream<LogMessage>>, lpchannel: &mut mpsc::Sender<Point>, config: &InstrumentConfig) {

// }

fn ledstatus_to_lineprotocol(msg: &LogMessage, timestamp: chrono::DateTime<chrono::Utc>) -> Result<Vec<DataPoint>> {
    debug_assert!(msg.topic == "LEDStatus");
    // Example: "MESSage LEDStatus Temperature:56.1791 Current:9.18727 Voltage:3.41406 JuncTemp:72.8079"
    // Note this doesn't follow standard -key=value format, so each field will be an arg in the message
    // and will need to be parsed manually.

    let mut fields = Vec::with_capacity(4);
    let mut args = msg.message.split_ascii_whitespace();
    while let Some(arg) = args.next() {
        let (key, value) = arg.split_once(':').ok_or(anyhow::anyhow!("Invalid format"))?;
        let val = value.parse::<f64>()?;
        fields.push((key.to_lowercase(), val));
    }
    Ok(vec![fields.into_iter().fold(
        DataPoint::builder("lamp"),
        |builder, (key, value)| builder.field(key, value)
    )
    .timestamp(timestamp.timestamp_nanos_opt().unwrap() as i64)
    .build()?])
}

fn run_to_lineprotocol(msg: &LogMessage, timestamp: chrono::DateTime<chrono::Utc>) -> Result<Vec<DataPoint>> {
    let mut points = Vec::new();
    let mut args = msg.message.split_ascii_whitespace();
    
    let action = args.next().ok_or(anyhow::anyhow!("Missing action"))?;
    
    // Create base point for run_action
    let mut point = DataPoint::builder("run_action")
        .tag("type", action.to_lowercase())
        .timestamp(timestamp.timestamp_nanos_opt().unwrap() as i64);

    match action {
        "Stage" | "Cycle" | "Step" => {
            let value = args.next()
                .ok_or(anyhow::anyhow!("Missing value"))?
                .parse::<i64>()?;
            
            point = point.field(action.to_lowercase(), value);
            points.push(point.build()?);

            // Also create run_status point
            points.push(DataPoint::builder("run_status")
                .tag("type", action.to_lowercase())
                .field(action.to_lowercase(), value)
                .timestamp(timestamp.timestamp_nanos_opt().unwrap() as i64)
                .build()?);
        },
        "Holding" => {
            if let Some(time_str) = args.next() {
                let time = time_str.parse::<f64>()?;
                point = point.field("holdtime", time);
            }
            points.push(point.build()?);
        },
        "Ramping" | "Acquiring" => {
            // These just need the basic point with type tag
            points.push(point.build()?);
        },
        "Error" | "Ended" | "Aborted" | "Stopped" | "Starting" => {
            // Collect remaining message
            let remaining = args.collect::<Vec<_>>().join(" ");
            if !remaining.is_empty() {
                point = point.field("message", remaining);
            }
            points.push(point.build()?);
        },
        "Collected" => {
            points.push(point.build()?);
        },
        _ => {
            // Handle other cases
            let message = format!("{} {}", action, args.collect::<Vec<_>>().join(" "));
            point = point
                .tag("type", "Other")
                .field("message", message);
            points.push(point.build()?);
        }
    }

    Ok(points)
}

fn temperature_to_lineprotocol(msg: &LogMessage, timestamp: chrono::DateTime<chrono::Utc>) -> Result<Vec<DataPoint>> {
    let mut points = Vec::new();
    let ts = timestamp.timestamp_nanos_opt().unwrap() as i64;

    // Parse the message into key-value pairs
    let mut args = msg.message.split_ascii_whitespace()
        .filter(|s| s.contains('='))
        .map(|s| {
            let parts: Vec<&str> = s.trim_start_matches('-').split('=').collect();
            (parts[0].to_lowercase(), parts[1].to_string())
        });

    // Handle sample and block temperatures for each zone
    if let (Some((_, sample_str)), Some((_, block_str))) = (
        args.find(|(key, _)| key == "sample"),
        args.find(|(key, _)| key == "block")
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
            points.push(DataPoint::builder("temperature")
                .tag("loc", "zones")
                .tag("zone", i.to_string())
                .field("sample", *sample)
                .field("block", *block)
                .timestamp(ts)
                .build()?);
        }
    }

    // Parse remaining args again since we consumed the iterator
    let args = msg.message.split_ascii_whitespace()
        .filter(|s| s.contains('='))
        .map(|s| {
            let parts: Vec<&str> = s.trim_start_matches('-').split('=').collect();
            (parts[0].to_lowercase(), parts[1].to_string())
        });

    // Handle cover and heatsink temperatures
    for (key, value) in args {
        match key.as_str() {
            "cover" => {
                points.push(DataPoint::builder("temperature")
                    .tag("loc", "cover")
                    .field("cover", value.parse::<f64>()?)
                    .timestamp(ts)
                    .build()?);
            },
            "heatsink" => {
                points.push(DataPoint::builder("temperature")
                    .tag("loc", "heatsink")
                    .field("heatsink", value.parse::<f64>()?)
                    .timestamp(ts)
                    .build()?);
            },
            _ => {} // Ignore other fields
        }
    }

    Ok(points)
}

fn time_to_lineprotocol(msg: &LogMessage, timestamp: chrono::DateTime<chrono::Utc>) -> Result<Vec<DataPoint>> {
    let ts = timestamp.timestamp_nanos_opt().unwrap() as i64;
    let mut point = DataPoint::builder("run_time");

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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use influxdb2::models::WriteDataPoint;

    #[test]
    fn test_ledparse_valid_message() {
        let msg = LogMessage {
            topic: "LEDStatus".to_string(),
            message: "Temperature:56.1791 Current:9.18727 Voltage:3.41406 JuncTemp:72.8079".to_string(),
        };
        let timestamp = chrono::Utc.timestamp_nanos(1_000_000_000);
        
        let points = ledstatus_to_lineprotocol(&msg, timestamp).unwrap();
        assert_eq!(points.len(), 1);

        let mut buf = Vec::new();
        points[0].write_data_point_to(&mut buf).unwrap();
        let line = String::from_utf8(buf).unwrap();
        assert_eq!(line, "lamp current=9.18727,junctemp=72.8079,temperature=56.1791,voltage=3.41406 1000000000\n");
    }

    #[test]
    fn test_ledparse_invalid_format() {
        let msg = LogMessage {
            topic: "LEDStatus".to_string(),
            message: "Invalid message format".to_string(),
        };
        let timestamp = chrono::Utc.timestamp_nanos(1_000_000_000);
        
        assert!(ledstatus_to_lineprotocol(&msg, timestamp).is_err());
    }

    #[test]
    fn test_ledparse_invalid_number() {
        let msg = LogMessage {
            topic: "LEDStatus".to_string(),
            message: "Temperature:not_a_number".to_string(),
        };
        let timestamp = chrono::Utc.timestamp_nanos(1_000_000_000);
        
        assert!(ledstatus_to_lineprotocol(&msg, timestamp).is_err());
    }

    #[test]
    fn test_run_stage_message() {
        let msg = LogMessage {
            topic: "Run".to_string(),
            message: "Stage 2".to_string(),
        };
        let timestamp = chrono::Utc.timestamp_nanos(1_000_000_000);
        
        let points = run_to_lineprotocol(&msg, timestamp).unwrap();
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
        
        let points = run_to_lineprotocol(&msg, timestamp).unwrap();
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
        
        let points = temperature_to_lineprotocol(&msg, timestamp).unwrap();
        
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
        
        let point = time_to_lineprotocol(&msg, timestamp).unwrap();
        
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


