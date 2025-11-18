# qs-monitor

A monitoring system for QuantStudio qPCR machines, which monitors multiple instruments, collects temperature, run status, and LED status data, and can send metrics to InfluxDB. It has optional Matrix bot integration which provides notifications and remote control capabilities.

Relies on [qslib](https://github.com/cgevans/qslib) for communication with the machines.

## Features

- **Multi-machine monitoring**: Monitor multiple QuantStudio instruments simultaneously
- **Automatic reconnection**: Handles connection failures and automatically reconnects with exponential backoff
- **Data collection**: Collects temperature, run status, time, and LED status data
- **InfluxDB integration**: Optional export of metrics to InfluxDB for time-series analysis
- **Matrix bot**: Optional Matrix bot for notifications and remote machine control
- **Robust error handling**: Logs errors without stopping the monitoring tasks

## Building

```bash
cargo build --release
```

## Configuration

Configuration is provided via a TOML file (default: `config.toml`). See `example-conf.toml` for a complete example.

### Machine Configuration

```toml
[[machines]]
name = "qpcr1"
host = "1.2.3.4"
```

### InfluxDB Configuration (Optional)

```toml
[influxdb]
url = "http://localhost:8086"
org = "my-org"
bucket = "qpcr-data"
token = "your-token-here"
batch_size = 100  # Optional, defaults to 100
flush_interval_ms = 10000  # Optional, defaults to 10000ms
```

### Matrix Bot Configuration (Optional)

```toml
[matrix]
host = "https://matrix.org"
user = "qsbot"
password = "your-secure-password"
rooms = ["!roomid:matrix.org"]
session_file = "matrix_session.json"
allow_verification = false  # Enable temporarily for session verification
allow_commands = true       # Allow commands to be sent to the bot
allow_control = false       # Allow commands that can control the instruments
```

### Global Settings

```toml
[global]
reconnect_wait_seconds = 60  # Optional, defaults to 60
```

## Usage

```bash
qs-monitor [OPTIONS]

Options:
  -c, --config <PATH>    Configuration file path [default: config.toml]
  -l, --log-level <LEVEL>  Log level [default: info] [possible values: trace, debug, info, warn, error]
```

## Matrix Bot Commands

When Matrix integration is enabled, the bot supports the following commands:

- `!status [machine]` - Show status of machine(s)
- `!command <machine> <command>` - Send a raw command to a machine
- `!close <machine>` - Close drawer and lower cover (requires control)
- `!open <machine>` - Open drawer (requires control)
- `!abort <machine>` - Abort current run (requires control)
- `!stop <machine>` - Stop current run (requires control)
- `!help [command]` - Show help or detailed help for a command

## Data Points

The monitor collects the following types of data points:

- **temperature**: Zone, cover, and heatsink temperatures
- **run_time**: Elapsed, remaining, and active time
- **run_action**: Run events (Stage, Cycle, Step, Holding, Ramping, Collected, Acquiring, Error, Ended, Aborted, Stopped, Starting)
- **run_status**: Current run status
- **lamp**: LED status (temperature, current, voltage, junction temperature)
- **fluorescence**: Filter data collected during runs

## License

Licensed under the EUPL-1.2. See LICENSE.txt for details.

## Author

Constantine Evans

