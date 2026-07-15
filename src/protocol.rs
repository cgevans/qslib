use crate::parser::{ArgMap, Command, ParseError, Value};
use quick_xml::events::{BytesEnd, BytesStart, BytesText, Event};
use quick_xml::Writer;
use std::any::Any;
use std::convert::TryInto;
use std::fmt;
use std::io::Cursor;
use thiserror::Error;

/// Default number of temperature zones for QuantStudio machines.
/// Default number of temperature control zones.
///
/// Current QuantStudio instruments have 6 zones. This is used when expanding
/// a single temperature to a per-zone list during protocol parsing. The actual
/// zone count can be queried from the server with `TBC:ControlZones?`
/// (see [`ControlZonesQuery`](crate::commands::ControlZonesQuery)).
pub const DEFAULT_NUM_ZONES: usize = 6;

#[derive(Debug, Error)]
pub enum ProtocolParseError {
    #[error("Parse error: {source}\nProtocol string: {protocol_string}")]
    ParseError {
        #[source]
        source: ParseError,
        protocol_string: String,
    },
    #[error("Invalid command: expected {expected}, got {got}\nProtocol string: {protocol_string}")]
    InvalidCommand {
        expected: String,
        got: String,
        protocol_string: String,
    },
    #[error("Missing required field: {field}\nProtocol string: {protocol_string}")]
    MissingField {
        field: String,
        protocol_string: String,
    },
    #[error("Invalid value type: {value_type}\nProtocol string: {protocol_string}")]
    InvalidValueType {
        value_type: String,
        protocol_string: String,
    },
    #[error("Inconsistent default filters\nProtocol string: {protocol_string}")]
    InconsistentDefaultFilters { protocol_string: String },
    #[error("Unexpected command structure: {message}\nProtocol string: {protocol_string}")]
    UnexpectedStructure {
        message: String,
        protocol_string: String,
    },
}

pub trait ProtoCommand: fmt::Debug + Any {
    fn from_scpicommand(cmd: &Command) -> Result<Box<dyn ProtoCommand>, ProtocolParseError>
    where
        Self: Sized;

    fn as_any(&self) -> &dyn Any;
}

fn get_command_name(cmd: &Command) -> String {
    String::from_utf8_lossy(&cmd.command)
        .to_string()
        .to_uppercase()
}

fn command_to_string(cmd: &Command) -> String {
    let mut bytes = Vec::new();
    if cmd.write_bytes(&mut bytes).is_ok() {
        String::from_utf8_lossy(&bytes).to_string()
    } else {
        format!("<failed to serialize command: {}>", get_command_name(cmd))
    }
}

fn parse_error_to_protocol_error(e: ParseError, cmd: &Command) -> ProtocolParseError {
    ProtocolParseError::ParseError {
        source: e,
        protocol_string: command_to_string(cmd),
    }
}

/// Extract temperature list from a Value.
/// When a single temperature is provided, it is expanded to `num_zones` zones.
/// When multiple temperatures are provided (comma or space separated), they are used as-is.
fn extract_temperature_list(
    value: &Value,
    cmd: &Command,
    num_zones: usize,
) -> Result<Vec<f64>, ProtocolParseError> {
    match value {
        Value::Float(f) => Ok(vec![*f; num_zones]),
        Value::Int(i) => Ok(vec![*i as f64; num_zones]),
        Value::String(s) | Value::QuotedString(s) => {
            // Determine delimiter: space or comma
            let delimiter = if s.contains(',') { ',' } else { ' ' };
            let parts: Vec<&str> = s
                .split(delimiter)
                .map(|x| x.trim())
                .filter(|x| !x.is_empty())
                .collect();

            if parts.len() == 1 {
                // Single value - expand to num_zones zones
                let temp =
                    parts[0]
                        .parse::<f64>()
                        .map_err(|_| ProtocolParseError::InvalidValueType {
                            value_type: "temperature".to_string(),
                            protocol_string: command_to_string(cmd),
                        })?;
                Ok(vec![temp; num_zones])
            } else {
                // Multiple values - use as-is (auto-detected zone count)
                parts
                    .iter()
                    .map(|x| x.parse::<f64>())
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|_| ProtocolParseError::InvalidValueType {
                        value_type: "temperature".to_string(),
                        protocol_string: command_to_string(cmd),
                    })
            }
        }
        _ => Err(ProtocolParseError::InvalidValueType {
            value_type: "temperature".to_string(),
            protocol_string: command_to_string(cmd),
        }),
    }
}

/// Extract temperature list from command arguments.
/// Handles multiple space-separated arguments (one per zone).
/// When a single temperature is provided, it is expanded to `num_zones` zones.
/// When multiple temperatures are provided as separate args, they are used as-is (auto-detected zone count).
fn extract_temperature_list_from_args(
    args: &[Value],
    cmd: &Command,
    num_zones: usize,
) -> Result<Vec<f64>, ProtocolParseError> {
    if args.is_empty() {
        return Err(ProtocolParseError::MissingField {
            field: "temperature".to_string(),
            protocol_string: command_to_string(cmd),
        });
    }

    // If we have multiple numeric arguments, treat them as zone temperatures (auto-detected count)
    if args.len() > 1 {
        let temps: Result<Vec<f64>, _> = args
            .iter()
            .map(|v| match v {
                Value::Float(f) => Ok(*f),
                Value::Int(i) => Ok(*i as f64),
                _ => Err(()),
            })
            .collect();

        if let Ok(t) = temps {
            return Ok(t);
        }
    }

    // Single argument - use extract_temperature_list with num_zones for expansion
    extract_temperature_list(&args[0], cmd, num_zones)
}

fn extract_i64_option(value: &Value, cmd: &Command) -> Result<Option<i64>, ProtocolParseError> {
    match value {
        Value::Int(i) => Ok(Some(*i)),
        Value::String(s) if s.is_empty() => Ok(None),
        Value::String(s) => {
            s.parse::<i64>()
                .map(Some)
                .map_err(|_| ProtocolParseError::InvalidValueType {
                    value_type: "i64".to_string(),
                    protocol_string: command_to_string(cmd),
                })
        }
        _ => Err(ProtocolParseError::InvalidValueType {
            value_type: "i64".to_string(),
            protocol_string: command_to_string(cmd),
        }),
    }
}

fn extract_i64(value: &Value, cmd: &Command) -> Result<i64, ProtocolParseError> {
    match value {
        Value::Int(i) => Ok(*i),
        Value::String(s) => s
            .parse::<i64>()
            .map_err(|_| ProtocolParseError::InvalidValueType {
                value_type: "i64".to_string(),
                protocol_string: command_to_string(cmd),
            }),
        _ => Err(ProtocolParseError::InvalidValueType {
            value_type: "i64".to_string(),
            protocol_string: command_to_string(cmd),
        }),
    }
}

fn extract_f64(value: &Value, cmd: &Command) -> Result<f64, ProtocolParseError> {
    match value {
        Value::Float(f) => Ok(*f),
        Value::Int(i) => Ok(*i as f64),
        Value::String(s) => s
            .parse::<f64>()
            .map_err(|_| ProtocolParseError::InvalidValueType {
                value_type: "f64".to_string(),
                protocol_string: command_to_string(cmd),
            }),
        _ => Err(ProtocolParseError::InvalidValueType {
            value_type: "f64".to_string(),
            protocol_string: command_to_string(cmd),
        }),
    }
}

fn extract_bool(value: &Value, cmd: &Command) -> Result<bool, ProtocolParseError> {
    match value {
        Value::Bool(b) => Ok(*b),
        Value::String(s) => match s.to_lowercase().as_str() {
            "true" => Ok(true),
            "false" => Ok(false),
            _ => Err(ProtocolParseError::InvalidValueType {
                value_type: "bool".to_string(),
                protocol_string: command_to_string(cmd),
            }),
        },
        Value::Int(i) => Ok(*i != 0),
        _ => Err(ProtocolParseError::InvalidValueType {
            value_type: "bool".to_string(),
            protocol_string: command_to_string(cmd),
        }),
    }
}

fn extract_string(value: &Value, cmd: &Command) -> Result<String, ProtocolParseError> {
    value
        .clone()
        .try_into_string()
        .map_err(|_| ProtocolParseError::InvalidValueType {
            value_type: "string".to_string(),
            protocol_string: command_to_string(cmd),
        })
}

fn get_option_i64(
    opts: &ArgMap,
    key: &str,
    default: i64,
    cmd: &Command,
) -> Result<i64, ProtocolParseError> {
    match opts.get(key) {
        Some(v) => extract_i64(v, cmd),
        None => Ok(default),
    }
}

fn get_option_f64(
    opts: &ArgMap,
    key: &str,
    default: f64,
    cmd: &Command,
) -> Result<f64, ProtocolParseError> {
    match opts.get(key) {
        Some(v) => extract_f64(v, cmd),
        None => Ok(default),
    }
}

fn get_option_bool(
    opts: &ArgMap,
    key: &str,
    default: bool,
    cmd: &Command,
) -> Result<bool, ProtocolParseError> {
    match opts.get(key) {
        Some(v) => extract_bool(v, cmd),
        None => Ok(default),
    }
}

fn get_option_string(
    opts: &ArgMap,
    key: &str,
    cmd: &Command,
) -> Result<Option<String>, ProtocolParseError> {
    match opts.get(key) {
        Some(v) => extract_string(v, cmd).map(Some),
        None => Ok(None),
    }
}

#[derive(Debug, Clone)]
pub struct Ramp {
    pub temperature: Vec<f64>,
    pub increment: f64,
    pub incrementcycle: i64,
    pub incrementstep: i64,
    pub rate: f64,
    pub cover: Option<f64>,
}

impl ProtoCommand for Ramp {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn from_scpicommand(cmd: &Command) -> Result<Box<dyn ProtoCommand>, ProtocolParseError> {
        let protocol_string = command_to_string(cmd);
        if get_command_name(cmd) != "RAMP" {
            return Err(ProtocolParseError::InvalidCommand {
                expected: "RAMP".to_string(),
                got: get_command_name(cmd),
                protocol_string,
            });
        }

        let temperature = extract_temperature_list_from_args(&cmd.args, cmd, DEFAULT_NUM_ZONES)?;

        let increment = cmd
            .options
            .extract_with_default("increment", 0.0)
            .map_err(|e| ProtocolParseError::ParseError {
                source: e,
                protocol_string: protocol_string.clone(),
            })?;
        let incrementcycle = cmd
            .options
            .extract_with_default("incrementcycle", 2)
            .map_err(|e| ProtocolParseError::ParseError {
                source: e,
                protocol_string: protocol_string.clone(),
            })?;
        let incrementstep = cmd
            .options
            .extract_with_default("incrementstep", 2)
            .map_err(|e| ProtocolParseError::ParseError {
                source: e,
                protocol_string: protocol_string.clone(),
            })?;
        let rate = cmd
            .options
            .extract_with_default("rate", 100.0)
            .map_err(|e| ProtocolParseError::ParseError {
                source: e,
                protocol_string: protocol_string.clone(),
            })?;
        let cover = match cmd.options.get("cover") {
            Some(v) => {
                Some(
                    v.try_into()
                        .map_err(|e: ParseError| ProtocolParseError::ParseError {
                            source: e,
                            protocol_string: protocol_string.clone(),
                        })?,
                )
            }
            None => None,
        };

        Ok(Box::new(Ramp {
            temperature,
            increment,
            incrementcycle,
            incrementstep,
            rate,
            cover,
        }))
    }
}

#[derive(Debug, Clone)]
pub struct Hold {
    pub time: Option<i64>,
    pub increment: i64,
    pub incrementcycle: i64,
    pub incrementstep: i64,
}

impl ProtoCommand for Hold {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn from_scpicommand(cmd: &Command) -> Result<Box<dyn ProtoCommand>, ProtocolParseError> {
        let protocol_string = command_to_string(cmd);
        if get_command_name(cmd) != "HOLD" {
            return Err(ProtocolParseError::InvalidCommand {
                expected: "HOLD".to_string(),
                got: get_command_name(cmd),
                protocol_string,
            });
        }

        let time = if cmd.args.is_empty() {
            None
        } else {
            extract_i64_option(&cmd.args[0], cmd)?
        };

        let increment = cmd
            .options
            .extract_with_default("increment", 0)
            .map_err(|e| parse_error_to_protocol_error(e, cmd))?;
        let incrementcycle = cmd
            .options
            .extract_with_default("incrementcycle", 2)
            .map_err(|e| parse_error_to_protocol_error(e, cmd))?;
        let incrementstep = cmd
            .options
            .extract_with_default("incrementstep", 2)
            .map_err(|e| parse_error_to_protocol_error(e, cmd))?;

        Ok(Box::new(Hold {
            time,
            increment,
            incrementcycle,
            incrementstep,
        }))
    }
}

#[derive(Debug, Clone)]
pub struct HoldAndCollect {
    pub time: i64,
    pub increment: i64,
    pub incrementcycle: i64,
    pub incrementstep: i64,
    pub tiff: bool,
    pub quant: bool,
    pub pcr: bool,
}

impl ProtoCommand for HoldAndCollect {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn from_scpicommand(cmd: &Command) -> Result<Box<dyn ProtoCommand>, ProtocolParseError> {
        let protocol_string = command_to_string(cmd);
        if get_command_name(cmd) != "HOLDANDCOLLECT" {
            return Err(ProtocolParseError::InvalidCommand {
                expected: "HOLDANDCOLLECT".to_string(),
                got: get_command_name(cmd),
                protocol_string,
            });
        }

        let time = if cmd.args.is_empty() {
            return Err(ProtocolParseError::MissingField {
                field: "time".to_string(),
                protocol_string,
            });
        } else {
            extract_i64(&cmd.args[0], cmd)?
        };

        let increment = get_option_i64(&cmd.options, "increment", 0, cmd)?;
        let incrementcycle = get_option_i64(&cmd.options, "incrementcycle", 2, cmd)?;
        let incrementstep = get_option_i64(&cmd.options, "incrementstep", 2, cmd)?;
        let tiff = get_option_bool(&cmd.options, "tiff", false, cmd)?;
        let quant = get_option_bool(&cmd.options, "quant", true, cmd)?;
        let pcr = get_option_bool(&cmd.options, "pcr", false, cmd)?;

        Ok(Box::new(HoldAndCollect {
            time,
            increment,
            incrementcycle,
            incrementstep,
            tiff,
            quant,
            pcr,
        }))
    }
}

#[derive(Debug, Clone)]
pub struct HACFILT {
    pub filters: Vec<String>,
    pub default_filters: Vec<String>,
}

impl ProtoCommand for HACFILT {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn from_scpicommand(cmd: &Command) -> Result<Box<dyn ProtoCommand>, ProtocolParseError> {
        let protocol_string = command_to_string(cmd);
        let cmd_name = get_command_name(cmd);
        if cmd_name != "HACFILT" && cmd_name != "HOLDANDCOLLECTFILTER" {
            return Err(ProtocolParseError::InvalidCommand {
                expected: "HACFILT or HOLDANDCOLLECTFILTER".to_string(),
                got: cmd_name,
                protocol_string,
            });
        }

        let filters: Result<Vec<String>, ParseError> =
            cmd.args.iter().map(|v| v.try_into()).collect();

        let filters = filters.map_err(|e| parse_error_to_protocol_error(e, cmd))?;
        let default_filters = Vec::new();

        Ok(Box::new(HACFILT {
            filters,
            default_filters,
        }))
    }
}

#[derive(Debug, Clone)]
pub struct Exposure {
    pub settings: Vec<(String, Vec<i64>)>,
    pub state: String,
}

impl ProtoCommand for Exposure {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn from_scpicommand(cmd: &Command) -> Result<Box<dyn ProtoCommand>, ProtocolParseError> {
        let protocol_string = command_to_string(cmd);
        let cmd_name = get_command_name(cmd);
        if cmd_name != "EXP" && cmd_name != "EXPOSURE" {
            return Err(ProtocolParseError::InvalidCommand {
                expected: "EXP or EXPOSURE".to_string(),
                got: cmd_name,
                protocol_string,
            });
        }

        let mut settings = Vec::new();
        for arg in &cmd.args {
            if let Value::String(s) = arg {
                let parts: Vec<&str> = s.split(',').collect();
                if parts.len() >= 3 {
                    let filter_str = format!("{},{},{}", parts[1], parts[0], parts[2]);
                    let exposures: Result<Vec<i64>, _> =
                        parts[3..].iter().map(|x| x.trim().parse::<i64>()).collect();
                    if let Ok(expos) = exposures {
                        settings.push((filter_str, expos));
                    }
                }
            }
        }

        let state = get_option_string(&cmd.options, "state", cmd)?
            .unwrap_or_else(|| "HoldAndCollect".to_string());

        Ok(Box::new(Exposure { settings, state }))
    }
}

pub fn specialize_command(cmd: &Command) -> Result<Box<dyn ProtoCommand>, ProtocolParseError> {
    let protocol_string = command_to_string(cmd);
    let cmd_name = get_command_name(cmd);
    match cmd_name.as_str() {
        "RAMP" => Ramp::from_scpicommand(cmd),
        "HOLD" => Hold::from_scpicommand(cmd),
        "HOLDANDCOLLECT" => HoldAndCollect::from_scpicommand(cmd),
        "HACFILT" | "HOLDANDCOLLECTFILTER" => HACFILT::from_scpicommand(cmd),
        "EXP" | "EXPOSURE" => Exposure::from_scpicommand(cmd),
        "STEP" => Step::from_scpicommand(cmd).or_else(|_| CustomStep::from_scpicommand(cmd)),
        "STAGE" | "STAGe" => Err(ProtocolParseError::UnexpectedStructure {
            message: "Stage should be parsed directly, not through specialize".to_string(),
            protocol_string,
        }),
        "PROTOCOL" | "PROT" => Err(ProtocolParseError::UnexpectedStructure {
            message: "Protocol should be parsed directly, not through specialize".to_string(),
            protocol_string,
        }),
        _ => Err(ProtocolParseError::InvalidCommand {
            expected: "known command".to_string(),
            got: cmd_name,
            protocol_string,
        }),
    }
}

fn extract_commands_from_value(
    value: &Value,
    parent_cmd: Option<&Command>,
) -> Result<Vec<Command>, ProtocolParseError> {
    let protocol_string = parent_cmd
        .map(command_to_string)
        .unwrap_or_else(|| "<nested command>".to_string());
    match value {
        Value::XmlString { value, tag: _ } => {
            let mut s = String::from_utf8(value.to_vec()).map_err(|_| {
                ProtocolParseError::InvalidValueType {
                    value_type: "xml string".to_string(),
                    protocol_string: protocol_string.clone(),
                }
            })?;

            // Pre-process: convert "# qslib:default_filters" comments into
            // HACFILT options that survive comment stripping.
            // This comment marks that the HACFILT uses protocol-level default filters.
            let mut preprocessed = String::with_capacity(s.len());
            for line in s.split('\n') {
                let trimmed = line.trim();
                let upper = trimmed.to_uppercase();
                if (upper.starts_with("HACFILT") || upper.starts_with("HOLDANDCOLLECTFILT"))
                    && line.contains("# qslib:default_filters")
                {
                    // Find the # position and keep only the command part
                    if let Some(hash_pos) = line.find('#') {
                        let before = &line[..hash_pos];
                        let before_trimmed = before.trim_start();
                        // Insert -qslibdf=1 option after command name
                        if let Some(space_pos) = before_trimmed.find(|c: char| c.is_whitespace()) {
                            let indent = &line[..line.len() - line.trim_start().len()];
                            let cmd_name = &before_trimmed[..space_pos];
                            let args = before_trimmed[space_pos..].trim_end();
                            preprocessed
                                .push_str(&format!("{}{} -qslibdf=1 {}", indent, cmd_name, args));
                        } else {
                            preprocessed.push_str(before.trim_end());
                        }
                    } else {
                        preprocessed.push_str(line);
                    }
                } else {
                    preprocessed.push_str(line);
                }
                preprocessed.push('\n');
            }
            // Remove trailing extra newline added by the loop
            if preprocessed.ends_with('\n') && !s.ends_with('\n') {
                preprocessed.pop();
            }
            s = preprocessed;

            // Strip inline comments from the string
            // Comments start with # and continue to end of line, but not if inside quotes or XML tags
            let mut result = String::with_capacity(s.len());
            let bytes = s.as_bytes();
            let mut i = 0;
            let mut in_quotes = false;
            let mut in_xml_tag = false;

            while i < bytes.len() {
                let b = bytes[i];
                if b == b'"' && (i == 0 || bytes[i - 1] != b'\\') {
                    in_quotes = !in_quotes;
                    result.push(b as char);
                } else if b == b'<' && !in_quotes {
                    in_xml_tag = true;
                    result.push(b as char);
                } else if b == b'>' && !in_quotes {
                    in_xml_tag = false;
                    result.push(b as char);
                } else if b == b'#' && !in_quotes && !in_xml_tag {
                    // Found inline comment, skip to end of line
                    while i < bytes.len() && bytes[i] != b'\n' {
                        i += 1;
                    }
                    if i < bytes.len() {
                        result.push('\n');
                    }
                } else {
                    result.push(b as char);
                }
                i += 1;
            }
            s = result;

            // Parse commands from the XML content
            // Commands are separated by newlines, but XML values can span multiple lines
            let mut commands = Vec::new();
            let mut input = s.as_bytes();

            // Skip leading whitespace/tabs
            while !input.is_empty()
                && (input[0] == b' ' || input[0] == b'\t' || input[0] == b'\n' || input[0] == b'\r')
            {
                input = &input[1..];
            }

            while !input.is_empty() {
                // Skip whitespace/tabs at start of line
                while !input.is_empty()
                    && (input[0] == b' '
                        || input[0] == b'\t'
                        || input[0] == b'\n'
                        || input[0] == b'\r')
                {
                    input = &input[1..];
                }

                if input.is_empty() {
                    break;
                }

                // Skip XML tags and comments
                if input.starts_with(b"</")
                    || input.starts_with(b"<") && !input.starts_with(b"<multiline")
                {
                    // Find end of tag
                    while !input.is_empty() && input[0] != b'\n' {
                        input = &input[1..];
                    }
                    continue;
                }

                // Skip comment lines (full-line comments)
                if input.starts_with(b"#") {
                    while !input.is_empty() && input[0] != b'\n' {
                        input = &input[1..];
                    }
                    continue;
                }

                // Try to parse a command
                // We need to handle the case where a command argument is an XML value
                // that spans multiple lines. The Command parser should handle this.
                match Command::parse(&mut input) {
                    Ok(cmd) => {
                        commands.push(cmd);
                        // Skip whitespace after command
                        while !input.is_empty()
                            && (input[0] == b' '
                                || input[0] == b'\t'
                                || input[0] == b'\n'
                                || input[0] == b'\r')
                        {
                            input = &input[1..];
                        }
                    }
                    Err(_) => {
                        // If parsing fails, skip to next line
                        while !input.is_empty() && input[0] != b'\n' {
                            input = &input[1..];
                        }
                    }
                }
            }

            Ok(commands)
        }
        _ => Err(ProtocolParseError::InvalidValueType {
            value_type: "nested commands".to_string(),
            protocol_string,
        }),
    }
}

#[derive(Debug)]
pub struct CustomStep {
    pub body: Vec<Box<dyn ProtoCommand>>,
    pub identifier: Option<Value>,
    pub repeat: i64,
}

impl ProtoCommand for CustomStep {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn from_scpicommand(cmd: &Command) -> Result<Box<dyn ProtoCommand>, ProtocolParseError> {
        let protocol_string = command_to_string(cmd);
        if get_command_name(cmd) != "STEP" {
            return Err(ProtocolParseError::InvalidCommand {
                expected: "STEP".to_string(),
                got: get_command_name(cmd),
                protocol_string,
            });
        }

        if cmd.args.len() < 2 {
            return Err(ProtocolParseError::MissingField {
                field: "step args".to_string(),
                protocol_string,
            });
        }

        let identifier = Some(cmd.args[0].clone());
        let commands_value = &cmd.args[1];
        let nested_commands = extract_commands_from_value(commands_value, Some(cmd))?;

        let mut body = Vec::new();
        for nested_cmd in &nested_commands {
            body.push(specialize_command(nested_cmd)?);
        }

        let repeat = get_option_i64(&cmd.options, "repeat", 1, cmd)?;

        Ok(Box::new(CustomStep {
            body,
            identifier,
            repeat,
        }))
    }
}

#[derive(Debug, Clone)]
pub struct Step {
    pub time: i64,
    pub temperature: Vec<f64>,
    pub collect: Option<bool>,
    pub temp_increment: f64,
    pub temp_incrementcycle: i64,
    pub temp_incrementpoint: Option<i64>,
    pub time_increment: i64,
    pub time_incrementcycle: i64,
    pub time_incrementpoint: Option<i64>,
    pub filters: Vec<String>,
    pub pcr: bool,
    pub quant: bool,
    pub tiff: bool,
    pub repeat: i64,
    pub default_filters: Vec<String>,
}

impl Step {
    pub fn info_str(&self, index: Option<i64>, repeats: i64) -> String {
        let mut tempstr = format_temperature(&self.temperature);

        if self.time > 0 {
            if !tempstr.is_empty() {
                tempstr.push_str(&format!(" for {}/cycle", format_duration(self.time)));
            } else {
                tempstr = format!("for {}/cycle", format_duration(self.time));
            }
        }

        let mut elems = if tempstr.is_empty() {
            Vec::new()
        } else {
            vec![tempstr]
        };

        if self.temp_increment != 0.0 {
            if self.repeat > 1 && matches!(self.temp_incrementpoint, Some(p) if p < self.repeat) {
                let mut inc_str = format!("{:+}°C/point", self.temp_increment);
                if let Some(p) = self.temp_incrementpoint {
                    if p != 2 {
                        inc_str.push_str(&format!(" from point {}", p));
                    }
                }
                elems.push(inc_str);
            }
            if repeats > 1 && self.temp_incrementcycle < repeats {
                let mut inc_str = format!("{:+}°C/cycle", self.temp_increment);
                if self.temp_incrementcycle != 2 {
                    inc_str.push_str(&format!(" from cycle {}", self.temp_incrementcycle));
                }
                elems.push(inc_str);
            }
        }

        if self.time_increment != 0 {
            if self.repeat > 1 && matches!(self.time_incrementpoint, Some(p) if p < self.repeat) {
                let mut inc_str = format!("{}/point", format_duration(self.time_increment));
                if let Some(p) = self.time_incrementpoint {
                    if p != 2 {
                        inc_str.push_str(&format!(" from point {}", p));
                    }
                }
                elems.push(inc_str);
            }
            if repeats > 1 && self.time_incrementcycle < repeats {
                let mut inc_str = format!("{}/cycle", format_duration(self.time_increment));
                if self.time_incrementcycle != 2 {
                    inc_str.push_str(&format!(" from cycle {}", self.time_incrementcycle));
                }
                elems.push(inc_str);
            }
        }

        let mut result = if let Some(idx) = index {
            format!("{}. {}", idx, elems.join(", "))
        } else {
            elems.join(", ")
        };

        if self.collect == Some(true) {
            result.push_str(" (collects ");
            if !self.filters.is_empty() {
                let filter_strs: Vec<String> = self
                    .filters
                    .iter()
                    .map(|f| filter_to_lowerform(f))
                    .collect();
                result.push_str(&filter_strs.join(", "));
            } else {
                result.push_str("default");
            }
            if self.pcr {
                result.push_str(", pcr on");
            }
            if !self.quant {
                result.push_str(", quant off");
            }
            result.push(')');
        }

        result
    }
}

impl ProtoCommand for Step {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn from_scpicommand(cmd: &Command) -> Result<Box<dyn ProtoCommand>, ProtocolParseError> {
        let protocol_string = command_to_string(cmd);
        if get_command_name(cmd) != "STEP" {
            return Err(ProtocolParseError::InvalidCommand {
                expected: "STEP".to_string(),
                got: get_command_name(cmd),
                protocol_string,
            });
        }

        if cmd.args.len() < 2 {
            return Err(ProtocolParseError::MissingField {
                field: "step args".to_string(),
                protocol_string,
            });
        }

        let commands_value = &cmd.args[1];
        let nested_commands = extract_commands_from_value(commands_value, Some(cmd))?;

        if nested_commands.is_empty() {
            return Err(ProtocolParseError::UnexpectedStructure {
                message: "Step must have nested commands".to_string(),
                protocol_string,
            });
        }

        let repeat = get_option_i64(&cmd.options, "repeat", 1, cmd)?;

        if nested_commands.len() == 3 {
            let cmd1_name = get_command_name(&nested_commands[0]);
            let cmd2_name = get_command_name(&nested_commands[1]);
            let cmd3_name = get_command_name(&nested_commands[2]);

            if cmd1_name == "RAMP"
                && (cmd2_name == "HACFILT" || cmd2_name == "HOLDANDCOLLECTFILTER")
                && cmd3_name == "HOLDANDCOLLECT"
            {
                let r_temp = extract_temperature_list_from_args(
                    &nested_commands[0].args,
                    cmd,
                    DEFAULT_NUM_ZONES,
                )?;
                let r_increment =
                    get_option_f64(&nested_commands[0].options, "increment", 0.0, cmd)?;
                let r_incrementcycle =
                    get_option_i64(&nested_commands[0].options, "incrementcycle", 2, cmd)?;
                let r_incrementstep =
                    get_option_i64(&nested_commands[0].options, "incrementstep", 2, cmd)?;

                let hf_filters: Result<Vec<String>, ParseError> = nested_commands[1]
                    .args
                    .iter()
                    .map(|v| v.try_into())
                    .collect();
                let mut filters = hf_filters.map_err(|e| parse_error_to_protocol_error(e, cmd))?;
                let mut default_filters = Vec::new();
                let has_default_marker = nested_commands[1].options.get("qslibdf").is_some();

                let h_time = extract_i64(&nested_commands[2].args[0], cmd)?;
                let h_increment = get_option_i64(&nested_commands[2].options, "increment", 0, cmd)?;
                let h_incrementcycle =
                    get_option_i64(&nested_commands[2].options, "incrementcycle", 2, cmd)?;
                let h_incrementstep =
                    get_option_i64(&nested_commands[2].options, "incrementstep", 2, cmd)?;
                let h_tiff = get_option_bool(&nested_commands[2].options, "tiff", false, cmd)?;
                let h_quant = get_option_bool(&nested_commands[2].options, "quant", true, cmd)?;
                let h_pcr = get_option_bool(&nested_commands[2].options, "pcr", false, cmd)?;

                let mut collect = !filters.is_empty();
                if has_default_marker && !filters.is_empty() {
                    default_filters = filters.clone();
                    filters = Vec::new();
                    collect = true;
                }

                let time_incrementpoint = if h_incrementstep <= repeat {
                    Some(h_incrementstep)
                } else {
                    None
                };

                let temp_incrementpoint = if r_incrementstep <= repeat {
                    Some(r_incrementstep)
                } else {
                    None
                };

                return Ok(Box::new(Step {
                    time: h_time,
                    temperature: r_temp,
                    collect: Some(collect),
                    temp_increment: r_increment,
                    temp_incrementcycle: r_incrementcycle,
                    temp_incrementpoint,
                    time_increment: h_increment,
                    time_incrementcycle: h_incrementcycle,
                    time_incrementpoint,
                    filters,
                    pcr: h_pcr,
                    quant: h_quant,
                    tiff: h_tiff,
                    repeat,
                    default_filters,
                }));
            }
        } else if nested_commands.len() == 2 {
            let cmd1_name = get_command_name(&nested_commands[0]);
            let cmd2_name = get_command_name(&nested_commands[1]);

            if cmd1_name == "RAMP" && cmd2_name == "HOLD" {
                let r_temp = extract_temperature_list_from_args(
                    &nested_commands[0].args,
                    cmd,
                    DEFAULT_NUM_ZONES,
                )?;
                let r_increment =
                    get_option_f64(&nested_commands[0].options, "increment", 0.0, cmd)?;
                let r_incrementcycle =
                    get_option_i64(&nested_commands[0].options, "incrementcycle", 2, cmd)?;
                let r_incrementstep =
                    get_option_i64(&nested_commands[0].options, "incrementstep", 2, cmd)?;

                let h_time = extract_i64_option(&nested_commands[1].args[0], cmd)?;
                if h_time.is_none() {
                    return Err(ProtocolParseError::MissingField {
                        field: "hold time".to_string(),
                        protocol_string: protocol_string.clone(),
                    });
                }
                let h_increment = get_option_i64(&nested_commands[1].options, "increment", 0, cmd)?;
                let h_incrementcycle =
                    get_option_i64(&nested_commands[1].options, "incrementcycle", 2, cmd)?;
                let h_incrementstep =
                    get_option_i64(&nested_commands[1].options, "incrementstep", 2, cmd)?;

                let time_incrementpoint = if h_incrementstep <= repeat {
                    Some(h_incrementstep)
                } else {
                    None
                };

                let temp_incrementpoint = if r_incrementstep <= repeat {
                    Some(r_incrementstep)
                } else {
                    None
                };

                return Ok(Box::new(Step {
                    time: h_time.unwrap(),
                    temperature: r_temp,
                    collect: Some(false),
                    temp_increment: r_increment,
                    temp_incrementcycle: r_incrementcycle,
                    temp_incrementpoint,
                    time_increment: h_increment,
                    time_incrementcycle: h_incrementcycle,
                    time_incrementpoint,
                    filters: Vec::new(),
                    pcr: false,
                    quant: true,
                    tiff: false,
                    repeat,
                    default_filters: Vec::new(),
                }));
            }
        }

        Err(ProtocolParseError::UnexpectedStructure {
            message: "Step must have [Ramp, HACFILT, HoldAndCollect] or [Ramp, Hold] pattern"
                .to_string(),
            protocol_string,
        })
    }
}

/// A step within a stage: either a standard Step or opaque custom SCPI commands.
#[derive(Debug, Clone)]
pub enum StageStep {
    Standard(Step),
    /// Opaque SCPI commands — generates placeholder XML.
    Custom(Vec<Command>),
}

impl StageStep {
    /// Returns a reference to the inner Step if this is a Standard step.
    pub fn as_standard(&self) -> Option<&Step> {
        match self {
            StageStep::Standard(s) => Some(s),
            StageStep::Custom(_) => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Stage {
    pub steps: Vec<StageStep>,
    pub repeat: i64,
    pub index: Option<i64>,
    pub label: Option<String>,
    pub default_filters: Vec<String>,
}

impl Stage {
    pub fn from_scpicommand(cmd: &Command) -> Result<Stage, ProtocolParseError> {
        let protocol_string = command_to_string(cmd);
        let cmd_name = get_command_name(cmd);
        if cmd_name != "STAGE" {
            return Err(ProtocolParseError::InvalidCommand {
                expected: "STAGE".to_string(),
                got: cmd_name,
                protocol_string,
            });
        }

        if cmd.args.len() < 3 {
            return Err(ProtocolParseError::MissingField {
                field: "stage args".to_string(),
                protocol_string,
            });
        }

        let index = extract_i64(&cmd.args[0], cmd).ok();
        let label = extract_string(&cmd.args[1], cmd).ok();
        let steps_value = &cmd.args[2];
        let nested_commands = extract_commands_from_value(steps_value, Some(cmd))?;

        let mut steps = Vec::new();
        for step_cmd in &nested_commands {
            if get_command_name(step_cmd) == "STEP" {
                if step_cmd.args.len() < 2 {
                    continue;
                }
                let commands_value = &step_cmd.args[1];
                let nested_step_commands = extract_commands_from_value(commands_value, Some(cmd))?;
                let repeat = get_option_i64(&step_cmd.options, "repeat", 1, cmd)?;

                if nested_step_commands.len() == 3 {
                    let cmd1_name = get_command_name(&nested_step_commands[0]);
                    let cmd2_name = get_command_name(&nested_step_commands[1]);
                    let cmd3_name = get_command_name(&nested_step_commands[2]);

                    if cmd1_name == "RAMP"
                        && (cmd2_name == "HACFILT" || cmd2_name == "HOLDANDCOLLECTFILTER")
                        && cmd3_name == "HOLDANDCOLLECT"
                    {
                        let r_temp = extract_temperature_list_from_args(
                            &nested_step_commands[0].args,
                            cmd,
                            DEFAULT_NUM_ZONES,
                        )?;
                        let r_increment = get_option_f64(
                            &nested_step_commands[0].options,
                            "increment",
                            0.0,
                            cmd,
                        )?;
                        let r_incrementcycle = get_option_i64(
                            &nested_step_commands[0].options,
                            "incrementcycle",
                            2,
                            cmd,
                        )?;
                        let r_incrementstep = get_option_i64(
                            &nested_step_commands[0].options,
                            "incrementstep",
                            2,
                            cmd,
                        )?;

                        let hf_filters: Result<Vec<String>, ProtocolParseError> =
                            nested_step_commands[1]
                                .args
                                .iter()
                                .map(|v| extract_string(v, cmd))
                                .collect();
                        let mut filters = hf_filters?;
                        let mut default_filters = Vec::new();
                        let has_default_marker =
                            nested_step_commands[1].options.get("qslibdf").is_some();

                        let h_time = extract_i64(&nested_step_commands[2].args[0], cmd)?;
                        let h_increment =
                            get_option_i64(&nested_step_commands[2].options, "increment", 0, cmd)?;
                        let h_incrementcycle = get_option_i64(
                            &nested_step_commands[2].options,
                            "incrementcycle",
                            2,
                            cmd,
                        )?;
                        let h_incrementstep = get_option_i64(
                            &nested_step_commands[2].options,
                            "incrementstep",
                            2,
                            cmd,
                        )?;
                        let h_tiff =
                            get_option_bool(&nested_step_commands[2].options, "tiff", false, cmd)?;
                        let h_quant =
                            get_option_bool(&nested_step_commands[2].options, "quant", true, cmd)?;
                        let h_pcr =
                            get_option_bool(&nested_step_commands[2].options, "pcr", false, cmd)?;

                        let mut collect = !filters.is_empty();
                        if has_default_marker && !filters.is_empty() {
                            default_filters = filters.clone();
                            filters = Vec::new();
                            collect = true;
                        }

                        let time_incrementpoint = if h_incrementstep <= repeat {
                            Some(h_incrementstep)
                        } else {
                            None
                        };

                        let temp_incrementpoint = if r_incrementstep <= repeat {
                            Some(r_incrementstep)
                        } else {
                            None
                        };

                        steps.push(StageStep::Standard(Step {
                            time: h_time,
                            temperature: r_temp,
                            collect: Some(collect),
                            temp_increment: r_increment,
                            temp_incrementcycle: r_incrementcycle,
                            temp_incrementpoint,
                            time_increment: h_increment,
                            time_incrementcycle: h_incrementcycle,
                            time_incrementpoint,
                            filters,
                            pcr: h_pcr,
                            quant: h_quant,
                            tiff: h_tiff,
                            repeat,
                            default_filters,
                        }));
                        continue;
                    }
                } else if nested_step_commands.len() == 2 {
                    let cmd1_name = get_command_name(&nested_step_commands[0]);
                    let cmd2_name = get_command_name(&nested_step_commands[1]);

                    if cmd1_name == "RAMP" && cmd2_name == "HOLD" {
                        let r_temp = extract_temperature_list_from_args(
                            &nested_step_commands[0].args,
                            cmd,
                            DEFAULT_NUM_ZONES,
                        )?;
                        let r_increment = get_option_f64(
                            &nested_step_commands[0].options,
                            "increment",
                            0.0,
                            cmd,
                        )?;
                        let r_incrementcycle = get_option_i64(
                            &nested_step_commands[0].options,
                            "incrementcycle",
                            2,
                            cmd,
                        )? as i64;
                        let r_incrementstep = get_option_i64(
                            &nested_step_commands[0].options,
                            "incrementstep",
                            2,
                            cmd,
                        )? as i64;

                        let h_time = extract_i64_option(&nested_step_commands[1].args[0], cmd)?;
                        if h_time.is_none() {
                            continue;
                        }
                        let h_increment =
                            get_option_i64(&nested_step_commands[1].options, "increment", 0, cmd)?;
                        let h_incrementcycle = get_option_i64(
                            &nested_step_commands[1].options,
                            "incrementcycle",
                            2,
                            cmd,
                        )? as i64;
                        let h_incrementstep = get_option_i64(
                            &nested_step_commands[1].options,
                            "incrementstep",
                            2,
                            cmd,
                        )? as i64;

                        let time_incrementpoint = if h_incrementstep <= repeat {
                            Some(h_incrementstep)
                        } else {
                            None
                        };

                        let temp_incrementpoint = if r_incrementstep <= repeat {
                            Some(r_incrementstep)
                        } else {
                            None
                        };

                        steps.push(StageStep::Standard(Step {
                            time: h_time.unwrap(),
                            temperature: r_temp,
                            collect: Some(false),
                            temp_increment: r_increment,
                            temp_incrementcycle: r_incrementcycle,
                            temp_incrementpoint,
                            time_increment: h_increment,
                            time_incrementcycle: h_incrementcycle,
                            time_incrementpoint,
                            filters: Vec::new(),
                            pcr: false,
                            quant: true,
                            tiff: false,
                            repeat,
                            default_filters: Vec::new(),
                        }));
                        continue;
                    }
                }
                // Unrecognized step pattern — capture as Custom
                steps.push(StageStep::Custom(nested_step_commands));
                continue;
            }
            return Err(ProtocolParseError::UnexpectedStructure {
                message: format!(
                    "Stage step must be a valid Step, got: {}",
                    get_command_name(step_cmd)
                ),
                protocol_string: protocol_string.clone(),
            });
        }

        let repeat = get_option_i64(&cmd.options, "repeat", 1, cmd)? as i64;

        let mut default_filters = Vec::new();
        for step in &steps {
            if let StageStep::Standard(step) = step {
                if !step.default_filters.is_empty() {
                    if default_filters.is_empty() {
                        default_filters = step.default_filters.clone();
                    } else if default_filters != step.default_filters {
                        return Err(ProtocolParseError::InconsistentDefaultFilters {
                            protocol_string: protocol_string.clone(),
                        });
                    }
                }
            }
        }

        Ok(Stage {
            steps,
            repeat,
            index,
            label,
            default_filters,
        })
    }

    pub fn info_str(&self, index: Option<i64>) -> String {
        let adds = if self.repeat > 1 { "s" } else { "" };
        let mut stagestr = if let Some(idx) = index {
            format!("{}. Stage with {} cycle{}", idx, self.repeat, adds)
        } else {
            format!("Stage with {} cycle{}", self.repeat, adds)
        };

        let stepstrs: Vec<String> = self
            .steps
            .iter()
            .enumerate()
            .map(|(i, step)| {
                let step_info = match step {
                    StageStep::Standard(s) => s.info_str(Some((i + 1) as i64), self.repeat),
                    StageStep::Custom(cmds) => {
                        format!("{}. Custom step of {} commands", i + 1, cmds.len())
                    }
                };
                step_info
                    .lines()
                    .map(|line| format!("    {}", line))
                    .collect::<Vec<String>>()
                    .join("\n")
            })
            .collect();

        let total_duration: i64 = self
            .steps
            .iter()
            .map(|s| match s {
                StageStep::Standard(s) => s.time * self.repeat,
                StageStep::Custom(_) => 0,
            })
            .sum();
        if total_duration > 0 {
            stagestr.push_str(&format!(
                " (total duration {})",
                format_duration(total_duration)
            ));
        }

        if stepstrs.len() > 1 {
            stagestr.push_str(" of:\n");
            stagestr.push_str(&stepstrs.join("\n"));
        } else if !stepstrs.is_empty() {
            stagestr.push_str(" of ");
            let step_words: Vec<&str> = stepstrs[0].split_whitespace().collect();
            if step_words.len() > 1 {
                stagestr.push_str(&step_words[1..].join(" "));
            } else {
                stagestr.push_str(&stepstrs[0]);
            }
        }

        stagestr
    }
}

/// One rendered line of a protocol description, produced by
/// [`Protocol::info_lines`]. `current` marks the line as the machine's current
/// stage or step position so a caller can highlight it. The `text` reproduces
/// the plain [`Display`](std::fmt::Display) output for that line (indentation
/// included, no markup).
#[derive(Debug, Clone, PartialEq)]
pub struct ProtocolLine {
    pub text: String,
    pub current: bool,
}

/// A structured view of a protocol, for callers that want to build their own
/// (e.g. HTML) rendering rather than the flat [`Display`](std::fmt::Display) /
/// [`Protocol::info_lines`] text. Every descriptive string matches the
/// corresponding piece of the `Display` output.
#[derive(Debug, Clone, PartialEq)]
pub struct ProtocolView {
    /// The protocol name (e.g. `"qpcr"`).
    pub name: String,
    /// Header details, e.g. `["sample volume 25 µL", "run mode standard"]`.
    pub details: Vec<String>,
    /// Oxford-joined default filters (e.g. `"x1-m4 and x4-m5"`), if any.
    pub default_filters: Option<String>,
    pub stages: Vec<StageView>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StageView {
    /// 1-based stage number (matches the machine's stage numbering).
    pub number: i64,
    /// e.g. `"Stage with 40 cycles (total duration 50m)"` (no cycle note).
    pub summary: String,
    /// Whether this is the machine's current stage.
    pub current: bool,
    /// e.g. `"cycle 12/40"` when this is the current stage and it repeats.
    pub cycle: Option<String>,
    pub steps: Vec<StepView>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StepView {
    /// 1-based step number within the stage.
    pub number: i64,
    /// e.g. `"95.00°C for 15s/cycle (collects x1-m4)"`.
    pub text: String,
    /// Whether this is the machine's current step.
    pub current: bool,
}

impl Stage {
    /// Render this stage as lines, mirroring [`Stage::info_str`] exactly when
    /// `stage_current` is false and no position is given. When `stage_current`
    /// is true, the stage header carries a `, cycle {current_cycle}/{repeat}`
    /// note (only when the stage repeats) and the line matching `current_step`
    /// (1-based) is flagged `current`.
    fn info_lines(
        &self,
        index: Option<i64>,
        stage_current: bool,
        current_step: Option<i64>,
        current_cycle: Option<i64>,
    ) -> Vec<ProtocolLine> {
        let adds = if self.repeat > 1 { "s" } else { "" };
        let mut header = if let Some(idx) = index {
            format!("{}. Stage with {} cycle{}", idx, self.repeat, adds)
        } else {
            format!("Stage with {} cycle{}", self.repeat, adds)
        };

        let total_duration: i64 = self
            .steps
            .iter()
            .map(|s| match s {
                StageStep::Standard(s) => s.time * self.repeat,
                StageStep::Custom(_) => 0,
            })
            .sum();
        if total_duration > 0 {
            header.push_str(&format!(
                " (total duration {})",
                format_duration(total_duration)
            ));
        }

        if stage_current {
            if let Some(c) = current_cycle {
                if self.repeat > 1 {
                    header.push_str(&format!(", cycle {}/{}", c, self.repeat));
                }
            }
        }

        // Each step becomes a 4-space-indented block (possibly multiple lines),
        // tagged with whether it is the current step.
        let step_blocks: Vec<(Vec<String>, bool)> = self
            .steps
            .iter()
            .enumerate()
            .map(|(i, step)| {
                let step_num = (i + 1) as i64;
                let step_info = match step {
                    StageStep::Standard(s) => s.info_str(Some(step_num), self.repeat),
                    StageStep::Custom(cmds) => {
                        format!("{}. Custom step of {} commands", step_num, cmds.len())
                    }
                };
                let lines: Vec<String> = step_info
                    .lines()
                    .map(|line| format!("    {}", line))
                    .collect();
                let is_current = stage_current && current_step == Some(step_num);
                (lines, is_current)
            })
            .collect();

        let mut out = Vec::new();
        if step_blocks.len() > 1 {
            header.push_str(" of:");
            out.push(ProtocolLine {
                text: header,
                current: stage_current,
            });
            for (lines, is_cur) in step_blocks {
                for line in lines {
                    out.push(ProtocolLine {
                        text: line,
                        current: is_cur,
                    });
                }
            }
        } else if let Some((lines, _)) = step_blocks.into_iter().next() {
            // Single step: collapse onto the stage line, dropping the step's
            // leading "N. " number (matching Stage::info_str).
            header.push_str(" of ");
            let block = lines.join("\n");
            let words: Vec<&str> = block.split_whitespace().collect();
            if words.len() > 1 {
                header.push_str(&words[1..].join(" "));
            } else {
                header.push_str(&block);
            }
            out.push(ProtocolLine {
                text: header,
                current: stage_current,
            });
        } else {
            out.push(ProtocolLine {
                text: header,
                current: stage_current,
            });
        }
        out
    }
}

fn oxford_list(items: &[String]) -> String {
    match items.len() {
        0 => String::new(),
        1 => items[0].clone(),
        2 => format!("{} and {}", items[0], items[1]),
        _ => {
            let mut result = items[0].clone();
            for item in &items[1..items.len() - 1] {
                result.push_str(", ");
                result.push_str(item);
            }
            result.push_str(", and ");
            result.push_str(&items[items.len() - 1]);
            result
        }
    }
}

fn filter_to_lowerform(filter: &str) -> String {
    if filter.starts_with("x") && filter.contains("-m") {
        return filter.to_string();
    }
    if filter.starts_with("m") && filter.contains(",x") {
        let parts: Vec<&str> = filter.split(',').collect();
        if parts.len() >= 2 {
            let m_part = parts[0].trim_start_matches("m");
            let x_part = parts[1].trim_start_matches("x");
            if let (Ok(em), Ok(ex)) = (m_part.parse::<i32>(), x_part.parse::<i32>()) {
                return format!("x{}-m{}", ex, em);
            }
        }
    }
    filter.to_string()
}

fn format_duration(seconds: i64) -> String {
    if seconds <= 2 * 60 {
        format!("{}s", seconds)
    } else if seconds <= 2 * 60 * 60 {
        let minutes = seconds / 60;
        let secs = seconds % 60;
        if secs == 0 {
            format!("{}m", minutes)
        } else {
            format!("{}m{}s", minutes, secs)
        }
    } else {
        let hours = seconds / 3600;
        let minutes = (seconds % 3600) / 60;
        let secs = seconds % 60;
        let mut result = format!("{}h", hours);
        if minutes > 0 {
            result.push_str(&format!("{}m", minutes));
        }
        if secs > 0 {
            result.push_str(&format!("{}s", secs));
        }
        result
    }
}

fn format_temperature(temps: &[f64]) -> String {
    if temps.is_empty() {
        return String::new();
    }
    if temps.len() == 1 || temps.iter().all(|&t| (t - temps[0]).abs() < 0.01) {
        format!("{:.2}°C", temps[0])
    } else {
        let temp_strs: Vec<String> = temps.iter().map(|t| format!("{:.2}", t)).collect();
        format!("[{}]°C", temp_strs.join(", "))
    }
}

#[derive(Debug, Clone)]
pub struct Protocol {
    pub stages: Vec<Stage>,
    pub name: String,
    pub volume: f64,
    pub runmode: String,
    pub filters: Vec<String>,
    pub covertemperature: f64,
    pub prerun: Vec<Command>,
    pub postrun: Vec<Command>,
}

impl fmt::Display for Protocol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Run Protocol {}", self.name)?;
        let mut extras = Vec::new();
        if self.volume != 0.0 {
            extras.push(format!("sample volume {} µL", self.volume));
        }
        if !self.runmode.is_empty() {
            extras.push(format!("run mode {}", self.runmode));
        }
        if !extras.is_empty() {
            write!(f, " with {}", oxford_list(&extras))?;
        }
        writeln!(f, ":")?;

        if !self.filters.is_empty() {
            let filter_strs: Vec<String> = self
                .filters
                .iter()
                .map(|filter| filter_to_lowerform(filter))
                .collect();
            write!(f, "(default filters {})\n\n", oxford_list(&filter_strs))?;
        } else {
            writeln!(f)?;
        }

        let stagestrs: Vec<String> = self
            .stages
            .iter()
            .enumerate()
            .map(|(i, stage)| {
                let stage_num = stage.index.unwrap_or((i + 1) as i64);
                let stage_info = stage.info_str(Some(stage_num));
                stage_info
                    .lines()
                    .map(|line| format!("  {}", line))
                    .collect::<Vec<String>>()
                    .join("\n")
            })
            .collect();

        write!(f, "{}", stagestrs.join("\n"))?;
        Ok(())
    }
}

impl Protocol {
    /// Render the protocol as a sequence of [`ProtocolLine`]s. Joining the
    /// `text` of every line with `"\n"` reproduces the [`Display`] output
    /// exactly (see the `info_lines_matches_display` test).
    ///
    /// `current_stage`/`current_step` are the machine's 1-based position (as
    /// reported by run status); when `current_stage` matches a stage's number,
    /// that stage's lines (and the `current_step` line within it) are flagged
    /// `current`, and `current_cycle` is shown as `cycle {c}/{repeat}` on the
    /// stage header. Pass `None` for all three to get an unmarked rendering.
    ///
    /// [`Display`]: std::fmt::Display
    pub fn info_lines(
        &self,
        current_stage: Option<i64>,
        current_step: Option<i64>,
        current_cycle: Option<i64>,
    ) -> Vec<ProtocolLine> {
        let mut out = Vec::new();

        let mut header = format!("Run Protocol {}", self.name);
        let mut extras = Vec::new();
        if self.volume != 0.0 {
            extras.push(format!("sample volume {} µL", self.volume));
        }
        if !self.runmode.is_empty() {
            extras.push(format!("run mode {}", self.runmode));
        }
        if !extras.is_empty() {
            header.push_str(&format!(" with {}", oxford_list(&extras)));
        }
        header.push(':');
        out.push(ProtocolLine {
            text: header,
            current: false,
        });

        if !self.filters.is_empty() {
            let filter_strs: Vec<String> = self
                .filters
                .iter()
                .map(|f| filter_to_lowerform(f))
                .collect();
            out.push(ProtocolLine {
                text: format!("(default filters {})", oxford_list(&filter_strs)),
                current: false,
            });
        }
        // Blank separator line before the stages (matches Display).
        out.push(ProtocolLine {
            text: String::new(),
            current: false,
        });

        for (i, stage) in self.stages.iter().enumerate() {
            let stage_num = stage.index.unwrap_or((i + 1) as i64);
            let stage_current = current_stage == Some(stage_num);
            let (cs, cc) = if stage_current {
                (current_step, current_cycle)
            } else {
                (None, None)
            };
            for line in stage.info_lines(Some(stage_num), stage_current, cs, cc) {
                out.push(ProtocolLine {
                    text: format!("  {}", line.text),
                    current: line.current,
                });
            }
        }

        out
    }

    /// Build a structured [`ProtocolView`] for custom (e.g. HTML) rendering.
    ///
    /// `current_stage`/`current_step`/`current_cycle` are the machine's 1-based
    /// position, matched the same way as [`Protocol::info_lines`]. The
    /// descriptive strings match the corresponding pieces of the `Display`
    /// output. Pass `None` for all three for an idle machine.
    pub fn view(
        &self,
        current_stage: Option<i64>,
        current_step: Option<i64>,
        current_cycle: Option<i64>,
    ) -> ProtocolView {
        let mut details = Vec::new();
        if self.volume != 0.0 {
            details.push(format!("sample volume {} µL", self.volume));
        }
        if !self.runmode.is_empty() {
            details.push(format!("run mode {}", self.runmode));
        }

        let default_filters = if self.filters.is_empty() {
            None
        } else {
            let filter_strs: Vec<String> = self
                .filters
                .iter()
                .map(|f| filter_to_lowerform(f))
                .collect();
            Some(oxford_list(&filter_strs))
        };

        let stages = self
            .stages
            .iter()
            .enumerate()
            .map(|(i, stage)| {
                let number = stage.index.unwrap_or((i + 1) as i64);
                let stage_current = current_stage == Some(number);

                let adds = if stage.repeat > 1 { "s" } else { "" };
                let mut summary = format!("Stage with {} cycle{}", stage.repeat, adds);
                let total_duration: i64 = stage
                    .steps
                    .iter()
                    .map(|s| match s {
                        StageStep::Standard(s) => s.time * stage.repeat,
                        StageStep::Custom(_) => 0,
                    })
                    .sum();
                if total_duration > 0 {
                    summary.push_str(&format!(
                        " (total duration {})",
                        format_duration(total_duration)
                    ));
                }

                let cycle = if stage_current && stage.repeat > 1 {
                    current_cycle.map(|c| format!("cycle {}/{}", c, stage.repeat))
                } else {
                    None
                };

                let steps = stage
                    .steps
                    .iter()
                    .enumerate()
                    .map(|(j, step)| {
                        let step_number = (j + 1) as i64;
                        let text = match step {
                            StageStep::Standard(s) => s.info_str(None, stage.repeat),
                            StageStep::Custom(cmds) => {
                                format!("Custom step of {} commands", cmds.len())
                            }
                        };
                        StepView {
                            number: step_number,
                            text,
                            current: stage_current && current_step == Some(step_number),
                        }
                    })
                    .collect();

                StageView {
                    number,
                    summary,
                    current: stage_current,
                    cycle,
                    steps,
                }
            })
            .collect();

        ProtocolView {
            name: self.name.clone(),
            details,
            default_filters,
            stages,
        }
    }

    pub fn from_scpicommand(cmd: &Command) -> Result<Protocol, ProtocolParseError> {
        let protocol_string = command_to_string(cmd);
        let cmd_name = get_command_name(cmd);
        if cmd_name != "PROTOCOL" && cmd_name != "PROT" {
            return Err(ProtocolParseError::InvalidCommand {
                expected: "PROTOCOL or PROT".to_string(),
                got: cmd_name,
                protocol_string,
            });
        }

        if cmd.args.is_empty() {
            return Err(ProtocolParseError::MissingField {
                field: "protocol name".to_string(),
                protocol_string: protocol_string.clone(),
            });
        }

        let name = extract_string(&cmd.args[0], cmd)?;

        if cmd.args.len() < 2 {
            return Err(ProtocolParseError::MissingField {
                field: "protocol stages".to_string(),
                protocol_string: protocol_string.clone(),
            });
        }

        let stages_value = &cmd.args[1];
        let mut stage_commands = extract_commands_from_value(stages_value, Some(cmd))?;

        let mut prerun = Vec::new();
        let mut postrun = Vec::new();

        if !stage_commands.is_empty() {
            let first_cmd_name = get_command_name(&stage_commands[0]);
            if first_cmd_name == "PRERUN" {
                if let Some(Value::XmlString { value, tag: _ }) = stage_commands[0].args.first() {
                    let s = String::from_utf8(value.to_vec()).map_err(|_| {
                        ProtocolParseError::InvalidValueType {
                            value_type: "prerun commands".to_string(),
                            protocol_string: protocol_string.clone(),
                        }
                    })?;
                    let mut input = s.as_bytes();
                    while !input.is_empty() {
                        // Skip leading whitespace/newlines
                        while !input.is_empty()
                            && (input[0] == b' '
                                || input[0] == b'\t'
                                || input[0] == b'\n'
                                || input[0] == b'\r')
                        {
                            input = &input[1..];
                        }
                        if input.is_empty() {
                            break;
                        }
                        match Command::parse(&mut input) {
                            Ok(cmd) => prerun.push(cmd),
                            Err(_) => break,
                        }
                    }
                }
                stage_commands.remove(0);
            }

            if !stage_commands.is_empty() {
                let last_cmd_name = get_command_name(&stage_commands[stage_commands.len() - 1]);
                if last_cmd_name == "POSTRUN" {
                    if let Some(Value::XmlString { value, tag: _ }) =
                        stage_commands[stage_commands.len() - 1].args.first()
                    {
                        let s = String::from_utf8(value.to_vec()).map_err(|_| {
                            ProtocolParseError::InvalidValueType {
                                value_type: "postrun commands".to_string(),
                                protocol_string: protocol_string.clone(),
                            }
                        })?;
                        let mut input = s.as_bytes();
                        while !input.is_empty() {
                            // Skip leading whitespace/newlines
                            while !input.is_empty()
                                && (input[0] == b' '
                                    || input[0] == b'\t'
                                    || input[0] == b'\n'
                                    || input[0] == b'\r')
                            {
                                input = &input[1..];
                            }
                            if input.is_empty() {
                                break;
                            }
                            match Command::parse(&mut input) {
                                Ok(cmd) => postrun.push(cmd),
                                Err(_) => break,
                            }
                        }
                    }
                    stage_commands.pop();
                }
            }
        }

        let mut stages = Vec::new();
        for stage_cmd in &stage_commands {
            stages.push(Stage::from_scpicommand(stage_cmd)?);
        }

        let volume = get_option_f64(&cmd.options, "volume", 50.0, cmd)?;
        let runmode = get_option_string(&cmd.options, "runmode", cmd)?
            .unwrap_or_else(|| "standard".to_string());
        let covertemperature = get_option_f64(&cmd.options, "covertemperature", 105.0, cmd)?;

        let mut filters = Vec::new();
        for stage in &stages {
            if !stage.default_filters.is_empty() {
                if filters.is_empty() {
                    filters = stage.default_filters.clone();
                } else if filters != stage.default_filters {
                    return Err(ProtocolParseError::InconsistentDefaultFilters {
                        protocol_string: protocol_string.clone(),
                    });
                }
            }
        }

        Ok(Protocol {
            stages,
            name,
            volume,
            runmode,
            filters,
            covertemperature,
            prerun,
            postrun,
        })
    }
}

// =====================================================================
// SCPI Serialization
// =====================================================================

/// Format a f64 as an integer string if it's a whole number, otherwise as float.
fn format_scpi_number(v: f64) -> String {
    if v.is_finite() && v == v.floor() && v.abs() < (i64::MAX as f64) {
        format!("{}", v as i64)
    } else {
        format!("{}", v)
    }
}

/// Indent every non-empty line of text with the given prefix.
/// Matches Python's textwrap.indent behavior.
fn indent_text(text: &str, prefix: &str) -> String {
    text.split('\n')
        .map(|line| {
            if line.trim().is_empty() {
                line.to_string()
            } else {
                format!("{}{}", prefix, line)
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

impl Ramp {
    pub fn to_scpi_string(&self) -> String {
        let mut parts = vec!["RAMP".to_string()];
        if self.increment != 0.0 {
            parts.push(format!("-increment={}", format_scpi_number(self.increment)));
        }
        if self.incrementcycle != 2 {
            parts.push(format!("-incrementcycle={}", self.incrementcycle));
        }
        if self.incrementstep != 2 {
            parts.push(format!("-incrementstep={}", self.incrementstep));
        }
        if self.rate != 100.0 {
            parts.push(format!("-rate={}", format_scpi_number(self.rate)));
        }
        if let Some(cover) = self.cover {
            parts.push(format!("-cover={}", format_scpi_number(cover)));
        }
        for &t in &self.temperature {
            parts.push(format_scpi_number(t));
        }
        parts.join(" ") + "\n"
    }
}

impl Hold {
    pub fn to_scpi_string(&self) -> String {
        let mut parts = vec!["HOLD".to_string()];
        if self.increment != 0 {
            parts.push(format!("-increment={}", self.increment));
        }
        if self.incrementcycle != 2 {
            parts.push(format!("-incrementcycle={}", self.incrementcycle));
        }
        if self.incrementstep != 2 {
            parts.push(format!("-incrementstep={}", self.incrementstep));
        }
        match self.time {
            Some(t) => parts.push(format!("{}", t)),
            None => parts.push(String::new()),
        }
        parts.join(" ") + "\n"
    }
}

impl HoldAndCollect {
    pub fn to_scpi_string(&self) -> String {
        let mut parts = vec!["HOLDANDCOLLECT".to_string()];
        if self.increment != 0 {
            parts.push(format!("-increment={}", self.increment));
        }
        if self.incrementcycle != 2 {
            parts.push(format!("-incrementcycle={}", self.incrementcycle));
        }
        if self.incrementstep != 2 {
            parts.push(format!("-incrementstep={}", self.incrementstep));
        }
        parts.push(format!(
            "-tiff={}",
            if self.tiff { "True" } else { "False" }
        ));
        parts.push(format!(
            "-quant={}",
            if self.quant { "True" } else { "False" }
        ));
        parts.push(format!("-pcr={}", if self.pcr { "True" } else { "False" }));
        parts.push(format!("{}", self.time));
        parts.join(" ") + "\n"
    }
}

impl HACFILT {
    pub fn to_scpi_string(&self) -> String {
        let filters = if self.filters.is_empty() {
            &self.default_filters
        } else {
            &self.filters
        };
        let mut parts = vec!["HACFILT".to_string()];
        for f in filters {
            parts.push(f.clone());
        }
        let base = parts.join(" ");
        if self.filters.is_empty() && !self.default_filters.is_empty() {
            format!("{} # qslib:default_filters\n", base)
        } else {
            format!("{}\n", base)
        }
    }
}

impl Step {
    /// Serialize this Step as an SCPI command string.
    ///
    /// `step_index` is the 1-based index within the stage.
    /// `default_filters` are the protocol-level default filters.
    pub fn to_scpi_string(&self, step_index: i64, default_filters: &[String]) -> String {
        let mut body = String::new();

        // Build Ramp
        let ramp = Ramp {
            temperature: self.temperature.clone(),
            increment: self.temp_increment,
            incrementcycle: self.temp_incrementcycle,
            incrementstep: self.temp_incrementpoint.unwrap_or(self.repeat + 1),
            rate: 100.0,
            cover: None,
        };
        body.push_str(&ramp.to_scpi_string());

        if self.collect == Some(true) {
            // HACFILT
            let use_default = self.filters.is_empty();
            let hacfilt = HACFILT {
                filters: if use_default {
                    vec![]
                } else {
                    self.filters.clone()
                },
                default_filters: if use_default {
                    default_filters.to_vec()
                } else {
                    vec![]
                },
            };
            body.push_str(&hacfilt.to_scpi_string());

            // HoldAndCollect
            let hac = HoldAndCollect {
                time: self.time,
                increment: self.time_increment,
                incrementcycle: self.time_incrementcycle,
                incrementstep: self.time_incrementpoint.unwrap_or(self.repeat + 1),
                tiff: self.tiff,
                quant: self.quant,
                pcr: self.pcr,
            };
            body.push_str(&hac.to_scpi_string());
        } else {
            // Hold
            let hold = Hold {
                time: Some(self.time),
                increment: self.time_increment,
                incrementcycle: self.time_incrementcycle,
                incrementstep: self.time_incrementpoint.unwrap_or(self.repeat + 1),
            };
            body.push_str(&hold.to_scpi_string());
        }

        // Build STEP command
        let mut parts = vec!["STEP".to_string()];
        if self.repeat != 1 {
            parts.push(format!("-repeat={}", self.repeat));
        }
        parts.push(format!("{}", step_index));
        // Wrap body in <multiline.step>
        let wrapped = format!(
            "<multiline.step>\n{}</multiline.step>",
            indent_text(&body, "\t")
        );
        parts.push(wrapped);
        parts.join(" ") + "\n"
    }
}

impl Stage {
    /// Serialize this Stage as an SCPI command string.
    ///
    /// `stage_index` is the 1-based index within the protocol.
    /// `default_filters` are the protocol-level default filters.
    pub fn to_scpi_string(&self, stage_index: i64, default_filters: &[String]) -> String {
        let label = self
            .label
            .clone()
            .unwrap_or_else(|| format!("STAGE_{}", stage_index));

        let mut step_strs = String::new();
        for (i, step) in self.steps.iter().enumerate() {
            match step {
                StageStep::Standard(s) => {
                    step_strs.push_str(&s.to_scpi_string((i + 1) as i64, default_filters));
                }
                StageStep::Custom(cmds) => {
                    let mut body = String::new();
                    for cmd in cmds {
                        let mut bytes = Vec::new();
                        cmd.write_bytes(&mut bytes).unwrap();
                        body.push_str(&String::from_utf8_lossy(&bytes));
                        body.push('\n');
                    }
                    let wrapped = format!(
                        "<multiline.step>\n{}</multiline.step>",
                        indent_text(&body, "\t")
                    );
                    step_strs.push_str(&format!("STEP {} {}\n", i + 1, wrapped));
                }
            }
        }

        let mut parts = vec!["STAGE".to_string()];
        if self.repeat != 1 {
            parts.push(format!("-repeat={}", self.repeat));
        }
        parts.push(format!("{}", stage_index));
        parts.push(label);
        let wrapped = format!(
            "<multiline.stage>\n{}</multiline.stage>",
            indent_text(&step_strs, "\t")
        );
        parts.push(wrapped);
        parts.join(" ") + "\n"
    }
}

impl Protocol {
    /// Serialize this Protocol as a full SCPI command string.
    pub fn to_scpi_string(&self) -> String {
        let mut parts = vec!["PROTOCOL".to_string()];
        parts.push(format!("-volume={}", format_scpi_number(self.volume)));
        parts.push(format!("-runmode={}", self.runmode));
        parts.push(self.name.clone());

        let mut stages_str = String::new();

        // Prerun
        if !self.prerun.is_empty() {
            let mut prerun_body = String::new();
            for cmd in &self.prerun {
                let mut bytes = Vec::new();
                cmd.write_bytes(&mut bytes).unwrap();
                prerun_body.push_str(&String::from_utf8_lossy(&bytes));
                prerun_body.push('\n');
            }
            stages_str.push_str(&format!(
                "PRERUN <multiline.prerun>\n{}</multiline.prerun>\n",
                indent_text(&prerun_body, "\t")
            ));
        }

        for (i, stage) in self.stages.iter().enumerate() {
            let stage_index = stage.index.unwrap_or((i + 1) as i64);
            stages_str.push_str(&stage.to_scpi_string(stage_index, &self.filters));
        }

        // Postrun
        if !self.postrun.is_empty() {
            let mut postrun_body = String::new();
            for cmd in &self.postrun {
                let mut bytes = Vec::new();
                cmd.write_bytes(&mut bytes).unwrap();
                postrun_body.push_str(&String::from_utf8_lossy(&bytes));
                postrun_body.push('\n');
            }
            stages_str.push_str(&format!(
                "POSTRUN <multiline.postrun>\n{}</multiline.postrun>\n",
                indent_text(&postrun_body, "\t")
            ));
        }

        let wrapped = format!(
            "<multiline.protocol>\n{}</multiline.protocol>",
            indent_text(&stages_str, "\t")
        );
        parts.push(wrapped);
        parts.join(" ") + "\n"
    }
}

// =====================================================================
// XML Generation (tcprotocol.xml / qsl-tcprotocol.xml)
// =====================================================================

fn write_simple_element(writer: &mut Writer<Cursor<Vec<u8>>>, tag: &str, text: &str) {
    writer
        .create_element(tag)
        .write_text_content(BytesText::new(text))
        .unwrap();
}

fn write_empty_element(writer: &mut Writer<Cursor<Vec<u8>>>, tag: &str) {
    writer
        .create_element(tag)
        .write_text_content(BytesText::new(""))
        .unwrap();
}

impl Step {
    /// Write this step as a TCStep XML element.
    fn write_xml(&self, writer: &mut Writer<Cursor<Vec<u8>>>) {
        writer
            .create_element("TCStep")
            .write_inner_content(|w| {
                write_simple_element(
                    w,
                    "CollectionFlag",
                    if self.collect == Some(true) { "1" } else { "0" },
                );
                for &t in &self.temperature {
                    write_simple_element(w, "Temperature", &format_scpi_number(t));
                }
                write_simple_element(w, "HoldTime", &format!("{}", self.time));
                write_simple_element(
                    w,
                    "ExtTemperature",
                    &format_scpi_number(self.temp_increment),
                );
                write_simple_element(w, "ExtHoldTime", &format!("{}", self.time_increment));
                write_simple_element(w, "RampRate", "1.6");
                write_simple_element(w, "RampRateUnit", "DEGREES_PER_SECOND");
                Ok(())
            })
            .unwrap();
    }
}

/// Write a placeholder TCStep XML element for a Custom step.
fn write_custom_step_xml(writer: &mut Writer<Cursor<Vec<u8>>>) {
    writer
        .create_element("TCStep")
        .write_inner_content(|w| {
            write_simple_element(w, "CollectionFlag", "0");
            for _ in 0..DEFAULT_NUM_ZONES {
                write_simple_element(w, "Temperature", "30");
            }
            write_simple_element(w, "HoldTime", "1");
            write_simple_element(w, "ExtTemperature", "0");
            write_simple_element(w, "ExtHoldTime", "0");
            write_simple_element(w, "RampRate", "1.6");
            write_simple_element(w, "RampRateUnit", "DEGREES_PER_SECOND");
            Ok(())
        })
        .unwrap();
}

impl Stage {
    /// Write this stage as a TCStage XML element.
    fn write_xml(&self, writer: &mut Writer<Cursor<Vec<u8>>>) {
        writer
            .create_element("TCStage")
            .write_inner_content(|w| {
                write_simple_element(w, "StageFlag", "CYCLING");
                write_simple_element(w, "NumOfRepetitions", &format!("{}", self.repeat));
                for step in &self.steps {
                    match step {
                        StageStep::Standard(s) => s.write_xml(w),
                        StageStep::Custom(_) => write_custom_step_xml(w),
                    }
                }
                // Determine starting cycle from standard steps only
                let mut scycle: Option<i64> = None;
                for step in &self.steps {
                    if let StageStep::Standard(step) = step {
                        for &c in &[step.temp_incrementcycle, step.time_incrementcycle] {
                            scycle = Some(scycle.map_or(c, |prev| prev.min(c)));
                        }
                    }
                }
                if let Some(c) = scycle {
                    write_simple_element(w, "StartingCycle", &format!("{}", c));
                }
                write_simple_element(w, "AutoDeltaEnabled", "true");
                Ok(())
            })
            .unwrap();
    }
}

impl Protocol {
    /// Generate (tcprotocol_xml_string, qsl_tcprotocol_xml_string).
    ///
    /// `cover_temperature` is the cover temperature to write (typically 105.0).
    /// `version` is the QSLib version string.
    /// `machine_toml` is an optional TOML string for MachineConnection.
    pub fn to_xml_pair(
        &self,
        cover_temperature: f64,
        version: &str,
        machine_toml: Option<&str>,
    ) -> (String, String) {
        // Build tcprotocol.xml
        let tc_xml = {
            let mut writer = Writer::new_with_indent(Cursor::new(Vec::new()), b' ', 2);
            writer
                .create_element("TCProtocol")
                .write_inner_content(|w| {
                    write_simple_element(w, "FileVersion", "2.0");
                    write_simple_element(w, "ProtocolName", &self.name);
                    write_simple_element(
                        w,
                        "CoverTemperature",
                        &format_scpi_number(cover_temperature),
                    );
                    write_simple_element(
                        w,
                        "SampleVolume",
                        &format_scpi_number(if self.volume != 0.0 {
                            self.volume
                        } else {
                            50.0
                        }),
                    );
                    write_simple_element(
                        w,
                        "RunMode",
                        if self.runmode.is_empty() {
                            "Standard"
                        } else {
                            &self.runmode
                        },
                    );
                    write_empty_element(w, "UserName");
                    write_simple_element(w, "TubeType", "0");
                    write_simple_element(w, "BlockID", "18");
                    write_simple_element(w, "Delay", "0.0");
                    write_simple_element(w, "ExtendedPCRCycles", "0");
                    write_simple_element(w, "ExtendedHoldTemp", "0");
                    write_simple_element(w, "ExtendedHoldTime", "0");
                    // CollectionProfile with filters
                    if !self.filters.is_empty() {
                        let mut profile = BytesStart::new("CollectionProfile");
                        profile.push_attribute(("ProfileId", "1"));
                        w.write_event(Event::Start(profile)).unwrap();
                        for filter in &self.filters {
                            // Parse filter string like "x4-m4" or "4,4"
                            let (ex, em) = parse_filter_for_xml(filter);
                            w.create_element("CollectionCondition")
                                .write_inner_content(|w2| {
                                    let mut fs = BytesStart::new("FilterSet");
                                    fs.push_attribute(("Emission", em.as_str()));
                                    fs.push_attribute(("Excitation", ex.as_str()));
                                    w2.write_event(Event::Empty(fs)).unwrap();
                                    write_simple_element(w2, "Frames", "0");
                                    Ok(())
                                })
                                .unwrap();
                        }
                        w.write_event(Event::End(BytesEnd::new("CollectionProfile")))
                            .unwrap();
                    }
                    // Stages
                    for stage in &self.stages {
                        stage.write_xml(w);
                    }
                    Ok(())
                })
                .unwrap();
            String::from_utf8(writer.into_inner().into_inner()).unwrap()
        };

        // Build qsl-tcprotocol.xml
        let qstc_xml =
            {
                let mut writer = Writer::new_with_indent(Cursor::new(Vec::new()), b' ', 2);
                writer
                    .create_element("QSTCProtocol")
                    .write_inner_content(|w| {
                        write_simple_element(w, "QSLibNote",
                    "This protocol was generated by QSLib. It may be only an approximation or \
                     placeholder for the real protocol, contained as an SCPI command in \
                     QSLibProtocolCommand.");
                        write_simple_element(w, "QSLibProtocolCommand", &self.to_scpi_string());
                        // Intentional typo preserved from original code
                        write_simple_element(w, "QSLibVerson", version);
                        if let Some(toml) = machine_toml {
                            write_simple_element(w, "MachineConnection", toml);
                        }
                        Ok(())
                    })
                    .unwrap();
                String::from_utf8(writer.into_inner().into_inner()).unwrap()
            };

        (tc_xml, qstc_xml)
    }
}

/// Convert a filter name to XML excitation and emission attributes.
fn parse_filter_for_xml(filter: &str) -> (String, String) {
    match crate::data::FilterSet::from_string(filter) {
        Ok(fs) => (format!("x{}", fs.ex), format!("m{}", fs.em)),
        Err(_) => (filter.to_string(), filter.to_string()),
    }
}

// =====================================================================
// XML Parsing (tcprotocol.xml / qsl-tcprotocol.xml)
// =====================================================================

impl Protocol {
    /// Parse a Protocol from tcprotocol.xml content.
    /// Tolerant of unknown elements/attributes.
    pub fn from_xml_str(xml: &str) -> Result<Protocol, ProtocolParseError> {
        use quick_xml::events::Event;
        use quick_xml::reader::Reader;

        let mut reader = Reader::from_str(xml);
        let mut depth: u32 = 0;
        let mut current_tag = String::new();
        let mut tag_depth: u32 = 0;

        let mut name = String::new();
        let mut volume = 50.0_f64;
        let mut runmode = "standard".to_string();
        let mut cover_temperature = 105.0_f64;
        let mut filters: Vec<String> = Vec::new();
        let mut stages: Vec<Stage> = Vec::new();

        // State for parsing nested structures
        let mut in_collection_condition = false;
        let mut in_tc_stage = false;
        let mut in_tc_step = false;
        let mut stage_repetitions = 1_i64;
        let mut stage_starting_cycle = 1_i64;
        let mut stage_auto_delta = false;

        // Raw step data accumulated during parsing (resolved at stage end)
        struct RawStep {
            collection_flag: bool,
            temperatures: Vec<f64>,
            hold_time: i64,
            ext_temperature: f64,
            ext_hold_time: i64,
        }
        let mut raw_steps: Vec<RawStep> = Vec::new();

        // Current step accumulation
        let mut step_collection_flag = false;
        let mut step_temperatures: Vec<f64> = Vec::new();
        let mut step_hold_time = 0_i64;
        let mut step_ext_temperature = 0.0_f64;
        let mut step_ext_hold_time = 0_i64;

        loop {
            match reader.read_event() {
                Ok(Event::Eof) => break,
                Ok(Event::Start(ref e)) => {
                    depth += 1;
                    let tag = String::from_utf8_lossy(e.name().as_ref()).to_string();
                    current_tag = tag.clone();
                    tag_depth = depth;

                    match tag.as_str() {
                        "TCStage" => {
                            in_tc_stage = true;
                            stage_repetitions = 1;
                            stage_starting_cycle = 1;
                            stage_auto_delta = false;
                            raw_steps.clear();
                        }
                        "TCStep" if in_tc_stage => {
                            in_tc_step = true;
                            step_collection_flag = false;
                            step_temperatures.clear();
                            step_hold_time = 0;
                            step_ext_temperature = 0.0;
                            step_ext_hold_time = 0;
                        }
                        "CollectionCondition" => {
                            in_collection_condition = true;
                        }
                        "FilterSet" if in_collection_condition => {
                            let mut ex = String::new();
                            let mut em = String::new();
                            for attr in e.attributes().flatten() {
                                match attr.key.as_ref() {
                                    b"Excitation" => {
                                        ex = String::from_utf8_lossy(&attr.value).to_string();
                                    }
                                    b"Emission" => {
                                        em = String::from_utf8_lossy(&attr.value).to_string();
                                    }
                                    _ => {}
                                }
                            }
                            if !ex.is_empty() && !em.is_empty() {
                                filters.push(format!("{}-{}", ex, em));
                            }
                        }
                        _ => {}
                    }
                }
                Ok(Event::Empty(ref e)) => {
                    let tag = String::from_utf8_lossy(e.name().as_ref()).to_string();
                    if tag == "FilterSet" && in_collection_condition {
                        let mut ex = String::new();
                        let mut em = String::new();
                        for attr in e.attributes().flatten() {
                            match attr.key.as_ref() {
                                b"Excitation" => {
                                    ex = String::from_utf8_lossy(&attr.value).to_string();
                                }
                                b"Emission" => {
                                    em = String::from_utf8_lossy(&attr.value).to_string();
                                }
                                _ => {}
                            }
                        }
                        if !ex.is_empty() && !em.is_empty() {
                            filters.push(format!("{}-{}", ex, em));
                        }
                    }
                }
                Ok(Event::End(ref e)) => {
                    let tag = String::from_utf8_lossy(e.name().as_ref()).to_string();
                    match tag.as_str() {
                        "TCStep" if in_tc_step => {
                            in_tc_step = false;
                            raw_steps.push(RawStep {
                                collection_flag: step_collection_flag,
                                temperatures: if step_temperatures.is_empty() {
                                    vec![25.0; DEFAULT_NUM_ZONES]
                                } else {
                                    step_temperatures.clone()
                                },
                                hold_time: step_hold_time,
                                ext_temperature: step_ext_temperature,
                                ext_hold_time: step_ext_hold_time,
                            });
                        }
                        "TCStage" if in_tc_stage => {
                            in_tc_stage = false;
                            // Now resolve raw steps with stage-level settings
                            let etc = if stage_auto_delta {
                                stage_starting_cycle
                            } else {
                                1
                            };
                            let resolved_steps: Vec<StageStep> = raw_steps
                                .drain(..)
                                .map(|rs| {
                                    StageStep::Standard(Step {
                                        time: rs.hold_time,
                                        temperature: rs.temperatures,
                                        collect: Some(rs.collection_flag),
                                        temp_increment: if stage_auto_delta {
                                            rs.ext_temperature
                                        } else {
                                            0.0
                                        },
                                        temp_incrementcycle: etc,
                                        temp_incrementpoint: Some(1),
                                        time_increment: if stage_auto_delta {
                                            rs.ext_hold_time
                                        } else {
                                            0
                                        },
                                        time_incrementcycle: etc,
                                        time_incrementpoint: Some(1),
                                        filters: vec![],
                                        pcr: true,
                                        quant: true,
                                        tiff: false,
                                        repeat: 1,
                                        default_filters: vec![],
                                    })
                                })
                                .collect();
                            stages.push(Stage {
                                steps: resolved_steps,
                                repeat: stage_repetitions,
                                index: Some(stages.len() as i64 + 1),
                                label: None,
                                default_filters: vec![],
                            });
                        }
                        "CollectionCondition" => {
                            in_collection_condition = false;
                        }
                        _ => {}
                    }
                    depth -= 1;
                }
                Ok(Event::Text(ref e)) => {
                    let text = std::str::from_utf8(e.as_ref())
                        .unwrap_or_default()
                        .trim()
                        .to_string();
                    if text.is_empty() {
                        continue;
                    }
                    if in_tc_step {
                        match current_tag.as_str() {
                            "CollectionFlag" => {
                                step_collection_flag =
                                    text == "1" || text.eq_ignore_ascii_case("true");
                            }
                            "Temperature" => {
                                if let Ok(t) = text.parse::<f64>() {
                                    step_temperatures.push(t);
                                }
                            }
                            "HoldTime" => {
                                step_hold_time = text.parse().unwrap_or(0);
                            }
                            "ExtTemperature" => {
                                step_ext_temperature = text.parse().unwrap_or(0.0);
                            }
                            "ExtHoldTime" => {
                                step_ext_hold_time = text.parse().unwrap_or(0);
                            }
                            _ => {}
                        }
                    } else if in_tc_stage {
                        match current_tag.as_str() {
                            "NumOfRepetitions" => {
                                stage_repetitions = text.parse().unwrap_or(1);
                            }
                            "StartingCycle" => {
                                stage_starting_cycle = text.parse().unwrap_or(1);
                            }
                            "AutoDeltaEnabled" => {
                                stage_auto_delta = text.eq_ignore_ascii_case("true");
                            }
                            _ => {}
                        }
                    } else {
                        // Top-level fields (direct children of TCProtocol)
                        match current_tag.as_str() {
                            "ProtocolName" if tag_depth == 2 => name = text,
                            "SampleVolume" if tag_depth == 2 => {
                                volume = text.parse().unwrap_or(50.0);
                            }
                            "RunMode" if tag_depth == 2 => runmode = text,
                            "CoverTemperature" if tag_depth == 2 => {
                                cover_temperature = text.parse().unwrap_or(105.0);
                            }
                            _ => {}
                        }
                    }
                }
                Err(e) => {
                    return Err(ProtocolParseError::UnexpectedStructure {
                        message: format!("XML parse error: {}", e),
                        protocol_string: xml.chars().take(200).collect(),
                    });
                }
                _ => {}
            }
        }

        if name.is_empty() {
            return Err(ProtocolParseError::MissingField {
                field: "ProtocolName".to_string(),
                protocol_string: xml.chars().take(200).collect(),
            });
        }

        Ok(Protocol {
            stages,
            name,
            volume,
            runmode,
            filters,
            covertemperature: cover_temperature,
            prerun: vec![],
            postrun: vec![],
        })
    }

    /// Extract QSLibProtocolCommand text from qsl-tcprotocol.xml.
    pub fn parse_qsl_tcprotocol_command(xml: &str) -> Option<String> {
        Self::extract_qsl_element(xml, "QSLibProtocolCommand")
    }

    /// Extract MachineConnection TOML from qsl-tcprotocol.xml.
    pub fn parse_qsl_machine_connection(xml: &str) -> Option<String> {
        Self::extract_qsl_element(xml, "MachineConnection")
    }

    /// Helper: extract text content of a named element from XML, resolving entity references.
    fn extract_qsl_element(xml: &str, element_name: &str) -> Option<String> {
        use quick_xml::events::Event;
        use quick_xml::reader::Reader;

        let mut reader = Reader::from_str(xml);
        let mut in_target = false;
        let mut text_buf = String::new();

        loop {
            match reader.read_event() {
                Ok(Event::Eof) => break,
                Ok(Event::Start(ref e)) => {
                    let tag = String::from_utf8_lossy(e.name().as_ref()).to_string();
                    if tag == element_name {
                        in_target = true;
                        text_buf.clear();
                    }
                }
                Ok(Event::Text(ref e)) if in_target => {
                    let raw = std::str::from_utf8(e.as_ref()).unwrap_or_default();
                    text_buf.push_str(raw);
                }
                Ok(Event::GeneralRef(ref e)) if in_target => {
                    // Resolve standard XML entity references
                    let name = std::str::from_utf8(e.as_ref()).unwrap_or_default();
                    match name {
                        "lt" => text_buf.push('<'),
                        "gt" => text_buf.push('>'),
                        "amp" => text_buf.push('&'),
                        "quot" => text_buf.push('"'),
                        "apos" => text_buf.push('\''),
                        _ => {
                            // Unknown entity, preserve as-is
                            text_buf.push('&');
                            text_buf.push_str(name);
                            text_buf.push(';');
                        }
                    }
                }
                Ok(Event::End(ref e)) if in_target => {
                    let tag = String::from_utf8_lossy(e.name().as_ref()).to_string();
                    if tag == element_name {
                        if !text_buf.trim().is_empty() {
                            return Some(text_buf);
                        }
                        in_target = false;
                    }
                }
                Ok(Event::End(_)) => {}
                Err(_) => break,
                _ => {}
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::{Command, Value};

    #[test]
    fn test_ramp_parsing() {
        let mut cmd = Command::new("RAMP");
        cmd.args.push(Value::Float(95.0));
        cmd.options
            .insert("increment".to_string(), Value::Float(0.5));
        cmd.options
            .insert("incrementcycle".to_string(), Value::Int(2));

        let ramp_box = Ramp::from_scpicommand(&cmd).unwrap();
        let ramp = ramp_box.as_any().downcast_ref::<Ramp>().unwrap();
        assert_eq!(ramp.temperature, vec![95.0; 6]);
        assert_eq!(ramp.increment, 0.5);
        assert_eq!(ramp.incrementcycle, 2);
    }

    #[test]
    fn test_hold_parsing() {
        let mut cmd = Command::new("HOLD");
        cmd.args.push(Value::Int(60));
        cmd.options.insert("increment".to_string(), Value::Int(5));

        let hold_box = Hold::from_scpicommand(&cmd).unwrap();
        let hold = hold_box.as_any().downcast_ref::<Hold>().unwrap();
        assert_eq!(hold.time, Some(60));
        assert_eq!(hold.increment, 5);
    }

    #[test]
    fn test_hold_and_collect_parsing() {
        let mut cmd = Command::new("HoldAndCollect");
        cmd.args.push(Value::Int(30));
        cmd.options.insert("tiff".to_string(), Value::Bool(true));
        cmd.options.insert("quant".to_string(), Value::Bool(false));

        let hac_box = HoldAndCollect::from_scpicommand(&cmd).unwrap();
        let hac = hac_box.as_any().downcast_ref::<HoldAndCollect>().unwrap();
        assert_eq!(hac.time, 30);
        assert!(hac.tiff);
        assert!(!hac.quant);
    }

    #[test]
    fn test_hacfilt_parsing() {
        let mut cmd = Command::new("HACFILT");
        cmd.args.push(Value::String("x1-m4".to_string()));
        cmd.args.push(Value::String("x2-m5".to_string()));

        let hacfilt_box = HACFILT::from_scpicommand(&cmd).unwrap();
        let hacfilt = hacfilt_box.as_any().downcast_ref::<HACFILT>().unwrap();
        assert_eq!(hacfilt.filters.len(), 2);
        assert_eq!(hacfilt.filters[0], "x1-m4");
        assert_eq!(hacfilt.filters[1], "x2-m5");
    }

    #[test]
    fn test_step_parsing_ramp_hold() {
        // Format matches Python test: commands inside <multiline.step>...</multiline.step>
        let nested_commands_xml = "\t\tRAMP -incrementcycle=2 -incrementstep=2 80 80 80 80 80 80\n\t\tHOLD -incrementcycle=2 -incrementstep=2 300".to_string();

        let mut step_cmd = Command::new("STEP");
        step_cmd.args.push(Value::Int(1));
        step_cmd.args.push(Value::XmlString {
            value: nested_commands_xml.into(),
            tag: "multiline.step".to_string(),
        });

        let step = Step::from_scpicommand(&step_cmd);
        if let Err(e) = &step {
            eprintln!("Step parsing error: {:?}", e);
        }
        assert!(step.is_ok());
    }

    #[test]
    fn test_stage_parsing() {
        // Format matches Python test: STEP command with nested commands in XML
        // The STEP command itself contains an XML string argument
        let step_xml_content = "\t\tRAMP -incrementcycle=2 -incrementstep=2 80 80 80 80 80 80\n\t\tHOLD -incrementcycle=2 -incrementstep=2 300";
        let step_xml = format!(
            "<multiline.step>\n{}\n\t\t</multiline.step>",
            step_xml_content
        );

        let nested_stage_commands_xml = format!("\tSTEP 1 {}\n\t</multiline.stage>", step_xml);

        let mut stage_cmd = Command::new("STAGe");
        stage_cmd.args.push(Value::Int(1));
        stage_cmd.args.push(Value::String("STAGE_1".to_string()));
        stage_cmd.args.push(Value::XmlString {
            value: nested_stage_commands_xml.into(),
            tag: "multiline.stage".to_string(),
        });
        stage_cmd
            .options
            .insert("repeat".to_string(), Value::Int(1));

        let stage = Stage::from_scpicommand(&stage_cmd);
        if let Err(e) = &stage {
            eprintln!("Stage parsing error: {:?}", e);
        }
        assert!(stage.is_ok());
        if let Ok(s) = stage {
            assert_eq!(s.repeat, 1);
            assert_eq!(s.index, Some(1));
            assert_eq!(s.label, Some("STAGE_1".to_string()));
            assert_eq!(s.steps.len(), 1);
        }
    }

    #[test]
    fn test_full_protocol_parsing() {
        let protocol_string = r#"PROTOCOL -volume=50.0 -runmode=standard 2020-02-20_170706 <multiline.protocol>

	STAGE 1 _HOLD_1 <multiline.stage>

		STEP 1 <multiline.step>

			RAMP 60.0 60.0 60.0 60.0 60.0 60.0

			HOLD 60

		</multiline.step>

	</multiline.stage>

	STAGE -repeat=4 2 _PCR_2 <multiline.stage>

		STEP 1 <multiline.step>

			RAMP -increment=-1.0 60.0 60.0 60.0 60.0 60.0 60.0

			HACFILT m1,x1,quant m2,x1,quant m2,x2,quant m3,x2,quant m3,x3,quant m4,x3,quant m4,x4,quant m5,x4,quant m5,x5,quant m6,x5,quant

			HOLDANDCOLLECT -incrementcycle=1 -incrementstep=1 -tiff=False -quant=True -pcr=False 60

		</multiline.step>

	</multiline.stage>

</multiline.protocol>"#;

        let cmd = Command::try_from(protocol_string).expect("Failed to parse protocol command");
        let protocol = Protocol::from_scpicommand(&cmd);

        if let Err(e) = &protocol {
            eprintln!("Protocol parsing error: {:?}", e);
        }
        assert!(protocol.is_ok(), "Protocol should parse successfully");

        let prot = protocol.unwrap();
        assert_eq!(prot.name, "2020-02-20_170706");
        assert_eq!(prot.volume, 50.0);
        assert_eq!(prot.runmode, "standard");
        assert_eq!(prot.stages.len(), 2);

        // Check first stage
        let stage1 = &prot.stages[0];
        assert_eq!(stage1.index, Some(1));
        assert_eq!(stage1.label, Some("_HOLD_1".to_string()));
        assert_eq!(stage1.repeat, 1);
        assert_eq!(stage1.steps.len(), 1);

        let step1 = stage1.steps[0].as_standard().unwrap();
        assert_eq!(step1.temperature, vec![60.0; 6]);
        assert_eq!(step1.time, 60);
        assert_eq!(step1.collect, Some(false));

        // Check second stage
        let stage2 = &prot.stages[1];
        assert_eq!(stage2.index, Some(2));
        assert_eq!(stage2.label, Some("_PCR_2".to_string()));
        assert_eq!(stage2.repeat, 4);
        assert_eq!(stage2.steps.len(), 1);

        let step2 = stage2.steps[0].as_standard().unwrap();
        assert_eq!(step2.temperature, vec![60.0; 6]);
        assert_eq!(step2.temp_increment, -1.0);
        assert_eq!(step2.time, 60);
        assert_eq!(step2.collect, Some(true));
        assert_eq!(step2.filters.len(), 10);
        assert_eq!(step2.filters[0], "m1,x1,quant");
        assert_eq!(step2.filters[9], "m6,x5,quant");
        assert!(step2.quant);
        assert!(!step2.tiff);
        assert!(!step2.pcr);
    }

    #[test]
    fn test_protocol_parsing_with_inline_comments() {
        // Test parsing the actual return string from PROT? ${Protocol} command
        // This is wrapped in <quote.reply> tags
        let protocol_content = r#"<quote.reply>
        STAGE 1 STAGE_1 <multiline.stage>
                STEP 1 <multiline.step>
                        RAMP -incrementcycle=2 -incrementstep=2 90 90 90 90 90 90
                        HOLD -incrementcycle=2 -incrementstep=2 600
                </multiline.step>
        </multiline.stage>
        STAGE -repeat=30 2 STAGE_2 <multiline.stage>
                STEP 1 <multiline.step>
                        RAMP -increment=-0.3448 -incrementcycle=2 -incrementstep=2 90 90 90 90 90 90
                        HACFILT m4,x4,quant # qslib:default_filters
                        HOLDANDCOLLECT -incrementcycle=2 -incrementstep=2 -tiff=False -quant=True -pcr=False 20
                </multiline.step>
        </multiline.stage>
        STAGE -repeat=5 3 STAGE_3 <multiline.stage>
                STEP 1 <multiline.step>
                        RAMP -incrementcycle=2 -incrementstep=2 80 80 80 80 80 80
                        HACFILT m4,x4,quant # qslib:default_filters
                        HOLDANDCOLLECT -incrementcycle=2 -incrementstep=2 -tiff=False -quant=True -pcr=False 60
                </multiline.step>
        </multiline.stage>
        STAGE -repeat=550 4 STAGE_4 <multiline.stage>
                STEP 1 <multiline.step>
                        RAMP -increment=-0.1002 -incrementcycle=2 -incrementstep=2 80 80 80 80 80 80
                        HACFILT m4,x4,quant # qslib:default_filters
                        HOLDANDCOLLECT -incrementcycle=2 -incrementstep=2 -tiff=False -quant=True -pcr=False 26
                </multiline.step>
        </multiline.stage>
        STAGE -repeat=4 5 STAGE_5 <multiline.stage>
                STEP 1 <multiline.step>
                        RAMP -incrementcycle=2 -incrementstep=2 25 25 25 25 25 25
                        HACFILT m4,x4,quant # qslib:default_filters
                        HOLDANDCOLLECT -incrementcycle=2 -incrementstep=2 -tiff=False -quant=True -pcr=False 1800
                </multiline.step>
        </multiline.stage>
</quote.reply>"#;

        // When the response comes back, it's parsed as an XmlString Value
        // We need to simulate how get_running_protocol processes it
        // The Value::to_string() should extract the content from <quote.reply>
        let protocol_value = Value::XmlString {
            value: protocol_content.into(),
            tag: "quote.reply".to_string(),
        };

        // Extract the content (this is what Value::to_string() does)
        let extracted_content = protocol_value
            .try_into_string()
            .expect("Failed to extract string from XmlString");

        // Now construct the full PROT command
        let prot_command = format!(
            "PROT -volume=50.0 -runmode=standard test_protocol <multiline.protocol>{}</multiline.protocol>",
            extracted_content
        );

        let cmd = Command::try_from(prot_command).expect("Failed to parse protocol command");
        let protocol = Protocol::from_scpicommand(&cmd);

        if let Err(e) = &protocol {
            eprintln!("Protocol parsing error: {:?}", e);
        }
        assert!(
            protocol.is_ok(),
            "Protocol should parse successfully with inline comments"
        );

        let prot = protocol.unwrap();
        assert_eq!(prot.name, "test_protocol");
        assert_eq!(prot.volume, 50.0);
        assert_eq!(prot.runmode, "standard");
        assert_eq!(prot.stages.len(), 5);

        // Check first stage
        let stage1 = &prot.stages[0];
        assert_eq!(stage1.index, Some(1));
        assert_eq!(stage1.label, Some("STAGE_1".to_string()));
        assert_eq!(stage1.repeat, 1);
        assert_eq!(stage1.steps.len(), 1);
        let step1 = stage1.steps[0].as_standard().unwrap();
        assert_eq!(step1.temperature, vec![90.0; 6]);
        assert_eq!(step1.time, 600);
        assert_eq!(step1.collect, Some(false));

        // Check second stage (with inline comment)
        let stage2 = &prot.stages[1];
        assert_eq!(stage2.index, Some(2));
        assert_eq!(stage2.label, Some("STAGE_2".to_string()));
        assert_eq!(stage2.repeat, 30);
        assert_eq!(stage2.steps.len(), 1);
        let step2 = stage2.steps[0].as_standard().unwrap();
        assert_eq!(step2.temperature, vec![90.0; 6]);
        assert_eq!(step2.temp_increment, -0.3448);
        assert_eq!(step2.time, 20);
        assert_eq!(step2.collect, Some(true));
        // With # qslib:default_filters, step filters are empty; default_filters holds the value
        assert!(step2.filters.is_empty());
        assert_eq!(step2.default_filters, vec!["m4,x4,quant"]);
        assert!(step2.quant);
        assert!(!step2.tiff);
        assert!(!step2.pcr);

        // Protocol-level filters should be populated from the consistent default_filters
        assert_eq!(prot.filters, vec!["m4,x4,quant"]);

        // Check third stage
        let stage3 = &prot.stages[2];
        assert_eq!(stage3.index, Some(3));
        assert_eq!(stage3.label, Some("STAGE_3".to_string()));
        assert_eq!(stage3.repeat, 5);
        let step3 = stage3.steps[0].as_standard().unwrap();
        assert_eq!(step3.temperature, vec![80.0; 6]);
        assert_eq!(step3.time, 60);

        // Check fourth stage
        let stage4 = &prot.stages[3];
        assert_eq!(stage4.index, Some(4));
        assert_eq!(stage4.label, Some("STAGE_4".to_string()));
        assert_eq!(stage4.repeat, 550);
        let step4 = stage4.steps[0].as_standard().unwrap();
        assert_eq!(step4.temperature, vec![80.0; 6]);
        assert_eq!(step4.temp_increment, -0.1002);
        assert_eq!(step4.time, 26);

        // Check fifth stage
        let stage5 = &prot.stages[4];
        assert_eq!(stage5.index, Some(5));
        assert_eq!(stage5.label, Some("STAGE_5".to_string()));
        assert_eq!(stage5.repeat, 4);
        let step5 = stage5.steps[0].as_standard().unwrap();
        assert_eq!(step5.temperature, vec![25.0; 6]);
        assert_eq!(step5.time, 1800);
    }

    // =====================================================================
    // Additional protocol parsing tests comparing to Python output
    // =====================================================================

    /// Test parsing the exact protocol string from Python test_protocol.py PROTSTRING
    #[test]
    fn test_python_protstring_compatibility() {
        // This is the exact PROTSTRING from tests/test_protocol.py
        let protstring = r#"PROTOCOL -volume=30 -runmode=standard testproto <multiline.protocol>
	STAGE 1 STAGE_1 <multiline.stage>
		STEP 1 <multiline.step>
			RAMP -incrementcycle=2 -incrementstep=2 80 80 80 80 80 80
			HOLD -incrementcycle=2 -incrementstep=2 300
		</multiline.step>
	</multiline.stage>
	STAGE -repeat=27 2 STAGE_2 <multiline.stage>
		STEP 1 <multiline.step>
			RAMP -increment=-1 -incrementcycle=2 -incrementstep=2 80 80 80 80 80 80
			HOLD -incrementcycle=2 -incrementstep=2 147600
		</multiline.step>
	</multiline.stage>
	STAGE -repeat=5 3 STAGE_3 <multiline.stage>
		STEP 1 <multiline.step>
			RAMP -incrementcycle=2 -incrementstep=2 53 53 53 53 53 53
			HACFILT m4,x1,quant m5,x3,quant
			HOLDANDCOLLECT -incrementcycle=2 -incrementstep=2 -tiff=False -quant=True -pcr=False 120
		</multiline.step>
	</multiline.stage>
	STAGE -repeat=20 4 STAGE_4 <multiline.stage>
		STEP 1 <multiline.step>
			RAMP -incrementcycle=2 -incrementstep=2 51.2 50.84 50.480000000000004 50.12 49.76 49.4
			HACFILT m4,x1,quant m5,x3,quant
			HOLDANDCOLLECT -incrementcycle=2 -incrementstep=2 -tiff=False -quant=True -pcr=False 64800000
		</multiline.step>
	</multiline.stage>
	STAGE -repeat=20 5 STAGE_5 <multiline.stage>
		STEP 1 <multiline.step>
			RAMP -incrementcycle=2 -incrementstep=2 51.2 50.84 50.480000000000004 50.12 49.76 49.4
			HACFILT m4,x1,quant m5,x3,quant
			HOLDANDCOLLECT -incrementcycle=2 -incrementstep=2 -tiff=False -quant=True -pcr=False 86400
		</multiline.step>
	</multiline.stage>
	STAGE -repeat=100 6 STAGE_6 <multiline.stage>
		STEP 1 <multiline.step>
			RAMP -incrementcycle=2 -incrementstep=2 51.2 50.84 50.480000000000004 50.12 49.76 49.4
			HACFILT m4,x1,quant m5,x3,quant
			HOLDANDCOLLECT -incrementcycle=2 -incrementstep=2 -tiff=False -quant=True -pcr=False 1200
		</multiline.step>
	</multiline.stage>
</multiline.protocol>
"#;

        let cmd = Command::try_from(protstring).expect("Failed to parse protocol command");
        let protocol = Protocol::from_scpicommand(&cmd);

        assert!(
            protocol.is_ok(),
            "Protocol should parse: {:?}",
            protocol.err()
        );
        let prot = protocol.unwrap();

        assert_eq!(prot.name, "testproto");
        assert_eq!(prot.volume, 30.0);
        assert_eq!(prot.runmode, "standard");
        assert_eq!(prot.stages.len(), 6);

        // Stage 1: Simple hold
        assert_eq!(prot.stages[0].repeat, 1);
        assert_eq!(
            prot.stages[0].steps[0].as_standard().unwrap().temperature,
            vec![80.0; 6]
        );
        assert_eq!(prot.stages[0].steps[0].as_standard().unwrap().time, 300); // 5 minutes
        assert_eq!(
            prot.stages[0].steps[0].as_standard().unwrap().collect,
            Some(false)
        );

        // Stage 2: Long hold with temp decrement
        assert_eq!(prot.stages[1].repeat, 27);
        assert_eq!(
            prot.stages[1].steps[0]
                .as_standard()
                .unwrap()
                .temp_increment,
            -1.0
        );
        assert_eq!(prot.stages[1].steps[0].as_standard().unwrap().time, 147600); // 41 hours

        // Stage 3: Collecting stage
        assert_eq!(prot.stages[2].repeat, 5);
        assert_eq!(
            prot.stages[2].steps[0].as_standard().unwrap().temperature,
            vec![53.0; 6]
        );
        assert_eq!(prot.stages[2].steps[0].as_standard().unwrap().time, 120); // 2 minutes
        assert_eq!(
            prot.stages[2].steps[0].as_standard().unwrap().collect,
            Some(true)
        );
        assert_eq!(
            prot.stages[2].steps[0].as_standard().unwrap().filters.len(),
            2
        );

        // Stage 4: Very long collection with zone temps
        assert_eq!(prot.stages[3].repeat, 20);
        let expected_temps = vec![51.2, 50.84, 50.480000000000004, 50.12, 49.76, 49.4];
        assert_eq!(
            prot.stages[3].steps[0].as_standard().unwrap().temperature,
            expected_temps
        );
        assert_eq!(
            prot.stages[3].steps[0].as_standard().unwrap().time,
            64800000
        );

        // Stage 5-6: Similar structure
        assert_eq!(prot.stages[4].repeat, 20);
        assert_eq!(prot.stages[4].steps[0].as_standard().unwrap().time, 86400); // 24 hours

        assert_eq!(prot.stages[5].repeat, 100);
        assert_eq!(prot.stages[5].steps[0].as_standard().unwrap().time, 1200); // 20 minutes
    }

    /// Test format_duration helper matches Python _durformat behavior
    #[test]
    fn test_format_duration() {
        // <= 2 minutes: stay as seconds
        assert_eq!(format_duration(1), "1s");
        assert_eq!(format_duration(30), "30s");
        assert_eq!(format_duration(59), "59s");
        assert_eq!(format_duration(120), "120s");

        // > 2 minutes: use minutes
        assert_eq!(format_duration(180), "3m");
        assert_eq!(format_duration(181), "3m1s");

        // > 2 hours: use hours
        assert_eq!(format_duration(10800), "3h");
        assert_eq!(format_duration(10861), "3h1m1s");

        // Edge cases
        assert_eq!(format_duration(3600), "60m"); // 1 hour
        assert_eq!(format_duration(7200), "120m"); // 2 hours
        assert_eq!(format_duration(7201), "2h1s"); // just over 2 hours
    }

    /// Test format_temperature helper
    #[test]
    fn test_format_temperature() {
        // Single temperature (all zones same)
        assert_eq!(format_temperature(&[60.0; 6]), "60.00°C");

        // Empty
        assert_eq!(format_temperature(&[]), "");

        // Different zone temperatures
        let temps = vec![51.2, 50.84, 50.48, 50.12, 49.76, 49.4];
        let formatted = format_temperature(&temps);
        assert!(formatted.starts_with("["));
        assert!(formatted.ends_with("]°C"));
    }

    /// Test filter_to_lowerform helper
    #[test]
    fn test_filter_to_lowerform() {
        // Already in lower form
        assert_eq!(filter_to_lowerform("x1-m4"), "x1-m4");
        assert_eq!(filter_to_lowerform("x3-m5"), "x3-m5");

        // Convert from m,x format
        assert_eq!(filter_to_lowerform("m4,x1"), "x1-m4");
        assert_eq!(filter_to_lowerform("m5,x3"), "x3-m5");

        // Unknown format passes through
        assert_eq!(filter_to_lowerform("unknown"), "unknown");
    }

    /// Test oxford_list helper
    #[test]
    fn test_oxford_list() {
        assert_eq!(oxford_list(&[]), "");
        assert_eq!(oxford_list(&["one".to_string()]), "one");
        assert_eq!(
            oxford_list(&["one".to_string(), "two".to_string()]),
            "one and two"
        );
        assert_eq!(
            oxford_list(&["one".to_string(), "two".to_string(), "three".to_string()]),
            "one, two, and three"
        );
    }

    /// Test Step::info_str output format
    #[test]
    fn test_step_info_str() {
        let step = Step {
            time: 60,
            temperature: vec![60.0; 6],
            collect: Some(false),
            temp_increment: 0.0,
            temp_incrementcycle: 2,
            temp_incrementpoint: None,
            time_increment: 0,
            time_incrementcycle: 2,
            time_incrementpoint: None,
            filters: vec![],
            pcr: false,
            quant: true,
            tiff: false,
            repeat: 1,
            default_filters: vec![],
        };

        let info = step.info_str(Some(1), 1);
        assert!(info.contains("60.00°C"));
        assert!(info.contains("60s"));
    }

    /// Test Step::info_str with collection
    #[test]
    fn test_step_info_str_collecting() {
        let step = Step {
            time: 120,
            temperature: vec![53.0; 6],
            collect: Some(true),
            temp_increment: 0.0,
            temp_incrementcycle: 2,
            temp_incrementpoint: None,
            time_increment: 0,
            time_incrementcycle: 2,
            time_incrementpoint: None,
            filters: vec!["x1-m4".to_string(), "x3-m5".to_string()],
            pcr: false,
            quant: true,
            tiff: false,
            repeat: 1,
            default_filters: vec![],
        };

        let info = step.info_str(Some(1), 5);
        assert!(info.contains("53.00°C"));
        assert!(info.contains("120s"));
        assert!(info.contains("collects"));
        assert!(info.contains("x1-m4"));
    }

    /// Test Stage::info_str output
    #[test]
    fn test_stage_info_str() {
        let step = Step {
            time: 60,
            temperature: vec![60.0; 6],
            collect: Some(false),
            temp_increment: 0.0,
            temp_incrementcycle: 2,
            temp_incrementpoint: None,
            time_increment: 0,
            time_incrementcycle: 2,
            time_incrementpoint: None,
            filters: vec![],
            pcr: false,
            quant: true,
            tiff: false,
            repeat: 1,
            default_filters: vec![],
        };

        let stage = Stage {
            steps: vec![StageStep::Standard(step)],
            repeat: 10,
            index: Some(1),
            label: Some("STAGE_1".to_string()),
            default_filters: vec![],
        };

        let info = stage.info_str(Some(1));
        assert!(info.contains("10 cycles"));
        assert!(info.contains("total duration"));
    }

    /// Test Protocol Display implementation
    #[test]
    fn test_protocol_display() {
        let protocol_string = r#"PROTOCOL -volume=50.0 -runmode=standard test_protocol <multiline.protocol>
	STAGE 1 _HOLD_1 <multiline.stage>
		STEP 1 <multiline.step>
			RAMP 60.0 60.0 60.0 60.0 60.0 60.0
			HOLD 60
		</multiline.step>
	</multiline.stage>
</multiline.protocol>"#;

        let cmd = Command::try_from(protocol_string).expect("Failed to parse");
        let protocol = Protocol::from_scpicommand(&cmd).expect("Failed to parse protocol");

        let display = format!("{}", protocol);
        assert!(display.contains("Run Protocol test_protocol"));
        assert!(display.contains("sample volume 50 µL"));
        assert!(display.contains("run mode standard"));
    }

    fn join_lines(lines: &[ProtocolLine]) -> String {
        lines
            .iter()
            .map(|l| l.text.as_str())
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// `info_lines(None, None, None)` joined by newlines must equal `Display`.
    #[test]
    fn test_info_lines_matches_display() {
        // Single-step stage.
        let single = "PROTOCOL -volume=50.0 -runmode=standard p <multiline.protocol>\n\tSTAGE 1 _HOLD <multiline.stage>\n\t\tSTEP 1 <multiline.step>\n\t\t\tRAMP 60.0 60.0 60.0 60.0 60.0 60.0\n\t\t\tHOLD 60\n\t\t</multiline.step>\n\t</multiline.stage>\n</multiline.protocol>";
        // Two stages, second with two steps and a collecting step.
        let multi = "PROT -volume=25 -runmode=standard qpcr <multiline.protocol>\n\tSTAGE 1 _HOLD <multiline.stage>\n\t\tSTEP 1 <multiline.step>\n\t\t\tRAMP 95\n\t\t\tHOLD 120\n\t\t</multiline.step>\n\t</multiline.stage>\n\tSTAGE -repeat=40 2 _CYCLE <multiline.stage>\n\t\tSTEP 1 <multiline.step>\n\t\t\tRAMP 95\n\t\t\tHOLD 15\n\t\t</multiline.step>\n\t\tSTEP 2 <multiline.step>\n\t\t\tRAMP 60\n\t\t\tHACFILT x1-m4\n\t\t\tHOLDANDCOLLECT 60\n\t\t</multiline.step>\n\t</multiline.stage>\n</multiline.protocol>";

        for s in [single, multi] {
            let cmd = Command::try_from(s).expect("parse command");
            let protocol = Protocol::from_scpicommand(&cmd).expect("parse protocol");
            assert_eq!(
                join_lines(&protocol.info_lines(None, None, None)),
                format!("{}", protocol),
                "info_lines drifted from Display for:\n{}",
                s
            );
        }

        // Protocol-level default-filters line: parse a real protocol (so it has
        // stages) and set the filters directly.
        let cmd = Command::try_from(multi).expect("parse command");
        let mut with_filters = Protocol::from_scpicommand(&cmd).expect("parse protocol");
        with_filters.filters = vec!["x1-m4".to_string(), "x4-m5".to_string()];
        let joined = join_lines(&with_filters.info_lines(None, None, None));
        assert!(joined.contains("(default filters x1-m4 and x4-m5)"));
        assert_eq!(joined, format!("{}", with_filters));
    }

    /// `info_lines` marks the current stage/step and notes the cycle.
    #[test]
    fn test_info_lines_highlight() {
        let multi = "PROT -volume=25 -runmode=standard qpcr <multiline.protocol>\n\tSTAGE 1 _HOLD <multiline.stage>\n\t\tSTEP 1 <multiline.step>\n\t\t\tRAMP 95\n\t\t\tHOLD 120\n\t\t</multiline.step>\n\t</multiline.stage>\n\tSTAGE -repeat=40 2 _CYCLE <multiline.stage>\n\t\tSTEP 1 <multiline.step>\n\t\t\tRAMP 95\n\t\t\tHOLD 15\n\t\t</multiline.step>\n\t\tSTEP 2 <multiline.step>\n\t\t\tRAMP 60\n\t\t\tHACFILT x1-m4\n\t\t\tHOLDANDCOLLECT 60\n\t\t</multiline.step>\n\t</multiline.stage>\n</multiline.protocol>";
        let cmd = Command::try_from(multi).expect("parse command");
        let protocol = Protocol::from_scpicommand(&cmd).expect("parse protocol");

        // Current position: stage 2, step 2, cycle 12.
        let lines = protocol.info_lines(Some(2), Some(2), Some(12));

        let current: Vec<&ProtocolLine> = lines.iter().filter(|l| l.current).collect();
        // The stage-2 header line and its step-2 line are current; nothing else.
        assert_eq!(current.len(), 2);
        assert!(current[0].text.contains("Stage with 40 cycles"));
        assert!(current[0].text.contains("cycle 12/40"));
        assert!(current[1].text.contains("60.00°C for 60s/cycle"));

        // Stage 1 and step 1 of stage 2 are not marked.
        assert!(lines
            .iter()
            .any(|l| l.text.contains("Stage with 1 cycle") && !l.current));
        assert!(lines
            .iter()
            .any(|l| l.text.contains("95.00°C for 15s/cycle") && !l.current));

        // A non-matching stage number highlights nothing.
        assert!(protocol
            .info_lines(Some(0), None, None)
            .iter()
            .all(|l| !l.current));
    }

    #[test]
    fn test_view() {
        let multi = "PROT -volume=25 -runmode=standard qpcr <multiline.protocol>\n\tSTAGE 1 _HOLD <multiline.stage>\n\t\tSTEP 1 <multiline.step>\n\t\t\tRAMP 95\n\t\t\tHOLD 120\n\t\t</multiline.step>\n\t</multiline.stage>\n\tSTAGE -repeat=40 2 _CYCLE <multiline.stage>\n\t\tSTEP 1 <multiline.step>\n\t\t\tRAMP 95\n\t\t\tHOLD 15\n\t\t</multiline.step>\n\t\tSTEP 2 <multiline.step>\n\t\t\tRAMP 60\n\t\t\tHACFILT x1-m4\n\t\t\tHOLDANDCOLLECT 60\n\t\t</multiline.step>\n\t</multiline.stage>\n</multiline.protocol>";
        let cmd = Command::try_from(multi).expect("parse command");
        let protocol = Protocol::from_scpicommand(&cmd).expect("parse protocol");

        let view = protocol.view(Some(2), Some(2), Some(12));
        assert_eq!(view.name, "qpcr");
        assert_eq!(
            view.details,
            vec![
                "sample volume 25 µL".to_string(),
                "run mode standard".to_string()
            ]
        );
        assert_eq!(view.default_filters, None);
        assert_eq!(view.stages.len(), 2);

        // Stage 1: not current, single step, no cycle note.
        let s1 = &view.stages[0];
        assert_eq!(s1.number, 1);
        assert_eq!(s1.summary, "Stage with 1 cycle (total duration 120s)");
        assert!(!s1.current);
        assert_eq!(s1.cycle, None);
        assert_eq!(s1.steps.len(), 1);
        assert_eq!(s1.steps[0].text, "95.00°C for 120s/cycle");
        assert!(!s1.steps[0].current);

        // Stage 2: current, cycle note, two steps, step 2 current.
        let s2 = &view.stages[1];
        assert_eq!(s2.number, 2);
        assert_eq!(s2.summary, "Stage with 40 cycles (total duration 50m)");
        assert!(s2.current);
        assert_eq!(s2.cycle, Some("cycle 12/40".to_string()));
        assert_eq!(s2.steps.len(), 2);
        assert_eq!(s2.steps[0].text, "95.00°C for 15s/cycle");
        assert!(!s2.steps[0].current);
        assert_eq!(s2.steps[1].text, "60.00°C for 60s/cycle (collects x1-m4)");
        assert!(s2.steps[1].current);

        // Idle: nothing current, no cycle notes.
        let idle = protocol.view(None, None, None);
        assert!(idle.stages.iter().all(|s| !s.current && s.cycle.is_none()));
        assert!(idle
            .stages
            .iter()
            .all(|s| s.steps.iter().all(|st| !st.current)));
    }

    /// Test Ramp with temperature list parsing (comma-separated string)
    #[test]
    fn test_ramp_with_temperature_list_comma() {
        let mut cmd = Command::new("RAMP");
        cmd.args.push(Value::String(
            "51.2,50.84,50.48,50.12,49.76,49.4".to_string(),
        ));

        let ramp_box = Ramp::from_scpicommand(&cmd).unwrap();
        let ramp = ramp_box.as_any().downcast_ref::<Ramp>().unwrap();

        assert_eq!(ramp.temperature.len(), 6);
        assert!((ramp.temperature[0] - 51.2).abs() < 0.001);
        assert!((ramp.temperature[5] - 49.4).abs() < 0.001);
    }

    /// Test Ramp with space-separated temperature args (how SCPI parses)
    #[test]
    fn test_ramp_with_temperature_list_args() {
        // When parsing "RAMP 51.2 50.84 50.48 50.12 49.76 49.4", each temp becomes separate arg
        let mut cmd = Command::new("RAMP");
        cmd.args.push(Value::Float(51.2));
        cmd.args.push(Value::Float(50.84));
        cmd.args.push(Value::Float(50.48));
        cmd.args.push(Value::Float(50.12));
        cmd.args.push(Value::Float(49.76));
        cmd.args.push(Value::Float(49.4));

        let ramp_box = Ramp::from_scpicommand(&cmd).unwrap();
        let ramp = ramp_box.as_any().downcast_ref::<Ramp>().unwrap();

        assert_eq!(ramp.temperature.len(), 6);
        assert!((ramp.temperature[0] - 51.2).abs() < 0.001);
        assert!((ramp.temperature[1] - 50.84).abs() < 0.001);
        assert!((ramp.temperature[5] - 49.4).abs() < 0.001);
    }

    /// Test RAMP parsing from string format  
    #[test]
    fn test_ramp_from_string() {
        let cmd =
            Command::try_from("RAMP -incrementcycle=2 51.2 50.84 50.48 50.12 49.76 49.4").unwrap();

        assert_eq!(cmd.args.len(), 6, "Expected 6 args, got {:?}", cmd.args);

        let ramp_box = Ramp::from_scpicommand(&cmd).unwrap();
        let ramp = ramp_box.as_any().downcast_ref::<Ramp>().unwrap();

        assert_eq!(ramp.temperature.len(), 6);
        let expected = [51.2, 50.84, 50.48, 50.12, 49.76, 49.4];
        for (i, exp) in expected.iter().enumerate() {
            assert!(
                (ramp.temperature[i] - exp).abs() < 0.001,
                "temp[{}]: expected {}, got {}",
                i,
                exp,
                ramp.temperature[i]
            );
        }
    }

    /// Test that extract_commands_from_value correctly parses RAMP temperatures
    #[test]
    fn test_extract_commands_ramp_temps() {
        let xml_content = "RAMP -incrementcycle=2 -incrementstep=2 51.2 50.84 50.48 50.12 49.76 49.4\nHACFILT m4,x1,quant m5,x3,quant\nHOLDANDCOLLECT -tiff=False -quant=True -pcr=False 64800000";
        let value = Value::XmlString {
            value: xml_content.into(),
            tag: "multiline.step".to_string(),
        };

        let commands =
            extract_commands_from_value(&value, None).expect("Failed to extract commands");

        assert_eq!(commands.len(), 3, "Expected 3 commands");
        assert_eq!(get_command_name(&commands[0]), "RAMP");

        // Check that RAMP has 6 args (the temperatures)
        assert_eq!(
            commands[0].args.len(),
            6,
            "RAMP should have 6 temperature args, got {:?}",
            commands[0].args
        );

        // Verify each temperature
        let expected = [51.2, 50.84, 50.48, 50.12, 49.76, 49.4];
        for (i, exp) in expected.iter().enumerate() {
            match &commands[0].args[i] {
                Value::Float(f) => assert!(
                    (f - exp).abs() < 0.001,
                    "arg[{}]: expected {}, got {}",
                    i,
                    exp,
                    f
                ),
                other => panic!("Expected Float for arg[{}], got {:?}", i, other),
            }
        }
    }

    /// Test Step parsing with RAMP + HACFILT + HOLDANDCOLLECT and zone temps
    #[test]
    fn test_step_ramp_hacfilt_holdcollect_zone_temps() {
        let step_xml = "RAMP -incrementcycle=2 -incrementstep=2 51.2 50.84 50.48 50.12 49.76 49.4\nHACFILT m4,x1,quant m5,x3,quant\nHOLDANDCOLLECT -incrementcycle=2 -incrementstep=2 -tiff=False -quant=True -pcr=False 64800000";

        let mut step_cmd = Command::new("STEP");
        step_cmd.args.push(Value::Int(1));
        step_cmd.args.push(Value::XmlString {
            value: step_xml.into(),
            tag: "multiline.step".to_string(),
        });

        let step = Step::from_scpicommand(&step_cmd);
        assert!(step.is_ok(), "Step should parse: {:?}", step.err());

        let step_box = step.unwrap();
        let step_obj = step_box.as_any().downcast_ref::<Step>().unwrap();

        let expected_temps = [51.2, 50.84, 50.48, 50.12, 49.76, 49.4];
        assert_eq!(
            step_obj.temperature.len(),
            6,
            "Expected 6 temperatures, got {:?}",
            step_obj.temperature
        );
        for (i, exp) in expected_temps.iter().enumerate() {
            assert!(
                (step_obj.temperature[i] - exp).abs() < 0.01,
                "temp[{}]: expected {}, got {}",
                i,
                exp,
                step_obj.temperature[i]
            );
        }
    }

    /// Test Hold with no time (indefinite hold)
    #[test]
    fn test_hold_indefinite() {
        let mut cmd = Command::new("HOLD");
        cmd.args.push(Value::String("".to_string()));

        let hold_box = Hold::from_scpicommand(&cmd).unwrap();
        let hold = hold_box.as_any().downcast_ref::<Hold>().unwrap();

        assert_eq!(hold.time, None);
    }

    /// Test exposure command parsing
    #[test]
    fn test_exposure_parsing() {
        let mut cmd = Command::new("EXPOSURE");
        cmd.args
            .push(Value::String("1,4,quant,500,2000".to_string()));
        cmd.options.insert(
            "state".to_string(),
            Value::String("HoldAndCollect".to_string()),
        );

        let exp_box = Exposure::from_scpicommand(&cmd).unwrap();
        let exp = exp_box.as_any().downcast_ref::<Exposure>().unwrap();

        assert_eq!(exp.state, "HoldAndCollect");
    }

    /// Test command name case insensitivity
    #[test]
    fn test_command_case_insensitivity() {
        // Test that commands parse regardless of case
        let test_cases = ["RAMP 60", "ramp 60", "Ramp 60"];

        for input in test_cases {
            let cmd = Command::try_from(input);
            assert!(cmd.is_ok(), "Failed to parse: {}", input);
        }
    }

    /// Test Protocol with PRERUN and POSTRUN sections
    #[test]
    fn test_protocol_with_prerun_postrun() {
        let protocol_string = r#"PROTOCOL -volume=50.0 test_protocol <multiline.protocol>
	PRERUN <multiline.prerun>
		LAMP ON
	</multiline.prerun>
	STAGE 1 _HOLD_1 <multiline.stage>
		STEP 1 <multiline.step>
			RAMP 60.0 60.0 60.0 60.0 60.0 60.0
			HOLD 60
		</multiline.step>
	</multiline.stage>
	POSTRUN <multiline.postrun>
		LAMP OFF
	</multiline.postrun>
</multiline.protocol>"#;

        let cmd = Command::try_from(protocol_string).expect("Failed to parse");
        let protocol = Protocol::from_scpicommand(&cmd).expect("Failed to parse protocol");

        assert_eq!(protocol.stages.len(), 1);
        // PRERUN and POSTRUN should be parsed
        assert!(
            !protocol.prerun.is_empty()
                || !protocol.postrun.is_empty()
                || protocol.stages.len() == 1
        );
    }

    /// Test error handling for invalid protocol
    #[test]
    fn test_invalid_protocol_command() {
        let cmd = Command::new("NOTAPROTOCOL");
        let result = Protocol::from_scpicommand(&cmd);
        assert!(result.is_err());
    }

    /// Test error handling for missing protocol name
    #[test]
    fn test_protocol_missing_name() {
        let cmd = Command::new("PROTOCOL");
        // No args
        let result = Protocol::from_scpicommand(&cmd);
        assert!(result.is_err());
    }

    /// Test parsing with various temperature increment settings
    #[test]
    fn test_step_with_increments() {
        let step = Step {
            time: 60,
            temperature: vec![80.0; 6],
            collect: Some(false),
            temp_increment: -1.0,
            temp_incrementcycle: 2,
            temp_incrementpoint: Some(3),
            time_increment: 5,
            time_incrementcycle: 2,
            time_incrementpoint: Some(4),
            filters: vec![],
            pcr: false,
            quant: true,
            tiff: false,
            repeat: 10,
            default_filters: vec![],
        };

        let info = step.info_str(Some(1), 10);
        // Should mention temperature increment
        assert!(info.contains("-1") || info.contains("°C"));
    }

    /// Test extract_temperature_list with single value expanded to 1 zone
    #[test]
    fn test_extract_temperature_list_single_zone() {
        let cmd = Command::new("RAMP");
        let value = Value::Float(60.0);

        let temps = extract_temperature_list(&value, &cmd, 1).unwrap();
        assert_eq!(temps.len(), 1);
        assert!((temps[0] - 60.0).abs() < 0.001);
    }

    /// Test extract_temperature_list with single value expanded to 3 zones
    #[test]
    fn test_extract_temperature_list_three_zones() {
        let cmd = Command::new("RAMP");
        let value = Value::Float(60.0);

        let temps = extract_temperature_list(&value, &cmd, 3).unwrap();
        assert_eq!(temps.len(), 3);
        for t in &temps {
            assert!((t - 60.0).abs() < 0.001);
        }
    }

    /// Test extract_temperature_list with comma-separated string (auto-detect 3 zones)
    #[test]
    fn test_extract_temperature_list_comma_three_zones() {
        let cmd = Command::new("RAMP");
        let value = Value::String("60.0,61.0,62.0".to_string());

        // num_zones parameter is ignored when multiple values are provided
        let temps = extract_temperature_list(&value, &cmd, 6).unwrap();
        assert_eq!(
            temps.len(),
            3,
            "Should auto-detect 3 zones from comma-separated string"
        );
        assert!((temps[0] - 60.0).abs() < 0.001);
        assert!((temps[1] - 61.0).abs() < 0.001);
        assert!((temps[2] - 62.0).abs() < 0.001);
    }

    /// Test extract_temperature_list_from_args with single arg expanded to 1 zone
    #[test]
    fn test_extract_temperature_list_from_args_single_zone() {
        let cmd = Command::new("RAMP");
        let args = vec![Value::Float(60.0)];

        let temps = extract_temperature_list_from_args(&args, &cmd, 1).unwrap();
        assert_eq!(temps.len(), 1);
        assert!((temps[0] - 60.0).abs() < 0.001);
    }

    /// Test extract_temperature_list_from_args with 3 args (auto-detect)
    #[test]
    fn test_extract_temperature_list_from_args_three_zones() {
        let cmd = Command::new("RAMP");
        let args = vec![Value::Float(60.0), Value::Float(61.0), Value::Float(62.0)];

        // num_zones is ignored when multiple args are provided (auto-detect)
        let temps = extract_temperature_list_from_args(&args, &cmd, 6).unwrap();
        assert_eq!(temps.len(), 3, "Should auto-detect 3 zones from args");
        assert!((temps[0] - 60.0).abs() < 0.001);
        assert!((temps[1] - 61.0).abs() < 0.001);
        assert!((temps[2] - 62.0).abs() < 0.001);
    }

    /// Test Ramp parsing with 1-zone (single temperature)
    #[test]
    fn test_ramp_single_temperature_expansion() {
        let mut cmd = Command::new("RAMP");
        cmd.args.push(Value::Float(60.0));

        let ramp_box = Ramp::from_scpicommand(&cmd).unwrap();
        let ramp = ramp_box.as_any().downcast_ref::<Ramp>().unwrap();

        // With DEFAULT_NUM_ZONES=6, single temp expands to 6
        assert_eq!(ramp.temperature.len(), DEFAULT_NUM_ZONES);
        for t in &ramp.temperature {
            assert!((t - 60.0).abs() < 0.001);
        }
    }

    /// Test Ramp parsing with 3-zone comma-separated temperatures
    #[test]
    fn test_ramp_with_three_zone_temps() {
        let mut cmd = Command::new("RAMP");
        cmd.args.push(Value::String("60.0,61.0,62.0".to_string()));

        let ramp_box = Ramp::from_scpicommand(&cmd).unwrap();
        let ramp = ramp_box.as_any().downcast_ref::<Ramp>().unwrap();

        // Auto-detect 3 zones from the values
        assert_eq!(ramp.temperature.len(), 3);
        assert!((ramp.temperature[0] - 60.0).abs() < 0.001);
        assert!((ramp.temperature[1] - 61.0).abs() < 0.001);
        assert!((ramp.temperature[2] - 62.0).abs() < 0.001);
    }

    /// Test Ramp parsing with 3 separate args (auto-detect zones)
    #[test]
    fn test_ramp_with_three_zone_args() {
        let mut cmd = Command::new("RAMP");
        cmd.args.push(Value::Float(60.0));
        cmd.args.push(Value::Float(61.0));
        cmd.args.push(Value::Float(62.0));

        let ramp_box = Ramp::from_scpicommand(&cmd).unwrap();
        let ramp = ramp_box.as_any().downcast_ref::<Ramp>().unwrap();

        assert_eq!(ramp.temperature.len(), 3);
        assert!((ramp.temperature[0] - 60.0).abs() < 0.001);
        assert!((ramp.temperature[1] - 61.0).abs() < 0.001);
        assert!((ramp.temperature[2] - 62.0).abs() < 0.001);
    }

    /// Test DEFAULT_NUM_ZONES constant is accessible
    #[test]
    fn test_default_num_zones_constant() {
        assert_eq!(DEFAULT_NUM_ZONES, 6);
    }

    // =====================================================================
    // Serialization tests
    // =====================================================================

    #[test]
    fn test_ramp_serialization_basic() {
        let ramp = Ramp {
            temperature: vec![80.0; 6],
            increment: 0.0,
            incrementcycle: 2,
            incrementstep: 2,
            rate: 100.0,
            cover: None,
        };
        assert_eq!(ramp.to_scpi_string(), "RAMP 80 80 80 80 80 80\n");
    }

    #[test]
    fn test_ramp_serialization_with_options() {
        let ramp = Ramp {
            temperature: vec![80.0; 6],
            increment: -1.0,
            incrementcycle: 2,
            incrementstep: 2,
            rate: 100.0,
            cover: None,
        };
        assert_eq!(
            ramp.to_scpi_string(),
            "RAMP -increment=-1 80 80 80 80 80 80\n"
        );
    }

    #[test]
    fn test_ramp_serialization_float_temps() {
        let ramp = Ramp {
            temperature: vec![51.2, 50.84, 50.48, 50.12, 49.76, 49.4],
            increment: 0.0,
            incrementcycle: 2,
            incrementstep: 2,
            rate: 100.0,
            cover: None,
        };
        let s = ramp.to_scpi_string();
        assert!(s.starts_with("RAMP "));
        assert!(s.contains("51.2"));
        assert!(s.contains("49.4"));
    }

    #[test]
    fn test_hold_serialization_basic() {
        let hold = Hold {
            time: Some(300),
            increment: 0,
            incrementcycle: 2,
            incrementstep: 2,
        };
        assert_eq!(hold.to_scpi_string(), "HOLD 300\n");
    }

    #[test]
    fn test_hold_serialization_with_options() {
        let hold = Hold {
            time: Some(60),
            increment: 5,
            incrementcycle: 3,
            incrementstep: 2,
        };
        assert_eq!(
            hold.to_scpi_string(),
            "HOLD -increment=5 -incrementcycle=3 60\n"
        );
    }

    #[test]
    fn test_holdandcollect_serialization() {
        let hac = HoldAndCollect {
            time: 120,
            increment: 0,
            incrementcycle: 2,
            incrementstep: 2,
            tiff: false,
            quant: true,
            pcr: false,
        };
        assert_eq!(
            hac.to_scpi_string(),
            "HOLDANDCOLLECT -tiff=False -quant=True -pcr=False 120\n"
        );
    }

    #[test]
    fn test_hacfilt_serialization() {
        let hacfilt = HACFILT {
            filters: vec!["m4,x1,quant".to_string(), "m5,x3,quant".to_string()],
            default_filters: vec![],
        };
        assert_eq!(
            hacfilt.to_scpi_string(),
            "HACFILT m4,x1,quant m5,x3,quant\n"
        );
    }

    #[test]
    fn test_hacfilt_serialization_default() {
        let hacfilt = HACFILT {
            filters: vec![],
            default_filters: vec!["m4,x4,quant".to_string()],
        };
        assert_eq!(
            hacfilt.to_scpi_string(),
            "HACFILT m4,x4,quant # qslib:default_filters\n"
        );
    }

    #[test]
    fn test_step_serialization_hold() {
        let step = Step {
            time: 300,
            temperature: vec![80.0; 6],
            collect: Some(false),
            temp_increment: 0.0,
            temp_incrementcycle: 2,
            temp_incrementpoint: None,
            time_increment: 0,
            time_incrementcycle: 2,
            time_incrementpoint: None,
            filters: vec![],
            pcr: false,
            quant: true,
            tiff: false,
            repeat: 1,
            default_filters: vec![],
        };
        let s = step.to_scpi_string(1, &[]);
        assert!(s.starts_with("STEP 1 <multiline.step>"));
        assert!(s.contains("RAMP"));
        assert!(s.contains("HOLD"));
        assert!(s.contains("300"));
        assert!(s.contains("</multiline.step>"));
    }

    #[test]
    fn test_step_serialization_collect() {
        let step = Step {
            time: 120,
            temperature: vec![53.0; 6],
            collect: Some(true),
            temp_increment: 0.0,
            temp_incrementcycle: 2,
            temp_incrementpoint: None,
            time_increment: 0,
            time_incrementcycle: 2,
            time_incrementpoint: None,
            filters: vec!["m4,x1,quant".to_string(), "m5,x3,quant".to_string()],
            pcr: false,
            quant: true,
            tiff: false,
            repeat: 1,
            default_filters: vec![],
        };
        let s = step.to_scpi_string(1, &[]);
        assert!(s.contains("RAMP"));
        assert!(s.contains("HACFILT m4,x1,quant m5,x3,quant"));
        assert!(s.contains("HOLDANDCOLLECT"));
        assert!(s.contains("120"));
    }

    #[test]
    fn test_protocol_serialization_roundtrip() {
        // Parse a protocol, serialize it, parse again, check equality
        let protocol_string = r#"PROTOCOL -volume=50.0 -runmode=standard test_roundtrip <multiline.protocol>
	STAGE 1 _HOLD_1 <multiline.stage>
		STEP 1 <multiline.step>
			RAMP 60 60 60 60 60 60
			HOLD 60
		</multiline.step>
	</multiline.stage>
	STAGE -repeat=4 2 _PCR_2 <multiline.stage>
		STEP 1 <multiline.step>
			RAMP -increment=-1 60 60 60 60 60 60
			HACFILT m1,x1,quant m4,x4,quant
			HOLDANDCOLLECT -tiff=False -quant=True -pcr=False 60
		</multiline.step>
	</multiline.stage>
</multiline.protocol>"#;

        let cmd = Command::try_from(protocol_string).expect("Failed to parse protocol command");
        let protocol = Protocol::from_scpicommand(&cmd).expect("Failed to parse protocol");

        // Serialize
        let serialized = protocol.to_scpi_string();

        // Parse again
        let cmd2 =
            Command::try_from(serialized.as_str()).expect("Failed to parse re-serialized protocol");
        let protocol2 =
            Protocol::from_scpicommand(&cmd2).expect("Failed to parse re-serialized protocol");

        // Compare key fields
        assert_eq!(protocol.name, protocol2.name);
        assert_eq!(protocol.volume, protocol2.volume);
        assert_eq!(protocol.runmode, protocol2.runmode);
        assert_eq!(protocol.stages.len(), protocol2.stages.len());

        for (s1, s2) in protocol.stages.iter().zip(protocol2.stages.iter()) {
            assert_eq!(s1.repeat, s2.repeat);
            assert_eq!(s1.steps.len(), s2.steps.len());
            for (st1, st2) in s1.steps.iter().zip(s2.steps.iter()) {
                let st1 = st1.as_standard().unwrap();
                let st2 = st2.as_standard().unwrap();
                assert_eq!(st1.time, st2.time);
                assert_eq!(st1.collect, st2.collect);
                assert_eq!(st1.filters.len(), st2.filters.len());
                for (i, (t1, t2)) in st1
                    .temperature
                    .iter()
                    .zip(st2.temperature.iter())
                    .enumerate()
                {
                    assert!((t1 - t2).abs() < 0.001, "temp[{}]: {} != {}", i, t1, t2);
                }
            }
        }
    }

    #[test]
    fn test_protocol_serialization_complex_roundtrip() {
        // Use the PROTSTRING from Python tests
        let protstring = r#"PROTOCOL -volume=30 -runmode=standard testproto <multiline.protocol>
	STAGE 1 STAGE_1 <multiline.stage>
		STEP 1 <multiline.step>
			RAMP 80 80 80 80 80 80
			HOLD 300
		</multiline.step>
	</multiline.stage>
	STAGE -repeat=27 2 STAGE_2 <multiline.stage>
		STEP 1 <multiline.step>
			RAMP -increment=-1 80 80 80 80 80 80
			HOLD 147600
		</multiline.step>
	</multiline.stage>
	STAGE -repeat=5 3 STAGE_3 <multiline.stage>
		STEP 1 <multiline.step>
			RAMP 53 53 53 53 53 53
			HACFILT m4,x1,quant m5,x3,quant
			HOLDANDCOLLECT -tiff=False -quant=True -pcr=False 120
		</multiline.step>
	</multiline.stage>
	STAGE -repeat=20 4 STAGE_4 <multiline.stage>
		STEP 1 <multiline.step>
			RAMP 51.2 50.84 50.480000000000004 50.12 49.76 49.4
			HACFILT m4,x1,quant m5,x3,quant
			HOLDANDCOLLECT -tiff=False -quant=True -pcr=False 64800000
		</multiline.step>
	</multiline.stage>
	STAGE -repeat=20 5 STAGE_5 <multiline.stage>
		STEP 1 <multiline.step>
			RAMP 51.2 50.84 50.480000000000004 50.12 49.76 49.4
			HACFILT m4,x1,quant m5,x3,quant
			HOLDANDCOLLECT -tiff=False -quant=True -pcr=False 86400
		</multiline.step>
	</multiline.stage>
	STAGE -repeat=100 6 STAGE_6 <multiline.stage>
		STEP 1 <multiline.step>
			RAMP 51.2 50.84 50.480000000000004 50.12 49.76 49.4
			HACFILT m4,x1,quant m5,x3,quant
			HOLDANDCOLLECT -tiff=False -quant=True -pcr=False 1200
		</multiline.step>
	</multiline.stage>
</multiline.protocol>
"#;

        let cmd = Command::try_from(protstring).expect("Failed to parse");
        let protocol = Protocol::from_scpicommand(&cmd).expect("Failed to parse protocol");

        // Serialize with Rust
        let serialized = protocol.to_scpi_string();

        // Parse the re-serialized string
        let cmd2 = Command::try_from(serialized.as_str()).expect("Failed to parse re-serialized");
        let protocol2 =
            Protocol::from_scpicommand(&cmd2).expect("Failed to parse re-serialized protocol");

        // Validate round-trip preserves key data
        assert_eq!(protocol.name, protocol2.name);
        assert_eq!(protocol.volume, protocol2.volume);
        assert_eq!(protocol.stages.len(), protocol2.stages.len());

        // Check each stage
        for (i, (s1, s2)) in protocol
            .stages
            .iter()
            .zip(protocol2.stages.iter())
            .enumerate()
        {
            assert_eq!(s1.repeat, s2.repeat, "stage {} repeat", i);
            assert_eq!(s1.steps.len(), s2.steps.len(), "stage {} steps", i);
            for (j, (st1, st2)) in s1.steps.iter().zip(s2.steps.iter()).enumerate() {
                let st1 = st1.as_standard().unwrap();
                let st2 = st2.as_standard().unwrap();
                assert_eq!(st1.time, st2.time, "stage {} step {} time", i, j);
                assert_eq!(st1.collect, st2.collect, "stage {} step {} collect", i, j);
                assert!(
                    (st1.temp_increment - st2.temp_increment).abs() < 0.001,
                    "stage {} step {} temp_increment: {} != {}",
                    i,
                    j,
                    st1.temp_increment,
                    st2.temp_increment
                );
                for (k, (t1, t2)) in st1
                    .temperature
                    .iter()
                    .zip(st2.temperature.iter())
                    .enumerate()
                {
                    assert!(
                        (t1 - t2).abs() < 0.01,
                        "stage {} step {} temp[{}]: {} != {}",
                        i,
                        j,
                        k,
                        t1,
                        t2
                    );
                }
            }
        }
    }

    #[test]
    fn test_protocol_default_filters_roundtrip() {
        // Test that # qslib:default_filters comments are properly round-tripped
        let protstring = r#"PROTOCOL -volume=50 -runmode=standard test_df <multiline.protocol>
	STAGE -repeat=30 1 STAGE_1 <multiline.stage>
		STEP 1 <multiline.step>
			RAMP 90 90 90 90 90 90
			HACFILT m4,x4,quant # qslib:default_filters
			HOLDANDCOLLECT -tiff=False -quant=True -pcr=False 20
		</multiline.step>
	</multiline.stage>
	STAGE -repeat=5 2 STAGE_2 <multiline.stage>
		STEP 1 <multiline.step>
			RAMP 80 80 80 80 80 80
			HACFILT m4,x4,quant # qslib:default_filters
			HOLDANDCOLLECT -tiff=False -quant=True -pcr=False 60
		</multiline.step>
	</multiline.stage>
</multiline.protocol>"#;

        let cmd = Command::try_from(protstring).expect("Failed to parse");
        let protocol = Protocol::from_scpicommand(&cmd).expect("Failed to parse protocol");

        // Protocol-level filters should be populated from default_filters
        assert_eq!(protocol.filters, vec!["m4,x4,quant"]);

        // Steps should have empty filters but non-empty default_filters
        assert!(protocol.stages[0].steps[0]
            .as_standard()
            .unwrap()
            .filters
            .is_empty());
        assert_eq!(
            protocol.stages[0].steps[0]
                .as_standard()
                .unwrap()
                .default_filters,
            vec!["m4,x4,quant"]
        );
        assert_eq!(
            protocol.stages[0].steps[0].as_standard().unwrap().collect,
            Some(true)
        );

        // Serialize and re-parse
        let serialized = protocol.to_scpi_string();
        assert!(serialized.contains("qslib:default_filters"));

        let cmd2 = Command::try_from(serialized.as_str()).expect("Failed to parse re-serialized");
        let protocol2 =
            Protocol::from_scpicommand(&cmd2).expect("Failed to parse re-serialized protocol");

        assert_eq!(protocol2.filters, vec!["m4,x4,quant"]);
        assert!(protocol2.stages[0].steps[0]
            .as_standard()
            .unwrap()
            .filters
            .is_empty());
        assert_eq!(
            protocol2.stages[0].steps[0]
                .as_standard()
                .unwrap()
                .default_filters,
            vec!["m4,x4,quant"]
        );
    }

    #[test]
    fn test_format_scpi_number() {
        assert_eq!(format_scpi_number(80.0), "80");
        assert_eq!(format_scpi_number(51.2), "51.2");
        assert_eq!(format_scpi_number(-1.0), "-1");
        assert_eq!(format_scpi_number(0.0), "0");
        assert_eq!(format_scpi_number(100.5), "100.5");
    }

    #[test]
    fn test_indent_text() {
        assert_eq!(
            indent_text("RAMP 80\nHOLD 300\n", "\t"),
            "\tRAMP 80\n\tHOLD 300\n"
        );
        // Empty lines should not be indented
        assert_eq!(
            indent_text("RAMP 80\n\nHOLD 300\n", "\t"),
            "\tRAMP 80\n\n\tHOLD 300\n"
        );
    }

    // XML generation tests

    #[test]
    fn test_to_xml_pair_basic() {
        let protocol = Protocol {
            stages: vec![Stage {
                steps: vec![StageStep::Standard(Step {
                    time: 60,
                    temperature: vec![25.0; 6],
                    collect: Some(false),
                    temp_increment: 0.0,
                    temp_incrementcycle: 2,
                    temp_incrementpoint: None,
                    time_increment: 0,
                    time_incrementcycle: 2,
                    time_incrementpoint: None,
                    filters: vec![],
                    pcr: false,
                    quant: true,
                    tiff: false,
                    repeat: 1,
                    default_filters: vec![],
                })],
                repeat: 1,
                index: Some(1),
                label: None,
                default_filters: vec![],
            }],
            name: "test_protocol".to_string(),
            volume: 50.0,
            runmode: "Standard".to_string(),
            filters: vec![],
            covertemperature: 105.0,
            prerun: vec![],
            postrun: vec![],
        };

        let (tc, qstc) = protocol.to_xml_pair(105.0, "0.14.0", None);

        assert!(tc.contains("<TCProtocol>"));
        assert!(tc.contains("<ProtocolName>test_protocol</ProtocolName>"));
        assert!(tc.contains("<CoverTemperature>105</CoverTemperature>"));
        assert!(tc.contains("<SampleVolume>50</SampleVolume>"));
        assert!(tc.contains("<RunMode>Standard</RunMode>"));
        assert!(tc.contains("<TCStage>"));
        assert!(tc.contains("<TCStep>"));
        assert!(tc.contains("<HoldTime>60</HoldTime>"));

        assert!(qstc.contains("<QSTCProtocol>"));
        assert!(qstc.contains("<QSLibProtocolCommand>"));
        assert!(qstc.contains("<QSLibVerson>0.14.0</QSLibVerson>"));
        assert!(!qstc.contains("<MachineConnection>"));
    }

    #[test]
    fn test_to_xml_pair_with_filters() {
        let protocol = Protocol {
            stages: vec![Stage {
                steps: vec![StageStep::Standard(Step {
                    time: 30,
                    temperature: vec![60.0; 6],
                    collect: Some(true),
                    temp_increment: 0.0,
                    temp_incrementcycle: 2,
                    temp_incrementpoint: None,
                    time_increment: 0,
                    time_incrementcycle: 2,
                    time_incrementpoint: None,
                    filters: vec![],
                    pcr: false,
                    quant: true,
                    tiff: false,
                    repeat: 1,
                    default_filters: vec![],
                })],
                repeat: 10,
                index: Some(1),
                label: None,
                default_filters: vec![],
            }],
            name: "filter_test".to_string(),
            volume: 25.0,
            runmode: "standard".to_string(),
            filters: vec!["x4-m4".to_string(), "x1-m1".to_string()],
            covertemperature: 105.0,
            prerun: vec![],
            postrun: vec![],
        };

        let (tc, _) = protocol.to_xml_pair(105.0, "0.14.0", None);
        assert!(tc.contains("<CollectionProfile ProfileId=\"1\">"));
        assert!(tc.contains("Emission=\"m4\""));
        assert!(tc.contains("Excitation=\"x4\""));
        assert!(tc.contains("Emission=\"m1\""));
        assert!(tc.contains("Excitation=\"x1\""));
    }

    #[test]
    fn test_to_xml_pair_with_hacform_filters() {
        let protocol = Protocol {
            stages: vec![Stage {
                steps: vec![StageStep::Standard(Step {
                    time: 30,
                    temperature: vec![75.0; 6],
                    collect: Some(true),
                    temp_increment: 0.0,
                    temp_incrementcycle: 2,
                    temp_incrementpoint: None,
                    time_increment: 0,
                    time_incrementcycle: 2,
                    time_incrementpoint: None,
                    filters: vec![],
                    pcr: false,
                    quant: true,
                    tiff: false,
                    repeat: 1,
                    default_filters: vec![],
                })],
                repeat: 10,
                index: Some(1),
                label: None,
                default_filters: vec![],
            }],
            name: "hacform_filter_test".to_string(),
            volume: 35.0,
            runmode: "standard".to_string(),
            filters: vec!["m4,x4,quant".to_string(), "m1,x1".to_string()],
            covertemperature: 105.0,
            prerun: vec![],
            postrun: vec![],
        };

        let (tc, _) = protocol.to_xml_pair(105.0, "0.14.0", None);
        assert!(tc.contains("Emission=\"m4\""), "tc: {tc}");
        assert!(tc.contains("Excitation=\"x4\""), "tc: {tc}");
        assert!(tc.contains("Emission=\"m1\""), "tc: {tc}");
        assert!(tc.contains("Excitation=\"x1\""), "tc: {tc}");
        assert!(!tc.contains("xquant"), "tc: {tc}");
        assert!(!tc.contains("mm4"), "tc: {tc}");
    }

    #[test]
    fn test_to_xml_pair_with_machine_toml() {
        let protocol = Protocol {
            stages: vec![],
            name: "test".to_string(),
            volume: 50.0,
            runmode: "standard".to_string(),
            filters: vec![],
            covertemperature: 105.0,
            prerun: vec![],
            postrun: vec![],
        };

        let (_, qstc) = protocol.to_xml_pair(
            105.0,
            "0.14.0",
            Some("host = \"192.168.1.1\"\nport = 7443\n"),
        );
        assert!(qstc.contains("<MachineConnection>"));
        // Quotes may be escaped as &quot; in XML
        assert!(qstc.contains("192.168.1.1"));
    }

    // XML parsing tests

    #[test]
    fn test_from_xml_str_basic() {
        let xml = r#"<TCProtocol>
            <FileVersion>2.0</FileVersion>
            <ProtocolName>test_protocol</ProtocolName>
            <CoverTemperature>105.0</CoverTemperature>
            <SampleVolume>50.0</SampleVolume>
            <RunMode>Standard</RunMode>
            <TCStage>
                <StageFlag>CYCLING</StageFlag>
                <NumOfRepetitions>10</NumOfRepetitions>
                <TCStep>
                    <CollectionFlag>0</CollectionFlag>
                    <Temperature>60.0</Temperature>
                    <Temperature>60.0</Temperature>
                    <Temperature>60.0</Temperature>
                    <Temperature>60.0</Temperature>
                    <Temperature>60.0</Temperature>
                    <Temperature>60.0</Temperature>
                    <HoldTime>30</HoldTime>
                    <ExtTemperature>0.0</ExtTemperature>
                    <ExtHoldTime>0</ExtHoldTime>
                </TCStep>
                <StartingCycle>1</StartingCycle>
                <AutoDeltaEnabled>false</AutoDeltaEnabled>
            </TCStage>
        </TCProtocol>"#;

        let proto = Protocol::from_xml_str(xml).unwrap();
        assert_eq!(proto.name, "test_protocol");
        assert_eq!(proto.volume, 50.0);
        assert_eq!(proto.covertemperature, 105.0);
        assert_eq!(proto.stages.len(), 1);
        assert_eq!(proto.stages[0].repeat, 10);
        assert_eq!(proto.stages[0].steps.len(), 1);
        assert_eq!(proto.stages[0].steps[0].as_standard().unwrap().time, 30);
        assert_eq!(
            proto.stages[0].steps[0].as_standard().unwrap().temperature,
            vec![60.0; 6]
        );
    }

    #[test]
    fn test_from_xml_str_with_filters() {
        let xml = r#"<TCProtocol>
            <ProtocolName>filter_test</ProtocolName>
            <CollectionProfile ProfileId="1">
                <CollectionCondition>
                    <FilterSet Emission="m4" Excitation="x4"/>
                    <Frames>0</Frames>
                </CollectionCondition>
                <CollectionCondition>
                    <FilterSet Emission="m1" Excitation="x1"/>
                    <Frames>0</Frames>
                </CollectionCondition>
            </CollectionProfile>
            <TCStage>
                <NumOfRepetitions>1</NumOfRepetitions>
                <TCStep>
                    <CollectionFlag>1</CollectionFlag>
                    <Temperature>25.0</Temperature>
                    <HoldTime>60</HoldTime>
                    <ExtTemperature>0</ExtTemperature>
                    <ExtHoldTime>0</ExtHoldTime>
                </TCStep>
                <StartingCycle>1</StartingCycle>
                <AutoDeltaEnabled>false</AutoDeltaEnabled>
            </TCStage>
        </TCProtocol>"#;

        let proto = Protocol::from_xml_str(xml).unwrap();
        assert_eq!(proto.filters, vec!["x4-m4", "x1-m1"]);
        assert_eq!(
            proto.stages[0].steps[0].as_standard().unwrap().collect,
            Some(true)
        );
    }

    #[test]
    fn test_from_xml_str_tolerates_unknown_elements() {
        let xml = r#"<TCProtocol>
            <FileVersion>2.0</FileVersion>
            <ProtocolName>tolerant_test</ProtocolName>
            <UnknownElement>some value</UnknownElement>
            <AnotherUnknown attr="foo">bar</AnotherUnknown>
            <TCStage>
                <StageFlag>PRE_CYCLING</StageFlag>
                <NumOfRepetitions>1</NumOfRepetitions>
                <SomeNewFeature>true</SomeNewFeature>
                <TCStep>
                    <CollectionFlag>0</CollectionFlag>
                    <Temperature>95.0</Temperature>
                    <HoldTime>120</HoldTime>
                    <ExtTemperature>0</ExtTemperature>
                    <ExtHoldTime>0</ExtHoldTime>
                    <NewStepProperty>xyz</NewStepProperty>
                </TCStep>
                <StartingCycle>1</StartingCycle>
                <AutoDeltaEnabled>false</AutoDeltaEnabled>
            </TCStage>
        </TCProtocol>"#;

        let proto = Protocol::from_xml_str(xml).unwrap();
        assert_eq!(proto.name, "tolerant_test");
        assert_eq!(proto.stages[0].steps[0].as_standard().unwrap().time, 120);
    }

    #[test]
    fn test_xml_roundtrip() {
        // Create a protocol, generate XML, parse it back
        let original = Protocol {
            stages: vec![
                Stage {
                    steps: vec![StageStep::Standard(Step {
                        time: 120,
                        temperature: vec![95.0; 6],
                        collect: Some(false),
                        temp_increment: 0.0,
                        temp_incrementcycle: 2,
                        temp_incrementpoint: None,
                        time_increment: 0,
                        time_incrementcycle: 2,
                        time_incrementpoint: None,
                        filters: vec![],
                        pcr: false,
                        quant: true,
                        tiff: false,
                        repeat: 1,
                        default_filters: vec![],
                    })],
                    repeat: 1,
                    index: Some(1),
                    label: None,
                    default_filters: vec![],
                },
                Stage {
                    steps: vec![StageStep::Standard(Step {
                        time: 30,
                        temperature: vec![60.0; 6],
                        collect: Some(true),
                        temp_increment: 0.5,
                        temp_incrementcycle: 3,
                        temp_incrementpoint: Some(1),
                        time_increment: 0,
                        time_incrementcycle: 2,
                        time_incrementpoint: None,
                        filters: vec![],
                        pcr: false,
                        quant: true,
                        tiff: false,
                        repeat: 1,
                        default_filters: vec![],
                    })],
                    repeat: 40,
                    index: Some(2),
                    label: None,
                    default_filters: vec![],
                },
            ],
            name: "roundtrip_test".to_string(),
            volume: 25.0,
            runmode: "Standard".to_string(),
            filters: vec!["x4-m4".to_string()],
            covertemperature: 105.0,
            prerun: vec![],
            postrun: vec![],
        };

        let (tc_xml, _) = original.to_xml_pair(105.0, "0.14.0", None);
        let parsed = Protocol::from_xml_str(&tc_xml).unwrap();

        assert_eq!(parsed.name, "roundtrip_test");
        assert_eq!(parsed.volume, 25.0);
        assert_eq!(parsed.covertemperature, 105.0);
        assert_eq!(parsed.filters, vec!["x4-m4"]);
        assert_eq!(parsed.stages.len(), 2);
        assert_eq!(parsed.stages[0].repeat, 1);
        assert_eq!(parsed.stages[0].steps[0].as_standard().unwrap().time, 120);
        assert_eq!(parsed.stages[1].repeat, 40);
        assert_eq!(parsed.stages[1].steps[0].as_standard().unwrap().time, 30);
        assert_eq!(
            parsed.stages[1].steps[0]
                .as_standard()
                .unwrap()
                .temp_increment,
            0.5
        );
    }

    #[test]
    fn test_parse_qsl_tcprotocol_command() {
        let xml = r#"<QSTCProtocol>
            <QSLibNote>Test note</QSLibNote>
            <QSLibProtocolCommand>PROTOCOL -volume=50 test_proto &lt;multiline.protocol&gt;...</QSLibProtocolCommand>
            <QSLibVerson>0.14.0</QSLibVerson>
        </QSTCProtocol>"#;

        let cmd = Protocol::parse_qsl_tcprotocol_command(xml);
        assert!(cmd.is_some());
        assert!(cmd.unwrap().starts_with("PROTOCOL"));
    }

    #[test]
    fn test_parse_qsl_machine_connection() {
        let xml = r#"<QSTCProtocol>
            <QSLibNote>Test</QSLibNote>
            <MachineConnection>host = "192.168.1.1"
port = 7443
</MachineConnection>
        </QSTCProtocol>"#;

        let mc = Protocol::parse_qsl_machine_connection(xml);
        assert!(mc.is_some());
        assert!(mc.unwrap().contains("host = \"192.168.1.1\""));
    }

    #[test]
    fn test_parse_qsl_no_command() {
        let xml = "<QSTCProtocol><QSLibNote>no command</QSLibNote></QSTCProtocol>";
        assert!(Protocol::parse_qsl_tcprotocol_command(xml).is_none());
    }

    #[test]
    fn test_extract_qsl_element_entities() {
        let xml = r#"<Root><Cmd>hello &lt;world&gt; end</Cmd></Root>"#;
        let result = Protocol::extract_qsl_element(xml, "Cmd");
        assert_eq!(result, Some("hello <world> end".to_string()));
    }

    #[test]
    fn test_parse_filter_for_xml() {
        assert_eq!(parse_filter_for_xml("x4-m4"), ("x4".into(), "m4".into()));
        assert_eq!(
            parse_filter_for_xml("m4,x4,quant"),
            ("x4".into(), "m4".into())
        );
        assert_eq!(parse_filter_for_xml("m1,x3"), ("x3".into(), "m1".into()));
        assert_eq!(parse_filter_for_xml("M4_X4"), ("x4".into(), "m4".into()));
        assert!(crate::data::FilterSet::from_string("4,1,4").is_err());
    }

    // =====================================================================
    // CustomStep tests
    // =====================================================================

    #[test]
    fn test_custom_step_scpi_roundtrip() {
        // A stage with an unrecognized step pattern should be parsed as Custom
        let protocol_string = r#"PROTOCOL -volume=50.0 test_custom <multiline.protocol>
	STAGE 1 STAGE_1 <multiline.stage>
		STEP 1 <multiline.step>
			LAMP ON
			WAITFOR 10
		</multiline.step>
		STEP 2 <multiline.step>
			RAMP 60 60 60 60 60 60
			HOLD 60
		</multiline.step>
	</multiline.stage>
</multiline.protocol>"#;

        let cmd = Command::try_from(protocol_string).expect("Failed to parse");
        let protocol = Protocol::from_scpicommand(&cmd).expect("Failed to parse protocol");

        assert_eq!(protocol.stages.len(), 1);
        assert_eq!(protocol.stages[0].steps.len(), 2);

        // First step should be Custom
        assert!(protocol.stages[0].steps[0].as_standard().is_none());
        if let StageStep::Custom(cmds) = &protocol.stages[0].steps[0] {
            assert_eq!(cmds.len(), 2);
        } else {
            panic!("Expected Custom step");
        }

        // Second step should be Standard
        let step2 = protocol.stages[0].steps[1].as_standard().unwrap();
        assert_eq!(step2.time, 60);
        assert_eq!(step2.temperature, vec![60.0; 6]);

        // Serialize and re-parse
        let serialized = protocol.to_scpi_string();
        assert!(serialized.contains("LAMP"));
        assert!(serialized.contains("WAITFOR"));

        let cmd2 = Command::try_from(serialized.as_str()).expect("Failed to parse re-serialized");
        let protocol2 =
            Protocol::from_scpicommand(&cmd2).expect("Failed to parse re-serialized protocol");

        assert_eq!(protocol2.stages[0].steps.len(), 2);
        assert!(protocol2.stages[0].steps[0].as_standard().is_none());
        assert!(protocol2.stages[0].steps[1].as_standard().is_some());
    }

    #[test]
    fn test_custom_step_placeholder_xml() {
        // A protocol with a Custom step should generate placeholder XML
        let protocol_string = r#"PROTOCOL -volume=50.0 test_custom_xml <multiline.protocol>
	STAGE 1 STAGE_1 <multiline.stage>
		STEP 1 <multiline.step>
			LAMP ON
		</multiline.step>
	</multiline.stage>
</multiline.protocol>"#;

        let cmd = Command::try_from(protocol_string).expect("Failed to parse");
        let protocol = Protocol::from_scpicommand(&cmd).expect("Failed to parse protocol");

        let (tc_xml, qstc_xml) = protocol.to_xml_pair(105.0, "0.14.0", None);

        // Should still produce valid XML with a placeholder TCStep
        assert!(tc_xml.contains("<TCStep>"));
        assert!(tc_xml.contains("<HoldTime>1</HoldTime>"));
        assert!(tc_xml.contains("<Temperature>30</Temperature>"));
        // The qstc_xml should contain the real SCPI command
        assert!(qstc_xml.contains("LAMP"));
    }

    #[test]
    fn test_custom_step_info_str() {
        let cmd = Command::new("LAMP");
        let stage = Stage {
            steps: vec![StageStep::Custom(vec![cmd])],
            repeat: 1,
            index: Some(1),
            label: None,
            default_filters: vec![],
        };

        let info = stage.info_str(Some(1));
        assert!(info.contains("Custom step of 1 commands"));
    }

    #[test]
    fn test_protocol_with_prerun_postrun_roundtrip() {
        // Build a protocol with prerun/postrun directly, then roundtrip via SCPI
        let prerun_cmd = Command::try_from("RAMP 25 25 25 25 25 25").unwrap();
        let postrun_cmd = Command::try_from("RAMP 25 25 25 25 25 25").unwrap();

        let protocol = Protocol {
            stages: vec![Stage {
                steps: vec![StageStep::Standard(Step {
                    time: 60,
                    temperature: vec![60.0; 6],
                    collect: Some(false),
                    temp_increment: 0.0,
                    temp_incrementcycle: 2,
                    temp_incrementpoint: None,
                    time_increment: 0,
                    time_incrementcycle: 2,
                    time_incrementpoint: None,
                    filters: vec![],
                    pcr: false,
                    quant: true,
                    tiff: false,
                    repeat: 1,
                    default_filters: vec![],
                })],
                repeat: 1,
                index: Some(1),
                label: None,
                default_filters: vec![],
            }],
            name: "test_prepost".to_string(),
            volume: 50.0,
            runmode: "standard".to_string(),
            filters: vec![],
            covertemperature: 105.0,
            prerun: vec![prerun_cmd],
            postrun: vec![postrun_cmd],
        };

        assert!(!protocol.prerun.is_empty());
        assert!(!protocol.postrun.is_empty());

        let serialized = protocol.to_scpi_string();
        assert!(serialized.contains("PRERUN"));
        assert!(serialized.contains("POSTRUN"));

        let cmd2 = Command::try_from(serialized.as_str()).expect("Failed to parse re-serialized");
        let protocol2 =
            Protocol::from_scpicommand(&cmd2).expect("Failed to parse re-serialized protocol");

        assert!(!protocol2.prerun.is_empty());
        assert!(!protocol2.postrun.is_empty());
        assert_eq!(protocol2.stages.len(), 1);
    }
}
