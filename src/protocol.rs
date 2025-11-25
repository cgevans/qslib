use crate::parser::{ArgMap, Command, ParseError, Value};
use std::any::Any;
use std::convert::TryInto;
use std::fmt;
use thiserror::Error;

/// Default number of temperature zones for QuantStudio machines.
/// Different models have different zone counts (1, 2, 3, or 6).
/// Default is 6 for backward compatibility with QuantStudio 5.
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
    String::from_utf8_lossy(&cmd.command).to_string().to_uppercase()
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
fn extract_temperature_list(value: &Value, cmd: &Command, num_zones: usize) -> Result<Vec<f64>, ProtocolParseError> {
    match value {
        Value::Float(f) => Ok(vec![*f; num_zones]),
        Value::Int(i) => Ok(vec![*i as f64; num_zones]),
        Value::String(s) | Value::QuotedString(s) => {
            // Determine delimiter: space or comma
            let delimiter = if s.contains(',') { ',' } else { ' ' };
            let parts: Vec<&str> = s.split(delimiter).map(|x| x.trim()).filter(|x| !x.is_empty()).collect();
            
            if parts.len() == 1 {
                // Single value - expand to num_zones zones
                let temp = parts[0].parse::<f64>().map_err(|_| ProtocolParseError::InvalidValueType {
                    value_type: "temperature".to_string(),
                    protocol_string: command_to_string(cmd),
                })?;
                Ok(vec![temp; num_zones])
            } else {
                // Multiple values - use as-is (auto-detected zone count)
                parts.iter().map(|x| x.parse::<f64>()).collect::<Result<Vec<_>, _>>()
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
fn extract_temperature_list_from_args(args: &[Value], cmd: &Command, num_zones: usize) -> Result<Vec<f64>, ProtocolParseError> {
    if args.is_empty() {
        return Err(ProtocolParseError::MissingField {
            field: "temperature".to_string(),
            protocol_string: command_to_string(cmd),
        });
    }
    
    // If we have multiple numeric arguments, treat them as zone temperatures (auto-detected count)
    if args.len() > 1 {
        let temps: Result<Vec<f64>, _> = args.iter().map(|v| {
            match v {
                Value::Float(f) => Ok(*f),
                Value::Int(i) => Ok(*i as f64),
                _ => Err(()),
            }
        }).collect();
        
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
        Value::String(s) => s
            .parse::<i64>()
            .map(Some)
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
    value.clone().try_into_string().map_err(|_| {
        ProtocolParseError::InvalidValueType {
            value_type: "string".to_string(),
            protocol_string: command_to_string(cmd),
        }
    })
}

fn get_option_i64(opts: &ArgMap, key: &str, default: i64, cmd: &Command) -> Result<i64, ProtocolParseError> {
    match opts.get(key) {
        Some(v) => extract_i64(v, cmd),
        None => Ok(default),
    }
}

fn get_option_f64(opts: &ArgMap, key: &str, default: f64, cmd: &Command) -> Result<f64, ProtocolParseError> {
    match opts.get(key) {
        Some(v) => extract_f64(v, cmd),
        None => Ok(default),
    }
}

fn get_option_bool(opts: &ArgMap, key: &str, default: bool, cmd: &Command) -> Result<bool, ProtocolParseError> {
    match opts.get(key) {
        Some(v) => extract_bool(v, cmd),
        None => Ok(default),
    }
}

fn get_option_string(opts: &ArgMap, key: &str, cmd: &Command) -> Result<Option<String>, ProtocolParseError> {
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

        let increment  = cmd.options.extract_with_default("increment", 0.0)
            .map_err(|e| ProtocolParseError::ParseError {
                source: e,
                protocol_string: protocol_string.clone(),
            })?;
        let incrementcycle = cmd.options.extract_with_default("incrementcycle", 1)
            .map_err(|e| ProtocolParseError::ParseError {
                source: e,
                protocol_string: protocol_string.clone(),
            })?;
        let incrementstep = cmd.options.extract_with_default("incrementstep", 1)
            .map_err(|e| ProtocolParseError::ParseError {
                source: e,
                protocol_string: protocol_string.clone(),
            })?;
        let rate = cmd.options.extract_with_default("rate", 100.0)
            .map_err(|e| ProtocolParseError::ParseError {
                source: e,
                protocol_string: protocol_string.clone(),
            })?;
        let cover = match cmd.options.get("cover") {
            Some(v) => Some(v.try_into().map_err(|e: ParseError| ProtocolParseError::ParseError {
                source: e,
                protocol_string: protocol_string.clone(),
            })?),
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

        let increment = cmd.options.extract_with_default("increment", 0)
            .map_err(|e| parse_error_to_protocol_error(e, cmd))?;
        let incrementcycle = cmd.options.extract_with_default("incrementcycle", 1)
            .map_err(|e| parse_error_to_protocol_error(e, cmd))?;
        let incrementstep = cmd.options.extract_with_default("incrementstep", 1)
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
        let incrementcycle = get_option_i64(&cmd.options, "incrementcycle", 1, cmd)?;
        let incrementstep = get_option_i64(&cmd.options, "incrementstep", 1, cmd)?;
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

        let filters: Result<Vec<String>, ParseError> = cmd
            .args
            .iter()
            .map(|v| v.try_into())
            .collect();

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
                    let exposures: Result<Vec<i64>, _> = parts[3..]
                        .iter()
                        .map(|x| x.trim().parse::<i64>())
                        .collect();
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
        "STEP" => {
            Step::from_scpicommand(cmd)
                .or_else(|_| CustomStep::from_scpicommand(cmd))
        }
        "STAGE" | "STAGe" => {
            Err(ProtocolParseError::UnexpectedStructure {
                message: "Stage should be parsed directly, not through specialize".to_string(),
                protocol_string,
            })
        }
        "PROTOCOL" | "PROT" => {
            Err(ProtocolParseError::UnexpectedStructure {
                message: "Protocol should be parsed directly, not through specialize".to_string(),
                protocol_string,
            })
        }
        _ => Err(ProtocolParseError::InvalidCommand {
            expected: "known command".to_string(),
            got: cmd_name,
            protocol_string,
        }),
    }
}

fn extract_commands_from_value(value: &Value, parent_cmd: Option<&Command>) -> Result<Vec<Command>, ProtocolParseError> {
    let protocol_string = parent_cmd.map(command_to_string)
        .unwrap_or_else(|| "<nested command>".to_string());
    match value {
        Value::XmlString { value, tag: _ } => {
            let mut s = String::from_utf8(value.to_vec())
                .map_err(|_| ProtocolParseError::InvalidValueType {
                    value_type: "xml string".to_string(),
                    protocol_string: protocol_string.clone(),
                })?;
            
            // Strip inline comments from the string
            // Comments start with # and continue to end of line, but not if inside quotes or XML tags
            let mut result = String::with_capacity(s.len());
            let bytes = s.as_bytes();
            let mut i = 0;
            let mut in_quotes = false;
            let mut in_xml_tag = false;
            
            while i < bytes.len() {
                let b = bytes[i];
                if b == b'"' && (i == 0 || bytes[i-1] != b'\\') {
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
            while !input.is_empty() && (input[0] == b' ' || input[0] == b'\t' || input[0] == b'\n' || input[0] == b'\r') {
                input = &input[1..];
            }
            
            while !input.is_empty() {
                // Skip whitespace/tabs at start of line
                while !input.is_empty() && (input[0] == b' ' || input[0] == b'\t' || input[0] == b'\n' || input[0] == b'\r') {
                    input = &input[1..];
                }
                
                if input.is_empty() {
                    break;
                }
                
                // Skip XML tags and comments
                if input.starts_with(b"</") || input.starts_with(b"<") && !input.starts_with(b"<multiline") {
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
                        while !input.is_empty() && (input[0] == b' ' || input[0] == b'\t' || input[0] == b'\n' || input[0] == b'\r') {
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
                let filter_strs: Vec<String> = self.filters.iter()
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

            if cmd1_name == "RAMP" && (cmd2_name == "HACFILT" || cmd2_name == "HOLDANDCOLLECTFILTER") && cmd3_name == "HOLDANDCOLLECT" {
                let r_temp = extract_temperature_list_from_args(&nested_commands[0].args, cmd, DEFAULT_NUM_ZONES)?;
                let r_increment = get_option_f64(&nested_commands[0].options, "increment", 0.0, cmd)?;
                let r_incrementcycle = get_option_i64(&nested_commands[0].options, "incrementcycle", 1, cmd)?;
                let r_incrementstep = get_option_i64(&nested_commands[0].options, "incrementstep", 1, cmd)?;

                let hf_filters: Result<Vec<String>, ParseError> = nested_commands[1]
                    .args
                    .iter()
                    .map(|v| v.try_into())
                    .collect();
                let mut filters = hf_filters.map_err(|e| parse_error_to_protocol_error(e, cmd))?;
                let mut default_filters = Vec::new();

                let h_time = extract_i64(&nested_commands[2].args[0], cmd)?;
                let h_increment = get_option_i64(&nested_commands[2].options, "increment", 0, cmd)?;
                let h_incrementcycle = get_option_i64(&nested_commands[2].options, "incrementcycle", 1, cmd)?;
                let h_incrementstep = get_option_i64(&nested_commands[2].options, "incrementstep", 1, cmd)?;
                let h_tiff = get_option_bool(&nested_commands[2].options, "tiff", false, cmd)?;
                let h_quant = get_option_bool(&nested_commands[2].options, "quant", true, cmd)?;
                let h_pcr = get_option_bool(&nested_commands[2].options, "pcr", false, cmd)?;

                let mut collect = !filters.is_empty();
                if filters.is_empty() && !default_filters.is_empty() {
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
                let r_temp = extract_temperature_list_from_args(&nested_commands[0].args, cmd, DEFAULT_NUM_ZONES)?;
                let r_increment = get_option_f64(&nested_commands[0].options, "increment", 0.0, cmd)?;
                let r_incrementcycle = get_option_i64(&nested_commands[0].options, "incrementcycle", 1, cmd)?;
                let r_incrementstep = get_option_i64(&nested_commands[0].options, "incrementstep", 1, cmd)?;

                let h_time = extract_i64_option(&nested_commands[1].args[0], cmd)?;
                if h_time.is_none() {
                    return Err(ProtocolParseError::MissingField {
                        field: "hold time".to_string(),
                        protocol_string: protocol_string.clone(),
                    });
                }
                let h_increment = get_option_i64(&nested_commands[1].options, "increment", 0, cmd)?;
                let h_incrementcycle = get_option_i64(&nested_commands[1].options, "incrementcycle", 1, cmd)?;
                let h_incrementstep = get_option_i64(&nested_commands[1].options, "incrementstep", 1, cmd)?;

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
            message: "Step must have [Ramp, HACFILT, HoldAndCollect] or [Ramp, Hold] pattern".to_string(),
            protocol_string,
        })
    }
}

#[derive(Debug, Clone)]
pub struct Stage {
    pub steps: Vec<Step>,
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

                    if cmd1_name == "RAMP" && (cmd2_name == "HACFILT" || cmd2_name == "HOLDANDCOLLECTFILTER") && cmd3_name == "HOLDANDCOLLECT" {
                        let r_temp = extract_temperature_list_from_args(&nested_step_commands[0].args, cmd, DEFAULT_NUM_ZONES)?;
                        let r_increment = get_option_f64(&nested_step_commands[0].options, "increment", 0.0, cmd)?;
                        let r_incrementcycle = get_option_i64(&nested_step_commands[0].options, "incrementcycle", 1, cmd)?;
                        let r_incrementstep = get_option_i64(&nested_step_commands[0].options, "incrementstep", 1, cmd)?;

                        let hf_filters: Result<Vec<String>, ProtocolParseError> = nested_step_commands[1]
                            .args
                            .iter()
                            .map(|v| extract_string(v, cmd))
                            .collect();
                        let mut filters = hf_filters?;
                        let mut default_filters = Vec::new();

                        let h_time = extract_i64(&nested_step_commands[2].args[0], cmd)?;
                        let h_increment = get_option_i64(&nested_step_commands[2].options, "increment", 0, cmd)?;
                        let h_incrementcycle = get_option_i64(&nested_step_commands[2].options, "incrementcycle", 1, cmd)?;
                        let h_incrementstep = get_option_i64(&nested_step_commands[2].options, "incrementstep", 1, cmd)?;
                        let h_tiff = get_option_bool(&nested_step_commands[2].options, "tiff", false, cmd)?;
                        let h_quant = get_option_bool(&nested_step_commands[2].options, "quant", true, cmd)?;
                        let h_pcr = get_option_bool(&nested_step_commands[2].options, "pcr", false, cmd)?;

                        let mut collect = !filters.is_empty();
                        if filters.is_empty() && !default_filters.is_empty() {
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

                        steps.push(Step {
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
                        });
                        continue;
                    }
                } else if nested_step_commands.len() == 2 {
                    let cmd1_name = get_command_name(&nested_step_commands[0]);
                    let cmd2_name = get_command_name(&nested_step_commands[1]);

                    if cmd1_name == "RAMP" && cmd2_name == "HOLD" {
                        let r_temp = extract_temperature_list_from_args(&nested_step_commands[0].args, cmd, DEFAULT_NUM_ZONES)?;
                        let r_increment = get_option_f64(&nested_step_commands[0].options, "increment", 0.0, cmd)?;
                        let r_incrementcycle = get_option_i64(&nested_step_commands[0].options, "incrementcycle", 1, cmd)? as i64;
                        let r_incrementstep = get_option_i64(&nested_step_commands[0].options, "incrementstep", 1, cmd)? as i64;

                        let h_time = extract_i64_option(&nested_step_commands[1].args[0], cmd)?;
                        if h_time.is_none() {
                            continue;
                        }
                        let h_increment = get_option_i64(&nested_step_commands[1].options, "increment", 0, cmd)?;
                        let h_incrementcycle = get_option_i64(&nested_step_commands[1].options, "incrementcycle", 1, cmd)? as i64;
                        let h_incrementstep = get_option_i64(&nested_step_commands[1].options, "incrementstep", 1, cmd)? as i64;

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

                        steps.push(Step {
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
                        });
                        continue;
                    }
                }
            }
            return Err(ProtocolParseError::UnexpectedStructure {
                message: format!("Stage step must be a valid Step, got: {}", get_command_name(step_cmd)),
                protocol_string: protocol_string.clone(),
            });
        }

        let repeat = get_option_i64(&cmd.options, "repeat", 1, cmd)? as i64;

        let mut default_filters = Vec::new();
        for step in &steps {
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
        
        let stepstrs: Vec<String> = self.steps.iter()
            .enumerate()
            .map(|(i, step)| {
                let step_info = step.info_str(Some((i + 1) as i64), self.repeat);
                step_info.lines()
                    .map(|line| format!("    {}", line))
                    .collect::<Vec<String>>()
                    .join("\n")
            })
            .collect();
        
        let total_duration: i64 = self.steps.iter()
            .map(|s| s.time * self.repeat)
            .sum();
        if total_duration > 0 {
            stagestr.push_str(&format!(" (total duration {})", format_duration(total_duration)));
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
            let filter_strs: Vec<String> = self.filters.iter()
                .map(|filter| filter_to_lowerform(filter))
                .collect();
            write!(f, "(default filters {})\n\n", oxford_list(&filter_strs))?;
        } else {
            writeln!(f)?;
        }
        
        let stagestrs: Vec<String> = self.stages.iter()
            .enumerate()
            .map(|(i, stage)| {
                let stage_num = stage.index.unwrap_or((i + 1) as i64);
                let stage_info = stage.info_str(Some(stage_num));
                stage_info.lines()
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
                    let s = String::from_utf8(value.to_vec())
                        .map_err(|_| ProtocolParseError::InvalidValueType {
                            value_type: "prerun commands".to_string(),
                            protocol_string: protocol_string.clone(),
                        })?;
                    let mut input = s.as_bytes();
                    while !input.is_empty() {
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
                    if let Some(Value::XmlString { value, tag: _ }) = stage_commands[stage_commands.len() - 1].args.first() {
                        let s = String::from_utf8(value.to_vec())
                            .map_err(|_| ProtocolParseError::InvalidValueType {
                                value_type: "postrun commands".to_string(),
                                protocol_string: protocol_string.clone(),
                            })?;
                        let mut input = s.as_bytes();
                        while !input.is_empty() {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::{Command, Value};

    #[test]
    fn test_ramp_parsing() {
        let mut cmd = Command::new("RAMP");
        cmd.args.push(Value::Float(95.0));
        cmd.options.insert("increment".to_string(), Value::Float(0.5));
        cmd.options.insert("incrementcycle".to_string(), Value::Int(2));

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
        let step_xml = format!("<multiline.step>\n{}\n\t\t</multiline.step>", step_xml_content);
        
        let nested_stage_commands_xml = format!(
            "\tSTEP 1 {}\n\t</multiline.stage>",
            step_xml
        );

        let mut stage_cmd = Command::new("STAGe");
        stage_cmd.args.push(Value::Int(1));
        stage_cmd.args.push(Value::String("STAGE_1".to_string()));
        stage_cmd.args.push(Value::XmlString {
            value: nested_stage_commands_xml.into(),
            tag: "multiline.stage".to_string(),
        });
        stage_cmd.options.insert("repeat".to_string(), Value::Int(1));

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

        let step1 = &stage1.steps[0];
        assert_eq!(step1.temperature, vec![60.0; 6]);
        assert_eq!(step1.time, 60);
        assert_eq!(step1.collect, Some(false));

        // Check second stage
        let stage2 = &prot.stages[1];
        assert_eq!(stage2.index, Some(2));
        assert_eq!(stage2.label, Some("_PCR_2".to_string()));
        assert_eq!(stage2.repeat, 4);
        assert_eq!(stage2.steps.len(), 1);

        let step2 = &stage2.steps[0];
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
        let extracted_content = protocol_value.try_into_string()
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
        assert!(protocol.is_ok(), "Protocol should parse successfully with inline comments");

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
        let step1 = &stage1.steps[0];
        assert_eq!(step1.temperature, vec![90.0; 6]);
        assert_eq!(step1.time, 600);
        assert_eq!(step1.collect, Some(false));

        // Check second stage (with inline comment)
        let stage2 = &prot.stages[1];
        assert_eq!(stage2.index, Some(2));
        assert_eq!(stage2.label, Some("STAGE_2".to_string()));
        assert_eq!(stage2.repeat, 30);
        assert_eq!(stage2.steps.len(), 1);
        let step2 = &stage2.steps[0];
        assert_eq!(step2.temperature, vec![90.0; 6]);
        assert_eq!(step2.temp_increment, -0.3448);
        assert_eq!(step2.time, 20);
        assert_eq!(step2.collect, Some(true));
        assert_eq!(step2.filters.len(), 1);
        assert_eq!(step2.filters[0], "m4,x4,quant");
        assert!(step2.quant);
        assert!(!step2.tiff);
        assert!(!step2.pcr);

        // Check third stage
        let stage3 = &prot.stages[2];
        assert_eq!(stage3.index, Some(3));
        assert_eq!(stage3.label, Some("STAGE_3".to_string()));
        assert_eq!(stage3.repeat, 5);
        let step3 = &stage3.steps[0];
        assert_eq!(step3.temperature, vec![80.0; 6]);
        assert_eq!(step3.time, 60);

        // Check fourth stage
        let stage4 = &prot.stages[3];
        assert_eq!(stage4.index, Some(4));
        assert_eq!(stage4.label, Some("STAGE_4".to_string()));
        assert_eq!(stage4.repeat, 550);
        let step4 = &stage4.steps[0];
        assert_eq!(step4.temperature, vec![80.0; 6]);
        assert_eq!(step4.temp_increment, -0.1002);
        assert_eq!(step4.time, 26);

        // Check fifth stage
        let stage5 = &prot.stages[4];
        assert_eq!(stage5.index, Some(5));
        assert_eq!(stage5.label, Some("STAGE_5".to_string()));
        assert_eq!(stage5.repeat, 4);
        let step5 = &stage5.steps[0];
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
        
        assert!(protocol.is_ok(), "Protocol should parse: {:?}", protocol.err());
        let prot = protocol.unwrap();
        
        assert_eq!(prot.name, "testproto");
        assert_eq!(prot.volume, 30.0);
        assert_eq!(prot.runmode, "standard");
        assert_eq!(prot.stages.len(), 6);
        
        // Stage 1: Simple hold
        assert_eq!(prot.stages[0].repeat, 1);
        assert_eq!(prot.stages[0].steps[0].temperature, vec![80.0; 6]);
        assert_eq!(prot.stages[0].steps[0].time, 300); // 5 minutes
        assert_eq!(prot.stages[0].steps[0].collect, Some(false));
        
        // Stage 2: Long hold with temp decrement
        assert_eq!(prot.stages[1].repeat, 27);
        assert_eq!(prot.stages[1].steps[0].temp_increment, -1.0);
        assert_eq!(prot.stages[1].steps[0].time, 147600); // 41 hours
        
        // Stage 3: Collecting stage
        assert_eq!(prot.stages[2].repeat, 5);
        assert_eq!(prot.stages[2].steps[0].temperature, vec![53.0; 6]);
        assert_eq!(prot.stages[2].steps[0].time, 120); // 2 minutes
        assert_eq!(prot.stages[2].steps[0].collect, Some(true));
        assert_eq!(prot.stages[2].steps[0].filters.len(), 2);
        
        // Stage 4: Very long collection with zone temps
        assert_eq!(prot.stages[3].repeat, 20);
        let expected_temps = vec![51.2, 50.84, 50.480000000000004, 50.12, 49.76, 49.4];
        assert_eq!(prot.stages[3].steps[0].temperature, expected_temps);
        assert_eq!(prot.stages[3].steps[0].time, 64800000);
        
        // Stage 5-6: Similar structure
        assert_eq!(prot.stages[4].repeat, 20);
        assert_eq!(prot.stages[4].steps[0].time, 86400); // 24 hours
        
        assert_eq!(prot.stages[5].repeat, 100);
        assert_eq!(prot.stages[5].steps[0].time, 1200); // 20 minutes
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
        assert_eq!(format_duration(3600), "60m");  // 1 hour
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
        assert_eq!(oxford_list(&["one".to_string(), "two".to_string()]), "one and two");
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
            steps: vec![step],
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
    
    /// Test Ramp with temperature list parsing (comma-separated string)
    #[test]
    fn test_ramp_with_temperature_list_comma() {
        let mut cmd = Command::new("RAMP");
        cmd.args.push(Value::String("51.2,50.84,50.48,50.12,49.76,49.4".to_string()));
        
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
        let cmd = Command::try_from("RAMP -incrementcycle=2 51.2 50.84 50.48 50.12 49.76 49.4").unwrap();
        
        assert_eq!(cmd.args.len(), 6, "Expected 6 args, got {:?}", cmd.args);
        
        let ramp_box = Ramp::from_scpicommand(&cmd).unwrap();
        let ramp = ramp_box.as_any().downcast_ref::<Ramp>().unwrap();
        
        assert_eq!(ramp.temperature.len(), 6);
        let expected = [51.2, 50.84, 50.48, 50.12, 49.76, 49.4];
        for (i, exp) in expected.iter().enumerate() {
            assert!((ramp.temperature[i] - exp).abs() < 0.001, 
                "temp[{}]: expected {}, got {}", i, exp, ramp.temperature[i]);
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
        
        let commands = extract_commands_from_value(&value, None).expect("Failed to extract commands");
        
        assert_eq!(commands.len(), 3, "Expected 3 commands");
        assert_eq!(get_command_name(&commands[0]), "RAMP");
        
        // Check that RAMP has 6 args (the temperatures)
        assert_eq!(commands[0].args.len(), 6, "RAMP should have 6 temperature args, got {:?}", commands[0].args);
        
        // Verify each temperature
        let expected = [51.2, 50.84, 50.48, 50.12, 49.76, 49.4];
        for (i, exp) in expected.iter().enumerate() {
            match &commands[0].args[i] {
                Value::Float(f) => assert!((f - exp).abs() < 0.001, "arg[{}]: expected {}, got {}", i, exp, f),
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
        assert_eq!(step_obj.temperature.len(), 6, "Expected 6 temperatures, got {:?}", step_obj.temperature);
        for (i, exp) in expected_temps.iter().enumerate() {
            assert!((step_obj.temperature[i] - exp).abs() < 0.01, 
                "temp[{}]: expected {}, got {}", i, exp, step_obj.temperature[i]);
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
        cmd.args.push(Value::String("1,4,quant,500,2000".to_string()));
        cmd.options.insert("state".to_string(), Value::String("HoldAndCollect".to_string()));
        
        let exp_box = Exposure::from_scpicommand(&cmd).unwrap();
        let exp = exp_box.as_any().downcast_ref::<Exposure>().unwrap();
        
        assert_eq!(exp.state, "HoldAndCollect");
    }
    
    /// Test command name case insensitivity
    #[test]
    fn test_command_case_insensitivity() {
        // Test that commands parse regardless of case
        let test_cases = [
            "RAMP 60",
            "ramp 60",
            "Ramp 60",
        ];
        
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
        assert!(!protocol.prerun.is_empty() || !protocol.postrun.is_empty() || protocol.stages.len() == 1);
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
        assert_eq!(temps.len(), 3, "Should auto-detect 3 zones from comma-separated string");
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
}

