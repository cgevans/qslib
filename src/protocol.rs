use crate::parser::{ArgMap, Command, ParseError, Value};
use std::any::Any;
use std::convert::TryInto;
use std::fmt;
use thiserror::Error;

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

fn extract_temperature_list(value: &Value, cmd: &Command) -> Result<Vec<f64>, ProtocolParseError> {
    match value {
        Value::Float(f) => Ok(vec![*f; 6]),
        Value::Int(i) => Ok(vec![*i as f64; 6]),
        Value::String(s) => {
            let temps: Result<Vec<f64>, _> = s
                .split(',')
                .map(|x| x.trim().parse::<f64>())
                .collect();
            temps.map_err(|_| ProtocolParseError::InvalidValueType {
                value_type: "temperature".to_string(),
                protocol_string: command_to_string(cmd),
            })
        }
        _ => Err(ProtocolParseError::InvalidValueType {
            value_type: "temperature".to_string(),
            protocol_string: command_to_string(cmd),
        }),
    }
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

        let temperature = if cmd.args.is_empty() {
            return Err(ProtocolParseError::MissingField {
                field: "temperature".to_string(),
                protocol_string,
            });
        } else {
            extract_temperature_list(&cmd.args[0], cmd)?
        };

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
                let r_temp = extract_temperature_list(&nested_commands[0].args[0], cmd)?;
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
                let r_temp = extract_temperature_list(&nested_commands[0].args[0], cmd)?;
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
                        let r_temp = extract_temperature_list(&nested_step_commands[0].args[0], cmd)?;
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
                        let r_temp = extract_temperature_list(&nested_step_commands[0].args[0], cmd)?;
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
        if !self.runmode.is_empty() && self.runmode != "standard" {
            extras.push(format!("run mode {}", self.runmode));
        }
        if !extras.is_empty() {
            write!(f, " with {}", extras.join(", "))?;
        }
        writeln!(f, ":")?;
        
        if !self.filters.is_empty() {
            write!(f, "(default filters {})\n\n", self.filters.join(", "))?;
        } else {
            writeln!(f)?;
        }
        
        for (i, stage) in self.stages.iter().enumerate() {
            let stage_num = stage.index.unwrap_or((i + 1) as i64);
            let cycles = if stage.repeat > 1 {
                format!("{} cycles", stage.repeat)
            } else {
                "1 cycle".to_string()
            };
            write!(f, "  {}. Stage with {}:", stage_num, cycles)?;
            if let Some(ref label) = stage.label {
                write!(f, " {}", label)?;
            }
            writeln!(f)?;
            
            for (j, step) in stage.steps.iter().enumerate() {
                write!(f, "    Step {}: ", j + 1)?;
                if !step.temperature.is_empty() {
                    let temps: Vec<String> = step.temperature.iter().map(|t| format!("{:.1}", t)).collect();
                    write!(f, "RAMP to [{}]", temps.join(", "))?;
                }
                if step.time > 0 {
                    write!(f, " HOLD {}s", step.time)?;
                }
                if step.collect == Some(true) {
                    write!(f, " COLLECT")?;
                }
                writeln!(f)?;
            }
        }
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
}

