use core::fmt;
use indexmap::IndexMap;
use std::fmt::Display;
use std::io::Write;
use thiserror::Error;
use winnow::combinator::seq;
use winnow::{
    ascii::{alphanumeric1, digit1, newline, space0, space1},
    combinator::{alt, delimited},
    error::{ErrMode, ParserError, StrContext},
    prelude::*,
    token::{literal, take_till, take_until, take_while},
};

#[derive(PartialEq, Clone)]
pub enum Value {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    QuotedString(String),
    XmlString { value: String, tag: String },
    XmlBinary { value: Vec<u8>, tag: String },
}

impl std::fmt::Debug for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::String(s) => write!(f, "String({})", s),
            Value::Int(i) => write!(f, "Int({})", i),
            Value::Float(float) => write!(f, "Float({})", float),
            Value::Bool(b) => write!(f, "Bool({})", b),
            Value::QuotedString(s) => write!(f, "QuotedString({})", s),
            Value::XmlString { value, tag } => {
                if value.len() > 20 {
                    write!(f, "XmlString({}, '{}...' len={})", tag, &value[..20], value.len())
                } else {
                    write!(f, "XmlString({}, '{}')", tag, value)
                }
            },
            Value::XmlBinary { value, tag } => {
                if value.len() > 20 {
                    write!(f, "XmlBinary({}, {:?}... len={})", tag, &value[..20], value.len())
                } else {
                    write!(f, "XmlBinary({}, {:?})", tag, value)
                }
            },
        }
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::String(s) => write!(f, "{}", s),
            Value::Int(i) => write!(f, "{}", i),
            Value::Float(float) => write!(f, "{}", float),
            Value::Bool(b) => write!(f, "{}", b),
            Value::QuotedString(s) => write!(f, "{}", s),
            Value::XmlString { value, tag: _ } => write!(f, "{}", value),
            Value::XmlBinary { value, tag: _ } => write!(f, "{:?}", value),
        }
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        if s.contains('\n') {
            Value::XmlString {
                value: s.to_string(),
                tag: "quote".to_string(),
            }
        } else if s.contains(' ') {
            Value::QuotedString(s.to_string())
        } else {
            Value::String(s.to_string())
        }
    }
}

impl From<i64> for Value {
    fn from(i: i64) -> Self {
        Value::Int(i)
    }
}

impl From<f64> for Value {
    fn from(f: f64) -> Self {
        Value::Float(f)
    }
}

impl From<bool> for Value {
    fn from(b: bool) -> Self {
        Value::Bool(b)
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        if s.contains('\n') {
            Value::XmlString {
                value: s,
                tag: "quote".to_string(),
            }
        } else if s.contains(' ') {
            Value::QuotedString(s)
        } else {
            Value::String(s)
        }
    }
}

impl Value {
    pub fn parse(input: &mut &[u8]) -> ModalResult<Value> {
        let v = alt((
            xml_delimited
                .map(|(tag, val)| {
                    let tag_str = String::from_utf8_lossy(tag).to_string();
                    match String::from_utf8(val.to_vec()) {
                        Ok(str_val) => Value::XmlString {
                            value: str_val,
                            tag: tag_str
                        },
                        Err(_) => Value::XmlBinary {
                            value: val.to_vec(),
                            tag: tag_str
                        }
                    }
                })
                .context(StrContext::Label("xml")),
            // Handle quoted strings
            delimited(
                literal(b'"'),
                take_till(0.., |c: u8| c == b'"'),
                literal(b'"'),
            )
            .map(|val| Value::QuotedString(String::from_utf8_lossy(val).to_string()))
            .context(StrContext::Label("quoted")),
            take_till(0.., |c: u8| c == b' ' || c == b'\n')
                .map(|val| Value::String(String::from_utf8_lossy(val).to_string()))
                .context(StrContext::Label("value")),
        ))
        .parse_next(input)?;
        if v == Value::String(String::from("")) {
            return Err(ErrMode::from_input(input));
        };
        Ok(v)
    }

    pub fn write_bytes(&self, bytes: &mut impl Write) -> Result<(), std::io::Error> {
        match self {
            Value::String(str) => bytes.write_all(str.as_bytes()),
            Value::Int(num) => bytes.write_all(num.to_string().as_bytes()),
            Value::Float(f) => bytes.write_all(f.to_string().as_bytes()),
            Value::Bool(b) => bytes.write_all(b.to_string().as_bytes()),
            Value::QuotedString(str) => bytes.write_all(format!("\"{}\"", str).as_bytes()),
            Value::XmlString { value, tag } => {
                bytes.write_all(format!("<{}>{}</{}>", tag, value, tag).as_bytes())
            }
            Value::XmlBinary { value, tag } => {
                bytes.write_all(format!("<{}>", tag).as_bytes())?;
                bytes.write_all(&value)?;
                bytes.write_all(format!("</{}>", tag).as_bytes())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MessageIdent {
    Number(u32),
    String(String),
}

impl MessageIdent {
    pub fn parse(input: &mut &[u8]) -> ModalResult<MessageIdent> {
        let r = alt((
            digit1.map(|val| {
                MessageIdent::Number(
                    String::from_utf8_lossy(val)
                        .to_string()
                        .parse::<u32>()
                        .unwrap(),
                )
            }),
            Command::parse
                .take()
                .map(|val| MessageIdent::String(String::from_utf8_lossy(val).to_string())),
        ))
        .parse_next(input)?;
        Ok(r)
    }

    pub fn write_bytes(&self, bytes: &mut impl Write) -> Result<(), std::io::Error> {
        match self {
            MessageIdent::Number(num) => bytes.write_all(num.to_string().as_bytes()),
            MessageIdent::String(str) => bytes.write_all(str.as_bytes()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Message {
    pub ident: Option<MessageIdent>,
    pub content: Vec<u8>,
}

impl Message {
    pub fn write_bytes(&self, bytes: &mut impl Write) -> Result<(), std::io::Error> {
        if let Some(ident) = &self.ident {
            ident.write_bytes(bytes)?;
            bytes.write_all(b" ")?;
        }
        bytes.write_all(&self.content)?;
        bytes.write_all(b"\n")?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct Command {
    pub command: Vec<u8>,
    pub options: IndexMap<String, Value>,
    pub args: Vec<Value>,
}

impl CommandBuilder for Command {
    const COMMAND: &'static [u8] = b"";
    type Response = OkResponse;

    fn args(&self) -> Option<Vec<Value>> {
        Some(self.args.clone())
    }

    fn options(&self) -> Option<IndexMap<String, Value>> {
        Some(self.options.clone())
    }

    fn write_command(&self, bytes: &mut impl Write) -> Result<(), QSConnectionError> {
        self.write_bytes(bytes).map_err(|e| QSConnectionError::IOError(e))
    }
    
}

impl TryFrom<&str> for Command {
    type Error = ParseError;
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let mut input = s.as_bytes();
        let c = Command::parse(&mut input).map_err(|e| ParseError::ParseError(e.to_string()))?;
        // FIXME: check if input is empty
        Ok(c)
    }
}

impl TryFrom<Vec<u8>> for Command {
    type Error = ParseError;
    fn try_from(s: Vec<u8>) -> Result<Self, Self::Error> {
        let mut input = &s[..];
        let c = Command::parse(&mut input).map_err(|e| ParseError::ParseError(e.to_string()))?;
        Ok(c)
    }
}

impl TryFrom<String> for Command {
    type Error = ParseError;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        let mut input = s.as_bytes();
        let c = Command::parse(&mut input).map_err(|e| ParseError::ParseError(e.to_string()))?;
        Ok(c)
    }
}
pub fn parse_option<'s>(input: &mut &'s [u8]) -> ModalResult<(String, Value)> {
    seq!(
        _: literal(b'-'),
        alphanumeric1.map(|val| String::from_utf8_lossy(val).to_string()),
        _: literal(b'='),
        Value::parse
    )
    .parse_next(input)
}

impl Command {
    pub fn new(command: &str) -> Self {
        Command {
            command: command.as_bytes().to_vec(),
            options: IndexMap::new(),
            args: Vec::new(),
        }
    }

    pub fn bytes(s: &[u8]) -> Self {
        Command {
            command: s.to_vec(),
            options: IndexMap::new(),
            args: Vec::new(),
        }
    }

    pub fn with_option(mut self, key: &str, value: impl Into<Value>) -> Self {
        self.options.insert(key.to_string(), value.into());
        self
    }

    pub fn with_arg(mut self, arg: impl Into<Value>) -> Self {
        self.args.push(arg.into());
        self
    }

    pub fn parse(input: &mut &[u8]) -> ModalResult<Command> {
        let comm = take_while(1.., |c: u8| {
            c.is_ascii_alphanumeric() || c == b'.' || c == b':' || c == b'?' || c == b'*'
        })
        .context(StrContext::Label("command"))
        .parse_next(input)?;
        space0
            .context(StrContext::Label("space"))
            .parse_next(input)?;
        let kv = parse_options
            .context(StrContext::Label("options"))
            .parse_next(input)?;
        space0
            .context(StrContext::Label("space"))
            .parse_next(input)?;
        let args = parse_args
            .context(StrContext::Label("arguments"))
            .parse_next(input)?;
        space0
            .context(StrContext::Label("space"))
            .parse_next(input)?;
        // newline.context(StrContext::Label("newline")).parse_next(input)?;
        Ok(Command {
            command: comm.to_vec(),
            options: kv,
            args: args,
        })
    }

    pub fn write_bytes(&self, bytes: &mut impl Write) -> Result<(), std::io::Error> {
        bytes.write_all(&self.command)?;
        for (key, value) in &self.options {
            bytes.write_all(b" ")?;
            bytes.write_all(format!("-{}=", key).as_bytes())?;
            value.write_bytes(bytes)?;
        }
        for arg in &self.args {
            bytes.write_all(b" ")?;
            arg.write_bytes(bytes)?;
        }
        Ok(())
    }
}

impl From<Command> for String {
    fn from(cmd: Command) -> Self {
        let mut bytes = Vec::new();
        cmd.write_bytes(&mut bytes).unwrap();
        String::from_utf8_lossy(&bytes).to_string()
    }
}

fn parse_args<'s>(input: &mut &'s [u8]) -> ModalResult<Vec<Value>> {
    winnow::combinator::separated(
        0..,
        Value::parse.context(StrContext::Label("argument")),
        space1,
    )
    .parse_next(input)
}

fn parse_options<'s>(input: &mut &'s [u8]) -> ModalResult<IndexMap<String, Value>> {
    let x: Vec<(String, Value)> = winnow::combinator::separated(
        0..,
        parse_option.context(StrContext::Label("option")),
        space1,
    )
    .parse_next(input)?;
    let mut map = IndexMap::new();
    for (key, value) in x {
        map.insert(key, value);
    }
    Ok(map)
}

#[derive(Debug, Clone)]
pub struct OkResponse {
    pub options: IndexMap<String, Value>,
    pub args: Vec<Value>,
}

impl OkResponse {
    pub fn parse(input: &mut &[u8]) -> ModalResult<OkResponse> {
        let kv = parse_options(input)?;
        let _ = space0(input)?;
        let args = parse_args(input)?;
        Ok(OkResponse {
            options: kv,
            args: args,
        })
    }

    pub fn write_bytes(&self, bytes: &mut impl Write) -> Result<(), std::io::Error> {
        let mut first = true;
        for (key, value) in &self.options {
            if !first {
                bytes.write_all(b" ")?;
            }
            first = false;
            bytes.write_all(format!("-{}=", key).as_bytes())?;
            value.write_bytes(bytes)?;
        }
        for arg in &self.args {
            if !first {
                bytes.write_all(b" ")?;
            }
            first = false;
            arg.write_bytes(bytes)?;
        }
        Ok(())
    }

    pub fn to_string(&self) -> String {
        let mut bytes = Vec::new();
        self.write_bytes(&mut bytes).unwrap();
        String::from_utf8_lossy(&bytes).to_string()
    }
}

impl From<OkResponse> for String {
    fn from(cmd: OkResponse) -> Self {
        let mut bytes = Vec::new();
        cmd.write_bytes(&mut bytes).unwrap();
        String::from_utf8_lossy(&bytes).to_string()
    }
}

#[derive(Debug)]
pub enum MessageResponse {
    Ok {
        ident: MessageIdent,
        message: OkResponse,
    },
    Error {
        ident: MessageIdent,
        error: ErrorResponse,
    },
    Next {
        ident: MessageIdent,
    },
    Message(LogMessage),
}

#[derive(Debug, Clone, Error)]
pub struct ErrorResponse {
    pub error: String,
    pub args: IndexMap<String, Value>,
    pub message: String,
}

impl Display for ErrorResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.error)?;
        for (key, _value) in &self.args {
            write!(f, " -{}=", key)?;
        }
        //FIXME
        write!(f, " {}", self.message)
    }
}

impl ErrorResponse {
    pub fn parse(input: &mut &[u8]) -> ModalResult<ErrorResponse> {
        return (seq!{
            ErrorResponse {
                error: take_till(0.., |c: u8| c == b' ' || c == b'\n').map(|val| String::from_utf8_lossy(val).to_string()),
                _: space0,
                args: parse_options,
                _: space0,
                message: take_till(0.., |c: u8| c == b'\n').map(|val| String::from_utf8_lossy(val).to_string()),
            }
        }).parse_next(input);
    }
}

fn parse_ok(input: &mut &[u8]) -> ModalResult<MessageResponse> {
    return (seq! {
        MessageResponse::Ok {
            _: literal(b"OK"),
            _: space1,
            ident: MessageIdent::parse,
            _: space0,
            message: OkResponse::parse,
            _: space0,
            _: newline
        }
    })
    .parse_next(input);
}

fn parse_error(input: &mut &[u8]) -> ModalResult<MessageResponse> {
    return (seq! {
        MessageResponse::Error {
            _: literal(b"ERRor"),
            _: space1,
            ident: MessageIdent::parse,
            _: space0,
            error: ErrorResponse::parse,
        }
    })
    .parse_next(input);
}

fn parse_next(input: &mut &[u8]) -> ModalResult<MessageResponse> {
    return (seq! {
        MessageResponse::Next {
            _: literal(b"NEXT"),
            _: space1,
            ident: MessageIdent::parse,
            _: space0,
            _: newline
        }
    })
    .parse_next(input);
}

/// Example ready message:
///
/// ```scpi
/// READy -session=474800 -product=QuantStudio3_5 -version=1.3.0 -build=001 -capabilities=Index
/// ```
#[derive(Debug)]
pub struct Ready {
    pub args: IndexMap<String, Value>,
}

impl Ready {
    pub fn parse(input: &mut &[u8]) -> ModalResult<Ready> {
        return (seq! {
            Ready {
                _: literal(b"READy"),
                _: space1,
                args: parse_options,
                _: space0,
                _: newline
            }
        })
        .parse_next(input);
    }
}

#[cfg(feature = "python")]
use pyo3::prelude::*;

use crate::com::QSConnectionError;
use crate::commands::CommandBuilder;

#[derive(Debug, Clone)]
#[cfg_attr(feature = "python", pyclass)]
pub struct LogMessage {
    pub topic: String,
    pub message: String,
}

#[cfg(feature = "python")]
#[pymethods]
impl LogMessage {
    fn __str__(&self) -> String {
        format!("{}: {}", self.topic, self.message)
    }

    fn get_message(&self) -> String {
        self.message.clone()
    }

    fn get_topic(&self) -> String {
        self.topic.clone()
    }
}

fn parse_message(input: &mut &[u8]) -> ModalResult<MessageResponse> {
    let msg = seq!{LogMessage {
            _: literal(b"MESSage"),
            _: space1,
            topic: take_while(1.., |c: u8| c.is_ascii_alphanumeric() || c == b'.' || c == b':' || c == b'?' || c == b'*').map(|val| String::from_utf8_lossy(val).to_string()),
            _: space1,
            // FIXME: this should handle xml quotes; it is also not really needed if using our receiver, which already splits messages.
            message: take_till(0.., |c: u8| c == b'\n').map(|val| String::from_utf8_lossy(val).to_string()),
            _: space0,
            _: newline
        }}.parse_next(input)?;
    Ok(MessageResponse::Message(msg))
}

impl MessageResponse {
    pub fn parse(input: &mut &[u8]) -> ModalResult<MessageResponse> {
        alt((parse_ok, parse_error, parse_next, parse_message)).parse_next(input)
    }
}

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("parse error")]
    ParseError(String),
}

impl TryFrom<&[u8]> for MessageResponse {
    type Error = ParseError;
    fn try_from(s: &[u8]) -> Result<Self, ParseError> {
        let mut input = s;
        MessageResponse::parse(&mut input).map_err(|e| ParseError::ParseError(e.to_string()))
    }
}

pub fn parse_tag<'s>(input: &mut &'s [u8]) -> ModalResult<&'s [u8]> {
    delimited(
        literal(b'<'),
        take_while(1.., |c: u8| c.is_ascii_alphanumeric() || c == b'.'),
        literal(b'>'),
    )
    .parse_next(input)
}

pub fn xml_delimited<'a>(input: &mut &'a [u8]) -> ModalResult<(&'a [u8], &'a [u8])> {
    let t = parse_tag(input)?;
    let closestr = format!("</{}>", String::from_utf8_lossy(t));
    let close = closestr.as_bytes();
    let val = take_until(0.., close).parse_next(input)?;
    literal(close).parse_next(input)?;
    Ok((t, val))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_led_status_message() {
        let input = b"MESSage LEDStatus Temperature:56.1434 Current:9.19802 Voltage:3.40984 JuncTemp:72.7511\n";
        let result = MessageResponse::try_from(&input[..]).unwrap();

        if let MessageResponse::Message(msg) = result {
            assert_eq!(msg.topic, "LEDStatus");
            assert_eq!(
                msg.message,
                "Temperature:56.1434 Current:9.19802 Voltage:3.40984 JuncTemp:72.7511"
            );
        } else {
            panic!("Expected MessageResponse::Message, got {:?}", result);
        }
    }

    #[test]
    fn test_parse_temperature_message() {
        let input = b"MESSage Temperature -sample=22.5,22.4,22.4,22.5,22.4,22.5 -heatsink=23.4 -cover=18.0 -block=22.5,22.4,22.4,22.5,22.4,22.5\n";
        let result = MessageResponse::try_from(&input[..]).unwrap();

        if let MessageResponse::Message(msg) = result {
            assert_eq!(msg.topic, "Temperature");
            assert_eq!(msg.message, "-sample=22.5,22.4,22.4,22.5,22.4,22.5 -heatsink=23.4 -cover=18.0 -block=22.5,22.4,22.4,22.5,22.4,22.5");
        } else {
            panic!("Expected MessageResponse::Message, got {:?}", result);
        }
    }

    #[test]
    fn test_parse_invalid_message() {
        let input = b"MESSage\n"; // Missing required topic and message
        assert!(MessageResponse::try_from(&input[..]).is_err());
    }

    #[test]
    fn test_parse_missing_newline() {
        let input = b"MESSage Topic message"; // Missing required newline
        assert!(MessageResponse::try_from(&input[..]).is_err());
    }

    #[test]
    fn test_parse_binary_xml() {
        let binary_data = vec![0x00, 0x01, 0x02, 0xFF];
        let mut input = b"<binary.data>".to_vec();
        input.extend_from_slice(&binary_data);
        input.extend_from_slice(b"</binary.data>");
        let result = Value::parse(&mut &input[..]).unwrap();
        
        match result {
            Value::XmlBinary { value, tag } => {
                assert_eq!(tag, "binary.data");
                assert_eq!(value, binary_data);
            },
            _ => panic!("Expected XmlBinary, got {:?}", result),
        }
    }

    #[test]
    fn test_parse_xml_string() {
        let input = b"<quote>Hello\nWorld</quote>";
        let result = Value::parse(&mut &input[..]).unwrap();
        
        match result {
            Value::XmlString { value, tag } => {
                assert_eq!(tag, "quote");
                assert_eq!(value, "Hello\nWorld");
            },
            _ => panic!("Expected XmlString, got {:?}", result),
        }
    }

    #[test]
    fn test_write_binary_xml() {
        let binary_data = vec![0x00, 0x01, 0x02, 0xFF];
        let value = Value::XmlBinary {
            value: binary_data.clone(),
            tag: "quote".to_string(),
        };
        
        let mut output = Vec::new();
        value.write_bytes(&mut output).unwrap();
        
        let mut expected = b"<quote>".to_vec();
        expected.extend_from_slice(&binary_data);
        expected.extend_from_slice(b"</quote>");
        assert_eq!(output, expected);

        let result = Value::parse(&mut &output[..]).unwrap();
        assert_eq!(result, value);   
    }
}
