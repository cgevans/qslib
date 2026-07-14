use bstr::BString;
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

#[cfg(feature = "python")]
use pyo3::IntoPyObjectExt;

#[cfg(feature = "python")]
use pyo3::prelude::*;

#[cfg(feature = "python")]
use pyo3::IntoPyObject;

#[derive(Debug, Clone)]
pub struct ArgMap(IndexMap<String, Value>);

impl Default for ArgMap {
    fn default() -> Self {
        Self::new()
    }
}

impl ArgMap {
    pub fn new() -> Self {
        Self(IndexMap::new())
    }

    pub fn insert(&mut self, key: impl Into<String>, value: impl Into<Value>) -> Option<Value> {
        self.0.insert(key.into(), value.into())
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        self.0.get(key)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &Value)> {
        self.0.iter()
    }

    pub fn with(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.insert(key, value);
        self
    }

    pub fn extract_with_default<'a, T>(&'a self, key: &str, default: T) -> Result<T, ParseError>
    where
        T: TryFrom<&'a Value, Error = ParseError>,
    {
        match self.get(key) {
            Some(v) => Ok(v.try_into()?),
            None => Ok(default),
        }
    }
}

impl IntoIterator for ArgMap {
    type Item = (String, Value);
    type IntoIter = <IndexMap<String, Value> as IntoIterator>::IntoIter;
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a> IntoIterator for &'a ArgMap {
    type Item = (&'a String, &'a Value);
    type IntoIter = <&'a IndexMap<String, Value> as IntoIterator>::IntoIter;
    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

#[cfg(feature = "python")]
impl<'py> IntoPyObject<'py> for ArgMap {
    type Target = pyo3::types::PyDict;
    type Output = Bound<'py, Self::Target>;
    type Error = std::convert::Infallible;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let dict = pyo3::types::PyDict::new(py);
        for (key, value) in self.0 {
            dict.set_item(key, value.into_pyobject(py).unwrap())
                .unwrap();
        }
        Ok(dict)
    }
}

#[cfg(feature = "python")]
impl<'py> FromPyObject<'py> for ArgMap {
    fn extract_bound(ob: &Bound<'py, pyo3::PyAny>) -> PyResult<Self> {
        use pyo3::types::PyDict;

        if let Ok(dict) = ob.downcast::<PyDict>() {
            let mut arg_map = ArgMap::new();
            for (key, value) in dict.iter() {
                // Convert key to string - try direct extraction first, then fallback to str()
                let key_str: String = match key.extract::<String>() {
                    Ok(s) => s,
                    Err(_) => key.str()?.extract()?,
                };
                let value_parsed: Value = value.extract()?;
                arg_map.insert(key_str, value_parsed);
            }
            Ok(arg_map)
        } else {
            Err(pyo3::exceptions::PyTypeError::new_err(
                "Expected a dictionary for ArgMap conversion",
            ))
        }
    }
}

#[derive(PartialEq, Clone)]
pub enum Value {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    QuotedString(String),
    XmlString { value: BString, tag: String },
}

#[cfg(feature = "python")]
impl<'py> IntoPyObject<'py> for Value {
    type Target = pyo3::types::PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = std::convert::Infallible;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            Value::String(s) => Ok(s.into_pyobject(py).unwrap().into_any()),
            Value::Int(i) => Ok(i.into_pyobject(py).unwrap().into_any()),
            Value::Float(f) => Ok(f.into_pyobject(py).unwrap().into_any()),
            Value::Bool(b) => Ok(b.into_py_any(py).unwrap().into_bound(py)),
            Value::QuotedString(s) => Ok(s.into_pyobject(py).unwrap().into_any()),
            Value::XmlString { value, tag: _ } => match String::from_utf8(value.to_vec()) {
                Ok(s) => Ok(s.into_pyobject(py).unwrap().into_any()),
                Err(_) => Ok(value.to_vec().into_pyobject(py).unwrap().into_any()),
            },
        }
    }
}

#[cfg(feature = "python")]
impl<'py> FromPyObject<'py> for Value {
    fn extract_bound(ob: &Bound<'py, pyo3::PyAny>) -> PyResult<Self> {
        use pyo3::types::{PyBool, PyBytes, PyFloat, PyInt, PyString};

        if let Ok(b) = ob.downcast::<PyBool>() {
            Ok(Value::Bool(b.is_true()))
        } else if let Ok(i) = ob.downcast::<PyInt>() {
            Ok(Value::Int(i.extract()?))
        } else if let Ok(f) = ob.downcast::<PyFloat>() {
            Ok(Value::Float(f.extract()?))
        } else if let Ok(s) = ob.downcast::<PyString>() {
            let string_val: String = s.extract()?;
            if string_val.contains('\n') {
                Ok(Value::XmlString {
                    value: BString::from(string_val.as_bytes()),
                    tag: "quote".to_string(),
                })
            } else if string_val.contains(' ') {
                Ok(Value::QuotedString(string_val))
            } else {
                Ok(Value::String(string_val))
            }
        } else if let Ok(b) = ob.downcast::<PyBytes>() {
            let bytes: Vec<u8> = b.extract()?;
            Ok(Value::XmlString {
                value: BString::from(bytes),
                tag: String::new(),
            })
        } else {
            let s: String = ob.str()?.extract()?;
            if s.contains('\n') {
                Ok(Value::XmlString {
                    value: BString::from(s.as_bytes()),
                    tag: "quote".to_string(),
                })
            } else if s.contains(' ') {
                Ok(Value::QuotedString(s))
            } else {
                Ok(Value::String(s))
            }
        }
    }
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
                    write!(
                        f,
                        "XmlString({}, '{:?}...' len={})",
                        tag,
                        &value[..20],
                        value.len()
                    )
                } else {
                    write!(f, "XmlString({}, '{}')", tag, value)
                }
            }
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
        }
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        if s.contains('\n') {
            Value::XmlString {
                value: s.into(),
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
                value: s.into(),
                tag: "quote".to_string(),
            }
        } else if s.contains(' ') {
            Value::QuotedString(s)
        } else {
            Value::String(s)
        }
    }
}

/// Parse a double-quoted string with backslash escape support.
/// Handles \" within the string without prematurely closing.
fn parse_quoted_string(input: &mut &[u8]) -> ModalResult<String> {
    literal(b'"').parse_next(input)?;
    let mut result = Vec::new();
    loop {
        let chunk = take_till(0.., |c: u8| c == b'"' || c == b'\\').parse_next(input)?;
        result.extend_from_slice(chunk);
        if input.is_empty() {
            return Err(ErrMode::from_input(input));
        }
        match input[0] {
            b'"' => {
                *input = &input[1..];
                return Ok(String::from_utf8_lossy(&result).to_string());
            }
            b'\\' => {
                if input.len() < 2 {
                    return Err(ErrMode::from_input(input));
                }
                match input[1] {
                    b'"' => result.push(b'"'),
                    b'\\' => result.push(b'\\'),
                    b'n' => result.push(b'\n'),
                    b't' => result.push(b'\t'),
                    b'r' => result.push(b'\r'),
                    other => {
                        // Preserve unknown escape sequences as-is
                        result.push(b'\\');
                        result.push(other);
                    }
                }
                *input = &input[2..];
            }
            _ => unreachable!(),
        }
    }
}

impl Value {
    pub fn parse(input: &mut &[u8]) -> ModalResult<Value> {
        let v = alt((
            xml_delimited
                .map(|(tag, val)| {
                    let tag_str = String::from_utf8_lossy(tag).to_string();
                    Value::XmlString {
                        value: val.into(),
                        tag: tag_str,
                    }
                })
                .context(StrContext::Label("xml")),
            // Handle double-quoted strings with backslash escape support
            parse_quoted_string
                .map(|val| Value::QuotedString(val))
                .context(StrContext::Label("quoted")),
            // Handle single-quoted strings (no escape processing)
            delimited(
                literal(b'\''),
                take_till(0.., |c: u8| c == b'\''),
                literal(b'\''),
            )
            .map(|val| Value::QuotedString(String::from_utf8_lossy(val).to_string()))
            .context(StrContext::Label("single_quoted")),
            take_till(0.., |c: u8| c == b' ' || c == b'\n')
                .map(|val| Value::String(String::from_utf8_lossy(val).to_string()))
                .context(StrContext::Label("value")),
        ))
        .parse_next(input)?;
        if v == Value::String(String::from("")) {
            return Err(ErrMode::from_input(input));
        };
        if let Value::String(s) = &v {
            match s.to_lowercase().as_str() {
                "true" | "yes" | "on" => return Ok(Value::Bool(true)),
                "false" | "no" | "off" => return Ok(Value::Bool(false)),
                _ => {}
            }
            if let Ok(i) = s.parse::<i64>() {
                return Ok(Value::Int(i));
            }
            if let Ok(f) = s.parse::<f64>() {
                return Ok(Value::Float(f));
            }
        }
        Ok(v)
    }

    pub fn write_bytes(&self, bytes: &mut impl Write) -> Result<(), std::io::Error> {
        match self {
            Value::String(str) => bytes.write_all(str.as_bytes()),
            Value::Int(num) => bytes.write_all(num.to_string().as_bytes()),
            Value::Float(f) => bytes.write_all(f.to_string().as_bytes()),
            Value::Bool(b) => bytes.write_all(b.to_string().as_bytes()),
            Value::QuotedString(str) => {
                let escaped = str
                    .replace('\\', "\\\\")
                    .replace('"', "\\\"")
                    .replace('\n', "\\n")
                    .replace('\t', "\\t")
                    .replace('\r', "\\r");
                bytes.write_all(format!("\"{}\"", escaped).as_bytes())
            }
            Value::XmlString { value, tag } => {
                bytes.write_all(format!("<{}>", tag).as_bytes())?;
                bytes.write_all(value)?;
                bytes.write_all(format!("</{}>", tag).as_bytes())
            }
        }
    }

    pub fn try_into_bool(self) -> Result<bool, ParseError> {
        match self {
            Value::Bool(b) => Ok(b),
            _ => Err(ParseError::ParseError("bool".to_string())),
        }
    }

    pub fn try_into_string(self) -> Result<String, ParseError> {
        match self {
            Value::String(s) => Ok(s),
            Value::QuotedString(s) => Ok(s),
            Value::XmlString { value, .. } => match String::from_utf8(value.to_vec()) {
                Ok(s) => Ok(s),
                Err(_) => Err(ParseError::ParseError("string".to_string())),
            },
            _ => Err(ParseError::ParseError("string".to_string())),
        }
    }

    pub fn try_into_f64(self) -> Result<f64, ParseError> {
        match self {
            Value::Float(f) => Ok(f),
            Value::Int(i) => Ok(i as f64),
            _ => Err(ParseError::ParseError("float".to_string())),
        }
    }

    pub fn try_into_i64(self) -> Result<i64, ParseError> {
        match self {
            Value::Int(i) => Ok(i),
            _ => Err(ParseError::ParseError("int".to_string())),
        }
    }
}

impl TryFrom<&Value> for i64 {
    type Error = ParseError;
    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        match value {
            Value::Int(i) => Ok(*i),
            _ => Err(ParseError::ParseError("int".to_string())),
        }
    }
}

impl TryFrom<&Value> for f64 {
    type Error = ParseError;
    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        match value {
            Value::Float(f) => Ok(*f),
            Value::Int(i) => Ok(*i as f64),
            _ => Err(ParseError::ParseError("float".to_string())),
        }
    }
}

impl TryFrom<&Value> for bool {
    type Error = ParseError;
    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        match value {
            Value::Bool(b) => Ok(*b),
            _ => Err(ParseError::ParseError("bool".to_string())),
        }
    }
}

impl TryFrom<&Value> for String {
    type Error = ParseError;
    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        match value {
            Value::String(s) | Value::QuotedString(s) => Ok(s.clone()),
            Value::XmlString { value, .. } => String::from_utf8(value.to_vec())
                .map_err(|_| ParseError::ParseError("string (invalid utf8)".to_string())),
            _ => Err(ParseError::ParseError("string".to_string())),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromPyObject))]
pub enum MessageIdent {
    Number(u32),
    String(String),
}

impl MessageIdent {
    pub fn parse(input: &mut &[u8]) -> ModalResult<MessageIdent> {
        let r = alt((
            digit1.try_map(|val| {
                String::from_utf8_lossy(val)
                    .to_string()
                    .parse::<u32>()
                    .map(MessageIdent::Number)
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
    pub content: Option<BString>, // If None, then we're just expecting a particular ident response
}

impl Message {
    pub fn write_bytes(&self, bytes: &mut impl Write) -> Result<(), std::io::Error> {
        let content = self.content.as_ref().unwrap();
        if let Some(ident) = &self.ident {
            ident.write_bytes(bytes)?;
            bytes.write_all(b" ")?;
        }
        bytes.write_all(content)?;
        if !content.ends_with(b"\n") {
            bytes.write_all(b"\n")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "python", pyclass(get_all, set_all, module = "qslib._qslib"))]
pub struct Command {
    pub command: Vec<u8>,
    pub options: ArgMap,
    pub args: Vec<Value>,
}

impl CommandBuilder for Command {
    const COMMAND: &'static [u8] = b"";
    type Response = OkResponse;
    type Error = ErrorResponse;

    fn args(&self) -> Option<Vec<Value>> {
        Some(self.args.clone())
    }

    fn options(&self) -> Option<ArgMap> {
        Some(self.options.clone())
    }

    fn write_command(&self, bytes: &mut impl Write) -> Result<(), QSConnectionError> {
        self.write_bytes(bytes).map_err(QSConnectionError::IOError)
    }
}

impl TryFrom<&str> for Command {
    type Error = ParseError;
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let mut input = s.as_bytes();
        let c = Command::parse(&mut input).map_err(|e| {
            let offset = s.len() - input.len();
            let remaining = &s[offset..s.len().min(offset + 200)];
            ParseError::ParseError(format!("{} at {:?}", e, remaining))
        })?;
        Ok(c)
    }
}

impl TryFrom<Vec<u8>> for Command {
    type Error = ParseError;
    fn try_from(s: Vec<u8>) -> Result<Self, Self::Error> {
        let mut input = &s[..];
        let c = Command::parse(&mut input).map_err(|e| {
            let remaining = String::from_utf8_lossy(&input[..input.len().min(200)]);
            ParseError::ParseError(format!("{} at {:?}", e, remaining))
        })?;
        Ok(c)
    }
}

impl TryFrom<String> for Command {
    type Error = ParseError;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        let mut input = s.as_bytes();
        let c = Command::parse(&mut input).map_err(|e| {
            let offset = s.len() - input.len();
            let remaining = &s[offset..s.len().min(offset + 200)];
            ParseError::ParseError(format!("{} at {:?}", e, remaining))
        })?;
        Ok(c)
    }
}
pub fn parse_option(input: &mut &[u8]) -> ModalResult<(String, Value)> {
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
            options: ArgMap::new(),
            args: Vec::new(),
        }
    }

    pub fn bytes(s: &[u8]) -> Self {
        Command {
            command: s.to_vec(),
            options: ArgMap::new(),
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
            c.is_ascii_alphanumeric()
                || c == b'.'
                || c == b':'
                || c == b'?'
                || c == b'*'
                || c == b'='
                || c == b'+'
                || c == b'-'
                || c == b'~'
                || c == b'<'
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
            args,
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

#[cfg(feature = "python")]
#[pymethods]
impl Command {
    #[new]
    fn py_new(command: String) -> Self {
        Command::new(&command)
    }

    #[pyo3(name = "to_string")]
    fn py_to_string(&self) -> String {
        let mut bytes = Vec::new();
        self.write_bytes(&mut bytes).unwrap();
        String::from_utf8_lossy(&bytes).to_string()
    }

    #[pyo3(name = "from_string")]
    #[staticmethod]
    fn py_from_string(s: String) -> Result<Self, ParseError> {
        Command::try_from(s)
    }

    fn __str__(&self) -> String {
        let mut bytes = Vec::new();
        self.write_bytes(&mut bytes).unwrap();
        String::from_utf8_lossy(&bytes).to_string()
    }

    fn __repr__(&self) -> String {
        let command_str = String::from_utf8_lossy(&self.command);
        let mut repr = format!("Command(command='{}', options={{", command_str);

        let mut first_option = true;
        for (key, value) in &self.options {
            if !first_option {
                repr.push_str(", ");
            }
            first_option = false;
            repr.push_str(&format!("'{}': {:?}", key, value));
        }

        repr.push_str("}, args=[");

        let mut first_arg = true;
        for arg in &self.args {
            if !first_arg {
                repr.push_str(", ");
            }
            first_arg = false;
            repr.push_str(&format!("{:?}", arg));
        }

        repr.push_str("])");
        repr
    }
}

impl From<Command> for String {
    fn from(cmd: Command) -> Self {
        let mut bytes = Vec::new();
        cmd.write_bytes(&mut bytes).unwrap();
        String::from_utf8_lossy(&bytes).to_string()
    }
}

fn parse_args(input: &mut &[u8]) -> ModalResult<Vec<Value>> {
    winnow::combinator::separated(
        0..,
        Value::parse.context(StrContext::Label("argument")),
        space1,
    )
    .parse_next(input)
}

pub fn parse_options(input: &mut &[u8]) -> ModalResult<ArgMap> {
    let x: Vec<(String, Value)> = winnow::combinator::separated(
        0..,
        parse_option.context(StrContext::Label("option")),
        space1,
    )
    .parse_next(input)?;
    let mut map = ArgMap::new();
    for (key, value) in x {
        map.insert(key, value);
    }
    Ok(map)
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "python", pyclass(module = "qslib._qslib"))]
pub struct OkResponse {
    pub options: ArgMap,
    pub args: Vec<Value>,
}

impl TryFrom<&str> for OkResponse {
    type Error = ParseError;
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let mut input = s.as_bytes();
        let c = OkResponse::parse(&mut input).map_err(|e| ParseError::ParseError(e.to_string()))?;
        Ok(c)
    }
}

impl TryFrom<Vec<u8>> for OkResponse {
    type Error = ParseError;
    fn try_from(s: Vec<u8>) -> Result<Self, Self::Error> {
        let mut input = &s[..];
        let c = OkResponse::parse(&mut input).map_err(|e| ParseError::ParseError(e.to_string()))?;
        Ok(c)
    }
}

impl TryFrom<String> for OkResponse {
    type Error = ParseError;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        let mut input = s.as_bytes();
        let c = OkResponse::parse(&mut input).map_err(|e| ParseError::ParseError(e.to_string()))?;
        Ok(c)
    }
}

impl Display for OkResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut bytes = Vec::new();
        self.write_bytes(&mut bytes).unwrap();
        write!(f, "{}", String::from_utf8_lossy(&bytes))
    }
}

impl OkResponse {
    pub fn parse(input: &mut &[u8]) -> ModalResult<OkResponse> {
        let kv = parse_options(input)?;
        let _ = space0(input)?;
        let args = parse_args(input)?;
        Ok(OkResponse { options: kv, args })
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

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        self.write_bytes(&mut bytes).unwrap();
        bytes
    }
}

impl From<OkResponse> for String {
    fn from(cmd: OkResponse) -> Self {
        let mut bytes = Vec::new();
        cmd.write_bytes(&mut bytes).unwrap();
        String::from_utf8_lossy(&bytes).to_string()
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl OkResponse {
    #[getter]
    fn opts(&self, py: Python<'_>) -> PyResult<Py<pyo3::types::PyAny>> {
        Ok(self
            .options
            .clone()
            .into_pyobject(py)
            .unwrap()
            .into_any()
            .unbind())
    }

    #[getter]
    fn args(&self, py: Python<'_>) -> PyResult<Vec<Py<pyo3::types::PyAny>>> {
        self.args
            .iter()
            .map(|v| Ok(v.clone().into_pyobject(py).unwrap().into_any().unbind()))
            .collect()
    }

    #[staticmethod]
    #[pyo3(name = "from_string")]
    fn py_from_string(s: String) -> PyResult<Self> {
        OkResponse::try_from(s)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("Failed to parse: {}", e)))
    }

    fn __repr__(&self) -> String {
        format!("{}", self)
    }

    fn __str__(&self) -> String {
        format!("{}", self)
    }
}

#[derive(Debug)]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
pub enum MessageResponse {
    Ok {
        ident: MessageIdent,
        message: OkResponse,
    },
    Warning {
        ident: MessageIdent,
        message: OkResponse,
    },
    CommandError {
        ident: MessageIdent,
        error: ErrorResponse,
    },
    Next {
        ident: MessageIdent,
    },
    Message(LogMessage),
}

#[derive(Debug, Clone, Error)]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
pub struct ErrorResponse {
    pub error: String,
    pub args: ArgMap,
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
        // Try bracketed format first: [ErrorClass] -options --> message
        // Fall back to treating entire text as the error message (for respondError paths)
        let bracketed = (seq!{
            ErrorResponse {
                error: delimited("[", take_till(0.., |c: u8| c == b']'), "]").map(|val| String::from_utf8_lossy(val).to_string()),
                _: space0,
                args: parse_options,
                _: space0,
                message: take_till(0.., |c: u8| c == b'\n').map(|val| String::from_utf8_lossy(val).to_string()),
            }
        }).parse_next(input);

        if bracketed.is_ok() {
            return bracketed;
        }

        // Fallback: no brackets, entire remaining text is the error message
        let msg = take_till(0.., |c: u8| c == b'\n')
            .map(|val: &[u8]| String::from_utf8_lossy(val).to_string())
            .parse_next(input)?;
        Ok(ErrorResponse {
            error: String::new(),
            args: ArgMap::new(),
            message: msg,
        })
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

fn parse_warning(input: &mut &[u8]) -> ModalResult<MessageResponse> {
    return (seq! {
        MessageResponse::Warning {
            _: literal(b"WARNing"),
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
        MessageResponse::CommandError {
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
    pub args: ArgMap,
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

    /// Check if the server advertises a specific capability.
    pub fn has_capability(&self, capability: &str) -> bool {
        match self.args.get("capabilities") {
            Some(Value::String(caps)) => caps.split(',').any(|c| c.trim() == capability),
            _ => false,
        }
    }

    /// Check that the server supports command indexing (required for proper operation).
    /// Returns an error message if the capability is missing.
    pub fn validate_capabilities(&self) -> Result<(), String> {
        if !self.has_capability("Index") {
            return Err("Server does not advertise 'Index' capability. \
                 Command indexing may not work correctly."
                .to_string());
        }
        Ok(())
    }
}

use crate::com::QSConnectionError;
use crate::commands::CommandBuilder;

#[derive(Debug, Clone)]
#[cfg_attr(feature = "python", pyclass(module = "qslib._qslib"))]
pub struct LogMessage {
    pub topic: String,
    pub timestamp: Option<f64>,
    pub message: String,
}

#[cfg(feature = "python")]
#[pymethods]
impl LogMessage {
    fn __str__(&self) -> String {
        if let Some(ts) = self.timestamp {
            format!("{} [{}]: {}", self.topic, ts, self.message)
        } else {
            format!("{}: {}", self.topic, self.message)
        }
    }

    fn get_message(&self) -> String {
        self.message.clone()
    }

    fn get_topic(&self) -> String {
        self.topic.clone()
    }

    fn get_timestamp(&self) -> Option<f64> {
        self.timestamp
    }
}

fn parse_message(input: &mut &[u8]) -> ModalResult<MessageResponse> {
    let _: &[u8] = literal(b"MESSage").parse_next(input)?;
    let _: &[u8] = space1.parse_next(input)?;
    let topic: String = take_while(1.., |c: u8| {
        c.is_ascii_alphanumeric() || c == b'.' || c == b':' || c == b'?' || c == b'*'
    })
    .map(|val: &[u8]| String::from_utf8_lossy(val).to_string())
    .parse_next(input)?;
    let _: &[u8] = space1.parse_next(input)?;

    // Try to parse an optional timestamp (float) before the message text.
    // The server prepends a Unix timestamp when subscribed with -timestamp.
    let rest: String = take_till(0.., |c: u8| c == b'\n')
        .map(|val: &[u8]| String::from_utf8_lossy(val).to_string())
        .parse_next(input)?;
    let _: () = (space0, newline).void().parse_next(input)?;

    let (timestamp, message) = match rest.find(' ') {
        Some(pos) => {
            let candidate = &rest[..pos];
            match candidate.parse::<f64>() {
                Ok(ts) if ts > 1_000_000_000.0 => (Some(ts), rest[pos + 1..].to_string()),
                _ => (None, rest),
            }
        }
        None => (None, rest),
    };

    Ok(MessageResponse::Message(LogMessage {
        topic,
        timestamp,
        message,
    }))
}

impl MessageResponse {
    pub fn parse(input: &mut &[u8]) -> ModalResult<MessageResponse> {
        alt((
            parse_ok,
            parse_warning,
            parse_error,
            parse_next,
            parse_message,
        ))
        .parse_next(input)
    }
}

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("parse error: expected {0}")]
    ParseError(String),
}

#[cfg(feature = "python")]
impl From<ParseError> for pyo3::PyErr {
    fn from(e: ParseError) -> Self {
        pyo3::exceptions::PyValueError::new_err(e.to_string())
    }
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
        take_while(1.., |c: u8| {
            c.is_ascii_alphanumeric() || c == b'.' || c == b'_'
        }),
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

// --- SCPICommand: Full SCPI command parser with nested commands, comments, comma lists ---

/// An SCPI argument value that may be a scalar, a comma-separated list, or a block of nested commands.
#[derive(Debug, Clone, PartialEq)]
pub enum SCPIArgValue {
    Scalar(Value),
    List(Vec<Value>),
    CommandBlock(Vec<SCPICommand>),
}

/// A fully-parsed SCPI command with support for nested commands, comments, and comma-separated lists.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "python", pyclass(module = "qslib._qslib"))]
pub struct SCPICommand {
    pub command: String,
    pub args: Vec<SCPIArgValue>,
    pub opts: IndexMap<String, SCPIArgValue>,
    pub comment: Option<String>,
}

impl PartialEq for SCPICommand {
    fn eq(&self, other: &Self) -> bool {
        self.command == other.command && self.args == other.args && self.opts == other.opts
    }
}

/// Quote a string for SCPI if it contains spaces, quotes, or newlines.
pub fn quote_string_if_needed(s: &str) -> String {
    if s.contains('\n') {
        return format!("<quote>{}</quote>", s);
    }
    if s.contains(' ') || s.contains('"') {
        return format!("\"{}\"", s.replace('"', "\\\""));
    }
    s.to_string()
}

impl SCPIArgValue {
    /// Format this argument value as an SCPI string.
    pub fn to_scpi_string(&self, parent_command: &str) -> String {
        match self {
            SCPIArgValue::Scalar(v) => v.to_scpi_string(),
            SCPIArgValue::List(items) => items
                .iter()
                .map(|v| v.to_scpi_string())
                .collect::<Vec<_>>()
                .join(","),
            SCPIArgValue::CommandBlock(cmds) => {
                let q = format!("multiline.{}", parent_command.to_lowercase());
                let body: String = cmds.iter().map(|c| c.to_command_string()).collect();
                let indented = body
                    .lines()
                    .map(|l| format!("\t{}", l))
                    .collect::<Vec<_>>()
                    .join("\n");
                format!("<{}>\n{}</{}>", q, indented, q)
            }
        }
    }
}

impl Value {
    /// Format this value for SCPI output, applying quoting as needed.
    pub fn to_scpi_string(&self) -> String {
        match self {
            Value::String(s) => quote_string_if_needed(s),
            Value::Int(i) => i.to_string(),
            Value::Float(f) => f.to_string(),
            Value::Bool(b) => b.to_string(),
            Value::QuotedString(s) => quote_string_if_needed(s),
            Value::XmlString { value, tag } => {
                let content = String::from_utf8_lossy(value);
                format!("<{}>{}</{}>", tag, content, tag)
            }
        }
    }
}

impl SCPICommand {
    /// Format this command as an SCPI command string, including terminal newline.
    pub fn to_command_string(&self) -> String {
        let mut parts = vec![self.command.clone()];
        for (k, v) in &self.opts {
            parts.push(format!("-{}={}", k, v.to_scpi_string(&self.command)));
        }
        for v in &self.args {
            parts.push(v.to_scpi_string(&self.command));
        }
        let mut result = parts.join(" ");
        if let Some(comment) = &self.comment {
            result.push_str(&format!(" # {}", comment));
        }
        result.push('\n');
        result
    }
}

/// Parse a single unquoted value token (stops at space, newline, comma, <, #, ")
fn scpi_value_one(input: &mut &[u8]) -> ModalResult<Value> {
    let v = alt((
        // Handle double-quoted strings
        parse_quoted_string
            .map(Value::QuotedString)
            .context(StrContext::Label("quoted")),
        // Handle single-quoted strings
        delimited(
            literal(b'\''),
            take_till(0.., |c: u8| c == b'\''),
            literal(b'\''),
        )
        .map(|val: &[u8]| Value::QuotedString(String::from_utf8_lossy(val).to_string()))
        .context(StrContext::Label("single_quoted")),
        // XML-delimited quoted content (not command blocks - those use multiline. prefix)
        xml_delimited
            .verify(|(tag, _): &(&[u8], &[u8])| !tag.starts_with(b"multiline."))
            .map(|(_, val)| Value::String(String::from_utf8_lossy(val).to_string()))
            .context(StrContext::Label("xml_quoted")),
        // Unquoted value (stops at whitespace, comma, <, #)
        take_till(1.., |c: u8| {
            c == b' '
                || c == b'\t'
                || c == b'\n'
                || c == b'\r'
                || c == b','
                || c == b'<'
                || c == b'#'
        })
        .map(|val: &[u8]| Value::String(String::from_utf8_lossy(val).to_string()))
        .context(StrContext::Label("unquoted")),
    ))
    .parse_next(input)?;
    // Convert string to bool/int/float
    if let Value::String(s) = &v {
        match s.to_lowercase().as_str() {
            "true" | "yes" | "on" | "open" | "opened" => return Ok(Value::Bool(true)),
            "false" | "no" | "off" | "close" | "closed" => return Ok(Value::Bool(false)),
            _ => {}
        }
        if let Ok(i) = s.parse::<i64>() {
            return Ok(Value::Int(i));
        }
        if let Ok(f) = s.parse::<f64>() {
            return Ok(Value::Float(f));
        }
    }
    Ok(v)
}

/// Parse a possibly comma-separated value (single value or comma-delimited list)
fn scpi_opt_value(input: &mut &[u8]) -> ModalResult<SCPIArgValue> {
    // Try parsing an XML-delimited block (<tag>...</tag>)
    if input.starts_with(b"<") && !input.starts_with(b"</") {
        // Save position for backtracking
        let saved = *input;
        if let Ok((tag, content)) = xml_delimited.parse_next(input) {
            // Check if content starts with newline (command block) or is plain text
            let trimmed = content.iter().position(|&c| c != b' ' && c != b'\t');
            let starts_with_newline = trimmed.is_some_and(|pos| content[pos] == b'\n');
            if starts_with_newline {
                // Try to parse as a command block (stages, steps, etc.)
                let mut cmds = Vec::new();
                let mut inner = content;
                loop {
                    let _: &[u8] = take_while(0.., |c: u8| {
                        c == b' ' || c == b'\t' || c == b'\n' || c == b'\r'
                    })
                    .parse_next(&mut inner)?;
                    if inner.is_empty() {
                        break;
                    }
                    match parse_scpi_command(&mut inner) {
                        Ok(cmd) => cmds.push(cmd),
                        Err(_) => break,
                    }
                }
                if !cmds.is_empty() {
                    return Ok(SCPIArgValue::CommandBlock(cmds));
                }
            }
            // Plain XML-quoted text
            let tag_str = String::from_utf8_lossy(tag).to_string();
            return Ok(SCPIArgValue::Scalar(Value::XmlString {
                value: content.into(),
                tag: tag_str,
            }));
        }
        // xml_delimited failed, restore position
        *input = saved;
    }
    // Parse first value
    let first = scpi_value_one(input)?;
    // Check for comma-separated list
    if input.first() == Some(&b',') {
        let mut items = vec![first];
        while input.first() == Some(&b',') {
            *input = &input[1..]; // consume comma
            items.push(scpi_value_one(input)?);
        }
        Ok(SCPIArgValue::List(items))
    } else {
        Ok(SCPIArgValue::Scalar(first))
    }
}

/// Parse one option key=value pair (-key=value)
fn scpi_opt_kv(input: &mut &[u8]) -> ModalResult<(String, SCPIArgValue)> {
    literal(b'-').parse_next(input)?;
    let key: &[u8] = alphanumeric1.parse_next(input)?;
    literal(b'=').parse_next(input)?;
    let val = scpi_opt_value(input)?;
    Ok((String::from_utf8_lossy(key).to_lowercase(), val))
}

/// Parse one SCPI command (command name + opts + args + optional comment)
pub fn parse_scpi_command(input: &mut &[u8]) -> ModalResult<SCPICommand> {
    // Command name: alphanumeric + special chars
    let comm: &[u8] = take_while(1.., |c: u8| {
        c.is_ascii_alphanumeric() || b".:?*=+-~<{}>".contains(&c)
    })
    .context(StrContext::Label("command"))
    .parse_next(input)?;
    let command = String::from_utf8_lossy(comm).to_uppercase();

    // Parse options and args mixed together
    let mut opts = IndexMap::new();
    let mut args = Vec::new();
    let mut comment = None;

    loop {
        // Skip horizontal whitespace
        let _: &[u8] = take_while(0.., |c: u8| c == b' ' || c == b'\t').parse_next(input)?;

        // Check for end conditions
        if input.is_empty() || input[0] == b'\n' {
            if !input.is_empty() {
                *input = &input[1..]; // consume newline
            }
            break;
        }

        // Check for comment
        if input[0] == b'#' {
            *input = &input[1..]; // skip #
                                  // Skip optional space after #
            let _: &[u8] = take_while(0.., |c: u8| c == b' ').parse_next(input)?;
            let comment_text: &[u8] = take_till(0.., |c: u8| c == b'\n').parse_next(input)?;
            comment = Some(String::from_utf8_lossy(comment_text).to_string());
            // Consume the newline if present
            if !input.is_empty() && input[0] == b'\n' {
                *input = &input[1..];
            }
            break;
        }

        // Try option (-key=value)
        if input[0] == b'-' && input.len() > 1 && input[1].is_ascii_alphanumeric() {
            // Peek to check it's really an option (has = sign)
            if let Some(eq_pos) = input.iter().position(|&c| c == b'=') {
                let is_option = input[1..eq_pos].iter().all(|c| c.is_ascii_alphanumeric());
                if is_option {
                    let (key, val) = scpi_opt_kv(input)?;
                    opts.insert(key, val);
                    continue;
                }
            }
        }

        // Parse argument value
        match scpi_opt_value(input) {
            Ok(val) => args.push(val),
            Err(_) => break,
        }
    }

    Ok(SCPICommand {
        command,
        args,
        opts,
        comment,
    })
}

#[cfg(feature = "python")]
#[pymethods]
impl SCPICommand {
    /// Parse an SCPI command string.
    #[staticmethod]
    #[pyo3(name = "from_string")]
    fn py_from_string(s: &str) -> PyResult<Self> {
        let mut input = s.as_bytes();
        parse_scpi_command(&mut input).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "Failed to parse SCPICommand from {:?}: {}",
                s, e
            ))
        })
    }

    /// Get the command name.
    #[getter]
    fn command(&self) -> &str {
        &self.command
    }

    /// Get the comment, if any.
    #[getter]
    fn comment(&self) -> Option<&str> {
        self.comment.as_deref()
    }

    /// Get options as a Python dict.
    #[getter]
    fn opts(&self, py: Python<'_>) -> PyResult<Py<pyo3::types::PyAny>> {
        let dict = pyo3::types::PyDict::new(py);
        for (key, val) in &self.opts {
            dict.set_item(key, scpi_arg_to_py(py, val)?)?;
        }
        Ok(dict.into_any().unbind())
    }

    /// Get positional args as a Python tuple.
    #[getter]
    fn args(&self, py: Python<'_>) -> PyResult<Py<pyo3::types::PyAny>> {
        let items: Vec<Py<pyo3::types::PyAny>> = self
            .args
            .iter()
            .map(|v| scpi_arg_to_py(py, v))
            .collect::<PyResult<_>>()?;
        let tuple = pyo3::types::PyTuple::new(py, items)?;
        Ok(tuple.into_any().unbind())
    }

    fn __repr__(&self) -> String {
        format!(
            "SCPICommand(command='{}', args={:?}, opts={:?}, comment={:?})",
            self.command, self.args, self.opts, self.comment
        )
    }

    fn __eq__(&self, other: &Self) -> bool {
        self == other
    }

    /// Format as an SCPI command string including terminal newline.
    #[pyo3(name = "to_command_string")]
    fn py_to_command_string(&self) -> String {
        self.to_command_string()
    }
}

/// Quote a string for SCPI if it contains spaces, quotes, or newlines.
#[cfg(feature = "python")]
#[pyfunction]
pub fn py_quote_string_if_needed(s: &str) -> String {
    quote_string_if_needed(s)
}

#[cfg(feature = "python")]
fn scpi_arg_to_py(py: Python<'_>, val: &SCPIArgValue) -> PyResult<Py<pyo3::types::PyAny>> {
    match val {
        SCPIArgValue::Scalar(v) => Ok(v.clone().into_pyobject(py).unwrap().into_any().unbind()),
        SCPIArgValue::List(items) => {
            let py_items: Vec<Py<pyo3::types::PyAny>> = items
                .iter()
                .map(|v| Ok(v.clone().into_pyobject(py).unwrap().into_any().unbind()))
                .collect::<PyResult<_>>()?;
            let list = pyo3::types::PyList::new(py, py_items)?;
            Ok(list.into_any().unbind())
        }
        SCPIArgValue::CommandBlock(cmds) => {
            let py_cmds: Vec<Py<SCPICommand>> = cmds
                .iter()
                .map(|cmd| Py::new(py, cmd.clone()))
                .collect::<PyResult<_>>()?;
            let list = pyo3::types::PyList::new(py, py_cmds)?;
            Ok(list.into_any().unbind())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bstr::ByteSlice;

    #[test]
    fn test_parse_led_status_message() {
        let input = b"MESSage LEDStatus Temperature:56.1434 Current:9.19802 Voltage:3.40984 JuncTemp:72.7511\n";
        let result = MessageResponse::try_from(&input[..]).unwrap();

        if let MessageResponse::Message(msg) = result {
            assert_eq!(msg.topic, "LEDStatus");
            assert!(msg.timestamp.is_none());
            assert_eq!(
                msg.message,
                "Temperature:56.1434 Current:9.19802 Voltage:3.40984 JuncTemp:72.7511"
            );
        } else {
            panic!("Expected MessageResponse::Message, got {:?}", result);
        }
    }

    #[test]
    fn test_parse_message_with_timestamp() {
        let input = b"MESSage Temperature 1614567890.123 -sample=22.5,22.4\n";
        let result = MessageResponse::try_from(&input[..]).unwrap();

        if let MessageResponse::Message(msg) = result {
            assert_eq!(msg.topic, "Temperature");
            assert!((msg.timestamp.unwrap() - 1614567890.123).abs() < 0.001);
            assert_eq!(msg.message, "-sample=22.5,22.4");
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
            Value::XmlString { value, tag } => {
                assert_eq!(tag, "binary.data");
                assert_eq!(value, binary_data);
            }
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
            }
            _ => panic!("Expected XmlString, got {:?}", result),
        }
    }

    #[test]
    fn test_write_binary_xml() {
        let binary_data: BString = b"\x00\x01\x02\xFF".into();
        let value = Value::XmlString {
            value: binary_data.clone(),
            tag: "quote".to_string(),
        };

        let mut output = Vec::new();
        value.write_bytes(&mut output).unwrap();

        let mut expected: BString = b"<quote>".into();
        expected.extend_from_slice(&binary_data);
        expected.extend_from_slice(b"</quote>");
        assert_eq!(output.as_bstr(), expected);

        let result = Value::parse(&mut &output[..]).unwrap();
        assert_eq!(result, value);
    }

    // =====================================================================
    // Additional parser tests
    // =====================================================================

    #[test]
    fn test_parse_value_types() {
        // Integer
        let result = Value::parse(&mut b"42"[..].as_ref()).unwrap();
        assert!(matches!(result, Value::Int(42)));

        // Negative integer
        let result = Value::parse(&mut b"-10"[..].as_ref()).unwrap();
        assert!(matches!(result, Value::Int(-10)));

        // Float
        let result = Value::parse(&mut b"3.24"[..].as_ref()).unwrap();
        match result {
            Value::Float(f) => assert!((f - 3.24).abs() < 0.001),
            _ => panic!("Expected Float"),
        }

        // Negative float
        let result = Value::parse(&mut b"-3.24"[..].as_ref()).unwrap();
        match result {
            Value::Float(f) => assert!((f + 3.24).abs() < 0.001),
            _ => panic!("Expected Float"),
        }

        // Boolean true
        let result = Value::parse(&mut b"true"[..].as_ref()).unwrap();
        assert!(matches!(result, Value::Bool(true)));

        // Boolean false (case insensitive)
        let result = Value::parse(&mut b"False"[..].as_ref()).unwrap();
        assert!(matches!(result, Value::Bool(false)));

        // String
        let result = Value::parse(&mut b"hello"[..].as_ref()).unwrap();
        assert!(matches!(result, Value::String(s) if s == "hello"));
    }

    #[test]
    fn test_parse_quoted_string() {
        let result = Value::parse(&mut b"\"hello world\""[..].as_ref()).unwrap();
        assert!(matches!(result, Value::QuotedString(s) if s == "hello world"));
    }

    #[test]
    fn test_parse_command_simple() {
        let input = b"HELP?";
        let result = Command::parse(&mut &input[..]).unwrap();
        assert_eq!(String::from_utf8_lossy(&result.command), "HELP?");
        assert!(result.options.is_empty());
        assert!(result.args.is_empty());
    }

    #[test]
    fn test_parse_command_with_options() {
        let input = b"CMD -opt1=value1 -opt2=42";
        let result = Command::parse(&mut &input[..]).unwrap();
        assert_eq!(String::from_utf8_lossy(&result.command), "CMD");
        assert_eq!(result.options.get("opt1").unwrap().to_string(), "value1");
        assert!(matches!(
            result.options.get("opt2").unwrap(),
            Value::Int(42)
        ));
    }

    #[test]
    fn test_parse_command_with_args() {
        let input = b"CMD arg1 arg2 42";
        let result = Command::parse(&mut &input[..]).unwrap();
        assert_eq!(String::from_utf8_lossy(&result.command), "CMD");
        assert_eq!(result.args.len(), 3);
        assert_eq!(result.args[0].to_string(), "arg1");
        assert_eq!(result.args[1].to_string(), "arg2");
        assert!(matches!(result.args[2], Value::Int(42)));
    }

    #[test]
    fn test_parse_command_mixed() {
        let input = b"POW -zone=1 ON";
        let result = Command::parse(&mut &input[..]).unwrap();
        assert_eq!(String::from_utf8_lossy(&result.command), "POW");
        assert!(matches!(result.options.get("zone").unwrap(), Value::Int(1)));
        assert!(matches!(result.args[0], Value::Bool(true)));
    }

    #[test]
    fn test_command_write_bytes() {
        let cmd = Command::new("TEST")
            .with_option("opt", Value::Int(42))
            .with_arg("arg1");

        let mut output = Vec::new();
        cmd.write_bytes(&mut output).unwrap();

        let output_str = String::from_utf8_lossy(&output);
        assert!(output_str.contains("TEST"));
        assert!(output_str.contains("-opt=42"));
        assert!(output_str.contains("arg1"));
    }

    #[test]
    fn test_message_no_double_newline() {
        // Content without trailing newline should get one added
        let msg = Message {
            ident: None,
            content: Some(b"CMD arg1".into()),
        };
        let mut output = Vec::new();
        msg.write_bytes(&mut output).unwrap();
        assert_eq!(&output, b"CMD arg1\n");

        // Content with trailing newline should NOT get another
        let msg = Message {
            ident: None,
            content: Some(b"CMD arg1\n".into()),
        };
        let mut output = Vec::new();
        msg.write_bytes(&mut output).unwrap();
        assert_eq!(&output, b"CMD arg1\n");
    }

    #[test]
    fn test_command_roundtrip() {
        let original = "CMD -opt1=value -opt2=3.14 arg1 arg2";
        let cmd = Command::try_from(original).unwrap();

        let mut output = Vec::new();
        cmd.write_bytes(&mut output).unwrap();

        let reparsed = Command::parse(&mut &output[..]).unwrap();
        assert_eq!(
            String::from_utf8_lossy(&cmd.command),
            String::from_utf8_lossy(&reparsed.command)
        );
    }

    #[test]
    fn test_parse_command_suffix_chars() {
        // Server commands can have +, -, ~, < suffixes
        let input = b"SUBS+ topic1";
        let result = Command::parse(&mut &input[..]).unwrap();
        assert_eq!(String::from_utf8_lossy(&result.command), "SUBS+");
        assert_eq!(result.args[0].to_string(), "topic1");

        let input = b"FLAG- topic1";
        let result = Command::parse(&mut &input[..]).unwrap();
        assert_eq!(String::from_utf8_lossy(&result.command), "FLAG-");

        let input = b"CLEAR~ item";
        let result = Command::parse(&mut &input[..]).unwrap();
        assert_eq!(String::from_utf8_lossy(&result.command), "CLEAR~");

        let input = b"LOAD< file";
        let result = Command::parse(&mut &input[..]).unwrap();
        assert_eq!(String::from_utf8_lossy(&result.command), "LOAD<");
    }

    #[test]
    fn test_parse_error_response_with_suffix_command() {
        // Server echoes command with suffix in error response (numeric ident)
        let input = b"ERRor 42 [SomeError] --> bad args\n";
        let result = MessageResponse::try_from(&input[..]).unwrap();
        match result {
            MessageResponse::CommandError { ident, error } => {
                assert_eq!(ident, MessageIdent::Number(42));
                assert_eq!(error.error, "SomeError");
            }
            _ => panic!("Expected CommandError response"),
        }
    }

    #[test]
    fn test_parse_ok_response() {
        let input = b"OK 123 -opt=val arg1\n";
        let result = MessageResponse::try_from(&input[..]).unwrap();

        match result {
            MessageResponse::Ok { ident, message } => {
                assert!(matches!(ident, MessageIdent::Number(123)));
                assert_eq!(message.options.get("opt").unwrap().to_string(), "val");
                assert_eq!(message.args[0].to_string(), "arg1");
            }
            _ => panic!("Expected Ok response"),
        }
    }

    #[test]
    fn test_parse_error_response() {
        let input = b"ERRor 456 [AuthenticationError] Invalid password\n";
        let result = MessageResponse::try_from(&input[..]).unwrap();

        match result {
            MessageResponse::CommandError { ident, error } => {
                assert!(matches!(ident, MessageIdent::Number(456)));
                assert_eq!(error.error, "AuthenticationError");
            }
            _ => panic!("Expected CommandError response"),
        }
    }

    #[test]
    fn test_parse_next_response() {
        let input = b"NEXT 789\n";
        let result = MessageResponse::try_from(&input[..]).unwrap();

        match result {
            MessageResponse::Next { ident } => {
                assert!(matches!(ident, MessageIdent::Number(789)));
            }
            _ => panic!("Expected Next response"),
        }
    }

    #[test]
    fn test_parse_ready_message() {
        let input = b"READy -session=474800 -product=QuantStudio3_5 -version=1.3.0 -build=001\n";
        let result = Ready::parse(&mut &input[..]).unwrap();

        assert_eq!(result.args.get("session").unwrap().to_string(), "474800");
        assert_eq!(
            result.args.get("product").unwrap().to_string(),
            "QuantStudio3_5"
        );
        assert_eq!(result.args.get("version").unwrap().to_string(), "1.3.0");
        assert_eq!(result.args.get("build").unwrap().to_string(), "1");
    }

    #[test]
    fn test_ready_capabilities_present() {
        let input = b"READy -session=1 -capabilities=Index\n";
        let result = Ready::parse(&mut &input[..]).unwrap();
        assert!(result.has_capability("Index"));
        assert!(!result.has_capability("Other"));
        assert!(result.validate_capabilities().is_ok());
    }

    #[test]
    fn test_ready_capabilities_missing() {
        let input = b"READy -session=1 -product=Test\n";
        let result = Ready::parse(&mut &input[..]).unwrap();
        assert!(!result.has_capability("Index"));
        assert!(result.validate_capabilities().is_err());
    }

    #[test]
    fn test_message_ident_number() {
        let result = MessageIdent::parse(&mut b"12345"[..].as_ref()).unwrap();
        assert!(matches!(result, MessageIdent::Number(12345)));
    }

    #[test]
    fn test_value_conversions() {
        // From i64
        let v: Value = 42i64.into();
        assert!(matches!(v, Value::Int(42)));

        // From f64
        let v: Value = 3.24f64.into();
        match v {
            Value::Float(f) => assert!((f - 3.24).abs() < 0.001),
            _ => panic!("Expected Float"),
        }

        // From bool
        let v: Value = true.into();
        assert!(matches!(v, Value::Bool(true)));

        // From &str
        let v: Value = "hello".into();
        assert!(matches!(v, Value::String(s) if s == "hello"));

        // From String with space
        let v: Value = "hello world".to_string().into();
        assert!(matches!(v, Value::QuotedString(s) if s == "hello world"));

        // From String with newline
        let v: Value = "hello\nworld".to_string().into();
        assert!(matches!(v, Value::XmlString { .. }));
    }

    #[test]
    fn test_value_try_into() {
        let v = Value::Int(42);
        assert_eq!(v.try_into_i64().unwrap(), 42);

        let v = Value::Float(3.2);
        assert!((v.try_into_f64().unwrap() - 3.2).abs() < 0.001);

        let v = Value::Bool(true);
        assert!(v.try_into_bool().unwrap());

        let v = Value::String("hello".to_string());
        assert_eq!(v.try_into_string().unwrap(), "hello");

        let v = Value::QuotedString("hello world".to_string());
        assert_eq!(v.try_into_string().unwrap(), "hello world");
    }

    #[test]
    fn test_argmap_operations() {
        let mut map = ArgMap::new();
        assert!(map.is_empty());

        map.insert("key1", Value::Int(42));
        assert_eq!(map.len(), 1);
        assert!(!map.is_empty());

        assert!(matches!(map.get("key1").unwrap(), Value::Int(42)));
        assert!(map.get("nonexistent").is_none());
    }

    #[test]
    fn test_argmap_extract_with_default() {
        let map = ArgMap::new().with("existing", Value::Int(42));

        let result: i64 = map.extract_with_default("existing", 0).unwrap();
        assert_eq!(result, 42);

        let result: i64 = map.extract_with_default("missing", 99).unwrap();
        assert_eq!(result, 99);
    }

    #[test]
    fn test_ok_response_roundtrip() {
        let response = OkResponse {
            options: ArgMap::new()
                .with("opt1", Value::Int(42))
                .with("opt2", Value::String("val".to_string())),
            args: vec![Value::String("arg1".to_string()), Value::Float(3.2)],
        };

        let bytes = response.to_bytes();
        let reparsed = OkResponse::parse(&mut &bytes[..]).unwrap();

        assert_eq!(
            reparsed.options.get("opt1").unwrap().to_string(),
            response.options.get("opt1").unwrap().to_string()
        );
    }

    #[test]
    fn test_command_with_xml_arg() {
        let cmd = Command::new("PROT").with_arg(Value::XmlString {
            value: "STAGE 1\nTEST".into(),
            tag: "multiline.protocol".to_string(),
        });

        let mut output = Vec::new();
        cmd.write_bytes(&mut output).unwrap();

        let output_str = String::from_utf8_lossy(&output);
        assert!(output_str.contains("<multiline.protocol>"));
        assert!(output_str.contains("</multiline.protocol>"));
        assert!(output_str.contains("STAGE 1\nTEST"));
    }

    #[test]
    fn test_parse_special_command_names() {
        // Commands with dots
        let cmd = Command::try_from("TBC:SETT?").unwrap();
        assert_eq!(String::from_utf8_lossy(&cmd.command), "TBC:SETT?");

        // Commands with asterisks
        let cmd = Command::try_from("IDN*").unwrap();
        assert_eq!(String::from_utf8_lossy(&cmd.command), "IDN*");
    }

    #[test]
    fn test_value_display() {
        assert_eq!(Value::Int(42).to_string(), "42");
        assert_eq!(Value::String("hello".to_string()).to_string(), "hello");
        assert_eq!(Value::Bool(true).to_string(), "true");

        let xml = Value::XmlString {
            value: "content".into(),
            tag: "tag".to_string(),
        };
        assert_eq!(xml.to_string(), "content");
    }

    #[test]
    fn test_tag_with_underscore() {
        let input = b"<quote_reply>content</quote_reply>";
        let mut inp: &[u8] = input;
        let result = xml_delimited(&mut inp).unwrap();
        assert_eq!(String::from_utf8_lossy(result.0), "quote_reply");
        assert_eq!(String::from_utf8_lossy(result.1), "content");
    }

    #[test]
    fn test_single_quoted_string() {
        let input = b"'hello world'";
        let mut inp: &[u8] = input;
        let result = Value::parse(&mut inp).unwrap();
        assert_eq!(result, Value::QuotedString("hello world".to_string()));
    }

    #[test]
    fn test_single_quoted_string_with_dollar() {
        let input = b"'Price is $100'";
        let mut inp: &[u8] = input;
        let result = Value::parse(&mut inp).unwrap();
        assert_eq!(result, Value::QuotedString("Price is $100".to_string()));
    }

    #[test]
    fn test_bool_parsing_extended() {
        let cases_true = vec!["true", "True", "yes", "Yes", "on", "On"];
        let cases_false = vec!["false", "False", "no", "No", "off", "Off"];
        for s in cases_true {
            let mut inp = s.as_bytes();
            let v = Value::parse(&mut inp).unwrap();
            assert_eq!(v, Value::Bool(true), "Expected true for '{}'", s);
        }
        for s in cases_false {
            let mut inp = s.as_bytes();
            let v = Value::parse(&mut inp).unwrap();
            assert_eq!(v, Value::Bool(false), "Expected false for '{}'", s);
        }
        // open/closed/close/opened stay as strings in response parsing
        // (they are valid machine responses like DRAW? → "Open")
        for s in ["open", "opened", "Open", "close", "closed", "Closed"] {
            let mut inp = s.as_bytes();
            let v = Value::parse(&mut inp).unwrap();
            assert!(
                matches!(v, Value::String(_)),
                "Expected string for '{}', got {:?}",
                s,
                v
            );
        }
    }

    #[test]
    fn test_quoted_string_with_escaped_quote() {
        let input = b"\"foo\\\"bar\"";
        let mut inp: &[u8] = input;
        let result = Value::parse(&mut inp).unwrap();
        assert_eq!(result, Value::QuotedString("foo\"bar".to_string()));
    }

    #[test]
    fn test_quoted_string_with_escaped_backslash() {
        let input = b"\"foo\\\\bar\"";
        let mut inp: &[u8] = input;
        let result = Value::parse(&mut inp).unwrap();
        assert_eq!(result, Value::QuotedString("foo\\bar".to_string()));
    }

    #[test]
    fn test_quoted_string_simple() {
        let input = b"\"hello world\"";
        let mut inp: &[u8] = input;
        let result = Value::parse(&mut inp).unwrap();
        assert_eq!(result, Value::QuotedString("hello world".to_string()));
    }

    #[test]
    fn test_quoted_string_roundtrip() {
        // Test that output from write_bytes can be re-parsed
        let original = Value::QuotedString("foo\"bar".to_string());
        let mut bytes = Vec::new();
        original.write_bytes(&mut bytes).unwrap();
        let mut inp: &[u8] = &bytes;
        let parsed = Value::parse(&mut inp).unwrap();
        assert_eq!(parsed, Value::QuotedString("foo\"bar".to_string()));
    }

    #[test]
    fn test_quoted_string_roundtrip_backslash() {
        // Backslashes must survive parse -> serialize -> parse
        let original = Value::QuotedString("foo\\bar".to_string());
        let mut bytes = Vec::new();
        original.write_bytes(&mut bytes).unwrap();
        assert_eq!(&bytes, b"\"foo\\\\bar\"");
        let mut inp: &[u8] = &bytes;
        let parsed = Value::parse(&mut inp).unwrap();
        assert_eq!(parsed, original);
    }

    #[test]
    fn test_quoted_string_roundtrip_newline() {
        let original = Value::QuotedString("line1\nline2".to_string());
        let mut bytes = Vec::new();
        original.write_bytes(&mut bytes).unwrap();
        assert_eq!(&bytes, b"\"line1\\nline2\"");
        let mut inp: &[u8] = &bytes;
        let parsed = Value::parse(&mut inp).unwrap();
        assert_eq!(parsed, original);
    }

    #[test]
    fn test_warning_response() {
        let input = b"WARNing 1 Some warning text\n";
        let result = MessageResponse::try_from(&input[..]).unwrap();
        if let MessageResponse::Warning { message, .. } = result {
            assert_eq!(message.args.first().unwrap().to_string(), "Some");
        } else {
            panic!("Expected Warning, got {:?}", result);
        }
    }

    #[test]
    fn test_warning_response_with_options() {
        let input = b"WARNing 1 -detail=info Something happened\n";
        let result = MessageResponse::try_from(&input[..]).unwrap();
        if let MessageResponse::Warning { message, .. } = result {
            assert!(message.options.get("detail").is_some());
        } else {
            panic!("Expected Warning, got {:?}", result);
        }
    }

    #[test]
    fn test_error_response_with_brackets() {
        let input = b"ERRor 1 [InsufficientAccess] Access denied\n";
        let result = MessageResponse::try_from(&input[..]).unwrap();
        if let MessageResponse::CommandError { error, .. } = result {
            assert_eq!(error.error, "InsufficientAccess");
            assert_eq!(error.message, "Access denied");
        } else {
            panic!("Expected CommandError, got {:?}", result);
        }
    }

    #[test]
    fn test_error_response_without_brackets() {
        let input = b"ERRor 1 Aborted\n";
        let result = MessageResponse::try_from(&input[..]).unwrap();
        if let MessageResponse::CommandError { error, .. } = result {
            assert_eq!(error.error, "");
            assert_eq!(error.message, "Aborted");
        } else {
            panic!("Expected CommandError, got {:?}", result);
        }
    }

    #[test]
    fn test_error_response_internal_error() {
        let input = b"ERRor 1 Internal error: something went wrong\n";
        let result = MessageResponse::try_from(&input[..]).unwrap();
        if let MessageResponse::CommandError { error, .. } = result {
            assert_eq!(error.error, "");
            assert_eq!(error.message, "Internal error: something went wrong");
        } else {
            panic!("Expected CommandError, got {:?}", result);
        }
    }

    // --- SCPICommand parser tests ---

    #[test]
    fn test_scpi_simple_command() {
        let mut input = b"TEST:CMD\n" as &[u8];
        let cmd = parse_scpi_command(&mut input).unwrap();
        assert_eq!(cmd.command, "TEST:CMD");
        assert!(cmd.args.is_empty());
        assert!(cmd.opts.is_empty());
        assert!(cmd.comment.is_none());
    }

    #[test]
    fn test_scpi_command_with_args() {
        let mut input = b"CMD arg1 42 3.14\n" as &[u8];
        let cmd = parse_scpi_command(&mut input).unwrap();
        assert_eq!(cmd.command, "CMD");
        assert_eq!(cmd.args.len(), 3);
        assert!(matches!(&cmd.args[0], SCPIArgValue::Scalar(Value::String(s)) if s == "arg1"));
        assert!(matches!(&cmd.args[1], SCPIArgValue::Scalar(Value::Int(42))));
        assert!(
            matches!(&cmd.args[2], SCPIArgValue::Scalar(Value::Float(f)) if (*f - 3.14).abs() < 0.001)
        );
    }

    #[test]
    fn test_scpi_command_with_opts() {
        let mut input = b"CMD -stage=3 -cycle=10\n" as &[u8];
        let cmd = parse_scpi_command(&mut input).unwrap();
        assert_eq!(cmd.command, "CMD");
        assert!(cmd.args.is_empty());
        assert!(matches!(
            cmd.opts.get("stage"),
            Some(SCPIArgValue::Scalar(Value::Int(3)))
        ));
        assert!(matches!(
            cmd.opts.get("cycle"),
            Some(SCPIArgValue::Scalar(Value::Int(10)))
        ));
    }

    #[test]
    fn test_scpi_command_with_comment() {
        let mut input = b"CMD arg1 # this is a comment\n" as &[u8];
        let cmd = parse_scpi_command(&mut input).unwrap();
        assert_eq!(cmd.command, "CMD");
        assert_eq!(cmd.args.len(), 1);
        assert_eq!(cmd.comment.as_deref(), Some("this is a comment"));
    }

    #[test]
    fn test_scpi_command_comma_list() {
        let mut input = b"CMD m4,x1,quant,500\n" as &[u8];
        let cmd = parse_scpi_command(&mut input).unwrap();
        assert_eq!(cmd.command, "CMD");
        assert_eq!(cmd.args.len(), 1);
        if let SCPIArgValue::List(items) = &cmd.args[0] {
            assert_eq!(items.len(), 4);
        } else {
            panic!("Expected list arg, got {:?}", cmd.args[0]);
        }
    }

    #[test]
    fn test_scpi_command_mixed_opts_and_args() {
        let mut input = b"CMD -opt=val arg1 -opt2=42 arg2\n" as &[u8];
        let cmd = parse_scpi_command(&mut input).unwrap();
        assert_eq!(cmd.command, "CMD");
        assert_eq!(cmd.args.len(), 2);
        assert!(
            matches!(cmd.opts.get("opt"), Some(SCPIArgValue::Scalar(Value::String(s))) if s == "val")
        );
        assert!(matches!(
            cmd.opts.get("opt2"),
            Some(SCPIArgValue::Scalar(Value::Int(42)))
        ));
    }

    #[test]
    fn test_scpi_command_quoted_string() {
        let mut input = b"CMD \"hello world\"\n" as &[u8];
        let cmd = parse_scpi_command(&mut input).unwrap();
        assert_eq!(cmd.args.len(), 1);
        assert!(
            matches!(&cmd.args[0], SCPIArgValue::Scalar(Value::QuotedString(s)) if s == "hello world")
        );
    }

    #[test]
    fn test_scpi_command_xml_quoted() {
        let mut input = b"CMD <quote>multiline\ncontent</quote>\n" as &[u8];
        let cmd = parse_scpi_command(&mut input).unwrap();
        assert_eq!(cmd.args.len(), 1);
    }

    #[test]
    fn test_scpi_command_no_newline() {
        let mut input = b"CMD arg1" as &[u8];
        let cmd = parse_scpi_command(&mut input).unwrap();
        assert_eq!(cmd.command, "CMD");
        assert_eq!(cmd.args.len(), 1);
    }

    #[test]
    fn test_scpi_command_bool_arg() {
        let mut input = b"CMD true false\n" as &[u8];
        let cmd = parse_scpi_command(&mut input).unwrap();
        assert_eq!(cmd.args.len(), 2);
        assert!(matches!(
            &cmd.args[0],
            SCPIArgValue::Scalar(Value::Bool(true))
        ));
        assert!(matches!(
            &cmd.args[1],
            SCPIArgValue::Scalar(Value::Bool(false))
        ));
    }

    #[test]
    fn test_scpi_command_nested_multiline() {
        let mut input = b"CMD <multiline.cmd>\nSUB1 arg1\nSUB2 arg2\n</multiline.cmd>\n" as &[u8];
        let cmd = parse_scpi_command(&mut input).unwrap();
        assert_eq!(cmd.args.len(), 1);
        if let SCPIArgValue::CommandBlock(cmds) = &cmd.args[0] {
            assert_eq!(cmds.len(), 2);
            assert_eq!(cmds[0].command, "SUB1");
            assert_eq!(cmds[1].command, "SUB2");
        } else {
            panic!("Expected CommandBlock, got {:?}", cmd.args[0]);
        }
    }
}
