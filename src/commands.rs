use std::{collections::VecDeque, future::Future, io::Write, marker::PhantomData};

use log::{info, warn};
use thiserror::Error;

#[cfg(feature = "python")]
use pyo3::prelude::*;

use crate::{
    com::{QSConnection, QSConnectionError, ResponseReceiver, SendCommandError},
    parser::{ArgMap, Command, ErrorResponse, MessageResponse, OkResponse, ParseError, Value},
};

pub struct CommandReceiver<T: TryFrom<OkResponse>, E: From<ErrorResponse>> {
    pub message_content: Vec<u8>,
    pub response: ResponseReceiver,
    pub response_type: PhantomData<T>,
    pub error_type: PhantomData<E>,
}

#[derive(Debug, Error)]
pub enum OkParseError {
    #[error("Unexpected values: {1} ({0:?})")]
    UnexpectedValues(OkResponse, String),
    #[error("Parse error: {0:?}")]
    ParseError(#[from] ParseError),
}

#[derive(Debug, Error)]
pub enum ReceiveOkResponseError {
    #[error("Connection closed.")]
    ConnectionClosed,
    #[error("OK response parsing error: {0:?}")]
    ResponseParsingError(#[from] OkParseError),
    #[error("Unexpected message response: {0:?}")]
    UnexpectedMessage(crate::parser::LogMessage),
    #[error("Timeout waiting for response")]
    Timeout,
}

#[derive(Debug, Error)]
pub enum ReceiveNextResponseError {
    #[error("Connection closed.")]
    ConnectionClosed,
    #[error("Unexpected OK response.")]
    UnexpectedOk(OkResponse),
    #[error("Unexpected error response.")]
    UnexpectedError(ErrorResponse),
    #[error("Unexpected message response: {0:?}")]
    UnexpectedMessage(crate::parser::LogMessage),
    #[error("Timeout waiting for response")]
    Timeout,
}

impl<T: TryFrom<OkResponse, Error = OkParseError>, E: From<ErrorResponse>> CommandReceiver<T, E> {
    pub async fn receive_response(&mut self) -> Result<Result<T, E>, ReceiveOkResponseError> {
        // Delegate to the timeout-bounded receiver so a lost or never-sent
        // response cannot block forever (e.g. a half-open connection). This
        // applies the connection's initial timeout for the first message and
        // next_to_ok timeout after a NEXT, ignoring intermediate NEXTs.
        match self.response.get_response().await? {
            Ok(ok) => Ok(Ok(ok.try_into()?)),
            Err(error) => Ok(Err(error.into())),
        }
    }

    pub async fn receive_next(&mut self) -> Result<Result<(), E>, ReceiveNextResponseError> {
        match self.response.recv_initial().await {
            Err(ReceiveOkResponseError::Timeout) => Err(ReceiveNextResponseError::Timeout),
            Err(_) => Err(ReceiveNextResponseError::ConnectionClosed),
            Ok(MessageResponse::CommandError { error, .. }) => Ok(Err(error.into())),
            Ok(MessageResponse::Next { .. }) => Ok(Ok(())),
            Ok(MessageResponse::Ok { message, .. } | MessageResponse::Warning { message, .. }) => {
                Err(ReceiveNextResponseError::UnexpectedOk(message))
            }
            Ok(MessageResponse::Message(message)) => {
                Err(ReceiveNextResponseError::UnexpectedMessage(message))
            }
        }
    }
}

pub trait CommandBuilder: Clone + Send + Sync {
    const COMMAND: &'static [u8];
    fn args(&self) -> Option<Vec<Value>> {
        None
    }
    fn options(&self) -> Option<ArgMap> {
        None
    }
    fn write_command(&self, bytes: &mut impl Write) -> Result<(), QSConnectionError> {
        bytes.write_all(Self::COMMAND)?;
        if let Some(options) = self.options() {
            for (key, value) in options {
                bytes.write_all(b" -")?;
                bytes.write_all(key.as_bytes())?;
                bytes.write_all(b"=")?;
                value.write_bytes(bytes)?;
            }
        }
        if let Some(args) = self.args() {
            for arg in args {
                bytes.write_all(b" ")?;
                arg.write_bytes(bytes)?;
            }
        }
        Ok(())
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut v = Vec::new();
        self.write_command(&mut v).unwrap();
        v
    }

    // fn command(&self) -> Command {
    //     let mut c = Command::raw(Self::COMMAND);
    //     if let Some(args) = self.args() {
    //         for arg in args {
    //             c = c.with_arg(arg);
    //         }
    //     }
    //     if let Some(options) = self.options() {
    //         for (key, value) in options {
    //             c = c.with_option(&key, value);
    //         }
    //     }
    //     c
    // }

    type Response: TryFrom<OkResponse>;
    type Error: From<ErrorResponse>;
    // fn send(&self) -> impl Future<Output = Result<CommandReceiver<Self::Response>, QSConnectionError>> + Send;
    fn send(
        self,
        connection: &QSConnection,
    ) -> impl Future<Output = Result<CommandReceiver<Self::Response, Self::Error>, SendCommandError>>
           + Send {
        let content = self.to_bytes();
        let r = connection.send_command(self);
        async move {
            let r = r.await?;
            Ok(CommandReceiver {
                message_content: content,
                response: r,
                response_type: PhantomData,
                error_type: PhantomData,
            })
        }
    }
}

impl TryFrom<OkResponse> for () {
    type Error = OkParseError;
    fn try_from(value: OkResponse) -> Result<Self, Self::Error> {
        // OkResponse has no data we care about, just return unit
        if value.args.is_empty() && value.options.is_empty() {
            Ok(())
        } else {
            Err(OkParseError::UnexpectedValues(
                value,
                "response should have been empty".to_string(),
            ))
        }
    }
}

#[derive(Debug, Clone)]
pub struct Subscribe {
    pub topics: Vec<String>,
    pub timestamp: bool,
}

impl Subscribe {
    pub fn topic(topic: &str) -> Self {
        Self {
            topics: vec![topic.to_string()],
            timestamp: false,
        }
    }

    pub fn topics(topics: &[&str]) -> Self {
        Self {
            topics: topics.iter().map(|t| t.to_string()).collect(),
            timestamp: false,
        }
    }

    pub fn with_timestamp(mut self, timestamp: bool) -> Self {
        self.timestamp = timestamp;
        self
    }
}

impl CommandBuilder for Subscribe {
    const COMMAND: &'static [u8] = b"SUBS+";
    type Response = ();
    type Error = ErrorResponse;
    fn args(&self) -> Option<Vec<Value>> {
        Some(self.topics.iter().map(|t| t.clone().into()).collect())
    }
    fn options(&self) -> Option<ArgMap> {
        if self.timestamp {
            let mut opts = ArgMap::new();
            opts.insert("timestamp", Value::Bool(true));
            Some(opts)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "python", pyclass(frozen, module = "qslib._qslib"))]
pub enum AccessLevel {
    Guest,
    Observer,
    Controller,
    Administrator,
    Full,
}

impl AccessLevel {
    fn order(&self) -> u8 {
        match self {
            AccessLevel::Guest => 0,
            AccessLevel::Observer => 1,
            AccessLevel::Controller => 2,
            AccessLevel::Administrator => 3,
            AccessLevel::Full => 4,
        }
    }
}

impl PartialOrd for AccessLevel {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for AccessLevel {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.order().cmp(&other.order())
    }
}

impl From<AccessLevel> for String {
    fn from(level: AccessLevel) -> Self {
        match level {
            AccessLevel::Guest => "Guest".to_string(),
            AccessLevel::Observer => "Observer".to_string(),
            AccessLevel::Controller => "Controller".to_string(),
            AccessLevel::Administrator => "Administrator".to_string(),
            AccessLevel::Full => "Full".to_string(),
        }
    }
}

impl TryFrom<String> for AccessLevel {
    type Error = ();
    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.to_lowercase().as_str() {
            "guest" => Ok(AccessLevel::Guest),
            "observer" => Ok(AccessLevel::Observer),
            "controller" => Ok(AccessLevel::Controller),
            "administrator" => Ok(AccessLevel::Administrator),
            "full" => Ok(AccessLevel::Full),
            _ => Err(()),
        }
    }
}

impl TryFrom<OkResponse> for AccessLevel {
    type Error = OkParseError;
    fn try_from(value: OkResponse) -> Result<Self, Self::Error> {
        let level = value
            .args
            .first()
            .ok_or_else(|| {
                OkParseError::UnexpectedValues(
                    value.clone(),
                    "missing access level argument".to_string(),
                )
            })?
            .clone()
            .try_into_string()?;
        AccessLevel::try_from(level).map_err(|_| {
            OkParseError::UnexpectedValues(value, "unexpected access level".to_string())
        })
    }
}

#[cfg(feature = "python")]
impl AccessLevel {
    fn extract_comparable(other: &Bound<'_, pyo3::PyAny>) -> PyResult<AccessLevel> {
        if let Ok(level) = other.extract::<AccessLevel>() {
            return Ok(level);
        }
        if let Ok(s) = other.extract::<String>() {
            if let Ok(level) = AccessLevel::try_from(s) {
                return Ok(level);
            }
        }
        Err(pyo3::exceptions::PyValueError::new_err(format!(
            "Cannot compare AccessLevel with {:?}",
            other
        )))
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl AccessLevel {
    #[new]
    fn py_new(value: &Bound<'_, pyo3::PyAny>) -> PyResult<Self> {
        // Allow constructing from an existing AccessLevel (idempotent)
        if let Ok(level) = value.extract::<AccessLevel>() {
            return Ok(level);
        }
        // Allow constructing from a string
        if let Ok(s) = value.extract::<String>() {
            return AccessLevel::try_from(s).map_err(|_| {
                pyo3::exceptions::PyValueError::new_err(format!("Invalid access level: {}", value))
            });
        }
        Err(pyo3::exceptions::PyValueError::new_err(format!(
            "Invalid access level: {:?}",
            value
        )))
    }

    fn __str__(&self) -> String {
        String::from(self.clone())
    }

    fn __repr__(&self) -> String {
        format!("AccessLevel.{}", String::from(self.clone()))
    }

    fn __lt__(&self, other: &Bound<'_, pyo3::PyAny>) -> PyResult<bool> {
        let other_level = Self::extract_comparable(other)?;
        Ok(self.order() < other_level.order())
    }

    fn __le__(&self, other: &Bound<'_, pyo3::PyAny>) -> PyResult<bool> {
        let other_level = Self::extract_comparable(other)?;
        Ok(self.order() <= other_level.order())
    }

    fn __gt__(&self, other: &Bound<'_, pyo3::PyAny>) -> PyResult<bool> {
        let other_level = Self::extract_comparable(other)?;
        Ok(self.order() > other_level.order())
    }

    fn __ge__(&self, other: &Bound<'_, pyo3::PyAny>) -> PyResult<bool> {
        let other_level = Self::extract_comparable(other)?;
        Ok(self.order() >= other_level.order())
    }

    fn __eq__(&self, other: &Bound<'_, pyo3::PyAny>) -> PyResult<bool> {
        if let Ok(other_level) = other.extract::<AccessLevel>() {
            return Ok(self == &other_level);
        }
        if let Ok(s) = other.extract::<String>() {
            if let Ok(other_level) = AccessLevel::try_from(s) {
                return Ok(self == &other_level);
            }
        }
        Ok(false)
    }

    fn __hash__(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }

    #[getter]
    fn value(&self) -> String {
        String::from(self.clone())
    }
}

impl From<AccessLevel> for Value {
    fn from(level: AccessLevel) -> Self {
        Value::String(level.into())
    }
}

#[derive(Debug, Clone)]
pub struct AccessLevelSet {
    pub level: AccessLevel,
    pub exclusive: bool,
    pub stealth: bool,
}

impl AccessLevelSet {
    pub fn new(level: AccessLevel) -> Self {
        Self {
            level,
            exclusive: false,
            stealth: false,
        }
    }

    pub fn with_exclusive(mut self, exclusive: bool) -> Self {
        self.exclusive = exclusive;
        self
    }

    pub fn with_stealth(mut self, stealth: bool) -> Self {
        self.stealth = stealth;
        self
    }
}

impl CommandBuilder for AccessLevelSet {
    type Response = ();
    type Error = ErrorResponse;
    const COMMAND: &'static [u8] = b"ACC";
    fn args(&self) -> Option<Vec<Value>> {
        Some(vec![self.level.clone().into()])
    }
    fn options(&self) -> Option<ArgMap> {
        let mut opts = ArgMap::new();
        opts.insert("exclusive", Value::Bool(self.exclusive));
        opts.insert("stealth", Value::Bool(self.stealth));
        Some(opts)
    }
}

#[derive(Debug, Clone)]
pub struct AccessLevelQuery;

impl CommandBuilder for AccessLevelQuery {
    type Response = AccessLevel;
    type Error = ErrorResponse;
    const COMMAND: &'static [u8] = b"ACC?";
}

/// Query the server for a 6-digit random authentication key.
///
/// Requires Controller or higher access. The key is valid for 10 minutes
/// and grants Administrator access when used with the AUTH challenge-response.
/// Only one key is active at a time; calling this again before expiry returns
/// the same key.
#[derive(Debug, Clone)]
pub struct RandomKeyQuery;

/// The 6-digit random key returned by RAND?.
#[derive(Debug, Clone)]
pub struct RandomKey(pub String);

impl TryFrom<OkResponse> for RandomKey {
    type Error = OkParseError;
    fn try_from(value: OkResponse) -> Result<Self, Self::Error> {
        let key = value
            .args
            .first()
            .ok_or_else(|| {
                OkParseError::UnexpectedValues(value.clone(), "missing random key".to_string())
            })?
            .to_string();
        Ok(RandomKey(key))
    }
}

impl CommandBuilder for RandomKeyQuery {
    type Response = RandomKey;
    type Error = ErrorResponse;
    const COMMAND: &'static [u8] = b"RAND?";
}

#[derive(Debug, Clone)]

pub struct Unsubscribe(pub String);

impl CommandBuilder for Unsubscribe {
    type Response = ();
    type Error = ErrorResponse;
    const COMMAND: &'static [u8] = b"UNS";
    fn args(&self) -> Option<Vec<Value>> {
        Some(vec![self.0.clone().into()])
    }
}

impl Unsubscribe {
    pub fn topic(topic: &str) -> Self {
        Self(topic.to_string())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PowerStatus {
    On,
    Off,
}

#[derive(Debug, Clone)]
pub struct PowerQuery;

impl CommandBuilder for PowerQuery {
    type Response = PowerStatus;
    type Error = ErrorResponse;
    const COMMAND: &'static [u8] = b"POW?";
}

impl TryFrom<OkResponse> for PowerStatus {
    type Error = OkParseError;
    fn try_from(value: OkResponse) -> Result<Self, Self::Error> {
        match value.args.first() {
            Some(Value::String(s)) if s.to_uppercase() == "ON" => Ok(PowerStatus::On),
            Some(Value::String(s)) if s.to_uppercase() == "OFF" => Ok(PowerStatus::Off),
            Some(Value::Bool(true)) => Ok(PowerStatus::On),
            Some(Value::Bool(false)) => Ok(PowerStatus::Off),
            _ => Err(OkParseError::UnexpectedValues(
                value,
                "response should have been ON or OFF".to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PowerSet(pub PowerStatus);

impl CommandBuilder for PowerSet {
    type Response = ();
    type Error = ErrorResponse;
    const COMMAND: &'static [u8] = b"POW";
    fn args(&self) -> Option<Vec<Value>> {
        Some(vec![self.0.into()])
    }
}

impl PowerSet {
    pub fn on() -> Self {
        Self(PowerStatus::On)
    }

    pub fn off() -> Self {
        Self(PowerStatus::Off)
    }

    pub fn set(power: impl Into<PowerStatus>) -> Self {
        Self(power.into())
    }
}

impl From<PowerStatus> for Value {
    fn from(power: PowerStatus) -> Self {
        match power {
            PowerStatus::On => Value::String("ON".to_string()),
            PowerStatus::Off => Value::String("OFF".to_string()),
        }
    }
}

impl From<PowerStatus> for bool {
    fn from(power: PowerStatus) -> Self {
        match power {
            PowerStatus::On => true,
            PowerStatus::Off => false,
        }
    }
}

impl From<bool> for PowerStatus {
    fn from(value: bool) -> Self {
        if value {
            PowerStatus::On
        } else {
            PowerStatus::Off
        }
    }
}

impl From<PowerSet> for Command {
    fn from(command: PowerSet) -> Self {
        Command::new("POW").with_arg(match command.0 {
            PowerStatus::On => "ON",
            PowerStatus::Off => "OFF",
        })
    }
}

// '-RunMode=- -Step=- -RunTitle=- -Cycle=- -Stage=-'
#[derive(Debug, Clone)]
pub struct RunProgress {
    pub run_mode: String,
    pub step: String,
    pub run_title: String,
    pub cycle: String,
    /// Raw stage token reported by the machine ("PRERUN", "1".."N", "POSTRun", "-").
    /// This is the stage *name*; cf. `RunStatus::stage` for the numeric position.
    pub stage: String,
}

#[derive(Debug)]
pub enum PossibleRunProgress {
    Running(RunProgress),
    NotRunning(RunProgress),
}

impl TryFrom<OkResponse> for PossibleRunProgress {
    type Error = OkParseError;
    fn try_from(value: OkResponse) -> Result<Self, Self::Error> {
        let rp = RunProgress {
            run_mode: value
                .options
                .get("RunMode")
                .ok_or_else(|| {
                    OkParseError::UnexpectedValues(value.clone(), "missing RunMode".to_string())
                })?
                .to_string(),
            step: value
                .options
                .get("Step")
                .ok_or_else(|| {
                    OkParseError::UnexpectedValues(value.clone(), "missing Step".to_string())
                })?
                .to_string(),
            run_title: value
                .options
                .get("RunTitle")
                .ok_or_else(|| {
                    OkParseError::UnexpectedValues(value.clone(), "missing RunTitle".to_string())
                })?
                .to_string(),
            cycle: value
                .options
                .get("Cycle")
                .ok_or_else(|| {
                    OkParseError::UnexpectedValues(value.clone(), "missing Cycle".to_string())
                })?
                .to_string(),
            stage: value
                .options
                .get("Stage")
                .ok_or_else(|| {
                    OkParseError::UnexpectedValues(value.clone(), "missing Stage".to_string())
                })?
                .to_string(),
        };

        if rp.run_mode == "-" {
            if rp.step != "-" || rp.run_title != "-" || rp.cycle != "-" || rp.stage != "-" {
                warn!("Not running but some fields were not empty: {:?}", value);
            }
            return Ok(PossibleRunProgress::NotRunning(rp));
        }

        if !value.args.is_empty() {
            return Err(OkParseError::UnexpectedValues(
                value,
                "unexpected arguments".to_string(),
            ));
        }
        if rp.step == "-" || rp.run_title == "-" || rp.cycle == "-" || rp.stage == "-" {
            return Err(OkParseError::UnexpectedValues(
                value,
                "running but some fields were empty".to_string(),
            ));
        }
        for (key, _) in value.options.iter() {
            if !["RunMode", "Step", "RunTitle", "Cycle", "Stage"].contains(&key.as_str()) {
                return Err(OkParseError::UnexpectedValues(
                    value.clone(),
                    format!("unexpected option {}", key),
                ));
            }
        }

        Ok(PossibleRunProgress::Running(rp))
    }
}

#[derive(Debug, Clone)]
pub struct RunProgressQuery;

impl CommandBuilder for RunProgressQuery {
    type Response = PossibleRunProgress;
    type Error = ErrorResponse;
    const COMMAND: &'static [u8] = b"RUN?";
}

impl From<RunProgressQuery> for Command {
    fn from(_command: RunProgressQuery) -> Self {
        Command::new("RunProgress?")
    }
}

#[derive(Debug, Clone)]
pub enum DrawerStatus {
    Closed,
    Open,
    Unknown,
}

#[derive(Debug, Clone)]
pub enum CoverPosition {
    Up,
    Down,
    Unknown,
}

#[derive(Debug, Clone)]
pub struct CoverPositionQuery;

impl CommandBuilder for CoverPositionQuery {
    type Response = CoverPosition;
    type Error = ErrorResponse;
    const COMMAND: &'static [u8] = b"eng?";
}

impl TryFrom<OkResponse> for CoverPosition {
    type Error = OkParseError;
    fn try_from(value: OkResponse) -> Result<Self, Self::Error> {
        let first = match value.args.first() {
            Some(v) => v,
            None => return Ok(CoverPosition::Unknown),
        };
        // The parser may interpret "On"/"Off" as Bool(true)/Bool(false)
        let position_str = match first {
            Value::String(s) => s.to_lowercase(),
            Value::Bool(true) => "on".to_string(),
            Value::Bool(false) => "off".to_string(),
            other => other.to_string().to_lowercase(),
        };
        match position_str.as_str() {
            "up" | "on" => Ok(CoverPosition::Up),
            "down" | "off" => Ok(CoverPosition::Down),
            _ => {
                warn!("Unexpected cover position: {}", position_str);
                Ok(CoverPosition::Unknown)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct CoverHeatStatus {
    pub on: bool,
    pub temperature: f64,
}

// drawer?
// OK drawer? Closed

impl TryFrom<OkResponse> for DrawerStatus {
    type Error = OkParseError;
    fn try_from(value: OkResponse) -> Result<Self, Self::Error> {
        match value.args.first() {
            Some(Value::String(s)) if s == "Closed" => Ok(DrawerStatus::Closed),
            Some(Value::String(s)) if s == "Open" => Ok(DrawerStatus::Open),
            Some(Value::String(s)) if s == "Unknown" => Ok(DrawerStatus::Unknown),
            // The parser may interpret "Closed"/"Open" as booleans
            Some(Value::Bool(false)) => Ok(DrawerStatus::Closed),
            Some(Value::Bool(true)) => Ok(DrawerStatus::Open),
            _ => Err(OkParseError::UnexpectedValues(
                value.clone(),
                "unexpected drawer status".to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DrawerStatusQuery;

impl CommandBuilder for DrawerStatusQuery {
    type Response = DrawerStatus;
    type Error = ErrorResponse;
    const COMMAND: &'static [u8] = b"drawer?";
}

// cover?
// OK cover? UP 105.0

impl TryFrom<OkResponse> for CoverHeatStatus {
    type Error = OkParseError;
    fn try_from(value: OkResponse) -> Result<Self, Self::Error> {
        let first = value.args.first().ok_or_else(|| {
            OkParseError::UnexpectedValues(
                value.clone(),
                "missing cover position argument".to_string(),
            )
        })?;
        // The parser may interpret "On"/"Off" as Bool(true)/Bool(false)
        let on = match first {
            Value::Bool(b) => *b,
            Value::String(s) => match s.to_lowercase().as_str() {
                "up" | "on" | "true" => true,
                "down" | "off" | "false" => false,
                _ => {
                    return Err(OkParseError::UnexpectedValues(
                        value.clone(),
                        "unexpected cover position".to_string(),
                    ))
                }
            },
            _ => {
                return Err(OkParseError::UnexpectedValues(
                    value.clone(),
                    "unexpected cover position type".to_string(),
                ))
            }
        };
        let temperature = value
            .args
            .get(1)
            .ok_or_else(|| {
                OkParseError::UnexpectedValues(
                    value.clone(),
                    "missing temperature argument".to_string(),
                )
            })?
            .clone()
            .try_into_f64()?;
        Ok(CoverHeatStatus { on, temperature })
    }
}

#[derive(Debug, Clone)]
pub struct CoverHeatStatusQuery;

impl CommandBuilder for CoverHeatStatusQuery {
    type Response = CoverHeatStatus;
    type Error = ErrorResponse;
    const COMMAND: &'static [u8] = b"cover?";
}

#[derive(Debug)]
pub struct QuickStatus {
    pub power: PowerStatus,
    pub drawer: DrawerStatus,
    pub cover: CoverHeatStatus,
    pub temperature_control: TemperatureControlStatus,
    pub sample_temperatures: Vec<f64>,
    pub block_temperatures: Vec<f64>,
    pub set_temperatures: SetTemperatures,
    pub runprogress: PossibleRunProgress,
}

impl TryFrom<OkResponse> for QuickStatus {
    type Error = OkParseError;
    fn try_from(value: OkResponse) -> Result<Self, Self::Error> {
        const REQUIRED_ARGS: usize = 8;
        let args_len = value.args.len();
        if args_len < REQUIRED_ARGS {
            return Err(OkParseError::UnexpectedValues(
                value,
                format!("expected {} arguments, got {}", REQUIRED_ARGS, args_len),
            ));
        }

        let args = value.args.into_iter();
        let mut args_deque = args.into_iter().collect::<VecDeque<_>>();
        info!("args_deque: {:?}", args_deque);

        fn into_okresponse(value: Value) -> Result<OkResponse, OkParseError> {
            // Convert the value to a string for re-parsing as an OkResponse.
            // Bools need special handling since try_into_string doesn't cover them,
            // but the server may return "on"/"off"/"True"/"False" etc. which the
            // parser converts to Bool.
            let s = match value {
                Value::Bool(true) => "on".to_string(),
                Value::Bool(false) => "off".to_string(),
                other => other.try_into_string()?,
            };
            let x = OkResponse::try_from(s)?;
            Ok(x)
        }

        // We've already verified we have enough args, so these unwraps are safe
        let power = into_okresponse(args_deque.pop_front().unwrap())?.try_into()?;
        let drawer = into_okresponse(args_deque.pop_front().unwrap())?.try_into()?;
        let cover = into_okresponse(args_deque.pop_front().unwrap())?.try_into()?;
        let set_temperatures = into_okresponse(args_deque.pop_front().unwrap())?.try_into()?;
        let temperature_control = into_okresponse(args_deque.pop_front().unwrap())?.try_into()?;
        let sample_temperatures = into_okresponse(args_deque.pop_front().unwrap())?.try_into()?;
        let block_temperatures = into_okresponse(args_deque.pop_front().unwrap())?.try_into()?;
        let runprogress = into_okresponse(args_deque.pop_front().unwrap())?.try_into()?;

        Ok(QuickStatus {
            power,
            drawer,
            cover,
            temperature_control,
            sample_temperatures,
            block_temperatures,
            set_temperatures,
            runprogress,
        })
    }
}

impl std::fmt::Display for QuickStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Power: {:?}\nDrawer: {:?}\nCover: {:?}\nSample Temperatures: {:?}\nBlock Temperatures: {:?}",
            self.power,
            self.drawer,
            self.cover,
            self.sample_temperatures,
            self.block_temperatures,
        )
    }
}

impl QuickStatus {
    pub fn to_html(&self) -> String {
        format!(
            "<ul>
<li>Power: {:?} | Drawer: {:?} | Cover: {:?} ({:.1}°C)</li>
<li>Set Temperatures: {} {} Cover: {}</li>
<li>Current Temperatures - Sample: {} | Block: {}</li>
<li>Run Status: {}</li>
</ul>",
            self.power,
            self.drawer,
            {
                if self.cover.on {
                    "<span style=\"color: red\">on</span>"
                } else {
                    "<span style=\"color: blue\">off</span>"
                }
            },
            if self.cover.on {
                format!(
                    "<span style=\"color: red\">{:.1}</span>",
                    self.cover.temperature
                )
            } else {
                format!(
                    "<span style=\"color: gray\">{:.1}</span>",
                    self.cover.temperature
                )
            },
            self.set_temperatures
                .zones
                .iter()
                .zip(self.temperature_control.zones.iter())
                .enumerate()
                .map(|(i, (temp, enabled))| format!(
                    "Zone{}: {}{:.1}°C{}",
                    i + 1,
                    if *enabled { "<b>" } else { "<i>" },
                    temp,
                    if *enabled { "</b>" } else { "</i>" }
                ))
                .collect::<Vec<_>>()
                .join(" "),
            self.set_temperatures
                .fans
                .iter()
                .zip(self.temperature_control.fans.iter())
                .enumerate()
                .map(|(i, (temp, enabled))| format!(
                    "Fan{}: {}{:.1}°C{}",
                    i + 1,
                    if *enabled { "<b>" } else { "<i>" },
                    temp,
                    if *enabled { "</b>" } else { "</i>" }
                ))
                .collect::<Vec<_>>()
                .join(" "),
            if self.temperature_control.cover {
                format!("<b>{:.1}°C</b>", self.set_temperatures.cover)
            } else {
                format!("<i>{:.1}°C</i>", self.set_temperatures.cover)
            },
            self.sample_temperatures
                .iter()
                .map(|t| format!("{:.1}°C", t))
                .collect::<Vec<_>>()
                .join(", "),
            self.block_temperatures
                .iter()
                .map(|t| format!("{:.1}°C", t))
                .collect::<Vec<_>>()
                .join(", "),
            match &self.runprogress {
                PossibleRunProgress::Running(progress) => {
                    let escaped_title = progress
                        .run_title
                        .replace('&', "&amp;")
                        .replace('<', "&lt;")
                        .replace('>', "&gt;")
                        .replace('"', "&quot;");
                    format!(
                        "Running {}: Stage {}, Cycle {}, Step {}",
                        escaped_title, progress.stage, progress.cycle, progress.step
                    )
                }
                PossibleRunProgress::NotRunning(_) => "Not Running".to_string(),
            }
        )
    }
}

#[derive(Debug, Clone)]
pub struct QuickStatusQuery;

impl CommandBuilder for QuickStatusQuery {
    type Response = QuickStatus;
    type Error = ErrorResponse;
    const COMMAND: &'static [u8] = b"RET $(POW?) $(drawer?) $(cover?) $(TBC:SETT?) $(TBC:CONT?) $(TBC:SampleTemperatures?) $(TBC:BlockTemperatures?) $(RunProgress?)";

    fn write_command(&self, bytes: &mut impl Write) -> Result<(), QSConnectionError> {
        bytes.write_all(Self::COMMAND)?;
        Ok(())
    }
}

// TBC:SETT?
// OK TBC:SETT? -Zone1=25.0 -Zone2=25.0 -Zone3=25.0 -Zone4=25.0 -Zone5=25.0 -Zone6=25.0 -Fan1=44.0 -Cover=30.0
#[derive(Debug, Clone)]
pub struct SetTemperaturesQuery;

#[derive(Debug, Clone)]
pub struct SetTemperatures {
    pub zones: Vec<f64>,
    pub fans: Vec<f64>,
    pub cover: f64,
}

impl TryFrom<OkResponse> for SetTemperatures {
    type Error = OkParseError;
    fn try_from(resp: OkResponse) -> Result<Self, Self::Error> {
        let mut zones = Vec::new();
        let mut fans = Vec::new();
        let mut cover = 0.0;

        for (key, value) in resp.options.iter() {
            if let Some(zone_num) = key.strip_prefix("Zone") {
                let zone_num = zone_num.parse::<usize>().map_err(|_| {
                    OkParseError::UnexpectedValues(
                        resp.clone(),
                        format!("Invalid zone number: {}", zone_num),
                    )
                })?;
                if zone_num != zones.len() + 1 {
                    return Err(OkParseError::UnexpectedValues(
                        resp.clone(),
                        format!("Zone {} is out of range", zone_num),
                    ));
                }
                zones.push(value.clone().try_into_f64().map_err(|e| {
                    OkParseError::UnexpectedValues(
                        resp.clone(),
                        format!("Failed to parse zone temperature: {}", e),
                    )
                })?);
            } else if let Some(fan_num) = key.strip_prefix("Fan") {
                let fan_num = fan_num.parse::<usize>().map_err(|_| {
                    OkParseError::UnexpectedValues(
                        resp.clone(),
                        format!("Invalid fan number: {}", fan_num),
                    )
                })?;
                if fan_num != fans.len() + 1 {
                    return Err(OkParseError::UnexpectedValues(
                        resp.clone(),
                        format!("Fan {} is out of range", fan_num),
                    ));
                }
                fans.push(value.clone().try_into_f64().map_err(|e| {
                    OkParseError::UnexpectedValues(
                        resp.clone(),
                        format!("Failed to parse fan temperature: {}", e),
                    )
                })?);
            } else if key == "Cover" {
                cover = value.clone().try_into_f64().map_err(|e| {
                    OkParseError::UnexpectedValues(
                        resp.clone(),
                        format!("Failed to parse cover temperature: {}", e),
                    )
                })?;
            }
        }

        Ok(SetTemperatures { zones, fans, cover })
    }
}

impl CommandBuilder for SetTemperaturesQuery {
    type Response = SetTemperatures;
    type Error = ErrorResponse;
    const COMMAND: &'static [u8] = b"TBC:SETT?";
}
// TBC:SampleTemperatures?
// OK TBC:SampleTemperatures? 21.1804 21.1467 21.1609 21.1917 21.1596 21.1843

#[derive(Debug, Clone)]
pub struct SampleTemperaturesQuery;

impl CommandBuilder for SampleTemperaturesQuery {
    type Response = Vec<f64>;
    type Error = ErrorResponse;
    const COMMAND: &'static [u8] = b"TBC:SampleTemperatures?";
}

// TBC:CONT?
// OK TBC:CONT? -Zone1=False -Zone2=False -Zone3=False -Zone4=False -Zone5=False -Zone6=False -Fan1=False -Cover=False

#[derive(Debug, Clone)]
pub struct TemperatureControlStatus {
    pub zones: Vec<bool>,
    pub fans: Vec<bool>,
    pub cover: bool,
}

impl TryFrom<OkResponse> for TemperatureControlStatus {
    type Error = OkParseError;
    fn try_from(resp: OkResponse) -> Result<Self, Self::Error> {
        let mut zones = Vec::new();
        let mut fans = Vec::new();
        let mut cover = false;

        for (key, value) in resp.options.iter() {
            if let Some(zone_num) = key.strip_prefix("Zone") {
                let zone_num = zone_num.parse::<usize>().map_err(|_| {
                    OkParseError::UnexpectedValues(
                        resp.clone(),
                        format!("Invalid zone number: {}", zone_num),
                    )
                })?;
                if zone_num != zones.len() + 1 {
                    return Err(OkParseError::UnexpectedValues(
                        resp.clone(),
                        format!("Zone {} is out of range", zone_num),
                    ));
                }
                zones.push(value.clone().try_into_bool().map_err(|e| {
                    OkParseError::UnexpectedValues(
                        resp.clone(),
                        format!("Failed to parse zone control: {}", e),
                    )
                })?);
            } else if let Some(fan_num) = key.strip_prefix("Fan") {
                let fan_num = fan_num.parse::<usize>().map_err(|_| {
                    OkParseError::UnexpectedValues(
                        resp.clone(),
                        format!("Invalid fan number: {}", fan_num),
                    )
                })?;
                if fan_num != fans.len() + 1 {
                    return Err(OkParseError::UnexpectedValues(
                        resp.clone(),
                        format!("Fan {} is out of range", fan_num),
                    ));
                }
                fans.push(value.clone().try_into_bool().map_err(|e| {
                    OkParseError::UnexpectedValues(
                        resp.clone(),
                        format!("Failed to parse fan control: {}", e),
                    )
                })?);
            } else if key == "Cover" {
                cover = value.clone().try_into_bool().map_err(|e| {
                    OkParseError::UnexpectedValues(
                        resp.clone(),
                        format!("Failed to parse cover control: {}", e),
                    )
                })?;
            }
        }

        Ok(TemperatureControlStatus { zones, fans, cover })
    }
}

#[derive(Debug, Clone)]
pub struct TemperatureControlStatusQuery;

impl CommandBuilder for TemperatureControlStatusQuery {
    type Response = TemperatureControlStatus;
    type Error = ErrorResponse;
    const COMMAND: &'static [u8] = b"TBC:CONT?";
}

#[derive(Debug, Clone)]
pub struct BlockTemperaturesQuery;

impl CommandBuilder for BlockTemperaturesQuery {
    type Response = Vec<f64>;
    type Error = ErrorResponse;
    const COMMAND: &'static [u8] = b"TBC:BlockTemperatures?";
}

/// Query the number of temperature control zones.
///
/// The server returns the zone count dynamically (typically 6 for current
/// QuantStudio instruments). Use this instead of assuming `DEFAULT_NUM_ZONES`.
#[derive(Debug, Clone)]
pub struct ControlZonesQuery;

impl CommandBuilder for ControlZonesQuery {
    type Response = usize;
    type Error = ErrorResponse;
    const COMMAND: &'static [u8] = b"TBC:ControlZones?";
}

impl TryFrom<OkResponse> for usize {
    type Error = OkParseError;
    fn try_from(value: OkResponse) -> Result<Self, Self::Error> {
        value
            .args
            .first()
            .ok_or_else(|| {
                OkParseError::UnexpectedValues(value.clone(), "missing value".to_string())
            })?
            .clone()
            .try_into_i64()
            .map(|v| v as usize)
            .map_err(|e| OkParseError::UnexpectedValues(value, format!("expected integer: {}", e)))
    }
}

#[derive(Debug, Clone)]
pub struct AbortRun(pub String);

impl CommandBuilder for AbortRun {
    type Response = ();
    type Error = ErrorResponse;
    const COMMAND: &'static [u8] = b"AbortRun";
    fn args(&self) -> Option<Vec<Value>> {
        Some(vec![self.0.clone().into()])
    }
}

#[derive(Debug, Clone)]
pub struct StopRun(pub String);

impl CommandBuilder for StopRun {
    type Response = ();
    type Error = ErrorResponse;
    const COMMAND: &'static [u8] = b"StopRun";
    fn args(&self) -> Option<Vec<Value>> {
        Some(vec![self.0.clone().into()])
    }
}

impl TryFrom<OkResponse> for Vec<f64> {
    type Error = OkParseError;
    fn try_from(value: OkResponse) -> Result<Self, Self::Error> {
        let mut result = Vec::new();
        for v in &value.args {
            match v.clone().try_into_f64() {
                Ok(f) => result.push(f),
                Err(_) => {
                    return Err(OkParseError::UnexpectedValues(
                        value.clone(),
                        "not a float".to_string(),
                    ))
                }
            }
        }
        Ok(result)
    }
}

// ---- Status Parsing: compound RET responses ----

use std::collections::HashMap;

/// Status of the current run on the machine.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "python", pyclass(frozen, get_all, module = "qslib._qslib"))]
pub struct RunStatus {
    pub name: String,
    /// Zero-indexed stage number in the run's full stage sequence: PRERUN = 0,
    /// numbered stage "k" = k, POSTRUN = num_stages + 1, unset/unknown = -1.
    /// Matches the integer `stage` column of collected fluorescence data.
    pub stage: i64,
    /// Raw stage token reported by the machine: "PRERUN", "1".."N", "POSTRun", "-".
    pub stage_name: String,
    pub num_stages: i64,
    pub cycle: i64,
    pub num_cycles: i64,
    pub step: i64,
    pub point: i64,
    pub state: String,
}

impl RunStatus {
    /// The SCPI command to fetch run status via compound RET.
    pub const COMMAND: &'static [u8] = b"RET ${RunTitle:--} ${Stage:--1} $[ top.getChild('PROTOcolDEFinition').variables.get('${RunMacro}-Stages'.lower(), -1) ] ${Cycle:--1} $[ top.getChild('PROTOcolDEFinition').variables.get('${RunMacro}-Stage${Stage}-Count'.lower(), -1) ] ${Step:--1} ${Point:--1} $(ISTAT?)";

    /// Parse a compound RET response into a RunStatus.
    pub fn parse(response: &[u8]) -> Result<Self, OkParseError> {
        let s = std::str::from_utf8(response).map_err(|e| {
            OkParseError::UnexpectedValues(
                OkResponse {
                    args: vec![],
                    options: ArgMap::new(),
                },
                format!("invalid UTF-8: {}", e),
            )
        })?;
        let tokens = shell_words::split(s).map_err(|e| {
            OkParseError::UnexpectedValues(
                OkResponse {
                    args: vec![],
                    options: ArgMap::new(),
                },
                format!("shell split error: {}", e),
            )
        })?;
        if tokens.len() < 8 {
            return Err(OkParseError::UnexpectedValues(
                OkResponse {
                    args: vec![],
                    options: ArgMap::new(),
                },
                format!("expected 8 tokens, got {}", tokens.len()),
            ));
        }
        // Strip XML-like tags from name: <tag>content</tag> → content
        let name = regex::Regex::new(r"(<[\w.]+>)?([^<]*)(</[\w.]+>)?")
            .unwrap()
            .replace_all(&tokens[0], "$2")
            .to_string();
        let num_stages: i64 = tokens[2].parse().unwrap_or(-1);
        // Stage name and stage number are distinct: the machine reports a raw token
        // (PRERUN, "1".."N", POSTRun, or "-"), while the number is the zero-indexed
        // position in the run's full stage sequence (PRERUN=0, "k"=k, POSTRUN=N+1).
        let stage_name = tokens[1].clone();
        let stage = if stage_name.eq_ignore_ascii_case("PRERUN") {
            0
        } else if stage_name.eq_ignore_ascii_case("POSTRUN") {
            num_stages + 1
        } else {
            stage_name.parse().unwrap_or(-1)
        };
        Ok(RunStatus {
            name,
            stage,
            stage_name,
            num_stages,
            cycle: tokens[3].parse().unwrap_or(-1),
            num_cycles: tokens[4].parse().unwrap_or(-1),
            step: tokens[5].parse().unwrap_or(-1),
            point: tokens[6].parse().unwrap_or(-1),
            state: tokens[7].clone(),
        })
    }
}

impl std::fmt::Display for RunStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RunStatus(name={:?}, stage={}, stage_name={:?}, num_stages={}, cycle={}, num_cycles={}, step={}, point={}, state={:?})",
            self.name, self.stage, self.stage_name, self.num_stages, self.cycle, self.num_cycles, self.step, self.point, self.state
        )
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl RunStatus {
    #[staticmethod]
    fn from_bytes(response: &[u8]) -> PyResult<Self> {
        Self::parse(response).map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("{}", e)))
    }

    fn __repr__(&self) -> String {
        self.to_string()
    }

    fn __str__(&self) -> String {
        self.to_string()
    }

    #[staticmethod]
    fn command() -> &'static [u8] {
        Self::COMMAND
    }
}

/// Status of the machine hardware (temperatures, drawer, cover, etc.).
#[derive(Debug, Clone)]
#[cfg_attr(feature = "python", pyclass(frozen, module = "qslib._qslib"))]
pub struct MachineStatus {
    pub drawer: String,
    pub cover: String,
    pub lamp_status: String,
    pub sample_temperatures: Vec<f64>,
    pub block_temperatures: Vec<f64>,
    pub cover_temperature: f64,
    pub target_temperatures: HashMap<String, f64>,
    pub target_controlled: HashMap<String, bool>,
    pub led_temperature: f64,
}

impl MachineStatus {
    /// The SCPI command to fetch machine status via compound RET.
    pub const COMMAND: &'static [u8] = b"RET $(DRAWER?) $[ \"$(ENG?)\" or \"unknown\" ] $(LST?) $(TBC:SampleTemperatures?) $(TBC:BlockTemperatures?) $(TBC:CoverTemperatures?) $(TBC:SETT?) $(TBC:CONT?) $(LED:LEDTemperature?)";

    /// Parse a compound RET response into a MachineStatus.
    pub fn parse(response: &[u8]) -> Result<Self, OkParseError> {
        let s = std::str::from_utf8(response).map_err(|e| {
            OkParseError::UnexpectedValues(
                OkResponse {
                    args: vec![],
                    options: ArgMap::new(),
                },
                format!("invalid UTF-8: {}", e),
            )
        })?;
        let tokens = shell_words::split(s).map_err(|e| {
            OkParseError::UnexpectedValues(
                OkResponse {
                    args: vec![],
                    options: ArgMap::new(),
                },
                format!("shell split error: {}", e),
            )
        })?;
        if tokens.len() < 9 {
            return Err(OkParseError::UnexpectedValues(
                OkResponse {
                    args: vec![],
                    options: ArgMap::new(),
                },
                format!("expected 9 tokens, got {}", tokens.len()),
            ));
        }

        let parse_floats = |s: &str| -> Vec<f64> {
            s.split_whitespace()
                .filter_map(|v| v.parse().ok())
                .collect()
        };

        let kv_re = regex::Regex::new(r"-(\w+)=([\d.eE+-]+)").unwrap();
        let target_temperatures: HashMap<String, f64> = kv_re
            .captures_iter(&tokens[6])
            .filter_map(|c| {
                let key = c[1].to_string();
                let val: f64 = c[2].parse().ok()?;
                Some((key, val))
            })
            .collect();

        let bool_re = regex::Regex::new(r"-(\w+)=(True|False)").unwrap();
        let target_controlled: HashMap<String, bool> = bool_re
            .captures_iter(&tokens[7])
            .map(|c| {
                let key = c[1].to_string();
                let val = &c[2] == "True";
                (key, val)
            })
            .collect();

        Ok(MachineStatus {
            drawer: tokens[0].clone(),
            cover: tokens[1].clone(),
            lamp_status: tokens[2].clone(),
            sample_temperatures: parse_floats(&tokens[3]),
            block_temperatures: parse_floats(&tokens[4]),
            cover_temperature: tokens[5].parse().unwrap_or(0.0),
            target_temperatures,
            target_controlled,
            led_temperature: tokens[8].parse().unwrap_or(0.0),
        })
    }
}

impl std::fmt::Display for MachineStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "MachineStatus(drawer={:?}, cover={:?}, lamp_status={:?}, cover_temp={:.1}°C, led_temp={:.1}°C)",
            self.drawer, self.cover, self.lamp_status, self.cover_temperature, self.led_temperature
        )
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl MachineStatus {
    #[staticmethod]
    fn from_bytes(response: &[u8]) -> PyResult<Self> {
        Self::parse(response).map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("{}", e)))
    }

    #[getter]
    fn get_drawer(&self) -> &str {
        &self.drawer
    }
    #[getter]
    fn get_cover(&self) -> &str {
        &self.cover
    }
    #[getter]
    fn get_lamp_status(&self) -> &str {
        &self.lamp_status
    }
    #[getter]
    fn get_sample_temperatures(&self) -> Vec<f64> {
        self.sample_temperatures.clone()
    }
    #[getter]
    fn get_block_temperatures(&self) -> Vec<f64> {
        self.block_temperatures.clone()
    }
    #[getter]
    fn get_cover_temperature(&self) -> f64 {
        self.cover_temperature
    }
    #[getter]
    fn get_target_temperatures(&self) -> HashMap<String, f64> {
        self.target_temperatures.clone()
    }
    #[getter]
    fn get_target_controlled(&self) -> HashMap<String, bool> {
        self.target_controlled.clone()
    }
    #[getter]
    fn get_led_temperature(&self) -> f64 {
        self.led_temperature
    }

    fn __repr__(&self) -> String {
        self.to_string()
    }

    fn __str__(&self) -> String {
        self.to_string()
    }

    #[staticmethod]
    fn command() -> &'static [u8] {
        Self::COMMAND
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::{OkResponse, Value};

    #[test]
    fn test_access_level_set_with_flags() {
        let cmd = AccessLevelSet::new(AccessLevel::Controller)
            .with_exclusive(true)
            .with_stealth(false);
        let bytes = cmd.to_bytes();
        let s = String::from_utf8_lossy(&bytes);
        assert!(s.contains("ACC"));
        assert!(s.contains("-exclusive=true"));
        assert!(s.contains("-stealth=false"));
        assert!(s.contains("Controller"));
    }

    #[test]
    fn test_access_level_set_defaults() {
        let cmd = AccessLevelSet::new(AccessLevel::Observer);
        let bytes = cmd.to_bytes();
        let s = String::from_utf8_lossy(&bytes);
        assert!(s.contains("-exclusive=false"));
        assert!(s.contains("-stealth=false"));
        assert!(s.contains("Observer"));
    }

    #[test]
    fn test_sample_temperatures_response() {
        let ok_response = OkResponse {
            args: vec![
                Value::Float(21.1804),
                Value::Float(21.1467),
                Value::Float(21.1609),
                Value::Float(21.1917),
                Value::Float(21.1596),
                Value::Float(21.1843),
            ],
            options: ArgMap::new(),
        };

        let temps = Vec::<f64>::try_from(ok_response).unwrap();
        assert_eq!(
            temps,
            vec![21.1804, 21.1467, 21.1609, 21.1917, 21.1596, 21.1843]
        );
    }

    #[test]
    fn test_sample_temperatures_invalid_response() {
        let ok_response = OkResponse {
            args: vec![Value::Float(21.1804), Value::String("invalid".to_string())],
            options: ArgMap::new(),
        };

        let result = Vec::<f64>::try_from(ok_response);
        assert!(result.is_err());
    }

    #[test]
    fn test_set_temperatures_valid() {
        let ok_response = OkResponse {
            args: vec![],
            options: ArgMap::new()
                .with("Zone1", Value::Float(25.0))
                .with("Zone2", Value::Float(26.0))
                .with("Zone3", Value::Float(27.0))
                .with("Fan1", Value::Float(44.0))
                .with("Cover", Value::Float(30.0)),
        };

        let set_temps = SetTemperatures::try_from(ok_response).unwrap();
        assert_eq!(set_temps.zones, vec![25.0, 26.0, 27.0]);
        assert_eq!(set_temps.fans, vec![44.0]);
        assert_eq!(set_temps.cover, 30.0);
    }

    #[test]
    fn test_set_temperatures_out_of_order_zones() {
        let mut options = ArgMap::new();
        options.insert("Zone2", Value::Float(26.0));
        options.insert("Zone1", Value::Float(25.0));
        options.insert("Cover", Value::Float(30.0));

        let ok_response = OkResponse {
            args: vec![],
            options,
        };

        let result = SetTemperatures::try_from(ok_response);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Zone 2 is out of range"));
    }

    #[test]
    fn test_set_temperatures_out_of_order_fans() {
        let ok_response = OkResponse {
            args: vec![],
            options: ArgMap::new()
                .with("Fan2", Value::Float(45.0))
                .with("Cover", Value::Float(30.0)),
        };

        let result = SetTemperatures::try_from(ok_response);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Fan 2 is out of range"));
    }

    #[test]
    fn test_set_temperatures_empty() {
        let ok_response = OkResponse {
            args: vec![],
            options: ArgMap::new(),
        };

        let set_temps = SetTemperatures::try_from(ok_response).unwrap();
        assert!(set_temps.zones.is_empty());
        assert!(set_temps.fans.is_empty());
        assert_eq!(set_temps.cover, 0.0);
    }

    #[test]
    fn test_subscribe_single_topic() {
        let cmd = Subscribe::topic("Temperature");
        let bytes = cmd.to_bytes();
        let s = String::from_utf8_lossy(&bytes);
        assert_eq!(s.trim(), "SUBS+ Temperature");
    }

    #[test]
    fn test_subscribe_multiple_topics() {
        let cmd = Subscribe::topics(&["Temperature", "RunProgress"]);
        let bytes = cmd.to_bytes();
        let s = String::from_utf8_lossy(&bytes);
        assert!(s.contains("SUBS+"));
        assert!(s.contains("Temperature"));
        assert!(s.contains("RunProgress"));
    }

    #[test]
    fn test_subscribe_with_timestamp() {
        let cmd = Subscribe::topic("Temperature").with_timestamp(true);
        let bytes = cmd.to_bytes();
        let s = String::from_utf8_lossy(&bytes);
        assert!(s.contains("SUBS+"));
        assert!(s.contains("-timestamp=true"));
        assert!(s.contains("Temperature"));
    }

    // --- TryFrom response parsing tests ---

    // PowerStatus
    #[test]
    fn test_power_status_on() {
        let resp = OkResponse {
            args: vec![Value::String("ON".to_string())],
            options: ArgMap::new(),
        };
        let status = PowerStatus::try_from(resp).unwrap();
        assert_eq!(status, PowerStatus::On);
    }

    #[test]
    fn test_power_status_off() {
        let resp = OkResponse {
            args: vec![Value::String("OFF".to_string())],
            options: ArgMap::new(),
        };
        let status = PowerStatus::try_from(resp).unwrap();
        assert_eq!(status, PowerStatus::Off);
    }

    #[test]
    fn test_power_status_bool_true() {
        let resp = OkResponse {
            args: vec![Value::Bool(true)],
            options: ArgMap::new(),
        };
        let status = PowerStatus::try_from(resp).unwrap();
        assert_eq!(status, PowerStatus::On);
    }

    #[test]
    fn test_power_status_invalid() {
        let resp = OkResponse {
            args: vec![Value::String("MAYBE".to_string())],
            options: ArgMap::new(),
        };
        assert!(PowerStatus::try_from(resp).is_err());
    }

    // AccessLevel
    #[test]
    fn test_access_level_from_response() {
        let resp = OkResponse {
            args: vec![Value::String("Controller".to_string())],
            options: ArgMap::new(),
        };
        let level = AccessLevel::try_from(resp).unwrap();
        assert!(matches!(level, AccessLevel::Controller));
    }

    #[test]
    fn test_access_level_invalid_response() {
        let resp = OkResponse {
            args: vec![Value::String("Superuser".to_string())],
            options: ArgMap::new(),
        };
        assert!(AccessLevel::try_from(resp).is_err());
    }

    // DrawerStatus
    #[test]
    fn test_drawer_status_closed() {
        let resp = OkResponse {
            args: vec![Value::String("Closed".to_string())],
            options: ArgMap::new(),
        };
        let status = DrawerStatus::try_from(resp).unwrap();
        assert!(matches!(status, DrawerStatus::Closed));
    }

    #[test]
    fn test_drawer_status_invalid() {
        let resp = OkResponse {
            args: vec![Value::String("Stuck".to_string())],
            options: ArgMap::new(),
        };
        assert!(DrawerStatus::try_from(resp).is_err());
    }

    // CoverPosition
    #[test]
    fn test_cover_position_up() {
        let resp = OkResponse {
            args: vec![Value::String("Up".to_string())],
            options: ArgMap::new(),
        };
        let pos = CoverPosition::try_from(resp).unwrap();
        assert!(matches!(pos, CoverPosition::Up));
    }

    #[test]
    fn test_cover_position_down() {
        let resp = OkResponse {
            args: vec![Value::String("Down".to_string())],
            options: ArgMap::new(),
        };
        let pos = CoverPosition::try_from(resp).unwrap();
        assert!(matches!(pos, CoverPosition::Down));
    }

    #[test]
    fn test_cover_position_unknown() {
        let resp = OkResponse {
            args: vec![Value::String("Moving".to_string())],
            options: ArgMap::new(),
        };
        let pos = CoverPosition::try_from(resp).unwrap();
        assert!(matches!(pos, CoverPosition::Unknown));
    }

    // CoverHeatStatus
    #[test]
    fn test_cover_heat_status_on() {
        let resp = OkResponse {
            args: vec![Value::String("ON".to_string()), Value::Float(105.0)],
            options: ArgMap::new(),
        };
        let status = CoverHeatStatus::try_from(resp).unwrap();
        assert!(status.on);
        assert_eq!(status.temperature, 105.0);
    }

    #[test]
    fn test_cover_heat_status_off() {
        let resp = OkResponse {
            args: vec![Value::String("OFF".to_string()), Value::Float(25.0)],
            options: ArgMap::new(),
        };
        let status = CoverHeatStatus::try_from(resp).unwrap();
        assert!(!status.on);
        assert_eq!(status.temperature, 25.0);
    }

    #[test]
    fn test_cover_heat_status_missing_temp() {
        let resp = OkResponse {
            args: vec![Value::String("ON".to_string())],
            options: ArgMap::new(),
        };
        assert!(CoverHeatStatus::try_from(resp).is_err());
    }

    // TemperatureControlStatus
    #[test]
    fn test_temperature_control_status_with_zones() {
        let resp = OkResponse {
            args: vec![],
            options: ArgMap::new()
                .with("Zone1", Value::Bool(true))
                .with("Zone2", Value::Bool(false))
                .with("Fan1", Value::Bool(true))
                .with("Cover", Value::Bool(false)),
        };
        let status = TemperatureControlStatus::try_from(resp).unwrap();
        assert_eq!(status.zones, vec![true, false]);
        assert_eq!(status.fans, vec![true]);
        assert!(!status.cover);
    }

    #[test]
    fn test_temperature_control_status_empty() {
        let resp = OkResponse {
            args: vec![],
            options: ArgMap::new(),
        };
        let status = TemperatureControlStatus::try_from(resp).unwrap();
        assert!(status.zones.is_empty());
        assert!(status.fans.is_empty());
        assert!(!status.cover);
    }

    // PossibleRunProgress
    #[test]
    fn test_run_progress_not_running() {
        let resp = OkResponse {
            args: vec![],
            options: ArgMap::new()
                .with("RunMode", Value::String("-".to_string()))
                .with("Step", Value::String("-".to_string()))
                .with("RunTitle", Value::String("-".to_string()))
                .with("Cycle", Value::String("-".to_string()))
                .with("Stage", Value::String("-".to_string())),
        };
        let progress = PossibleRunProgress::try_from(resp).unwrap();
        assert!(matches!(progress, PossibleRunProgress::NotRunning(_)));
    }

    #[test]
    fn test_run_progress_running() {
        let resp = OkResponse {
            args: vec![],
            options: ArgMap::new()
                .with("RunMode", Value::String("Standard".to_string()))
                .with("Step", Value::String("1".to_string()))
                .with("RunTitle", Value::String("MyRun".to_string()))
                .with("Cycle", Value::String("5".to_string()))
                .with("Stage", Value::String("2".to_string())),
        };
        let progress = PossibleRunProgress::try_from(resp).unwrap();
        match progress {
            PossibleRunProgress::Running(rp) => {
                assert_eq!(rp.run_mode, "Standard");
                assert_eq!(rp.run_title, "MyRun");
                assert_eq!(rp.cycle, "5");
                assert_eq!(rp.stage, "2");
                assert_eq!(rp.step, "1");
            }
            _ => panic!("Expected Running"),
        }
    }

    #[test]
    fn test_run_progress_partial_dashes_error() {
        // Running mode but some fields are dashes - should error
        let resp = OkResponse {
            args: vec![],
            options: ArgMap::new()
                .with("RunMode", Value::String("Standard".to_string()))
                .with("Step", Value::String("-".to_string()))
                .with("RunTitle", Value::String("MyRun".to_string()))
                .with("Cycle", Value::String("5".to_string()))
                .with("Stage", Value::String("2".to_string())),
        };
        assert!(PossibleRunProgress::try_from(resp).is_err());
    }

    #[test]
    fn test_run_progress_unexpected_option() {
        let resp = OkResponse {
            args: vec![],
            options: ArgMap::new()
                .with("RunMode", Value::String("Standard".to_string()))
                .with("Step", Value::String("1".to_string()))
                .with("RunTitle", Value::String("MyRun".to_string()))
                .with("Cycle", Value::String("5".to_string()))
                .with("Stage", Value::String("2".to_string()))
                .with("Extra", Value::String("bad".to_string())),
        };
        assert!(PossibleRunProgress::try_from(resp).is_err());
    }

    // --- CommandBuilder::to_bytes tests ---

    #[test]
    fn test_power_query_to_bytes() {
        let bytes = PowerQuery.to_bytes();
        assert_eq!(&bytes, b"POW?");
    }

    #[test]
    fn test_power_set_on_to_bytes() {
        let bytes = PowerSet::on().to_bytes();
        assert_eq!(&bytes, b"POW ON");
    }

    #[test]
    fn test_power_set_off_to_bytes() {
        let bytes = PowerSet::off().to_bytes();
        assert_eq!(&bytes, b"POW OFF");
    }

    #[test]
    fn test_access_level_query_to_bytes() {
        let bytes = AccessLevelQuery.to_bytes();
        assert_eq!(&bytes, b"ACC?");
    }

    #[test]
    fn test_random_key_query_to_bytes() {
        let bytes = RandomKeyQuery.to_bytes();
        assert_eq!(&bytes, b"RAND?");
    }

    #[test]
    fn test_unsubscribe_to_bytes() {
        let bytes = Unsubscribe::topic("Temperature").to_bytes();
        assert_eq!(&bytes, b"UNS Temperature");
    }

    #[test]
    fn test_drawer_status_query_to_bytes() {
        let bytes = DrawerStatusQuery.to_bytes();
        assert_eq!(&bytes, b"drawer?");
    }

    #[test]
    fn test_abort_run_to_bytes() {
        let bytes = AbortRun("myrun".to_string()).to_bytes();
        assert_eq!(&bytes, b"AbortRun myrun");
    }

    #[test]
    fn test_control_zones_query_to_bytes() {
        let bytes = ControlZonesQuery.to_bytes();
        assert_eq!(&bytes, b"TBC:ControlZones?");
    }

    // --- Unit conversion tests ---

    #[test]
    fn test_power_status_from_bool() {
        assert_eq!(PowerStatus::from(true), PowerStatus::On);
        assert_eq!(PowerStatus::from(false), PowerStatus::Off);
    }

    #[test]
    fn test_power_status_into_bool() {
        assert_eq!(bool::from(PowerStatus::On), true);
        assert_eq!(bool::from(PowerStatus::Off), false);
    }

    #[test]
    fn test_access_level_from_string() {
        assert!(matches!(
            AccessLevel::try_from("guest".to_string()),
            Ok(AccessLevel::Guest)
        ));
        assert!(matches!(
            AccessLevel::try_from("OBSERVER".to_string()),
            Ok(AccessLevel::Observer)
        ));
        assert!(matches!(
            AccessLevel::try_from("Controller".to_string()),
            Ok(AccessLevel::Controller)
        ));
        assert!(matches!(
            AccessLevel::try_from("administrator".to_string()),
            Ok(AccessLevel::Administrator)
        ));
        assert!(matches!(
            AccessLevel::try_from("full".to_string()),
            Ok(AccessLevel::Full)
        ));
        assert!(AccessLevel::try_from("invalid".to_string()).is_err());
    }

    #[test]
    fn test_access_level_into_string() {
        assert_eq!(String::from(AccessLevel::Guest), "Guest");
        assert_eq!(String::from(AccessLevel::Controller), "Controller");
    }

    #[test]
    fn test_unit_try_from_ok_response_empty() {
        let resp = OkResponse {
            args: vec![],
            options: ArgMap::new(),
        };
        assert!(<()>::try_from(resp).is_ok());
    }

    #[test]
    fn test_unit_try_from_ok_response_nonempty() {
        let resp = OkResponse {
            args: vec![Value::String("unexpected".to_string())],
            options: ArgMap::new(),
        };
        assert!(<()>::try_from(resp).is_err());
    }

    // --- QuickStatus Display and to_html ---

    fn make_quick_status(
        running: bool,
        cover_on: bool,
        num_zones: usize,
        num_fans: usize,
    ) -> QuickStatus {
        let runprogress = if running {
            PossibleRunProgress::Running(RunProgress {
                run_mode: "Standard".to_string(),
                step: "1".to_string(),
                run_title: "TestRun".to_string(),
                cycle: "3".to_string(),
                stage: "2".to_string(),
            })
        } else {
            PossibleRunProgress::NotRunning(RunProgress {
                run_mode: "-".to_string(),
                step: "-".to_string(),
                run_title: "-".to_string(),
                cycle: "-".to_string(),
                stage: "-".to_string(),
            })
        };
        QuickStatus {
            power: PowerStatus::On,
            drawer: DrawerStatus::Closed,
            cover: CoverHeatStatus {
                on: cover_on,
                temperature: 105.0,
            },
            temperature_control: TemperatureControlStatus {
                zones: vec![true; num_zones],
                fans: vec![false; num_fans],
                cover: cover_on,
            },
            sample_temperatures: vec![25.0; num_zones],
            block_temperatures: vec![24.5; num_zones],
            set_temperatures: SetTemperatures {
                zones: vec![25.0; num_zones],
                fans: vec![44.0; num_fans],
                cover: 105.0,
            },
            runprogress,
        }
    }

    #[test]
    fn test_quick_status_display_not_running() {
        let qs = make_quick_status(false, false, 6, 1);
        let s = format!("{}", qs);
        assert!(s.contains("Power: On"));
        assert!(s.contains("Drawer: Closed"));
        assert!(s.contains("Cover:"));
    }

    #[test]
    fn test_quick_status_display_running() {
        let qs = make_quick_status(true, true, 6, 1);
        let s = format!("{}", qs);
        assert!(s.contains("Power: On"));
        assert!(s.contains("Drawer: Closed"));
    }

    #[test]
    fn test_quick_status_to_html_not_running_cover_off() {
        let qs = make_quick_status(false, false, 6, 1);
        let html = qs.to_html();
        assert!(html.contains("Not Running"));
        // {:?} wraps the &str in quotes, so check for the span content
        assert!(html.contains("color: blue"));
        assert!(html.contains("off"));
        // Zones should be bold (enabled)
        assert!(html.contains("<b>25.0°C</b>"));
        // Fans should be italic (disabled)
        assert!(html.contains("<i>44.0°C</i>"));
    }

    #[test]
    fn test_quick_status_to_html_running_cover_on() {
        let mut qs = make_quick_status(true, true, 6, 1);
        // Set a title with special HTML characters
        if let PossibleRunProgress::Running(ref mut rp) = qs.runprogress {
            rp.run_title = "Test&<Run>".to_string();
        }
        let html = qs.to_html();
        assert!(html.contains("color: red"));
        // HTML entity escaping
        assert!(html.contains("Test&amp;&lt;Run&gt;"));
        assert!(html.contains("Running"));
        assert!(html.contains("Stage 2"));
        assert!(html.contains("Cycle 3"));
        assert!(html.contains("Step 1"));
    }

    #[test]
    fn test_quick_status_to_html_single_zone_fan() {
        let qs = make_quick_status(false, true, 1, 1);
        let html = qs.to_html();
        assert!(html.contains("Zone1:"));
        assert!(html.contains("Fan1:"));
        // Cover should be bold (on)
        assert!(html.contains("<b>105.0°C</b>"));
    }

    #[test]
    fn test_quick_status_try_from_too_few_args() {
        let resp = OkResponse {
            args: vec![
                Value::String("ON".to_string()),
                Value::String("Closed".to_string()),
            ],
            options: ArgMap::new(),
        };
        let result = QuickStatus::try_from(resp);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("expected 8"));
    }

    // --- CoverPosition edge cases ---

    #[test]
    fn test_cover_position_from_bool_true() {
        let resp = OkResponse {
            args: vec![Value::Bool(true)],
            options: ArgMap::new(),
        };
        let pos = CoverPosition::try_from(resp).unwrap();
        assert!(matches!(pos, CoverPosition::Up));
    }

    #[test]
    fn test_cover_position_from_bool_false() {
        let resp = OkResponse {
            args: vec![Value::Bool(false)],
            options: ArgMap::new(),
        };
        let pos = CoverPosition::try_from(resp).unwrap();
        assert!(matches!(pos, CoverPosition::Down));
    }

    #[test]
    fn test_cover_position_from_unknown_string() {
        let resp = OkResponse {
            args: vec![Value::String("Wiggling".to_string())],
            options: ArgMap::new(),
        };
        let pos = CoverPosition::try_from(resp).unwrap();
        assert!(matches!(pos, CoverPosition::Unknown));
    }

    #[test]
    fn test_cover_position_from_empty_args() {
        let resp = OkResponse {
            args: vec![],
            options: ArgMap::new(),
        };
        let pos = CoverPosition::try_from(resp).unwrap();
        assert!(matches!(pos, CoverPosition::Unknown));
    }

    // --- CoverHeatStatus edge cases ---

    #[test]
    fn test_cover_heat_from_bool_on() {
        let resp = OkResponse {
            args: vec![Value::Bool(true), Value::Float(105.0)],
            options: ArgMap::new(),
        };
        let status = CoverHeatStatus::try_from(resp).unwrap();
        assert!(status.on);
        assert_eq!(status.temperature, 105.0);
    }

    #[test]
    fn test_cover_heat_from_string_off() {
        let resp = OkResponse {
            args: vec![Value::String("Off".to_string()), Value::Float(25.0)],
            options: ArgMap::new(),
        };
        let status = CoverHeatStatus::try_from(resp).unwrap();
        assert!(!status.on);
        assert_eq!(status.temperature, 25.0);
    }

    #[test]
    fn test_cover_heat_invalid_position_string() {
        let resp = OkResponse {
            args: vec![Value::String("maybe".to_string()), Value::Float(25.0)],
            options: ArgMap::new(),
        };
        assert!(CoverHeatStatus::try_from(resp).is_err());
    }

    #[test]
    fn test_cover_heat_invalid_type() {
        let resp = OkResponse {
            args: vec![Value::Int(42), Value::Float(25.0)],
            options: ArgMap::new(),
        };
        assert!(CoverHeatStatus::try_from(resp).is_err());
    }

    // --- DrawerStatus from Bool ---

    #[test]
    fn test_drawer_status_from_bool_true() {
        let resp = OkResponse {
            args: vec![Value::Bool(true)],
            options: ArgMap::new(),
        };
        let status = DrawerStatus::try_from(resp).unwrap();
        assert!(matches!(status, DrawerStatus::Open));
    }

    #[test]
    fn test_drawer_status_from_bool_false() {
        let resp = OkResponse {
            args: vec![Value::Bool(false)],
            options: ArgMap::new(),
        };
        let status = DrawerStatus::try_from(resp).unwrap();
        assert!(matches!(status, DrawerStatus::Closed));
    }

    // --- PowerStatus from Bool false ---

    #[test]
    fn test_power_status_bool_false() {
        let resp = OkResponse {
            args: vec![Value::Bool(false)],
            options: ArgMap::new(),
        };
        let status = PowerStatus::try_from(resp).unwrap();
        assert_eq!(status, PowerStatus::Off);
    }

    #[test]
    fn test_power_status_from_float_error() {
        let resp = OkResponse {
            args: vec![Value::Float(1.0)],
            options: ArgMap::new(),
        };
        assert!(PowerStatus::try_from(resp).is_err());
    }

    // --- PossibleRunProgress edge cases ---

    #[test]
    fn test_run_progress_missing_run_mode() {
        let resp = OkResponse {
            args: vec![],
            options: ArgMap::new()
                .with("Step", Value::String("-".to_string()))
                .with("RunTitle", Value::String("-".to_string()))
                .with("Cycle", Value::String("-".to_string()))
                .with("Stage", Value::String("-".to_string())),
        };
        assert!(PossibleRunProgress::try_from(resp).is_err());
    }

    #[test]
    fn test_run_progress_not_running_with_nonempty() {
        // Not running (RunMode="-") but step is not "-" — should warn but still Ok
        let resp = OkResponse {
            args: vec![],
            options: ArgMap::new()
                .with("RunMode", Value::String("-".to_string()))
                .with("Step", Value::String("1".to_string()))
                .with("RunTitle", Value::String("-".to_string()))
                .with("Cycle", Value::String("-".to_string()))
                .with("Stage", Value::String("-".to_string())),
        };
        let result = PossibleRunProgress::try_from(resp).unwrap();
        assert!(matches!(result, PossibleRunProgress::NotRunning(_)));
    }

    #[test]
    fn test_run_progress_running_with_args_error() {
        let resp = OkResponse {
            args: vec![Value::String("extra".to_string())],
            options: ArgMap::new()
                .with("RunMode", Value::String("Standard".to_string()))
                .with("Step", Value::String("1".to_string()))
                .with("RunTitle", Value::String("MyRun".to_string()))
                .with("Cycle", Value::String("5".to_string()))
                .with("Stage", Value::String("2".to_string())),
        };
        assert!(PossibleRunProgress::try_from(resp).is_err());
    }

    // --- CommandBuilder to_bytes for remaining commands ---

    #[test]
    fn test_stop_run_to_bytes() {
        let bytes = StopRun("myrun".to_string()).to_bytes();
        assert_eq!(&bytes, b"StopRun myrun");
    }

    #[test]
    fn test_set_temperatures_invalid_zone_key() {
        let ok_response = OkResponse {
            args: vec![],
            options: ArgMap::new()
                .with("ZoneABC", Value::Float(25.0))
                .with("Cover", Value::Float(30.0)),
        };
        let result = SetTemperatures::try_from(ok_response);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid zone number"));
    }

    #[test]
    fn test_set_temperatures_invalid_fan_key() {
        let ok_response = OkResponse {
            args: vec![],
            options: ArgMap::new()
                .with("FanXYZ", Value::Float(44.0))
                .with("Cover", Value::Float(30.0)),
        };
        let result = SetTemperatures::try_from(ok_response);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid fan number"));
    }

    #[test]
    fn test_temperature_control_invalid_zone_key() {
        let ok_response = OkResponse {
            args: vec![],
            options: ArgMap::new()
                .with("ZoneABC", Value::Bool(true))
                .with("Cover", Value::Bool(false)),
        };
        let result = TemperatureControlStatus::try_from(ok_response);
        assert!(result.is_err());
    }

    #[test]
    fn test_temperature_control_out_of_order_zones() {
        let mut options = ArgMap::new();
        options.insert("Zone2", Value::Bool(true));
        options.insert("Zone1", Value::Bool(false));
        options.insert("Cover", Value::Bool(false));

        let ok_response = OkResponse {
            args: vec![],
            options,
        };

        let result = TemperatureControlStatus::try_from(ok_response);
        assert!(result.is_err());
    }

    #[test]
    fn test_temperature_control_out_of_order_fans() {
        let ok_response = OkResponse {
            args: vec![],
            options: ArgMap::new()
                .with("Fan2", Value::Bool(true))
                .with("Cover", Value::Bool(false)),
        };
        let result = TemperatureControlStatus::try_from(ok_response);
        assert!(result.is_err());
    }

    #[test]
    fn test_quick_status_query_to_bytes() {
        let bytes = QuickStatusQuery.to_bytes();
        assert!(bytes.starts_with(b"RET $(POW?)"));
    }

    // ---- RunStatus tests ----

    #[test]
    fn test_run_status_parse_basic() {
        let response = b"TestRun 2 5 3 10 1 42 Running";
        let status = RunStatus::parse(response).unwrap();
        assert_eq!(status.name, "TestRun");
        assert_eq!(status.stage, 2);
        assert_eq!(status.stage_name, "2");
        assert_eq!(status.num_stages, 5);
        assert_eq!(status.cycle, 3);
        assert_eq!(status.num_cycles, 10);
        assert_eq!(status.step, 1);
        assert_eq!(status.point, 42);
        assert_eq!(status.state, "Running");
    }

    #[test]
    fn test_run_status_parse_defaults() {
        let response = b"- -1 -1 -1 -1 -1 -1 Idle";
        let status = RunStatus::parse(response).unwrap();
        assert_eq!(status.name, "-");
        assert_eq!(status.stage, -1);
        // The RunStatus command defaults an unset stage to "-1" (${Stage:--1}).
        assert_eq!(status.stage_name, "-1");
        assert_eq!(status.state, "Idle");
    }

    #[test]
    fn test_run_status_parse_prerun_stage() {
        let response = b"MyRun PRERUN 5 1 10 1 0 Running";
        let status = RunStatus::parse(response).unwrap();
        assert_eq!(status.stage, 0);
        assert_eq!(status.stage_name, "PRERUN");
    }

    #[test]
    fn test_run_status_parse_postrun_stage() {
        let response = b"MyRun POSTRun 5 1 10 1 0 Complete";
        let status = RunStatus::parse(response).unwrap();
        // POSTRUN sits just past the last numbered stage: num_stages + 1.
        assert_eq!(status.stage, 6);
        assert_eq!(status.num_stages, 5);
        assert_eq!(status.stage_name, "POSTRun");
    }

    #[test]
    fn test_run_status_parse_xml_tag_name() {
        let response = b"\"<run.name>Test Run</run.name>\" 1 3 1 10 1 5 Running";
        let status = RunStatus::parse(response).unwrap();
        assert_eq!(status.name, "Test Run");
    }

    #[test]
    fn test_run_status_parse_quoted_name() {
        let response = b"\"My Experiment\" 1 3 1 10 1 5 Running";
        let status = RunStatus::parse(response).unwrap();
        assert_eq!(status.name, "My Experiment");
    }

    #[test]
    fn test_run_status_parse_too_few_tokens() {
        let response = b"TestRun 2 5";
        assert!(RunStatus::parse(response).is_err());
    }

    #[test]
    fn test_run_status_command() {
        assert!(RunStatus::COMMAND.starts_with(b"RET "));
    }

    // ---- MachineStatus tests ----

    #[test]
    fn test_machine_status_parse_basic() {
        let response = br#"Closed On off "25.0 25.1 25.2 25.3 25.4 25.5" "24.9 25.0 25.1 25.2 25.3 25.4" 30.5 "-Zone1=25.0 -Zone2=25.0 -Fan1=44.0 -Cover=30.0" "-Zone1=True -Zone2=True -Fan1=True -Cover=True" 35.2"#;
        let status = MachineStatus::parse(response).unwrap();
        assert_eq!(status.drawer, "Closed");
        assert_eq!(status.cover, "On");
        assert_eq!(status.lamp_status, "off");
        assert_eq!(status.sample_temperatures.len(), 6);
        assert!((status.sample_temperatures[0] - 25.0).abs() < 0.01);
        assert_eq!(status.block_temperatures.len(), 6);
        assert!((status.cover_temperature - 30.5).abs() < 0.01);
        assert_eq!(status.target_temperatures.len(), 4);
        assert!((status.target_temperatures["Zone1"] - 25.0).abs() < 0.01);
        assert!((status.target_temperatures["Fan1"] - 44.0).abs() < 0.01);
        assert_eq!(status.target_controlled.len(), 4);
        assert_eq!(status.target_controlled["Zone1"], true);
        assert_eq!(status.target_controlled["Cover"], true);
        assert!((status.led_temperature - 35.2).abs() < 0.01);
    }

    #[test]
    fn test_machine_status_parse_too_few_tokens() {
        let response = b"Closed On off";
        assert!(MachineStatus::parse(response).is_err());
    }

    #[test]
    fn test_machine_status_command() {
        assert!(MachineStatus::COMMAND.starts_with(b"RET "));
    }
}
