use std::{collections::VecDeque, future::Future, io::Write, marker::PhantomData};

use log::{info, warn};
use thiserror::Error;

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
}

impl<T: TryFrom<OkResponse, Error = OkParseError>, E: From<ErrorResponse>> CommandReceiver<T, E> {
    pub async fn receive_response(&mut self) -> Result<Result<T, E>, ReceiveOkResponseError> {
        loop {
            match self.response.recv().await {
                None => return Err(ReceiveOkResponseError::ConnectionClosed),
                Some(MessageResponse::Ok { message, .. }) => return Ok(Ok(message.try_into()?)),
                Some(MessageResponse::CommandError { error, .. }) => return Ok(Err(error.into())),
                Some(MessageResponse::Next { .. }) => (),
                Some(MessageResponse::Message(message)) => {
                    return Err(ReceiveOkResponseError::UnexpectedMessage(message));
                }
            }
        }
    }

    pub async fn receive_next(&mut self) -> Result<Result<(), E>, ReceiveNextResponseError> {
        match self.response.recv().await {
            None => Err(ReceiveNextResponseError::ConnectionClosed),
            Some(MessageResponse::CommandError { error, .. }) => Ok(Err(error.into())),
            Some(MessageResponse::Next { .. }) => Ok(Ok(())),
            Some(MessageResponse::Ok { message, .. }) => {
                Err(ReceiveNextResponseError::UnexpectedOk(message))
            }
            Some(MessageResponse::Message(message)) => {
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
pub struct Subscribe(pub String);

impl CommandBuilder for Subscribe {
    const COMMAND: &'static [u8] = b"SUBS";
    type Response = ();
    type Error = ErrorResponse;
    fn args(&self) -> Option<Vec<Value>> {
        Some(vec![self.0.clone().into()])
    }
}

impl Subscribe {
    pub fn topic(topic: &str) -> Self {
        Self(topic.to_string())
    }
}

#[derive(Debug, Clone)]
pub enum AccessLevel {
    Guest,
    Observer,
    Controller,
    Administrator,
    Full,
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
        let level = value.args.first()
            .ok_or_else(|| OkParseError::UnexpectedValues(value.clone(), "missing access level argument".to_string()))?
            .clone()
            .try_into_string()?;
        AccessLevel::try_from(level).map_err(|_| {
            OkParseError::UnexpectedValues(value, "unexpected access level".to_string())
        })
    }
}

impl From<AccessLevel> for Value {
    fn from(level: AccessLevel) -> Self {
        Value::String(level.into())
    }
}

#[derive(Debug, Clone)]
pub struct AccessLevelSet(pub AccessLevel);

impl AccessLevelSet {
    pub fn level(level: AccessLevel) -> Self {
        Self(level)
    }
}

impl CommandBuilder for AccessLevelSet {
    type Response = ();
    type Error = ErrorResponse;
    const COMMAND: &'static [u8] = b"ACC";
    fn args(&self) -> Option<Vec<Value>> {
        Some(vec![self.0.clone().into()])
    }
}

#[derive(Debug, Clone)]
pub struct AccessLevelQuery;

impl CommandBuilder for AccessLevelQuery {
    type Response = AccessLevel;
    type Error = ErrorResponse;
    const COMMAND: &'static [u8] = b"ACC?";
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
    pub stage: String,
}

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
        let position_str = value
            .args
            .first()
            .ok_or_else(|| OkParseError::UnexpectedValues(value.clone(), "missing cover position argument".to_string()))?
            .clone()
            .try_into_string()?;
        match position_str.to_lowercase().as_str() {
            "up" => Ok(CoverPosition::Up),
            "down" => Ok(CoverPosition::Down),
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
        let position = value.args.first()
            .ok_or_else(|| OkParseError::UnexpectedValues(value.clone(), "missing cover position argument".to_string()))?
            .clone()
            .try_into_string()?;
        let on = match position.to_lowercase().as_str() {
            "up" | "on" | "true" => true, // FIXME
            "down" | "off" | "false" => false,
            _ => {
                return Err(OkParseError::UnexpectedValues(
                    value.clone(),
                    "unexpected cover position".to_string(),
                ))
            }
        };
        let temperature = value.args.get(1)
            .ok_or_else(|| OkParseError::UnexpectedValues(value.clone(), "missing temperature argument".to_string()))?
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
                format!("expected {} arguments, got {}", REQUIRED_ARGS, args_len)
            ));
        }

        let args = value.args.into_iter();
        let mut args_deque = args.into_iter().collect::<VecDeque<_>>();
        info!("args_deque: {:?}", args_deque);

        fn into_okresponse(value: Value) -> Result<OkResponse, OkParseError> {
            let x = OkResponse::try_from(value.try_into_string()?)?;
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
                PossibleRunProgress::Running(progress) => format!(
                    "Running {}: Stage {}, Cycle {}, Step {}",
                    progress.run_title, progress.stage, progress.cycle, progress.step
                ),
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
                let zone_num = zone_num.parse::<usize>()
                    .map_err(|_| OkParseError::UnexpectedValues(resp.clone(), format!("Invalid zone number: {}", zone_num)))?;
                if zone_num != zones.len() + 1 {
                    return Err(OkParseError::UnexpectedValues(
                        resp.clone(),
                        format!("Zone {} is out of range", zone_num),
                    ));
                }
                zones.push(value.clone().try_into_f64()
                    .map_err(|e| OkParseError::UnexpectedValues(resp.clone(), format!("Failed to parse zone temperature: {}", e)))?);
            } else if let Some(fan_num) = key.strip_prefix("Fan") {
                let fan_num = fan_num.parse::<usize>()
                    .map_err(|_| OkParseError::UnexpectedValues(resp.clone(), format!("Invalid fan number: {}", fan_num)))?;
                if fan_num != fans.len() + 1 {
                    return Err(OkParseError::UnexpectedValues(
                        resp.clone(),
                        format!("Fan {} is out of range", fan_num),
                    ));
                }
                fans.push(value.clone().try_into_f64()
                    .map_err(|e| OkParseError::UnexpectedValues(resp.clone(), format!("Failed to parse fan temperature: {}", e)))?);
            } else if key == "Cover" {
                cover = value.clone().try_into_f64()
                    .map_err(|e| OkParseError::UnexpectedValues(resp.clone(), format!("Failed to parse cover temperature: {}", e)))?;
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
                let zone_num = zone_num.parse::<usize>()
                    .map_err(|_| OkParseError::UnexpectedValues(resp.clone(), format!("Invalid zone number: {}", zone_num)))?;
                if zone_num != zones.len() + 1 {
                    return Err(OkParseError::UnexpectedValues(
                        resp.clone(),
                        format!("Zone {} is out of range", zone_num),
                    ));
                }
                zones.push(value.clone().try_into_bool()
                    .map_err(|e| OkParseError::UnexpectedValues(resp.clone(), format!("Failed to parse zone control: {}", e)))?);
            } else if let Some(fan_num) = key.strip_prefix("Fan") {
                let fan_num = fan_num.parse::<usize>()
                    .map_err(|_| OkParseError::UnexpectedValues(resp.clone(), format!("Invalid fan number: {}", fan_num)))?;
                if fan_num != fans.len() + 1 {
                    return Err(OkParseError::UnexpectedValues(
                        resp.clone(),
                        format!("Fan {} is out of range", fan_num),
                    ));
                }
                fans.push(value.clone().try_into_bool()
                    .map_err(|e| OkParseError::UnexpectedValues(resp.clone(), format!("Failed to parse fan control: {}", e)))?);
            } else if key == "Cover" {
                cover = value.clone().try_into_bool()
                    .map_err(|e| OkParseError::UnexpectedValues(resp.clone(), format!("Failed to parse cover control: {}", e)))?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::{OkResponse, Value};

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
}
