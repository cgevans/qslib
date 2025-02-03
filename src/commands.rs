use std::{collections::VecDeque, future::Future, io::Write, marker::PhantomData};

use indexmap::IndexMap;
use thiserror::Error;
use tokio::sync::mpsc;

use crate::{
    com::{QSConnection, QSConnectionError, ResponseReceiver},
    parser::{Command, ErrorResponse, MessageResponse, OkResponse, ParseError, Value},
};

pub struct CommandReceiver<T: TryFrom<OkResponse>> {
    pub message_content: Vec<u8>,
    pub response: ResponseReceiver,
    pub response_type: PhantomData<T>,
}

#[derive(Debug, Error)]
pub enum OkParseError {
    #[error("Unexpected values: {1} ({0:?})")]
    UnexpectedValues(OkResponse, String),
    #[error("Parse error: {0:?}")]
    ParseError(#[from] ParseError),
}

#[derive(Debug, Error)]
pub enum CommandResponseError {
    #[error("Connection closed.")]
    ConnectionClosed,
    #[error("Command response error: {0:?}")]
    CommandResponseError(ErrorResponse),
    #[error("Response parsing error: {0:?}")]
    ResponseParsingError(#[from] OkParseError),
    #[error("Received OK when expecting NEXT")]
    UnexpectedOk(OkResponse),
}

impl<T: TryFrom<OkResponse, Error = OkParseError>> CommandReceiver<T> {
    pub async fn recv_response(&mut self) -> Result<T, CommandResponseError> {
        loop {
            match self.response.recv().await {
                None => return Err(CommandResponseError::ConnectionClosed),
                Some(MessageResponse::Ok { message, .. }) => return Ok(message.try_into()?),
                Some(MessageResponse::Error { error, .. }) => {
                    return Err(CommandResponseError::CommandResponseError(error))
                }
                Some(MessageResponse::Next { .. }) => (),
                Some(MessageResponse::Message(message)) => panic!(
                    "Message response to command should not be possible: {:?}",
                    message
                ),
            }
        }
    }

    pub async fn recv_next(&mut self) -> Result<(), CommandResponseError> {
        match self.response.recv().await {
            None => return Err(CommandResponseError::ConnectionClosed),
            Some(MessageResponse::Error { error, .. }) => {
                return Err(CommandResponseError::CommandResponseError(error))
            }
            Some(MessageResponse::Next { .. }) => Ok(()),
            Some(MessageResponse::Ok { message, .. }) => {
                return Err(CommandResponseError::UnexpectedOk(message))
            }
            Some(MessageResponse::Message(message)) => {
                panic!(
                    "Message response to command should not be possible: {:?}",
                    message
                )
            }
        }
    }
}

pub trait CommandBuilder: Clone + Send + Sync {
    const COMMAND: &'static [u8];
    fn args(&self) -> Option<Vec<Value>> {
        None
    }
    fn options(&self) -> Option<IndexMap<String, Value>> {
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
    // fn send(&self) -> impl Future<Output = Result<CommandReceiver<Self::Response>, QSConnectionError>> + Send;
    fn send(
        self,
        connection: &mut QSConnection,
    ) -> impl Future<Output = Result<CommandReceiver<Self::Response>, QSConnectionError>> + Send
    {
        let content = self.to_bytes();
        let r = connection.send_command(self);
        async move {
            let r = r.await?;
            Ok(CommandReceiver {
                message_content: content,
                response: r,
                response_type: PhantomData,
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

impl From<AccessLevel> for Value {
    fn from(level: AccessLevel) -> Self {
        Value::String(level.into())
    }
}

#[derive(Debug, Clone)]
pub struct AccessSet(pub AccessLevel);


impl AccessSet {
    pub fn level(level: AccessLevel) -> Self {
        Self(level)
    }
}

impl CommandBuilder for AccessSet {
    type Response = ();
    const COMMAND: &'static [u8] = b"ACC";
    fn args(&self) -> Option<Vec<Value>> {
        Some(vec![self.0.clone().into()])
    }
}

#[derive(Debug, Clone)]

pub struct Unsubscribe(pub String);

impl CommandBuilder for Unsubscribe {
    type Response = ();
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
pub enum Power {
    On,
    Off,
}

#[derive(Debug, Clone)]
pub struct PowerQuery;

impl CommandBuilder for PowerQuery {
    type Response = Power;
    const COMMAND: &'static [u8] = b"POW?";
}

impl TryFrom<OkResponse> for Power {
    type Error = OkParseError;
    fn try_from(value: OkResponse) -> Result<Self, Self::Error> {
        match value.args.get(0) {
            Some(Value::String(s)) if s == "ON" => Ok(Power::On),
            Some(Value::String(s)) if s == "OFF" => Ok(Power::Off),
            _ => Err(OkParseError::UnexpectedValues(
                value,
                "response should have been ON or OFF".to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PowerSet(pub Power);

impl CommandBuilder for PowerSet {
    type Response = ();
    const COMMAND: &'static [u8] = b"POW";
    fn args(&self) -> Option<Vec<Value>> {
        Some(vec![self.0.into()])
    }
}

impl PowerSet {
    pub fn on() -> Self {
        Self(Power::On)
    }

    pub fn off() -> Self {
        Self(Power::Off)
    }

    pub fn set(power: impl Into<Power>) -> Self {
        Self(power.into())
    }
}

impl From<Power> for Value {
    fn from(power: Power) -> Self {
        match power {
            Power::On => Value::String("ON".to_string()),
            Power::Off => Value::String("OFF".to_string()),
        }
    }
}

impl From<Power> for bool {
    fn from(power: Power) -> Self {
        match power {
            Power::On => true,
            Power::Off => false,
        }
    }
}

impl From<bool> for Power {
    fn from(value: bool) -> Self {
        if value {
            Power::On
        } else {
            Power::Off
        }
    }
}

impl From<PowerSet> for Command {
    fn from(command: PowerSet) -> Self {
        Command::new("POW").with_arg(match command.0 {
            Power::On => "ON",
            Power::Off => "OFF",
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
    NotRunning,
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
                return Err(OkParseError::UnexpectedValues(
                    value,
                    "not running but some fields were not empty".to_string(),
                ));
            }
            return Ok(PossibleRunProgress::NotRunning);
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
    const COMMAND: &'static [u8] = b"RUN?";
}

impl From<RunProgressQuery> for Command {
    fn from(_command: RunProgressQuery) -> Self {
        Command::new("RunProgress?")
    }
}

pub struct QuickStatus {
    pub power: Power,
    // pub drawer: Drawer,
    // pub cover: Cover,
    pub sample_temperatures: Vec<f64>,
    pub block_temperatures: Vec<f64>,
    pub set_temperatures: Vec<f64>,
    // pub run_title: String,
    // pub run_mode: String,
    // pub step: String,
    // pub cycle: String,
    // pub stage: String,
}

impl TryFrom<OkResponse> for QuickStatus {
    type Error = OkParseError;
    fn try_from(value: OkResponse) -> Result<Self, Self::Error> {
        let args = value.args.into_iter();
        let mut args_deque = args.into_iter().collect::<VecDeque<_>>();
        Ok(QuickStatus {
            power: args_deque.pop_front().unwrap().try_into_bool().unwrap().into(),
            // drawer: value.args[1].try_into_string()?,
            // cover: value.args[2].try_into_string()?,
            set_temperatures: OkResponse::parse(&mut args_deque.pop_front().unwrap().try_into_string()?.into_bytes().as_slice()).unwrap().options.iter().map(|(k, v)| v.clone().try_into_f64().unwrap()).collect(),
            sample_temperatures: args_deque.pop_front().unwrap().try_into_string()?.split_whitespace().map(|s| s.parse::<f64>().unwrap()).collect(),
            block_temperatures: args_deque.pop_front().unwrap().try_into_string()?.split_whitespace().map(|s| s.parse::<f64>().unwrap()).collect(),
            // run_title: value.args[6].try_into_string()?,
        })
    }
}

impl QuickStatus {
    pub fn to_string(&self) -> String {
        format!(
            "Power: {:?}\nSample Temperatures: {:?}\nBlock Temperatures: {:?}\nSet Temperatures: {:?}",
            self.power,
            self.sample_temperatures,
            self.block_temperatures,
            self.set_temperatures
        )
    }
}

#[derive(Debug, Clone)]
pub struct QuickStatusQuery;

impl CommandBuilder for QuickStatusQuery {
    type Response = QuickStatus;
    const COMMAND: &'static [u8] = b"RET $(POW?) $(TBC:SETT?) $(TBC:CONT?) $(RunProgress?)";

    fn write_command(&self, bytes: &mut impl Write) -> Result<(), QSConnectionError> {
        bytes.write_all(Self::COMMAND)?;
        Ok(())
    }
}


