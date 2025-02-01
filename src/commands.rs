use std::{future::Future, marker::PhantomData};

use thiserror::Error;
use tokio::sync::mpsc;

use crate::{
    com::{QSConnection, QSConnectionError},
    parser::{Command, ErrorResponse, MessageResponse, OkResponse, Value},
};

pub struct CommandReceiver<T: TryFrom<OkResponse>> {
    pub command: Command,
    pub response: mpsc::Receiver<MessageResponse>,
    pub response_type: PhantomData<T>,
}

#[derive(Debug, Error)]
pub enum OkParseError {
    #[error("Unexpected values: {0:?}")]
    UnexpectedValues(OkResponse),
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
                panic!("Message response to command should not be possible: {:?}", message)
            }
        }
    }
}

pub trait CommandBuilder: Into<Command> {
    type Response: TryFrom<OkResponse>;
    // fn send(&self) -> impl Future<Output = Result<CommandReceiver<Self::Response>, QSConnectionError>> + Send;
    fn send(self, connection: &mut QSConnection) -> impl Future<Output = Result<CommandReceiver<Self::Response>, QSConnectionError>> + Send {
        let command: Command = self.into();
        let command_clone = command.clone();
        let r = connection.send_command(command);
        async move {
            let r = r.await?;
            Ok(CommandReceiver {
                command: command_clone,
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
            Err(OkParseError::UnexpectedValues(value))
        }
    }
}

#[derive(Debug)]
pub struct Subscribe(pub String);

impl CommandBuilder for Subscribe {
    type Response = ();
}

impl From<Subscribe> for Command {
    fn from(command: Subscribe) -> Self {
        Command::new("SUBS").with_arg(command.0)
    }
}

impl Subscribe {
    pub fn topic(topic: &str) -> Self {
        Self(topic.to_string())
    }
}


#[derive(Debug)]
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

pub struct Access(pub AccessLevel);

impl Access {
    pub fn level(level: AccessLevel) -> Self {
        Self(level)
    }
}

impl CommandBuilder for Access {
    type Response = ();
}

impl From<Access> for Command {
    fn from(command: Access) -> Self {
        Command::new("ACC").with_arg(command.0)
    }
}

pub struct Unsubscribe(pub String);

impl CommandBuilder for Unsubscribe {
    type Response = ();
}

impl From<Unsubscribe> for Command {
    fn from(command: Unsubscribe) -> Self {
        Command::new("UNS").with_arg(command.0)
    }
}

impl Unsubscribe {
    pub fn topic(topic: &str) -> Self {
        Self(topic.to_string())
    }
}

pub struct PowerQuery;

impl CommandBuilder for PowerQuery {
    type Response = bool;
}

impl From<PowerQuery> for Command {
    fn from(_command: PowerQuery) -> Self {
        Command::new("PWR?")
    }
}

impl TryFrom<OkResponse> for bool {
    type Error = OkParseError;
    fn try_from(value: OkResponse) -> Result<Self, Self::Error> {
        match value.args.get(0) {
            Some(Value::String(s)) if s == "ON" => Ok(true),
            Some(Value::String(s)) if s == "OFF" => Ok(false),
            _ => Err(OkParseError::UnexpectedValues(value)),
        }
    }
}

pub struct PowerSet(pub bool);

impl CommandBuilder for PowerSet {
    type Response = ();
}

impl From<PowerSet> for Command {
    fn from(command: PowerSet) -> Self {
        Command::new("PWR").with_arg(match command.0 {
            true => "ON",
            false => "OFF",
        })
    }
}



impl CommandBuilder for Command {
    type Response = OkResponse; 
}
