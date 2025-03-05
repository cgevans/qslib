use crate::com::{QSConnection, ConnectionType, ResponseReceiver};
use crate::parser::Command;
use crate::parser::{LogMessage, MessageResponse, MessageIdent};
use pyo3::exceptions::{PyTimeoutError, PyValueError, PyException};
use pyo3::prelude::*;
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::select;
use tokio::time::Duration;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::{StreamExt, StreamMap};
use crate::com::{ConnectionError, QSConnectionError, SendCommandError};
use crate::parser::ParseError;
use pyo3::PyErr;

pyo3::create_exception!(qslib, QslibException, PyException);
pyo3::create_exception!(qslib, CommandResponseError, QslibException);
pyo3::create_exception!(qslib, CommandError, CommandResponseError);
pyo3::create_exception!(qslib, UnexpectedMessageResponse, CommandResponseError);
pyo3::create_exception!(qslib, DisconnectedBeforeResponse, CommandResponseError);

#[pyclass]
#[pyo3(name = "QSConnection")]
pub struct PyQSConnection {
    conn: QSConnection,
    rt: Arc<Runtime>,
}

#[pyclass]
pub struct PyMessageResponse {
    rx: ResponseReceiver,
    rt: Arc<Runtime>,
}

#[pymethods]
impl PyMessageResponse {
    /// Get the response from the server
    ///
    /// Returns:
    ///     str: Response message from server
    ///
    /// Raises:
    ///     ValueError: If response is an error or invalid
    pub fn get_response_bytes(&mut self) -> PyResult<Vec<u8>> {
        let ret = self.rt.block_on(self.rx.recv());
        match ret {
            Some(x) => {
                match x {
                    MessageResponse::Ok { ident: _, message } => Ok(message.to_bytes()),
                    MessageResponse::CommandError { ident: _, error } => Err(CommandError::new_err(error)),
                    MessageResponse::Next { ident: _ } => self.get_response_bytes(),
                    MessageResponse::Message(message) => Err(UnexpectedMessageResponse::new_err(format!("Received log message as response to command: {:?}", message))),
                }
            },
            None => Err(DisconnectedBeforeResponse::new_err("Disconnected before response")),
        }
    }

    pub fn get_response(&mut self) -> PyResult<String> {
        let bytes = self.get_response_bytes()?;
        String::from_utf8(bytes).map_err(|e| PyValueError::new_err(e.to_string()))
    }

    /// Get acknowledgment from server
    ///
    /// Returns:
    ///     str: Empty string on success
    ///
    /// Raises:
    ///     ValueError: If response is not an acknowledgment
    pub fn get_ack(&mut self) -> PyResult<()> {
        let x = self.rt.block_on(self.rx.recv());
        match x {
            Some(x) => match x {
                MessageResponse::Ok { ident: _, message } => {
                    Err(UnexpectedMessageResponse::new_err(format!("OK message received as acknowledgment: {:?}", message)))
                }
                MessageResponse::CommandError { ident: _, error } => {
                    Err(CommandError::new_err(error.to_string()))
                }
                MessageResponse::Next { ident: _ } => Ok(()),
                MessageResponse::Message(message) => {
                    Err(UnexpectedMessageResponse::new_err(format!("Received log message as response to command: {:?}", message)))
                }
            },
            None => Err(DisconnectedBeforeResponse::new_err("Disconnected before response")),
        }
    }

    /// Get response from server with timeout
    ///
    /// Args:
    ///     timeout: Timeout in seconds
    ///
    /// Returns:
    ///     str: Response message from server
    ///
    /// Raises:
    ///     TimeoutError: If timeout occurs
    ///     ValueError: If response is an error or invalid
    pub fn get_response_with_timeout(&mut self, timeout: u64) -> PyResult<String> {
        let x = self.rt.block_on(async {
            select! {
                rx = self.rx.recv() => Ok(rx),
                _ = tokio::time::sleep(Duration::from_secs(timeout)) => {
                    Err(PyTimeoutError::new_err("Timeout"))
                }
            }
        })?;
        match x {
            Some(x) => match x {
                MessageResponse::Ok { ident: _, message } => Ok(message.to_string()),
                MessageResponse::CommandError { ident: _, error } => {
                    Err(CommandError::new_err(error.to_string()))
                }
                MessageResponse::Next { ident: _ } => {
                    Err(UnexpectedMessageResponse::new_err("Next message received"))
                }
                MessageResponse::Message(message) => {
                    Err(UnexpectedMessageResponse::new_err(format!("Received log message as response to command: {:?}", message)))
                }
            },
            None => Err(DisconnectedBeforeResponse::new_err("Disconnected before response")),
        }
    }
}

#[pyclass]
pub struct PyLogReceiver {
    rx: StreamMap<String, BroadcastStream<LogMessage>>,
    rt: Arc<Runtime>,
}

#[pymethods]
impl PyLogReceiver {
    fn __next__(&mut self) -> PyResult<LogMessage> {
        let x = self.rt.block_on(self.rx.next());
        match x {
            Some(x) => x.1.map_err(|e| PyValueError::new_err(e.to_string())),
            None => Err(PyValueError::new_err("No message received")),
        }
    }

    fn next(&mut self) -> PyResult<LogMessage> {
        self.__next__()
    }
}


#[derive(Debug, Clone, FromPyObject)]
enum CommandInput {
    String(String),
    Bytes(Vec<u8>),
}

#[pymethods]
impl PyQSConnection {
    /// Create a new QSConnection to communicate with a QuantStudio machine
    /// 
    /// Args:
    ///     host: Hostname or IP address to connect to
    ///     port: Port number (default: 7443)
    ///     connection_type: Connection type - "Auto", "SSL", or "TCP" (default: "Auto")
    /// 
    /// Returns:
    ///     A new QSConnection instance
    ///
    /// Raises:
    ///     ValueError: If connection_type is invalid or connection fails
    #[new]
    #[pyo3(signature = (host, port = 7443, connection_type = "Auto", timeout = 10))]
    fn new(host: &str, port: u16, connection_type: &str, timeout: Option<u64>) -> PyResult<Self> {
        let rt = Runtime::new()?;
        let connection_type = match connection_type {
            "SSL" => ConnectionType::SSL,
            "TCP" => ConnectionType::TCP,
            "Auto" => ConnectionType::Auto,
            _ => return Err(PyValueError::new_err("Invalid connection type")),
        };
        let conn = match timeout {
            Some(timeout) => rt.block_on(QSConnection::connect_with_timeout(host, port, connection_type, Duration::from_secs(timeout)))?,
            None => rt.block_on(QSConnection::connect(host, port, connection_type))?,
        };
        Ok(Self {
            conn,
            rt: Arc::new(rt),
        })
    }

    /// Send a command 
    ///
    /// Args:
    ///     command: Command string or bytes to send
    ///
    /// Returns:
    ///     MessageResponse object to get the server's response
    ///
    /// Raises:
    ///     ValueError: If command is invalid or connection error occurs
    fn run_command(&mut self, command: CommandInput) -> PyResult<PyMessageResponse> {
        let command = match command {
            CommandInput::String(s) => Command::try_from(s)?,
            CommandInput::Bytes(b) => Command::try_from(b)?,
        };
        let rx = self.rt.block_on(self.conn.send_command(command))?;
        Ok(PyMessageResponse {
            rx,
            rt: self.rt.clone(),
        })
    }

    fn expect_ident(&mut self, ident: MessageIdent) -> PyResult<PyMessageResponse> {
        let rx = self.rt.block_on(self.conn.expect_ident(ident))?;
        Ok(PyMessageResponse {
            rx,
            rt: self.rt.clone(),
        })
    }

    /// Send a raw bytes command 
    ///
    /// Args:
    ///     bytes: Raw command bytes to send
    ///
    /// Returns:
    ///     MessageResponse object to get the server's response
    ///
    /// Raises:
    ///     ValueError: If command is invalid or connection error occurs
    #[pyo3(signature = (bytes)) ]
    fn run_command_bytes(&mut self, bytes: &[u8]) -> PyResult<PyMessageResponse> {
        let rx = self.rt.block_on(self.conn.send_command_bytes(bytes))?;
        Ok(PyMessageResponse {
            rx,
            rt: self.rt.clone(),
        })
    }

    // fn run_command_bytes_with_timeout(&mut self, bytes: &[u8], timeout: u64) -> PyResult<PyMessageResponse> {
    //     let rx = self.rt.block_on(async move {
    //         select! {
    //             rx = self.conn.send_command_bytes(bytes) => rx,
    //             _ = tokio::time::sleep(Duration::from_secs(timeout)) => {
    //                 return Err(PyValueError::new_err("Timeout"))
    //             }
    //         }
    //     })?;
    //     Ok(PyMessageResponse { rx, rt: self.rt.clone() })
    // }

    /// Subscribe to log messages for specified topics
    ///
    /// Args:
    ///     topics: List of topic strings to subscribe to
    ///
    /// Returns:
    ///     LogReceiver object to receive log messages
    ///
    /// Raises:
    ///     ValueError: If subscription fails
    #[pyo3(signature = (topics) )]
    fn subscribe_log(&mut self, topics: Vec<String>) -> PyResult<PyLogReceiver> {
        let topics_refs: Vec<&str> = topics.iter().map(|s| s.as_str()).collect();
        let rx = self.rt.block_on(self.conn.subscribe_log(&topics_refs));
        Ok(PyLogReceiver {
            rx,
            rt: self.rt.clone(),
        })
    }

    /// Check if the connection is still active
    ///
    /// Returns:
    ///     bool: True if connected, False otherwise
    fn connected(&self) -> bool {
        self.rt.block_on(self.conn.is_connected())
    }
}


impl From<ConnectionError> for PyErr {
    fn from(e: ConnectionError) -> Self {
        PyValueError::new_err(e.to_string())
    }
}

impl From<ParseError> for PyErr {
    fn from(e: ParseError) -> Self {
        PyValueError::new_err(e.to_string())
    }
}

impl From<QSConnectionError> for PyErr {
    fn from(e: QSConnectionError) -> Self {
        PyValueError::new_err(e.to_string())
    }
}

impl From<SendCommandError> for PyErr {
    fn from(e: SendCommandError) -> Self {
        PyValueError::new_err(e.to_string())
    }
}
