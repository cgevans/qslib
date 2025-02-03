use crate::com::ConnectionError;
use crate::com::{QSConnection, QSConnectionError, ConnectionType};
use crate::parser;
use crate::parser::Command;
use crate::parser::{LogMessage, MessageResponse};
use pyo3::exceptions::{PyTimeoutError, PyValueError};
use pyo3::prelude::*;
use pyo3::ToPyErr;
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::select;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::time::Duration;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::{StreamExt, StreamMap};

#[cfg(feature = "python")]
use pyo3_async_runtimes::tokio::future_into_py;

#[pyclass]
#[pyo3(name = "QSConnection")]
pub struct PyQSConnection {
    conn: QSConnection,
    rt: Arc<Runtime>,
}

#[cfg(feature = "python")]
#[pyclass]
pub struct PyMessageResponse {
    rx: mpsc::Receiver<MessageResponse>,
    rt: Arc<Runtime>,
}

#[cfg(feature = "python")]
#[pymethods]
impl PyMessageResponse {
    /// Get the response from the server
    ///
    /// Returns:
    ///     str: Response message from server
    ///
    /// Raises:
    ///     ValueError: If response is an error or invalid
    pub fn get_response(&mut self) -> PyResult<String> {
        let x = self.rt.block_on(self.rx.recv());
        match x {
            Some(x) => match x {
                MessageResponse::Ok { ident, message } => Ok(message.to_string()),
                MessageResponse::Error { ident, error } => {
                    Err(PyValueError::new_err(error.to_string()))
                }
                MessageResponse::Next { ident } => {
                    Err(PyValueError::new_err("Next message received"))
                }
                MessageResponse::Message(message) => {
                    panic!("Received log message as response to command")
                }
            },
            None => Err(PyValueError::new_err("No message received")),
        }
    }

    pub fn __next__(&mut self) -> PyResult<String> {
        self.get_response()
    }

    /// Get acknowledgment from server
    ///
    /// Returns:
    ///     str: Empty string on success
    ///
    /// Raises:
    ///     ValueError: If response is not an acknowledgment
    pub fn get_ack(&mut self) -> PyResult<String> {
        let x = self.rt.block_on(self.rx.recv());
        match x {
            Some(x) => match x {
                MessageResponse::Ok { ident, message } => {
                    Err(PyValueError::new_err("OK message received"))
                }
                MessageResponse::Error { ident, error } => {
                    Err(PyValueError::new_err(error.to_string()))
                }
                MessageResponse::Next { ident } => Ok("".to_string()),
                MessageResponse::Message(message) => {
                    panic!("Received log message as response to command")
                }
            },
            None => Err(PyValueError::new_err("No message received")),
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
                    return Err(PyTimeoutError::new_err("Timeout"))
                }
            }
        })?;
        match x {
            Some(x) => match x {
                MessageResponse::Ok { ident, message } => Ok(message.to_string()),
                MessageResponse::Error { ident, error } => {
                    Err(PyValueError::new_err(error.to_string()))
                }
                MessageResponse::Next { ident } => {
                    Err(PyValueError::new_err("Next message received"))
                }
                MessageResponse::Message(message) => {
                    panic!("Received log message as response to command")
                }
            },
            None => Err(PyValueError::new_err("No message received")),
        }
    }
}

#[cfg(feature = "python")]
#[pyclass]
pub struct PyLogReceiver {
    rx: StreamMap<String, BroadcastStream<LogMessage>>,
    rt: Arc<Runtime>,
}

#[cfg(feature = "python")]
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

impl From<ConnectionError> for PyErr {
    fn from(e: ConnectionError) -> Self {
        PyValueError::new_err(e.to_string())
    }
}

impl From<parser::ParseError> for PyErr {
    fn from(e: parser::ParseError) -> Self {
        PyValueError::new_err(e.to_string())
    }
}

impl From<QSConnectionError> for PyErr {
    fn from(e: QSConnectionError) -> Self {
        PyValueError::new_err(e.to_string())
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
    #[pyo3(signature = (host, port = 7443, connection_type = "Auto")) ]
    fn new(host: &str, port: u16, connection_type: &str) -> PyResult<Self> {
        let rt = Runtime::new()?;
        let connection_type = match connection_type {
            "SSL" => ConnectionType::SSL,
            "TCP" => ConnectionType::TCP,
            "Auto" => ConnectionType::Auto,
            _ => return Err(PyValueError::new_err("Invalid connection type")),
        };
        let conn = rt.block_on(QSConnection::connect(host, port, connection_type))?;
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
