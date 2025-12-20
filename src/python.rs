use crate::com::{QSConnection, ConnectionType, ResponseReceiver, TlsConfig};
use crate::parser::Command;
use crate::parser::{LogMessage, MessageResponse, MessageIdent};
use crate::protocol::Protocol;
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
#[pyo3(name = "Protocol")]
pub struct PyProtocol {
    protocol: Protocol,
}

#[pymethods]
impl PyProtocol {
    fn __str__(&self) -> String {
        format!("{}", self.protocol)
    }

    fn __repr__(&self) -> String {
        format!("Protocol(name='{}', volume={}, runmode='{}', stages={})",
                self.protocol.name, self.protocol.volume, self.protocol.runmode, self.protocol.stages.len())
    }

    #[getter]
    fn name(&self) -> String {
        self.protocol.name.clone()
    }

    #[getter]
    fn volume(&self) -> f64 {
        self.protocol.volume
    }

    #[getter]
    fn runmode(&self) -> String {
        self.protocol.runmode.clone()
    }

    #[getter]
    fn stages(&self) -> usize {
        self.protocol.stages.len()
    }
}

#[pyclass]
#[pyo3(name = "QSConnection")]
pub struct PyQSConnection {
    conn: QSConnection,
    rt: Arc<Runtime>,
}

#[pyclass]
#[pyo3(name = "MessageResponse")]
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
#[pyo3(name = "LogReceiver")]
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
    ///     timeout: Connection timeout in seconds (default: 10)
    ///     client_cert_path: Path to PEM file containing client certificate (optional)
    ///     client_key_path: Path to PEM file containing client private key, if separate from cert (optional)
    ///     server_ca_path: Path to PEM file containing CA certificate(s) for server verification (optional)
    ///     tls_server_name: Expected server name for TLS hostname verification (optional).
    ///         If server_ca_path is set but tls_server_name is None, chain verification is
    ///         performed but hostname is not checked (useful for tunneled connections).
    ///
    /// Returns:
    ///     A new QSConnection instance
    ///
    /// Raises:
    ///     ValueError: If connection_type is invalid or connection fails
    #[new]
    #[pyo3(signature = (host, port = 7443, connection_type = "Auto", timeout = 10, client_cert_path = None, client_key_path = None, server_ca_path = None, tls_server_name = None))]
    fn new(
        host: &str,
        port: u16,
        connection_type: &str,
        timeout: Option<u64>,
        client_cert_path: Option<String>,
        client_key_path: Option<String>,
        server_ca_path: Option<String>,
        tls_server_name: Option<String>,
    ) -> PyResult<Self> {
        let rt = Runtime::new()?;
        let connection_type = match connection_type {
            "SSL" => ConnectionType::SSL,
            "TCP" => ConnectionType::TCP,
            "Auto" => ConnectionType::Auto,
            _ => return Err(PyValueError::new_err("Invalid connection type")),
        };

        let tls_config = TlsConfig {
            client_cert_path,
            client_key_path,
            server_ca_path,
            tls_server_name,
        };

        let conn = match timeout {
            Some(timeout) => rt.block_on(QSConnection::connect_with_timeout_and_config(
                host, port, connection_type, Duration::from_secs(timeout), tls_config
            ))?,
            None => rt.block_on(QSConnection::connect_with_config(host, port, connection_type, tls_config))?,
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

    /// Authenticate with a password
    ///
    /// Args:
    ///     password: Password string
    ///
    /// Raises:
    ///     CommandError: If authentication fails
    fn authenticate(&mut self, py: Python<'_>, password: &str) -> PyResult<()> {
        use crate::parser::Value;
        use bstr::ByteSlice;
        
        // Get challenge
        let rx = self.rt.block_on(
            self.conn.send_command_bytes(b"CHAL?".as_bstr())
        )?;
        let mut challenge_response = PyMessageResponse {
            rx,
            rt: self.rt.clone(),
        };
        let challenge = challenge_response.get_response()?;
        
        // Generate auth response using Python's hmac module
        let hmac_module = PyModule::import(py, "hmac")?;
        let digest_func = hmac_module.getattr("digest")?;
        let password_bytes = password.as_bytes();
        let challenge_bytes = challenge.as_bytes();
        let auth_response_bytes: Vec<u8> = digest_func
            .call1((password_bytes, challenge_bytes, "md5"))?
            .extract()?;
        let auth_response = hex::encode(auth_response_bytes);
        
        // Send authentication
        let mut auth_cmd = Command::new("AUTH");
        auth_cmd.args.push(Value::String(auth_response));
        let rx = self.rt.block_on(self.conn.send_command(auth_cmd))?;
        let mut auth_response_recv = PyMessageResponse {
            rx,
            rt: self.rt.clone(),
        };
        auth_response_recv.get_response()?;
        
        Ok(())
    }

    /// Set access level
    ///
    /// Args:
    ///     level: Access level string ("Guest", "Observer", "Controller", "Administrator", "Full")
    ///
    /// Raises:
    ///     CommandError: If setting access level fails
    fn set_access_level(&mut self, level: &str) -> PyResult<()> {
        use crate::commands::AccessLevel;
        let access_level = match level {
            "Guest" => AccessLevel::Guest,
            "Observer" => AccessLevel::Observer,
            "Controller" => AccessLevel::Controller,
            "Administrator" => AccessLevel::Administrator,
            "Full" => AccessLevel::Full,
            _ => return Err(PyValueError::new_err(format!("Invalid access level: {}", level))),
        };
        let result = self.rt.block_on(self.conn.set_access_level(access_level));
        match result {
            Ok(()) => Ok(()),
            Err(e) => Err(CommandError::new_err(e.to_string())),
        }
    }

    /// Get the currently running protocol (parsed by Rust)
    ///
    /// Returns:
    ///     PyProtocol: Parsed protocol object
    ///
    /// Raises:
    ///     CommandError: If no protocol is running or if an error occurs
    fn get_running_protocol(&mut self) -> PyResult<PyProtocol> {
        let result = self.rt.block_on(self.conn.get_running_protocol());
        match result {
            Ok(protocol) => Ok(PyProtocol { protocol }),
            Err(e) => Err(CommandError::new_err(e.to_string())),
        }
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
