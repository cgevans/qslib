use crate::com::{ConnectionError, QSConnectionError, SendCommandError};
use crate::com::{ConnectionType, QSConnection, ResponseReceiver, TlsConfig};
use crate::commands::ReceiveOkResponseError;
use crate::parser::Command;
use crate::parser::ParseError;
use crate::parser::{LogMessage, MessageIdent, MessageResponse};
use crate::protocol::{Protocol, Stage, StageStep, Step};
use pyo3::exceptions::{PyException, PyStopIteration, PyTimeoutError, PyValueError};
use pyo3::prelude::*;
use pyo3::PyErr;
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::select;
use tokio::time::Duration;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::{StreamExt, StreamMap};

pyo3::create_exception!("qslib._qslib", QslibException, PyException);
pyo3::create_exception!("qslib._qslib", CommandResponseError, QslibException);
pyo3::create_exception!("qslib._qslib", CommandError, CommandResponseError);
pyo3::create_exception!(
    "qslib._qslib",
    UnexpectedMessageResponse,
    CommandResponseError
);
pyo3::create_exception!(
    "qslib._qslib",
    DisconnectedBeforeResponse,
    CommandResponseError
);

#[pyclass(module = "qslib._qslib")]
#[pyo3(name = "RustStep")]
pub struct PyStep {
    step: Step,
}

#[pymethods]
impl PyStep {
    #[new]
    #[pyo3(signature = (time, temperature, collect=None, temp_increment=0.0, temp_incrementcycle=2,
        temp_incrementpoint=None, time_increment=0, time_incrementcycle=2,
        time_incrementpoint=None, filters=vec![], pcr=false, quant=true,
        tiff=false, repeat=1))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        time: i64,
        temperature: Vec<f64>,
        collect: Option<bool>,
        temp_increment: f64,
        temp_incrementcycle: i64,
        temp_incrementpoint: Option<i64>,
        time_increment: i64,
        time_incrementcycle: i64,
        time_incrementpoint: Option<i64>,
        filters: Vec<String>,
        pcr: bool,
        quant: bool,
        tiff: bool,
        repeat: i64,
    ) -> Self {
        PyStep {
            step: Step {
                time,
                temperature,
                collect,
                temp_increment,
                temp_incrementcycle,
                temp_incrementpoint,
                time_increment,
                time_incrementcycle,
                time_incrementpoint,
                filters,
                pcr,
                quant,
                tiff,
                repeat,
                default_filters: vec![],
            },
        }
    }

    #[getter]
    fn time(&self) -> i64 {
        self.step.time
    }
    #[getter]
    fn temperature(&self) -> Vec<f64> {
        self.step.temperature.clone()
    }
    #[getter]
    fn collect(&self) -> Option<bool> {
        self.step.collect
    }
    #[getter]
    fn temp_increment(&self) -> f64 {
        self.step.temp_increment
    }
    #[getter]
    fn temp_incrementcycle(&self) -> i64 {
        self.step.temp_incrementcycle
    }
    #[getter]
    fn temp_incrementpoint(&self) -> Option<i64> {
        self.step.temp_incrementpoint
    }
    #[getter]
    fn time_increment(&self) -> i64 {
        self.step.time_increment
    }
    #[getter]
    fn time_incrementcycle(&self) -> i64 {
        self.step.time_incrementcycle
    }
    #[getter]
    fn time_incrementpoint(&self) -> Option<i64> {
        self.step.time_incrementpoint
    }
    #[getter]
    fn filters(&self) -> Vec<String> {
        self.step.filters.clone()
    }
    #[getter]
    fn pcr(&self) -> bool {
        self.step.pcr
    }
    #[getter]
    fn quant(&self) -> bool {
        self.step.quant
    }
    #[getter]
    fn tiff(&self) -> bool {
        self.step.tiff
    }
    #[getter]
    fn repeat(&self) -> i64 {
        self.step.repeat
    }
    #[getter]
    fn default_filters(&self) -> Vec<String> {
        self.step.default_filters.clone()
    }

    fn to_scpi_string(&self, step_index: i64, default_filters: Vec<String>) -> String {
        self.step.to_scpi_string(step_index, &default_filters)
    }

    fn info_str(&self, index: Option<i64>, repeats: i64) -> String {
        self.step.info_str(index, repeats)
    }

    fn __repr__(&self) -> String {
        format!(
            "RustStep(time={}, temperature={:?}, collect={:?})",
            self.step.time, self.step.temperature, self.step.collect
        )
    }
}

#[pyclass(module = "qslib._qslib")]
#[pyo3(name = "RustStage")]
pub struct PyStage {
    stage: Stage,
}

#[pymethods]
impl PyStage {
    #[new]
    #[pyo3(signature = (steps, repeat=1, index=None, label=None, custom_step_scpi=vec![]))]
    fn new(
        steps: Vec<PyRef<PyStep>>,
        repeat: i64,
        index: Option<i64>,
        label: Option<String>,
        custom_step_scpi: Vec<(usize, String)>,
    ) -> PyResult<Self> {
        let mut rust_steps: Vec<StageStep> = steps
            .iter()
            .map(|s| StageStep::Standard(s.step.clone()))
            .collect();
        // Insert custom steps at specified positions
        for (pos, scpi) in custom_step_scpi {
            let mut input = scpi.as_bytes();
            let mut cmds = Vec::new();
            while !input.is_empty() {
                // Skip blank lines / whitespace between commands
                while !input.is_empty()
                    && (input[0] == b'\n'
                        || input[0] == b'\r'
                        || input[0] == b' '
                        || input[0] == b'\t')
                {
                    input = &input[1..];
                }
                if input.is_empty() {
                    break;
                }
                let remaining_preview: String =
                    String::from_utf8_lossy(&input[..input.len().min(200)]).to_string();
                match Command::parse(&mut input) {
                    Ok(cmd) => cmds.push(cmd),
                    Err(e) => {
                        return Err(PyValueError::new_err(format!(
                            "Failed to parse custom step SCPI at: {:?}\nError: {}",
                            remaining_preview, e
                        )))
                    }
                }
            }
            if pos <= rust_steps.len() {
                rust_steps.insert(pos, StageStep::Custom(cmds));
            } else {
                rust_steps.push(StageStep::Custom(cmds));
            }
        }
        Ok(PyStage {
            stage: Stage {
                steps: rust_steps,
                repeat,
                index,
                label,
                default_filters: vec![],
            },
        })
    }

    #[getter]
    fn repeat(&self) -> i64 {
        self.stage.repeat
    }
    #[getter]
    fn index(&self) -> Option<i64> {
        self.stage.index
    }
    #[getter]
    fn label(&self) -> Option<String> {
        self.stage.label.clone()
    }
    #[getter]
    fn default_filters(&self) -> Vec<String> {
        self.stage.default_filters.clone()
    }

    #[getter]
    fn steps(&self) -> Vec<PyStep> {
        self.stage
            .steps
            .iter()
            .filter_map(|s| match s {
                StageStep::Standard(step) => Some(PyStep { step: step.clone() }),
                StageStep::Custom(_) => None,
            })
            .collect()
    }

    #[getter]
    fn has_custom_steps(&self) -> bool {
        self.stage
            .steps
            .iter()
            .any(|s| matches!(s, StageStep::Custom(_)))
    }

    /// Returns all steps as a list of ("standard", PyStep) or ("custom", scpi_body_string) tuples,
    /// preserving their original order (including interleaved custom steps).
    fn all_steps(&self, py: Python<'_>) -> Vec<(String, PyObject)> {
        self.stage
            .steps
            .iter()
            .map(|s| match s {
                StageStep::Standard(step) => (
                    "standard".to_string(),
                    PyStep { step: step.clone() }
                        .into_pyobject(py)
                        .unwrap()
                        .into_any()
                        .unbind(),
                ),
                StageStep::Custom(cmds) => {
                    let body = cmds
                        .iter()
                        .map(|c| {
                            let mut buf = Vec::new();
                            c.write_bytes(&mut buf).unwrap();
                            String::from_utf8(buf).unwrap()
                        })
                        .collect::<Vec<_>>()
                        .join("\n");
                    (
                        "custom".to_string(),
                        body.into_pyobject(py).unwrap().into_any().unbind(),
                    )
                }
            })
            .collect()
    }

    fn to_scpi_string(&self, stage_index: i64, default_filters: Vec<String>) -> String {
        self.stage.to_scpi_string(stage_index, &default_filters)
    }

    fn info_str(&self, index: Option<i64>) -> String {
        self.stage.info_str(index)
    }

    fn __repr__(&self) -> String {
        let custom = self
            .stage
            .steps
            .iter()
            .filter(|s| matches!(s, StageStep::Custom(_)))
            .count();
        if custom > 0 {
            format!(
                "RustStage(repeat={}, steps={}, custom={})",
                self.stage.repeat,
                self.stage.steps.len(),
                custom
            )
        } else {
            format!(
                "RustStage(repeat={}, steps={})",
                self.stage.repeat,
                self.stage.steps.len()
            )
        }
    }
}

#[pyclass(module = "qslib._qslib")]
#[pyo3(name = "Protocol")]
pub struct PyProtocol {
    pub(crate) protocol: Protocol,
}

#[pymethods]
impl PyProtocol {
    fn __str__(&self) -> String {
        format!("{}", self.protocol)
    }

    fn __repr__(&self) -> String {
        format!(
            "Protocol(name='{}', volume={}, runmode='{}', stages={})",
            self.protocol.name,
            self.protocol.volume,
            self.protocol.runmode,
            self.protocol.stages.len()
        )
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
    fn covertemperature(&self) -> f64 {
        self.protocol.covertemperature
    }

    #[getter]
    fn filters(&self) -> Vec<String> {
        self.protocol.filters.clone()
    }

    #[getter]
    fn num_stages(&self) -> usize {
        self.protocol.stages.len()
    }

    #[getter]
    fn stages(&self) -> Vec<PyStage> {
        self.protocol
            .stages
            .iter()
            .map(|s| PyStage { stage: s.clone() })
            .collect()
    }

    #[getter]
    fn prerun(&self) -> Vec<String> {
        self.protocol
            .prerun
            .iter()
            .map(|c| {
                let mut buf = Vec::new();
                c.write_bytes(&mut buf).unwrap();
                String::from_utf8(buf).unwrap()
            })
            .collect()
    }

    #[getter]
    fn postrun(&self) -> Vec<String> {
        self.protocol
            .postrun
            .iter()
            .map(|c| {
                let mut buf = Vec::new();
                c.write_bytes(&mut buf).unwrap();
                String::from_utf8(buf).unwrap()
            })
            .collect()
    }

    /// Serialize to SCPI command string.
    fn to_scpi_string(&self) -> String {
        self.protocol.to_scpi_string()
    }

    /// Generate (tcprotocol_xml, qsl_tcprotocol_xml) pair.
    #[pyo3(signature = (cover_temperature, version, machine_toml=None))]
    fn to_xml_pair(
        &self,
        cover_temperature: f64,
        version: &str,
        machine_toml: Option<&str>,
    ) -> (String, String) {
        self.protocol
            .to_xml_pair(cover_temperature, version, machine_toml)
    }

    /// Parse a Protocol from tcprotocol.xml content.
    #[staticmethod]
    fn from_xml_string(xml: &str) -> PyResult<PyProtocol> {
        let protocol = Protocol::from_xml_str(xml)
            .map_err(|e| PyValueError::new_err(format!("Failed to parse tcprotocol XML: {}", e)))?;
        Ok(PyProtocol { protocol })
    }

    /// Extract QSLibProtocolCommand text from qsl-tcprotocol.xml.
    #[staticmethod]
    fn parse_qsl_tcprotocol_command(xml: &str) -> Option<String> {
        Protocol::parse_qsl_tcprotocol_command(xml)
    }

    /// Extract MachineConnection TOML text from qsl-tcprotocol.xml.
    #[staticmethod]
    fn parse_qsl_machine_connection(xml: &str) -> Option<String> {
        Protocol::parse_qsl_machine_connection(xml)
    }

    /// Parse a protocol from an SCPI command string.
    #[staticmethod]
    fn from_scpi_string(s: &str) -> PyResult<PyProtocol> {
        let cmd = Command::try_from(s)
            .map_err(|e| PyValueError::new_err(format!("Failed to parse SCPI command: {}", e)))?;
        let protocol = Protocol::from_scpicommand(&cmd)
            .map_err(|e| PyValueError::new_err(format!("Failed to parse protocol: {}", e)))?;
        Ok(PyProtocol { protocol })
    }

    /// Create a Protocol from components.
    #[staticmethod]
    #[pyo3(signature = (name, stages, volume=50.0, runmode="standard".to_string(),
        filters=vec![], covertemperature=105.0, prerun=vec![], postrun=vec![]))]
    fn create(
        name: String,
        stages: Vec<PyRef<PyStage>>,
        volume: f64,
        runmode: String,
        filters: Vec<String>,
        covertemperature: f64,
        prerun: Vec<String>,
        postrun: Vec<String>,
    ) -> PyResult<PyProtocol> {
        let rust_stages: Vec<Stage> = stages.iter().map(|s| s.stage.clone()).collect();

        let parse_commands = |strings: Vec<String>| -> PyResult<Vec<Command>> {
            let mut cmds = Vec::new();
            for s in strings {
                let mut input = s.as_bytes();
                while !input.is_empty() {
                    // Skip blank lines / whitespace between commands
                    while !input.is_empty() && matches!(input[0], b'\n' | b'\r' | b' ' | b'\t') {
                        input = &input[1..];
                    }
                    if input.is_empty() {
                        break;
                    }
                    let remaining_preview: String =
                        String::from_utf8_lossy(&input[..input.len().min(200)]).to_string();
                    match Command::parse(&mut input) {
                        Ok(cmd) => cmds.push(cmd),
                        Err(e) => {
                            return Err(PyValueError::new_err(format!(
                                "Failed to parse SCPI command at: {:?}\nError: {}",
                                remaining_preview, e
                            )))
                        }
                    }
                }
            }
            Ok(cmds)
        };

        Ok(PyProtocol {
            protocol: Protocol {
                stages: rust_stages,
                name,
                volume,
                runmode,
                filters,
                covertemperature,
                prerun: parse_commands(prerun)?,
                postrun: parse_commands(postrun)?,
            },
        })
    }
}

#[pyclass(module = "qslib._qslib")]
#[pyo3(name = "QSConnection")]
pub struct PyQSConnection {
    conn: QSConnection,
    rt: Arc<Runtime>,
}

#[pyclass(module = "qslib._qslib")]
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
    pub fn get_response_bytes(&mut self, py: Python<'_>) -> PyResult<Vec<u8>> {
        // Bounded by the connection's timeouts (initial for the first message,
        // next_to_ok after a NEXT) so a lost or never-sent response cannot
        // block forever. The GIL is released for the duration of the wait so
        // other Python threads keep running and Ctrl-C stays responsive.
        let rt = Arc::clone(&self.rt);
        let res = py.detach(|| rt.block_on(self.rx.get_response()));
        match res {
            Ok(Ok(message)) => Ok(message.to_bytes()),
            Ok(Err(error)) => Err(CommandError::new_err(error)),
            Err(ReceiveOkResponseError::Timeout) => {
                Err(PyTimeoutError::new_err("Timed out waiting for response"))
            }
            Err(ReceiveOkResponseError::ConnectionClosed) => Err(
                DisconnectedBeforeResponse::new_err("Disconnected before response"),
            ),
            Err(ReceiveOkResponseError::UnexpectedMessage(message)) => {
                Err(UnexpectedMessageResponse::new_err(format!(
                    "Received log message as response to command: {:?}",
                    message
                )))
            }
            Err(e @ ReceiveOkResponseError::ResponseParsingError(_)) => {
                Err(CommandResponseError::new_err(e.to_string()))
            }
        }
    }

    pub fn get_response(&mut self, py: Python<'_>) -> PyResult<String> {
        let bytes = self.get_response_bytes(py)?;
        String::from_utf8(bytes).map_err(|e| PyValueError::new_err(e.to_string()))
    }

    /// Get acknowledgment from server
    ///
    /// Returns:
    ///     str: Empty string on success
    ///
    /// Raises:
    ///     ValueError: If response is not an acknowledgment
    pub fn get_ack(&mut self, py: Python<'_>) -> PyResult<()> {
        // Bounded by the connection's initial timeout, with the GIL released
        // during the wait, so a missing acknowledgement cannot block forever.
        let rt = Arc::clone(&self.rt);
        let x = py.detach(|| rt.block_on(self.rx.recv_initial()));
        match x {
            Ok(MessageResponse::Ok { ident: _, message })
            | Ok(MessageResponse::Warning { ident: _, message }) => {
                Err(UnexpectedMessageResponse::new_err(format!(
                    "OK message received as acknowledgment: {:?}",
                    message
                )))
            }
            Ok(MessageResponse::CommandError { ident: _, error }) => {
                Err(CommandError::new_err(error.to_string()))
            }
            Ok(MessageResponse::Next { ident: _ }) => Ok(()),
            Ok(MessageResponse::Message(message)) => Err(UnexpectedMessageResponse::new_err(
                format!("Received log message as response to command: {:?}", message),
            )),
            Err(ReceiveOkResponseError::Timeout) => Err(PyTimeoutError::new_err(
                "Timed out waiting for acknowledgment",
            )),
            Err(_) => Err(DisconnectedBeforeResponse::new_err(
                "Disconnected before response",
            )),
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
                MessageResponse::Ok { ident: _, message }
                | MessageResponse::Warning { ident: _, message } => Ok(message.to_string()),
                MessageResponse::CommandError { ident: _, error } => {
                    Err(CommandError::new_err(error.to_string()))
                }
                MessageResponse::Next { ident: _ } => {
                    Err(UnexpectedMessageResponse::new_err("Next message received"))
                }
                MessageResponse::Message(message) => Err(UnexpectedMessageResponse::new_err(
                    format!("Received log message as response to command: {:?}", message),
                )),
            },
            None => Err(DisconnectedBeforeResponse::new_err(
                "Disconnected before response",
            )),
        }
    }
}

#[pyclass(module = "qslib._qslib")]
#[pyo3(name = "LogReceiver")]
pub struct PyLogReceiver {
    rx: StreamMap<String, BroadcastStream<LogMessage>>,
    rt: Arc<Runtime>,
}

#[pymethods]
impl PyLogReceiver {
    fn __next__(&mut self, py: Python<'_>) -> PyResult<LogMessage> {
        // Release the GIL while waiting for the next log message so other
        // Python threads keep running. The stream ends (yields None) when the
        // connection task exits and drops the log senders, at which point we
        // raise StopIteration so a `for msg in receiver:` loop terminates
        // cleanly instead of blocking forever.
        let rt = Arc::clone(&self.rt);
        let x = py.detach(|| rt.block_on(self.rx.next()));
        match x {
            Some(x) => x.1.map_err(|e| PyValueError::new_err(e.to_string())),
            None => Err(PyStopIteration::new_err("Connection closed")),
        }
    }

    fn next(&mut self, py: Python<'_>) -> PyResult<LogMessage> {
        self.__next__(py)
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
                host,
                port,
                connection_type,
                Duration::from_secs(timeout),
                tls_config,
            ))?,
            None => rt.block_on(QSConnection::connect_with_config(
                host,
                port,
                connection_type,
                tls_config,
            ))?,
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

    /// Send QUIT command to the server for a clean disconnect.
    fn disconnect(&self) {
        self.rt.block_on(self.conn.disconnect());
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
        let rx = self
            .rt
            .block_on(self.conn.send_command_bytes(b"CHAL?".as_bstr()))?;
        let mut challenge_response = PyMessageResponse {
            rx,
            rt: self.rt.clone(),
        };
        let challenge = challenge_response.get_response(py)?;

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
        auth_response_recv.get_response(py)?;

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
            _ => {
                return Err(PyValueError::new_err(format!(
                    "Invalid access level: {}",
                    level
                )))
            }
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
