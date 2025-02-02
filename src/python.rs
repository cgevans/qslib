
use crate::com::{QSConnection, QSConnectionError};
use crate::parser::{MessageResponse, LogMessage};
use crate::parser::Command;
use pyo3::prelude::*;
use pyo3::exceptions::{PyValueError, PyTimeoutError};
use pyo3::ToPyErr;
use tokio::sync::mpsc;
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::sync::broadcast;
use crate::parser;
use crate::com::ConnectionError;
use tokio::time::Duration;
use tokio::select;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::{StreamMap, StreamExt};

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
    pub fn get_response(&mut self) -> PyResult<String> {
        let x = self.rt.block_on(self.rx.recv());
        match x {
            Some(x) => match x {
                MessageResponse::Ok { ident, message } => Ok(message.to_string()),
                MessageResponse::Error { ident, error } => Err(PyValueError::new_err(error.to_string())),
                MessageResponse::Next { ident } => Err(PyValueError::new_err("Next message received")),
                MessageResponse::Message(message) => panic!("Received log message as response to command"),
            },
            None => Err(PyValueError::new_err("No message received")),
        }
    }

    pub fn __next__(&mut self) -> PyResult<String> {
        self.get_response(self)
    }

    pub fn get_ack(&mut self) -> PyResult<String> {
        let x = self.rt.block_on(self.rx.recv());
        match x {
            Some(x) => match x {
                MessageResponse::Ok { ident, message } => Err(PyValueError::new_err("OK message received")),
                MessageResponse::Error { ident, error } => Err(PyValueError::new_err(error.to_string())),
                MessageResponse::Next { ident } => Ok("".to_string()),
                MessageResponse::Message(message) => panic!("Received log message as response to command"),
            },
            None => Err(PyValueError::new_err("No message received")),
        }
    }
    
    pub fn get_response_with_timeout(&mut self, timeout: u64) -> PyResult<String> {
        let x = self.rt.block_on(async  {
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
                MessageResponse::Error { ident, error } => Err(PyValueError::new_err(error.to_string())),
                MessageResponse::Next { ident } => Err(PyValueError::new_err("Next message received")),
                MessageResponse::Message(message) => panic!("Received log message as response to command"),
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
        let x =self.rt.block_on(self.rx.next());
        match x {
            Some(x) => x.1.map_err(|e| PyValueError::new_err(e.to_string())),
            None => Err(PyValueError::new_err("No message received")),
        }
    }

    fn next(&mut self) -> PyResult<LogMessage> {
        self.__next__(self)
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
    #[new]
    #[pyo3(signature = (host, port = 7443)) ]
    fn new(host: &str, port: u16) -> PyResult<Self> {
        let rt = Runtime::new()?;
        let conn = rt.block_on(QSConnection::connect(host, port))?;
        Ok(Self { conn, rt: Arc::new(rt) })
    }

    fn run_command(&mut self, command: CommandInput) -> PyResult<PyMessageResponse> {
        let command = match command {
            CommandInput::String(s) => Command::try_from(s)?,
            CommandInput::Bytes(b) => Command::try_from(b)?,
        };
        let rx = self.rt.block_on(self.conn.send_command(command))?;
        Ok(PyMessageResponse { rx, rt: self.rt.clone() })
    }

    #[pyo3(signature = (bytes)) ]
    fn run_command_bytes(&mut self, bytes: &[u8]) -> PyResult<PyMessageResponse> {
        let rx = self.rt.block_on(self.conn.send_command_bytes(bytes))?;
        Ok(PyMessageResponse { rx, rt: self.rt.clone() })
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

    #[pyo3(signature = (topics) )]
    fn subscribe_log(&mut self, topics: Vec<String>) -> PyResult<PyLogReceiver> {
        let topics_refs: Vec<&str> = topics.iter().map(|s| s.as_str()).collect();
        let rx = self.rt.block_on(self.conn.subscribe_log(&topics_refs));
        Ok(PyLogReceiver { rx, rt: self.rt.clone() })
    }

    fn connected(&self) -> bool {
        self.rt.block_on(self.conn.is_connected())
    }
}
