use crate::com::{ConnectionError, QSConnectionError, SendCommandError};
use crate::parser::ParseError;
use pyo3::exceptions::PyValueError;
use pyo3::PyErr;

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
