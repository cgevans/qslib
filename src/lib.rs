// use pyo3::prelude::*;

pub mod com;
pub mod commands;
pub mod parser;
pub mod plate_setup;
#[cfg(feature = "python")]
pub mod python;
#[cfg(feature = "python")]
use pyo3::prelude::*;

/// A Python module implemented in Rust.
#[cfg(feature = "python")]
#[pymodule]
mod qslib_rs {
    use super::*;

    #[pymodule_export]
    use crate::python::PyQSConnection;

    #[pymodule_export]
    use crate::python::PyMessageResponse;

    #[pymodule_export]
    use crate::python::PyLogReceiver;
}
