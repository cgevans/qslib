pub mod com;
pub mod commands;
pub mod data;
pub mod message_receiver;
pub mod parser;
pub mod plate_setup;
pub mod message_log;

#[cfg(feature = "python")]
pub mod python;

#[cfg(feature = "python")]
use pyo3::prelude::*;
#[cfg(feature = "python")]
#[pymodule(name = "_qslib")]
mod qslib {

    #[pymodule_export]
    use crate::python::PyQSConnection;

    #[pymodule_export]
    use crate::python::PyMessageResponse;

    #[pymodule_export]
    use crate::python::PyLogReceiver;

    #[pymodule_export]
    use crate::parser::OkResponse;

    #[pymodule_export]
    use crate::python::UnexpectedMessageResponse;

    #[pymodule_export]
    use crate::python::DisconnectedBeforeResponse;

    #[pymodule_export]
    use crate::python::CommandError;

    #[pymodule_export]
    use crate::python::QslibException;

    #[pymodule_export]
    use crate::python::CommandResponseError;

    #[pymodule_export]
    use crate::message_log::TemperatureLog;

    #[pymodule_export]
    use crate::message_log::get_n_zones;
    
}
