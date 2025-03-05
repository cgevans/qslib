pub mod com;
pub mod commands;
pub mod data;
pub mod message_receiver;
pub mod parser;
pub mod plate_setup;

#[cfg(feature = "python")]
pub mod python;

#[cfg(feature = "python")]
#[pymodule(name = "_qslib")]
mod qslib {
    use pyo3::prelude::*;

    #[pymodule_export]
    use crate::python::PyQSConnection;

    #[pymodule_export]
    use crate::python::PyMessageResponse;

    #[pymodule_export]
    use crate::python::PyLogReceiver;

    #[pymodule_export]
    use qslib::parser::OkResponse;

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

    
    
}
