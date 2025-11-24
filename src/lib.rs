pub mod com;
pub mod commands;
pub mod data;
pub mod message_receiver;
pub mod parser;
pub mod plate_setup;
pub mod message_log;
pub mod protocol;

#[cfg(feature = "python")]
pub mod python;

#[cfg(feature = "python")]
use pyo3::prelude::*;
#[cfg(feature = "python")]
#[pymodule(name = "_qslib")]
mod qslib {
    use pyo3::prelude::*;

    #[pymodule_export]
    use crate::python::PyQSConnection;

    #[pymodule_export]
    use crate::python::PyProtocol;

    #[pymodule_export]
    use crate::python::PyMessageResponse;

    #[pymodule_export]
    use crate::python::PyLogReceiver;

    #[pymodule_export]
    use crate::parser::OkResponse;

    #[pymodule_export]
    use crate::parser::Command;

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

    #[pymodule_export]
    use crate::plate_setup::PlateSetup;

    #[pymodule_export]
    use crate::plate_setup::Sample;

    #[pymodule_export]
    use crate::data::FilterDataCollection;

    #[pymodule_export]
    use crate::data::PlatePointData;

    #[pymodule_export]
    use crate::message_log::RunLogInfo;

    // #[pymodule_export]
    // use crate::message_log::RunState;

    /// Parse a string into an ArgMap
    #[pyfunction]
    fn parse_argmap(input: String) -> PyResult<crate::parser::ArgMap> {
        use crate::parser::parse_options;
        parse_options(&mut input.as_bytes()).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("Failed to parse ArgMap: {}", e))
        })
    }

    /// Parse bytes into an ArgMap
    #[pyfunction]
    fn parse_argmap_bytes(input: &[u8]) -> PyResult<crate::parser::ArgMap> {
        use crate::parser::parse_options;
        parse_options(&mut &input[..]).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("Failed to parse ArgMap: {}", e))
        })
    }

    /// Parse a string into a Value
    #[pyfunction]
    fn parse_value(input: String) -> PyResult<crate::parser::Value> {
        use crate::parser::Value;
        Value::parse(&mut input.as_bytes()).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("Failed to parse Value: {}", e))
        })
    }

    /// Parse bytes into a Value
    #[pyfunction]
    fn parse_value_bytes(input: &[u8]) -> PyResult<crate::parser::Value> {
        use crate::parser::Value;
        Value::parse(&mut &input[..]).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("Failed to parse Value: {}", e))
        })
    }

}
