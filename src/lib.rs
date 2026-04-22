#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

pub mod calibration;
pub mod com;
pub mod commands;
pub mod data;
pub mod eds;
pub mod experiment_xml;
pub mod message_receiver;
pub mod parser;
pub mod plate_setup;
pub mod message_log;
pub mod protocol;
pub mod quant;
pub mod tiff;

#[cfg(test)]
#[macro_use]
mod test_utils;

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
    use crate::python::PyStep;

    #[pymodule_export]
    use crate::python::PyStage;

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

    #[pymodule_export]
    use crate::commands::AccessLevel;

    #[pymodule_export]
    use crate::commands::RunStatus;

    #[pymodule_export]
    use crate::commands::MachineStatus;

    #[pymodule_export]
    use crate::data::FilterSet;

    #[pymodule_export]
    use crate::parser::SCPICommand;

    #[pymodule_export]
    use crate::parser::py_quote_string_if_needed;

    // Calibration types
    #[pymodule_export]
    use crate::calibration::UniformityCalibration;

    #[pymodule_export]
    use crate::calibration::BackgroundCalibration;

    #[pymodule_export]
    use crate::calibration::PureDyeCalibration;

    #[pymodule_export]
    use crate::calibration::WellMatrix;

    // Quant types
    #[pymodule_export]
    use crate::quant::QuantFile;

    #[pymodule_export]
    use crate::quant::QuantConditions;

    #[pymodule_export]
    use crate::quant::QuantRegion;

    #[pymodule_export]
    use crate::quant::WellQuant;

    #[pymodule_export]
    use crate::quant::CollectionKey;

    #[pymodule_export]
    use crate::quant::QuantDataCollection;

    // ROI calibration
    #[pymodule_export]
    use crate::calibration::RoiCalibration;

    // EDS Archive
    #[pymodule_export]
    use crate::eds::EdsArchive;

    // TIFF processing
    #[pymodule_export]
    use crate::tiff::py_apply_roi_to_tiff;

    #[pymodule_export]
    use crate::tiff::py_decode_tiff;

    // Reconstruction functions
    #[pymodule_export]
    use crate::data::py_reconstruct_filterdata_from_eds;

    #[pymodule_export]
    use crate::data::py_reconstruct_filterdata;

    #[pymodule_export]
    use crate::data::py_reconstruct_filterdata_from_tiffs;

    #[pymodule_export]
    use crate::data::py_parse_filterdata_v2_json;

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

    /// Parse a string into an OkResponse (options dict + positional args list)
    #[pyfunction]
    fn parse_arglist(input: String) -> PyResult<crate::parser::OkResponse> {
        crate::parser::OkResponse::try_from(input).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("Failed to parse arglist: {}", e))
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
