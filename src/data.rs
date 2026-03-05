use polars::prelude::*;
use serde::{Deserialize, Deserializer, Serialize};
use thiserror::Error;

/// Escape a string for use as an InfluxDB line protocol tag value.
pub(crate) fn escape_lp_tag(s: &str) -> String {
    s.replace(' ', "\\ ").replace(',', "\\,").replace('=', "\\=")
}

/// Escape a string for use as an InfluxDB line protocol field (string) value.
pub(crate) fn escape_lp_field(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

#[cfg(feature = "python")]
use pyo3::prelude::*;

#[cfg(feature = "python")]
use pyo3::exceptions::PyValueError;

#[cfg(feature = "python")]
use pyo3_polars::PyDataFrame;

// use once_cell::sync::Lazy;

// static WELL_DATA_SCHEMA: Lazy<Schema> = Lazy::new(|| {
//     Schema::from_iter(vec![
//         Field::new("filter_set".into(), DataType::String),
//         Field::new("stage".into(), DataType::UInt32),
//         Field::new("cycle".into(), DataType::UInt32),
//         Field::new("step".into(), DataType::UInt32),
//         Field::new("point".into(), DataType::UInt32),
//         Field::new("well".into(), DataType::String),
//         Field::new("row".into(), DataType::UInt32),
//         Field::new("column".into(), DataType::UInt32),
//         Field::new("fluorescence".into(), DataType::Float64),
//     ])
// });

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "python", pyclass(frozen, eq, hash, ord, subclass, module = "qslib._qslib"))]
pub struct FilterSet {
    pub ex: u8,
    pub em: u8,
    pub quant: bool,
}

impl FilterSet {
    pub fn new(ex: u8, em: u8, quant: bool) -> Self {
        Self { ex, em, quant }
    }

    pub fn from_string(s: &str) -> Result<Self, String> {
        if s.starts_with('x') {
            // "x1-m4" or "x1,m4[,quant]" format
            if s.len() >= 5 && (s.as_bytes()[2] == b'-' || s.as_bytes()[2] == b',') {
                let ex = s.as_bytes()[1] - b'0';
                let em = s.as_bytes()[4] - b'0';
                Ok(Self::new(ex, em, true))
            } else {
                Err(format!("Invalid filter set format: {}", s))
            }
        } else if s.starts_with('M') {
            // "M4_X1" format
            if s.len() >= 5 {
                let em = s.as_bytes()[1] - b'0';
                let ex = s.as_bytes()[4] - b'0';
                Ok(Self::new(ex, em, true))
            } else {
                Err(format!("Invalid filter set format: {}", s))
            }
        } else if s.starts_with('m') {
            // "m4,x1" or "m4,x1,quant" format
            let parts: Vec<&str> = s.split(',').collect();
            if parts.len() >= 2 {
                let em = parts[0].as_bytes()[1] - b'0';
                let ex = parts[1].as_bytes()[1] - b'0';
                let quant = parts.len() >= 3 && parts[2] == "quant";
                Ok(Self::new(ex, em, quant))
            } else {
                Err(format!("Invalid filter set format: {}", s))
            }
        } else {
            Err(format!("Invalid filter set format: {}", s))
        }
    }

    pub fn lowerform(&self) -> String {
        format!("x{}-m{}", self.ex, self.em)
    }

    pub fn upperform(&self) -> String {
        format!("M{}_X{}", self.em, self.ex)
    }

    pub fn hacform(&self) -> String {
        let base = format!("m{},x{}", self.em, self.ex);
        if self.quant { format!("{},quant", base) } else { base }
    }
}

impl PartialOrd for FilterSet {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FilterSet {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.ex.cmp(&other.ex).then(self.em.cmp(&other.em))
    }
}

impl std::fmt::Display for FilterSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.lowerform())?;
        if !self.quant {
            write!(f, "-noquant")?;
        }
        Ok(())
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl FilterSet {
    #[new]
    #[pyo3(signature = (ex, em, quant=true))]
    fn py_new(ex: u8, em: u8, quant: bool) -> Self {
        Self::new(ex, em, quant)
    }

    #[getter]
    fn ex(&self) -> u8 {
        self.ex
    }

    #[getter]
    fn em(&self) -> u8 {
        self.em
    }

    #[getter]
    fn quant(&self) -> bool {
        self.quant
    }

    #[staticmethod]
    fn fromstring(value: &Bound<'_, pyo3::PyAny>) -> PyResult<Self> {
        // If it's already a FilterSet, return it
        if let Ok(fs) = value.extract::<FilterSet>() {
            return Ok(fs);
        }
        // If it's a string, parse it
        if let Ok(s) = value.extract::<String>() {
            return Self::from_string(&s).map_err(PyValueError::new_err);
        }
        // If it's a sequence, join with comma and parse
        if let Ok(seq) = value.extract::<Vec<String>>() {
            let s = seq.join(",");
            return Self::from_string(&s).map_err(PyValueError::new_err);
        }
        Err(PyValueError::new_err(format!("Cannot convert {:?} to FilterSet", value)))
    }

    #[getter]
    #[pyo3(name = "lowerform")]
    fn py_lowerform(&self) -> String {
        self.lowerform()
    }

    #[getter]
    #[pyo3(name = "upperform")]
    fn py_upperform(&self) -> String {
        self.upperform()
    }

    #[getter]
    #[pyo3(name = "hacform")]
    fn py_hacform(&self) -> String {
        self.hacform()
    }

    #[allow(clippy::wrong_self_convention)]
    fn to_xml(&self) -> String {
        format!(
            "<CollectionCondition><FilterSet Emission=\"m{}\" Excitation=\"x{}\"/><Frames>0</Frames></CollectionCondition>",
            self.em, self.ex
        )
    }

    fn __str__(&self) -> String {
        format!("{}", self)
    }

    fn __repr__(&self) -> String {
        format!("FilterSet(ex={}, em={}, quant={})", self.ex, self.em, self.quant)
    }

    fn __reduce__(slf: &Bound<'_, Self>) -> PyResult<(Py<pyo3::types::PyType>, (u8, u8, bool))> {
        let py = slf.py();
        let this = slf.borrow();
        let cls = <Self as pyo3::PyTypeInfo>::type_object(py);
        Ok((cls.into(), (this.ex, this.em, this.quant)))
    }

    fn __copy__(&self) -> Self {
        *self
    }

    fn __deepcopy__(&self, _memo: &Bound<'_, pyo3::PyAny>) -> Self {
        *self
    }
}

fn parse_well_data<'de, D>(deserializer: D) -> Result<Vec<f64>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = String::deserialize(deserializer)?;
    s.split_whitespace()
        .map(|num_str| {
            num_str
                .parse::<f64>()
                .map_err(|e| serde::de::Error::custom(format!("Failed to parse float: {}", e)))
        })
        .collect()
}

fn serialize_well_data<S>(data: &[f64], serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let s = data
        .iter()
        .map(|f| f.to_string())
        .collect::<Vec<String>>()
        .join(" ");
    serializer.serialize_str(&s)
}

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "python", pyclass(get_all, set_all, module = "qslib._qslib"))]
pub struct FilterDataCollection {
    #[serde(rename = "Name")]
    pub name: String,
    #[serde(rename = "PlatePointData")]
    pub plate_point_data: Vec<PlatePointData>,
}


#[derive(Debug, Serialize, Deserialize, Clone)]
#[cfg_attr(feature = "python", pyclass(get_all, set_all, module = "qslib._qslib"))]
pub struct PlatePointData {
    #[serde(rename = "Stage")]
    pub stage: i32,
    #[serde(rename = "Cycle")]
    pub cycle: i32,
    #[serde(rename = "Step")]
    pub step: i32,
    #[serde(rename = "Point")]
    pub point: i32,
    #[serde(rename = "PlateData")]
    pub plate_data: Vec<PlateData>,
}

impl PlatePointData {
    pub fn to_polars(&self) -> Result<LazyFrame, PolarsError> {
        let lfs: Result<Vec<_>, _> = self.plate_data.iter().map(|pd| pd.to_polars()).collect();
        let lfs = lfs?;
        concat(lfs, UnionArgs::default())
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl PlatePointData {
    #[pyo3(name = "to_polars")]
    fn py_to_polars(&self) -> PyResult<PyDataFrame> {
        Ok(PyDataFrame(
            self.to_polars()
                .map_err(|e| PyValueError::new_err(format!("Failed to convert to Polars: {}", e)))?
                .collect()
                .map_err(|e| PyValueError::new_err(format!("Failed to collect Polars DataFrame: {}", e)))?
        ))
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[cfg_attr(feature = "python", pyclass(get_all, set_all, module = "qslib._qslib"))]
pub struct PlateData {
    #[serde(rename = "Rows")]
    pub rows: u32,
    #[serde(rename = "Cols")]
    pub cols: u32,
    #[serde(
        rename = "WellData",
        deserialize_with = "parse_well_data",
        serialize_with = "serialize_well_data"
    )]
    pub well_data: Vec<f64>,
    #[serde(rename = "Attribute")]
    pub attributes: Vec<Attribute>,
    #[serde(skip)]
    pub timestamp: Option<f64>,
    #[serde(skip)]
    pub set_temperatures: Option<Vec<f64>>,
}

#[derive(Error, Debug)]
pub enum DataError {
    #[error("Attribute not found: {0}")]
    AttributeNotFound(String),
}

impl PlateData {
    pub fn filter_set(&self) -> Result<&str, DataError> {
        self.get_attribute("FILTER_SET")
            .ok_or(DataError::AttributeNotFound("FILTER_SET".to_string()))
    }

    fn well_names(&self) -> Vec<(char, u32)> {
        (0..self.rows)
            .flat_map(|row| (1..=self.cols).map(move |col| ((b'A' + row as u8) as char, col)))
            .collect()
    }

    fn col_indices(&self) -> Vec<u32> {
        (0..self.rows).flat_map(|_row| 0..self.cols ).collect()
    }

    fn row_indices(&self) -> Vec<u32> {
        (0..self.rows).flat_map(|row| (0..self.cols).map(move |_col| row)).collect()
    }

    /// Convert plate data to InfluxDB line protocol format
    pub fn to_lineprotocol(
        &self,
        run_name: Option<&str>,
        sample_array: Option<&[String]>,
        set_temperatures: Option<&[f64]>,
        additional_tags: Option<&[(&str, &str)]>,
    ) -> Result<Vec<String>, DataError> {
        let mut lines = Vec::new();
        let filter_set = self.filter_set()?;
        let mut gs = format!("filterdata,filter_set={}", escape_lp_tag(&filter_set));

        if let Some(tags) = additional_tags {
            for (key, value) in tags {
                gs.push_str(&format!(",{}={}", escape_lp_tag(key), escape_lp_tag(value)));
            }
        }
        let gs = gs;

        // Get timestamp in nanoseconds
        let timestamp_ns = match self.timestamp {
            Some(ts) => format!(" {}", (ts * 1e9) as i64),
            None => String::new(),
        };

        // Generate well names (A01, A02, etc.)
        let well_names = self.well_names();

        // Get temperatures if available
        let temperatures: Option<&[f64]> = if let Some(temps) = set_temperatures {
            Some(temps)
        } else {
            self.set_temperatures.as_deref()
        };

        // Parse read temperatures for zone mapping (auto-detect zone count)
        let read_temperatures = self.get_temperatures();
        let num_zones = read_temperatures.as_ref().map(|t| t.len()).unwrap_or(1);
        let zone_size = if num_zones > 0 && (num_zones as u32) <= self.cols {
            self.cols / num_zones as u32
        } else {
            self.cols
        };

        // Generate a line for each well
        for ((row_letter, col), &fluorescence) in well_names.iter().zip(self.well_data.iter()) {
            let mut line = format!(
                "{},row={},col={:02} fluorescence={}",
                gs, row_letter, col, fluorescence
            );

            // Add stage, cycle, step, point if available
            if let Some(stage) = self
                .get_attribute("STAGE")
                .and_then(|s| s.parse::<i32>().ok())
            {
                line.push_str(&format!(",stage={:02}i", stage));
            }
            if let Some(cycle) = self
                .get_attribute("CYCLE")
                .and_then(|s| s.parse::<i32>().ok())
            {
                line.push_str(&format!(",cycle={:03}i", cycle));
            }
            if let Some(step) = self
                .get_attribute("STEP")
                .and_then(|s| s.parse::<i32>().ok())
            {
                line.push_str(&format!(",step={:02}i", step));
            }
            if let Some(point) = self
                .get_attribute("POINT")
                .and_then(|s| s.parse::<i32>().ok())
            {
                line.push_str(&format!(",point={:04}i", point));
            }

            // Add temperature if available (using auto-detected zone count)
            if let Some(ref temps) = read_temperatures {
                let zone_idx = ((col - 1) / zone_size) as usize;
                if let Some(&temp) = temps.get(zone_idx) {
                    line.push_str(&format!(",temperature_read={}", temp));
                }
            }

            // Add sample if provided
            if let Some(samples) = sample_array {
                let idx =
                    ((*row_letter as u8 - b'A') as usize) * self.cols as usize + (col - 1) as usize;
                if idx < samples.len() {
                    line.push_str(&format!(",sample=\"{}\"", escape_lp_field(&samples[idx])));
                }
            }

            // Add run name if provided
            if let Some(name) = run_name {
                line.push_str(&format!(",run_name=\"{}\"", escape_lp_field(name)));
            }

            // Add set temperature if available
            if let Some(temps) = temperatures {
                let divisor = if temps.len() > 0 && temps.len() <= self.cols as usize {
                    self.cols as usize / temps.len()
                } else {
                    1
                };
                let x = (col - 1) as usize / divisor;
                if let Some(&temp) = temps.get(x) {
                    line.push_str(&format!(",temperature_set={}", temp));
                }
            }

            line.push_str(&timestamp_ns);
            lines.push(line);
        }

        Ok(lines)
    }

    /// Get an attribute value by key
    pub fn get_attribute(&self, key: &str) -> Option<&str> {
        self.attributes
            .iter()
            .find(|attr| attr.key == key)
            .map(|attr| attr.value.as_str())
    }

    pub fn get_temperatures(&self) -> Option<Vec<f64>> {
        self.get_attribute("TEMPERATURE").and_then(|t| {
            t.split(',')
                .map(|t| t.parse::<f64>())
                .collect::<Result<Vec<_>, _>>()
                .ok()
        })
    }

    pub fn get_exposure(&self) -> Option<i32> {
        self.get_attribute("EXPOSURE").and_then(|e| e.parse::<i32>().ok())
    }

    pub fn get_stage(&self) -> Option<i32> {
        self.get_attribute("STAGE").and_then(|s| s.parse::<i32>().ok())
    }

    pub fn get_cycle(&self) -> Option<i32> {
        self.get_attribute("CYCLE").and_then(|s| s.parse::<i32>().ok())
    }

    pub fn get_step(&self) -> Option<i32> {
        self.get_attribute("STEP").and_then(|s| s.parse::<i32>().ok())
    }

    pub fn get_point(&self) -> Option<i32> {
        self.get_attribute("POINT").and_then(|s| s.parse::<i32>().ok())
    }

    pub fn to_polars(&self) -> Result<LazyFrame, PolarsError> {
        let well_names = self.well_names();
        let c = self.col_indices();
        let templist = self.get_temperatures()
            .ok_or_else(|| PolarsError::ComputeError("Missing TEMPERATURE attribute".into()))?;
        if templist.is_empty() {
            return Err(PolarsError::ComputeError("TEMPERATURE attribute is empty".into()));
        }
        let zone_size = self.cols / templist.len() as u32;
        if zone_size == 0 {
            return Err(PolarsError::ComputeError("Invalid zone size calculation".into()));
        }
        let sts = self.col_indices().iter().map(|c| {
            let idx = (c / zone_size) as usize;
            if idx >= templist.len() {
                templist[templist.len() - 1] // Use last temperature if out of bounds
            } else {
                templist[idx]
            }
        }).collect::<Vec<_>>();
        let zone = self.col_indices().iter().map(|c| 1 + c / zone_size).collect::<Vec<_>>();
        let df = df![
            "well" => well_names.iter().map(|(row, col)| format!("{row}{col}")).collect::<Vec<String>>(),
            "row" => self.row_indices(),
            "column" => c,
            "fluorescence" => self.well_data.clone(),
            "sample_temperature" => sts,
            "zone" => zone,
        ]?;
        let timestamp_col: Expr = match self.timestamp {
            Some(ts) => lit(ts).alias("timestamp"),
            None => lit(NULL).cast(DataType::Float64).alias("timestamp"),
        };
        Ok(df.lazy().with_columns([
            lit(self.filter_set().map_err(|e| PolarsError::ComputeError(e.to_string().into()))?.to_string()).alias("filter_set"),
            lit(self.get_stage().ok_or_else(|| PolarsError::ComputeError("Missing STAGE attribute".into()))?).alias("stage"),
            lit(self.get_cycle().ok_or_else(|| PolarsError::ComputeError("Missing CYCLE attribute".into()))?).alias("cycle"),
            lit(self.get_step().ok_or_else(|| PolarsError::ComputeError("Missing STEP attribute".into()))?).alias("step"),
            lit(self.get_point().ok_or_else(|| PolarsError::ComputeError("Missing POINT attribute".into()))?).alias("point"),
            lit(self.get_exposure().ok_or_else(|| PolarsError::ComputeError("Missing EXPOSURE attribute".into()))?).alias("exposure"),
            timestamp_col,
        ]))
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl PlateData {
    #[pyo3(name = "to_polars")]
    fn py_to_polars(&self) -> PyResult<PyDataFrame> {
        Ok(PyDataFrame(
            self.to_polars()
                .map_err(|e| PyValueError::new_err(format!("Failed to convert to Polars: {}", e)))?
                .collect()
                .map_err(|e| PyValueError::new_err(format!("Failed to collect Polars DataFrame: {}", e)))?
        ))
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[cfg_attr(feature = "python", pyclass(get_all, set_all, module = "qslib._qslib"))]
pub struct Attribute {
    pub key: String,
    pub value: String,
}


impl FilterDataCollection {
    /// Load and parse filter data from XML file
    pub fn from_file<P: AsRef<std::path::Path>>(
        path: P,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let xml_str = std::fs::read_to_string(path)?;
        let data: FilterDataCollection = quick_xml::de::from_str(&xml_str)?;
        Ok(data)
    }

    /// Load and parse filter data from XML bytes.
    pub fn from_xml_bytes(data: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        let xml_str = std::str::from_utf8(data)?;
        let fdc: FilterDataCollection = quick_xml::de::from_str(xml_str)?;
        Ok(fdc)
    }

    /// Load filter data from individual per-reading XML files.
    ///
    /// Each file contains a `<PlatePointData>` element with one or more `<PlateData>` inside.
    /// Files are parsed and collected into a single `FilterDataCollection`.
    pub fn from_individual_files<P: AsRef<std::path::Path>>(
        paths: &[P],
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let mut plate_point_data = Vec::new();
        for path in paths {
            let xml_str = std::fs::read_to_string(path)?;
            // Each file is a standalone <PlatePointData> element.
            // Try parsing as PlatePointData first, then fall back to full collection.
            if let Ok(ppd) = quick_xml::de::from_str::<PlatePointData>(&xml_str) {
                plate_point_data.push(ppd);
            } else if let Ok(fdc) = quick_xml::de::from_str::<FilterDataCollection>(&xml_str) {
                plate_point_data.extend(fdc.plate_point_data);
            } else {
                return Err(format!(
                    "Failed to parse {}: not a PlatePointData or FilterDataCollection",
                    path.as_ref().display()
                ).into());
            }
        }
        Ok(FilterDataCollection {
            name: "FilterData".to_string(),
            plate_point_data,
        })
    }

    /// Populate timestamps on each `PlateData` from quant data.
    ///
    /// For each plate data entry, finds the longest-exposure quant file for the
    /// matching (stage, cycle, step, point, filter_set) and sets the timestamp.
    pub fn set_timestamps_from_quant(&mut self, quant_data: &QuantDataCollection) {
        for ppd in &mut self.plate_point_data {
            for pd in &mut ppd.plate_data {
                if let Ok(fs_str) = pd.filter_set() {
                    if let Ok(fs) = FilterSet::from_string(fs_str) {
                        let exposures = quant_data.get_exposures(
                            ppd.stage as u32,
                            ppd.cycle as u32,
                            ppd.step as u32,
                            ppd.point as u32,
                            fs,
                        );
                        if let Some(qf) = exposures
                            .iter()
                            .max_by(|a, b| {
                                a.conditions
                                    .exposure_ms
                                    .partial_cmp(&b.conditions.exposure_ms)
                                    .unwrap()
                            })
                        {
                            pd.timestamp = Some(qf.conditions.timestamp);
                        }
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Filterdata reconstruction from quant + calibrations
// ---------------------------------------------------------------------------

use crate::calibration::{BackgroundCalibration, PureDyeCalibration, UniformityCalibration};
use crate::quant::{QuantDataCollection, QuantFile};

const DEFAULT_EXPOSURE: f64 = 600.0;
const MAX_NUM_SAT: u32 = 3;

/// Select the best exposure for each well from a set of quant files
/// at different exposure indices for the same collection point.
///
/// Returns (selected_file_index, exposure_ms) per well.
fn select_exposures(quant_files: &[&QuantFile], n_wells: usize) -> Vec<(usize, f64)> {
    if quant_files.len() == 1 {
        let exp = quant_files[0].conditions.exposure_ms;
        return vec![(0, exp); n_wells];
    }

    // Sort by exposure_ms descending (longest first)
    let mut by_exp: Vec<(usize, f64)> = quant_files
        .iter()
        .enumerate()
        .map(|(i, qf)| (i, qf.conditions.exposure_ms))
        .collect();
    by_exp.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

    let mut result = Vec::with_capacity(n_wells);

    for w in 0..n_wells {
        let mut selected = None;

        // Try longest first: pick first where saturation < MAX_NUM_SAT
        for &(idx, exp) in &by_exp {
            if quant_files[idx].wells[w].inner.saturation < MAX_NUM_SAT {
                selected = Some((idx, exp));
                break;
            }
        }

        // Fallback: shortest exposure
        if selected.is_none() {
            let (idx, exp) = *by_exp.last().unwrap();
            selected = Some((idx, exp));
        }

        result.push(selected.unwrap());
    }

    result
}

/// Reconstruct filterdata from quant data and calibrations.
pub fn reconstruct_filterdata(
    quant_data: &QuantDataCollection,
    uniformity: &UniformityCalibration,
    background: &BackgroundCalibration,
    puredye: Option<&PureDyeCalibration>,
) -> Result<FilterDataCollection, DataError> {
    let n_wells = (quant_data.n_rows * quant_data.n_cols) as usize;

    // Collect unique (stage, cycle, step, point, filter) tuples
    let mut collection_filter_points: Vec<(u32, u32, u32, u32, FilterSet)> = quant_data
        .iter()
        .map(|(k, _)| (k.stage, k.cycle, k.step, k.point, k.filter_set))
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();
    collection_filter_points.sort();

    // Group by (stage, cycle, step, point)
    let mut point_groups: std::collections::BTreeMap<(u32, u32, u32, u32), Vec<PlateData>> =
        std::collections::BTreeMap::new();

    for (stage, cycle, step, point, filter_set) in &collection_filter_points {
        let exposures = quant_data.get_exposures(*stage, *cycle, *step, *point, *filter_set);
        if exposures.is_empty() {
            continue;
        }

        // Look up calibration data
        let uni_mat = match uniformity.uniformity.get(filter_set) {
            Some(m) => m,
            None => continue,
        };
        let bg_offset = match background.offset.get(filter_set) {
            Some(m) => m,
            None => continue,
        };
        let bg_slope = match background.slope.get(filter_set) {
            Some(m) => m,
            None => continue,
        };
        let color_balance = puredye
            .and_then(|pd| pd.color_balance.get(filter_set).copied())
            .unwrap_or(1.0);

        // Select exposures per well
        let exposure_refs: Vec<&QuantFile> = exposures.iter().copied().collect();
        let selected = select_exposures(&exposure_refs, n_wells);

        // Compute filterdata for each well
        let mut well_data = vec![0.0f64; n_wells];
        for w in 0..n_wells {
            let (file_idx, exp) = selected[w];
            let wq = &exposure_refs[file_idx].wells[w];

            let net_total = wq.inner.sum
                - wq.inner.count as f64 * wq.outer.sum / wq.outer.count as f64;

            let fd = uni_mat.data[w]
                * color_balance
                * (DEFAULT_EXPOSURE * uniformity.signal_norm / exp)
                * (net_total - bg_offset.data[w] - bg_slope.data[w] * exp);

            well_data[w] = fd;
        }

        // Get temperature and timestamp from the longest exposure
        let longest_idx = exposures
            .iter()
            .enumerate()
            .max_by(|a, b| {
                a.1.conditions
                    .exposure_ms
                    .partial_cmp(&b.1.conditions.exposure_ms)
                    .unwrap()
            })
            .map(|(i, _)| i)
            .unwrap_or(0);
        let cond = &exposures[longest_idx].conditions;
        let timestamp = cond.timestamp;
        let temp_str = cond
            .sample_temperatures
            .iter()
            .map(|t| t.to_string())
            .collect::<Vec<_>>()
            .join(",");

        let attributes = vec![
            Attribute {
                key: "FILTER_SET".to_string(),
                value: filter_set.lowerform(),
            },
            Attribute {
                key: "STAGE".to_string(),
                value: stage.to_string(),
            },
            Attribute {
                key: "CYCLE".to_string(),
                value: cycle.to_string(),
            },
            Attribute {
                key: "STEP".to_string(),
                value: step.to_string(),
            },
            Attribute {
                key: "POINT".to_string(),
                value: point.to_string(),
            },
            Attribute {
                key: "EXPOSURE".to_string(),
                value: (cond.exposure_ms as i32).to_string(),
            },
            Attribute {
                key: "TEMPERATURE".to_string(),
                value: temp_str,
            },
        ];

        let pd = PlateData {
            rows: quant_data.n_rows,
            cols: quant_data.n_cols,
            well_data,
            attributes,
            timestamp: Some(timestamp),
            set_temperatures: None,
        };

        point_groups
            .entry((*stage, *cycle, *step, *point))
            .or_default()
            .push(pd);
    }

    // Build PlatePointData from groups
    let plate_point_data: Vec<PlatePointData> = point_groups
        .into_iter()
        .map(|((stage, cycle, step, point), plate_data)| PlatePointData {
            stage: stage as i32,
            cycle: cycle as i32,
            step: step as i32,
            point: point as i32,
            plate_data,
        })
        .collect();

    Ok(FilterDataCollection {
        name: "FilterData".to_string(),
        plate_point_data,
    })
}

/// Detect the calibration path prefix in an EDS archive.
fn detect_cal_prefix(archive: &mut ::zip::ZipArchive<std::fs::File>) -> String {
    for i in 0..archive.len() {
        if let Ok(entry) = archive.by_index(i) {
            let name = entry.name().to_string();
            if name.starts_with("apldbio/sds/calibrations/") {
                return "apldbio/sds/calibrations/".to_string();
            }
            if name.starts_with("calibrations/") {
                return "calibrations/".to_string();
            }
        }
    }
    "apldbio/sds/calibrations/".to_string()
}

/// High-level: reconstruct filterdata from an EDS archive.
pub fn reconstruct_filterdata_from_eds<P: AsRef<std::path::Path>>(
    path: P,
) -> Result<FilterDataCollection, Box<dyn std::error::Error>> {
    let path = path.as_ref();

    // Load quant data
    let quant_data = QuantDataCollection::from_eds(path)?;

    // Read calibration files
    let file = std::fs::File::open(path)?;
    let mut archive = ::zip::ZipArchive::new(file)?;
    let cal_prefix = detect_cal_prefix(&mut archive);

    let mut read_cal = |name: &str| -> Result<String, Box<dyn std::error::Error>> {
        let path = format!("{}{}", cal_prefix, name);
        let mut entry = archive.by_name(&path)?;
        let mut content = String::new();
        std::io::Read::read_to_string(&mut entry, &mut content)?;
        Ok(content)
    };

    let uniformity = UniformityCalibration::parse(&read_cal("uniformity.ini")?)?;
    let background = BackgroundCalibration::parse(&read_cal("background.ini")?)?;
    let puredye = match read_cal("puredye.ini") {
        Ok(text) => Some(PureDyeCalibration::parse(&text)?),
        Err(_) => None,
    };

    let fdc = reconstruct_filterdata(&quant_data, &uniformity, &background, puredye.as_ref())
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;

    Ok(fdc)
}

/// High-level: reconstruct filterdata from TIFFs in an EDS archive.
///
/// Same as `reconstruct_filterdata_from_eds` but derives quant data from
/// TIFF images + ROI calibration instead of pre-computed .quant files.
pub fn reconstruct_filterdata_from_tiffs<P: AsRef<std::path::Path>>(
    path: P,
) -> Result<FilterDataCollection, Box<dyn std::error::Error>> {
    let path = path.as_ref();

    // Load quant data from TIFFs
    let quant_data = QuantDataCollection::from_tiffs_in_eds(path)?;

    // Read calibration files
    let file = std::fs::File::open(path)?;
    let mut archive = ::zip::ZipArchive::new(file)?;
    let cal_prefix = detect_cal_prefix(&mut archive);

    let mut read_cal = |name: &str| -> Result<String, Box<dyn std::error::Error>> {
        let path = format!("{}{}", cal_prefix, name);
        let mut entry = archive.by_name(&path)?;
        let mut content = String::new();
        std::io::Read::read_to_string(&mut entry, &mut content)?;
        Ok(content)
    };

    let uniformity = UniformityCalibration::parse(&read_cal("uniformity.ini")?)?;
    let background = BackgroundCalibration::parse(&read_cal("background.ini")?)?;
    let puredye = match read_cal("puredye.ini") {
        Ok(text) => Some(PureDyeCalibration::parse(&text)?),
        Err(_) => None,
    };

    let fdc = reconstruct_filterdata(&quant_data, &uniformity, &background, puredye.as_ref())
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;

    Ok(fdc)
}

/// Reconstruct filterdata from an EDS archive (PyO3 wrapper).
#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "reconstruct_filterdata_from_eds")]
pub fn py_reconstruct_filterdata_from_eds(path: &str) -> PyResult<FilterDataCollection> {
    reconstruct_filterdata_from_eds(path)
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

/// Reconstruct filterdata from TIFFs in an EDS archive (PyO3 wrapper).
#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "reconstruct_filterdata_from_tiffs")]
pub fn py_reconstruct_filterdata_from_tiffs(path: &str) -> PyResult<FilterDataCollection> {
    reconstruct_filterdata_from_tiffs(path)
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

/// Reconstruct filterdata from parsed components (PyO3 wrapper).
#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "reconstruct_filterdata")]
pub fn py_reconstruct_filterdata(
    quant_data: &QuantDataCollection,
    uniformity: &UniformityCalibration,
    background: &BackgroundCalibration,
    puredye: Option<&PureDyeCalibration>,
) -> PyResult<FilterDataCollection> {
    reconstruct_filterdata(quant_data, uniformity, background, puredye)
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

#[cfg(feature = "python")]
#[pymethods]
impl FilterDataCollection {

    #[staticmethod]
    #[pyo3(name = "read_file")]
    pub fn py_read_file(path: &str) -> PyResult<Self> {
        let xml_str = std::fs::read_to_string(path)
            .map_err(|e| PyValueError::new_err(format!("Failed to read file {}: {}", path, e)))?;
        let data: FilterDataCollection = quick_xml::de::from_str(&xml_str)
            .map_err(|e| PyValueError::new_err(format!("Failed to parse XML: {}", e)))?;
        Ok(data)
    }

    #[staticmethod]
    #[pyo3(name = "from_xml_bytes")]
    pub fn py_from_xml_bytes(data: &[u8]) -> PyResult<Self> {
        Self::from_xml_bytes(data)
            .map_err(|e| PyValueError::new_err(format!("Failed to parse XML bytes: {}", e)))
    }

    #[staticmethod]
    #[pyo3(name = "from_individual_files")]
    pub fn py_from_individual_files(paths: Vec<String>) -> PyResult<Self> {
        Self::from_individual_files(&paths)
            .map_err(|e| PyValueError::new_err(format!("Failed to load individual files: {}", e)))
    }

    #[pyo3(name = "set_timestamps_from_quant")]
    pub fn py_set_timestamps_from_quant(&mut self, quant_data: &QuantDataCollection) {
        self.set_timestamps_from_quant(quant_data);
    }

    #[pyo3(name = "to_polars")]
    pub fn py_to_polars(&self) -> PyResult<PyDataFrame> {
        let lfs: Result<Vec<_>, _> = self.plate_point_data.iter().map(|pd| pd.to_polars()).collect();
        let lfs = lfs.map_err(|e| PyValueError::new_err(format!("Failed to convert plate point data to Polars: {}", e)))?;
        let lf = concat(lfs, UnionArgs::default())
            .map_err(|e| PyValueError::new_err(format!("Failed to concat Polars DataFrames: {}", e)))?;
        Ok(PyDataFrame(
            lf.collect()
                .map_err(|e| PyValueError::new_err(format!("Failed to collect Polars DataFrame: {}", e)))?
        ))
    }
}

/// Generate well names for a plate type (96 or 384).
fn gen_well_names(plate_type: u32) -> Vec<String> {
    let (rows, cols) = match plate_type {
        96 => (8u8, 12u32),
        384 => (16u8, 24u32),
        _ => panic!("Unsupported plate type: {plate_type}"),
    };
    (0..rows)
        .flat_map(|row| {
            (1..=cols).map(move |col| format!("{}{}", (b'A' + row) as char, col))
        })
        .collect()
}

/// Parse v2 filter data JSON into a Polars DataFrame.
///
/// The JSON is an array of collection points, each with:
/// - `collectionPoint`: {stage, cycle, step, point}
/// - `filterData`: [{filterSet, exposure, wellFluorescences: [...]}]
/// - `zoneTemperatures`: [...]
///
/// Returns a long-form DataFrame with columns:
///   filter_set, stage, cycle, step, point, well, fluorescence, zone, temperature, exposure
pub fn parse_filterdata_v2_json(json_str: &str, plate_type: u32) -> Result<DataFrame, PolarsError> {
    let data: Vec<serde_json::Value> = serde_json::from_str(json_str)
        .map_err(|e| PolarsError::ComputeError(format!("JSON parse error: {e}").into()))?;

    let well_names = gen_well_names(plate_type);
    let n_wells = well_names.len();

    let mut filter_sets: Vec<String> = Vec::new();
    let mut stages: Vec<i64> = Vec::new();
    let mut cycles: Vec<i64> = Vec::new();
    let mut steps: Vec<i64> = Vec::new();
    let mut points: Vec<i64> = Vec::new();
    let mut wells: Vec<String> = Vec::new();
    let mut fluorescences: Vec<f64> = Vec::new();
    let mut zones: Vec<u32> = Vec::new();
    let mut temperatures: Vec<f64> = Vec::new();
    let mut exposures: Vec<f64> = Vec::new();

    for entry in &data {
        let cp = &entry["collectionPoint"];
        let stage = cp["stage"].as_i64().unwrap_or(0);
        let cycle = cp["cycle"].as_i64().unwrap_or(0);
        let step = cp["step"].as_i64().unwrap_or(0);
        let point = cp["point"].as_i64().unwrap_or(0);

        let zone_temps: Vec<f64> = entry["zoneTemperatures"]
            .as_array()
            .map(|a| a.iter().filter_map(|v| v.as_f64()).collect())
            .unwrap_or_default();
        let n_zones = zone_temps.len();
        let zone_size = if n_zones > 0 { n_wells / n_zones } else { n_wells };

        let filter_data = entry["filterData"].as_array();
        if let Some(fds) = filter_data {
            for fd in fds {
                let fs_raw = fd["filterSet"].as_str().unwrap_or("unknown");
                let fs = fs_raw.to_lowercase().replace('_', "-");
                let exposure = fd["exposure"].as_f64().unwrap_or(0.0);

                let well_fl = fd["wellFluorescences"].as_array();
                if let Some(wf) = well_fl {
                    for (i, (wname, fl_val)) in well_names.iter().zip(wf.iter()).enumerate() {
                        filter_sets.push(fs.clone());
                        stages.push(stage);
                        cycles.push(cycle);
                        steps.push(step);
                        points.push(point);
                        wells.push(wname.clone());
                        fluorescences.push(fl_val.as_f64().unwrap_or(f64::NAN));
                        exposures.push(exposure);

                        let zone_idx = if zone_size > 0 { i / zone_size } else { 0 };
                        let zone_idx = zone_idx.min(n_zones.saturating_sub(1));
                        zones.push(zone_idx as u32);
                        temperatures.push(
                            zone_temps.get(zone_idx).copied().unwrap_or(f64::NAN),
                        );
                    }
                }
            }
        }
    }

    DataFrame::new(vec![
        Column::new("filter_set".into(), &filter_sets),
        Column::new("stage".into(), &stages),
        Column::new("cycle".into(), &cycles),
        Column::new("step".into(), &steps),
        Column::new("point".into(), &points),
        Column::new("well".into(), &wells),
        Column::new("fluorescence".into(), &fluorescences),
        Column::new("zone".into(), &zones),
        Column::new("temperature".into(), &temperatures),
        Column::new("exposure".into(), &exposures),
    ])
}

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "parse_filterdata_v2_json")]
pub fn py_parse_filterdata_v2_json(json_str: &str, plate_type: u32) -> PyResult<PyDataFrame> {
    parse_filterdata_v2_json(json_str, plate_type)
        .map(PyDataFrame)
        .map_err(|e| PyValueError::new_err(format!("Failed to parse v2 filter data: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_filter_data() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
        <PlatePointDataCollection>
            <Name>FilterData</Name>
            <PlatePointData>
                <Stage>2</Stage>
                <Cycle>1</Cycle>
                <Step>1</Step>
                <Point>1</Point>
                <PlateData>
                    <Rows>8</Rows>
                    <Cols>12</Cols>
                    <WellData>1.0 2.0 3.0</WellData>
                    <Attribute>
                        <key>FILTER_SET</key>
                        <value>x1-m1</value>
                    </Attribute>
                </PlateData>
            </PlatePointData>
        </PlatePointDataCollection>"#;

        let data: FilterDataCollection = quick_xml::de::from_str(xml).unwrap();
        assert_eq!(data.name, "FilterData");
        assert_eq!(data.plate_point_data.len(), 1);

        let plate_data = &data.plate_point_data[0].plate_data[0];
        assert_eq!(plate_data.rows, 8);
        assert_eq!(plate_data.cols, 12);
        assert_eq!(plate_data.well_data, vec![1.0, 2.0, 3.0]);
        assert_eq!(plate_data.get_attribute("FILTER_SET"), Some("x1-m1"));
    }

    #[test]
    fn test_to_lineprotocol() {
        let plate_data = PlateData {
            rows: 2,
            cols: 12,
            well_data: vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
            attributes: vec![
                Attribute {
                    key: "FILTER_SET".to_string(),
                    value: "x1-m1".to_string(),
                },
                Attribute {
                    key: "STAGE".to_string(),
                    value: "2".to_string(),
                },
                Attribute {
                    key: "CYCLE".to_string(),
                    value: "1".to_string(),
                },
                Attribute {
                    key: "STEP".to_string(),
                    value: "1".to_string(),
                },
                Attribute {
                    key: "POINT".to_string(),
                    value: "1".to_string(),
                },
                Attribute {
                    key: "TEMPERATURE".to_string(),
                    value: "25.0,26.0,27.0".to_string(),
                },
            ],
            timestamp: Some(1234567890.123),
            set_temperatures: Some(vec![25.0, 26.0, 27.0]),
        };

        let lines = plate_data
            .to_lineprotocol(Some("test_run"), None, None, None)
            .unwrap();

        assert_eq!(lines.len(), 6);
        assert!(lines[0].starts_with("filterdata,filter_set=x1-m1,row=A,col=01"));
        assert!(lines[0].contains("fluorescence=1"));
        assert!(lines[0].contains("stage=02i"));
        assert!(lines[0].contains("cycle=001i"));
        assert!(lines[0].contains("run_name=\"test_run\""));
        assert!(lines[0].contains("temperature_read=25"));
        assert!(lines[0].contains("temperature_set=25"));
    }

    // =====================================================================
    // Additional data module tests
    // =====================================================================

    #[test]
    fn test_plate_data_get_attribute() {
        let plate_data = PlateData {
            rows: 8,
            cols: 12,
            well_data: vec![],
            attributes: vec![
                Attribute { key: "KEY1".to_string(), value: "value1".to_string() },
                Attribute { key: "KEY2".to_string(), value: "value2".to_string() },
            ],
            timestamp: None,
            set_temperatures: None,
        };

        assert_eq!(plate_data.get_attribute("KEY1"), Some("value1"));
        assert_eq!(plate_data.get_attribute("KEY2"), Some("value2"));
        assert_eq!(plate_data.get_attribute("NONEXISTENT"), None);
    }

    #[test]
    fn test_plate_data_filter_set() {
        let plate_data = PlateData {
            rows: 8,
            cols: 12,
            well_data: vec![],
            attributes: vec![
                Attribute { key: "FILTER_SET".to_string(), value: "x1-m4".to_string() },
            ],
            timestamp: None,
            set_temperatures: None,
        };

        assert_eq!(plate_data.filter_set().unwrap(), "x1-m4");
    }

    #[test]
    fn test_plate_data_filter_set_missing() {
        let plate_data = PlateData {
            rows: 8,
            cols: 12,
            well_data: vec![],
            attributes: vec![],
            timestamp: None,
            set_temperatures: None,
        };

        assert!(plate_data.filter_set().is_err());
    }

    #[test]
    fn test_plate_data_get_temperatures() {
        let plate_data = PlateData {
            rows: 8,
            cols: 12,
            well_data: vec![],
            attributes: vec![
                Attribute { key: "TEMPERATURE".to_string(), value: "25.0,26.5,27.0".to_string() },
            ],
            timestamp: None,
            set_temperatures: None,
        };

        let temps = plate_data.get_temperatures().unwrap();
        assert_eq!(temps.len(), 3);
        assert!((temps[0] - 25.0).abs() < 0.001);
        assert!((temps[1] - 26.5).abs() < 0.001);
        assert!((temps[2] - 27.0).abs() < 0.001);
    }

    #[test]
    fn test_plate_data_get_numeric_attributes() {
        let plate_data = PlateData {
            rows: 8,
            cols: 12,
            well_data: vec![],
            attributes: vec![
                Attribute { key: "STAGE".to_string(), value: "2".to_string() },
                Attribute { key: "CYCLE".to_string(), value: "5".to_string() },
                Attribute { key: "STEP".to_string(), value: "1".to_string() },
                Attribute { key: "POINT".to_string(), value: "10".to_string() },
                Attribute { key: "EXPOSURE".to_string(), value: "500".to_string() },
            ],
            timestamp: None,
            set_temperatures: None,
        };

        assert_eq!(plate_data.get_stage(), Some(2));
        assert_eq!(plate_data.get_cycle(), Some(5));
        assert_eq!(plate_data.get_step(), Some(1));
        assert_eq!(plate_data.get_point(), Some(10));
        assert_eq!(plate_data.get_exposure(), Some(500));
    }

    #[test]
    fn test_plate_point_data_fields() {
        let ppd = PlatePointData {
            stage: 1,
            cycle: 2,
            step: 3,
            point: 4,
            plate_data: vec![],
        };

        assert_eq!(ppd.stage, 1);
        assert_eq!(ppd.cycle, 2);
        assert_eq!(ppd.step, 3);
        assert_eq!(ppd.point, 4);
    }

    #[test]
    fn test_filter_data_collection_parse_multiple_plates() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
        <PlatePointDataCollection>
            <Name>MultiPlateData</Name>
            <PlatePointData>
                <Stage>1</Stage>
                <Cycle>1</Cycle>
                <Step>1</Step>
                <Point>1</Point>
                <PlateData>
                    <Rows>8</Rows>
                    <Cols>12</Cols>
                    <WellData>1.0 2.0 3.0</WellData>
                    <Attribute>
                        <key>FILTER_SET</key>
                        <value>x1-m1</value>
                    </Attribute>
                </PlateData>
                <PlateData>
                    <Rows>8</Rows>
                    <Cols>12</Cols>
                    <WellData>4.0 5.0 6.0</WellData>
                    <Attribute>
                        <key>FILTER_SET</key>
                        <value>x1-m2</value>
                    </Attribute>
                </PlateData>
            </PlatePointData>
        </PlatePointDataCollection>"#;

        let data: FilterDataCollection = quick_xml::de::from_str(xml).unwrap();
        assert_eq!(data.name, "MultiPlateData");
        assert_eq!(data.plate_point_data.len(), 1);
        assert_eq!(data.plate_point_data[0].plate_data.len(), 2);
        assert_eq!(data.plate_point_data[0].plate_data[0].get_attribute("FILTER_SET"), Some("x1-m1"));
        assert_eq!(data.plate_point_data[0].plate_data[1].get_attribute("FILTER_SET"), Some("x1-m2"));
    }

    #[test]
    fn test_well_data_parsing() {
        let xml = r#"<?xml version="1.0"?>
        <PlatePointDataCollection>
            <Name>Test</Name>
            <PlatePointData>
                <Stage>1</Stage>
                <Cycle>1</Cycle>
                <Step>1</Step>
                <Point>1</Point>
                <PlateData>
                    <Rows>2</Rows>
                    <Cols>3</Cols>
                    <WellData>1.5 2.5 3.5 4.5 5.5 6.5</WellData>
                    <Attribute>
                        <key>FILTER_SET</key>
                        <value>test</value>
                    </Attribute>
                </PlateData>
            </PlatePointData>
        </PlatePointDataCollection>"#;

        let data: FilterDataCollection = quick_xml::de::from_str(xml).unwrap();
        let plate = &data.plate_point_data[0].plate_data[0];
        assert_eq!(plate.well_data, vec![1.5, 2.5, 3.5, 4.5, 5.5, 6.5]);
    }

    #[test]
    fn test_lineprotocol_with_additional_tags() {
        let plate_data = PlateData {
            rows: 1,
            cols: 2,
            well_data: vec![1.0, 2.0],
            attributes: vec![
                Attribute { key: "FILTER_SET".to_string(), value: "x1-m1".to_string() },
            ],
            timestamp: None,
            set_temperatures: None,
        };

        let lines = plate_data
            .to_lineprotocol(None, None, None, Some(&[("machine", "QS5")]))
            .unwrap();

        assert!(lines[0].contains("machine=QS5"));
    }

    #[test]
    fn test_lineprotocol_with_samples() {
        let plate_data = PlateData {
            rows: 1,
            cols: 2,
            well_data: vec![1.0, 2.0],
            attributes: vec![
                Attribute { key: "FILTER_SET".to_string(), value: "x1-m1".to_string() },
            ],
            timestamp: None,
            set_temperatures: None,
        };

        let samples = vec!["sample1".to_string(), "sample2".to_string()];
        let lines = plate_data
            .to_lineprotocol(None, Some(&samples), None, None)
            .unwrap();

        assert!(lines[0].contains("sample=\"sample1\""));
        assert!(lines[1].contains("sample=\"sample2\""));
    }

    // ===================================================================
    // Filterdata reconstruction tests
    // ===================================================================

    fn test_eds_path() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/test.eds")
    }

    #[test]
    fn test_reconstruct_from_eds() {
        let fdc = reconstruct_filterdata_from_eds(test_eds_path()).unwrap();
        assert_eq!(fdc.name, "FilterData");
        assert!(!fdc.plate_point_data.is_empty());
    }

    #[test]
    fn test_reconstruct_matches_filterdata() {
        // Load existing filterdata.xml
        let file = std::fs::File::open(test_eds_path()).expect("test.eds not found");
        let mut archive = ::zip::ZipArchive::new(file).expect("invalid zip");
        let mut fd_entry = archive
            .by_name("apldbio/sds/filterdata.xml")
            .expect("filterdata.xml not found");
        let mut fd_xml = Vec::new();
        std::io::Read::read_to_end(&mut fd_entry, &mut fd_xml).unwrap();
        drop(fd_entry);
        drop(archive);

        let actual = FilterDataCollection::from_xml_bytes(&fd_xml).unwrap();

        // Reconstruct
        let reconstructed = reconstruct_filterdata_from_eds(test_eds_path()).unwrap();

        // Build lookup: (stage, cycle, step, point, filter_set) -> well_data
        let mut actual_map: std::collections::HashMap<(i32, i32, i32, i32, String), Vec<f64>> =
            std::collections::HashMap::new();
        for ppd in &actual.plate_point_data {
            for pd in &ppd.plate_data {
                if let Some(fs) = pd.get_attribute("FILTER_SET") {
                    actual_map.insert(
                        (ppd.stage, ppd.cycle, ppd.step, ppd.point, fs.to_string()),
                        pd.well_data.clone(),
                    );
                }
            }
        }

        let mut recon_map: std::collections::HashMap<(i32, i32, i32, i32, String), Vec<f64>> =
            std::collections::HashMap::new();
        for ppd in &reconstructed.plate_point_data {
            for pd in &ppd.plate_data {
                if let Some(fs) = pd.get_attribute("FILTER_SET") {
                    recon_map.insert(
                        (ppd.stage, ppd.cycle, ppd.step, ppd.point, fs.to_string()),
                        pd.well_data.clone(),
                    );
                }
            }
        }

        // Compare matching keys
        let mut total_points = 0;
        let mut max_abs_err = 0.0f64;
        let mut matched_keys = 0;

        for (key, actual_vals) in &actual_map {
            if let Some(recon_vals) = recon_map.get(key) {
                matched_keys += 1;
                let n = actual_vals.len().min(recon_vals.len());
                for i in 0..n {
                    let abs_err = (actual_vals[i] - recon_vals[i]).abs();
                    total_points += 1;
                    if abs_err > max_abs_err {
                        max_abs_err = abs_err;
                    }
                    assert!(
                        abs_err < 0.1,
                        "Abs error {:.6} at key {:?}, well {}: actual={:.4}, recon={:.4}",
                        abs_err,
                        key,
                        i,
                        actual_vals[i],
                        recon_vals[i]
                    );
                }
            }
        }

        assert!(matched_keys > 0, "No matching keys between actual and reconstructed");
        assert!(total_points > 0, "No data points compared");
        eprintln!(
            "Reconstruction validated: {} keys, {} points, max_abs_err={:.6}",
            matched_keys, total_points, max_abs_err
        );
    }

    #[test]
    fn test_reconstruct_exposure_selection() {
        // Create synthetic quant files to test exposure selection
        use crate::quant::{QuantConditions, QuantFile, QuantRegion, WellQuant};

        let make_qf = |exposure: f64, saturation: u32| QuantFile {
            conditions: QuantConditions {
                stage: 2,
                cycle: 1,
                step: 1,
                point: 1,
                excitation: "x1".to_string(),
                emission: "m1".to_string(),
                filter_set: FilterSet::new(1, 1, true),
                exposure_ms: exposure,
                timestamp: 0.0,
                block_temperatures: vec![60.0; 6],
                sample_temperatures: vec![60.0; 6],
                cover_temperature: 105.0,
            },
            wells: vec![
                WellQuant {
                    inner: QuantRegion {
                        sum: 1000.0,
                        count: 1000,
                        saturation,
                    },
                    outer: QuantRegion {
                        sum: 200.0,
                        count: 200,
                        saturation: 0,
                    },
                };
                1
            ],
            n_rows: 1,
            n_cols: 1,
        };

        // Non-saturated at both exposures: should pick longest
        let qf_short = make_qf(10.0, 0);
        let qf_long = make_qf(600.0, 0);
        let files: Vec<&QuantFile> = vec![&qf_short, &qf_long];
        let sel = select_exposures(&files, 1);
        assert_eq!(sel[0].1, 600.0);

        // Long exposure saturated: should pick short
        let qf_long_sat = make_qf(600.0, 5);
        let files: Vec<&QuantFile> = vec![&qf_short, &qf_long_sat];
        let sel = select_exposures(&files, 1);
        assert_eq!(sel[0].1, 10.0);
    }

    #[test]
    fn test_reconstruct_all_saturated_fallback() {
        use crate::quant::{QuantConditions, QuantFile, QuantRegion, WellQuant};

        let make_qf_sat = |exposure: f64| QuantFile {
            conditions: QuantConditions {
                stage: 2,
                cycle: 1,
                step: 1,
                point: 1,
                excitation: "x1".to_string(),
                emission: "m1".to_string(),
                filter_set: FilterSet::new(1, 1, true),
                exposure_ms: exposure,
                timestamp: 0.0,
                block_temperatures: vec![60.0; 6],
                sample_temperatures: vec![60.0; 6],
                cover_temperature: 105.0,
            },
            wells: vec![
                WellQuant {
                    inner: QuantRegion {
                        sum: 1000.0,
                        count: 1000,
                        saturation: 10,
                    },
                    outer: QuantRegion {
                        sum: 200.0,
                        count: 200,
                        saturation: 0,
                    },
                };
                1
            ],
            n_rows: 1,
            n_cols: 1,
        };

        // All saturated: should fall back to shortest
        let qf_short = make_qf_sat(10.0);
        let qf_long = make_qf_sat(600.0);
        let files: Vec<&QuantFile> = vec![&qf_short, &qf_long];
        let sel = select_exposures(&files, 1);
        assert_eq!(sel[0].1, 10.0);
    }

    #[test]
    fn test_reconstruct_filterdata_from_tiffs_matches_quants() {
        let tiff_eds_path =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tiff-collection.eds");

        // Reconstruct from quant files
        let from_quants = reconstruct_filterdata_from_eds(&tiff_eds_path).unwrap();
        // Reconstruct from TIFFs
        let from_tiffs = reconstruct_filterdata_from_tiffs(&tiff_eds_path).unwrap();

        assert_eq!(
            from_quants.plate_point_data.len(),
            from_tiffs.plate_point_data.len(),
            "Different number of plate point data entries"
        );

        for (q_ppd, t_ppd) in from_quants
            .plate_point_data
            .iter()
            .zip(from_tiffs.plate_point_data.iter())
        {
            assert_eq!(q_ppd.stage, t_ppd.stage);
            assert_eq!(q_ppd.cycle, t_ppd.cycle);
            assert_eq!(q_ppd.step, t_ppd.step);
            assert_eq!(q_ppd.point, t_ppd.point);
            assert_eq!(q_ppd.plate_data.len(), t_ppd.plate_data.len());

            for (q_pd, t_pd) in q_ppd.plate_data.iter().zip(t_ppd.plate_data.iter()) {
                assert_eq!(q_pd.well_data.len(), t_pd.well_data.len());
                for (i, (q_val, t_val)) in
                    q_pd.well_data.iter().zip(t_pd.well_data.iter()).enumerate()
                {
                    assert!(
                        (q_val - t_val).abs() < 0.1,
                        "Stage {}, Cycle {}, well {}: quant={} vs tiff={}",
                        q_ppd.stage,
                        q_ppd.cycle,
                        i,
                        q_val,
                        t_val
                    );
                }
            }
        }
    }

    #[test]
    fn test_parse_filterdata_v2_json() {
        let json = r#"[
            {
                "collectionPoint": {"stage": 2, "cycle": 1, "step": 2, "point": 1},
                "filterData": [
                    {"filterSet": "X1_M1", "exposure": 600, "wellFluorescences": [100.0, 200.0, 300.0]}
                ],
                "zoneTemperatures": [60.0]
            }
        ]"#;

        let df = parse_filterdata_v2_json(json, 96).unwrap();
        assert_eq!(df.height(), 3);
        assert_eq!(df.column("filter_set").unwrap().str().unwrap().get(0).unwrap(), "x1-m1");
        assert_eq!(df.column("stage").unwrap().i64().unwrap().get(0).unwrap(), 2);
        assert_eq!(df.column("well").unwrap().str().unwrap().get(0).unwrap(), "A1");
        assert_eq!(df.column("well").unwrap().str().unwrap().get(1).unwrap(), "A2");
        assert_eq!(df.column("well").unwrap().str().unwrap().get(2).unwrap(), "A3");
        assert!((df.column("fluorescence").unwrap().f64().unwrap().get(0).unwrap() - 100.0).abs() < 1e-10);
        assert!((df.column("temperature").unwrap().f64().unwrap().get(0).unwrap() - 60.0).abs() < 1e-10);
    }

    #[test]
    fn test_gen_well_names() {
        let names96 = gen_well_names(96);
        assert_eq!(names96.len(), 96);
        assert_eq!(names96[0], "A1");
        assert_eq!(names96[11], "A12");
        assert_eq!(names96[12], "B1");
        assert_eq!(names96[95], "H12");

        let names384 = gen_well_names(384);
        assert_eq!(names384.len(), 384);
        assert_eq!(names384[0], "A1");
        assert_eq!(names384[383], "P24");
    }
}
