use std::collections::HashMap;
use thiserror::Error;
use winnow::ascii::{float, line_ending, space0};
use winnow::combinator::{alt, opt, separated};
use winnow::error::ModalResult;
use winnow::prelude::*;
use winnow::token::{take_till, take_until};

use crate::data::FilterSet;

#[cfg(feature = "python")]
use pyo3::prelude::*;

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

#[derive(Debug, Error)]
pub enum CalibrationError {
    #[error("INI parse error: {0}")]
    ParseError(String),
    #[error("Missing section: {0}")]
    MissingSection(String),
    #[error("Missing key '{key}' in section '{section}'")]
    MissingKey { section: String, key: String },
    #[error("Invalid matrix dimensions: expected consistent row lengths")]
    InvalidMatrix,
    #[error("Invalid filter set key: {0}")]
    InvalidFilterSet(String),
}

// ---------------------------------------------------------------------------
// Generic INI types
// ---------------------------------------------------------------------------

/// A value in a calibration INI file: either a scalar string or a matrix.
#[derive(Debug, Clone)]
pub enum IniValue {
    Scalar(String),
    Matrix(Vec<Vec<f64>>),
}

/// A parsed INI section: name → entries.
pub type IniSection = HashMap<String, IniValue>;

/// A parsed INI file: section name → entries.
pub type IniFile = HashMap<String, IniSection>;

// ---------------------------------------------------------------------------
// Winnow parsers for INI-with-heredoc format
// ---------------------------------------------------------------------------

/// Parse a line ending (\r\n or \n).
fn le(input: &mut &[u8]) -> ModalResult<()> {
    line_ending.void().parse_next(input)
}

/// Skip blank lines (lines containing only whitespace).
fn skip_blank_lines(input: &mut &[u8]) -> ModalResult<()> {
    loop {
        let checkpoint = *input;
        if space0::<_, winnow::error::ContextError>
            .parse_next(input)
            .is_ok()
            && le(input).is_ok()
        {
            continue;
        }
        *input = checkpoint;
        break;
    }
    Ok(())
}

/// Parse a comment line (starting with # or ;).
fn comment_line(input: &mut &[u8]) -> ModalResult<()> {
    alt((b"#", b";")).parse_next(input)?;
    take_till(0.., |c: u8| c == b'\n' || c == b'\r').parse_next(input)?;
    le(input)?;
    Ok(())
}

/// Skip any number of blank and comment lines.
fn skip_blanks_and_comments(input: &mut &[u8]) -> ModalResult<()> {
    loop {
        let checkpoint = *input;
        if comment_line(input).is_ok() {
            continue;
        }
        *input = checkpoint;
        // Try blank line (whitespace-only line)
        if space0::<_, winnow::error::ContextError>
            .parse_next(input)
            .is_ok()
            && le(input).is_ok()
        {
            continue;
        }
        *input = checkpoint;
        break;
    }
    Ok(())
}

/// Parse `[section_name]` followed by newline.
fn section_header(input: &mut &[u8]) -> ModalResult<String> {
    b"[".parse_next(input)?;
    let name = take_until(0.., b"]" as &[u8]).parse_next(input)?;
    b"]".parse_next(input)?;
    space0.parse_next(input)?;
    le(input)?;
    Ok(String::from_utf8_lossy(name).to_string())
}

/// Parse a single row of comma-separated f64 values.
fn matrix_row(input: &mut &[u8]) -> ModalResult<Vec<f64>> {
    let row: Vec<f64> = separated(1.., float::<_, f64, _>, b",").parse_next(input)?;
    // Optional trailing comma
    opt(b",").parse_next(input)?;
    // Optional trailing whitespace
    space0.parse_next(input)?;
    le(input)?;
    Ok(row)
}

/// Parse a matrix entry: `key=<<eof\n rows \neof\n`.
fn matrix_entry(input: &mut &[u8]) -> ModalResult<(String, IniValue)> {
    let key = take_till(1.., |c: u8| c == b'=' || c == b'\n' || c == b'\r').parse_next(input)?;
    let key = String::from_utf8_lossy(key).trim().to_string();
    space0.parse_next(input)?;
    b"=".parse_next(input)?;
    space0.parse_next(input)?;
    b"<<eof".parse_next(input)?;
    space0.parse_next(input)?;
    le(input)?;

    // Parse rows until we see "eof" on its own line
    let mut rows = Vec::new();
    loop {
        let checkpoint = *input;
        // Check for eof terminator
        space0::<_, winnow::error::ContextError>
            .parse_next(input)
            .ok();
        if input.starts_with(b"eof") {
            let after_eof = &input[3..];
            // eof must be followed by newline, EOF, or whitespace+newline
            if after_eof.is_empty() || after_eof[0] == b'\n' || after_eof[0] == b'\r' {
                // Consume "eof" and the newline
                *input = &input[3..];
                space0::<_, winnow::error::ContextError>
                    .parse_next(input)
                    .ok();
                opt(le).parse_next(input)?;
                break;
            }
        }
        *input = checkpoint;

        // Parse a matrix row
        match matrix_row(input) {
            Ok(row) => rows.push(row),
            Err(_) => {
                // Skip non-parseable lines within heredoc (shouldn't happen)
                *input = checkpoint;
                take_till(0.., |c: u8| c == b'\n' || c == b'\r').parse_next(input)?;
                le(input)?;
            }
        }
    }

    Ok((key, IniValue::Matrix(rows)))
}

/// Parse a scalar entry: `key = value\n`. Value must not start with `<<eof`.
fn scalar_entry(input: &mut &[u8]) -> ModalResult<(String, IniValue)> {
    let key = take_till(1.., |c: u8| c == b'=' || c == b'\n' || c == b'\r').parse_next(input)?;
    let key = String::from_utf8_lossy(key).trim().to_string();
    b"=".parse_next(input)?;
    space0.parse_next(input)?;

    // Make sure this isn't a heredoc
    if input.starts_with(b"<<eof") {
        return Err(winnow::error::ErrMode::from_input(input));
    }

    let value = take_till(0.., |c: u8| c == b'\n' || c == b'\r').parse_next(input)?;
    le(input)?;
    let value = String::from_utf8_lossy(value).trim().to_string();
    Ok((key, IniValue::Scalar(value)))
}

/// Parse a single INI entry (matrix or scalar or comment).
fn ini_entry(input: &mut &[u8]) -> ModalResult<Option<(String, IniValue)>> {
    // Skip blank lines
    skip_blank_lines(input)?;

    if input.is_empty() || input[0] == b'[' {
        return Err(winnow::error::ErrMode::from_input(input));
    }

    // Try comment line first
    let checkpoint_comment = *input;
    if comment_line(input).is_ok() {
        return Ok(None);
    }
    *input = checkpoint_comment;

    // Try matrix entry first (since it also starts with key=)
    let checkpoint_matrix = *input;
    if let Ok(entry) = matrix_entry(input) {
        return Ok(Some(entry));
    }
    *input = checkpoint_matrix;

    // Try scalar entry
    let entry = scalar_entry(input)?;
    Ok(Some(entry))
}

/// Parse a single INI section: header + entries.
fn ini_section(input: &mut &[u8]) -> ModalResult<(String, IniSection)> {
    skip_blanks_and_comments(input)?;
    let name = section_header(input)?;

    let mut section = IniSection::new();
    loop {
        let checkpoint = *input;
        match ini_entry(input) {
            Ok(Some((key, value))) => {
                section.insert(key, value);
            }
            Ok(None) => {
                // Comment line, skip
            }
            Err(_) => {
                *input = checkpoint;
                break;
            }
        }
    }

    Ok((name, section))
}

/// Parse a complete INI file.
pub fn parse_ini(input: &[u8]) -> Result<IniFile, CalibrationError> {
    let mut rest = input;
    let inp = &mut rest;

    skip_blanks_and_comments(inp).map_err(|e| CalibrationError::ParseError(format!("{}", e)))?;

    let mut file = IniFile::new();
    while !inp.is_empty() {
        match ini_section(inp) {
            Ok((name, section)) => {
                file.insert(name, section);
            }
            Err(_) => {
                // Skip any remaining unparseable content
                break;
            }
        }
    }

    Ok(file)
}

// ---------------------------------------------------------------------------
// Helper: extract filter-keyed matrices from an INI section
// ---------------------------------------------------------------------------

fn extract_filter_matrices(
    ini: &IniFile,
    section_name: &str,
) -> Result<HashMap<FilterSet, WellMatrix>, CalibrationError> {
    let section = ini
        .get(section_name)
        .ok_or_else(|| CalibrationError::MissingSection(section_name.to_string()))?;

    let mut result = HashMap::new();
    for (key, value) in section {
        // Only process filter-set keys (e.g., "x1-m1")
        if let Ok(fs) = FilterSet::from_string(key) {
            match value {
                IniValue::Matrix(rows) => {
                    let wm = WellMatrix::from_rows(rows)?;
                    result.insert(fs, wm);
                }
                _ => {
                    return Err(CalibrationError::InvalidFilterSet(format!(
                        "Expected matrix for key '{}' in section '{}'",
                        key, section_name
                    )));
                }
            }
        }
    }

    Ok(result)
}

/// Extract filter-keyed scalar values from an INI section.
fn extract_filter_scalars(
    ini: &IniFile,
    section_name: &str,
) -> Result<HashMap<FilterSet, f64>, CalibrationError> {
    let section = ini
        .get(section_name)
        .ok_or_else(|| CalibrationError::MissingSection(section_name.to_string()))?;

    let mut result = HashMap::new();
    for (key, value) in section {
        if let Ok(fs) = FilterSet::from_string(key) {
            if let IniValue::Scalar(s) = value {
                let val: f64 = s.parse().map_err(|_| {
                    CalibrationError::ParseError(format!(
                        "Invalid float for key '{}' in section '{}': {}",
                        key, section_name, s
                    ))
                })?;
                result.insert(fs, val);
            }
        }
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// WellMatrix
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
#[cfg_attr(feature = "python", pyclass(get_all, module = "qslib._qslib"))]
pub struct WellMatrix {
    pub data: Vec<f64>,
    pub n_rows: u32,
    pub n_cols: u32,
}

impl WellMatrix {
    pub fn from_rows(rows: &[Vec<f64>]) -> Result<Self, CalibrationError> {
        if rows.is_empty() {
            return Err(CalibrationError::InvalidMatrix);
        }
        let n_cols = rows[0].len();
        for row in rows {
            if row.len() != n_cols {
                return Err(CalibrationError::InvalidMatrix);
            }
        }
        let data: Vec<f64> = rows.iter().flat_map(|r| r.iter().copied()).collect();
        Ok(Self {
            data,
            n_rows: rows.len() as u32,
            n_cols: n_cols as u32,
        })
    }

    pub fn get(&self, row: u32, col: u32) -> f64 {
        self.data[(row * self.n_cols + col) as usize]
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl WellMatrix {
    fn __repr__(&self) -> String {
        format!("WellMatrix({}x{})", self.n_rows, self.n_cols)
    }

    #[pyo3(name = "get")]
    fn py_get(&self, row: u32, col: u32) -> f64 {
        self.get(row, col)
    }
}

// ---------------------------------------------------------------------------
// UniformityCalibration
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
#[cfg_attr(feature = "python", pyclass(module = "qslib._qslib"))]
pub struct UniformityCalibration {
    pub uniformity: HashMap<FilterSet, WellMatrix>,
    pub signal_norm: f64,
}

impl UniformityCalibration {
    pub fn parse(text: &str) -> Result<Self, CalibrationError> {
        let ini = parse_ini(text.as_bytes())?;

        // Extract signal_norm factor
        let signal_norm_section = ini
            .get("signal_norm")
            .ok_or_else(|| CalibrationError::MissingSection("signal_norm".to_string()))?;
        let factor =
            signal_norm_section
                .get("factor")
                .ok_or_else(|| CalibrationError::MissingKey {
                    section: "signal_norm".to_string(),
                    key: "factor".to_string(),
                })?;
        let signal_norm = match factor {
            IniValue::Scalar(s) => s.parse::<f64>().map_err(|_| {
                CalibrationError::ParseError(format!("Invalid signal_norm factor: {}", s))
            })?,
            _ => {
                return Err(CalibrationError::ParseError(
                    "signal_norm factor should be scalar".to_string(),
                ))
            }
        };

        // Extract uniformity matrices
        let uniformity = extract_filter_matrices(&ini, "uniformity")?;

        Ok(Self {
            uniformity,
            signal_norm,
        })
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl UniformityCalibration {
    #[staticmethod]
    #[pyo3(name = "parse")]
    fn py_parse(text: &str) -> PyResult<Self> {
        Self::parse(text).map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
    }

    #[getter]
    fn signal_norm(&self) -> f64 {
        self.signal_norm
    }

    fn __repr__(&self) -> String {
        format!(
            "UniformityCalibration(signal_norm={}, filters={})",
            self.signal_norm,
            self.uniformity.len()
        )
    }

    fn get_uniformity(&self, filter_set: &FilterSet) -> Option<WellMatrix> {
        self.uniformity.get(filter_set).cloned()
    }
}

// ---------------------------------------------------------------------------
// BackgroundCalibration
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
#[cfg_attr(feature = "python", pyclass(module = "qslib._qslib"))]
pub struct BackgroundCalibration {
    pub offset: HashMap<FilterSet, WellMatrix>,
    pub slope: HashMap<FilterSet, WellMatrix>,
}

impl BackgroundCalibration {
    pub fn parse(text: &str) -> Result<Self, CalibrationError> {
        let ini = parse_ini(text.as_bytes())?;
        let offset = extract_filter_matrices(&ini, "background_offset")?;
        let slope = extract_filter_matrices(&ini, "background_slope")?;
        Ok(Self { offset, slope })
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl BackgroundCalibration {
    #[staticmethod]
    #[pyo3(name = "parse")]
    fn py_parse(text: &str) -> PyResult<Self> {
        Self::parse(text).map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
    }

    fn __repr__(&self) -> String {
        format!(
            "BackgroundCalibration(offset_filters={}, slope_filters={})",
            self.offset.len(),
            self.slope.len()
        )
    }

    fn get_offset(&self, filter_set: &FilterSet) -> Option<WellMatrix> {
        self.offset.get(filter_set).cloned()
    }

    fn get_slope(&self, filter_set: &FilterSet) -> Option<WellMatrix> {
        self.slope.get(filter_set).cloned()
    }
}

// ---------------------------------------------------------------------------
// PureDyeCalibration
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
#[cfg_attr(feature = "python", pyclass(module = "qslib._qslib"))]
pub struct PureDyeCalibration {
    pub color_balance: HashMap<FilterSet, f64>,
}

impl PureDyeCalibration {
    pub fn parse(text: &str) -> Result<Self, CalibrationError> {
        let ini = parse_ini(text.as_bytes())?;
        let color_balance = extract_filter_scalars(&ini, "color_balance")?;
        Ok(Self { color_balance })
    }

    /// Create an empty calibration (used when puredye.ini is absent).
    pub fn empty() -> Self {
        Self {
            color_balance: HashMap::new(),
        }
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl PureDyeCalibration {
    #[staticmethod]
    #[pyo3(name = "parse")]
    fn py_parse(text: &str) -> PyResult<Self> {
        Self::parse(text).map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
    }

    fn __repr__(&self) -> String {
        format!("PureDyeCalibration(filters={})", self.color_balance.len())
    }

    fn get_color_balance(&self, filter_set: &FilterSet) -> f64 {
        self.color_balance.get(filter_set).copied().unwrap_or(1.0)
    }
}

// ---------------------------------------------------------------------------
// RoiCalibration
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
#[cfg_attr(feature = "python", pyclass(module = "qslib._qslib"))]
pub struct RoiCalibration {
    pub roi_diameter: HashMap<FilterSet, f64>,
    pub ring_size: HashMap<FilterSet, f64>,
    pub horizontal_pos: HashMap<FilterSet, WellMatrix>,
    pub vertical_pos: HashMap<FilterSet, WellMatrix>,
    pub well_area: HashMap<FilterSet, WellMatrix>,
    pub ring_area: HashMap<FilterSet, WellMatrix>,
    pub n_rows: u32,
    pub n_cols: u32,
    pub image_width: u32,
    pub image_height: u32,
    pub filter_sets: Vec<FilterSet>,
    pub instrument_type: String,
}

/// Extract a scalar string value from an INI section.
fn extract_scalar_string(
    ini: &IniFile,
    section_name: &str,
    key: &str,
) -> Result<String, CalibrationError> {
    let section = ini
        .get(section_name)
        .ok_or_else(|| CalibrationError::MissingSection(section_name.to_string()))?;
    match section.get(key) {
        Some(IniValue::Scalar(s)) => Ok(s.clone()),
        Some(_) => Err(CalibrationError::ParseError(format!(
            "Expected scalar for key '{}' in section '{}'",
            key, section_name
        ))),
        None => Err(CalibrationError::MissingKey {
            section: section_name.to_string(),
            key: key.to_string(),
        }),
    }
}

/// Extract a scalar u32 value from an INI section.
fn extract_scalar_u32(
    ini: &IniFile,
    section_name: &str,
    key: &str,
) -> Result<u32, CalibrationError> {
    let s = extract_scalar_string(ini, section_name, key)?;
    s.parse::<u32>().map_err(|_| {
        CalibrationError::ParseError(format!(
            "Invalid u32 for key '{}' in section '{}': {}",
            key, section_name, s
        ))
    })
}

impl RoiCalibration {
    pub fn parse(text: &str) -> Result<Self, CalibrationError> {
        let ini = parse_ini(text.as_bytes())?;

        let roi_diameter = extract_filter_scalars(&ini, "roi_diameter")?;
        let ring_size = extract_filter_scalars(&ini, "ring_size")?;
        let horizontal_pos = extract_filter_matrices(&ini, "horizontal_pos")?;
        let vertical_pos = extract_filter_matrices(&ini, "vertical_pos")?;
        let well_area = extract_filter_matrices(&ini, "well_area")?;
        let ring_area = extract_filter_matrices(&ini, "ring_area")?;

        let n_rows = extract_scalar_u32(&ini, "instrument", "numofrow")?;
        let n_cols = extract_scalar_u32(&ini, "instrument", "numofcol")?;
        let instrument_type = extract_scalar_string(&ini, "instrument", "type")?;

        // Parse imagesize "width,height"
        let imagesize_str = extract_scalar_string(&ini, "camera", "imagesize")?;
        let parts: Vec<&str> = imagesize_str.split(',').collect();
        if parts.len() != 2 {
            return Err(CalibrationError::ParseError(format!(
                "Invalid imagesize format: {}",
                imagesize_str
            )));
        }
        let image_width: u32 = parts[0].trim().parse().map_err(|_| {
            CalibrationError::ParseError(format!("Invalid imagesize width: {}", parts[0]))
        })?;
        let image_height: u32 = parts[1].trim().parse().map_err(|_| {
            CalibrationError::ParseError(format!("Invalid imagesize height: {}", parts[1]))
        })?;

        // Parse filtersets "x1-m1,x2-m2,..."
        let filtersets_str = extract_scalar_string(&ini, "filter", "filtersets")?;
        let filter_sets: Vec<FilterSet> = filtersets_str
            .split(',')
            .map(|s| FilterSet::from_string(s.trim()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(CalibrationError::ParseError)?;

        Ok(Self {
            roi_diameter,
            ring_size,
            horizontal_pos,
            vertical_pos,
            well_area,
            ring_area,
            n_rows,
            n_cols,
            image_width,
            image_height,
            filter_sets,
            instrument_type,
        })
    }

    /// Find the filter set that uses the given emission filter number.
    /// E.g., emission=4 finds x1-m4 from the filter_sets list.
    pub fn get_for_emission(&self, emission: u8) -> Option<FilterSet> {
        self.filter_sets
            .iter()
            .find(|fs| fs.em == emission)
            .copied()
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl RoiCalibration {
    #[staticmethod]
    #[pyo3(name = "parse")]
    fn py_parse(text: &str) -> PyResult<Self> {
        Self::parse(text).map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
    }

    #[getter]
    fn n_rows(&self) -> u32 {
        self.n_rows
    }

    #[getter]
    fn n_cols(&self) -> u32 {
        self.n_cols
    }

    #[getter]
    fn image_width(&self) -> u32 {
        self.image_width
    }

    #[getter]
    fn image_height(&self) -> u32 {
        self.image_height
    }

    #[getter]
    fn instrument_type(&self) -> &str {
        &self.instrument_type
    }

    #[getter]
    fn filter_sets(&self) -> Vec<FilterSet> {
        self.filter_sets.clone()
    }

    fn get_roi_diameter(&self, filter_set: &FilterSet) -> Option<f64> {
        self.roi_diameter.get(filter_set).copied()
    }

    fn get_ring_size(&self, filter_set: &FilterSet) -> Option<f64> {
        self.ring_size.get(filter_set).copied()
    }

    fn get_horizontal_pos(&self, filter_set: &FilterSet) -> Option<WellMatrix> {
        self.horizontal_pos.get(filter_set).cloned()
    }

    fn get_vertical_pos(&self, filter_set: &FilterSet) -> Option<WellMatrix> {
        self.vertical_pos.get(filter_set).cloned()
    }

    fn get_well_area(&self, filter_set: &FilterSet) -> Option<WellMatrix> {
        self.well_area.get(filter_set).cloned()
    }

    fn get_ring_area(&self, filter_set: &FilterSet) -> Option<WellMatrix> {
        self.ring_area.get(filter_set).cloned()
    }

    #[pyo3(name = "get_for_emission")]
    fn py_get_for_emission(&self, emission: u8) -> Option<FilterSet> {
        self.get_for_emission(emission)
    }

    fn __repr__(&self) -> String {
        format!(
            "RoiCalibration({}x{}, image={}x{}, filters={})",
            self.n_rows,
            self.n_cols,
            self.image_width,
            self.image_height,
            self.filter_sets.len()
        )
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::require_tiff_eds;
    use std::io::Read;

    fn read_from_test_eds(path: &str) -> String {
        let eds_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/test.eds");
        let file = std::fs::File::open(eds_path).expect("test.eds not found");
        let mut archive = ::zip::ZipArchive::new(file).expect("invalid zip");
        let mut entry = archive.by_name(path).expect("file not found in EDS");
        let mut content = String::new();
        entry.read_to_string(&mut content).expect("read failed");
        content
    }

    #[test]
    fn test_parse_ini_basic() {
        let input = b"[section1]\nkey1 = value1\nkey2=value2\n\n[section2]\nfoo=bar\n";
        let ini = parse_ini(input).unwrap();
        assert!(ini.contains_key("section1"));
        assert!(ini.contains_key("section2"));
        match ini["section1"].get("key1").unwrap() {
            IniValue::Scalar(s) => assert_eq!(s, "value1"),
            _ => panic!("expected scalar"),
        }
        match ini["section1"].get("key2").unwrap() {
            IniValue::Scalar(s) => assert_eq!(s, "value2"),
            _ => panic!("expected scalar"),
        }
    }

    #[test]
    fn test_parse_ini_heredoc() {
        let input = b"[data]\nmat=<<eof\n1.0,2.0,3.0\n4.0,5.0,6.0\neof\n";
        let ini = parse_ini(input).unwrap();
        match ini["data"].get("mat").unwrap() {
            IniValue::Matrix(rows) => {
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0], vec![1.0, 2.0, 3.0]);
                assert_eq!(rows[1], vec![4.0, 5.0, 6.0]);
            }
            _ => panic!("expected matrix"),
        }
    }

    #[test]
    fn test_parse_ini_heredoc_trailing_comma() {
        let input = b"[data]\nmat=<<eof\n1.0,2.0,3.0,\n4.0,5.0,6.0,\neof\n";
        let ini = parse_ini(input).unwrap();
        match ini["data"].get("mat").unwrap() {
            IniValue::Matrix(rows) => {
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0], vec![1.0, 2.0, 3.0]);
                assert_eq!(rows[1], vec![4.0, 5.0, 6.0]);
            }
            _ => panic!("expected matrix"),
        }
    }

    #[test]
    fn test_parse_ini_comments_and_blanks() {
        let input = b"# comment\n\n[section]\n# another comment\nkey = val\n\n";
        let ini = parse_ini(input).unwrap();
        assert!(ini.contains_key("section"));
        match ini["section"].get("key").unwrap() {
            IniValue::Scalar(s) => assert_eq!(s, "val"),
            _ => panic!("expected scalar"),
        }
    }

    #[test]
    fn test_parse_uniformity() {
        let text = read_from_test_eds("apldbio/sds/calibrations/uniformity.ini");
        let cal = UniformityCalibration::parse(&text).unwrap();

        // Check signal_norm is a reasonable value
        assert!(cal.signal_norm > 0.0 && cal.signal_norm < 1.0);
        assert!((cal.signal_norm - 0.0191165917).abs() < 1e-8);

        // Check uniformity matrices exist for expected filters
        let x1m1 = FilterSet::new(1, 1, true);
        assert!(cal.uniformity.contains_key(&x1m1));
        let mat = &cal.uniformity[&x1m1];
        assert_eq!(mat.n_rows, 8);
        assert_eq!(mat.n_cols, 12);
        assert_eq!(mat.data.len(), 96);

        // Spot-check: first value should be ~1.920635
        assert!((mat.get(0, 0) - 1.920635).abs() < 0.001);
    }

    #[test]
    fn test_parse_background() {
        let text = read_from_test_eds("apldbio/sds/calibrations/background.ini");
        let cal = BackgroundCalibration::parse(&text).unwrap();

        let x1m1 = FilterSet::new(1, 1, true);
        assert!(cal.offset.contains_key(&x1m1));
        assert!(cal.slope.contains_key(&x1m1));

        let offset = &cal.offset[&x1m1];
        assert_eq!(offset.n_rows, 8);
        assert_eq!(offset.n_cols, 12);

        let slope = &cal.slope[&x1m1];
        assert_eq!(slope.n_rows, 8);
        assert_eq!(slope.n_cols, 12);
    }

    #[test]
    fn test_parse_puredye() {
        let text = read_from_test_eds("apldbio/sds/calibrations/puredye.ini");
        let cal = PureDyeCalibration::parse(&text).unwrap();

        // All color_balance values should be 1.0 in test.eds
        let x1m1 = FilterSet::new(1, 1, true);
        assert!(cal.color_balance.contains_key(&x1m1));
        assert!((cal.color_balance[&x1m1] - 1.0).abs() < 1e-10);

        // Check multiple filters
        assert!(!cal.color_balance.is_empty());
        for &val in cal.color_balance.values() {
            assert!((val - 1.0).abs() < 1e-10);
        }
    }

    #[test]
    fn test_parse_puredye_absent() {
        let cal = PureDyeCalibration::empty();
        assert!(cal.color_balance.is_empty());
    }

    #[test]
    fn test_well_matrix_indexing() {
        let wm = WellMatrix::from_rows(&[vec![1.0, 2.0, 3.0], vec![4.0, 5.0, 6.0]]).unwrap();
        assert_eq!(wm.n_rows, 2);
        assert_eq!(wm.n_cols, 3);
        assert_eq!(wm.get(0, 0), 1.0);
        assert_eq!(wm.get(0, 2), 3.0);
        assert_eq!(wm.get(1, 0), 4.0);
        assert_eq!(wm.get(1, 2), 6.0);
    }

    #[test]
    fn test_well_matrix_invalid() {
        let result = WellMatrix::from_rows(&[vec![1.0, 2.0], vec![3.0]]);
        assert!(result.is_err());
    }

    #[test]
    fn test_uniformity_filter_count() {
        let text = read_from_test_eds("apldbio/sds/calibrations/uniformity.ini");
        let cal = UniformityCalibration::parse(&text).unwrap();
        // Should have 21 filter combinations (6 emissions × varying excitations)
        assert!(cal.uniformity.len() >= 10);
    }

    #[test]
    fn test_background_filter_count() {
        let text = read_from_test_eds("apldbio/sds/calibrations/background.ini");
        let cal = BackgroundCalibration::parse(&text).unwrap();
        // offset and slope should have the same number of filters
        assert_eq!(cal.offset.len(), cal.slope.len());
        assert!(cal.offset.len() >= 10);
    }

    fn read_from_tiff_eds(eds_path: &std::path::Path, path: &str) -> String {
        let file = std::fs::File::open(eds_path).expect("tiff-collection.eds not found");
        let mut archive = ::zip::ZipArchive::new(file).expect("invalid zip");
        let mut entry = archive.by_name(path).expect("file not found in EDS");
        let mut content = String::new();
        entry.read_to_string(&mut content).expect("read failed");
        content
    }

    #[test]
    fn test_parse_roi_calibration() {
        let eds_path = require_tiff_eds!();
        let text = read_from_tiff_eds(&eds_path, "apldbio/sds/calibrations/roi.ini");
        let cal = RoiCalibration::parse(&text).unwrap();

        assert_eq!(cal.n_rows, 8);
        assert_eq!(cal.n_cols, 12);
        assert_eq!(cal.image_width, 648);
        assert_eq!(cal.image_height, 486);
        assert_eq!(cal.instrument_type, "appletini");
        assert_eq!(cal.filter_sets.len(), 6);
    }

    #[test]
    fn test_roi_filter_sets() {
        let eds_path = require_tiff_eds!();
        let text = read_from_tiff_eds(&eds_path, "apldbio/sds/calibrations/roi.ini");
        let cal = RoiCalibration::parse(&text).unwrap();

        // Should have x1-m1, x2-m2, x3-m3, x4-m4, x5-m5, x5-m6
        let x1m1 = FilterSet::new(1, 1, true);
        let x4m4 = FilterSet::new(4, 4, true);
        assert!(cal.roi_diameter.contains_key(&x1m1));
        assert!(cal.roi_diameter.contains_key(&x4m4));
        assert!(cal.horizontal_pos.contains_key(&x4m4));
        assert!(cal.vertical_pos.contains_key(&x4m4));
    }

    #[test]
    fn test_roi_position_dimensions() {
        let eds_path = require_tiff_eds!();
        let text = read_from_tiff_eds(&eds_path, "apldbio/sds/calibrations/roi.ini");
        let cal = RoiCalibration::parse(&text).unwrap();

        let x4m4 = FilterSet::new(4, 4, true);
        let hpos = &cal.horizontal_pos[&x4m4];
        assert_eq!(hpos.n_rows, 8);
        assert_eq!(hpos.n_cols, 12);

        let vpos = &cal.vertical_pos[&x4m4];
        assert_eq!(vpos.n_rows, 8);
        assert_eq!(vpos.n_cols, 12);

        // Positions should be within image bounds
        for r in 0..8 {
            for c in 0..12 {
                let h = hpos.get(r, c);
                let v = vpos.get(r, c);
                assert!(h > 0.0 && h < cal.image_width as f64);
                assert!(v > 0.0 && v < cal.image_height as f64);
            }
        }
    }

    #[test]
    fn test_roi_get_for_emission() {
        let eds_path = require_tiff_eds!();
        let text = read_from_tiff_eds(&eds_path, "apldbio/sds/calibrations/roi.ini");
        let cal = RoiCalibration::parse(&text).unwrap();

        // m4 should map to x4-m4
        let fs = cal.get_for_emission(4).unwrap();
        assert_eq!(fs.em, 4);

        // m6 should map to x5-m6
        let fs = cal.get_for_emission(6).unwrap();
        assert_eq!(fs, FilterSet::new(5, 6, true));

        // m7 should not exist
        assert!(cal.get_for_emission(7).is_none());
    }

    #[test]
    fn test_roi_diameter_values() {
        let eds_path = require_tiff_eds!();
        let text = read_from_tiff_eds(&eds_path, "apldbio/sds/calibrations/roi.ini");
        let cal = RoiCalibration::parse(&text).unwrap();

        // ROI diameters should be around 35-36 pixels
        for (_, &d) in &cal.roi_diameter {
            assert!(d > 34.0 && d < 37.0, "ROI diameter {} out of range", d);
        }

        // Ring sizes should be 2
        for (_, &r) in &cal.ring_size {
            assert!((r - 2.0).abs() < 0.01);
        }
    }
}
