use std::collections::HashMap;
use thiserror::Error;

use crate::data::FilterSet;

#[cfg(feature = "python")]
use pyo3::prelude::*;

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

#[derive(Debug, Error)]
pub enum QuantError {
    #[error("Parse error: {0}")]
    ParseError(String),
    #[error("Missing section: {0}")]
    MissingSection(String),
    #[error("Missing condition field: {0}")]
    MissingField(String),
    #[error("Unpaired region: {0}")]
    UnpairedRegion(String),
    #[error("Invalid filename: {0}")]
    InvalidFilename(String),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("ZIP error: {0}")]
    ZipError(#[from] zip::result::ZipError),
}

// ---------------------------------------------------------------------------
// Data structures
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
#[cfg_attr(feature = "python", pyclass(get_all, module = "qslib._qslib"))]
pub struct QuantConditions {
    pub stage: u32,
    pub cycle: u32,
    pub step: u32,
    pub point: u32,
    pub excitation: String,
    pub emission: String,
    pub filter_set: FilterSet,
    pub exposure_ms: f64,
    pub timestamp: f64,
    pub block_temperatures: Vec<f64>,
    pub sample_temperatures: Vec<f64>,
    pub cover_temperature: f64,
}

#[derive(Debug, Clone, Copy)]
#[cfg_attr(feature = "python", pyclass(get_all, module = "qslib._qslib"))]
pub struct QuantRegion {
    pub sum: f64,
    pub count: u32,
    pub saturation: u32,
}

#[derive(Debug, Clone, Copy)]
#[cfg_attr(feature = "python", pyclass(get_all, module = "qslib._qslib"))]
pub struct WellQuant {
    pub inner: QuantRegion,
    pub outer: QuantRegion,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "python", pyclass(module = "qslib._qslib"))]
pub struct QuantFile {
    pub conditions: QuantConditions,
    pub wells: Vec<WellQuant>,
    pub n_rows: u32,
    pub n_cols: u32,
}

#[cfg(feature = "python")]
#[pymethods]
impl QuantFile {
    #[getter]
    fn conditions(&self) -> QuantConditions {
        self.conditions.clone()
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
    fn wells(&self) -> Vec<WellQuant> {
        self.wells.clone()
    }

    #[staticmethod]
    #[pyo3(name = "parse")]
    fn py_parse(data: &str) -> PyResult<Self> {
        Self::parse(data.as_bytes())
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
    }

    fn __repr__(&self) -> String {
        format!(
            "QuantFile(stage={}, cycle={}, filter={}, exposure={}ms, wells={})",
            self.conditions.stage,
            self.conditions.cycle,
            self.conditions.filter_set.lowerform(),
            self.conditions.exposure_ms,
            self.wells.len()
        )
    }
}

// ---------------------------------------------------------------------------
// Quant filename parser
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct QuantFilename {
    pub stage: u32,
    pub cycle: u32,
    pub step: u32,
    pub point: u32,
    pub emission: u8,
    pub excitation: u8,
    pub exposure_index: u32,
}

impl QuantFilename {
    /// Parse a quant/tiff filename like `S02_C001_T01_P0001_M1_X1_E1.quant` or `.tiff`.
    /// The filename may include a path prefix; only the basename is parsed.
    pub fn parse(filename: &str) -> Result<Self, QuantError> {
        // Extract basename
        let basename = filename
            .rsplit('/')
            .next()
            .unwrap_or(filename);

        let stem = basename
            .strip_suffix(".quant")
            .or_else(|| basename.strip_suffix(".tiff"))
            .ok_or_else(|| QuantError::InvalidFilename(filename.to_string()))?;

        let parts: Vec<&str> = stem.split('_').collect();
        if parts.len() != 7 {
            return Err(QuantError::InvalidFilename(filename.to_string()));
        }

        let parse_part = |part: &str, prefix: char| -> Result<u32, QuantError> {
            if !part.starts_with(prefix) {
                return Err(QuantError::InvalidFilename(filename.to_string()));
            }
            part[prefix.len_utf8()..]
                .parse::<u32>()
                .map_err(|_| QuantError::InvalidFilename(filename.to_string()))
        };

        Ok(Self {
            stage: parse_part(parts[0], 'S')?,
            cycle: parse_part(parts[1], 'C')?,
            step: parse_part(parts[2], 'T')?,
            point: parse_part(parts[3], 'P')?,
            emission: parse_part(parts[4], 'M')? as u8,
            excitation: parse_part(parts[5], 'X')? as u8,
            exposure_index: parse_part(parts[6], 'E')?,
        })
    }
}

// ---------------------------------------------------------------------------
// Quant file parser
// ---------------------------------------------------------------------------

impl QuantFile {
    /// Parse a quant file from raw bytes.
    pub fn parse(data: &[u8]) -> Result<Self, QuantError> {
        let text = String::from_utf8_lossy(data);

        // Split into sections by blank lines
        let sections: Vec<&str> = text.split("\n\n").collect();

        let mut conditions = None;
        let mut quant_data = None;

        for section in &sections {
            let section = section.trim();
            if section.starts_with("[conditions]") {
                conditions = Some(parse_conditions_section(section)?);
            } else if section.starts_with("[quant]") {
                quant_data = Some(parse_quant_section(section)?);
            }
        }

        let conditions =
            conditions.ok_or_else(|| QuantError::MissingSection("conditions".to_string()))?;
        let (wells, n_rows, n_cols) =
            quant_data.ok_or_else(|| QuantError::MissingSection("quant".to_string()))?;

        Ok(QuantFile {
            conditions,
            wells,
            n_rows,
            n_cols,
        })
    }
}

/// Parse the [conditions] section.
fn parse_conditions_section(section: &str) -> Result<QuantConditions, QuantError> {
    let lines: Vec<&str> = section.lines().collect();
    if lines.len() < 3 {
        return Err(QuantError::ParseError(
            "conditions section needs at least 3 lines".to_string(),
        ));
    }

    // Line 0: [conditions]
    // Line 1: headers (tab-separated)
    // Line 2: values (tab-separated)
    let headers: Vec<&str> = lines[1].split('\t').collect();
    let values: Vec<&str> = lines[2].split('\t').collect();

    let mut map = HashMap::new();
    for (h, v) in headers.iter().zip(values.iter()) {
        map.insert(*h, *v);
    }

    let get_str = |key: &str| -> Result<&str, QuantError> {
        map.get(key)
            .copied()
            .ok_or_else(|| QuantError::MissingField(key.to_string()))
    };

    let get_u32 = |key: &str| -> Result<u32, QuantError> {
        get_str(key)?
            .parse::<u32>()
            .map_err(|_| QuantError::ParseError(format!("Invalid u32 for {}", key)))
    };

    let get_f64 = |key: &str| -> Result<f64, QuantError> {
        get_str(key)?
            .parse::<f64>()
            .map_err(|_| QuantError::ParseError(format!("Invalid f64 for {}", key)))
    };

    let parse_comma_temps = |key: &str| -> Result<Vec<f64>, QuantError> {
        let s = get_str(key)?;
        s.split(',')
            .map(|t| {
                t.trim()
                    .parse::<f64>()
                    .map_err(|_| QuantError::ParseError(format!("Invalid temp in {}", key)))
            })
            .collect()
    };

    let excitation = get_str("ExcitationColor")?.to_string();
    let emission = get_str("EmissionColor")?.to_string();
    let filter_set = FilterSet::from_string(&format!("{}-{}", excitation, emission))
        .map_err(QuantError::ParseError)?;

    Ok(QuantConditions {
        stage: get_u32("Stage")?,
        cycle: get_u32("Cycle")?,
        step: get_u32("Step")?,
        point: get_u32("Point")?,
        excitation,
        emission,
        filter_set,
        exposure_ms: get_f64("Exposure")?,
        timestamp: get_f64("Time")?,
        block_temperatures: parse_comma_temps("BlockTemperature")?,
        sample_temperatures: parse_comma_temps("SampleTemperature")?,
        cover_temperature: get_f64("CoverTemperature")?,
    })
}

/// Parsed region info: (is_inner, row_index, col_index, sum, count, saturation).
struct RegionRow {
    is_inner: bool,
    row: u32,
    col: u32,
    sum: f64,
    count: u32,
    saturation: u32,
}

/// Parse one row of the [quant] section.
fn parse_region_row(line: &str) -> Result<RegionRow, QuantError> {
    let parts: Vec<&str> = line.split('\t').collect();
    if parts.len() < 4 {
        return Err(QuantError::ParseError(format!(
            "quant row needs 4 columns, got {}: {}",
            parts.len(),
            line
        )));
    }

    let region = parts[0];
    if region.len() < 3 {
        return Err(QuantError::ParseError(format!(
            "Invalid region name: {}",
            region
        )));
    }

    let is_inner = match region.as_bytes()[0] {
        b'I' => true,
        b'O' => false,
        _ => {
            return Err(QuantError::ParseError(format!(
                "Region must start with I or O: {}",
                region
            )))
        }
    };

    let row_letter = region.as_bytes()[1];
    if !row_letter.is_ascii_uppercase() {
        return Err(QuantError::ParseError(format!(
            "Invalid row letter in region: {}",
            region
        )));
    }
    let row = (row_letter - b'A') as u32;

    let col_str = &region[2..];
    let col: u32 = col_str
        .parse()
        .map_err(|_| QuantError::ParseError(format!("Invalid column in region: {}", region)))?;
    let col = col - 1; // Convert to 0-indexed

    let sum: f64 = parts[1]
        .parse()
        .map_err(|_| QuantError::ParseError(format!("Invalid sum: {}", parts[1])))?;
    let count: u32 = parts[2]
        .parse()
        .map_err(|_| QuantError::ParseError(format!("Invalid count: {}", parts[2])))?;
    let saturation: u32 = parts[3]
        .parse()
        .map_err(|_| QuantError::ParseError(format!("Invalid saturation: {}", parts[3])))?;

    Ok(RegionRow {
        is_inner,
        row,
        col,
        sum,
        count,
        saturation,
    })
}

/// Parse the [quant] section, returning (wells, n_rows, n_cols).
fn parse_quant_section(section: &str) -> Result<(Vec<WellQuant>, u32, u32), QuantError> {
    let lines: Vec<&str> = section.lines().collect();
    // Line 0: [quant]
    // Line 1: header row
    // Lines 2+: data rows

    if lines.len() < 3 {
        return Err(QuantError::ParseError(
            "quant section needs at least 3 lines".to_string(),
        ));
    }

    // Parse all region rows
    let mut inner_map: HashMap<(u32, u32), QuantRegion> = HashMap::new();
    let mut outer_map: HashMap<(u32, u32), QuantRegion> = HashMap::new();
    let mut max_row: u32 = 0;
    let mut max_col: u32 = 0;

    for line in &lines[2..] {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let rr = parse_region_row(line)?;

        if rr.row > max_row {
            max_row = rr.row;
        }
        if rr.col > max_col {
            max_col = rr.col;
        }

        let region = QuantRegion {
            sum: rr.sum,
            count: rr.count,
            saturation: rr.saturation,
        };

        if rr.is_inner {
            inner_map.insert((rr.row, rr.col), region);
        } else {
            outer_map.insert((rr.row, rr.col), region);
        }
    }

    let n_rows = max_row + 1;
    let n_cols = max_col + 1;

    // Build well array in row-major order
    let mut wells = Vec::with_capacity((n_rows * n_cols) as usize);
    for row in 0..n_rows {
        for col in 0..n_cols {
            let inner = inner_map
                .get(&(row, col))
                .ok_or_else(|| {
                    let row_letter = (b'A' + row as u8) as char;
                    QuantError::UnpairedRegion(format!("I{}{}", row_letter, col + 1))
                })?;
            let outer = outer_map
                .get(&(row, col))
                .ok_or_else(|| {
                    let row_letter = (b'A' + row as u8) as char;
                    QuantError::UnpairedRegion(format!("O{}{}", row_letter, col + 1))
                })?;
            wells.push(WellQuant {
                inner: *inner,
                outer: *outer,
            });
        }
    }

    Ok((wells, n_rows, n_cols))
}

// ---------------------------------------------------------------------------
// CollectionKey
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "python", pyclass(frozen, eq, hash, ord, get_all, module = "qslib._qslib"))]
pub struct CollectionKey {
    pub stage: u32,
    pub cycle: u32,
    pub step: u32,
    pub point: u32,
    pub filter_set: FilterSet,
    pub exposure_index: u32,
}

impl PartialOrd for CollectionKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CollectionKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.stage
            .cmp(&other.stage)
            .then(self.cycle.cmp(&other.cycle))
            .then(self.step.cmp(&other.step))
            .then(self.point.cmp(&other.point))
            .then(self.filter_set.cmp(&other.filter_set))
            .then(self.exposure_index.cmp(&other.exposure_index))
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl CollectionKey {
    fn __repr__(&self) -> String {
        format!(
            "CollectionKey(stage={}, cycle={}, step={}, point={}, filter={}, exposure_index={})",
            self.stage,
            self.cycle,
            self.step,
            self.point,
            self.filter_set.lowerform(),
            self.exposure_index
        )
    }
}

// ---------------------------------------------------------------------------
// QuantDataCollection
// ---------------------------------------------------------------------------

#[derive(Debug)]
#[cfg_attr(feature = "python", pyclass(module = "qslib._qslib"))]
pub struct QuantDataCollection {
    entries: HashMap<CollectionKey, QuantFile>,
    pub n_rows: u32,
    pub n_cols: u32,
}

impl QuantDataCollection {
    /// Build from parsed (filename, data) pairs.
    pub fn from_quant_files(files: Vec<(String, Vec<u8>)>) -> Result<Self, QuantError> {
        let mut entries = HashMap::new();
        let mut n_rows: Option<u32> = None;
        let mut n_cols: Option<u32> = None;

        for (filename, data) in files {
            // Skip non-.quant files
            if !filename.ends_with(".quant") {
                continue;
            }

            let qfn = QuantFilename::parse(&filename)?;
            let qf = QuantFile::parse(&data)?;

            // Verify/set geometry
            match (n_rows, n_cols) {
                (None, None) => {
                    n_rows = Some(qf.n_rows);
                    n_cols = Some(qf.n_cols);
                }
                (Some(r), Some(c)) => {
                    if qf.n_rows != r || qf.n_cols != c {
                        return Err(QuantError::ParseError(format!(
                            "Inconsistent plate geometry: {}x{} vs {}x{}",
                            qf.n_rows, qf.n_cols, r, c
                        )));
                    }
                }
                _ => unreachable!(),
            }

            let key = CollectionKey {
                stage: qf.conditions.stage,
                cycle: qf.conditions.cycle,
                step: qf.conditions.step,
                point: qf.conditions.point,
                filter_set: qf.conditions.filter_set,
                exposure_index: qfn.exposure_index,
            };

            entries.insert(key, qf);
        }

        Ok(Self {
            entries,
            n_rows: n_rows.unwrap_or(8),
            n_cols: n_cols.unwrap_or(12),
        })
    }

    /// Load from an EDS archive path.
    pub fn from_eds<P: AsRef<std::path::Path>>(path: P) -> Result<Self, QuantError> {
        let file = std::fs::File::open(path)?;
        let mut archive = ::zip::ZipArchive::new(file)?;

        // Detect EDS version by checking which path prefix exists
        let quant_prefix = detect_quant_prefix(&mut archive);

        // Collect all .quant files
        let mut files = Vec::new();
        for i in 0..archive.len() {
            let mut entry = archive.by_index(i)?;
            let name = entry.name().to_string();
            if name.starts_with(&quant_prefix) && name.ends_with(".quant") {
                let mut data = Vec::new();
                std::io::Read::read_to_end(&mut entry, &mut data)?;
                files.push((name, data));
            }
        }

        Self::from_quant_files(files)
    }

    /// Load from an extracted directory containing .quant files.
    /// Searches for a `quant/` subdirectory, or reads .quant files directly from the given path.
    pub fn from_directory<P: AsRef<std::path::Path>>(path: P) -> Result<Self, QuantError> {
        let path = path.as_ref();

        // Try to find quant files in a "quant" subdirectory first
        let quant_dir = if path.join("quant").is_dir() {
            path.join("quant")
        } else {
            path.to_path_buf()
        };

        let mut files = Vec::new();
        for entry in std::fs::read_dir(&quant_dir)? {
            let entry = entry?;
            let file_path = entry.path();
            if file_path.extension().and_then(|e| e.to_str()) == Some("quant") {
                let filename = file_path
                    .file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .to_string();
                let data = std::fs::read(&file_path)?;
                files.push((filename, data));
            }
        }

        Self::from_quant_files(files)
    }

    /// All unique collection keys, sorted.
    pub fn keys(&self) -> Vec<&CollectionKey> {
        let mut keys: Vec<&CollectionKey> = self.entries.keys().collect();
        keys.sort();
        keys
    }

    /// Get a single quant file by key.
    pub fn get(&self, key: &CollectionKey) -> Option<&QuantFile> {
        self.entries.get(key)
    }

    /// Get a specific quant file by component parts.
    pub fn get_by_parts(
        &self,
        stage: u32,
        cycle: u32,
        step: u32,
        point: u32,
        filter_set: FilterSet,
        exposure_index: u32,
    ) -> Option<&QuantFile> {
        self.entries.get(&CollectionKey {
            stage,
            cycle,
            step,
            point,
            filter_set,
            exposure_index,
        })
    }

    /// Get all exposures for a collection point, sorted by exposure index.
    pub fn get_exposures(
        &self,
        stage: u32,
        cycle: u32,
        step: u32,
        point: u32,
        filter_set: FilterSet,
    ) -> Vec<&QuantFile> {
        let mut result: Vec<(u32, &QuantFile)> = self
            .entries
            .iter()
            .filter(|(k, _)| {
                k.stage == stage
                    && k.cycle == cycle
                    && k.step == step
                    && k.point == point
                    && k.filter_set == filter_set
            })
            .map(|(k, v)| (k.exposure_index, v))
            .collect();
        result.sort_by_key(|(idx, _)| *idx);
        result.into_iter().map(|(_, v)| v).collect()
    }

    /// All unique filter sets, sorted.
    pub fn filter_sets(&self) -> Vec<FilterSet> {
        let mut fs: Vec<FilterSet> = self
            .entries
            .keys()
            .map(|k| k.filter_set)
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        fs.sort();
        fs
    }

    /// All unique exposure indices, sorted.
    pub fn exposure_indices(&self) -> Vec<u32> {
        let mut indices: Vec<u32> = self
            .entries
            .keys()
            .map(|k| k.exposure_index)
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        indices.sort();
        indices
    }

    /// All unique (stage, cycle, step, point) tuples, sorted.
    pub fn collection_points(&self) -> Vec<(u32, u32, u32, u32)> {
        let mut pts: Vec<(u32, u32, u32, u32)> = self
            .entries
            .keys()
            .map(|k| (k.stage, k.cycle, k.step, k.point))
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        pts.sort();
        pts
    }

    /// Iterate over all entries.
    pub fn iter(&self) -> impl Iterator<Item = (&CollectionKey, &QuantFile)> {
        self.entries.iter()
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

// ---------------------------------------------------------------------------
// TiffImageMeta — metadata from quant XML <image> elements
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct TiffImageMeta {
    pub stage: u32,
    pub cycle: u32,
    pub step: u32,
    pub point: u32,
    pub emission: u8,
    pub excitation: u8,
    pub exposure_ms: f64,
    pub exposure_index: u32,
    pub timestamp: f64,
    pub block_temperatures: Vec<f64>,
    pub sample_temperatures: Vec<f64>,
    pub cover_temperature: f64,
    pub filename: String,
}

/// Parse a quant XML file (e.g. `1-1-1-1-m4-x1.xml`) to extract image metadata.
///
/// The XML has structure:
/// ```xml
/// <run>
///   <images>
///     <image stage="01" cycle="001" step="01" point="0001"
///            emissionColor="m4" excitationColor="x1"
///            exposure="40" exposureIndex="1" time="1772576982.871"
///            blockTemperature="24.99,25.00,..." sampleTemperature="25.00,25.00,..."
///            coverTemperature="104.957">S01_C001_T01_P0001_M4_X1_E1.tiff</image>
///   </images>
/// </run>
/// ```
pub fn parse_quant_xml(data: &[u8]) -> Result<Vec<TiffImageMeta>, QuantError> {
    use quick_xml::events::Event;
    use quick_xml::reader::Reader;

    let mut reader = Reader::from_reader(data);
    let mut buf = Vec::new();
    let mut results = Vec::new();
    let mut in_images = false;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) | Ok(Event::Empty(ref e))
                if e.name().as_ref() == b"images" =>
            {
                in_images = true;
            }
            Ok(Event::End(ref e)) if e.name().as_ref() == b"images" => {
                in_images = false;
            }
            Ok(Event::Start(ref e)) if in_images && e.name().as_ref() == b"image" => {
                let mut meta = TiffImageMeta {
                    stage: 0,
                    cycle: 0,
                    step: 0,
                    point: 0,
                    emission: 0,
                    excitation: 0,
                    exposure_ms: 0.0,
                    exposure_index: 0,
                    timestamp: 0.0,
                    block_temperatures: Vec::new(),
                    sample_temperatures: Vec::new(),
                    cover_temperature: 0.0,
                    filename: String::new(),
                };

                for attr in e.attributes().flatten() {
                    let key = String::from_utf8_lossy(attr.key.as_ref()).to_string();
                    let val =
                        String::from_utf8_lossy(&attr.value).to_string();
                    match key.as_str() {
                        "stage" => {
                            meta.stage = val.parse().unwrap_or(0);
                        }
                        "cycle" => {
                            meta.cycle = val.parse().unwrap_or(0);
                        }
                        "step" => {
                            meta.step = val.parse().unwrap_or(0);
                        }
                        "point" => {
                            meta.point = val.parse().unwrap_or(0);
                        }
                        "emissionColor" => {
                            if let Some(suffix) = val.strip_prefix('m') {
                                meta.emission = suffix.parse().unwrap_or(0);
                            }
                        }
                        "excitationColor" => {
                            if let Some(suffix) = val.strip_prefix('x') {
                                meta.excitation = suffix.parse().unwrap_or(0);
                            }
                        }
                        "exposure" => {
                            meta.exposure_ms = val.parse().unwrap_or(0.0);
                        }
                        "exposureIndex" => {
                            meta.exposure_index = val.parse().unwrap_or(0);
                        }
                        "time" => {
                            meta.timestamp = val.parse().unwrap_or(0.0);
                        }
                        "blockTemperature" => {
                            meta.block_temperatures = val
                                .split(',')
                                .filter_map(|s| s.trim().parse().ok())
                                .collect();
                        }
                        "sampleTemperature" => {
                            meta.sample_temperatures = val
                                .split(',')
                                .filter_map(|s| s.trim().parse().ok())
                                .collect();
                        }
                        "coverTemperature" => {
                            meta.cover_temperature = val.parse().unwrap_or(0.0);
                        }
                        _ => {}
                    }
                }

                // Read the text content (filename)
                if let Ok(Event::Text(t)) = reader.read_event_into(&mut buf) {
                    meta.filename =
                        String::from_utf8_lossy(t.as_ref()).trim().to_string();
                }

                results.push(meta);
            }
            Ok(Event::Eof) => break,
            Err(e) => {
                return Err(QuantError::ParseError(format!(
                    "XML parse error: {}",
                    e
                )));
            }
            _ => {}
        }
        buf.clear();
    }

    Ok(results)
}

// ---------------------------------------------------------------------------
// from_tiffs constructors
// ---------------------------------------------------------------------------

impl QuantDataCollection {
    /// Build from TIFF images + ROI calibration + quant XML metadata in an EDS archive.
    pub fn from_tiffs_in_eds<P: AsRef<std::path::Path>>(path: P) -> Result<Self, QuantError> {
        let file = std::fs::File::open(path.as_ref())?;
        let mut archive = ::zip::ZipArchive::new(file)?;

        let cal_prefix = detect_cal_prefix_generic(&mut archive);
        let quant_prefix = detect_quant_prefix(&mut archive);
        let image_prefix = detect_image_prefix(&mut archive);

        // Read ROI calibration
        let roi_path = format!("{}roi.ini", cal_prefix);
        let roi_text = read_zip_string(&mut archive, &roi_path)
            .map_err(|e| QuantError::ParseError(format!("Failed to read roi.ini: {}", e)))?;
        let roi = crate::calibration::RoiCalibration::parse(&roi_text)
            .map_err(|e| QuantError::ParseError(format!("Failed to parse roi.ini: {}", e)))?;

        // Read quant XML files for metadata
        let mut image_meta: HashMap<String, TiffImageMeta> = HashMap::new();
        let xml_names: Vec<String> = (0..archive.len())
            .filter_map(|i| {
                archive.by_index(i).ok().map(|e| e.name().to_string())
            })
            .filter(|n| n.starts_with(&quant_prefix) && n.ends_with(".xml"))
            .collect();

        for name in &xml_names {
            let data = read_zip_bytes(&mut archive, name)
                .map_err(|e| QuantError::ParseError(format!("Failed to read {}: {}", name, e)))?;
            let metas = parse_quant_xml(&data)?;
            for m in metas {
                image_meta.insert(m.filename.clone(), m);
            }
        }

        // Read and process TIFF files
        let tiff_names: Vec<String> = (0..archive.len())
            .filter_map(|i| {
                archive.by_index(i).ok().map(|e| e.name().to_string())
            })
            .filter(|n| n.starts_with(&image_prefix) && n.ends_with(".tiff"))
            .collect();

        let mut entries = HashMap::new();
        let mut n_rows: Option<u32> = None;
        let mut n_cols: Option<u32> = None;

        for tiff_name in &tiff_names {
            let basename = tiff_name.rsplit('/').next().unwrap_or(tiff_name);
            let qfn = QuantFilename::parse(basename)?;

            let meta = image_meta.get(basename).ok_or_else(|| {
                QuantError::ParseError(format!("No metadata found for TIFF {}", basename))
            })?;

            let tiff_data = read_zip_bytes(&mut archive, tiff_name)
                .map_err(|e| QuantError::ParseError(format!("Failed to read {}: {}", tiff_name, e)))?;

            let wells = crate::tiff::apply_roi_calibration_to_tiff(&tiff_data, &roi, qfn.emission, None)
                .map_err(|e| QuantError::ParseError(format!("TIFF ROI error: {}", e)))?;

            let well_n_rows = roi.n_rows;
            let well_n_cols = roi.n_cols;

            match (n_rows, n_cols) {
                (None, None) => {
                    n_rows = Some(well_n_rows);
                    n_cols = Some(well_n_cols);
                }
                (Some(r), Some(c)) => {
                    if well_n_rows != r || well_n_cols != c {
                        return Err(QuantError::ParseError(format!(
                            "Inconsistent plate geometry: {}x{} vs {}x{}",
                            well_n_rows, well_n_cols, r, c
                        )));
                    }
                }
                _ => unreachable!(),
            }

            // Use the actual excitation/emission from metadata, not the ROI key
            let actual_filter_set = FilterSet::from_string(&format!(
                "x{}-m{}", meta.excitation, meta.emission
            )).map_err(QuantError::ParseError)?;

            let conditions = QuantConditions {
                stage: meta.stage,
                cycle: meta.cycle,
                step: meta.step,
                point: meta.point,
                excitation: format!("x{}", meta.excitation),
                emission: format!("m{}", meta.emission),
                filter_set: actual_filter_set,
                exposure_ms: meta.exposure_ms,
                timestamp: meta.timestamp,
                block_temperatures: meta.block_temperatures.clone(),
                sample_temperatures: meta.sample_temperatures.clone(),
                cover_temperature: meta.cover_temperature,
            };

            let qf = QuantFile {
                conditions,
                wells,
                n_rows: well_n_rows,
                n_cols: well_n_cols,
            };

            let key = CollectionKey {
                stage: meta.stage,
                cycle: meta.cycle,
                step: meta.step,
                point: meta.point,
                filter_set: actual_filter_set,
                exposure_index: qfn.exposure_index,
            };

            entries.insert(key, qf);
        }

        Ok(Self {
            entries,
            n_rows: n_rows.unwrap_or(8),
            n_cols: n_cols.unwrap_or(12),
        })
    }

    /// Build from TIFF images + ROI calibration + quant XML metadata in a directory.
    pub fn from_tiffs_in_directory<P: AsRef<std::path::Path>>(path: P) -> Result<Self, QuantError> {
        let path = path.as_ref();

        // Find calibrations dir
        let cal_dir = if path.join("calibrations").is_dir() {
            path.join("calibrations")
        } else {
            path.to_path_buf()
        };

        // Read ROI calibration
        let roi_path = cal_dir.join("roi.ini");
        let roi_text = std::fs::read_to_string(&roi_path)
            .map_err(|e| QuantError::ParseError(format!("Failed to read roi.ini: {}", e)))?;
        let roi = crate::calibration::RoiCalibration::parse(&roi_text)
            .map_err(|e| QuantError::ParseError(format!("Failed to parse roi.ini: {}", e)))?;

        // Find quant dir for XML metadata
        let quant_dir = if path.join("quant").is_dir() {
            path.join("quant")
        } else {
            path.to_path_buf()
        };

        // Read quant XML files for metadata
        let mut image_meta: HashMap<String, TiffImageMeta> = HashMap::new();
        if quant_dir.is_dir() {
            for entry in std::fs::read_dir(&quant_dir)? {
                let entry = entry?;
                let file_path = entry.path();
                if file_path.extension().and_then(|e| e.to_str()) == Some("xml") {
                    let data = std::fs::read(&file_path)?;
                    if let Ok(metas) = parse_quant_xml(&data) {
                        for m in metas {
                            image_meta.insert(m.filename.clone(), m);
                        }
                    }
                }
            }
        }

        // Find images dir
        let images_dir = if path.join("images").is_dir() {
            path.join("images")
        } else {
            path.to_path_buf()
        };

        let mut entries = HashMap::new();
        let mut n_rows: Option<u32> = None;
        let mut n_cols: Option<u32> = None;

        for entry in std::fs::read_dir(&images_dir)? {
            let entry = entry?;
            let file_path = entry.path();
            if file_path.extension().and_then(|e| e.to_str()) != Some("tiff") {
                continue;
            }

            let basename = file_path
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string();
            let qfn = QuantFilename::parse(&basename)?;

            let meta = image_meta.get(&basename).ok_or_else(|| {
                QuantError::ParseError(format!("No metadata found for TIFF {}", basename))
            })?;

            let tiff_data = std::fs::read(&file_path)?;
            let wells = crate::tiff::apply_roi_calibration_to_tiff(&tiff_data, &roi, qfn.emission, None)
                .map_err(|e| QuantError::ParseError(format!("TIFF ROI error: {}", e)))?;

            let well_n_rows = roi.n_rows;
            let well_n_cols = roi.n_cols;

            match (n_rows, n_cols) {
                (None, None) => {
                    n_rows = Some(well_n_rows);
                    n_cols = Some(well_n_cols);
                }
                (Some(r), Some(c)) => {
                    if well_n_rows != r || well_n_cols != c {
                        return Err(QuantError::ParseError(format!(
                            "Inconsistent plate geometry",
                        )));
                    }
                }
                _ => unreachable!(),
            }

            // Use the actual excitation/emission from metadata, not the ROI key
            let actual_filter_set = FilterSet::from_string(&format!(
                "x{}-m{}", meta.excitation, meta.emission
            )).map_err(QuantError::ParseError)?;

            let conditions = QuantConditions {
                stage: meta.stage,
                cycle: meta.cycle,
                step: meta.step,
                point: meta.point,
                excitation: format!("x{}", meta.excitation),
                emission: format!("m{}", meta.emission),
                filter_set: actual_filter_set,
                exposure_ms: meta.exposure_ms,
                timestamp: meta.timestamp,
                block_temperatures: meta.block_temperatures.clone(),
                sample_temperatures: meta.sample_temperatures.clone(),
                cover_temperature: meta.cover_temperature,
            };

            let qf = QuantFile {
                conditions,
                wells,
                n_rows: well_n_rows,
                n_cols: well_n_cols,
            };

            let key = CollectionKey {
                stage: meta.stage,
                cycle: meta.cycle,
                step: meta.step,
                point: meta.point,
                filter_set: actual_filter_set,
                exposure_index: qfn.exposure_index,
            };

            entries.insert(key, qf);
        }

        Ok(Self {
            entries,
            n_rows: n_rows.unwrap_or(8),
            n_cols: n_cols.unwrap_or(12),
        })
    }
}

// ---------------------------------------------------------------------------
// ZIP helpers
// ---------------------------------------------------------------------------

fn read_zip_string(
    archive: &mut ::zip::ZipArchive<std::fs::File>,
    path: &str,
) -> Result<String, QuantError> {
    let mut entry = archive.by_name(path)?;
    let mut content = String::new();
    std::io::Read::read_to_string(&mut entry, &mut content)?;
    Ok(content)
}

fn read_zip_bytes(
    archive: &mut ::zip::ZipArchive<std::fs::File>,
    path: &str,
) -> Result<Vec<u8>, QuantError> {
    let mut entry = archive.by_name(path)?;
    let mut content = Vec::new();
    std::io::Read::read_to_end(&mut entry, &mut content)?;
    Ok(content)
}

/// Detect calibration prefix in an EDS archive.
fn detect_cal_prefix_generic(archive: &mut ::zip::ZipArchive<std::fs::File>) -> String {
    for i in 0..archive.len() {
        if let Ok(entry) = archive.by_index(i) {
            let name = entry.name().to_string();
            if name.contains("calibrations/") {
                if name.starts_with("apldbio/sds/calibrations/") {
                    return "apldbio/sds/calibrations/".to_string();
                }
                if name.starts_with("calibrations/") {
                    return "calibrations/".to_string();
                }
            }
        }
    }
    "apldbio/sds/calibrations/".to_string()
}

/// Detect image prefix in an EDS archive.
fn detect_image_prefix(archive: &mut ::zip::ZipArchive<std::fs::File>) -> String {
    for i in 0..archive.len() {
        if let Ok(entry) = archive.by_index(i) {
            let name = entry.name().to_string();
            if name.ends_with(".tiff") {
                if name.starts_with("apldbio/sds/images/") {
                    return "apldbio/sds/images/".to_string();
                }
                if name.starts_with("run/images/") {
                    return "run/images/".to_string();
                }
            }
        }
    }
    "apldbio/sds/images/".to_string()
}

/// Detect the quant file prefix within an EDS archive.
fn detect_quant_prefix(archive: &mut ::zip::ZipArchive<std::fs::File>) -> String {
    // Try v1 path first
    for i in 0..archive.len() {
        if let Ok(entry) = archive.by_index(i) {
            let name = entry.name().to_string();
            if name.starts_with("apldbio/sds/quant/") && name.ends_with(".quant") {
                return "apldbio/sds/quant/".to_string();
            }
            if name.starts_with("run/quant/") && name.ends_with(".quant") {
                return "run/quant/".to_string();
            }
        }
    }
    // Default to v1
    "apldbio/sds/quant/".to_string()
}

// ---------------------------------------------------------------------------
// to_polars() for QuantDataCollection
// ---------------------------------------------------------------------------

impl QuantDataCollection {
    /// Convert to a long-format Polars DataFrame.
    pub fn to_polars(&self) -> Result<polars::prelude::DataFrame, polars::prelude::PolarsError> {
        use polars::prelude::*;

        let n_wells = (self.n_rows * self.n_cols) as usize;
        let n_entries = self.entries.len();
        let capacity = n_wells * n_entries;

        let mut well_names = Vec::with_capacity(capacity);
        let mut rows = Vec::with_capacity(capacity);
        let mut columns = Vec::with_capacity(capacity);
        let mut filter_sets = Vec::with_capacity(capacity);
        let mut stages = Vec::with_capacity(capacity);
        let mut cycles = Vec::with_capacity(capacity);
        let mut steps = Vec::with_capacity(capacity);
        let mut points = Vec::with_capacity(capacity);
        let mut exposure_indices = Vec::with_capacity(capacity);
        let mut exposure_ms_vec = Vec::with_capacity(capacity);
        let mut timestamps = Vec::with_capacity(capacity);
        let mut inner_sums = Vec::with_capacity(capacity);
        let mut inner_counts = Vec::with_capacity(capacity);
        let mut inner_saturations = Vec::with_capacity(capacity);
        let mut outer_sums = Vec::with_capacity(capacity);
        let mut outer_counts = Vec::with_capacity(capacity);
        let mut net_totals = Vec::with_capacity(capacity);
        let mut sample_temps = Vec::with_capacity(capacity);
        let mut zones = Vec::with_capacity(capacity);

        let mut sorted_keys: Vec<&CollectionKey> = self.entries.keys().collect();
        sorted_keys.sort();

        for key in sorted_keys {
            let qf = &self.entries[key];
            let cond = &qf.conditions;
            let n_zones = cond.sample_temperatures.len();
            let zone_size = self.n_cols as usize / n_zones.max(1);

            for (well_idx, wq) in qf.wells.iter().enumerate() {
                let row = (well_idx / self.n_cols as usize) as u32;
                let col = (well_idx % self.n_cols as usize) as u32;
                let row_letter = (b'A' + row as u8) as char;
                let col_1based = col + 1;

                well_names.push(format!("{}{}", row_letter, col_1based));
                rows.push(row);
                columns.push(col);
                filter_sets.push(cond.filter_set.lowerform());
                stages.push(cond.stage);
                cycles.push(cond.cycle);
                steps.push(cond.step);
                points.push(cond.point);
                exposure_indices.push(key.exposure_index);
                exposure_ms_vec.push(cond.exposure_ms);
                timestamps.push(cond.timestamp);
                inner_sums.push(wq.inner.sum);
                inner_counts.push(wq.inner.count);
                inner_saturations.push(wq.inner.saturation);
                outer_sums.push(wq.outer.sum);
                outer_counts.push(wq.outer.count);

                let net_total = wq.inner.sum
                    - wq.inner.count as f64 * wq.outer.sum / wq.outer.count as f64;
                net_totals.push(net_total);

                let zone_idx = (col as usize / zone_size).min(n_zones.saturating_sub(1));
                let st = cond.sample_temperatures.get(zone_idx).copied().unwrap_or(0.0);
                sample_temps.push(st);
                zones.push(zone_idx as u32);
            }
        }

        df![
            "well" => well_names,
            "row" => rows,
            "column" => columns,
            "filter_set" => filter_sets,
            "stage" => stages,
            "cycle" => cycles,
            "step" => steps,
            "point" => points,
            "exposure_index" => exposure_indices,
            "exposure_ms" => exposure_ms_vec,
            "timestamp" => timestamps,
            "inner_sum" => inner_sums,
            "inner_count" => inner_counts,
            "inner_saturation" => inner_saturations,
            "outer_sum" => outer_sums,
            "outer_count" => outer_counts,
            "net_total" => net_totals,
            "sample_temperature" => sample_temps,
            "zone" => zones,
        ]
    }
}

// ---------------------------------------------------------------------------
// PyO3 bindings for QuantDataCollection
// ---------------------------------------------------------------------------

#[cfg(feature = "python")]
#[pymethods]
impl QuantDataCollection {
    #[staticmethod]
    #[pyo3(name = "from_eds")]
    fn py_from_eds(path: &str) -> PyResult<Self> {
        Self::from_eds(path)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
    }

    #[staticmethod]
    #[pyo3(name = "from_directory")]
    fn py_from_directory(path: &str) -> PyResult<Self> {
        Self::from_directory(path)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
    }

    #[staticmethod]
    #[pyo3(name = "from_tiffs_in_eds")]
    fn py_from_tiffs_in_eds(path: &str) -> PyResult<Self> {
        Self::from_tiffs_in_eds(path)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
    }

    #[staticmethod]
    #[pyo3(name = "from_tiffs_in_directory")]
    fn py_from_tiffs_in_directory(path: &str) -> PyResult<Self> {
        Self::from_tiffs_in_directory(path)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
    }

    #[pyo3(name = "to_polars")]
    fn py_to_polars(&self) -> PyResult<pyo3_polars::PyDataFrame> {
        let df = self
            .to_polars()
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        Ok(pyo3_polars::PyDataFrame(df))
    }

    #[pyo3(name = "keys")]
    fn py_keys(&self) -> Vec<CollectionKey> {
        self.keys().into_iter().cloned().collect()
    }

    #[pyo3(name = "filter_sets")]
    fn py_filter_sets(&self) -> Vec<FilterSet> {
        self.filter_sets()
    }

    #[pyo3(name = "exposure_indices")]
    fn py_exposure_indices(&self) -> Vec<u32> {
        self.exposure_indices()
    }

    #[pyo3(name = "collection_points")]
    fn py_collection_points(&self) -> Vec<(u32, u32, u32, u32)> {
        self.collection_points()
    }

    #[pyo3(name = "get_exposures")]
    fn py_get_exposures(
        &self,
        stage: u32,
        cycle: u32,
        step: u32,
        point: u32,
        filter_set: &FilterSet,
    ) -> Vec<QuantFile> {
        self.get_exposures(stage, cycle, step, point, *filter_set)
            .into_iter()
            .cloned()
            .collect()
    }

    #[pyo3(name = "get")]
    fn py_get(
        &self,
        stage: u32,
        cycle: u32,
        step: u32,
        point: u32,
        filter_set: &FilterSet,
        exposure_index: u32,
    ) -> Option<QuantFile> {
        self.get_by_parts(stage, cycle, step, point, *filter_set, exposure_index)
            .cloned()
    }

    fn __len__(&self) -> usize {
        self.len()
    }

    fn __repr__(&self) -> String {
        format!(
            "QuantDataCollection(entries={}, filters={}, geometry={}x{})",
            self.len(),
            self.filter_sets().len(),
            self.n_rows,
            self.n_cols
        )
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    fn read_quant_from_test_eds(path: &str) -> Vec<u8> {
        let eds_path =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/test.eds");
        let file = std::fs::File::open(eds_path).expect("test.eds not found");
        let mut archive = ::zip::ZipArchive::new(file).expect("invalid zip");
        let mut entry = archive.by_name(path).expect("file not found in EDS");
        let mut content = Vec::new();
        entry.read_to_end(&mut content).expect("read failed");
        content
    }

    #[test]
    fn test_parse_quant_file() {
        let data = read_quant_from_test_eds("apldbio/sds/quant/S02_C001_T01_P0001_M1_X1_E1.quant");
        let qf = QuantFile::parse(&data).unwrap();

        assert_eq!(qf.conditions.stage, 2);
        assert_eq!(qf.conditions.cycle, 1);
        assert_eq!(qf.conditions.step, 1);
        assert_eq!(qf.conditions.point, 1);
        assert_eq!(qf.conditions.excitation, "x1");
        assert_eq!(qf.conditions.emission, "m1");
        assert_eq!(qf.conditions.filter_set, FilterSet::new(1, 1, true));
        assert!((qf.conditions.exposure_ms - 10.0).abs() < 0.01);
        assert!(qf.conditions.timestamp > 1.5e9); // Unix timestamp > 2017
        assert_eq!(qf.n_rows, 8);
        assert_eq!(qf.n_cols, 12);
        assert_eq!(qf.wells.len(), 96);
    }

    #[test]
    fn test_parse_quant_regions() {
        let data = read_quant_from_test_eds("apldbio/sds/quant/S02_C001_T01_P0001_M1_X1_E1.quant");
        let qf = QuantFile::parse(&data).unwrap();

        // First well (A1)
        let w = &qf.wells[0];
        assert!(w.inner.sum > 0.0);
        assert!(w.inner.count > 900 && w.inner.count < 1100); // ~1016
        assert!(w.outer.count > 200 && w.outer.count < 300); // ~244
        assert_eq!(w.inner.saturation, 0);
    }

    #[test]
    fn test_parse_quant_conditions_temperatures() {
        let data = read_quant_from_test_eds("apldbio/sds/quant/S02_C001_T01_P0001_M1_X1_E1.quant");
        let qf = QuantFile::parse(&data).unwrap();

        assert_eq!(qf.conditions.block_temperatures.len(), 6);
        assert_eq!(qf.conditions.sample_temperatures.len(), 6);
        assert!(qf.conditions.cover_temperature > 100.0); // ~105°C
    }

    #[test]
    fn test_parse_quant_e2_different_exposure() {
        let data = read_quant_from_test_eds("apldbio/sds/quant/S02_C001_T01_P0001_M1_X1_E2.quant");
        let qf = QuantFile::parse(&data).unwrap();

        // E2 has a longer exposure
        assert!((qf.conditions.exposure_ms - 450.0).abs() < 0.01);
        assert_eq!(qf.wells.len(), 96);
    }

    #[test]
    fn test_parse_quant_filename() {
        let qfn = QuantFilename::parse("S02_C001_T01_P0001_M1_X1_E1.quant").unwrap();
        assert_eq!(qfn.stage, 2);
        assert_eq!(qfn.cycle, 1);
        assert_eq!(qfn.step, 1);
        assert_eq!(qfn.point, 1);
        assert_eq!(qfn.emission, 1);
        assert_eq!(qfn.excitation, 1);
        assert_eq!(qfn.exposure_index, 1);
    }

    #[test]
    fn test_parse_quant_filename_with_path() {
        let qfn =
            QuantFilename::parse("apldbio/sds/quant/S02_C003_T01_P0001_M4_X3_E2.quant").unwrap();
        assert_eq!(qfn.stage, 2);
        assert_eq!(qfn.cycle, 3);
        assert_eq!(qfn.emission, 4);
        assert_eq!(qfn.excitation, 3);
        assert_eq!(qfn.exposure_index, 2);
    }

    #[test]
    fn test_parse_quant_filename_invalid() {
        assert!(QuantFilename::parse("invalid.quant").is_err());
        assert!(QuantFilename::parse("not_a_quant.txt").is_err());
    }

    #[test]
    fn test_quant_net_total_calculation() {
        let data = read_quant_from_test_eds("apldbio/sds/quant/S02_C001_T01_P0001_M1_X1_E1.quant");
        let qf = QuantFile::parse(&data).unwrap();

        // Verify net_total for first well (A1)
        let w = &qf.wells[0];
        let net_total =
            w.inner.sum - w.inner.count as f64 * w.outer.sum / w.outer.count as f64;
        // net_total should be positive (signal above background)
        assert!(net_total > 0.0);
    }

    // ===================================================================
    // Collection tests
    // ===================================================================

    fn test_eds_path() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/test.eds")
    }

    #[test]
    fn test_quant_collection_from_eds() {
        let coll = QuantDataCollection::from_eds(test_eds_path()).unwrap();

        // test.eds has 4 cycles × 13 filters × 2 exposures = ~104 quant files
        // Actually: 4 cycles, various filters with 2 exposures each
        assert!(coll.len() > 50);
        assert_eq!(coll.n_rows, 8);
        assert_eq!(coll.n_cols, 12);

        let fs = coll.filter_sets();
        assert!(!fs.is_empty());

        let pts = coll.collection_points();
        assert!(!pts.is_empty());
    }

    #[test]
    fn test_quant_collection_get_exposures() {
        let coll = QuantDataCollection::from_eds(test_eds_path()).unwrap();

        let pts = coll.collection_points();
        let fs = coll.filter_sets();

        // Each (stage, cycle, step, point, filter) should have 2 exposures in test.eds
        let (s, c, t, p) = pts[0];
        let exposures = coll.get_exposures(s, c, t, p, fs[0]);
        assert_eq!(exposures.len(), 2);

        // Exposures should have different exposure_ms values
        assert!(
            (exposures[0].conditions.exposure_ms - exposures[1].conditions.exposure_ms).abs() > 1.0
        );
    }

    #[test]
    fn test_quant_collection_keys_sorted() {
        let coll = QuantDataCollection::from_eds(test_eds_path()).unwrap();
        let keys = coll.keys();

        for i in 1..keys.len() {
            assert!(keys[i] > keys[i - 1], "Keys not sorted at index {}", i);
        }
    }

    #[test]
    fn test_quant_collection_to_polars() {
        let coll = QuantDataCollection::from_eds(test_eds_path()).unwrap();
        let df = coll.to_polars().unwrap();

        // Should have 19 columns
        assert_eq!(df.width(), 19);

        // Row count = n_wells × n_entries
        let expected_rows = 96 * coll.len();
        assert_eq!(df.height(), expected_rows);

        // Check column names exist
        assert!(df.column("well").is_ok());
        assert!(df.column("filter_set").is_ok());
        assert!(df.column("net_total").is_ok());
        assert!(df.column("inner_sum").is_ok());
        assert!(df.column("exposure_ms").is_ok());
    }

    #[test]
    fn test_quant_collection_net_total() {
        let coll = QuantDataCollection::from_eds(test_eds_path()).unwrap();
        let df = coll.to_polars().unwrap();

        // Verify net_total matches manual calculation for a few rows
        let inner_sum = df.column("inner_sum").unwrap().f64().unwrap();
        let inner_count = df.column("inner_count").unwrap().u32().unwrap();
        let outer_sum = df.column("outer_sum").unwrap().f64().unwrap();
        let outer_count = df.column("outer_count").unwrap().u32().unwrap();
        let net_total = df.column("net_total").unwrap().f64().unwrap();

        for i in 0..5.min(df.height()) {
            let is = inner_sum.get(i).unwrap();
            let ic = inner_count.get(i).unwrap() as f64;
            let os = outer_sum.get(i).unwrap();
            let oc = outer_count.get(i).unwrap() as f64;
            let nt = net_total.get(i).unwrap();
            let expected = is - ic * os / oc;
            assert!(
                (nt - expected).abs() < 0.001,
                "net_total mismatch at row {}: got {}, expected {}",
                i,
                nt,
                expected
            );
        }
    }

    // ===================================================================
    // TIFF filename + quant XML + from_tiffs tests
    // ===================================================================

    #[test]
    fn test_parse_tiff_filename() {
        let qfn = QuantFilename::parse("S01_C001_T01_P0001_M4_X1_E1.tiff").unwrap();
        assert_eq!(qfn.stage, 1);
        assert_eq!(qfn.cycle, 1);
        assert_eq!(qfn.emission, 4);
        assert_eq!(qfn.excitation, 1);
        assert_eq!(qfn.exposure_index, 1);
    }

    #[test]
    fn test_parse_quant_xml() {
        let eds_path =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tiff-collection.eds");
        let file = std::fs::File::open(eds_path).expect("tiff-collection.eds not found");
        let mut archive = ::zip::ZipArchive::new(file).expect("invalid zip");
        let mut entry = archive
            .by_name("apldbio/sds/quant/1-1-1-1-m4-x1.xml")
            .expect("file not found");
        let mut data = Vec::new();
        entry.read_to_end(&mut data).expect("read failed");

        let metas = parse_quant_xml(&data).unwrap();
        assert_eq!(metas.len(), 2); // E1 and E2

        let e1 = &metas[0];
        assert_eq!(e1.stage, 1);
        assert_eq!(e1.cycle, 1);
        assert_eq!(e1.step, 1);
        assert_eq!(e1.point, 1);
        assert_eq!(e1.emission, 4);
        assert_eq!(e1.excitation, 1);
        assert_eq!(e1.exposure_index, 1);
        assert!((e1.exposure_ms - 40.0).abs() < 0.01);
        assert!(e1.timestamp > 1.7e9);
        assert_eq!(e1.block_temperatures.len(), 6);
        assert_eq!(e1.sample_temperatures.len(), 6);
        assert!(e1.cover_temperature > 100.0);
        assert!(e1.filename.ends_with(".tiff"));

        let e2 = &metas[1];
        assert_eq!(e2.exposure_index, 2);
        assert!((e2.exposure_ms - 550.0).abs() < 0.01);
    }

    fn tiff_eds_path() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tiff-collection.eds")
    }

    #[test]
    fn test_from_tiffs_in_eds() {
        let coll = QuantDataCollection::from_tiffs_in_eds(tiff_eds_path()).unwrap();

        // tiff-collection.eds has 3 collection points × 1 filter × 2 exposures = 6 entries
        assert_eq!(coll.len(), 6);
        assert_eq!(coll.n_rows, 8);
        assert_eq!(coll.n_cols, 12);

        let fs = coll.filter_sets();
        assert_eq!(fs.len(), 1); // Only x1-m4
        assert_eq!(fs[0], FilterSet::new(1, 4, true));
    }

    #[test]
    fn test_from_tiffs_matches_from_quants() {
        // TIFF-derived quant data should exactly match the .quant file data
        let tiff_coll = QuantDataCollection::from_tiffs_in_eds(tiff_eds_path()).unwrap();
        let quant_coll = QuantDataCollection::from_eds(tiff_eds_path()).unwrap();

        assert_eq!(tiff_coll.len(), quant_coll.len());

        let tiff_keys = tiff_coll.keys();
        let quant_keys = quant_coll.keys();
        assert_eq!(tiff_keys.len(), quant_keys.len());

        for (tk, qk) in tiff_keys.iter().zip(quant_keys.iter()) {
            assert_eq!(tk, qk, "Key mismatch");

            let tqf = tiff_coll.get(tk).unwrap();
            let qqf = quant_coll.get(qk).unwrap();

            for (i, (tw, qw)) in tqf.wells.iter().zip(qqf.wells.iter()).enumerate() {
                assert_eq!(tw.inner.sum, qw.inner.sum, "Key {:?} well {} inner sum", tk, i);
                assert_eq!(tw.inner.count, qw.inner.count, "Key {:?} well {} inner count", tk, i);
                assert_eq!(tw.outer.sum, qw.outer.sum, "Key {:?} well {} outer sum", tk, i);
                assert_eq!(tw.outer.count, qw.outer.count, "Key {:?} well {} outer count", tk, i);
            }
        }
    }

    #[test]
    fn test_from_tiffs_polars_matches() {
        let tiff_coll = QuantDataCollection::from_tiffs_in_eds(tiff_eds_path()).unwrap();
        let quant_coll = QuantDataCollection::from_eds(tiff_eds_path()).unwrap();

        let tiff_df = tiff_coll.to_polars().unwrap();
        let quant_df = quant_coll.to_polars().unwrap();

        assert_eq!(tiff_df.height(), quant_df.height());
        assert_eq!(tiff_df.width(), quant_df.width());

        // Compare inner_sum columns
        let tiff_sums = tiff_df.column("inner_sum").unwrap().f64().unwrap();
        let quant_sums = quant_df.column("inner_sum").unwrap().f64().unwrap();
        for i in 0..tiff_df.height() {
            assert_eq!(
                tiff_sums.get(i).unwrap(),
                quant_sums.get(i).unwrap(),
                "inner_sum mismatch at row {}",
                i
            );
        }
    }
}
