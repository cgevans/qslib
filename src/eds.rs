//! EDS Archive handling: ZIP-based experiment document files.
//!
//! Handles reading, parsing metadata, and writing EDS files (both v1 and v2 spec versions).

use std::collections::HashMap;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

#[cfg(feature = "python")]
use pyo3::prelude::*;

use crate::plate_setup::PlateSetup;

/// Parsed experiment metadata from an EDS archive.
#[derive(Debug)]
#[cfg_attr(feature = "python", pyclass(module = "qslib._qslib"))]
pub struct EdsArchive {
    /// Base directory (temp dir root)
    base_dir: PathBuf,
    /// Keep the temp dir alive
    _tmp_dir: Option<tempfile::TempDir>,

    // Metadata (parsed eagerly)
    pub name: String,
    pub operator: Option<String>,
    pub plate_type: Option<u32>, // 96 or 384
    pub plate_type_id: Option<String>,
    pub spec_major_version: u32,
    pub spec_version: String,
    pub created_time_ms: Option<f64>,
    pub run_start_time_ms: Option<f64>,
    pub run_end_time_ms: Option<f64>,
    pub run_state: String,
    pub write_software: Option<String>,

    /// Manifest key-value pairs
    pub manifest: HashMap<String, String>,
}

impl EdsArchive {
    /// Open an EDS archive from a file path.
    pub fn from_path(path: &Path) -> Result<Self, EdsError> {
        let file = std::fs::File::open(path)
            .map_err(|e| EdsError::Io(format!("Failed to open {}: {}", path.display(), e)))?;
        let mut archive = zip::ZipArchive::new(file)
            .map_err(|e| EdsError::Zip(format!("Failed to read ZIP: {}", e)))?;

        let tmp_dir = tempfile::TempDir::new()
            .map_err(|e| EdsError::Io(format!("Failed to create temp dir: {}", e)))?;
        let base_dir = tmp_dir.path().to_path_buf();

        Self::extract_zip(&mut archive, &base_dir)?;

        let manifest = Self::parse_manifest(&base_dir)?;
        let spec_major_version = manifest
            .get("Specification-Version")
            .and_then(|v| v.chars().next())
            .and_then(|c| c.to_digit(10))
            .unwrap_or(1);
        let spec_version = manifest
            .get("Specification-Version")
            .cloned()
            .unwrap_or_else(|| "1.0.0".to_string());

        let mut eds = EdsArchive {
            base_dir,
            _tmp_dir: Some(tmp_dir),
            name: String::new(),
            operator: None,
            plate_type: None,
            plate_type_id: None,
            spec_major_version,
            spec_version,
            created_time_ms: None,
            run_start_time_ms: None,
            run_end_time_ms: None,
            run_state: "UNKNOWN".to_string(),
            write_software: None,
            manifest,
        };

        eds.parse_metadata()?;
        Ok(eds)
    }

    /// Open an EDS archive from bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self, EdsError> {
        let cursor = std::io::Cursor::new(data);
        let mut archive = zip::ZipArchive::new(cursor)
            .map_err(|e| EdsError::Zip(format!("Failed to read ZIP from bytes: {}", e)))?;

        let tmp_dir = tempfile::TempDir::new()
            .map_err(|e| EdsError::Io(format!("Failed to create temp dir: {}", e)))?;
        let base_dir = tmp_dir.path().to_path_buf();

        Self::extract_zip(&mut archive, &base_dir)?;

        let manifest = Self::parse_manifest(&base_dir)?;
        let spec_major_version = manifest
            .get("Specification-Version")
            .and_then(|v| v.chars().next())
            .and_then(|c| c.to_digit(10))
            .unwrap_or(1);
        let spec_version = manifest
            .get("Specification-Version")
            .cloned()
            .unwrap_or_else(|| "1.0.0".to_string());

        let mut eds = EdsArchive {
            base_dir,
            _tmp_dir: Some(tmp_dir),
            name: String::new(),
            operator: None,
            plate_type: None,
            plate_type_id: None,
            spec_major_version,
            spec_version,
            created_time_ms: None,
            run_start_time_ms: None,
            run_end_time_ms: None,
            run_state: "UNKNOWN".to_string(),
            write_software: None,
            manifest,
        };

        eds.parse_metadata()?;
        Ok(eds)
    }

    /// Extract ZIP safely to a directory (no path traversal).
    fn extract_zip<R: Read + std::io::Seek>(
        archive: &mut zip::ZipArchive<R>,
        dest: &Path,
    ) -> Result<(), EdsError> {
        for i in 0..archive.len() {
            let mut file = archive
                .by_index(i)
                .map_err(|e| EdsError::Zip(format!("Failed to read ZIP entry: {}", e)))?;

            let name = file.name().to_string();
            // Security: reject paths with .. or absolute paths
            if name.contains("..") || name.starts_with('/') || name.starts_with('\\') {
                continue;
            }

            let out_path = dest.join(&name);
            if file.is_dir() {
                std::fs::create_dir_all(&out_path)
                    .map_err(|e| EdsError::Io(format!("mkdir {}: {}", out_path.display(), e)))?;
            } else {
                if let Some(parent) = out_path.parent() {
                    std::fs::create_dir_all(parent)
                        .map_err(|e| EdsError::Io(format!("mkdir {}: {}", parent.display(), e)))?;
                }
                let mut out = std::fs::File::create(&out_path)
                    .map_err(|e| EdsError::Io(format!("create {}: {}", out_path.display(), e)))?;
                std::io::copy(&mut file, &mut out)
                    .map_err(|e| EdsError::Io(format!("write {}: {}", out_path.display(), e)))?;
            }
        }
        Ok(())
    }

    /// Parse manifest file from the extracted directory.
    fn parse_manifest(base_dir: &Path) -> Result<HashMap<String, String>, EdsError> {
        // Try v1 location first, then v2
        let paths = [
            base_dir.join("apldbio/sds/Manifest.mf"),
            base_dir.join("Manifest.mf"),
        ];
        let manifest_path = paths
            .iter()
            .find(|p| p.exists())
            .ok_or_else(|| EdsError::Format("No manifest file found".into()))?;

        let content = std::fs::read_to_string(manifest_path)
            .map_err(|e| EdsError::Io(format!("read manifest: {}", e)))?;

        let mut props = HashMap::new();
        for line in content.lines() {
            if line.len() > 2 {
                if let Some((key, value)) = line.split_once(": ") {
                    props.insert(key.to_string(), value.to_string());
                }
            }
        }
        Ok(props)
    }

    /// Parse metadata based on spec version.
    fn parse_metadata(&mut self) -> Result<(), EdsError> {
        if self.spec_major_version == 1 {
            self.parse_experiment_xml()?;
        } else if self.spec_major_version == 2 {
            self.parse_summary_json()?;
            // Also update write_software from manifest
            let title = self.manifest.get("Implementation-Title").cloned().unwrap_or_default();
            let version = self.manifest.get("Implementation-Version").cloned().unwrap_or_default();
            if !title.is_empty() {
                self.write_software = Some(format!("{} {}", title, version));
            }
        }
        Ok(())
    }

    /// Parse experiment.xml (v1 format).
    fn parse_experiment_xml(&mut self) -> Result<(), EdsError> {
        let xml_path = self.eds_dir().join("experiment.xml");
        if !xml_path.exists() {
            return Ok(());
        }
        let content = std::fs::read_to_string(&xml_path)
            .map_err(|e| EdsError::Io(format!("read experiment.xml: {}", e)))?;

        use quick_xml::events::Event;
        use quick_xml::reader::Reader;

        let mut reader = Reader::from_str(&content);
        let mut current_tag = String::new();
        let mut depth: u32 = 0;
        let mut tag_depth: u32 = 0;
        let mut in_run_info = false;
        let mut in_software_version = false;

        loop {
            match reader.read_event() {
                Ok(Event::Eof) => break,
                Ok(Event::Start(ref e)) => {
                    depth += 1;
                    let tag = String::from_utf8_lossy(e.name().as_ref()).to_string();
                    current_tag = tag.clone();
                    tag_depth = depth;

                    if tag == "ExperimentProperty" {
                        for attr in e.attributes().flatten() {
                            if attr.key.as_ref() == b"type"
                                && attr.value.as_ref() == b"RunInfo"
                            {
                                in_run_info = true;
                            }
                        }
                    }
                    if in_run_info && tag == "PropertyValue" {
                        for attr in e.attributes().flatten() {
                            if attr.key.as_ref() == b"key"
                                && attr.value.as_ref() == b"softwareVersion"
                            {
                                in_software_version = true;
                            }
                        }
                    }
                }
                Ok(Event::End(ref e)) => {
                    let tag = String::from_utf8_lossy(e.name().as_ref()).to_string();
                    if tag == "ExperimentProperty" {
                        in_run_info = false;
                    }
                    if tag == "PropertyValue" {
                        in_software_version = false;
                    }
                    depth -= 1;
                }
                Ok(Event::Text(ref e)) => {
                    let text = std::str::from_utf8(e.as_ref()).unwrap_or_default().trim().to_string();
                    if text.is_empty() {
                        continue;
                    }
                    // Only match direct children of root (depth == 2: root=1, child=2)
                    match current_tag.as_str() {
                        "Name" if tag_depth == 2 && !in_run_info => self.name = text,
                        "Operator" if tag_depth == 2 => self.operator = Some(text),
                        "CreatedTime" if tag_depth == 2 => {
                            self.created_time_ms = text.parse().ok();
                        }
                        "RunState" if tag_depth == 2 => self.run_state = text,
                        "PlateTypeID" if tag_depth == 2 => {
                            self.plate_type_id = Some(text.clone());
                            self.plate_type = match text.as_str() {
                                "TYPE_8X12" => Some(96),
                                "TYPE_16X24" => Some(384),
                                _ => None,
                            };
                        }
                        "RunStartTime" if tag_depth == 2 => {
                            self.run_start_time_ms = text.parse().ok();
                        }
                        "RunEndTime" if tag_depth == 2 => {
                            self.run_end_time_ms = text.parse().ok();
                        }
                        "String" if in_software_version => {
                            self.write_software = Some(text);
                        }
                        _ => {}
                    }
                }
                Err(e) => {
                    return Err(EdsError::Xml(format!("experiment.xml parse error: {}", e)));
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// Parse summary.json (v2 format).
    fn parse_summary_json(&mut self) -> Result<(), EdsError> {
        let json_path = self.base_dir.join("summary.json");
        if !json_path.exists() {
            return Ok(());
        }
        let content = std::fs::read_to_string(&json_path)
            .map_err(|e| EdsError::Io(format!("read summary.json: {}", e)))?;

        let summary: serde_json::Value = serde_json::from_str(&content)
            .map_err(|e| EdsError::Format(format!("summary.json parse error: {}", e)))?;

        self.name = summary["name"]
            .as_str()
            .unwrap_or("unknown")
            .to_string();
        self.run_state = summary["runStatus"]
            .as_str()
            .unwrap_or("UNKNOWN")
            .to_string();
        if let Some(ct) = summary["createdTime"].as_f64() {
            self.created_time_ms = Some(ct);
        }

        let block_type = summary["blockType"].as_str().unwrap_or("");
        self.plate_type_id = Some(block_type.to_string());
        self.plate_type = match block_type {
            "BLOCK_96W" => Some(96),
            "BLOCK_384W" => Some(384),
            _ => None,
        };

        Ok(())
    }

    /// Get the EDS subdirectory (apldbio/sds).
    pub fn eds_dir(&self) -> PathBuf {
        self.base_dir.join("apldbio").join("sds")
    }

    /// Get the base directory path.
    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    /// Read a file from the archive as bytes.
    pub fn read_file(&self, relative_path: &str) -> Result<Vec<u8>, EdsError> {
        let path = self.base_dir.join(relative_path);
        std::fs::read(&path)
            .map_err(|e| EdsError::Io(format!("read {}: {}", path.display(), e)))
    }

    /// Read a file from the EDS subdirectory as a string.
    pub fn read_eds_file(&self, filename: &str) -> Result<String, EdsError> {
        let path = self.eds_dir().join(filename);
        std::fs::read_to_string(&path)
            .map_err(|e| EdsError::Io(format!("read {}: {}", path.display(), e)))
    }

    /// Check if a file exists in the archive.
    pub fn file_exists(&self, relative_path: &str) -> bool {
        self.base_dir.join(relative_path).exists()
    }

    /// Parse the plate setup XML.
    pub fn plate_setup(&self) -> Result<PlateSetup, EdsError> {
        let xml = self.read_eds_file("plate_setup.xml")?;
        PlateSetup::from_xml(&xml).map_err(|e| EdsError::Xml(format!("plate_setup.xml: {}", e)))
    }

    /// Get the messages.log bytes (location depends on spec version).
    pub fn log_bytes(&self) -> Result<Vec<u8>, EdsError> {
        if self.spec_major_version == 2 {
            self.read_file("run/messages.log")
        } else {
            self.read_file("apldbio/sds/messages.log")
        }
    }

    /// Check if messages.log exists.
    pub fn has_log(&self) -> bool {
        if self.spec_major_version == 2 {
            self.file_exists("run/messages.log")
        } else {
            self.file_exists("apldbio/sds/messages.log")
        }
    }

    /// Save the archive to a ZIP file.
    pub fn save(&self, path: &Path) -> Result<(), EdsError> {
        let file = std::fs::File::create(path)
            .map_err(|e| EdsError::Io(format!("create {}: {}", path.display(), e)))?;
        let mut zip = zip::ZipWriter::new(file);
        let options = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Deflated);

        for entry in walkdir(self.base_dir())? {
            let rel_path = entry
                .strip_prefix(&self.base_dir)
                .unwrap()
                .to_string_lossy()
                .replace('\\', "/");

            if entry.is_dir() {
                zip.add_directory(&rel_path, options)
                    .map_err(|e| EdsError::Zip(format!("add dir {}: {}", rel_path, e)))?;
            } else {
                zip.start_file(&rel_path, options)
                    .map_err(|e| EdsError::Zip(format!("start file {}: {}", rel_path, e)))?;
                let data = std::fs::read(&entry)
                    .map_err(|e| EdsError::Io(format!("read {}: {}", entry.display(), e)))?;
                zip.write_all(&data)
                    .map_err(|e| EdsError::Io(format!("write {}: {}", rel_path, e)))?;
            }
        }
        zip.finish()
            .map_err(|e| EdsError::Zip(format!("finish ZIP: {}", e)))?;
        Ok(())
    }

    /// Write a file to the EDS subdirectory.
    pub fn write_eds_file(&self, filename: &str, content: &str) -> Result<(), EdsError> {
        let path = self.eds_dir().join(filename);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| EdsError::Io(format!("mkdir: {}", e)))?;
        }
        std::fs::write(&path, content)
            .map_err(|e| EdsError::Io(format!("write {}: {}", path.display(), e)))
    }

    /// Write a file to the base directory.
    pub fn write_file(&self, relative_path: &str, data: &[u8]) -> Result<(), EdsError> {
        let path = self.base_dir.join(relative_path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| EdsError::Io(format!("mkdir: {}", e)))?;
        }
        std::fs::write(&path, data)
            .map_err(|e| EdsError::Io(format!("write {}: {}", path.display(), e)))
    }
}

/// Walk a directory recursively, returning file/dir paths.
fn walkdir(dir: &Path) -> Result<Vec<PathBuf>, EdsError> {
    let mut entries = Vec::new();
    fn walk_inner(dir: &Path, entries: &mut Vec<PathBuf>) -> Result<(), EdsError> {
        for entry in std::fs::read_dir(dir)
            .map_err(|e| EdsError::Io(format!("readdir {}: {}", dir.display(), e)))?
        {
            let entry =
                entry.map_err(|e| EdsError::Io(format!("readdir entry: {}", e)))?;
            let path = entry.path();
            entries.push(path.clone());
            if path.is_dir() {
                walk_inner(&path, entries)?;
            }
        }
        Ok(())
    }
    walk_inner(dir, &mut entries)?;
    Ok(entries)
}

#[derive(Debug, thiserror::Error)]
pub enum EdsError {
    #[error("IO error: {0}")]
    Io(String),
    #[error("ZIP error: {0}")]
    Zip(String),
    #[error("XML error: {0}")]
    Xml(String),
    #[error("Format error: {0}")]
    Format(String),
}

#[cfg(feature = "python")]
impl From<EdsError> for PyErr {
    fn from(e: EdsError) -> PyErr {
        pyo3::exceptions::PyValueError::new_err(format!("{}", e))
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl EdsArchive {
    /// Load an EDS archive from a file path.
    #[staticmethod]
    #[pyo3(name = "from_path")]
    fn py_from_path(path: &str) -> PyResult<Self> {
        Self::from_path(Path::new(path)).map_err(|e| e.into())
    }

    /// Load an EDS archive from bytes.
    #[staticmethod]
    #[pyo3(name = "from_bytes")]
    fn py_from_bytes(data: &[u8]) -> PyResult<Self> {
        Self::from_bytes(data).map_err(|e| e.into())
    }

    #[getter]
    fn get_name(&self) -> &str {
        &self.name
    }

    #[getter]
    fn get_operator(&self) -> Option<&str> {
        self.operator.as_deref()
    }

    #[getter]
    fn get_plate_type(&self) -> Option<u32> {
        self.plate_type
    }

    #[getter]
    fn get_plate_type_id(&self) -> Option<&str> {
        self.plate_type_id.as_deref()
    }

    #[getter]
    fn get_spec_major_version(&self) -> u32 {
        self.spec_major_version
    }

    #[getter]
    fn get_spec_version(&self) -> &str {
        &self.spec_version
    }

    #[getter]
    fn get_run_state(&self) -> &str {
        &self.run_state
    }

    #[getter]
    fn get_write_software(&self) -> Option<&str> {
        self.write_software.as_deref()
    }

    /// Created time in milliseconds since epoch (or None).
    #[getter]
    fn get_created_time_ms(&self) -> Option<f64> {
        self.created_time_ms
    }

    /// Run start time in milliseconds since epoch (or None).
    #[getter]
    fn get_run_start_time_ms(&self) -> Option<f64> {
        self.run_start_time_ms
    }

    /// Run end time in milliseconds since epoch (or None).
    #[getter]
    fn get_run_end_time_ms(&self) -> Option<f64> {
        self.run_end_time_ms
    }

    /// Get the manifest as a dict.
    #[getter]
    fn get_manifest(&self) -> HashMap<String, String> {
        self.manifest.clone()
    }

    /// Get the base directory path.
    #[getter]
    fn get_base_dir(&self) -> String {
        self.base_dir.to_string_lossy().to_string()
    }

    /// Get the EDS subdirectory path.
    #[getter]
    fn get_eds_dir(&self) -> String {
        self.eds_dir().to_string_lossy().to_string()
    }

    /// Read a file from the archive as bytes.
    #[pyo3(name = "read_file")]
    fn py_read_file(&self, relative_path: &str) -> PyResult<Vec<u8>> {
        self.read_file(relative_path).map_err(|e| e.into())
    }

    /// Read a file from the EDS subdirectory as a string.
    #[pyo3(name = "read_eds_file")]
    fn py_read_eds_file(&self, filename: &str) -> PyResult<String> {
        self.read_eds_file(filename).map_err(|e| e.into())
    }

    /// Parse the plate setup XML.
    #[pyo3(name = "plate_setup")]
    fn py_plate_setup(&self) -> PyResult<PlateSetup> {
        self.plate_setup().map_err(|e| e.into())
    }

    /// Get the messages.log bytes.
    #[pyo3(name = "log_bytes")]
    fn py_log_bytes(&self) -> PyResult<Vec<u8>> {
        self.log_bytes().map_err(|e| e.into())
    }

    /// Check if messages.log exists.
    #[pyo3(name = "has_log")]
    fn py_has_log(&self) -> bool {
        self.has_log()
    }

    /// Check if a file exists.
    #[pyo3(name = "file_exists")]
    fn py_file_exists(&self, relative_path: &str) -> bool {
        self.file_exists(relative_path)
    }

    /// Save the archive to a ZIP file.
    #[pyo3(name = "save")]
    fn py_save(&self, path: &str) -> PyResult<()> {
        self.save(Path::new(path)).map_err(|e| e.into())
    }

    /// Write a file to the EDS subdirectory.
    #[pyo3(name = "write_eds_file")]
    fn py_write_eds_file(&self, filename: &str, content: &str) -> PyResult<()> {
        self.write_eds_file(filename, content).map_err(|e| e.into())
    }

    /// Write a file to the base directory.
    #[pyo3(name = "write_file")]
    fn py_write_file(&self, relative_path: &str, data: &[u8]) -> PyResult<()> {
        self.write_file(relative_path, data).map_err(|e| e.into())
    }

    fn __repr__(&self) -> String {
        format!(
            "EdsArchive(name='{}', spec_version='{}', run_state='{}')",
            self.name, self.spec_version, self.run_state
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_manifest() {
        let tmp = tempfile::TempDir::new().unwrap();
        let manifest_dir = tmp.path().join("apldbio/sds");
        std::fs::create_dir_all(&manifest_dir).unwrap();
        std::fs::write(
            manifest_dir.join("Manifest.mf"),
            "Specification-Title: Experiment Document Specification\nSpecification-Version: 1.3.0\n",
        )
        .unwrap();

        let manifest = EdsArchive::parse_manifest(tmp.path()).unwrap();
        assert_eq!(
            manifest["Specification-Title"],
            "Experiment Document Specification"
        );
        assert_eq!(manifest["Specification-Version"], "1.3.0");
    }

    #[test]
    fn test_eds_from_path() {
        let test_path = Path::new("tests/test.eds");
        if test_path.exists() {
            let archive = EdsArchive::from_path(test_path);
            assert!(
                archive.is_ok(),
                "Failed to load test.eds: {:?}",
                archive.err()
            );
            let archive = archive.unwrap();
            assert!(!archive.name.is_empty(), "Name should not be empty");
        }
    }

    #[test]
    fn test_eds_from_bytes() {
        let test_path = Path::new("tests/test.eds");
        if test_path.exists() {
            let data = std::fs::read(test_path).unwrap();
            let archive = EdsArchive::from_bytes(&data);
            assert!(archive.is_ok(), "Failed to load from bytes: {:?}", archive.err());
            let archive = archive.unwrap();
            assert!(!archive.name.is_empty());
        }
    }

    #[test]
    fn test_eds_v2() {
        let test_path = Path::new("tests/v2_test.eds");
        if test_path.exists() {
            let archive = EdsArchive::from_path(test_path);
            assert!(archive.is_ok(), "Failed to load v2: {:?}", archive.err());
            let archive = archive.unwrap();
            assert!(archive.spec_major_version == 2);
        }
    }
}
