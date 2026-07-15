//! Safe, bounded experiment-package staging.

use std::collections::HashSet;
use std::io::{BufRead, Cursor, Read, Write};
use std::path::{Component, Path, PathBuf};

use axum::body::Bytes;
use qslib_core::protocol::{ProtocolDefinition, ProtocolModel, ProtocolSettings};
use serde::Serialize;
use zip::ZipArchive;

use crate::error::ServerError;
use crate::state::sha256_hex;

const MAX_COMPRESSED: usize = 128 * 1024 * 1024;
const MAX_EXPANDED: u64 = 512 * 1024 * 1024;
const MAX_ENTRIES: usize = 20_000;

#[derive(Debug, Clone, Serialize)]
pub struct StagedPackage {
    pub name: String,
    pub etag: String,
    pub compressed_size: usize,
    pub expanded_size: u64,
    pub entries: usize,
}

pub async fn stage_package(
    experiments_root: PathBuf,
    name: String,
    body: Bytes,
) -> Result<StagedPackage, ServerError> {
    validate_experiment_name(&name)?;
    if body.len() > MAX_COMPRESSED {
        return Err(ServerError::bad_request(
            "experiment package exceeds the 128 MiB compressed limit",
        ));
    }
    tokio::task::spawn_blocking(move || stage_package_blocking(experiments_root, name, body))
        .await
        .map_err(|error| ServerError::internal(format!("package staging task failed: {error}")))?
}

fn stage_package_blocking(
    experiments_root: PathBuf,
    name: String,
    body: Bytes,
) -> Result<StagedPackage, ServerError> {
    std::fs::create_dir_all(&experiments_root).map_err(|error| {
        ServerError::internal(format!("failed to create experiments root: {error}"))
    })?;
    let root = experiments_root.join(".qslib-staging");
    std::fs::create_dir_all(&root).map_err(|error| {
        ServerError::internal(format!("failed to create staging root: {error}"))
    })?;
    let sequence = uuid::Uuid::new_v4();
    let temp = root.join(format!(".{name}.{sequence}.tmp"));
    let final_path = root.join(&name);
    let backup = root.join(format!(".{name}.{sequence}.old"));
    std::fs::create_dir(&temp).map_err(|error| {
        ServerError::internal(format!("failed to create staging directory: {error}"))
    })?;

    let result = (|| {
        let mut archive = ZipArchive::new(Cursor::new(body.as_ref()))
            .map_err(|error| ServerError::bad_request(format!("invalid ZIP package: {error}")))?;
        if archive.len() > MAX_ENTRIES {
            return Err(ServerError::bad_request(
                "experiment package exceeds the 20,000-entry limit",
            ));
        }
        let mut paths = HashSet::with_capacity(archive.len());
        let mut folded_paths = HashSet::with_capacity(archive.len());
        let mut expanded_size = 0u64;
        let mut has_experiment = false;
        let mut has_protocol = false;

        for index in 0..archive.len() {
            let entry = archive
                .by_index(index)
                .map_err(|error| ServerError::bad_request(format!("invalid ZIP entry: {error}")))?;
            let relative = safe_zip_path(entry.name())?;
            let normalized = relative.to_string_lossy().replace('\\', "/");
            if !paths.insert(normalized.clone()) {
                return Err(ServerError::bad_request(format!(
                    "duplicate ZIP path {normalized:?}"
                )));
            }
            if !folded_paths.insert(normalized.to_ascii_lowercase()) {
                return Err(ServerError::bad_request(format!(
                    "case-conflicting ZIP path {normalized:?}"
                )));
            }
            reject_special_entry(entry.unix_mode(), entry.is_dir(), &normalized)?;
            let remaining = MAX_EXPANDED.saturating_sub(expanded_size);
            if entry.size() > remaining {
                return Err(ServerError::bad_request(
                    "experiment package exceeds the 512 MiB expanded limit",
                ));
            }

            let lower = normalized.to_ascii_lowercase();
            has_experiment |= lower == "apldbio/sds/experiment.xml" || lower == "experiment.xml";
            has_protocol |= lower == "apldbio/sds/tcprotocol.xml" || lower == "tcprotocol.xml";
            let destination = temp.join(&relative);
            if entry.is_dir() {
                std::fs::create_dir_all(&destination).map_err(|error| {
                    ServerError::internal(format!("failed to create package directory: {error}"))
                })?;
                continue;
            }
            if let Some(parent) = destination.parent() {
                std::fs::create_dir_all(parent).map_err(|error| {
                    ServerError::internal(format!("failed to create package parent: {error}"))
                })?;
            }
            let mut output = std::fs::File::create(&destination).map_err(|error| {
                ServerError::internal(format!("failed to create staged file: {error}"))
            })?;
            let copied =
                std::io::copy(&mut entry.take(remaining + 1), &mut output).map_err(|error| {
                    ServerError::bad_request(format!("failed to extract package entry: {error}"))
                })?;
            if copied > remaining {
                return Err(ServerError::bad_request(
                    "experiment package exceeds the 512 MiB expanded limit",
                ));
            }
            expanded_size += copied;
        }
        if !has_experiment || !has_protocol {
            return Err(ServerError::bad_request(
                "package must contain experiment.xml and tcprotocol.xml",
            ));
        }

        let experiment_path = find_required_file(&temp, "experiment.xml")?;
        validate_xml_file(&experiment_path, "experiment.xml")?;
        let protocol_path = find_required_file(&temp, "tcprotocol.xml")?;
        validate_xml_file(&protocol_path, "tcprotocol.xml")?;
        let etag = format!("\"sha256{}\"", sha256_hex(&body));
        let mut package_file = std::fs::File::create(temp.join(".qslib-package.zip"))
            .map_err(|error| ServerError::internal(format!("failed to store package: {error}")))?;
        package_file
            .write_all(&body)
            .map_err(|error| ServerError::internal(format!("failed to store package: {error}")))?;
        std::fs::write(temp.join(".qslib-package.etag"), etag.as_bytes()).map_err(|error| {
            ServerError::internal(format!("failed to store package ETag: {error}"))
        })?;

        if final_path.exists() {
            std::fs::rename(&final_path, &backup).map_err(|error| {
                ServerError::internal(format!(
                    "failed to preserve previous staged package: {error}"
                ))
            })?;
        }
        if let Err(error) = std::fs::rename(&temp, &final_path) {
            if backup.exists() {
                let _ = std::fs::rename(&backup, &final_path);
            }
            return Err(ServerError::internal(format!(
                "failed to atomically stage package: {error}"
            )));
        }
        if backup.exists() {
            let _ = std::fs::remove_dir_all(&backup);
        }
        Ok(StagedPackage {
            name,
            etag,
            compressed_size: body.len(),
            expanded_size,
            entries: archive.len(),
        })
    })();

    if result.is_err() {
        let _ = std::fs::remove_dir_all(&temp);
    }
    result
}

pub fn staged_path(experiments_root: &Path, name: &str) -> Result<PathBuf, ServerError> {
    validate_experiment_name(name)?;
    Ok(experiments_root.join(".qslib-staging").join(name))
}

pub fn read_package(experiments_root: &Path, name: &str) -> Result<(Vec<u8>, String), ServerError> {
    let root = staged_path(experiments_root, name)?;
    let bytes = std::fs::read(root.join(".qslib-package.zip"))
        .map_err(|_| ServerError::not_found("staged experiment package not found"))?;
    let etag = std::fs::read_to_string(root.join(".qslib-package.etag"))
        .map_err(|_| ServerError::not_found("staged experiment package ETag not found"))?;
    Ok((bytes, etag))
}

pub fn package_etag(experiments_root: &Path, name: &str) -> Result<String, ServerError> {
    let root = staged_path(experiments_root, name)?;
    std::fs::read_to_string(root.join(".qslib-package.etag"))
        .map(|value| value.trim().to_string())
        .map_err(|_| ServerError::not_found("staged experiment package not found"))
}

pub fn delete_staged_package(
    experiments_root: &Path,
    name: &str,
    expected_etag: &str,
) -> Result<(), ServerError> {
    let root = staged_path(experiments_root, name)?;
    let actual = package_etag(experiments_root, name)?;
    if expected_etag != actual {
        return Err(ServerError::conflict(format!(
            "package ETag mismatch: current value is {actual}"
        )));
    }
    std::fs::remove_dir_all(&root)
        .map_err(|error| ServerError::internal(format!("failed to delete staged package: {error}")))
}

pub fn load_protocol(
    experiments_root: &Path,
    name: &str,
) -> Result<(ProtocolSettings, String), ServerError> {
    let root = staged_path(experiments_root, name)?;
    let qsl_path = find_required_file(&root, "qsl-tcprotocol.xml").ok();
    let exact_scpi = qsl_path
        .and_then(|path| std::fs::read_to_string(path).ok())
        .and_then(|xml| ProtocolModel::scpi_from_qsl_xml(&xml));
    if let Some(scpi) = exact_scpi {
        let definition = ProtocolDefinition::new(scpi.clone()).map_err(|error| {
            ServerError::bad_request(format!("invalid qsl-tcprotocol.xml protocol: {error}"))
        })?;
        let settings = definition.settings().map_err(|error| {
            ServerError::bad_request(format!("invalid qsl-tcprotocol.xml protocol: {error}"))
        })?;
        return Ok((settings, scpi));
    }

    // Vendor XML is only the final fallback when no lossless QSLib definition
    // is available.
    let tc_path = find_required_file(&root, "tcprotocol.xml")?;
    let tc_xml = std::fs::read_to_string(tc_path).map_err(|error| {
        ServerError::bad_request(format!("cannot read fallback tcprotocol.xml: {error}"))
    })?;
    let model = ProtocolModel::from_xml(&tc_xml).map_err(|error| {
        ServerError::bad_request(format!("invalid fallback tcprotocol.xml: {error}"))
    })?;
    let settings = ProtocolSettings {
        name: model.name.clone(),
        sample_volume: model.volume,
        run_mode: model.run_mode.clone(),
    };
    Ok((settings, model.to_scpi()))
}

pub(crate) fn validate_experiment_name(name: &str) -> Result<(), ServerError> {
    if name.is_empty()
        || name.len() > 128
        || name.starts_with('.')
        || name.contains(['/', '\\', '\0'])
        || !name
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || " _-.()".contains(character))
    {
        return Err(ServerError::bad_request("invalid experiment name"));
    }
    Ok(())
}

fn safe_zip_path(name: &str) -> Result<PathBuf, ServerError> {
    if name.is_empty() || name.contains('\\') || name.contains('\0') {
        return Err(ServerError::bad_request(format!(
            "unsafe ZIP path {name:?}"
        )));
    }
    let path = Path::new(name);
    let mut safe = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(value) => safe.push(value),
            Component::CurDir => {}
            _ => {
                return Err(ServerError::bad_request(format!(
                    "unsafe ZIP path {name:?}"
                )))
            }
        }
    }
    if safe.as_os_str().is_empty() {
        return Err(ServerError::bad_request(format!(
            "unsafe ZIP path {name:?}"
        )));
    }
    Ok(safe)
}

fn reject_special_entry(mode: Option<u32>, directory: bool, name: &str) -> Result<(), ServerError> {
    let Some(mode) = mode else {
        return Ok(());
    };
    let kind = mode & 0o170000;
    let allowed = kind == 0 || kind == 0o100000 || (directory && kind == 0o040000);
    if !allowed {
        return Err(ServerError::bad_request(format!(
            "ZIP entry {name:?} is a link, device, or other special file"
        )));
    }
    Ok(())
}

fn find_required_file(root: &Path, name: &str) -> Result<PathBuf, ServerError> {
    for candidate in [root.join("apldbio/sds").join(name), root.join(name)] {
        if candidate.is_file() {
            return Ok(candidate);
        }
    }
    Err(ServerError::bad_request(format!(
        "package is missing {name}"
    )))
}

fn validate_xml_file(path: &Path, display_name: &str) -> Result<(), ServerError> {
    let reader = quick_xml::Reader::from_file(path).map_err(|error| {
        ServerError::bad_request(format!("cannot read {display_name}: {error}"))
    })?;
    validate_xml_reader(reader, display_name)
}

pub(crate) fn validate_xml_document(xml: &str, display_name: &str) -> Result<(), ServerError> {
    validate_xml_reader(quick_xml::Reader::from_str(xml), display_name)
}

fn validate_xml_reader<R: BufRead>(
    mut reader: quick_xml::Reader<R>,
    display_name: &str,
) -> Result<(), ServerError> {
    let mut buffer = Vec::new();
    let mut elements: Vec<Vec<u8>> = Vec::new();
    let mut root_seen = false;
    let mut root_closed = false;
    loop {
        match reader.read_event_into(&mut buffer) {
            Ok(quick_xml::events::Event::Start(event)) => {
                if elements.is_empty() {
                    if root_seen || root_closed {
                        return Err(ServerError::bad_request(format!(
                            "invalid {display_name}: multiple root elements"
                        )));
                    }
                    root_seen = true;
                }
                elements.push(event.name().as_ref().to_vec());
                buffer.clear();
            }
            Ok(quick_xml::events::Event::Empty(_)) => {
                if elements.is_empty() {
                    if root_seen || root_closed {
                        return Err(ServerError::bad_request(format!(
                            "invalid {display_name}: multiple root elements"
                        )));
                    }
                    root_seen = true;
                    root_closed = true;
                }
                buffer.clear();
            }
            Ok(quick_xml::events::Event::End(event)) => {
                let Some(expected) = elements.pop() else {
                    return Err(ServerError::bad_request(format!(
                        "invalid {display_name}: unexpected closing element"
                    )));
                };
                if expected != event.name().as_ref() {
                    return Err(ServerError::bad_request(format!(
                        "invalid {display_name}: mismatched closing element"
                    )));
                }
                if elements.is_empty() {
                    root_closed = true;
                }
                buffer.clear();
            }
            Ok(quick_xml::events::Event::Text(event)) if elements.is_empty() => {
                let bytes: &[u8] = event.as_ref();
                if bytes.iter().any(|byte| !byte.is_ascii_whitespace()) {
                    return Err(ServerError::bad_request(format!(
                        "invalid {display_name}: text outside the root element"
                    )));
                }
                buffer.clear();
            }
            Ok(quick_xml::events::Event::CData(event)) if elements.is_empty() => {
                let bytes: &[u8] = event.as_ref();
                if bytes.iter().any(|byte| !byte.is_ascii_whitespace()) {
                    return Err(ServerError::bad_request(format!(
                        "invalid {display_name}: data outside the root element"
                    )));
                }
                buffer.clear();
            }
            Ok(quick_xml::events::Event::DocType(_)) => {
                return Err(ServerError::bad_request(format!(
                    "invalid {display_name}: document types are not allowed"
                )));
            }
            Ok(quick_xml::events::Event::Eof)
                if root_seen && root_closed && elements.is_empty() =>
            {
                return Ok(());
            }
            Ok(quick_xml::events::Event::Eof) => {
                return Err(ServerError::bad_request(format!(
                    "invalid {display_name}: incomplete XML document"
                )));
            }
            Ok(_) => buffer.clear(),
            Err(error) => {
                return Err(ServerError::bad_request(format!(
                    "invalid {display_name}: {error}"
                )))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PROTOCOL: &str = r#"<TCProtocol><ProtocolName>demo</ProtocolName><SampleVolume>20</SampleVolume><RunMode>Standard</RunMode><TCStage><NumOfRepetitions>1</NumOfRepetitions><TCStep><CollectionFlag>0</CollectionFlag><Temperature>60</Temperature><HoldTime>10</HoldTime></TCStep></TCStage></TCProtocol>"#;

    fn archive(entries: &[(&str, &str)]) -> Bytes {
        let cursor = Cursor::new(Vec::new());
        let mut writer = zip::ZipWriter::new(cursor);
        for (name, contents) in entries {
            writer
                .start_file(*name, zip::write::SimpleFileOptions::default())
                .unwrap();
            writer.write_all(contents.as_bytes()).unwrap();
        }
        Bytes::from(writer.finish().unwrap().into_inner())
    }

    #[test]
    fn traversal_and_windows_paths_are_rejected() {
        assert!(safe_zip_path("../escape").is_err());
        assert!(safe_zip_path("a/../../escape").is_err());
        assert!(safe_zip_path("/absolute").is_err());
        assert!(safe_zip_path("a\\b").is_err());
    }

    #[test]
    fn special_unix_types_are_rejected() {
        assert!(reject_special_entry(Some(0o120777), false, "link").is_err());
        assert!(reject_special_entry(Some(0o060600), false, "device").is_err());
        assert!(reject_special_entry(Some(0o100644), false, "file").is_ok());
    }

    #[test]
    fn valid_package_is_staged_atomically() {
        let root = tempfile::tempdir().unwrap();
        let bytes = archive(&[
            ("apldbio/sds/experiment.xml", "<Experiment/>"),
            ("apldbio/sds/tcprotocol.xml", PROTOCOL),
        ]);
        let staged =
            stage_package_blocking(root.path().to_path_buf(), "demo".into(), bytes).unwrap();
        assert_eq!(staged.entries, 2);
        assert!(root
            .path()
            .join(".qslib-staging/demo/apldbio/sds/tcprotocol.xml")
            .is_file());
    }

    #[test]
    fn lossless_qsl_protocol_precedes_approximate_display_xml() {
        let root = tempfile::tempdir().unwrap();
        let display = r#"<TCProtocol><ProtocolName>wrong_display</ProtocolName><CollectionProfile><CollectionCondition><FilterSet Emission="mm4" Excitation="xquant"/></CollectionCondition></CollectionProfile></TCProtocol>"#;
        let qsl = r#"<QSTCProtocol><QSLibProtocolCommand>PROT -volume=12 -runmode=fast exact_qsl &lt;multiline.protocol&gt;
	STAGE 1 S &lt;multiline.stage&gt;
		STEP 1 &lt;multiline.step&gt;
			RAMP 25
			HOLD 60
		&lt;/multiline.step&gt;
	&lt;/multiline.stage&gt;
&lt;/multiline.protocol&gt;</QSLibProtocolCommand></QSTCProtocol>"#;
        let bytes = archive(&[
            ("apldbio/sds/experiment.xml", "<Experiment/>"),
            ("apldbio/sds/tcprotocol.xml", display),
            ("apldbio/sds/qsl-tcprotocol.xml", qsl),
        ]);
        stage_package_blocking(root.path().to_path_buf(), "demo".into(), bytes).unwrap();

        let (settings, scpi) = load_protocol(root.path(), "demo").unwrap();

        assert_eq!(settings.name, "exact_qsl");
        assert_eq!(settings.sample_volume, 12.0);
        assert_eq!(settings.run_mode, "fast");
        assert!(scpi.contains("exact_qsl"));
        assert!(!scpi.contains("wrong_display"));
    }

    #[test]
    fn duplicate_case_and_malformed_required_xml_are_rejected() {
        let root = tempfile::tempdir().unwrap();
        let case_conflict = archive(&[
            ("experiment.xml", "<Experiment/>"),
            ("Experiment.xml", "<Experiment/>"),
            ("tcprotocol.xml", PROTOCOL),
        ]);
        assert!(
            stage_package_blocking(root.path().to_path_buf(), "case".into(), case_conflict)
                .is_err()
        );

        let malformed = archive(&[
            ("experiment.xml", "<Experiment>"),
            ("tcprotocol.xml", PROTOCOL),
        ]);
        assert!(
            stage_package_blocking(root.path().to_path_buf(), "malformed".into(), malformed)
                .is_err()
        );
    }
}
