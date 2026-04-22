//! Experiment XML handling with full round-trip fidelity.
//!
//! Handles creation, parsing, and updating of experiment.xml files.
//! Uses event-copy-modify pattern to preserve unknown elements from
//! AB's Design & Analysis software.

use crate::eds::EdsError;
use quick_xml::events::{BytesEnd, BytesStart, BytesText, Event};
use quick_xml::reader::Reader;
use quick_xml::Writer;
use std::collections::HashSet;
use std::io::Cursor;

/// Parsed experiment metadata.
#[derive(Debug, Clone)]
pub struct ExperimentMeta {
    pub name: String,
    pub operator: Option<String>,
    pub created_time_ms: Option<f64>,
    pub run_start_time_ms: Option<f64>,
    pub run_end_time_ms: Option<f64>,
    pub run_state: String,
    pub plate_type_id: Option<String>,
    pub write_software: Option<String>,
}

/// Create a minimal experiment.xml template for a new experiment.
pub fn new_experiment_xml(plate_type: u32) -> String {
    let plate_type_id = match plate_type {
        384 => "TYPE_16X24",
        _ => "TYPE_8X12",
    };

    let mut writer = Writer::new_with_indent(Cursor::new(Vec::new()), b' ', 2);
    writer
        .create_element("Experiment")
        .write_inner_content(|w| {
            w.create_element("Label")
                .write_text_content(BytesText::new("ruo"))
                .unwrap();
            w.create_element("Type")
                .write_inner_content(|w2| {
                    w2.create_element("Id")
                        .write_text_content(BytesText::new("Custom"))
                        .unwrap();
                    w2.create_element("Name")
                        .write_text_content(BytesText::new("Custom"))
                        .unwrap();
                    w2.create_element("Description")
                        .write_text_content(BytesText::new("Custom QSLib experiment"))
                        .unwrap();
                    w2.create_element("ResultPersisterName")
                        .write_text_content(BytesText::new("scAnalysisResultPersister"))
                        .unwrap();
                    w2.create_element("ContributedResultPersisterName")
                        .write_text_content(BytesText::new("mcAnalysisResultPersister"))
                        .unwrap();
                    Ok(())
                })
                .unwrap();
            w.create_element("ChemistryType")
                .write_text_content(BytesText::new("Other"))
                .unwrap();
            w.create_element("TCProtocolMode")
                .write_text_content(BytesText::new("Standard"))
                .unwrap();
            w.create_element("DNATemplateType")
                .write_text_content(BytesText::new("WET_DNA"))
                .unwrap();
            w.create_element("InstrumentTypeId")
                .write_text_content(BytesText::new("appletini"))
                .unwrap();
            w.create_element("BlockTypeID")
                .write_text_content(BytesText::new("18"))
                .unwrap();
            w.create_element("PlateTypeID")
                .write_text_content(BytesText::new(plate_type_id))
                .unwrap();
            Ok(())
        })
        .unwrap();
    String::from_utf8(writer.into_inner().into_inner()).unwrap()
}

/// Create a plate_setup.xml template.
pub fn new_plate_setup_xml(plate_type: u32) -> String {
    let (rows, cols, plate_type_id, plate_kind_name) = match plate_type {
        384 => (16, 24, "TYPE_16X24", "384-Well Plate (16x24)"),
        _ => (8, 12, "TYPE_8X12", "96-Well Plate (8x12)"),
    };

    let mut writer = Writer::new_with_indent(Cursor::new(Vec::new()), b' ', 2);
    writer
        .create_element("Plate")
        .write_inner_content(|w| {
            w.create_element("Name")
                .write_text_content(BytesText::new(""))
                .unwrap();
            w.create_element("BarCode")
                .write_text_content(BytesText::new(""))
                .unwrap();
            w.create_element("Description")
                .write_text_content(BytesText::new(""))
                .unwrap();
            w.create_element("Rows")
                .write_text_content(BytesText::new(&rows.to_string()))
                .unwrap();
            w.create_element("Columns")
                .write_text_content(BytesText::new(&cols.to_string()))
                .unwrap();
            w.create_element("PlateKind")
                .write_inner_content(|w2| {
                    w2.create_element("Name")
                        .write_text_content(BytesText::new(plate_kind_name))
                        .unwrap();
                    w2.create_element("Type")
                        .write_text_content(BytesText::new(plate_type_id))
                        .unwrap();
                    w2.create_element("RowCount")
                        .write_text_content(BytesText::new(&rows.to_string()))
                        .unwrap();
                    w2.create_element("ColumnCount")
                        .write_text_content(BytesText::new(&cols.to_string()))
                        .unwrap();
                    Ok(())
                })
                .unwrap();
            w.create_element("FeatureMap")
                .write_inner_content(|w2| {
                    w2.create_element("Feature")
                        .write_inner_content(|w3| {
                            w3.create_element("Id")
                                .write_text_content(BytesText::new("marker-task"))
                                .unwrap();
                            w3.create_element("Name")
                                .write_text_content(BytesText::new("marker-task"))
                                .unwrap();
                            Ok(())
                        })
                        .unwrap();
                    Ok(())
                })
                .unwrap();
            Ok(())
        })
        .unwrap();
    String::from_utf8(writer.into_inner().into_inner()).unwrap()
}

/// Generate a manifest file content.
pub fn new_manifest(version: &str) -> String {
    format!(
        "Manifest-Version: 1.0\n\
         Content-Type: Std\n\
         Implementation-Title: QSLib\n\
         Implementation-Version: {}\n\
         Specification-Vendor: Applied Biosystems\n\
         Specification-Title: Experiment Document Specification\n\
         Specification-Version: 1.3.2\n",
        version
    )
}

/// Decompress the embedded analysis_protocol.xml.
pub fn analysis_protocol_xml() -> String {
    use flate2::read::ZlibDecoder;
    use std::io::Read;

    let compressed = include_bytes!("analysis_protocol.xml.zz");
    let mut decoder = ZlibDecoder::new(&compressed[..]);
    let mut result = String::new();
    decoder
        .read_to_string(&mut result)
        .expect("Failed to decompress analysis_protocol.xml");
    result
}

/// Update experiment.xml preserving unknown elements (event-copy-modify).
///
/// Returns the updated XML string.
pub fn update_experiment_xml(
    raw_xml: &str,
    name: &str,
    operator: Option<&str>,
    created_time_ms: i64,
    modified_time_ms: i64,
    run_start_time_ms: Option<i64>,
    run_end_time_ms: Option<i64>,
    run_state: &str,
    software_version: &str,
) -> Result<String, EdsError> {
    let mut reader = Reader::from_str(raw_xml);
    let mut writer = Writer::new_with_indent(Cursor::new(Vec::new()), b' ', 2);

    let mut depth: u32 = 0;
    let mut seen_elements: HashSet<String> = HashSet::new();
    let mut in_run_info = false;
    let mut in_software_version = false;
    let mut skip_depth: Option<u32> = None;
    let mut suppress_text = false;

    loop {
        match reader.read_event() {
            Ok(Event::Eof) => break,
            Ok(event) => {
                // If we're skipping an element
                if let Some(sd) = skip_depth {
                    match &event {
                        Event::Start(_) => {
                            // Skip nested elements too
                            depth += 1;
                        }
                        Event::End(_) => {
                            if depth == sd {
                                skip_depth = None;
                            }
                            depth -= 1;
                        }
                        _ => {}
                    }
                    continue;
                }

                match &event {
                    Event::Start(ref e) => {
                        depth += 1;
                        let tag = String::from_utf8_lossy(e.name().as_ref()).to_string();

                        // depth==2 means direct child of <Experiment>
                        if depth == 2 {
                            seen_elements.insert(tag.clone());

                            // Handle Operator: skip if should be removed
                            if tag == "Operator" && operator.is_none() {
                                skip_depth = Some(depth);
                                continue;
                            }
                        }

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

                        // Intercept known elements at depth 2 to replace their text
                        suppress_text = false;
                        if depth == 2 {
                            match tag.as_str() {
                                "Name" | "CreatedTime" | "ModifiedTime"
                                | "RunStartTime" | "RunEndTime" | "RunState" | "Operator" => {
                                    suppress_text = true;
                                }
                                _ => {}
                            }
                        }
                        if in_software_version && tag == "String" {
                            suppress_text = true;
                        }

                        writer.write_event(Event::Start(e.clone())).map_err(|e| {
                            EdsError::Xml(format!("write error: {}", e))
                        })?;

                        // Write new text content for intercepted elements
                        if depth == 2 && suppress_text {
                            let new_text = match tag.as_str() {
                                "Name" => Some(name.to_string()),
                                "Operator" => operator.map(|s| s.to_string()),
                                "CreatedTime" => Some(created_time_ms.to_string()),
                                "ModifiedTime" => Some(modified_time_ms.to_string()),
                                "RunStartTime" => run_start_time_ms.map(|t| t.to_string()),
                                "RunEndTime" => run_end_time_ms.map(|t| t.to_string()),
                                "RunState" => Some(run_state.to_string()),
                                _ => None,
                            };
                            if let Some(text) = new_text {
                                writer
                                    .write_event(Event::Text(BytesText::new(&text)))
                                    .map_err(|e| EdsError::Xml(format!("write error: {}", e)))?;
                            }
                        }
                        if in_software_version && tag == "String" {
                            writer
                                .write_event(Event::Text(BytesText::new(software_version)))
                                .map_err(|e| EdsError::Xml(format!("write error: {}", e)))?;
                        }
                    }
                    Event::End(ref e) => {
                        let tag = String::from_utf8_lossy(e.name().as_ref()).to_string();

                        // Before closing </Experiment>, emit missing elements
                        if tag == "Experiment" && depth == 1 {
                            emit_missing_elements(
                                &mut writer,
                                &seen_elements,
                                name,
                                operator,
                                created_time_ms,
                                modified_time_ms,
                                run_start_time_ms,
                                run_end_time_ms,
                                run_state,
                                software_version,
                                in_run_info,
                            )?;
                        }

                        if tag == "ExperimentProperty" {
                            in_run_info = false;
                        }
                        if tag == "PropertyValue" {
                            in_software_version = false;
                        }

                        suppress_text = false;
                        writer.write_event(Event::End(e.clone())).map_err(|e| {
                            EdsError::Xml(format!("write error: {}", e))
                        })?;
                        depth -= 1;
                    }
                    Event::Text(_) | Event::GeneralRef(_) if suppress_text => {
                        // Skip original text for elements we're replacing
                        continue;
                    }
                    _ => {
                        writer.write_event(event.into_owned()).map_err(|e| {
                            EdsError::Xml(format!("write error: {}", e))
                        })?;
                    }
                }
            }
            Err(e) => {
                return Err(EdsError::Xml(format!("experiment.xml parse error: {}", e)));
            }
        }
    }

    String::from_utf8(writer.into_inner().into_inner())
        .map_err(|e| EdsError::Xml(format!("UTF-8 error: {}", e)))
}

/// Emit elements that weren't found in the original XML.
fn emit_missing_elements(
    writer: &mut Writer<Cursor<Vec<u8>>>,
    seen: &HashSet<String>,
    name: &str,
    operator: Option<&str>,
    created_time_ms: i64,
    modified_time_ms: i64,
    run_start_time_ms: Option<i64>,
    run_end_time_ms: Option<i64>,
    run_state: &str,
    software_version: &str,
    had_run_info: bool,
) -> Result<(), EdsError> {
    let write_err = |e| EdsError::Xml(format!("write error: {}", e));

    if !seen.contains("Name") {
        writer.create_element("Name")
            .write_text_content(BytesText::new(name))
            .map_err(write_err)?;
    }
    if !seen.contains("Operator") {
        if let Some(op) = operator {
            writer.create_element("Operator")
                .write_text_content(BytesText::new(op))
                .map_err(write_err)?;
        }
    }
    if !seen.contains("CreatedTime") {
        writer.create_element("CreatedTime")
            .write_text_content(BytesText::new(&created_time_ms.to_string()))
            .map_err(write_err)?;
    }
    if !seen.contains("ModifiedTime") {
        writer.create_element("ModifiedTime")
            .write_text_content(BytesText::new(&modified_time_ms.to_string()))
            .map_err(write_err)?;
    }
    if !seen.contains("RunStartTime") {
        if let Some(t) = run_start_time_ms {
            writer.create_element("RunStartTime")
                .write_text_content(BytesText::new(&t.to_string()))
                .map_err(write_err)?;
        }
    }
    if !seen.contains("RunEndTime") {
        if let Some(t) = run_end_time_ms {
            writer.create_element("RunEndTime")
                .write_text_content(BytesText::new(&t.to_string()))
                .map_err(write_err)?;
        }
    }
    if !seen.contains("RunState") {
        writer.create_element("RunState")
            .write_text_content(BytesText::new(run_state))
            .map_err(write_err)?;
    }
    // Emit software version in ExperimentProperty if not already present
    if !had_run_info {
        let mut ep = BytesStart::new("ExperimentProperty");
        ep.push_attribute(("type", "RunInfo"));
        writer.write_event(Event::Start(ep)).map_err(write_err)?;

        let mut pv = BytesStart::new("PropertyValue");
        pv.push_attribute(("key", "softwareVersion"));
        writer.write_event(Event::Start(pv)).map_err(write_err)?;

        writer.create_element("String")
            .write_text_content(BytesText::new(software_version))
            .map_err(write_err)?;

        writer.write_event(Event::End(BytesEnd::new("PropertyValue"))).map_err(write_err)?;
        writer.write_event(Event::End(BytesEnd::new("ExperimentProperty"))).map_err(write_err)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_experiment_xml_96() {
        let xml = new_experiment_xml(96);
        assert!(xml.contains("<Experiment>"));
        assert!(xml.contains("<PlateTypeID>TYPE_8X12</PlateTypeID>"));
        assert!(xml.contains("<Label>ruo</Label>"));
        assert!(xml.contains("<InstrumentTypeId>appletini</InstrumentTypeId>"));
    }

    #[test]
    fn test_new_experiment_xml_384() {
        let xml = new_experiment_xml(384);
        assert!(xml.contains("<PlateTypeID>TYPE_16X24</PlateTypeID>"));
    }

    #[test]
    fn test_new_plate_setup_xml() {
        let xml = new_plate_setup_xml(96);
        assert!(xml.contains("<Rows>8</Rows>"));
        assert!(xml.contains("<Columns>12</Columns>"));
        assert!(xml.contains("<Type>TYPE_8X12</Type>"));
    }

    #[test]
    fn test_new_manifest() {
        let m = new_manifest("0.14.0");
        assert!(m.contains("Implementation-Version: 0.14.0"));
        assert!(m.contains("Specification-Version: 1.3.2"));
    }

    #[test]
    fn test_analysis_protocol_xml() {
        let xml = analysis_protocol_xml();
        assert!(xml.contains("<JaxbAnalysisProtocol>"));
        assert!(xml.len() > 1000);
    }

    #[test]
    fn test_update_experiment_xml_basic() {
        let original = new_experiment_xml(96);
        let updated = update_experiment_xml(
            &original,
            "my_experiment",
            Some("operator1"),
            1000000,
            2000000,
            Some(1500000),
            None,
            "RUNNING",
            "QSLib 0.14.0",
        )
        .unwrap();

        assert!(updated.contains("<Name>my_experiment</Name>"));
        assert!(updated.contains("<Operator>operator1</Operator>"));
        assert!(updated.contains("<CreatedTime>1000000</CreatedTime>"));
        assert!(updated.contains("<ModifiedTime>2000000</ModifiedTime>"));
        assert!(updated.contains("<RunStartTime>1500000</RunStartTime>"));
        assert!(updated.contains("<RunState>RUNNING</RunState>"));
        assert!(updated.contains("QSLib 0.14.0"));
        // Original template elements preserved
        assert!(updated.contains("<Label>ruo</Label>"));
        assert!(updated.contains("<PlateTypeID>TYPE_8X12</PlateTypeID>"));
    }

    #[test]
    fn test_update_experiment_xml_preserves_unknowns() {
        let original = r#"<Experiment>
  <Label>ruo</Label>
  <Name>old_name</Name>
  <UnknownElement>preserve me</UnknownElement>
  <RunState>INIT</RunState>
  <AnotherCustom attr="yes">data</AnotherCustom>
</Experiment>"#;

        let updated = update_experiment_xml(
            original,
            "new_name",
            None,
            1000,
            2000,
            None,
            None,
            "COMPLETE",
            "QSLib 0.14.0",
        )
        .unwrap();

        assert!(updated.contains("<Name>new_name</Name>"));
        assert!(updated.contains("<RunState>COMPLETE</RunState>"));
        assert!(updated.contains("<UnknownElement>preserve me</UnknownElement>"));
        assert!(updated.contains("AnotherCustom"));
        assert!(updated.contains("data"));
        assert!(!updated.contains("old_name"));
        // Operator should not be present (None)
        assert!(!updated.contains("<Operator>"));
    }

    #[test]
    fn test_update_removes_operator_when_none() {
        let original = r#"<Experiment>
  <Name>test</Name>
  <Operator>old_user</Operator>
  <RunState>INIT</RunState>
</Experiment>"#;

        let updated = update_experiment_xml(
            original, "test", None, 1000, 2000, None, None, "INIT", "v1",
        )
        .unwrap();

        assert!(!updated.contains("<Operator>"));
        assert!(!updated.contains("old_user"));
    }

    #[test]
    fn test_update_preserves_software_version() {
        let original = r#"<Experiment>
  <Name>test</Name>
  <ExperimentProperty type="RunInfo">
    <PropertyValue key="softwareVersion">
      <String>old version</String>
    </PropertyValue>
  </ExperimentProperty>
</Experiment>"#;

        let updated = update_experiment_xml(
            original, "test", None, 1000, 2000, None, None, "INIT", "QSLib 0.14.0",
        )
        .unwrap();

        assert!(updated.contains("QSLib 0.14.0"));
        assert!(!updated.contains("old version"));
    }

    #[test]
    fn test_roundtrip_update() {
        let template = new_experiment_xml(96);
        let updated1 = update_experiment_xml(
            &template, "exp1", Some("user1"), 1000, 2000, Some(1500), None, "RUNNING", "v1",
        )
        .unwrap();
        let updated2 = update_experiment_xml(
            &updated1, "exp1", Some("user1"), 1000, 3000, Some(1500), Some(2500), "COMPLETE", "v2",
        )
        .unwrap();

        assert!(updated2.contains("<Name>exp1</Name>"));
        assert!(updated2.contains("<RunEndTime>2500</RunEndTime>"));
        assert!(updated2.contains("<RunState>COMPLETE</RunState>"));
        assert!(updated2.contains("<ModifiedTime>3000</ModifiedTime>"));
        // Template elements still present
        assert!(updated2.contains("<Label>ruo</Label>"));
    }
}
