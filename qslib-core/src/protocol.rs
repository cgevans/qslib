//! Lightweight thermal-protocol XML model and SCPI serialization.
//!
//! This deliberately contains no analysis, dataframe, or HTTP dependencies so
//! both direct clients and qslib-server validate exactly the same protocol.

use std::io::Write;

use quick_xml::events::Event;
use quick_xml::Reader;
use thiserror::Error;

use crate::com::QSConnectionError;
use crate::commands::CommandBuilder;
use crate::parser::ErrorResponse;

#[derive(Debug, Clone, PartialEq)]
pub struct ProtocolModel {
    pub name: String,
    pub volume: f64,
    pub run_mode: String,
    pub cover_temperature_c: f64,
    pub filters: Vec<String>,
    pub stages: Vec<ProtocolStage>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProtocolStage {
    pub repeat: i64,
    pub steps: Vec<ProtocolStep>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProtocolStep {
    pub collect: bool,
    pub temperatures_c: Vec<f64>,
    pub hold_s: i64,
    pub temperature_increment_c: f64,
    pub time_increment_s: i64,
    pub increment_cycle: i64,
}

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("invalid XML: {0}")]
    Xml(String),
    #[error("missing required protocol field {0}")]
    Missing(&'static str),
    #[error("invalid protocol field: {0}")]
    Invalid(String),
}

#[derive(Default)]
struct StepBuilder {
    collect: bool,
    temperatures_c: Vec<f64>,
    hold_s: i64,
    temperature_increment_c: f64,
    time_increment_s: i64,
}

#[derive(Default)]
struct StageBuilder {
    repeat: i64,
    starting_cycle: i64,
    auto_delta: bool,
    steps: Vec<ProtocolStep>,
}

impl ProtocolModel {
    /// Parse canonical `tcprotocol.xml`, ignoring unknown vendor extensions.
    pub fn from_xml(xml: &str) -> Result<Self, ProtocolError> {
        let mut reader = Reader::from_str(xml);
        reader.config_mut().trim_text(true);
        let mut stack: Vec<String> = Vec::new();
        let mut name = String::new();
        let mut volume = 50.0;
        let mut run_mode = "standard".to_string();
        let mut cover_temperature_c = 105.0;
        let mut filters = Vec::new();
        let mut stages = Vec::new();
        let mut stage: Option<StageBuilder> = None;
        let mut step: Option<StepBuilder> = None;

        loop {
            match reader.read_event() {
                Ok(Event::Eof) => break,
                Ok(Event::Start(event)) => {
                    let tag = String::from_utf8_lossy(event.name().as_ref()).into_owned();
                    if tag == "TCStage" {
                        stage = Some(StageBuilder {
                            repeat: 1,
                            starting_cycle: 1,
                            ..StageBuilder::default()
                        });
                    } else if tag == "TCStep" {
                        step = Some(StepBuilder::default());
                    } else if tag == "FilterSet" {
                        parse_filter(&event, &mut filters);
                    }
                    stack.push(tag);
                }
                Ok(Event::Empty(event)) if event.name().as_ref() == b"FilterSet" => {
                    parse_filter(&event, &mut filters);
                }
                Ok(Event::Empty(_)) => {}
                Ok(Event::Text(event)) => {
                    let text = event
                        .decode()
                        .map_err(|error| ProtocolError::Xml(error.to_string()))?
                        .trim()
                        .to_string();
                    let tag = stack.last().map(String::as_str).unwrap_or("");
                    if let Some(step) = step.as_mut() {
                        match tag {
                            "CollectionFlag" => {
                                step.collect = text == "1" || text.eq_ignore_ascii_case("true")
                            }
                            "Temperature" => step.temperatures_c.push(parse_f64(tag, &text)?),
                            "HoldTime" => step.hold_s = parse_i64(tag, &text)?,
                            "ExtTemperature" => {
                                step.temperature_increment_c = parse_f64(tag, &text)?
                            }
                            "ExtHoldTime" => step.time_increment_s = parse_i64(tag, &text)?,
                            _ => {}
                        }
                    } else if let Some(stage) = stage.as_mut() {
                        match tag {
                            "NumOfRepetitions" => stage.repeat = parse_i64(tag, &text)?,
                            "StartingCycle" => stage.starting_cycle = parse_i64(tag, &text)?,
                            "AutoDeltaEnabled" => {
                                stage.auto_delta = text == "1" || text.eq_ignore_ascii_case("true")
                            }
                            _ => {}
                        }
                    } else {
                        match tag {
                            "ProtocolName" => name = text,
                            "SampleVolume" => volume = parse_f64(tag, &text)?,
                            "RunMode" => run_mode = text,
                            "CoverTemperature" => cover_temperature_c = parse_f64(tag, &text)?,
                            _ => {}
                        }
                    }
                }
                Ok(Event::End(event)) => {
                    let tag = String::from_utf8_lossy(event.name().as_ref()).into_owned();
                    if tag == "TCStep" {
                        let built = step.take().ok_or_else(|| {
                            ProtocolError::Invalid("TCStep ended without starting".to_string())
                        })?;
                        if built.temperatures_c.is_empty() {
                            return Err(ProtocolError::Invalid(
                                "TCStep contains no Temperature values".to_string(),
                            ));
                        }
                        let parent = stage.as_mut().ok_or_else(|| {
                            ProtocolError::Invalid("TCStep is outside TCStage".to_string())
                        })?;
                        parent.steps.push(ProtocolStep {
                            collect: built.collect,
                            temperatures_c: built.temperatures_c,
                            hold_s: built.hold_s,
                            temperature_increment_c: if parent.auto_delta {
                                built.temperature_increment_c
                            } else {
                                0.0
                            },
                            time_increment_s: if parent.auto_delta {
                                built.time_increment_s
                            } else {
                                0
                            },
                            increment_cycle: if parent.auto_delta {
                                parent.starting_cycle.max(1)
                            } else {
                                1
                            },
                        });
                    } else if tag == "TCStage" {
                        let built = stage.take().ok_or_else(|| {
                            ProtocolError::Invalid("TCStage ended without starting".to_string())
                        })?;
                        if built.steps.is_empty() || built.repeat < 1 {
                            return Err(ProtocolError::Invalid(
                                "TCStage must contain steps and repeat at least once".to_string(),
                            ));
                        }
                        stages.push(ProtocolStage {
                            repeat: built.repeat,
                            steps: built.steps,
                        });
                    }
                    if stack.last().is_some_and(|open| open == &tag) {
                        stack.pop();
                    }
                }
                Err(error) => return Err(ProtocolError::Xml(error.to_string())),
                _ => {}
            }
        }
        if name.is_empty() {
            return Err(ProtocolError::Missing("ProtocolName"));
        }
        if stages.is_empty() {
            return Err(ProtocolError::Missing("TCStage"));
        }
        if !volume.is_finite() || volume <= 0.0 {
            return Err(ProtocolError::Invalid(
                "SampleVolume must be positive and finite".to_string(),
            ));
        }
        Ok(Self {
            name,
            volume,
            run_mode,
            cover_temperature_c,
            filters,
            stages,
        })
    }

    /// Extract the lossless QSLib SCPI representation when a package includes
    /// `qsl-tcprotocol.xml`.
    pub fn scpi_from_qsl_xml(xml: &str) -> Option<String> {
        extract_element(xml, "QSLibProtocolCommand")
    }

    pub fn to_scpi(&self) -> String {
        let mut stage_body = String::new();
        for (stage_index, stage) in self.stages.iter().enumerate() {
            let mut step_body = String::new();
            for (step_index, step) in stage.steps.iter().enumerate() {
                let mut commands = format!(
                    "RAMP{}\n",
                    step.temperatures_c
                        .iter()
                        .map(|value| format!(" {}", number(*value)))
                        .collect::<String>()
                );
                if step.collect {
                    commands.push_str("HACFILT");
                    for filter in &self.filters {
                        commands.push(' ');
                        commands.push_str(filter);
                    }
                    commands.push('\n');
                    commands.push_str(&format!(
                        "HOLDANDCOLLECT -increment={} -incrementcycle={} -tiff=False -quant=True -pcr=True {}\n",
                        step.time_increment_s, step.increment_cycle, step.hold_s
                    ));
                } else {
                    commands.push_str(&format!(
                        "HOLD -increment={} -incrementcycle={} {}\n",
                        step.time_increment_s, step.increment_cycle, step.hold_s
                    ));
                }
                step_body.push_str(&format!(
                    "STEP {} <multiline.step>\n{}</multiline.step>\n",
                    step_index + 1,
                    indent(&commands, "\t")
                ));
            }
            let repeat = if stage.repeat == 1 {
                String::new()
            } else {
                format!(" -repeat={}", stage.repeat)
            };
            stage_body.push_str(&format!(
                "STAGE{} {} STAGE_{} <multiline.stage>\n{}</multiline.stage>\n",
                repeat,
                stage_index + 1,
                stage_index + 1,
                indent(&step_body, "\t")
            ));
        }
        format!(
            "PROTOCOL -volume={} -runmode={} {} <multiline.protocol>\n{}</multiline.protocol>\n",
            number(self.volume),
            self.run_mode,
            self.name,
            indent(&stage_body, "\t")
        )
    }

    /// Check whether replacing this protocol can preserve all work already
    /// executed at `stage`/`cycle`. Future stages may change freely; completed
    /// stages must be identical and the current stage may only change its
    /// repeat count to a value no lower than the current cycle.
    pub fn check_compatible(
        &self,
        new: &Self,
        stage: i64,
        cycle: i64,
    ) -> Result<(), ProtocolError> {
        if self.volume != new.volume
            || self.run_mode != new.run_mode
            || self.cover_temperature_c != new.cover_temperature_c
            || self.filters != new.filters
        {
            return Err(ProtocolError::Invalid(
                "protocol settings already in use cannot be changed".to_string(),
            ));
        }

        for index in 0..self.stages.len().max(new.stages.len()) {
            let position = index as i64 + 1;
            if position > stage {
                continue;
            }
            let old_stage = self.stages.get(index).ok_or_else(|| {
                ProtocolError::Invalid("old protocol is missing an executed stage".to_string())
            })?;
            let new_stage = new.stages.get(index).ok_or_else(|| {
                ProtocolError::Invalid("new protocol removes an executed stage".to_string())
            })?;
            if position < stage {
                if old_stage != new_stage {
                    return Err(ProtocolError::Invalid(format!(
                        "completed stage {position} cannot be changed"
                    )));
                }
            } else {
                if new_stage.repeat < cycle.max(1) {
                    return Err(ProtocolError::Invalid(format!(
                        "current stage repeat {} is below current cycle {}",
                        new_stage.repeat, cycle
                    )));
                }
                let mut old_for_comparison = old_stage.clone();
                old_for_comparison.repeat = new_stage.repeat;
                if old_for_comparison != *new_stage {
                    return Err(ProtocolError::Invalid(format!(
                        "current stage {position} may only change its repeat count"
                    )));
                }
            }
        }
        Ok(())
    }
}

/// Validated protocol definition command used by qslib-server operations.
#[derive(Debug, Clone)]
pub struct ProtocolDefinition(String);

#[derive(Debug, Clone, PartialEq)]
pub struct ProtocolSettings {
    pub name: String,
    pub sample_volume: f64,
    pub run_mode: String,
}

impl ProtocolDefinition {
    /// Validate that `scpi` contains exactly one protocol-definition command.
    pub fn new(scpi: String) -> Result<Self, ProtocolError> {
        let mut input = scpi.as_bytes();
        let command = crate::parser::Command::parse(&mut input).map_err(|error| {
            ProtocolError::Invalid(format!("invalid SCPI protocol command: {error}"))
        })?;
        if !input.iter().all(u8::is_ascii_whitespace) {
            return Err(ProtocolError::Invalid(
                "SCPI protocol contains trailing commands".to_string(),
            ));
        }
        let name = String::from_utf8_lossy(&command.command);
        if !name.eq_ignore_ascii_case("PROT") && !name.eq_ignore_ascii_case("PROTOCOL") {
            return Err(ProtocolError::Invalid(
                "SCPI command is not a protocol definition".to_string(),
            ));
        }
        Ok(Self(scpi))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Extract the run-start settings from the exact SCPI definition without
    /// interpreting or approximating its stage contents.
    pub fn settings(&self) -> Result<ProtocolSettings, ProtocolError> {
        let command = crate::parser::Command::try_from(self.0.clone()).map_err(|error| {
            ProtocolError::Invalid(format!("invalid SCPI protocol command: {error}"))
        })?;
        let name = command
            .args
            .first()
            .and_then(|value| String::try_from(value).ok())
            .ok_or(ProtocolError::Missing("protocol name"))?;
        let option = |key: &str| {
            command
                .options
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case(key))
                .map(|(_, value)| value)
        };
        let sample_volume = match option("volume") {
            Some(crate::parser::Value::Float(value)) => *value,
            Some(crate::parser::Value::Int(value)) => *value as f64,
            Some(value) => value.to_string().parse::<f64>().map_err(|_| {
                ProtocolError::Invalid("protocol volume is not numeric".to_string())
            })?,
            None => 50.0,
        };
        let run_mode = option("runmode")
            .map(ToString::to_string)
            .unwrap_or_else(|| "standard".to_string());
        Ok(ProtocolSettings {
            name,
            sample_volume,
            run_mode,
        })
    }
}

impl CommandBuilder for ProtocolDefinition {
    type Response = ();
    type Error = ErrorResponse;
    const COMMAND: &'static [u8] = b"PROTOCOL";

    fn write_command(&self, bytes: &mut impl Write) -> Result<(), QSConnectionError> {
        bytes.write_all(self.0.as_bytes())?;
        Ok(())
    }
}

fn parse_filter(event: &quick_xml::events::BytesStart<'_>, filters: &mut Vec<String>) {
    let mut excitation = None;
    let mut emission = None;
    for attribute in event.attributes().flatten() {
        match attribute.key.as_ref() {
            b"Excitation" => {
                excitation = Some(String::from_utf8_lossy(&attribute.value).into_owned())
            }
            b"Emission" => emission = Some(String::from_utf8_lossy(&attribute.value).into_owned()),
            _ => {}
        }
    }
    if let (Some(excitation), Some(emission)) = (excitation, emission) {
        filters.push(format!(
            "{}-{}",
            excitation.to_ascii_lowercase(),
            emission.to_ascii_lowercase()
        ));
    }
}

fn parse_f64(field: &str, value: &str) -> Result<f64, ProtocolError> {
    value
        .parse::<f64>()
        .ok()
        .filter(|number| number.is_finite())
        .ok_or_else(|| ProtocolError::Invalid(format!("{field} is not a finite number")))
}

fn parse_i64(field: &str, value: &str) -> Result<i64, ProtocolError> {
    value
        .parse()
        .map_err(|_| ProtocolError::Invalid(format!("{field} is not an integer")))
}

fn extract_element(xml: &str, target: &str) -> Option<String> {
    let mut reader = Reader::from_str(xml);
    let mut inside = false;
    let mut output = String::new();
    loop {
        match reader.read_event().ok()? {
            Event::Eof => return None,
            Event::Start(event) if event.name().as_ref() == target.as_bytes() => inside = true,
            Event::End(event) if event.name().as_ref() == target.as_bytes() => return Some(output),
            Event::Text(text) if inside => output.push_str(text.decode().ok()?.as_ref()),
            Event::GeneralRef(reference) if inside => {
                let name = std::str::from_utf8(reference.as_ref()).ok()?;
                match name {
                    "lt" => output.push('<'),
                    "gt" => output.push('>'),
                    "amp" => output.push('&'),
                    "quot" => output.push('"'),
                    "apos" => output.push('\''),
                    _ => return None,
                }
            }
            _ => {}
        }
    }
}

fn number(value: f64) -> String {
    if value.fract() == 0.0 {
        format!("{}", value as i64)
    } else {
        value.to_string()
    }
}

fn indent(value: &str, prefix: &str) -> String {
    value
        .lines()
        .map(|line| format!("{prefix}{line}\n"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINIMAL: &str = r#"<TCProtocol><ProtocolName>demo</ProtocolName><SampleVolume>20</SampleVolume><RunMode>Standard</RunMode><TCStage><NumOfRepetitions>2</NumOfRepetitions><TCStep><CollectionFlag>0</CollectionFlag><Temperature>60</Temperature><HoldTime>10</HoldTime><ExtTemperature>0</ExtTemperature><ExtHoldTime>0</ExtHoldTime></TCStep></TCStage></TCProtocol>"#;

    #[test]
    fn parses_and_serializes_minimal_protocol() {
        let protocol = ProtocolModel::from_xml(MINIMAL).unwrap();
        assert_eq!(protocol.name, "demo");
        assert!(protocol.to_scpi().starts_with("PROTOCOL -volume=20"));
    }

    #[test]
    fn exact_scpi_definition_provides_run_settings() {
        let scpi = "PROT -volume=12 -runmode=fast exact <multiline.protocol>\n\tSTAGE 1 S <multiline.stage>\n\t</multiline.stage>\n</multiline.protocol>";
        let definition = ProtocolDefinition::new(scpi.to_string()).unwrap();
        let settings = definition.settings().unwrap();
        assert_eq!(settings.name, "exact");
        assert_eq!(settings.sample_volume, 12.0);
        assert_eq!(settings.run_mode, "fast");
        assert_eq!(definition.as_str(), scpi);
    }

    #[test]
    fn exact_scpi_definition_rejects_trailing_commands() {
        let scpi = "PROT exact <multiline.protocol></multiline.protocol>\nPOW OFF";
        assert!(ProtocolDefinition::new(scpi.to_string()).is_err());
    }

    #[test]
    fn qsl_xml_extracts_entity_escaped_exact_protocol() {
        let xml = "<QSTCProtocol><QSLibProtocolCommand>PROT exact &lt;multiline.protocol&gt;&lt;/multiline.protocol&gt;</QSLibProtocolCommand></QSTCProtocol>";
        assert_eq!(
            ProtocolModel::scpi_from_qsl_xml(xml).as_deref(),
            Some("PROT exact <multiline.protocol></multiline.protocol>")
        );
    }

    #[test]
    fn compatibility_preserves_completed_and_current_work() {
        let old = ProtocolModel::from_xml(MINIMAL).unwrap();
        let mut extended = old.clone();
        extended.stages[0].repeat = 5;
        extended.stages.push(old.stages[0].clone());
        assert!(old.check_compatible(&extended, 1, 2).is_ok());

        extended.stages[0].steps[0].hold_s += 1;
        assert!(old.check_compatible(&extended, 1, 2).is_err());
    }
}
