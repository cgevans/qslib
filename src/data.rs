use serde::{Deserialize, Deserializer, Serialize};
use thiserror::Error;

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
pub struct FilterDataCollection {
    #[serde(rename = "Name")]
    pub name: String,
    #[serde(rename = "PlatePointData")]
    pub plate_point_data: Vec<PlatePointData>,
}

#[derive(Debug, Serialize, Deserialize)]
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

#[derive(Debug, Serialize, Deserialize)]
pub struct PlateData {
    #[serde(rename = "Rows")]
    pub rows: i32,
    #[serde(rename = "Cols")]
    pub cols: i32,
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

    /// Convert plate data to InfluxDB line protocol format
    pub fn to_lineprotocol(
        &self,
        run_name: Option<&str>,
        sample_array: Option<&[String]>,
        additional_tags: Option<&[(&str, &str)]>,
    ) -> Result<Vec<String>, DataError> {
        let mut lines = Vec::new();
        let filter_set = self.filter_set()?;
        let mut gs = format!("filterdata,filter_set={}", filter_set);

        if let Some(tags) = additional_tags {
            for (key, value) in tags {
                gs.push_str(&format!(",{}=\"{}\"", key, value));
            }
        }
        let gs = gs;

        // Get timestamp in nanoseconds
        let timestamp_ns = match self.timestamp {
            Some(ts) => format!(" {}", (ts * 1e9) as i64),
            None => String::new(),
        };

        // Generate well names (A01, A02, etc.)
        let well_names: Vec<(char, i32)> = (0..self.rows)
            .flat_map(|row| (1..=self.cols).map(move |col| ((b'A' + row as u8) as char, col)))
            .collect();

        // Get temperatures if available
        let temperatures = self.set_temperatures.as_ref();

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

            // Add temperature if available
            if let Some(temp) = self
                .get_attribute("TEMPERATURE")
                .and_then(|t| t.split(',').nth(((col - 1) / (self.cols / 6)) as usize))
                .and_then(|t| t.parse::<f64>().ok())
            {
                line.push_str(&format!(",temperature_read={}", temp));
            }

            // Add sample if provided
            if let Some(samples) = sample_array {
                let idx =
                    ((*row_letter as u8 - b'A') as usize) * self.cols as usize + (col - 1) as usize;
                if idx < samples.len() {
                    line.push_str(&format!(",sample=\"{}\"", samples[idx]));
                }
            }

            // Add run name if provided
            if let Some(name) = run_name {
                line.push_str(&format!(",run_name=\"{}\"", name));
            }

            // Add set temperature if available
            if let Some(temps) = temperatures {
                let x = (col - 1) as usize / (self.cols as usize / temps.len());
                if let Some(&temp) = temps.get(x) {
                    line.push_str(&format!(",temperature_set={}", temp));
                }
            }

            line.push_str(&timestamp_ns);
            lines.push(line);
        }

        Ok(lines)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Attribute {
    pub key: String,
    pub value: String,
}

impl PlateData {
    /// Get an attribute value by key
    pub fn get_attribute(&self, key: &str) -> Option<&str> {
        self.attributes
            .iter()
            .find(|attr| attr.key == key)
            .map(|attr| attr.value.as_str())
    }
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
            .to_lineprotocol(Some("test_run"), None, None)
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
}
