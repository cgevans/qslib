use polars::prelude::*;
use serde::{Deserialize, Deserializer, Serialize};
use thiserror::Error;

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
#[cfg_attr(feature = "python", pyclass(get_all, set_all))]
pub struct FilterDataCollection {
    #[serde(rename = "Name")]
    pub name: String,
    #[serde(rename = "PlatePointData")]
    pub plate_point_data: Vec<PlatePointData>,
}


#[derive(Debug, Serialize, Deserialize, Clone)]
#[cfg_attr(feature = "python", pyclass(get_all, set_all))]
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
#[cfg_attr(feature = "python", pyclass(get_all, set_all))]
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
        let zone_size = if num_zones > 0 { self.cols / num_zones as u32 } else { self.cols };

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
        let zone = self.col_indices().iter().map(|c| c / zone_size).collect::<Vec<_>>();
        let df = df![
            "well" => well_names.iter().map(|(row, col)| format!("{row}{col}")).collect::<Vec<String>>(),
            "row" => self.row_indices(),
            "column" => c,
            "fluorescence" => self.well_data.clone(),
            "sample_temperature" => sts,
            "zone" => zone,
        ]?;
        Ok(df.lazy().with_columns([
            lit(self.filter_set().map_err(|e| PolarsError::ComputeError(e.to_string().into()))?.to_string()).alias("filter_set"),
            lit(self.get_stage().ok_or_else(|| PolarsError::ComputeError("Missing STAGE attribute".into()))?).alias("stage"),
            lit(self.get_cycle().ok_or_else(|| PolarsError::ComputeError("Missing CYCLE attribute".into()))?).alias("cycle"),
            lit(self.get_step().ok_or_else(|| PolarsError::ComputeError("Missing STEP attribute".into()))?).alias("step"),
            lit(self.get_point().ok_or_else(|| PolarsError::ComputeError("Missing POINT attribute".into()))?).alias("point"),
            lit(self.get_exposure().ok_or_else(|| PolarsError::ComputeError("Missing EXPOSURE attribute".into()))?).alias("exposure"),
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
#[cfg_attr(feature = "python", pyclass(get_all, set_all))]
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

        assert!(lines[0].contains("machine=\"QS5\""));
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
}
