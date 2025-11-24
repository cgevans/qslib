use std::collections::HashMap;

use quick_xml::{de::from_str, se::to_string};
use serde::de::Error;
use serde::{Deserialize, Serialize};

#[cfg(feature = "python")]
use pyo3::prelude::*;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename = "Plate", deny_unknown_fields)]
#[cfg_attr(feature = "python", pyclass(get_all, set_all))]
pub struct PlateSetup {
    #[serde(rename = "Name")]
    pub name: Option<String>,
    #[serde(rename = "BarCode")]
    pub barcode: Option<String>,
    #[serde(rename = "Description")]
    pub description: Option<String>,
    #[serde(rename = "Rows")]
    pub rows: u32,
    #[serde(rename = "Columns")]
    pub columns: u32,
    #[serde(rename = "PlateKind")]
    pub plate_kinds: Vec<PlateKind>,
    #[serde(rename = "FeatureMap", default)]
    pub feature_maps: Vec<FeatureMap>,
    #[serde(skip)]
    pub plate_type: PlateType,
    #[serde(rename = "Wells", default)]
    pub wells: Vec<OtherTag>,
    #[serde(rename = "MultiZoneEnabled")]
    pub multi_zone_enabled: Option<String>,
    #[serde(rename = "LogicalZone", default)]
    pub logical_zones: Vec<OtherTag>,
    #[serde(rename = "PassiveReferenceDye", default)]
    pub passive_reference_dye: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[cfg_attr(feature = "python", pyclass(get_all, set_all))]
pub struct OtherTag {
    #[serde(flatten)]
    pub other: HashMap<String, MapOrString>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[cfg_attr(feature = "python", pyclass(get_all, set_all))]
pub struct PlateKind {
    #[serde(rename = "Name")]
    pub name: String,
    #[serde(rename = "Type")]
    pub kind_type: PlateType,
    #[serde(rename = "RowCount")]
    pub row_count: u32,
    #[serde(rename = "ColumnCount")]
    pub column_count: u32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[cfg_attr(feature = "python", pyclass(get_all, set_all))]
pub struct FeatureMap {
    #[serde(rename = "Feature")]
    pub feature: Feature,
    #[serde(rename = "FeatureValue", default)]
    pub feature_values: Vec<FeatureValue>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[cfg_attr(feature = "python", pyclass(get_all, set_all))]
pub struct Feature {
    #[serde(rename = "Id")]
    pub id: String,
    #[serde(rename = "Name")]
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[cfg_attr(feature = "python", pyclass(get_all, set_all))]
pub struct FeatureValue {
    #[serde(rename = "Index")]
    pub index: u32,
    #[serde(rename = "FeatureItem")]
    pub feature_item: FeatureItem,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[cfg_attr(feature = "python", pyclass(get_all, set_all))]
#[serde(untagged)]
pub enum MapOrString {
    Map(HashMap<String, MapOrString>),
    String(String),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[cfg_attr(feature = "python", pyclass(get_all, set_all))]
pub struct FeatureItem {
    #[serde(rename = "Sample", skip_serializing_if = "Option::is_none")]
    pub sample: Option<Sample>,
    #[serde(flatten)]
    pub other: HashMap<String, MapOrString>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "python", pyclass)]
pub struct Sample {
    #[serde(rename = "Name")]
    pub name: String,
    #[serde(rename = "Color")]
    pub color: Color,
    #[serde(rename = "Description")]
    pub description: Option<String>,
    #[serde(rename = "CustomProperty")]
    custom_properties: Vec<CustomProperty>,
}

impl Sample {
    pub fn new(name: String) -> Self {
        Self {
            name,
            color: Color::rgb(100, 100, 100),
            description: None,
            custom_properties: vec![],
        }
    }

    pub fn with_color(mut self, color: Color) -> Self {
        self.color = color;
        self
    }

    pub fn with_property(mut self, key: &str, value: String) -> Self {
        self.set_property(key, value);
        self
    }

    pub fn with_generated_uuid(self) -> Self {
        self.with_property("SP_UUID", uuid::Uuid::new_v4().simple().to_string())
    }

    pub fn get_property(&self, key: &str) -> Option<String> {
        self.custom_properties.iter().find(|p| p.property == key).map(|p| p.value.clone())
    }

    pub fn get_property_ref(&self, key: &str) -> Option<&str> {
        self.custom_properties.iter().find(|p| p.property == key).map(|p| p.value.as_str())
    }

    pub fn set_property(&mut self, key: &str, value: String) {
        if let Some(prop) = self.custom_properties.iter_mut().find(|p| p.property == key) {
            prop.value = value;
        } else {
            self.custom_properties.push(CustomProperty { property: key.to_string(), value });
        }
    }

    pub fn clear_property(&mut self, key: &str) {
        self.custom_properties.retain(|p| p.property != key);
    }
}


#[cfg(feature = "python")]
#[pymethods]
impl Sample {
    #[new]
    #[pyo3(signature = (name, uuid=None, color=None, properties=None, description=None))]
    fn new_py(name: String, uuid: Option<String>, color: Option<(u8, u8, u8, u8)>, properties: Option<HashMap<String, String>>, description: Option<String>) -> Self {
        let mut sample = Self::new(name);
        if let Some(uuid) = uuid {
            sample.set_property("SP_UUID", uuid);
        } else {
            sample.set_property("SP_UUID", uuid::Uuid::new_v4().simple().to_string());
        }
        if let Some(color) = color {
            sample.color = Color::rgba(color.0, color.1, color.2, color.3);
        } else {
            sample.color = Color::rgb(100, 100, 100);
        }
        if let Some(properties) = properties {
            for (key, value) in properties {
                sample.set_property(&key, value);
            }
        }
        if let Some(description) = description {
            sample.description = Some(description);
        }
        sample
    }

    #[getter]
    fn name(&self) -> String {
        self.name.clone()
    }

    #[setter]
    fn py_set_name(&mut self, name: String) {
        self.name = name;
    }

    #[getter]
    fn color(&self) -> String {
        self.color.to_hex()
    }

    #[setter]
    fn set_color(&mut self, color: String) {
        self.color = Color::try_from(color).unwrap();
    }

    /// Set color from RGBA tuple
    #[pyo3(name = "set_color_rgba")]
    fn py_set_color_rgba(&mut self, r: u8, g: u8, b: u8, a: u8) {
        self.color = Color::rgba(r, g, b, a);
    }

    /// Get color as RGBA tuple
    #[getter]
    fn color_rgba(&self) -> (u8, u8, u8, u8) {
        self.color.to_rgba()
    }

    #[getter]
    fn description(&self) -> Option<String> {
        self.description.clone()
    }

    #[setter]
    fn set_description(&mut self, description: Option<String>) {
        self.description = description;
    }

    #[getter]
    fn uuid(&self) -> Option<String> {
        self.get_property("SP_UUID")
    }

    #[setter]
    fn set_uuid(&mut self, uuid: String) {
        self.set_property("SP_UUID", uuid);
    }

    /// Get a property value by key
    #[pyo3(name = "get_property")]
    fn py_get_property(&self, key: &str) -> Option<String> {
        self.get_property(key)
    }

    /// Get all properties as a dictionary
    #[pyo3(name = "get_properties")]
    fn get_properties(&self) -> std::collections::HashMap<String, String> {
        self.custom_properties.iter()
            .map(|prop| (prop.property.clone(), prop.value.clone()))
            .collect()
    }

    /// Set a property
    #[pyo3(name = "set_property")]
    fn py_set_property(&mut self, key: String, value: String) {
        self.set_property(&key, value);
    }

    fn __repr__(&self) -> String {
        format!("Sample(name='{}', color={:?})", self.name, self.color)
    }

    fn __eq__(&self, other: &Self) -> bool {
        self.name == other.name
        && self.color == other.color
        && self.description == other.description
        && self.custom_properties.iter().filter(|prop| prop.property != "SP_UUID").eq(
            other.custom_properties.iter().filter(|prop| prop.property != "SP_UUID")
        )
    }

    #[pyo3(name = "to_record")]
    fn to_record(&self, py: pyo3::Python<'_>) -> pyo3::Py<pyo3::PyAny> {
        use pyo3::types::{PyDict};
        let record = PyDict::new(py);

        record.set_item("name", self.name.clone()).unwrap();
        record.set_item("color", self.color.to_hex()).unwrap();
        record.set_item("description", self.description()).unwrap_or(());
        record.set_item("uuid", self.get_property("SP_UUID")).unwrap_or(());

        let properties = PyDict::new(py);
        for prop in &self.custom_properties {
            properties.set_item(&prop.property, &prop.value).unwrap();
        }
        record.set_item("properties", properties).unwrap();

        record.into()
    }

    fn __getitem__(&self, key: &str) -> pyo3::PyResult<String> {
        self.get_property(key).ok_or_else(|| pyo3::exceptions::PyKeyError::new_err(format!("No property found for key: {}", key)))
    }


    fn __setitem__(&mut self, key: &str, value: String) {
        self.set_property(key, value);
    }

    fn __delitem__(&mut self, key: &str) {
        self.clear_property(key);
    }
}

/// A color in RGBA format, but serialized and deserialized as an i32.
/// This makes little sense, but it is the format used by the QuantStudio
/// files.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Color {
    r: u8,
    g: u8,
    b: u8,
    a: u8,
}

impl Color {
    pub fn new(r: u8, g: u8, b: u8, a: u8) -> Self {
        Self { r, g, b, a }
    }

    pub fn rgb(r: u8, g: u8, b: u8) -> Self {
        Self { r, g, b, a: 255 }
    }

    pub fn with_a(self, a: u8) -> Self {
        Self { a, ..self }
    }

    pub fn rgba(r: u8, g: u8, b: u8, a: u8) -> Self {
        Self { r, g, b, a }
    }

    pub fn to_rgba(&self) -> (u8, u8, u8, u8) {
        (self.r, self.g, self.b, self.a)
    }

    pub fn to_hex(&self) -> String {
        format!("#{:02X}{:02X}{:02X}{:02X}", self.r, self.g, self.b, self.a)
    }
}

impl Serialize for Color {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let value = i32::from_le_bytes([self.r, self.g, self.b, self.a]);
        serializer.serialize_i32(value)
    }
}

impl<'de> Deserialize<'de> for Color {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = i32::deserialize(deserializer)?;
        let [r, g, b, a] = value.to_le_bytes();
        Ok(Color { r, g, b, a })
    }
}

impl TryFrom<String> for Color {
    type Error = String;
    fn try_from(color: String) -> Result<Self, Self::Error> {
        if let [r, g, b, a] = color.as_bytes() {
            Ok(Self { r: r.to_owned(), g: g.to_owned(), b: b.to_owned(), a: a.to_owned() })
        } else {
            Err(format!("Invalid color: {}", color))
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct CustomProperty {
    #[serde(rename = "Property")]
    pub property: String,
    #[serde(rename = "Value")]
    pub value: String,
}


impl PlateSetup {
    /// Clean up malformed XML by removing duplicate Property and Value tags within CustomProperty elements
    fn clean_xml(xml: &str) -> String {
        use regex::Regex;
        
        // Pattern to match CustomProperty blocks with duplicate Property/Value tags
        let custom_property_re = Regex::new(
            r"<CustomProperty>\s*(?:<Property>([^<]*)</Property>\s*)*<Property>([^<]*)</Property>\s*(?:<Value>([^<]*)</Value>\s*)*<Value>([^<]*)</Value>\s*</CustomProperty>"
        ).unwrap();
        
        custom_property_re.replace_all(xml, |caps: &regex::Captures| {
            let property = caps.get(2).map_or("", |m| m.as_str());
            let value = caps.get(4).map_or("", |m| m.as_str());
            format!("<CustomProperty><Property>{}</Property><Value>{}</Value></CustomProperty>", property, value)
        }).to_string()
    }

    pub fn from_xml(xml: &str) -> Result<Self, quick_xml::DeError> {
        let cleaned_xml = Self::clean_xml(xml);
        let mut plate: PlateSetup = from_str(&cleaned_xml)?;

        // Determine plate type from PlateKind
        if let Some(kind) = plate.plate_kinds.first() {
            plate.plate_type = kind.kind_type;
        } else {
            return Err(quick_xml::DeError::Custom("Missing plate kind".into()));
        }

        Ok(plate)
    }

    pub fn to_xml(&self) -> Result<String, quick_xml::SeError> {
        to_string(self)
    }

    // Helper method to get well names based on plate type
    pub fn well_names(&self) -> Vec<String> {
        let (rows, cols) = match self.plate_type {
            PlateType::Well96 => ("ABCDEFGH", 12),
            PlateType::Well384 => ("ABCDEFGHIJKLMNOP", 24),
        };

        rows.chars()
            .flat_map(|r| (1..=cols).map(move |c| format!("{}{}", r, c)))
            .collect()
    }

    /// Get wells for each sample, and the sample itself.  Return is a hashmap of sample name to (sample, wells).
    pub fn get_sample_wells(&self) -> HashMap<String, (Sample, Vec<String>)> {
        let well_names = self.well_names();
        let mut sample_wells: HashMap<String, (Sample, Vec<String>)> = HashMap::new();

        for feature_map in &self.feature_maps {
            if feature_map.feature.id == "sample" {
                for value in &feature_map.feature_values {
                    if let Some(well_name) = well_names.get(value.index as usize) {
                        if let Some(sample) = &value.feature_item.sample {
                            if let Some(existing) = sample_wells.get_mut(&sample.name) {
                                existing.1.push(well_name.clone());
                            } else {
                                sample_wells.insert(sample.name.clone(), (sample.clone(), vec![well_name.clone()]));
                            }
                        }
                    }
                }
            }
        }
        sample_wells
    }

    /// Get the well sample names as a 1D array, row-major order (eg, A1, A2, ..., H12, A13, A14, ..., H24).
    /// Empty wells are empty strings.
    pub fn well_samples_as_array(&self) -> Vec<String> {
        let well_names = self.well_names();
        let well_sample: HashMap<String, String> = self
            .get_sample_wells()
            .into_iter()
            .flat_map(|(sample_name, (_, wells))| {
                wells.into_iter().map(move |well| (well, sample_name.clone()))
            })
            .collect();

        well_names
            .into_iter()
            .map(|well| well_sample.get(&well).cloned().unwrap_or_default())
            .collect()
    }

    /// Convert plate setup to InfluxDB line protocol format
    ///
    /// # Arguments
    /// * `timestamp` - Unix timestamp in nanoseconds
    /// * `run_name` - Optional run name to include in the tags
    pub fn to_lineprotocol(
        &self,
        timestamp: i64,
        run_name: Option<&str>,
        machine_name: Option<&str>,
    ) -> Vec<String> {
        let well_sample = self
            .get_sample_wells()
            .into_iter()
            .flat_map(|(sample_name, (sample, wells))| wells.into_iter().map(move |well| (well, (sample_name.clone(), sample.clone()))))
            .collect::<HashMap<String, (String, Sample)>>();

        let (rows, cols) = match self.plate_type {
            PlateType::Well96 => ("ABCDEFGH", 12),
            PlateType::Well384 => ("ABCDEFGHIJKLMNOP", 24),
        };

        let run_tag = run_name.map_or(String::new(), |name| format!(",run_name=\"{}\"", name));
        let machine_tag =
            machine_name.map_or(String::new(), |name| format!(",machine_name=\"{}\"", name));
        let well_sample_ref = &well_sample;

        let run_tag_ref = &run_tag;
        let machine_tag_ref = &machine_tag;

        rows.chars()
            .flat_map(|row| {
                (1..=cols).map(move |col| {
                    let well = format!("{}{}", row, col);
                    let sample = well_sample_ref
                        .get(&well)
                        .map_or("", |s| s.0.as_str())
                        .to_string();
                    format!(
                        "platesetup,row={},col={:02}{}{} sample=\"{}\" {}",
                        row, col, run_tag_ref, machine_tag_ref, sample, timestamp
                    )
                })
            })
            .collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(feature = "python", pyclass)]
pub enum PlateType {
    #[default]
    Well96,
    Well384,
}

impl Serialize for PlateType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(match self {
            PlateType::Well96 => "TYPE_8X12",
            PlateType::Well384 => "TYPE_16X24",
        })
    }
}

impl<'de> Deserialize<'de> for PlateType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "TYPE_8X12" => Ok(PlateType::Well96),
            "TYPE_16X24" => Ok(PlateType::Well384),
            _ => Err(D::Error::custom("Invalid plate type")),
        }
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl PlateSetup {
    #[new]
    #[pyo3(signature = (name=None, plate_type=None))]
    fn new(name: Option<String>, plate_type: Option<String>) -> PyResult<Self> {

        let plate_type = match plate_type.as_deref().unwrap_or("TYPE_8X12") {
            "TYPE_8X12" => PlateType::Well96,
            "TYPE_16X24" => PlateType::Well384,
            _ => return Err(pyo3::exceptions::PyValueError::new_err("Invalid plate type")),
        };

        let (rows, columns) = match plate_type {
            PlateType::Well96 => (8, 12),
            PlateType::Well384 => (16, 24),
        };

        Ok(PlateSetup {
            name,
            barcode: None,
            description: None,
            rows,
            columns,
            plate_kinds: vec![PlateKind {
                name: match plate_type {
                    PlateType::Well96 => "96-Well Plate (8x12)".to_string(),
                    PlateType::Well384 => "384-Well Plate (16x24)".to_string(),
                },
                kind_type: plate_type,
                row_count: rows,
                column_count: columns,
            }],
            feature_maps: vec![],
            plate_type,
            wells: vec![],
            multi_zone_enabled: None,
            logical_zones: vec![],
            passive_reference_dye: None,
        })
    }

    fn print_debug(&self) {
        println!("{:?}", self);
    }

    #[staticmethod]
    fn from_xml_string(xml: &str) -> PyResult<Self> {
        PlateSetup::from_xml(xml).map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
    }

    fn to_xml_string(&self) -> PyResult<String> {
        self.to_xml().map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
    }

    #[getter]
    fn name(&self) -> Option<String> {
        self.name.clone()
    }

    #[setter]
    fn set_name(&mut self, name: Option<String>) {
        self.name = name;
    }

    #[getter]
    fn barcode(&self) -> Option<String> {
        self.barcode.clone()
    }

    #[setter]
    fn set_barcode(&mut self, barcode: Option<String>) {
        self.barcode = barcode;
    }

    #[getter]
    fn description(&self) -> Option<String> {
        self.description.clone()
    }

    #[setter]
    fn set_description(&mut self, description: Option<String>) {
        self.description = description;
    }

    #[getter]
    fn rows(&self) -> u32 {
        self.rows
    }

    #[getter]
    fn columns(&self) -> u32 {
        self.columns
    }

    #[getter]
    fn get_plate_type(&self) -> String {
        match self.plate_type {
            PlateType::Well96 => "TYPE_8X12".to_string(),
            PlateType::Well384 => "TYPE_16X24".to_string(),
        }
    }

    #[setter]
    fn set_plate_type(&mut self, plate_type: String) -> PyResult<()> {
        self.plate_type = match plate_type.as_str() {
            "TYPE_8X12" => PlateType::Well96,
            "TYPE_16X24" => PlateType::Well384,
            "96" => PlateType::Well96,
            "384" => PlateType::Well384,
            _ => return Err(pyo3::exceptions::PyValueError::new_err("Invalid plate type")),
        };
        Ok(())
    }

    fn get_well_names(&self) -> Vec<String> {
        self.well_names()
    }

    fn get_samples_and_wells(&self) -> std::collections::HashMap<String, (Sample, Vec<String>)> {
        self.get_sample_wells()
    }

    /// Get a specific sample by name
    fn get_sample(&self, name: &str) -> Option<Sample> {
        for feature_map in &self.feature_maps {
            if feature_map.feature.id == "sample" {
                for value in &feature_map.feature_values {
                    if let Some(sample) = &value.feature_item.sample {
                        if sample.name == name {
                            return Some(sample.clone());
                        }
                    }
                }
            }
        }
        None
    }

    fn to_line_protocol(&self, timestamp: i64, run_name: Option<&str>, machine_name: Option<&str>) -> Vec<String> {
        self.to_lineprotocol(timestamp, run_name, machine_name)
    }

    fn __repr__(&self) -> String {
        format!("PlateSetup(name={:?}, plate_type={}, rows={}, columns={})", 
                self.name, self.get_plate_type(), self.rows, self.columns)
    }

    fn __str__(&self) -> String {
        let sample_wells = self.get_sample_wells();
        if sample_wells.is_empty() {
            format!("Empty PlateSetup ({} wells)", self.rows * self.columns)
        } else {
            let mut result = format!("PlateSetup with {} samples:\n", sample_wells.len());
            for (sample, wells) in sample_wells.iter() {
                result.push_str(&format!("  {}: {} wells\n", sample, wells.1.len()));
            }
            result
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::path::{Path, PathBuf};
    use zip::ZipArchive;

    #[test]
    fn test_deserialize_plate_setup() {
        let xml = r#"
    <Plate>
        <Name>Test Plate</Name>
        <BarCode>BC123</BarCode>
        <Description>Test Description</Description>
        <Rows>8</Rows>
        <Columns>12</Columns>
        <FeatureMap>
            <Feature>
                <Id>sample</Id>
                <Name>Sample</Name>
            </Feature>
            <FeatureValue>
                <Index>0</Index>
                <FeatureItem>
                    <Sample>
                        <Name>Test Sample</Name>
                        <Color>-16776961</Color>
                        <CustomProperty>
                            <Property>SP_UUID</Property>
                            <Value>f29793389d7511efbfaeb88584b13f7c</Value>
                        </CustomProperty>
                    </Sample>
                </FeatureItem>
            </FeatureValue>
        </FeatureMap>
        <PlateKind>
            <Name>96-Well Plate (8x12)</Name>
            <Type>TYPE_8X12</Type>
            <RowCount>8</RowCount>
            <ColumnCount>12</ColumnCount>
        </PlateKind>
    </Plate>
    "#;

        let result = PlateSetup::from_xml(xml);
        assert!(result.is_ok());
        let plate = result.unwrap();
        assert_eq!(plate.plate_type, PlateType::Well96);

        // Test sample data
        let sample_wells = plate.get_sample_wells();
        assert!(sample_wells.contains_key("Test Sample"));
        assert_eq!(sample_wells["Test Sample"].1, vec!["A1"]);
    }

    #[test]
    fn test_plate_type_detection() {
        let xml_96 = r#"
        <Plate>
            <Name>Test Plate</Name>
            <BarCode>BC123</BarCode>
            <Description>Test Description</Description>
            <Rows>8</Rows>
            <Columns>12</Columns>
            <PlateKind>
                <Name>96-Well Plate (8x12)</Name>
                <Type>TYPE_8X12</Type>
                <RowCount>8</RowCount>
                <ColumnCount>12</ColumnCount>
            </PlateKind>
        </Plate>
        "#;

        let plate_96 = PlateSetup::from_xml(xml_96).unwrap();
        assert_eq!(plate_96.plate_type, PlateType::Well96);
        let xml_384 = r#"
        <Plate>
            <Name>Test Plate</Name>
            <BarCode>BC123</BarCode>
            <Description>Test Description</Description>
            <Rows>16</Rows>
            <Columns>24</Columns>
            <PlateKind>
                <Name>384-Well Plate (16x24)</Name>
                <Type>TYPE_16X24</Type>
                <RowCount>16</RowCount>
                <ColumnCount>24</ColumnCount>
            </PlateKind>
        </Plate>
        "#;

        let plate_384 = PlateSetup::from_xml(xml_384).unwrap();
        assert_eq!(plate_384.plate_type, PlateType::Well384);
    }

    #[test]
    fn test_well_names() {
        let xml = r#"
        <Plate>
            <Name />
            <BarCode />
            <Description />
            <Rows>8</Rows>
            <Columns>12</Columns>
            <PlateKind>
                <Name>96-Well Plate (8x12)</Name>
                <Type>TYPE_8X12</Type>
                <RowCount>8</RowCount>
                <ColumnCount>12</ColumnCount>
            </PlateKind>
            <FeatureMap>
                <Feature>
                    <Id>sample</Id>
                    <Name>Sample</Name>
                </Feature>
                <FeatureValue>
                    <Index>0</Index>
                    <FeatureItem>
                        <Sample>
                            <Name>Test Sample</Name>
                            <Color>-16776961</Color>
                            <CustomProperty>
                                <Property>SP_UUID</Property>
                                <Value>f29793389d7511efbfaeb88584b13f7c</Value>
                            </CustomProperty>
                        </Sample>
                    </FeatureItem>
                </FeatureValue>
            </FeatureMap>
        </Plate>
        "#;

        let plate = PlateSetup::from_xml(xml).unwrap();
        let well_names = plate.well_names();
        assert_eq!(well_names[0], "A1");
        assert_eq!(well_names[95], "H12");
        assert_eq!(well_names.len(), 96);
    }

    #[test]
    fn test_sample_serialization() {
        let sample = Sample::new("Test Sample".to_string())
            .with_color(Color::rgb(255, 128, 64))
            .with_property("SP_UUID", "f29793389d7511efbfaeb88584b13f7c".to_string());
        let serialized = to_string(&sample).unwrap();
        let deserialized: Sample = from_str(&serialized).unwrap();
        assert_eq!(sample.color.to_rgba(), deserialized.color.to_rgba());
        assert_eq!(sample.name, deserialized.name);
        assert_eq!(
            sample.get_property("SP_UUID").unwrap(),
            deserialized.get_property("SP_UUID").unwrap()
        );
    }

    #[test]
    fn test_get_sample_wells() {
        let xml = r#"
        <Plate>
            <Name>Test Plate</Name>
            <BarCode>BC123</BarCode>
            <Description>Test Description</Description>
            <Rows>8</Rows>
            <Columns>12</Columns>
            <PlateKind>
                <Name>96-Well Plate (8x12)</Name>
                <Type>TYPE_8X12</Type>
                <RowCount>8</RowCount>
                <ColumnCount>12</ColumnCount>
            </PlateKind>
            <FeatureMap>
                <Feature>
                    <Id>sample</Id>
                    <Name>Sample</Name>
                </Feature>
                <FeatureValue>
                    <Index>0</Index>
                    <FeatureItem>
                        <Sample>
                            <Name>Sample1</Name>
                            <Color>-16776961</Color>
                            <CustomProperty>
                                <Property>SP_UUID</Property>
                                <Value>uuid1</Value>
                            </CustomProperty>
                        </Sample>
                    </FeatureItem>
                </FeatureValue>
                <FeatureValue>
                    <Index>1</Index>
                    <FeatureItem>
                        <Sample>
                            <Name>Sample1</Name>
                            <Color>-16776961</Color>
                            <CustomProperty>
                                <Property>SP_UUID</Property>
                                <Value>uuid1</Value>
                            </CustomProperty>
                        </Sample>
                    </FeatureItem>
                </FeatureValue>
                <FeatureValue>
                    <Index>12</Index>
                    <FeatureItem>
                        <Sample>
                            <Name>Sample2</Name>
                            <Color>-16776961</Color>
                            <CustomProperty>
                                <Property>SP_UUID</Property>
                                <Value>uuid2</Value>
                            </CustomProperty>
                        </Sample>
                    </FeatureItem>
                </FeatureValue>
            </FeatureMap>
        </Plate>
        "#;

        let plate = PlateSetup::from_xml(xml).unwrap();
        let sample_wells = plate.get_sample_wells();

        // Test that we have the expected number of samples
        assert_eq!(sample_wells.len(), 2);

        // Test Sample1 wells
        assert!(sample_wells.contains_key("Sample1"));
        let sample1_wells = &sample_wells["Sample1"];
        assert_eq!(sample1_wells.1.len(), 2);
        assert!(sample1_wells.1.contains(&"A1".to_string()));
        assert!(sample1_wells.1.contains(&"A2".to_string()));

        // Test Sample2 wells
        assert!(sample_wells.contains_key("Sample2"));
        let sample2_wells = &sample_wells["Sample2"];
        assert_eq!(sample2_wells.1.len(), 1);
        assert!(sample2_wells.1.contains(&"B1".to_string()));
    }

    #[test]
    fn test_get_sample_wells_384() {
        let xml = r#"
        <Plate>
            <Name>Test Plate 384</Name>
            <BarCode>BC123</BarCode>
            <Description>Test Description</Description>
            <Rows>16</Rows>
            <Columns>24</Columns>
            <PlateKind>
                <Name>384-Well Plate (16x24)</Name>
                <Type>TYPE_16X24</Type>
                <RowCount>16</RowCount>
                <ColumnCount>24</ColumnCount>
            </PlateKind>
            <FeatureMap>
                <Feature>
                    <Id>sample</Id>
                    <Name>Sample</Name>
                </Feature>
                <FeatureValue>
                    <Index>0</Index>
                    <FeatureItem>
                        <Sample>
                            <Name>Sample1</Name>
                            <Color>-16776961</Color>
                            <CustomProperty>
                                <Property>SP_UUID</Property>
                                <Value>uuid1</Value>
                            </CustomProperty>
                        </Sample>
                    </FeatureItem>
                </FeatureValue>
                <FeatureValue>
                    <Index>23</Index>
                    <FeatureItem>
                        <Sample>
                            <Name>Sample1</Name>
                            <Color>-16776961</Color>
                            <CustomProperty>
                                <Property>SP_UUID</Property>
                                <Value>uuid1</Value>
                            </CustomProperty>
                        </Sample>
                    </FeatureItem>
                </FeatureValue>
                <FeatureValue>
                    <Index>383</Index>
                    <FeatureItem>
                        <Sample>
                            <Name>Sample2</Name>
                            <Color>-16776961</Color>
                            <CustomProperty>
                                <Property>SP_UUID</Property>
                                <Value>uuid2</Value>
                            </CustomProperty>
                        </Sample>
                    </FeatureItem>
                </FeatureValue>
            </FeatureMap>
        </Plate>
        "#;

        let plate = PlateSetup::from_xml(xml).unwrap();
        let sample_wells = plate.get_sample_wells();

        // Test that we have the expected number of samples
        assert_eq!(sample_wells.len(), 2);

        // Test Sample1 wells
        assert!(sample_wells.contains_key("Sample1"));
        let sample1_wells = &sample_wells["Sample1"];
        assert_eq!(sample1_wells.1.len(), 2);
        assert!(sample1_wells.1.contains(&"A1".to_string()));
        assert!(sample1_wells.1.contains(&"A24".to_string()));

        assert!(sample_wells.contains_key("Sample2"));
        let sample2_wells = &sample_wells["Sample2"];
        assert_eq!(sample2_wells.1.len(), 1);
    }

    #[test]
    fn test_get_sample_wells_empty() {
        let xml = r#"
        <Plate>
            <Name>Empty Plate</Name>
            <BarCode>BC123</BarCode>
            <Description>Test Description</Description>
            <Rows>8</Rows>
            <Columns>12</Columns>
            <PlateKind>
                <Name>96-Well Plate (8x12)</Name>
                <Type>TYPE_8X12</Type>
                <RowCount>8</RowCount>
                <ColumnCount>12</ColumnCount>
            </PlateKind>
            <FeatureMap>
                <Feature>
                    <Id>sample</Id>
                    <Name>Sample</Name>
                </Feature>
            </FeatureMap>
        </Plate>
        "#;

        let plate = PlateSetup::from_xml(xml).unwrap();
        let sample_wells = plate.get_sample_wells();

        // Test that we have no samples
        assert_eq!(sample_wells.len(), 0);
    }

    #[test]
    fn test_to_lineprotocol() {
        let xml = r#"
        <Plate>
            <Name>Test Plate</Name>
            <BarCode>BC123</BarCode>
            <Description>Test Description</Description>
            <Rows>8</Rows>
            <Columns>12</Columns>
            <PlateKind>
                <Name>96-Well Plate (8x12)</Name>
                <Type>TYPE_8X12</Type>
                <RowCount>8</RowCount>
                <ColumnCount>12</ColumnCount>
            </PlateKind>
            <FeatureMap>
                <Feature>
                    <Id>sample</Id>
                    <Name>Sample</Name>
                </Feature>
                <FeatureValue>
                    <Index>0</Index>
                    <FeatureItem>
                        <Sample>
                            <Name>Sample1</Name>
                            <Color>-16776961</Color>
                            <CustomProperty>
                                <Property>SP_UUID</Property>
                                <Value>uuid1</Value>
                            </CustomProperty>
                        </Sample>
                    </FeatureItem>
                </FeatureValue>
            </FeatureMap>
        </Plate>
        "#;

        let plate = PlateSetup::from_xml(xml).unwrap();
        let lines = plate.to_lineprotocol(1234567890, None, None);

        // Check total number of lines (96-well plate)
        assert_eq!(lines.len(), 96);

        // Check first line (A1 with Sample1)
        assert_eq!(
            lines[0],
            "platesetup,row=A,col=01 sample=\"Sample1\" 1234567890"
        );

        // Check a random empty well (H12)
        assert_eq!(lines[95], "platesetup,row=H,col=12 sample=\"\" 1234567890");

        // Test with run name
        let lines_with_run = plate.to_lineprotocol(1234567890, Some("Test Run"), None);
        assert_eq!(
            lines_with_run[0],
            "platesetup,row=A,col=01,run_name=\"Test Run\" sample=\"Sample1\" 1234567890"
        );
    }

    #[test]
    fn test_to_lineprotocol_384() {
        let xml = r#"
        <Plate>
            <Name>Test Plate 384</Name>
            <BarCode>BC123</BarCode>
            <Description>Test Description</Description>
            <Rows>16</Rows>
            <Columns>24</Columns>
            <PlateKind>
                <Name>384-Well Plate (16x24)</Name>
                <Type>TYPE_16X24</Type>
                <RowCount>16</RowCount>
                <ColumnCount>24</ColumnCount>
            </PlateKind>
            <FeatureMap>
                <Feature>
                    <Id>sample</Id>
                    <Name>Sample</Name>
                </Feature>
                <FeatureValue>
                    <Index>0</Index>
                    <FeatureItem>
                        <Sample>
                            <Name>Sample1</Name>
                            <Color>-16776961</Color>
                            <CustomProperty>
                                <Property>SP_UUID</Property>
                                <Value>uuid1</Value>
                            </CustomProperty>
                        </Sample>
                    </FeatureItem>
                </FeatureValue>
            </FeatureMap>
        </Plate>
        "#;

        let plate = PlateSetup::from_xml(xml).unwrap();
        let lines = plate.to_lineprotocol(1234567890, None, None);

        // Check total number of lines (384-well plate)
        assert_eq!(lines.len(), 384);

        // Check first line (A1 with Sample1)
        assert_eq!(
            lines[0],
            "platesetup,row=A,col=01 sample=\"Sample1\" 1234567890"
        );

        // Check last well (P24)
        assert_eq!(lines[383], "platesetup,row=P,col=24 sample=\"\" 1234567890");
    }

    #[test]
    fn test_deserialize_plate_setup_with_duplicate_sp_uuid() {
        // Test XML with duplicate SP_UUID Property and Value tags (from Python bug)
        let xml = r#"
    <Plate>
        <Name>Test Plate</Name>
        <BarCode>BC123</BarCode>
        <Description>Test Description</Description>
        <Rows>8</Rows>
        <Columns>12</Columns>
        <FeatureMap>
            <Feature>
                <Id>sample</Id>
                <Name>Sample</Name>
            </Feature>
            <FeatureValue>
                <Index>0</Index>
                <FeatureItem>
                    <Sample>
                        <Name>Test Sample</Name>
                        <Color>-16776961</Color>
                        <CustomProperty>
                            <Property>SP_UUID</Property>
                            <Property>SP_UUID</Property>
                            <Value>f29793389d7511efbfaeb88584b13f7c</Value>
                            <Value>f29793389d7511efbfaeb88584b13f7c</Value>
                        </CustomProperty>
                    </Sample>
                </FeatureItem>
            </FeatureValue>
        </FeatureMap>
        <PlateKind>
            <Name>96-Well Plate (8x12)</Name>
            <Type>TYPE_8X12</Type>
            <RowCount>8</RowCount>
            <ColumnCount>12</ColumnCount>
        </PlateKind>
    </Plate>
    "#;

        let result = PlateSetup::from_xml(xml);
        assert!(result.is_ok(), "Should handle duplicate SP_UUID tags gracefully");
        let plate = result.unwrap();
        assert_eq!(plate.plate_type, PlateType::Well96);

        // Test sample data
        let sample_wells = plate.get_sample_wells();
        assert!(sample_wells.contains_key("Test Sample"));
        assert_eq!(sample_wells["Test Sample"].1, vec!["A1"]);
    }

    #[test]
    fn test_parse_example_eds_files() {
        let example_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");

        // Only test "mid-run.eds" and "test.eds"
        let eds_files: Vec<PathBuf> = ["mid-run.eds", "test.eds"]
            .iter()
            .map(|name| example_dir.join(name))
            .filter(|path| path.exists())
            .collect();

        if eds_files.is_empty() {
            return;
        }

        for eds_path in eds_files {
            let file = File::open(&eds_path)
                .unwrap_or_else(|e| panic!("Failed to open {}: {}", eds_path.display(), e));

            let mut archive = ZipArchive::new(file)
                .unwrap_or_else(|e| panic!("Failed to read {} as zip: {}", eds_path.display(), e));

            // Try to find and read the plate setup XML file
            let mut plate_setup_xml = archive
                .by_name("apldbio/sds/plate_setup.xml")
                .unwrap_or_else(|e| {
                    panic!(
                        "Failed to find plate_setup.xml in {}: {}",
                        eds_path.display(),
                        e
                    )
                });

            let mut xml_content = String::new();
            std::io::Read::read_to_string(&mut plate_setup_xml, &mut xml_content).unwrap_or_else(
                |e| {
                    panic!(
                        "Failed to read plate_setup.xml content from {}: {}",
                        eds_path.display(),
                        e
                    )
                },
            );

            // Try to parse the XML
            let result = PlateSetup::from_xml(&xml_content);
            assert!(
                result.is_ok(),
                "Failed to parse plate_setup.xml from {}: {:?}",
                eds_path.display(),
                result.err()
            );

            // Additional validation of the parsed plate setup
            let plate = result.unwrap();

            // Verify plate type matches dimensions
            match plate.plate_type {
                PlateType::Well96 => {
                    assert_eq!(
                        plate.rows,
                        8,
                        "96-well plate should have 8 rows in {}",
                        eds_path.display()
                    );
                    assert_eq!(
                        plate.columns,
                        12,
                        "96-well plate should have 12 columns in {}",
                        eds_path.display()
                    );
                }
                PlateType::Well384 => {
                    assert_eq!(
                        plate.rows,
                        16,
                        "384-well plate should have 16 rows in {}",
                        eds_path.display()
                    );
                    assert_eq!(
                        plate.columns,
                        24,
                        "384-well plate should have 24 columns in {}",
                        eds_path.display()
                    );
                }
            }

            // Test round-trip serialization
            let serialized = plate.to_xml().unwrap_or_else(|e| {
                panic!(
                    "Failed to serialize plate setup from {}: {}",
                    eds_path.display(),
                    e
                )
            });

            let reparse_result = PlateSetup::from_xml(&serialized);
            assert!(
                reparse_result.is_ok(),
                "Failed to reparse serialized plate setup from {}: {:?}",
                eds_path.display(),
                reparse_result.err()
            );
        }
    }

    #[test]
    fn test_well_samples_as_array_empty() {
        let xml = r#"
        <Plate>
            <Name>Empty Plate</Name>
            <BarCode>BC123</BarCode>
            <Description>Test Description</Description>
            <Rows>8</Rows>
            <Columns>12</Columns>
            <PlateKind>
                <Name>96-Well Plate (8x12)</Name>
                <Type>TYPE_8X12</Type>
                <RowCount>8</RowCount>
                <ColumnCount>12</ColumnCount>
            </PlateKind>
            <FeatureMap>
                <Feature>
                    <Id>sample</Id>
                    <Name>Sample</Name>
                </Feature>
            </FeatureMap>
        </Plate>
        "#;

        let plate = PlateSetup::from_xml(xml).unwrap();
        let samples = plate.well_samples_as_array();

        assert_eq!(samples.len(), 96);
        assert!(samples.iter().all(|s| s.is_empty()));
    }

    #[test]
    fn test_well_samples_as_array_96() {
        let xml = r#"
        <Plate>
            <Name>Test Plate</Name>
            <BarCode>BC123</BarCode>
            <Description>Test Description</Description>
            <Rows>8</Rows>
            <Columns>12</Columns>
            <PlateKind>
                <Name>96-Well Plate (8x12)</Name>
                <Type>TYPE_8X12</Type>
                <RowCount>8</RowCount>
                <ColumnCount>12</ColumnCount>
            </PlateKind>
            <FeatureMap>
                <Feature>
                    <Id>sample</Id>
                    <Name>Sample</Name>
                </Feature>
                <FeatureValue>
                    <Index>0</Index>
                    <FeatureItem>
                        <Sample>
                            <Name>Sample1</Name>
                            <Color>-16776961</Color>
                            <CustomProperty>
                                <Property>SP_UUID</Property>
                                <Value>uuid1</Value>
                            </CustomProperty>
                        </Sample>
                    </FeatureItem>
                </FeatureValue>
                <FeatureValue>
                    <Index>1</Index>
                    <FeatureItem>
                        <Sample>
                            <Name>Sample1</Name>
                            <Color>-16776961</Color>
                            <CustomProperty>
                                <Property>SP_UUID</Property>
                                <Value>uuid1</Value>
                            </CustomProperty>
                        </Sample>
                    </FeatureItem>
                </FeatureValue>
                <FeatureValue>
                    <Index>12</Index>
                    <FeatureItem>
                        <Sample>
                            <Name>Sample2</Name>
                            <Color>-16776961</Color>
                            <CustomProperty>
                                <Property>SP_UUID</Property>
                                <Value>uuid2</Value>
                            </CustomProperty>
                        </Sample>
                    </FeatureItem>
                </FeatureValue>
            </FeatureMap>
        </Plate>
        "#;

        let plate = PlateSetup::from_xml(xml).unwrap();
        let samples = plate.well_samples_as_array();

        assert_eq!(samples.len(), 96);
        assert_eq!(samples[0], "Sample1");
        assert_eq!(samples[1], "Sample1");
        assert_eq!(samples[12], "Sample2");
        assert_eq!(samples[2], "");
        assert_eq!(samples[11], "");
        assert_eq!(samples[13], "");
        assert_eq!(samples[95], "");
    }

    #[test]
    fn test_well_samples_as_array_384() {
        let xml = r#"
        <Plate>
            <Name>Test Plate 384</Name>
            <BarCode>BC123</BarCode>
            <Description>Test Description</Description>
            <Rows>16</Rows>
            <Columns>24</Columns>
            <PlateKind>
                <Name>384-Well Plate (16x24)</Name>
                <Type>TYPE_16X24</Type>
                <RowCount>16</RowCount>
                <ColumnCount>24</ColumnCount>
            </PlateKind>
            <FeatureMap>
                <Feature>
                    <Id>sample</Id>
                    <Name>Sample</Name>
                </Feature>
                <FeatureValue>
                    <Index>0</Index>
                    <FeatureItem>
                        <Sample>
                            <Name>Sample1</Name>
                            <Color>-16776961</Color>
                            <CustomProperty>
                                <Property>SP_UUID</Property>
                                <Value>uuid1</Value>
                            </CustomProperty>
                        </Sample>
                    </FeatureItem>
                </FeatureValue>
                <FeatureValue>
                    <Index>23</Index>
                    <FeatureItem>
                        <Sample>
                            <Name>Sample1</Name>
                            <Color>-16776961</Color>
                            <CustomProperty>
                                <Property>SP_UUID</Property>
                                <Value>uuid1</Value>
                            </CustomProperty>
                        </Sample>
                    </FeatureItem>
                </FeatureValue>
                <FeatureValue>
                    <Index>24</Index>
                    <FeatureItem>
                        <Sample>
                            <Name>Sample2</Name>
                            <Color>-16776961</Color>
                            <CustomProperty>
                                <Property>SP_UUID</Property>
                                <Value>uuid2</Value>
                            </CustomProperty>
                        </Sample>
                    </FeatureItem>
                </FeatureValue>
                <FeatureValue>
                    <Index>383</Index>
                    <FeatureItem>
                        <Sample>
                            <Name>Sample3</Name>
                            <Color>-16776961</Color>
                            <CustomProperty>
                                <Property>SP_UUID</Property>
                                <Value>uuid3</Value>
                            </CustomProperty>
                        </Sample>
                    </FeatureItem>
                </FeatureValue>
            </FeatureMap>
        </Plate>
        "#;

        let plate = PlateSetup::from_xml(xml).unwrap();
        let samples = plate.well_samples_as_array();

        assert_eq!(samples.len(), 384);
        assert_eq!(samples[0], "Sample1");
        assert_eq!(samples[23], "Sample1");
        assert_eq!(samples[24], "Sample2");
        assert_eq!(samples[383], "Sample3");
        assert_eq!(samples[1], "");
        assert_eq!(samples[22], "");
        assert_eq!(samples[25], "");
        assert_eq!(samples[382], "");
    }

    #[test]
    fn test_well_samples_as_array_row_major_order() {
        let xml = r#"
        <Plate>
            <Name>Test Plate</Name>
            <BarCode>BC123</BarCode>
            <Description>Test Description</Description>
            <Rows>8</Rows>
            <Columns>12</Columns>
            <PlateKind>
                <Name>96-Well Plate (8x12)</Name>
                <Type>TYPE_8X12</Type>
                <RowCount>8</RowCount>
                <ColumnCount>12</ColumnCount>
            </PlateKind>
            <FeatureMap>
                <Feature>
                    <Id>sample</Id>
                    <Name>Sample</Name>
                </Feature>
                <FeatureValue>
                    <Index>0</Index>
                    <FeatureItem>
                        <Sample>
                            <Name>A1</Name>
                            <Color>-16776961</Color>
                            <CustomProperty>
                                <Property>SP_UUID</Property>
                                <Value>uuid1</Value>
                            </CustomProperty>
                        </Sample>
                    </FeatureItem>
                </FeatureValue>
                <FeatureValue>
                    <Index>11</Index>
                    <FeatureItem>
                        <Sample>
                            <Name>A12</Name>
                            <Color>-16776961</Color>
                            <CustomProperty>
                                <Property>SP_UUID</Property>
                                <Value>uuid2</Value>
                            </CustomProperty>
                        </Sample>
                    </FeatureItem>
                </FeatureValue>
                <FeatureValue>
                    <Index>12</Index>
                    <FeatureItem>
                        <Sample>
                            <Name>B1</Name>
                            <Color>-16776961</Color>
                            <CustomProperty>
                                <Property>SP_UUID</Property>
                                <Value>uuid3</Value>
                            </CustomProperty>
                        </Sample>
                    </FeatureItem>
                </FeatureValue>
                <FeatureValue>
                    <Index>95</Index>
                    <FeatureItem>
                        <Sample>
                            <Name>H12</Name>
                            <Color>-16776961</Color>
                            <CustomProperty>
                                <Property>SP_UUID</Property>
                                <Value>uuid4</Value>
                            </CustomProperty>
                        </Sample>
                    </FeatureItem>
                </FeatureValue>
            </FeatureMap>
        </Plate>
        "#;

        let plate = PlateSetup::from_xml(xml).unwrap();
        let samples = plate.well_samples_as_array();

        assert_eq!(samples.len(), 96);
        assert_eq!(samples[0], "A1");
        assert_eq!(samples[11], "A12");
        assert_eq!(samples[12], "B1");
        assert_eq!(samples[95], "H12");
    }
}
