use std::collections::HashMap;

use quick_xml::events::Event;
use quick_xml::name::QName;
use quick_xml::Reader;
use quick_xml::{de::from_str, se::to_string};
use serde::de::Error;
use serde::{Deserialize, Serialize};
use zip;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename = "Plate", deny_unknown_fields)]
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
}

#[derive(Debug, Serialize, Deserialize)]
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

#[derive(Debug, Serialize, Deserialize)]
pub struct FeatureMap {
    #[serde(rename = "Feature")]
    pub feature: Feature,
    #[serde(rename = "FeatureValue", default)]
    pub feature_values: Vec<FeatureValue>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Feature {
    #[serde(rename = "Id")]
    pub id: String,
    #[serde(rename = "Name")]
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FeatureValue {
    #[serde(rename = "Index")]
    pub index: u32,
    #[serde(rename = "FeatureItem")]
    pub feature_item: FeatureItem,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FeatureItem {
    #[serde(rename = "Sample")]
    pub sample: Sample,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Sample {
    #[serde(rename = "Name")]
    pub name: String,
    #[serde(rename = "Color")]
    pub color: Color,
    #[serde(rename = "CustomProperty")]
    pub custom_properties: Vec<CustomProperty>,
}

impl Sample {
    pub fn new(name: String) -> Self {
        Self {
            name,
            color: Color::rgb(100, 100, 100),
            custom_properties: vec![],
        }
    }

    pub fn with_color(mut self, color: Color) -> Self {
        self.color = color;
        self
    }

    pub fn with_custom_property(mut self, property: CustomProperty) -> Self {
        self.custom_properties.push(property);
        self
    }
}

/// A color in RGBA format, but serialized and deserialized as an i32.
/// This makes little sense, but it is the format used by the QuantStudio
/// files.
#[derive(Debug)]
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

#[derive(Debug, Serialize, Deserialize)]
pub struct CustomProperty {
    #[serde(rename = "Property")]
    pub property: Vec<String>,
    #[serde(rename = "Value")]
    pub value: Vec<String>,
}

impl PlateSetup {
    pub fn from_xml(xml: &str) -> Result<Self, quick_xml::DeError> {
        let mut plate: PlateSetup = from_str(xml)?;

        // Determine plate type from PlateKind
        if let Some(kind) = plate.plate_kinds.first() {
            plate.plate_type = match kind.kind_type {
                PlateType::Well96 => PlateType::Well96,
                PlateType::Well384 => PlateType::Well384,
                _ => return Err(quick_xml::DeError::Custom("Invalid plate type".into())),
            };
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

    // Get samples by well position
    pub fn get_sample_wells(&self) -> HashMap<String, Vec<String>> {
        let well_names = self.well_names();
        let mut sample_wells: HashMap<String, Vec<String>> = HashMap::new();

        for feature_map in &self.feature_maps {
            // Find the sample feature
            if feature_map.feature.id == "sample" {
                for value in &feature_map.feature_values {
                    if let Some(well_name) = well_names.get(value.index as usize) {
                        let sample_name = &value.feature_item.sample.name;
                        sample_wells
                            .entry(sample_name.clone())
                            .or_default()
                            .push(well_name.clone());
                    }
                }
            }
        }
        sample_wells
    }

    /// Convert plate setup to InfluxDB line protocol format
    ///
    /// # Arguments
    /// * `timestamp` - Unix timestamp in nanoseconds
    /// * `run_name` - Optional run name to include in the tags
    pub fn to_lineprotocol(&self, timestamp: i64, run_name: Option<&str>) -> Vec<String> {
        let well_sample = self
            .get_sample_wells()
            .into_iter()
            .flat_map(|(sample, wells)| wells.into_iter().map(move |well| (well, sample.clone())))
            .collect::<HashMap<_, _>>();

        let (rows, cols) = match self.plate_type {
            PlateType::Well96 => ("ABCDEFGH", 12),
            PlateType::Well384 => ("ABCDEFGHIJKLMNOP", 24),
        };

        let run_tag = run_name.map_or(String::new(), |name| format!(",run_name=\"{}\"", name));

        let well_sample_ref = &well_sample;
        let run_tag_ref = &run_tag;

        rows.chars()
            .flat_map(|row| {
                (1..=cols).map(move |col| {
                    let well = format!("{}{}", row, col);
                    let sample = well_sample_ref
                        .get(&well)
                        .map_or("", |s| s.as_str())
                        .to_string();
                    format!(
                        "platesetup,row={},col={}{} sample=\"{}\" {}",
                        row, col, run_tag_ref, sample, timestamp
                    )
                })
            })
            .collect()
    }

    fn parse_feature_map(reader: &mut Reader<&[u8]>) -> Result<FeatureMap, quick_xml::DeError> {
        let mut feature_map = FeatureMap {
            feature: Feature {
                id: String::new(),
                name: String::new(),
            },
            feature_values: Vec::new(),
        };

        let mut buf = Vec::new();
        let mut current_path = Vec::new();

        loop {
            match reader.read_event_into(&mut buf) {
                Ok(Event::Start(e)) => {
                    let name = e.name().into_inner().to_vec();
                    current_path.push(name);

                    if current_path.last().unwrap() == b"FeatureValue" {
                        let value = Self::parse_feature_value(reader)?;
                        feature_map.feature_values.push(value);
                        current_path.pop();
                    }
                }
                Ok(Event::Text(e)) => {
                    let text = e.unescape()?.into_owned();
                    match current_path.as_slice() {
                        path if path.ends_with(&[b"Feature".to_vec(), b"Id".to_vec()]) => feature_map.feature.id = text,
                        path if path.ends_with(&[b"Feature".to_vec(), b"Name".to_vec()]) => feature_map.feature.name = text,
                        _ => (),
                    }
                }
                Ok(Event::End(e)) => {
                    let name = e.name().into_inner().to_vec();
                    if name == b"FeatureMap" {
                        break;
                    }
                    current_path.pop();
                }
                Ok(Event::Eof) => {
                    return Err(quick_xml::DeError::Custom(
                        "Unexpected EOF in FeatureMap".into(),
                    ))
                }
                Err(e) => return Err(quick_xml::DeError::from(e)),
                _ => (),
            }
        }

        Ok(feature_map)
    }

    fn parse_feature_value(reader: &mut Reader<&[u8]>) -> Result<FeatureValue, quick_xml::DeError> {
        let mut feature_value = FeatureValue {
            index: 0,
            feature_item: FeatureItem {
                sample: Sample {
                    name: String::new(),
                    color: Color::rgb(0, 0, 0),
                    custom_properties: Vec::new(),
                },
            },
        };

        let mut buf = Vec::new();
        let mut current_path = Vec::new();
        let mut current_property: Option<CustomProperty> = None;

        loop {
            match reader.read_event_into(&mut buf) {
                Ok(Event::Start(e)) => {
                    let name = e.name().into_inner().to_vec();
                    current_path.push(name);

                    if e.name() == quick_xml::name::QName(b"CustomProperty") {
                        current_property = Some(CustomProperty {
                            property: Vec::new(),
                            value: Vec::new(),
                        });
                    }
                }
                Ok(Event::Text(e)) => {
                    let text = e.unescape()?.into_owned();
                    match current_path.last() {
                        Some(path) if path == b"Index" => {
                            feature_value.index = text
                                .parse()
                                .map_err(|_| quick_xml::DeError::Custom("Invalid index".into()))?;
                        }
                        Some(path) if path == b"Name" => {
                            let current = current_path.last().unwrap();
                            if current == b"Sample" {
                                feature_value.feature_item.sample.name = text;
                            }
                        }
                        Some(path) if path == b"Color" => {
                            let color_value: i32 = text.parse().map_err(|_| {
                                quick_xml::DeError::Custom("Invalid color value".into())
                            })?;
                            let [r, g, b, a] = color_value.to_le_bytes();
                            feature_value.feature_item.sample.color = Color::new(r, g, b, a);
                        }
                        Some(path) if path == b"Property" => {
                            if let Some(prop) = &mut current_property {
                                prop.property.push(text);
                            }
                        }
                        Some(path) if path == b"Value" => {
                            if let Some(prop) = &mut current_property {
                                prop.value.push(text);
                            }
                        }
                        _ => (),
                    }
                }
                Ok(Event::End(e)) => {
                    if e.name().into_inner() == b"CustomProperty" {
                        if let Some(prop) = current_property.take() {
                            feature_value
                                .feature_item
                                .sample
                                .custom_properties
                                .push(prop);
                        }
                    } else if e.name().into_inner() == b"FeatureValue" {
                        break;
                    }
                    current_path.pop();
                }
                Ok(Event::Eof) => {
                    return Err(quick_xml::DeError::Custom(
                        "Unexpected EOF in FeatureValue".into(),
                    ))
                }
                Err(e) => return Err(quick_xml::DeError::from(e)),
                _ => (),
            }
        }

        Ok(feature_value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
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
        assert_eq!(sample_wells["Test Sample"], vec!["A1"]);
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
            .with_custom_property(CustomProperty {
                property: vec!["SP_UUID".to_string()],
                value: vec!["f29793389d7511efbfaeb88584b13f7c".to_string()],
            });
        let serialized = to_string(&sample).unwrap();
        let deserialized: Sample = from_str(&serialized).unwrap();
        assert_eq!(sample.color.to_rgba(), deserialized.color.to_rgba());
        assert_eq!(sample.name, deserialized.name);
        assert_eq!(
            sample.custom_properties[0].value[0],
            deserialized.custom_properties[0].value[0]
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
        assert_eq!(sample1_wells.len(), 2);
        assert!(sample1_wells.contains(&"A1".to_string()));
        assert!(sample1_wells.contains(&"A2".to_string()));

        // Test Sample2 wells
        assert!(sample_wells.contains_key("Sample2"));
        let sample2_wells = &sample_wells["Sample2"];
        assert_eq!(sample2_wells.len(), 1);
        assert!(sample2_wells.contains(&"B1".to_string()));
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
        assert_eq!(sample1_wells.len(), 2);
        assert!(sample1_wells.contains(&"A1".to_string()));
        assert!(sample1_wells.contains(&"A24".to_string()));

        assert!(sample_wells.contains_key("Sample2"));
        let sample2_wells = &sample_wells["Sample2"];
        assert_eq!(sample2_wells.len(), 1);
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
        let lines = plate.to_lineprotocol(1234567890, None);

        // Check total number of lines (96-well plate)
        assert_eq!(lines.len(), 96);

        // Check first line (A1 with Sample1)
        assert_eq!(
            lines[0],
            "platesetup,row=A,col=1 sample=\"Sample1\" 1234567890"
        );

        // Check a random empty well (H12)
        assert_eq!(lines[95], "platesetup,row=H,col=12 sample=\"\" 1234567890");

        // Test with run name
        let lines_with_run = plate.to_lineprotocol(1234567890, Some("Test Run"));
        assert_eq!(
            lines_with_run[0],
            "platesetup,row=A,col=1,run_name=\"Test Run\" sample=\"Sample1\" 1234567890"
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
        let lines = plate.to_lineprotocol(1234567890, None);

        // Check total number of lines (384-well plate)
        assert_eq!(lines.len(), 384);

        // Check first line (A1 with Sample1)
        assert_eq!(
            lines[0],
            "platesetup,row=A,col=1 sample=\"Sample1\" 1234567890"
        );

        // Check last well (P24)
        assert_eq!(lines[383], "platesetup,row=P,col=24 sample=\"\" 1234567890");
    }

    #[test]
    fn test_parse_example_eds_files() {
        let example_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("example-eds");

        // Get all .eds files in the directory
        let eds_files: Vec<PathBuf> = std::fs::read_dir(example_dir)
            .expect("Failed to read example-eds directory")
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let path = entry.path();
                if path.extension()? == "eds" {
                    Some(path)
                } else {
                    None
                }
            })
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
}
