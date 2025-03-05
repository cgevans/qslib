use anyhow::Context;
use bstr::ByteSlice;
use lazy_static::lazy_static;
use polars::prelude::*;
#[cfg(feature = "python")]
use pyo3_polars::PyDataFrame;
use regex::bytes::{Captures, Regex};
use tracing::error;

lazy_static! {
    static ref LOG_TEMP_REGEX: Regex = Regex::new(r"(?m)^Temperature ([\d.]+) -sample=([\d.,]+) -heatsink=([\d.]+) -cover=([\d.]+) -block=([\d.,]+)$").unwrap();
}

#[cfg_attr(feature = "python", pyo3::pyclass(get_all, set_all))]
pub struct TemperatureLog {
    pub timestamps: Vec<f64>,
    pub heatsink_temps: Vec<f64>,
    pub cover_temperatures: Vec<f64>,
    pub block_temperatures: Vec<Vec<f64>>,
    pub sample_temperatures: Vec<Vec<f64>>,
    pub num_zones: usize,
}

impl TemperatureLog {
    pub fn add_line_from_capture(&mut self, captures: &Captures<'_>) -> anyhow::Result<()> {
        let (_, [timestamp, sample_temps, heatsink_temp, cover_temp, block_temps]) =
            captures.extract();
        let timestamp = timestamp
            .to_str_lossy()
            .parse::<f64>()
            .with_context(|| format!("Failed to parse timestamp: {}", timestamp.as_bstr()))?;
        let heatsink_temp = heatsink_temp
            .to_str_lossy()
            .parse::<f64>()
            .with_context(|| {
                format!(
                    "Failed to parse heatsink temperature: {}",
                    heatsink_temp.as_bstr()
                )
            })?;
        let cover_temp = cover_temp.to_str_lossy().parse::<f64>().with_context(|| {
            format!(
                "Failed to parse cover temperature: {}",
                cover_temp.as_bstr()
            )
        })?;

        sample_temps
            .to_str_lossy()
            .split(',')
            .zip(self.sample_temperatures.iter_mut())
            .try_for_each(|(s, t)| {
                t.push(
                    s.parse::<f64>()
                        .with_context(|| format!("Failed to parse sample temperature: {}", s))?,
                );
                Ok::<_, anyhow::Error>(())
            })?;
        block_temps
            .to_str_lossy()
            .split(',')
            .zip(self.block_temperatures.iter_mut())
            .try_for_each(|(s, t)| {
                t.push(
                    s.parse::<f64>()
                        .with_context(|| format!("Failed to parse block temperature: {}", s))?,
                );
                Ok::<_, anyhow::Error>(())
            })?;

        self.timestamps.push(timestamp);
        self.heatsink_temps.push(heatsink_temp);
        self.cover_temperatures.push(cover_temp);
        Ok(())
    }

    pub fn empty(num_zones: usize) -> Self {
        let mut log = Self {
            timestamps: Vec::new(),
            sample_temperatures: Vec::new(),
            block_temperatures: Vec::new(),
            cover_temperatures: Vec::new(),
            heatsink_temps: Vec::new(),
            num_zones,
        };
        for _ in 0..num_zones {
            log.sample_temperatures.push(Vec::new());
            log.block_temperatures.push(Vec::new());
        }
        log
    }

    pub fn parse(log: &[u8]) -> anyhow::Result<Self> {
        let n_zones = get_n_zones(log).context("Failed to get number of zones")?;
        let mut parsed_log = Self::empty(n_zones);
        for captures in LOG_TEMP_REGEX.captures_iter(log) {
            match parsed_log.add_line_from_capture(&captures) {
                Ok(_) => (),
                Err(e) => {
                    let c = captures.get(0).unwrap();
                    error!(
                        "Failed to parse line at byte {}: {}: {}",
                        c.start(),
                        c.as_bytes().to_str_lossy(),
                        e
                    );
                }
            }
        }
        Ok(parsed_log)
    }

    pub fn parse_to_polars(log: &[u8]) -> anyhow::Result<DataFrame> {
        let log = Self::parse(log)?;
        // convert in place
        let mut df = df! {
            "timestamp" => log.timestamps,
        }?;
        let mut dfr = &mut df;
        for i in 0..log.num_zones {
            dfr = dfr.with_column(Column::new(
                format!("sample_{}", i + 1).into(),
                &log.sample_temperatures[i],
            ))?;
        }
        dfr = dfr.with_column(Column::new("heatsink".into(), &log.heatsink_temps))?;
        dfr = dfr.with_column(Column::new("cover".into(), &log.cover_temperatures))?;
        for i in 0..log.num_zones {
            dfr = dfr.with_column(Column::new(
                format!("block_{}", i + 1).into(),
                &log.block_temperatures[i],
            ))?;
        }
        Ok(dfr.clone())
    }

    pub fn to_polars(&self) -> anyhow::Result<DataFrame> {
        let mut df = df! {
            "timestamp" => &self.timestamps,
        }?;
        let mut dfr = &mut df;
        for i in 0..self.num_zones {
            dfr = dfr.with_column(Column::new(
                format!("sample_{}", i + 1).into(),
                self.sample_temperatures.get(i).unwrap(),
            ))?;
        }
        dfr = dfr.with_column(Column::new("heatsink".into(), &self.heatsink_temps))?;
        dfr = dfr.with_column(Column::new("cover".into(), &self.cover_temperatures))?;
        for i in 0..self.num_zones {
            dfr = dfr.with_column(Column::new(
                format!("block_{}", i + 1).into(),
                self.block_temperatures.get(i).unwrap(),
            ))?;
        }
        Ok(dfr.clone())
    }
}

#[cfg(feature = "python")]
#[pyo3::pymethods]
impl TemperatureLog {
    #[staticmethod]
    #[pyo3(name = "parse")]
    pub fn py_parse(log: &[u8]) -> anyhow::Result<Self> {
        Self::parse(log)
    }

    #[pyo3(name = "to_polars")]
    pub fn py_to_polars(&self) -> anyhow::Result<PyDataFrame> {
        self.to_polars().map(PyDataFrame)
    }

    #[staticmethod]
    #[pyo3(name = "parse_to_polars")]
    pub fn py_parse_to_polars(log: &[u8]) -> anyhow::Result<PyDataFrame> {
        Self::parse_to_polars(log).map(PyDataFrame)
    }
}

#[cfg_attr(feature = "python", pyo3::pyfunction)]
pub fn get_n_zones(log: &[u8]) -> anyhow::Result<usize> {
    let captures = LOG_TEMP_REGEX
        .captures(log)
        .ok_or(anyhow::anyhow!("No temperature data found in log"))?;
    let (_, [_, _, _, _, block_temps]) = captures.extract();
    Ok(block_temps.to_str_lossy().split(',').count())
}

#[cfg(test)]
mod tests {
    use super::*;

    static LOG: &[u8] = br#"
LEDStatus 1739920066.578 Temperature:54.1342 Current:9.50963 Voltage:3.12903 JuncTemp:69.8972
Debug 1739920066.581 C: 559790648 DRAW?
Debug 1739920066.598 S: OK 559790648 Closed
Debug 1739920066.623 LLAC ACK: Acknowledge (type=3), name=Unknown, dest=0x00, source=0xD1, control=0x01, id=0x0000, msgid=0x7A11, data=[]
Debug 1739920066.624 LLAC ACK reply: Acknowledge (type=3), name=Unknown, dest=0xD1, source=0x00, control=0x01, id=0x0000, msgid=0x7A11, data=[]
Debug 1739920066.631 Cover Raising
Time 1739920066.922 -elapsed=0 -remaining=247261
Temperature 1739920066.918 -sample=36.4,35.9,36.0,36.0,35.9,36.5 -heatsink=35.7 -cover=104.7 -block=36.4,35.9,36.0,36.0,35.9,36.5
LEDStatus 1739920067.525 Temperature:54.1342 Current:9.54187 Voltage:3.13325 JuncTemp:69.8896
Debug 1739920067.886 LLAC ACK: Acknowledge (type=3), name=Unknown, dest=0x00, source=0x91, control=0x01, id=0x0000, msgid=0x7A20, data=[]
Debug 1739920067.887 LLAC ACK reply: Acknowledge (type=3), name=Unknown, dest=0x91, source=0x00, control=0x01, id=0x0000, msgid=0x7A20, data=[]
Time 1739920067.905 -elapsed=1 -remaining=247261
Temperature 1739920067.924 -sample=36.4,35.9,36.0,36.0,35.9,36.5 -heatsink=35.9 -cover=104.7 -block=36.4,35.9,36.0,36.0,35.9,36.5
Event 1739920068.412 -target=0x00 -id=0x803F -source=0x87 -control=0x00 -data=0x6A,0x6A,0x24,0x00
Event 1739920068.419 -severity=Info -subsystem=TBC -id=0x803F -text='Board7: LLAC Retry occurred.' -data=0x6A,0x6A,0x24,0x00
Warning 1739920068.419 Ignoring response to unknown request: Acknowledge (type=3), name=Unknown, dest=0x00, source=0x87, control=0x01, id=0x0000, msgid=0x6A6A, data=[]
Debug 1739920068.421 LLAC ACK: Acknowledge (type=3), name=Unknown, dest=0x00, source=0x91, control=0x01, id=0x0000, msgid=0x7A21, data=[]
Debug 1739920068.422 LLAC ACK reply: Acknowledge (type=3), name=Unknown, dest=0x91, source=0x00, control=0x01, id=0x0000, msgid=0x7A21, data=[]
LEDStatus 1739920068.519 Temperature:54.1702 Current:9.48814 Voltage:3.13114 JuncTemp:69.9116
Warning 1739920068.630 Ignoring response to unknown request: ReadReply (type=5), name=Unknown, dest=0x00, source=0xD1, control=0x00, id=0x0000, msgid=0x7A4B, data=[0x42 0x8B 0xD2 0xC0]
Debug 1739920068.811 C: ISTAT?
Debug 1739920068.812 S: ERRor ISTAT? [InsufficientAccess] -requiredAccess="Observer" -currentAccess="Guest" --> This operation requires Observer access or higher; current level is Guest
Info 1739920068.813 Error in command from network client at 169.254.217.1:59519: ISTAT? --> [InsufficientAccess] -requiredAccess="Observer" -currentAccess="Guest" --> This operation requires Observer access or higher; current level is Guest
Time 1739920068.905 -elapsed=2 -remaining=247261
Temperature 1739920068.921 -sample=36.4,35.9,36.0,36.0,35.9,36.5 -heatsink=35.7 -cover=104.7 -block=36.4,35.9,36.0,36.0,35.9,36.5
LEDStatus 1739920069.523 Temperature:54.2784 Current:9.48814 Voltage:3.13114 JuncTemp:70.0308
Time 1739920069.907 -elapsed=3 -remaining=247261
Temperature 1739920069.921 -sample=36.6,35.9,36.0,36.1,35.9,36.5 -heatsink=35.7 -cover=104.6 -block=36.4,35.9,36.0,36.1,35.9,36.5"#;

    #[test]
    fn test_parse() {
        let log = LOG.as_bytes();
        let parsed_log = TemperatureLog::parse(log).unwrap();
        assert_eq!(parsed_log.num_zones, 6);
        assert_eq!(parsed_log.timestamps.len(), 4);
        assert_eq!(parsed_log.sample_temperatures.len(), 6);
        assert_eq!(parsed_log.block_temperatures.len(), 6);
        assert_eq!(parsed_log.cover_temperatures.len(), 4);
        assert_eq!(parsed_log.heatsink_temps.len(), 4);
        assert_eq!(
            parsed_log.timestamps,
            vec![
                1739920066.918,
                1739920067.924,
                1739920068.921,
                1739920069.921
            ]
        );
        assert_eq!(
            parsed_log.sample_temperatures[0],
            vec![36.4, 36.4, 36.4, 36.6]
        );
        assert_eq!(
            parsed_log.block_temperatures[0],
            vec![36.4, 36.4, 36.4, 36.4]
        );
        assert_eq!(
            parsed_log.cover_temperatures,
            vec![104.7, 104.7, 104.7, 104.6]
        );
        assert_eq!(parsed_log.heatsink_temps, vec![35.7, 35.9, 35.7, 35.7]);
    }

    #[test]
    fn test_get_n_zones() {
        let log = LOG.as_bytes();
        let n_zones = get_n_zones(log).unwrap();
        assert_eq!(n_zones, 6);
    }

    #[test]
    fn test_add_line_from_capture() {
        let mut log = TemperatureLog::empty(6);
        let captures = LOG_TEMP_REGEX.captures(LOG).unwrap();
        log.add_line_from_capture(&captures).unwrap();
        assert_eq!(log.sample_temperatures[0], vec![36.4]);
    }
}
