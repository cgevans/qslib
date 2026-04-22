use std::io::Cursor;
use thiserror::Error;

use crate::calibration::RoiCalibration;
use crate::quant::{QuantRegion, WellQuant};

#[cfg(feature = "python")]
use pyo3::prelude::*;

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

#[derive(Debug, Error)]
pub enum TiffError {
    #[error("TIFF decode error: {0}")]
    DecodeError(String),
    #[error("Unsupported TIFF format: {0}")]
    UnsupportedFormat(String),
    #[error("Missing filter set for emission {0}")]
    MissingFilter(u8),
    #[error("Missing calibration data for filter {0}")]
    MissingCalibration(String),
}

/// Default saturation threshold: 12-bit max (4095) × 16 = 65520.
pub const DEFAULT_SATURATION_THRESHOLD: u16 = 65520;

// ---------------------------------------------------------------------------
// TIFF decoding
// ---------------------------------------------------------------------------

/// Decode a 16-bit grayscale TIFF from raw bytes.
/// Returns (pixels, width, height) where pixels is row-major u16 data.
pub fn decode_tiff_u16(tiff_bytes: &[u8]) -> Result<(Vec<u16>, u32, u32), TiffError> {
    let cursor = Cursor::new(tiff_bytes);
    let mut decoder = tiff::decoder::Decoder::new(cursor)
        .map_err(|e| TiffError::DecodeError(e.to_string()))?;

    let (width, height) = decoder.dimensions()
        .map_err(|e| TiffError::DecodeError(e.to_string()))?;

    let result = decoder.read_image()
        .map_err(|e| TiffError::DecodeError(e.to_string()))?;

    match result {
        tiff::decoder::DecodingResult::U16(pixels) => Ok((pixels, width, height)),
        tiff::decoder::DecodingResult::U8(pixels) => {
            // Convert 8-bit to 16-bit
            Ok((pixels.iter().map(|&p| p as u16).collect(), width, height))
        }
        _ => Err(TiffError::UnsupportedFormat(
            "Expected 16-bit or 8-bit grayscale TIFF".to_string(),
        )),
    }
}

// ---------------------------------------------------------------------------
// ROI circle mask application
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
/// Apply circular ROI masks to a TIFF image, producing one WellQuant per well.
///
/// Parameters:
/// - `pixels`: row-major u16 pixel data
/// - `width`, `height`: image dimensions
/// - `horizontal_pos`: per-well X center positions (n_rows × n_cols matrix)
/// - `vertical_pos`: per-well Y center positions
/// - `roi_diameter`: diameter of the inner ROI circle
/// - `ring_size`: width of the outer ring (added to radius)
/// - `sat_threshold`: pixel value considered saturated
pub fn apply_roi_to_tiff_pixels(
    pixels: &[u16],
    width: u32,
    height: u32,
    horizontal_pos: &crate::calibration::WellMatrix,
    vertical_pos: &crate::calibration::WellMatrix,
    roi_diameter: f64,
    ring_size: f64,
    sat_threshold: u16,
) -> Vec<WellQuant> {
    let inner_radius = roi_diameter / 2.0;
    let outer_radius = inner_radius + ring_size;
    let inner_r2 = inner_radius * inner_radius;
    let outer_r2 = outer_radius * outer_radius;

    let n_rows = horizontal_pos.n_rows;
    let n_cols = horizontal_pos.n_cols;
    let mut wells = Vec::with_capacity((n_rows * n_cols) as usize);

    for row in 0..n_rows {
        for col in 0..n_cols {
            let cx = horizontal_pos.get(row, col);
            let cy = vertical_pos.get(row, col);

            // Bounding box for outer circle
            let x_min = ((cx - outer_radius).floor() as i32).max(0) as u32;
            let x_max = ((cx + outer_radius).ceil() as i32).min(width as i32 - 1) as u32;
            let y_min = ((cy - outer_radius).floor() as i32).max(0) as u32;
            let y_max = ((cy + outer_radius).ceil() as i32).min(height as i32 - 1) as u32;

            let mut inner_sum: f64 = 0.0;
            let mut inner_count: u32 = 0;
            let mut inner_saturation: u32 = 0;
            let mut outer_sum: f64 = 0.0;
            let mut outer_count: u32 = 0;
            let mut outer_saturation: u32 = 0;

            for py in y_min..=y_max {
                let dy = py as f64 - cy;
                let dy2 = dy * dy;
                let row_offset = (py * width) as usize;

                for px in x_min..=x_max {
                    let dx = px as f64 - cx;
                    let dist2 = dx * dx + dy2;

                    if dist2 <= inner_r2 {
                        let val = pixels[row_offset + px as usize];
                        inner_sum += val as f64;
                        inner_count += 1;
                        if val >= sat_threshold {
                            inner_saturation += 1;
                        }
                    } else if dist2 <= outer_r2 {
                        let val = pixels[row_offset + px as usize];
                        outer_sum += val as f64;
                        outer_count += 1;
                        if val >= sat_threshold {
                            outer_saturation += 1;
                        }
                    }
                }
            }

            wells.push(WellQuant {
                inner: QuantRegion {
                    sum: inner_sum,
                    count: inner_count,
                    saturation: inner_saturation,
                },
                outer: QuantRegion {
                    sum: outer_sum,
                    count: outer_count,
                    saturation: outer_saturation,
                },
            });
        }
    }

    wells
}

/// Apply ROI calibration to a TIFF image for a given emission filter.
///
/// Convenience wrapper that looks up the correct filter set and calibration data.
pub fn apply_roi_calibration_to_tiff(
    tiff_bytes: &[u8],
    roi: &RoiCalibration,
    emission: u8,
    sat_threshold: Option<u16>,
) -> Result<Vec<WellQuant>, TiffError> {
    let filter_set = roi
        .get_for_emission(emission)
        .ok_or(TiffError::MissingFilter(emission))?;

    let horizontal_pos = roi
        .horizontal_pos
        .get(&filter_set)
        .ok_or_else(|| {
            TiffError::MissingCalibration(format!("horizontal_pos for {}", filter_set.lowerform()))
        })?;
    let vertical_pos = roi
        .vertical_pos
        .get(&filter_set)
        .ok_or_else(|| {
            TiffError::MissingCalibration(format!("vertical_pos for {}", filter_set.lowerform()))
        })?;
    let roi_diameter = roi
        .roi_diameter
        .get(&filter_set)
        .ok_or_else(|| {
            TiffError::MissingCalibration(format!("roi_diameter for {}", filter_set.lowerform()))
        })?;
    let ring_size = roi
        .ring_size
        .get(&filter_set)
        .ok_or_else(|| {
            TiffError::MissingCalibration(format!("ring_size for {}", filter_set.lowerform()))
        })?;

    let (pixels, width, height) = decode_tiff_u16(tiff_bytes)?;

    Ok(apply_roi_to_tiff_pixels(
        &pixels,
        width,
        height,
        horizontal_pos,
        vertical_pos,
        *roi_diameter,
        *ring_size,
        sat_threshold.unwrap_or(DEFAULT_SATURATION_THRESHOLD),
    ))
}

// ---------------------------------------------------------------------------
// Python bindings
// ---------------------------------------------------------------------------

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "apply_roi_to_tiff")]
pub fn py_apply_roi_to_tiff(
    tiff_bytes: &[u8],
    roi: &RoiCalibration,
    emission: u8,
    threshold: Option<u16>,
) -> PyResult<Vec<WellQuant>> {
    apply_roi_calibration_to_tiff(tiff_bytes, roi, emission, threshold)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
}

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "decode_tiff")]
pub fn py_decode_tiff<'py>(
    py: Python<'py>,
    tiff_bytes: &[u8],
) -> PyResult<Bound<'py, numpy::PyArray2<u16>>> {
    use numpy::PyArrayMethods;

    let (pixels, width, height) = decode_tiff_u16(tiff_bytes)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
    let array = numpy::PyArray1::from_vec(py, pixels)
        .reshape([height as usize, width as usize])
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
    Ok(array)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::require_tiff_eds;
    use std::io::Read;

    fn read_bytes_from_tiff_eds(eds_path: &std::path::Path, path: &str) -> Vec<u8> {
        let file = std::fs::File::open(eds_path).expect("tiff-collection.eds not found");
        let mut archive = ::zip::ZipArchive::new(file).expect("invalid zip");
        let mut entry = archive.by_name(path).expect("file not found in EDS");
        let mut content = Vec::new();
        entry.read_to_end(&mut content).expect("read failed");
        content
    }

    fn read_string_from_tiff_eds(eds_path: &std::path::Path, path: &str) -> String {
        String::from_utf8(read_bytes_from_tiff_eds(eds_path, path)).expect("invalid utf8")
    }

    #[test]
    fn test_decode_tiff() {
        let eds_path = require_tiff_eds!();
        let data = read_bytes_from_tiff_eds(&eds_path, "apldbio/sds/images/S01_C001_T01_P0001_M4_X1_E1.tiff");
        let (pixels, width, height) = decode_tiff_u16(&data).unwrap();
        assert_eq!(width, 648);
        assert_eq!(height, 486);
        assert_eq!(pixels.len(), (648 * 486) as usize);
    }

    #[test]
    fn test_apply_roi_to_tiff() {
        let eds_path = require_tiff_eds!();
        let tiff_data =
            read_bytes_from_tiff_eds(&eds_path, "apldbio/sds/images/S01_C001_T01_P0001_M4_X1_E1.tiff");
        let roi_text = read_string_from_tiff_eds(&eds_path, "apldbio/sds/calibrations/roi.ini");
        let roi = RoiCalibration::parse(&roi_text).unwrap();

        let wells = apply_roi_calibration_to_tiff(&tiff_data, &roi, 4, None).unwrap();
        assert_eq!(wells.len(), 96);

        // All wells should have non-zero inner counts (~1016 pixels)
        for (i, w) in wells.iter().enumerate() {
            assert!(
                w.inner.count > 900 && w.inner.count < 1100,
                "Well {} inner count {} out of range",
                i,
                w.inner.count
            );
            assert!(
                w.outer.count > 200 && w.outer.count < 300,
                "Well {} outer count {} out of range",
                i,
                w.outer.count
            );
            assert!(w.inner.sum > 0.0, "Well {} inner sum is zero", i);
        }
    }

    #[test]
    fn test_tiff_roi_matches_quant() {
        let eds_path = require_tiff_eds!();
        // Compare TIFF+ROI result against the quant file for the same image
        let tiff_data =
            read_bytes_from_tiff_eds(&eds_path, "apldbio/sds/images/S01_C001_T01_P0001_M4_X1_E1.tiff");
        let roi_text = read_string_from_tiff_eds(&eds_path, "apldbio/sds/calibrations/roi.ini");
        let roi = RoiCalibration::parse(&roi_text).unwrap();

        let tiff_wells = apply_roi_calibration_to_tiff(&tiff_data, &roi, 4, None).unwrap();

        // Parse the corresponding quant file
        let quant_data =
            read_bytes_from_tiff_eds(&eds_path, "apldbio/sds/quant/S01_C001_T01_P0001_M4_X1_E1.quant");
        let qf = crate::quant::QuantFile::parse(&quant_data).unwrap();

        // Verify exact match for all 96 wells
        assert_eq!(tiff_wells.len(), qf.wells.len());
        for (i, (tw, qw)) in tiff_wells.iter().zip(qf.wells.iter()).enumerate() {
            assert_eq!(
                tw.inner.sum, qw.inner.sum,
                "Well {} inner sum mismatch: tiff={} vs quant={}",
                i, tw.inner.sum, qw.inner.sum
            );
            assert_eq!(
                tw.inner.count, qw.inner.count,
                "Well {} inner count mismatch: tiff={} vs quant={}",
                i, tw.inner.count, qw.inner.count
            );
            assert_eq!(
                tw.inner.saturation, qw.inner.saturation,
                "Well {} inner saturation mismatch: tiff={} vs quant={}",
                i, tw.inner.saturation, qw.inner.saturation
            );
            assert_eq!(
                tw.outer.sum, qw.outer.sum,
                "Well {} outer sum mismatch: tiff={} vs quant={}",
                i, tw.outer.sum, qw.outer.sum
            );
            assert_eq!(
                tw.outer.count, qw.outer.count,
                "Well {} outer count mismatch: tiff={} vs quant={}",
                i, tw.outer.count, qw.outer.count
            );
            assert_eq!(
                tw.outer.saturation, qw.outer.saturation,
                "Well {} outer saturation mismatch: tiff={} vs quant={}",
                i, tw.outer.saturation, qw.outer.saturation
            );
        }
    }

    #[test]
    fn test_tiff_roi_matches_quant_e2() {
        let eds_path = require_tiff_eds!();
        // Also verify E2 (longer exposure) matches
        let tiff_data =
            read_bytes_from_tiff_eds(&eds_path, "apldbio/sds/images/S01_C001_T01_P0001_M4_X1_E2.tiff");
        let roi_text = read_string_from_tiff_eds(&eds_path, "apldbio/sds/calibrations/roi.ini");
        let roi = RoiCalibration::parse(&roi_text).unwrap();

        let tiff_wells = apply_roi_calibration_to_tiff(&tiff_data, &roi, 4, None).unwrap();

        let quant_data =
            read_bytes_from_tiff_eds(&eds_path, "apldbio/sds/quant/S01_C001_T01_P0001_M4_X1_E2.quant");
        let qf = crate::quant::QuantFile::parse(&quant_data).unwrap();

        for (i, (tw, qw)) in tiff_wells.iter().zip(qf.wells.iter()).enumerate() {
            assert_eq!(
                tw.inner.sum, qw.inner.sum,
                "Well {} inner sum mismatch", i
            );
            assert_eq!(
                tw.inner.count, qw.inner.count,
                "Well {} inner count mismatch", i
            );
            assert_eq!(
                tw.outer.sum, qw.outer.sum,
                "Well {} outer sum mismatch", i
            );
            assert_eq!(
                tw.outer.count, qw.outer.count,
                "Well {} outer count mismatch", i
            );
        }
    }

    #[test]
    fn test_all_tiffs_match_quants() {
        let eds_path = require_tiff_eds!();
        // Verify all 6 TIFFs in tiff-collection.eds match their quant files
        let roi_text = read_string_from_tiff_eds(&eds_path, "apldbio/sds/calibrations/roi.ini");
        let roi = RoiCalibration::parse(&roi_text).unwrap();

        let tiff_quant_pairs = [
            ("apldbio/sds/images/S01_C001_T01_P0001_M4_X1_E1.tiff", "apldbio/sds/quant/S01_C001_T01_P0001_M4_X1_E1.quant"),
            ("apldbio/sds/images/S01_C001_T01_P0001_M4_X1_E2.tiff", "apldbio/sds/quant/S01_C001_T01_P0001_M4_X1_E2.quant"),
            ("apldbio/sds/images/S02_C001_T01_P0001_M4_X1_E1.tiff", "apldbio/sds/quant/S02_C001_T01_P0001_M4_X1_E1.quant"),
            ("apldbio/sds/images/S02_C001_T01_P0001_M4_X1_E2.tiff", "apldbio/sds/quant/S02_C001_T01_P0001_M4_X1_E2.quant"),
            ("apldbio/sds/images/S02_C002_T01_P0001_M4_X1_E1.tiff", "apldbio/sds/quant/S02_C002_T01_P0001_M4_X1_E1.quant"),
            ("apldbio/sds/images/S02_C002_T01_P0001_M4_X1_E2.tiff", "apldbio/sds/quant/S02_C002_T01_P0001_M4_X1_E2.quant"),
        ];

        for (tiff_path, quant_path) in &tiff_quant_pairs {
            let tiff_data = read_bytes_from_tiff_eds(&eds_path, tiff_path);
            let tiff_wells = apply_roi_calibration_to_tiff(&tiff_data, &roi, 4, None).unwrap();

            let quant_data = read_bytes_from_tiff_eds(&eds_path, quant_path);
            let qf = crate::quant::QuantFile::parse(&quant_data).unwrap();

            assert_eq!(tiff_wells.len(), qf.wells.len(), "Well count mismatch for {}", tiff_path);
            for (i, (tw, qw)) in tiff_wells.iter().zip(qf.wells.iter()).enumerate() {
                assert_eq!(tw.inner.sum, qw.inner.sum, "{} well {} inner sum", tiff_path, i);
                assert_eq!(tw.inner.count, qw.inner.count, "{} well {} inner count", tiff_path, i);
                assert_eq!(tw.outer.sum, qw.outer.sum, "{} well {} outer sum", tiff_path, i);
                assert_eq!(tw.outer.count, qw.outer.count, "{} well {} outer count", tiff_path, i);
            }
        }
    }
}
