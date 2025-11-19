# SPDX-FileCopyrightText: 2021-2023 Constantine Evans <qslib@mb.costi.net>
# SPDX-License-Identifier: EUPL-1.2

import json
import os
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from qslib import Experiment
from qslib.data import _filterdata_df_v2, _parse_multicomponent_data_v2
from qslib.experiment import _get_manifest_info


@pytest.fixture(scope="module")
def v2_test_eds_file():
    """Path to the v2 test EDS file."""
    return Path(__file__).parent / "v2_test.eds"


@pytest.fixture(scope="module")
def v2_test_experiment(v2_test_eds_file):
    """Load v2 test experiment from EDS file."""
    return Experiment.from_file(v2_test_eds_file)


@pytest.fixture(scope="module") 
def v2_manifest_data(v2_test_experiment):
    """Load manifest data for testing."""
    return _get_manifest_info(v2_test_experiment.root_dir, checkinfo=False)


@pytest.fixture(scope="module")
def v2_multicomponent_data(v2_test_experiment):
    """Load multicomponent data for testing."""
    mdp = os.path.join(v2_test_experiment.root_dir, "primary/multicomponent_data.json")
    with open(mdp, "r") as f:
        return json.load(f)


@pytest.fixture(scope="module") 
def v2_filter_data(v2_test_experiment):
    """Load filter data for testing."""
    fdp = os.path.join(v2_test_experiment.root_dir, "run/filter_data.json")
    with open(fdp, "r") as f:
        return json.load(f)


@pytest.fixture(scope="module")
def v2_analysis_result(v2_test_experiment):
    """Load analysis result data for testing."""
    return v2_test_experiment._analysis_dict_v2


@pytest.fixture(scope="module")
def v2_summary_data(v2_test_experiment):
    """Load summary data for testing."""
    sdp = os.path.join(v2_test_experiment.root_dir, "summary.json")
    with open(sdp, "r") as f:
        return json.load(f)


class TestV2ManifestParsing:
    """Test v2 EDS manifest and metadata parsing."""
    
    def test_manifest_info_parsing(self, v2_manifest_data):
        """Test that manifest info is parsed correctly for v2 format."""
        assert v2_manifest_data["Specification-Title"] == "Experiment Document Specification"
        assert v2_manifest_data["Specification-Version"] == "2.0.0"
        assert v2_manifest_data["Implementation-Title"] == "qPCR File API"
        assert v2_manifest_data["Implementation-Version"] == "2.5.6"
    
    def test_manifest_version_detection(self, v2_manifest_data):
        """Test that v2 specification version is correctly detected."""
        assert v2_manifest_data["Specification-Version"].startswith("2")
    
    def test_summary_json_loading(self, v2_summary_data):
        """Test loading of summary.json metadata."""
        summary = v2_summary_data
        
        assert summary["name"] == "Test_Experiment_001"
        assert summary["blockType"] == "BLOCK_384W"
        assert summary["instrumentType"] == "QS6PRO"
        assert summary["runStatus"] == "COMPLETED"
        assert summary["regulatoryLabel"] == "RUO"
        
        # Check analysis info
        assert summary["analysis"]["primary"]["status"] == "COMPLETED"
        assert summary["analysis"]["primary"]["id"] == "algo.primary"


class TestV2MulticomponentData:
    """Test v2 multicomponent data parsing."""
    
    def test_multicomponent_data_structure(self, v2_multicomponent_data):
        """Test that multicomponent data has expected structure."""
        assert "collectionPoints" in v2_multicomponent_data
        assert "wellData" in v2_multicomponent_data
        assert len(v2_multicomponent_data["collectionPoints"]) == 5  # Our test sample
        assert len(v2_multicomponent_data["wellData"]) == 8  # Our test sample
    
    def test_collection_points_format(self, v2_multicomponent_data):
        """Test collection points have correct format."""
        cp = v2_multicomponent_data["collectionPoints"][0]
        assert "cycle" in cp
        assert "point" in cp  
        assert "stage" in cp
        assert "step" in cp
        assert isinstance(cp["cycle"], int)
        assert isinstance(cp["point"], int)
        assert isinstance(cp["stage"], int)
        assert isinstance(cp["step"], int)
    
    def test_well_data_format(self, v2_multicomponent_data):
        """Test well data has correct format."""
        well = v2_multicomponent_data["wellData"][0]
        assert "wellIndex" in well
        assert "dyeData" in well
        assert "temperatures" in well
        assert isinstance(well["wellIndex"], int)
        assert isinstance(well["dyeData"], list)
        assert isinstance(well["temperatures"], list)
        
        # Test dye data format
        dye = well["dyeData"][0]
        assert "dyeName" in dye
        assert "fluorescences" in dye
        assert isinstance(dye["fluorescences"], list)
    
    def test_multicomponent_data_parsing_function(self, v2_multicomponent_data):
        """Test _parse_multicomponent_data_v2 function."""
        # Test with 384-well plate (our test data is from 384-well)
        df = _parse_multicomponent_data_v2(v2_multicomponent_data, 384)
        
        assert isinstance(df, pd.DataFrame)
        assert df.index.names == ["well", "collection_cycle"]
        
        # Check that we have the expected columns
        expected_cols = ["cycle", "point", "stage", "step", "temperature"]
        for col in expected_cols:
            assert col in df.columns
        
        # Check that we have dye columns (FAM, ROX, etc.)
        dye_cols = [col for col in df.columns if col not in ["cycle", "point", "stage", "step", "temperature"]]
        assert len(dye_cols) > 0
        
        # Check data types
        assert df["temperature"].dtype in [np.float64, np.float32]
        for dye_col in dye_cols:
            assert df[dye_col].dtype in [np.float64, np.float32]


class TestV2FilterData:
    """Test v2 filter data parsing."""
    
    def test_filter_data_structure(self, v2_filter_data):
        """Test that filter data has expected structure."""
        assert isinstance(v2_filter_data, list)
        assert len(v2_filter_data) == 3  # Our test sample
        
        item = v2_filter_data[0]
        assert "collectionPoint" in item
        assert "filterData" in item
        assert "zoneTemperatures" in item
    
    def test_collection_point_format(self, v2_filter_data):
        """Test collection point format in filter data."""
        cp = v2_filter_data[0]["collectionPoint"]
        assert "cycle" in cp
        assert "point" in cp
        assert "stage" in cp
        assert "step" in cp
    
    def test_filter_data_format(self, v2_filter_data):
        """Test filter data format."""
        filter_item = v2_filter_data[0]["filterData"][0]
        assert "exposure" in filter_item
        assert "filterSet" in filter_item
        assert "wellFluorescences" in filter_item
        assert isinstance(filter_item["wellFluorescences"], list)
        assert len(filter_item["wellFluorescences"]) == 384  # Our test data is 384-well
    
    def test_filterdata_df_v2_function(self, v2_filter_data, v2_test_experiment):
        """Test _filterdata_df_v2 function."""
        # Test with 384-well plate (our test data is 384-well)
        df = _filterdata_df_v2(
            v2_filter_data, 
            384,
            quant_files_path=(Path(v2_test_experiment.root_dir) / "run/quant")
        )
        
        assert isinstance(df, pd.DataFrame)
        assert df.index.names == ["filter_set", "stage", "cycle", "step", "point"]
        
        # Check that we have fluorescence columns for wells
        fl_cols = [col for col in df.columns if col[1] == "fl"]
        assert len(fl_cols) == 384
        
        # Check that we have temperature columns
        rt_cols = [col for col in df.columns if col[1] == "rt"]
        assert len(rt_cols) == 384
        
        # Check that we have exposure column
        assert ("exposure", "exposure") in df.columns
        
        # Check that timestamps were added from quant files
        assert ("time", "timestamp") in df.columns
    
    def test_filterdata_without_quant_files(self, v2_filter_data):
        """Test filter data parsing without quant files."""
        df = _filterdata_df_v2(v2_filter_data, 384, quant_files_path=None)
        
        assert isinstance(df, pd.DataFrame)
        # Should not have time columns without quant files
        time_cols = [col for col in df.columns if col[0] == "time"]
        assert len(time_cols) == 0


class TestV2AnalysisResults:
    """Test v2 analysis results parsing."""
    
    def test_analysis_result_structure(self, v2_analysis_result):
        """Test analysis result data structure."""
        assert "replicateGroupResults" in v2_analysis_result
        assert isinstance(v2_analysis_result["replicateGroupResults"], list)
        assert len(v2_analysis_result["replicateGroupResults"]) == 10  # Our test sample
    
    def test_replicate_group_format(self, v2_analysis_result):
        """Test replicate group result format."""
        group = v2_analysis_result["replicateGroupResults"][0]
        
        expected_fields = [
            "cqMean", "cqSD", "cqSE", "flags", "numberOfReplicates",
            "quantity", "resultQCIssues", "sampleName", "targetName"
        ]
        
        for field in expected_fields:
            assert field in group
        
        # Check data types
        assert isinstance(group["cqMean"], (int, float))
        assert isinstance(group["cqSD"], (int, float))
        assert isinstance(group["cqSE"], (int, float))
        assert isinstance(group["numberOfReplicates"], int)
        assert isinstance(group["quantity"], (int, float))
        assert isinstance(group["sampleName"], str)
        assert isinstance(group["targetName"], str)
        assert isinstance(group["flags"], list)
        assert isinstance(group["resultQCIssues"], list)
    
    def test_anonymized_sample_names(self, v2_analysis_result):
        """Test that sample names were properly anonymized."""
        sample_names = [
            group["sampleName"] 
            for group in v2_analysis_result["replicateGroupResults"]
        ]
        
        # Should all start with "Sample_"
        for name in sample_names:
            assert name.startswith("Sample_")
        
        # Should have target names like "Target_1", "Target_2", etc.
        target_names = [
            group["targetName"]
            for group in v2_analysis_result["replicateGroupResults"]
        ]
        
        for name in target_names:
            assert name.startswith("Target_")


class TestV2ExperimentLoading:
    """Test full v2 experiment loading integration."""
    
    def test_experiment_from_eds_file(self, v2_test_experiment):
        """Test loading a v2 experiment from EDS file."""
        exp = v2_test_experiment
        
        # Test basic properties
        assert exp.spec_major_version == 2
        assert exp.name == "Test_Experiment_001"
        
        # Test that we can access v2-specific properties
        analysis_dict = exp._analysis_dict_v2
        assert isinstance(analysis_dict, dict)
        assert "replicateGroupResults" in analysis_dict
        
        # Test multicomponent data access
        try:
            mc_data = exp.multicomponent_data
            assert isinstance(mc_data, pd.DataFrame)
        except ValueError:
            # This is expected if plate_type is not set
            pass
    
    def test_v2_experiment_properties(self, v2_test_experiment):
        """Test v2-specific experiment properties."""
        exp = v2_test_experiment
        
        # Set plate type for data access
        exp.plate_type = 384
        
        # Test multicomponent data
        mc_data = exp.multicomponent_data
        assert isinstance(mc_data, pd.DataFrame)
        assert not mc_data.empty
        
        # Test analysis results
        analysis_dict = exp._analysis_dict_v2
        assert len(analysis_dict["replicateGroupResults"]) == 10


class TestV2ErrorHandling:
    """Test error handling and edge cases for v2 EDS."""
    
    def test_missing_manifest_file(self, tmp_path):
        """Test error when manifest file is missing."""
        with pytest.raises(ValueError, match="No EDS manifest file found"):
            _get_manifest_info(tmp_path)
    
    def test_invalid_specification_version(self, tmp_path):
        """Test error for unsupported specification version."""
        manifest_path = tmp_path / "Manifest.mf"
        manifest_path.write_text(
            "Manifest-Version: 1.0\n"
            "Specification-Title: Experiment Document Specification\n"
            "Specification-Version: 3.0.0\n"
        )
        
        with pytest.raises(ValueError, match="QSLib does not support EDS files of specification version"):
            _get_manifest_info(tmp_path)
    
    def test_multicomponent_data_unsupported_plate_type(self, v2_multicomponent_data):
        """Test error for unsupported plate type in multicomponent data."""
        with pytest.raises(ValueError, match="Unsupported number of wells"):
            _parse_multicomponent_data_v2(v2_multicomponent_data, 48)  # Unsupported plate type
    
    def test_filterdata_unsupported_plate_type(self, v2_filter_data):
        """Test error for unsupported plate type in filter data."""
        with pytest.raises(ValueError):
            _filterdata_df_v2(v2_filter_data, 48)  # Unsupported plate type
    
    def test_missing_quant_files(self, v2_filter_data, tmp_path):
        """Test handling of missing quant files."""
        # Current implementation raises FileNotFoundError for missing quant files
        with pytest.raises(FileNotFoundError):
            _filterdata_df_v2(v2_filter_data, 384, quant_files_path=tmp_path / "nonexistent")


class TestV2DataValidation:
    """Test data validation and consistency for v2 EDS."""
    
    def test_multicomponent_data_consistency(self, v2_multicomponent_data):
        """Test data consistency in multicomponent data."""
        collection_points = v2_multicomponent_data["collectionPoints"]
        well_data = v2_multicomponent_data["wellData"]
        
        # All wells should have same number of temperature readings as collection points
        for well in well_data:
            assert len(well["temperatures"]) == len(collection_points)
            
            # All dyes should have same number of fluorescence readings
            for dye in well["dyeData"]:
                assert len(dye["fluorescences"]) == len(collection_points)
    
    def test_filter_data_consistency(self, v2_filter_data):
        """Test data consistency in filter data."""
        for item in v2_filter_data:
            # All filter sets should have same number of well fluorescences
            well_counts = [
                len(fd["wellFluorescences"]) 
                for fd in item["filterData"]
            ]
            assert len(set(well_counts)) == 1  # All should be the same
    
    def test_data_types_and_ranges(self, v2_multicomponent_data, v2_filter_data, v2_analysis_result):
        """Test that data types and ranges are reasonable."""
        # Test multicomponent data
        for well in v2_multicomponent_data["wellData"]:
            for temp in well["temperatures"]:
                assert isinstance(temp, (int, float))
                assert 0 <= temp <= 100  # Reasonable temperature range
            
            for dye in well["dyeData"]:
                for fl in dye["fluorescences"]:
                    assert isinstance(fl, (int, float))
                    # Fluorescence can be negative due to background subtraction
                    assert -10000 <= fl <= 1000000  # Reasonable fluorescence range
        
        # Test filter data
        for item in v2_filter_data:
            for fd in item["filterData"]:
                assert isinstance(fd["exposure"], int)
                assert fd["exposure"] > 0
                
                for fl in fd["wellFluorescences"]:
                    assert isinstance(fl, (int, float))
                    # Fluorescence can be negative due to background subtraction
                    assert -10000 <= fl <= 1000000  # Reasonable fluorescence range
        
        # Test analysis results
        for group in v2_analysis_result["replicateGroupResults"]:
            assert group["numberOfReplicates"] > 0
            assert group["cqSD"] >= 0  # Standard deviation should be non-negative
            assert group["cqSE"] >= 0  # Standard error should be non-negative
