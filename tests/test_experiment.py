# SPDX-FileCopyrightText: 2021-2023 Constantine Evans <qslib@mb.costi.net>
# SPDX-License-Identifier: EUPL-1.2

from pathlib import Path

import pytest

from qslib import Experiment, Protocol, Stage, Step
from qslib.experiment import DataNotAvailableError


def test_create():
    Experiment(protocol=Protocol([Stage([Step(30, 25)])]))


def test_fail_plots_temperature():
    exp = Experiment(protocol=Protocol([Stage([Step(30, 25)])]))

    with pytest.raises(DataNotAvailableError):
        exp.plot_temperatures()


def test_fail_plots_over_time():
    exp = Experiment(protocol=Protocol([Stage([Step(30, 25)])]))
    with pytest.raises(DataNotAvailableError):
        exp.plot_over_time()


def test_fail_plots_anneal_melt():
    exp = Experiment(protocol=Protocol([Stage([Step(30, 25)])]))
    with pytest.raises(DataNotAvailableError): # FIXME: why is this inconsistent?
        exp.plot_anneal_melt()


@pytest.mark.parametrize("ch", ["/", "!", "}"])
def test_unsafe_names(ch):
    with pytest.raises(ValueError, match=r"Invalid characters \(" + ch + r"\)"):
        Experiment(name=f"a{ch}b")


def test_all_filters_no_data() -> None:
    """Test that all_filters returns protocol filters when experiment has no data."""
    from qslib import Experiment, Protocol, Stage, Step
    from qslib.data import FilterSet
    
    # Create a new experiment with no data - should use protocol filters
    exp_no_data = Experiment(name="test_no_data")
    
    # Default protocol has no filters, so all_filters should be empty
    assert len(exp_no_data.all_filters) == 0
    assert list(exp_no_data.all_filters) == []
    
    # Create an experiment with protocol that has filters
    step_with_filters = Step(time=60, temperature=95, filters=["x1-m1", "x2-m2"], collect=True)
    protocol_with_filters = Protocol([Stage([step_with_filters])], filters=["x3-m3"])
    exp_with_protocol_filters = Experiment(name="test_with_filters", protocol=protocol_with_filters)
    
    # Should return filters from protocol (both default filters and step filters)
    expected_protocol_filters = {FilterSet.fromstring("x1-m1"), FilterSet.fromstring("x2-m2"), FilterSet.fromstring("x3-m3")}
    actual_protocol_filters = set(exp_with_protocol_filters.all_filters)
    
    assert actual_protocol_filters == expected_protocol_filters
    assert len(exp_with_protocol_filters.all_filters) == 3


def test_available_data_with_data():
    """Test available_data method with experiment loaded from test.eds file."""
    exp = Experiment.from_file(Path(__file__).parent / "test.eds")
    available = exp.available_data()
    
    # test.eds should have all these data types available
    expected_data = ["filter_data", "multicomponent_data", "amplification_data", "analysis_result", "temperatures", "quant_data", "calibrations"]

    assert set(available) == set(expected_data)
    assert len(available) == 7


def test_available_data_no_data():
    """Test available_data method with newly-created experiment (no data)."""
    exp = Experiment(name="test_no_data", protocol=Protocol([Stage([Step(30, 25)])]))
    available = exp.available_data()

    # New experiment should have no data available
    assert available == []
    assert len(available) == 0


def test_multicomponent_sample_temperatures_whitespace():
    """Test that SampleTemperatures parsing handles both tab and space separators."""
    import numpy as np
    from qslib.data import _parse_multicomponent_data_v1
    import xml.etree.ElementTree as ET
    import zipfile

    # Parse the real multicomponent data from test.eds (tab-separated)
    with zipfile.ZipFile(Path(__file__).parent / "test.eds") as z:
        with z.open("apldbio/sds/multicomponentdata.xml") as f:
            tree = ET.parse(f)

    result_original = _parse_multicomponent_data_v1(tree)
    assert "temperature" in result_original.columns
    assert len(result_original) > 0
    # Temperatures should all be positive and reasonable
    temps = result_original["temperature"].dropna()
    assert len(temps) > 0
    assert (temps > 0).all()
    assert (temps < 150).all()

    # Now modify the XML to use space-separated values (simulating server aggregation)
    with zipfile.ZipFile(Path(__file__).parent / "test.eds") as z:
        with z.open("apldbio/sds/multicomponentdata.xml") as f:
            tree2 = ET.parse(f)

    st_elem = tree2.find("SampleTemperatures")
    original_text = st_elem.text
    # Replace tabs with spaces (what server aggregation does)
    st_elem.text = " ".join(original_text.split())

    result_space = _parse_multicomponent_data_v1(tree2)
    assert np.array_equal(
        result_original["temperature"].values,
        result_space["temperature"].values,
    )


def test_create_96_well_plate():
    """Test that default experiment creates a 96-well plate."""
    exp = Experiment(protocol=Protocol([Stage([Step(30, 25)])]))
    assert exp.plate_type == 96


def test_create_384_well_plate():
    """Test that 384-well plate can be created via plate_setup."""
    from qslib.plate_setup import PlateSetup
    ps = PlateSetup(plate_type=384)
    exp = Experiment(protocol=Protocol([Stage([Step(30, 25)])]), plate_setup=ps)
    assert exp.plate_type == 384