# SPDX-FileCopyrightText: 2021-2023 Constantine Evans <qslib@mb.costi.net>
# SPDX-License-Identifier: EUPL-1.2

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
    exp = Experiment.from_file("tests/test.eds")
    available = exp.available_data()
    
    # test.eds should have all these data types available
    expected_data = ["filter_data", "multicomponent_data", "amplification_data", "analysis_result", "temperatures"]
    
    assert set(available) == set(expected_data)
    assert len(available) == 5


def test_available_data_no_data():
    """Test available_data method with newly-created experiment (no data)."""
    exp = Experiment(name="test_no_data", protocol=Protocol([Stage([Step(30, 25)])]))
    available = exp.available_data()
    
    # New experiment should have no data available
    assert available == []
    assert len(available) == 0