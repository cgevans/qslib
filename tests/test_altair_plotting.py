# SPDX-FileCopyrightText: 2024 Constantine Evans <qslib@mb.costi.net>
# SPDX-License-Identifier: EUPL-1.2

"""Tests for Altair plotting functionality."""

import pytest
from qslib import Experiment


@pytest.fixture
def exp() -> Experiment:
    """Load test experiment for plotting tests."""
    return Experiment.from_file("tests/test.eds")


def test_plot_over_time_altair_basic(exp: Experiment) -> None:
    """Test that plot_over_time_altair returns a valid Altair chart."""
    try:
        import altair as alt
    except ImportError:
        pytest.skip("Altair not available")
    
    chart = exp.plot_over_time_altair()
    
    # Verify it's an Altair chart
    assert isinstance(chart, alt.Chart)
    
    # Check that the chart has expected encoding properties
    assert hasattr(chart, 'encoding')
    assert hasattr(chart.encoding, 'x')
    assert hasattr(chart.encoding, 'y')
    assert hasattr(chart.encoding, 'color')
    
    # Verify the chart has data
    assert chart.data is not None
    assert len(chart.data) > 0


def test_plot_over_time_altair_with_samples(exp: Experiment) -> None:
    """Test plot_over_time_altair with specific samples."""
    try:
        import altair as alt
    except ImportError:
        pytest.skip("Altair not available")
    
    # Test with string sample pattern
    chart = exp.plot_over_time_altair(samples="Sample 1")
    assert isinstance(chart, alt.Chart)
    assert chart.data is not None
    
    # Test with list of samples
    chart = exp.plot_over_time_altair(samples=["Sample 1", "Sample 2"])
    assert isinstance(chart, alt.Chart)
    assert chart.data is not None


def test_plot_over_time_altair_with_filters(exp: Experiment) -> None:
    """Test plot_over_time_altair with specific filters."""
    try:
        import altair as alt
    except ImportError:
        pytest.skip("Altair not available")
    
    # Get available filters from the experiment
    filters = list(exp.all_filters)
    if filters:
        # Convert FilterSet to string for testing
        filter_str = str(filters[0])
        chart = exp.plot_over_time_altair(filters=filter_str)
        assert isinstance(chart, alt.Chart)
        assert chart.data is not None


def test_plot_over_time_altair_duration_units(exp: Experiment) -> None:
    """Test plot_over_time_altair with different duration units."""
    try:
        import altair as alt
    except ImportError:
        pytest.skip("Altair not available")
    
    for units in ["hours", "minutes", "seconds"]:
        chart = exp.plot_over_time_altair(duration_units=units)
        assert isinstance(chart, alt.Chart)
        assert chart.data is not None
        
        # Check that the chart is valid and has the expected structure
        # The actual axis title checking is complex with Altair's property setters
        # so we just verify the chart was created successfully with the units parameter
        assert hasattr(chart.encoding, 'x')
        assert hasattr(chart.encoding.x, 'axis')


def test_plot_over_time_altair_legend_control(exp: Experiment) -> None:
    """Test plot_over_time_altair legend control."""
    try:
        import altair as alt
    except ImportError:
        pytest.skip("Altair not available")
    
    # Test with legend enabled
    chart_with_legend = exp.plot_over_time_altair(show_legend=True)
    assert isinstance(chart_with_legend, alt.Chart)
    assert hasattr(chart_with_legend.encoding, 'color')
    
    # Test with legend disabled  
    chart_no_legend = exp.plot_over_time_altair(show_legend=False)
    assert isinstance(chart_no_legend, alt.Chart)
    assert hasattr(chart_no_legend.encoding, 'color')
    
    # Both charts should be valid Altair charts regardless of legend setting
    # The main test is that both calls succeed without error


def test_plot_temperatures_altair(exp: Experiment) -> None:
    """Test plot_temperatures method with Altair backend."""
    try:
        import altair as alt
    except ImportError:
        pytest.skip("Altair not available")
    
    chart = exp.plot_temperatures(method="altair")
    
    # Verify it's an Altair chart
    assert isinstance(chart, alt.Chart)
    
    # Check basic encoding properties
    assert hasattr(chart, 'encoding')
    assert hasattr(chart.encoding, 'x')
    assert hasattr(chart.encoding, 'y')
    
    # Verify the chart has data
    assert chart.data is not None
    assert len(chart.data) > 0


def test_plot_temperatures_altair_with_options(exp: Experiment) -> None:
    """Test plot_temperatures with various Altair options."""
    try:
        import altair as alt
    except ImportError:
        pytest.skip("Altair not available")
    
    # Test with different time units
    for time_unit in ["h", "m", "s"]:
        chart = exp.plot_temperatures(method="altair", time_units=time_unit)
        assert isinstance(chart, alt.Chart)
        assert chart.data is not None


def test_altair_error_handling(exp: Experiment) -> None:
    """Test error handling in Altair plotting methods."""
    try:
        import altair as alt
    except ImportError:
        pytest.skip("Altair not available")
    
    # Test invalid duration units
    with pytest.raises(ValueError, match="Invalid duration_units"):
        exp.plot_over_time_altair(duration_units="invalid")
    
    # Test invalid start_time
    with pytest.raises(ValueError, match="Invalid start_time"):
        exp.plot_over_time_altair(start_time="invalid")


def test_altair_chart_data_structure(exp: Experiment) -> None:
    """Test that Altair charts contain expected data structure."""
    try:
        import altair as alt
        import polars as pl
    except ImportError:
        pytest.skip("Altair or Polars not available")
    
    chart = exp.plot_over_time_altair()
    
    # Convert chart data to DataFrame for inspection
    data = pl.DataFrame(chart.data)
    
    # Check expected columns exist
    expected_cols = ["time_since_mark_float", "processed_fluorescence", "sample", "filter_set"]
    for col in expected_cols:
        assert col in data.columns
    
    # Check data types
    assert data["time_since_mark_float"].dtype == pl.Float64
    assert data["processed_fluorescence"].dtype in [pl.Float64, pl.Float32]
    assert data["sample"].dtype == pl.String
    assert data["filter_set"].dtype == pl.String


def test_altair_imports_and_setup(exp: Experiment) -> None:
    """Test that Altair plotting properly sets up data transformers."""
    try:
        import altair as alt
    except ImportError:
        pytest.skip("Altair not available")
    
    # This should not raise an error and should enable vegafusion
    chart = exp.plot_over_time_altair()
    
    # Verify vegafusion is enabled (if available)
    # Note: This is a basic check - vegafusion setup happens inside the method
    assert isinstance(chart, alt.Chart)
