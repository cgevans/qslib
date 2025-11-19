# SPDX-FileCopyrightText: 2021-2023 Constantine Evans <qslib@mb.costi.net>
# SPDX-License-Identifier: EUPL-1.2


import datetime
import numpy as np
import pytest

from qslib import Experiment
import qslib
from qslib.experiment import DataNotAvailableError
from qslib.processors import (
    NormToMaxPerWell,
    NormToMeanPerWell,
    SmoothEMWMean,
    SmoothWindowMean,
    SubtractByMeanPerWell,
)


@pytest.fixture(scope="module")
def exp() -> Experiment:
    exp = Experiment.from_file("tests/test.eds")
    # We need better sample arrangements:
    exp.sample_wells["Sample 1"] = ["A7", "A8"]
    exp.sample_wells["Sample 2"] = ["A9", "A10"]
    exp.sample_wells["othersample"] = ["B7"]
    return exp


@pytest.fixture(scope="module")
def exp_reloaded(
    exp: Experiment, tmp_path_factory: pytest.TempPathFactory
) -> Experiment:
    tmp_path = tmp_path_factory.mktemp("exp")
    exp.save_file(tmp_path / "test_loaded.eds")
    return Experiment.from_file(tmp_path / "test_loaded.eds")


def test_props(exp: Experiment, exp_reloaded: Experiment) -> None:
    assert exp.name == "2020-02-20_170706"

    assert exp.info() == str(exp) == exp.summary()


def test_reload(exp: Experiment, exp_reloaded: Experiment) -> None:
    assert (exp.welldata == exp_reloaded.welldata).all().all()
    assert exp.name == exp_reloaded.name
    assert exp.protocol == exp_reloaded.protocol
    assert exp.plate_setup == exp_reloaded.plate_setup


def test_plot_ntmpw_smoothmw(exp: Experiment) -> None:
    axf, axt = exp.plot_over_time(
        process=[SmoothWindowMean(4), NormToMeanPerWell(2)], annotate_stage_lines=False
    )
    assert axf.get_ylabel() == "fluorescence (window mean 4, norm. to mean)"


def test_plot_emw_maxperwell(exp: Experiment) -> None:
    axf, axt = exp.plot_over_time(
        process=[SmoothEMWMean(alpha=0.1), NormToMaxPerWell(2)],
        annotate_stage_lines=False,
    )
    assert axf.get_ylabel() == "fluorescence (EMW-smoothed, norm. to max)"


def test_plot_subtrbymean(exp: Experiment) -> None:
    axf, axt = exp.plot_over_time(
        process=SubtractByMeanPerWell(2), annotate_stage_lines=False
    )
    assert axf.get_ylabel() == "fluorescence (subtr. by mean)"


def test_plots(exp: Experiment) -> None:
    axf, axt = exp.plot_over_time(
        legend=False, figure_kw={"constrained_layout": False}, annotate_stage_lines=True
    )

    assert len(axf.get_lines()) == 5 * len(exp.all_filters) # + 2 # No +2, because stage lines are outside of plot
    assert np.allclose(
        axf.get_xlim(), (0.0287576, 0.089), atol=0.01
    )

    with pytest.raises(ValueError, match="Samples not found"):
        exp.plot_over_time("Sampl(e|a)")

    with pytest.raises(ValueError, match="Samples not found"):
        exp.plot_anneal_melt("Sampl(e|a)")

    import matplotlib.pyplot as plt

    _, ax = plt.subplots()
    axs = exp.plot_over_time(
        "Sample .*",
        "x1-m1",
        stages=2,
        temperatures=False,
        stage_lines=False,
        ax=ax,
        marker=".",
        legend=False,
    )

    axs2 = exp.plot_over_time(
        ["Sample 1", "Sample 2"],
        "x1-m1",
        stages=2,
        temperatures=False,
        stage_lines="fluorescence",
        annotate_stage_lines=("fluorescence", 0.1),
        marker=".",
        legend=True,
    )

    assert len(axs) == 1 == len(axs2)
    # assert len(axs[0].get_lines()) == 4 == len(axs2[0].get_lines()) - 3 # FIXME

    axs = exp.plot_over_time("Sample .*")

    exp.plot_anneal_melt(samples="Sample 1")

    exp.protocol.plot_protocol()
    exp.plot_protocol()


# rawquant has been removed
# def test_rawquant(exp: Experiment) -> None:
#     exp.rawdata.loc[:, :]


def test_all_filters(exp: Experiment) -> None:
    """Test that all_filters returns the expected filter sets."""
    expected_filters = {
        "x1-m1", "x1-m2", "x2-m2", "x2-m3", "x3-m3", 
        "x3-m4", "x4-m4", "x4-m5", "x5-m5", "x5-m6"
    }
    
    actual_filters = {str(f) for f in exp.all_filters}
    
    assert actual_filters == expected_filters
    assert len(exp.all_filters) == 10


def test_filter_strings(exp: Experiment) -> None:
    """Test that filter_strings returns the expected filter strings."""
    expected_filter_strings = [
        "x1-m1", "x1-m2", "x2-m2", "x2-m3", "x3-m3", 
        "x3-m4", "x4-m4", "x4-m5", "x5-m5", "x5-m6"
    ]
    
    actual_filter_strings = exp.filter_strings
    
    # Convert to sets for comparison since order might vary
    assert set(actual_filter_strings) == set(expected_filter_strings)
    assert len(actual_filter_strings) == 10
    
    # Verify that filter_strings matches string conversion of all_filters
    assert set(actual_filter_strings) == {str(f) for f in exp.all_filters}





def test_save_file_with_dots(exp: Experiment, tmp_path_factory: pytest.TempPathFactory) -> None: # test for issue #33
    tmp_path = tmp_path_factory.mktemp("exp")
    
    # Create a new experiment with dots in the name
    exp.name = "test.with.dots"
    exp.save_file(tmp_path)
    
    # Check that the file was saved with correct name
    saved_file = tmp_path / "test.with.dots.eds"
    assert saved_file.exists()
    
    # Load the file back to verify it's valid
    loaded_exp = Experiment.from_file(saved_file)
    assert loaded_exp.name == "test.with.dots"

    # Now try with dots and spaces in the name; the spaces should be converted to underscores
    exp.name = "test.with.dots and spaces"
    exp.save_file(tmp_path)
    
    # Check that the file was saved with correct name
    saved_file = tmp_path / "test.with.dots_and_spaces.eds"
    assert saved_file.exists()

def test_mid_run_eds():
    exp = Experiment.from_file("tests/mid-run.eds")

    assert exp.runstate == "RUNNING"
    assert exp.activeendtime is None
    assert exp.activestarttime == datetime.datetime(2025, 11, 18, 1, 52, 11, 949000, tzinfo=datetime.timezone.utc) # datetime.datetime(2025, 11, 18, 1, 52, 11, 949000, tzinfo=datetime.timezone.utc)

    # ps = qslib.PlateSetup({
    #     "s1": ["A1"],
    #     "s2": ["A2", "B4"],
    # })
    prot = qslib.Protocol(
        [
            qslib.Stage.hold_at(25, "5min", "60s", collect=True)
        ], filters=["x1-m1", "x3-m5", "x2-m2"]
    )

    # assert exp.plate_setup == ps
    assert exp.protocol == prot

    exp.filter_data_polars

    # Test that analysis data is not available for mid-run experiments
    with pytest.raises(DataNotAvailableError):
        exp.multicomponent_data
    
    with pytest.raises(DataNotAvailableError):
        exp.analysis_result
    
    with pytest.raises(DataNotAvailableError):
        exp.amplification_data