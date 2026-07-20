# SPDX-FileCopyrightText: 2021-2023 Constantine Evans <qslib@mb.costi.net>
# SPDX-License-Identifier: EUPL-1.2


import datetime
import io
from pathlib import Path
import zipfile
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

_TESTS_DIR = Path(__file__).parent


@pytest.fixture(scope="module")
def exp() -> Experiment:
    exp = Experiment.from_file(_TESTS_DIR / "test.eds")
    # We need better sample arrangements:
    exp.sample_wells["Sample 1"] = ["A7", "A8"]
    exp.sample_wells["Sample 2"] = ["A9", "A10"]
    exp.sample_wells["othersample"] = ["B7"]
    return exp


@pytest.fixture(scope="module")
def exp_reloaded(exp: Experiment, tmp_path_factory: pytest.TempPathFactory) -> Experiment:
    tmp_path = tmp_path_factory.mktemp("exp")
    exp.save_file(tmp_path / "test_loaded.eds")
    return Experiment.from_file(tmp_path / "test_loaded.eds")


@pytest.mark.parametrize("use_stream", [False, True], ids=["path", "stream"])
def test_save_file_uses_deflate(exp: Experiment, tmp_path: Path, use_stream: bool) -> None:
    output = io.BytesIO() if use_stream else tmp_path / "compressed.eds"

    exp.save_file(output, update_files=False)

    with zipfile.ZipFile(output) as archive:
        members = [member for member in archive.infolist() if not member.is_dir()]
        assert members
        assert {member.compress_type for member in members} == {zipfile.ZIP_DEFLATED}

        messages = archive.getinfo("apldbio/sds/messages.log")
        assert messages.compress_size < messages.file_size


def test_props(exp: Experiment, exp_reloaded: Experiment) -> None:
    assert exp.name == "2020-02-20_170706"

    assert exp.info() == str(exp) == exp.summary()


def test_reload(exp: Experiment, exp_reloaded: Experiment) -> None:
    assert (exp.welldata == exp_reloaded.welldata).all().all()
    assert exp.name == exp_reloaded.name
    assert exp.protocol == exp_reloaded.protocol
    assert exp.plate_setup == exp_reloaded.plate_setup


def test_welldata_time_columns_are_unix_seconds(exp: Experiment) -> None:
    # Regression: in 0.15.0, _filter_data_v1_pandas converted the polars
    # Datetime[ms] timestamp via .astype("int64") / 1e9, assuming ns precision,
    # which produced timestamps 1e6× too small and huge negative seconds/hours.
    wd = exp.welldata
    assert exp.activestarttime is not None
    start_ts = exp.activestarttime.timestamp()
    ts = wd[("time", "timestamp")]
    secs = wd[("time", "seconds")]
    hours = wd[("time", "hours")]
    # Unix timestamps for these files are post-2010 and pre-2040.
    assert (ts > 1.26e9).all() and (ts < 2.20e9).all()
    # `seconds` is offset from activestarttime, always non-negative and bounded
    # by a year (no single run is longer than that).
    assert (secs >= 0).all() and (secs < 365 * 24 * 3600).all()
    assert np.allclose(secs, ts - start_ts)
    assert np.allclose(hours, secs / 3600.0)


def test_temperatures_polars_long_schema(exp: Experiment) -> None:
    # The default temperatures_polars schema in 0.15+ is long format.
    import polars as pl

    t = exp.temperatures_polars
    assert set(t.columns) >= {"timestamp", "temperature", "zone", "kind", "time"}
    assert t.schema["time"] == pl.Datetime(time_unit="ms", time_zone="UTC")
    # Long format: each (zone, kind) combination contributes its own rows.
    assert set(t["kind"].unique().to_list()) >= {"sample", "block", "heatsink", "cover"}


def test_temperatures_polars_wide_legacy_schema(exp: Experiment) -> None:
    # Opt-in 0.14-style wide schema for callers that depend on it.
    import polars as pl

    t = exp.temperatures_polars_wide
    cols = t.columns
    assert "timestamp" in cols
    sample_cols = [c for c in cols if c.startswith("sample_")]
    block_cols = [c for c in cols if c.startswith("block_")]
    assert len(sample_cols) >= 1
    assert len(sample_cols) == len(block_cols)
    assert "heatsink" in cols
    assert "cover" in cols
    assert t.schema["timestamp"] == pl.Datetime(time_unit="ms", time_zone="UTC")


def test_plot_ntmpw_smoothmw(exp: Experiment) -> None:
    axf, axt = exp.plot_over_time(process=[SmoothWindowMean(4), NormToMeanPerWell(2)], annotate_stage_lines=False)
    assert axf.get_ylabel() == "fluorescence (window mean 4, norm. to mean)"


def test_plot_emw_maxperwell(exp: Experiment) -> None:
    axf, axt = exp.plot_over_time(
        process=[SmoothEMWMean(alpha=0.1), NormToMaxPerWell(2)],
        annotate_stage_lines=False,
    )
    assert axf.get_ylabel() == "fluorescence (EMW-smoothed, norm. to max)"


@pytest.mark.parametrize("wells", [["A3", "A1", "A2"], ["A2", "A3", "A1"]])
def test_plot_over_time_line_order(exp: Experiment, wells) -> None:
    """Lines are plotted, and so colored, in the order the wells were given."""
    import polars as pl

    ax = exp.plot_over_time(
        samples=wells,
        filters="x1-m1",
        temperatures=False,
        stage_lines=False,
        annotate_stage_lines=False,
        annotate_events=False,
    )

    data = exp.filter_data_polars.filter(pl.col("filter_set") == "x1-m1").sort("timestamp")
    expected = [data.filter(pl.col("well") == well)["fluorescence"][0] for well in wells]

    assert [line.get_ydata()[0] for line in ax[0].get_lines()] == expected


def test_plot_over_time_sample_order(exp: Experiment) -> None:
    """Lines follow the order of the samples, with wells in plate order within each."""
    import polars as pl

    ax = exp.plot_over_time(
        samples=["Sample 2", "Sample 1"],
        filters="x1-m1",
        temperatures=False,
        stage_lines=False,
        annotate_stage_lines=False,
        annotate_events=False,
    )

    data = exp.filter_data_polars.filter(pl.col("filter_set") == "x1-m1").sort("timestamp")
    # Sample 2 is A9 and A10, Sample 1 is A7 and A8.
    expected = [data.filter(pl.col("well") == well)["fluorescence"][0] for well in ["A9", "A10", "A7", "A8"]]

    assert [line.get_ydata()[0] for line in ax[0].get_lines()] == expected


@pytest.mark.parametrize(
    "samples,order",
    [
        # Sample 1 is A7 and A8, so A8 is named twice over: it keeps the earlier place.
        (["A8", "Sample 1"], ["A8", "A7"]),
        (["Sample 1", "A8"], ["A7", "A8"]),
        # Sample 2 is A9 and A10, named here only by its wells.
        (["A10", "Sample 1", "A9"], ["A10", "A7", "A8", "A9"]),
    ],
)
def test_plot_over_time_mixed_order(exp: Experiment, samples, order) -> None:
    """Samples and wells together are plotted in the order given, each by its first mention."""
    import polars as pl

    ax = exp.plot_over_time(
        samples=samples,
        filters="x1-m1",
        temperatures=False,
        stage_lines=False,
        annotate_stage_lines=False,
        annotate_events=False,
    )

    data = exp.filter_data_polars.filter(pl.col("filter_set") == "x1-m1").sort("timestamp")
    expected = [data.filter(pl.col("well") == well)["fluorescence"][0] for well in order]

    assert [line.get_ydata()[0] for line in ax[0].get_lines()] == expected


def test_plot_over_time_filter_order(exp: Experiment) -> None:
    """With several filter sets, lines follow the order the filters were given."""
    import polars as pl

    filters = ["x3-m3", "x1-m1"]
    ax = exp.plot_over_time(
        samples=["A1"],
        filters=filters,
        temperatures=False,
        stage_lines=False,
        annotate_stage_lines=False,
        annotate_events=False,
    )

    data = exp.filter_data_polars.filter(pl.col("well") == "A1").sort("timestamp")
    expected = [data.filter(pl.col("filter_set") == f)["fluorescence"][0] for f in filters]

    assert [line.get_ydata()[0] for line in ax[0].get_lines()] == expected


def test_plot_subtrbymean(exp: Experiment) -> None:
    axf, axt = exp.plot_over_time(process=SubtractByMeanPerWell(2), annotate_stage_lines=False)
    assert axf.get_ylabel() == "fluorescence (subtr. by mean)"


def test_plots(exp: Experiment) -> None:
    axf, axt = exp.plot_over_time(legend=False, figure_kw={"constrained_layout": False}, annotate_stage_lines=True)

    assert len(axf.get_lines()) == 5 * len(exp.all_filters)  # + 2 # No +2, because stage lines are outside of plot
    assert np.allclose(axf.get_xlim(), (0.0287576, 0.089), atol=0.01)

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
    expected_filters = {"x1-m1", "x1-m2", "x2-m2", "x2-m3", "x3-m3", "x3-m4", "x4-m4", "x4-m5", "x5-m5", "x5-m6"}

    actual_filters = {str(f) for f in exp.all_filters}

    assert actual_filters == expected_filters
    assert len(exp.all_filters) == 10


def test_filter_strings(exp: Experiment) -> None:
    """Test that filter_strings returns the expected filter strings."""
    expected_filter_strings = ["x1-m1", "x1-m2", "x2-m2", "x2-m3", "x3-m3", "x3-m4", "x4-m4", "x4-m5", "x5-m5", "x5-m6"]

    actual_filter_strings = exp.filter_strings

    # Convert to sets for comparison since order might vary
    assert set(actual_filter_strings) == set(expected_filter_strings)
    assert len(actual_filter_strings) == 10

    # Verify that filter_strings matches string conversion of all_filters
    assert set(actual_filter_strings) == {str(f) for f in exp.all_filters}


def test_save_file_with_dots(exp: Experiment, tmp_path_factory: pytest.TempPathFactory) -> None:  # test for issue #33
    tmp_path = tmp_path_factory.mktemp("exp")
    original_name = exp.name

    try:
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
    finally:
        exp.name = original_name


def test_mid_run_eds():
    exp = Experiment.from_file(_TESTS_DIR / "mid-run.eds")

    assert exp.runstate == "RUNNING"
    assert exp.activeendtime is None
    assert exp.activestarttime == datetime.datetime(
        2025, 11, 18, 1, 52, 11, 949000, tzinfo=datetime.timezone.utc
    )  # datetime.datetime(2025, 11, 18, 1, 52, 11, 949000, tzinfo=datetime.timezone.utc)

    # ps = qslib.PlateSetup({
    #     "s1": ["A1"],
    #     "s2": ["A2", "B4"],
    # })
    prot = qslib.Protocol([qslib.Stage.hold_at(25, "5min", "60s", collect=True)], filters=["x1-m1", "x3-m5", "x2-m2"])

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
