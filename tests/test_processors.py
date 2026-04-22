# SPDX-FileCopyrightText: 2021-2023 Constantine Evans <qslib@mb.costi.net>
# SPDX-License-Identifier: EUPL-1.2

"""Unit tests for qslib.processors — Polars-based processor logic."""

import polars as pl
import pytest

from qslib.processors import (
    NormRaw,
    NormToMeanPerWell,
    NormToMaxPerWell,
    SubtractByMeanPerWell,
    SmoothWindowMean,
    SmoothEMWMean,
    match_expr,
    polars_process,
    _is_polars,
)


def _make_test_data() -> pl.LazyFrame:
    """Create a small synthetic LazyFrame mimicking fluorescence data."""
    return pl.DataFrame(
        {
            "well": ["A1"] * 10 + ["A2"] * 10,
            "filter_set": ["x1-m4"] * 20,
            "stage": [1] * 20,
            "cycle": list(range(1, 11)) * 2,
            "step": [1] * 20,
            "point": [1] * 20,
            "fluorescence": [100.0 + i for i in range(10)]
                + [200.0 + i for i in range(10)],
        }
    ).lazy()


def _make_processed_data() -> pl.LazyFrame:
    """Create data with processed_fluorescence column, as polars_process would."""
    df = _make_test_data()
    return df.with_columns(pl.col("fluorescence").alias("processed_fluorescence"))


# --- NormRaw ---


def test_norm_raw_passthrough():
    data = _make_processed_data()
    proc = NormRaw()
    result = proc._process_polars(data).collect()
    expected = data.collect()
    assert result.equals(expected)


def test_norm_raw_ylabel_default():
    proc = NormRaw()
    assert proc.ylabel() == "fluorescence (AU)"


def test_norm_raw_ylabel_with_previous():
    proc = NormRaw()
    assert "AU" in proc.ylabel("temperature (K)")


# --- NormToMeanPerWell ---


def test_norm_to_mean_per_well():
    data = _make_processed_data()
    proc = NormToMeanPerWell()
    result = proc._process_polars(data).collect()
    # After normalization, mean of processed_fluorescence per well should be 1.0
    means = result.group_by("well").agg(
        pl.col("processed_fluorescence").mean().alias("mean")
    )
    for row in means.iter_rows(named=True):
        assert abs(row["mean"] - 1.0) < 1e-10


# --- NormToMaxPerWell ---


def test_norm_to_max_per_well():
    data = _make_processed_data()
    proc = NormToMaxPerWell()
    result = proc._process_polars(data).collect()
    # After normalization, max of processed_fluorescence per well should be 1.0
    maxes = result.group_by("well").agg(
        pl.col("processed_fluorescence").max().alias("max")
    )
    for row in maxes.iter_rows(named=True):
        assert abs(row["max"] - 1.0) < 1e-10


# --- SubtractByMeanPerWell ---


def test_subtract_by_mean_per_well():
    data = _make_processed_data()
    proc = SubtractByMeanPerWell()
    result = proc._process_polars(data).collect()
    # After subtraction, mean of processed_fluorescence per well should be ~0
    means = result.group_by("well").agg(
        pl.col("processed_fluorescence").mean().alias("mean")
    )
    for row in means.iter_rows(named=True):
        assert abs(row["mean"]) < 1e-10


# --- SmoothWindowMean ---


def test_smooth_window_mean():
    data = _make_processed_data()
    proc = SmoothWindowMean(window=3)
    result = proc._process_polars(data).collect()
    # Should have same shape
    assert result.shape == data.collect().shape
    # First value should be null (window not full)
    pf = result["processed_fluorescence"]
    assert pf[0] is None


# --- SmoothEMWMean ---


def test_smooth_emw_mean():
    data = _make_processed_data()
    proc = SmoothEMWMean(span=3)
    result = proc._process_polars(data).collect()
    assert result.shape == data.collect().shape


# --- match_expr ---


def test_match_expr_stage_int():
    expr = match_expr(stage=1)
    df = pl.DataFrame({"stage": [1, 2, 3], "val": [10, 20, 30]}).lazy()
    result = df.filter(expr).collect()
    assert result.height == 1
    assert result["stage"][0] == 1


def test_match_expr_stage_sequence():
    expr = match_expr(stage=[1, 3])
    df = pl.DataFrame({"stage": [1, 2, 3], "val": [10, 20, 30]}).lazy()
    result = df.filter(expr).collect()
    assert result.height == 2


def test_match_expr_cycle_range():
    expr = match_expr(cycle=range(2, 5))
    df = pl.DataFrame({"cycle": [1, 2, 3, 4, 5], "val": [10, 20, 30, 40, 50]}).lazy()
    result = df.filter(expr).collect()
    assert result.height == 3
    assert result["cycle"].to_list() == [2, 3, 4]


def test_match_expr_combined():
    expr = match_expr(stage=1, cycle=2)
    df = pl.DataFrame(
        {"stage": [1, 1, 2], "cycle": [1, 2, 2], "val": [10, 20, 30]}
    ).lazy()
    result = df.filter(expr).collect()
    assert result.height == 1
    assert result["val"][0] == 20


# --- polars_process ---


def test_polars_process_single():
    data = _make_test_data()
    result = polars_process(data, NormRaw())
    collected = result.collect()
    assert "processed_fluorescence" in collected.columns


def test_polars_process_chain():
    data = _make_test_data()
    result = polars_process(data, [NormRaw(), SubtractByMeanPerWell()])
    collected = result.collect()
    means = collected.group_by("well").agg(
        pl.col("processed_fluorescence").mean().alias("mean")
    )
    for row in means.iter_rows(named=True):
        assert abs(row["mean"]) < 1e-10


def test_polars_process_with_ylabel():
    data = _make_test_data()
    result, ylabel = polars_process(data, NormRaw(), ylabel="fluorescence")
    assert "AU" in ylabel


# --- Type detection helpers ---


def test_is_polars_dataframe():
    df = pl.DataFrame({"a": [1, 2]})
    assert _is_polars(df) is True


def test_is_polars_lazyframe():
    lf = pl.DataFrame({"a": [1, 2]}).lazy()
    assert _is_polars(lf) is True
