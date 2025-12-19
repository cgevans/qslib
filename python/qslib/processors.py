# SPDX-FileCopyrightText: 2021 - 2023 Constantine Evans <qslib@mb.costi.net>
#
# SPDX-License-Identifier: EUPL-1.2

"""
Fluorescence data processors supporting both Polars and Pandas DataFrames.

This module provides processor classes that can work with either Polars or Pandas
data without requiring the user to choose. The `process()` method automatically
detects the input type and applies the appropriate implementation.
"""

from __future__ import annotations

import re
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Sequence, TypedDict, Union

import polars as pl

if TYPE_CHECKING:
    import pandas as pd

ScopeType = Literal["all", "limited"]

# Type alias for DataFrame inputs
DataFrameType = Union["pl.LazyFrame", "pl.DataFrame", "pd.DataFrame"]


def _is_pandas(data: DataFrameType) -> bool:
    """Check if data is a Pandas DataFrame."""
    return type(data).__module__.startswith("pandas")


def _is_polars(data: DataFrameType) -> bool:
    """Check if data is a Polars DataFrame or LazyFrame."""
    return isinstance(data, (pl.DataFrame, pl.LazyFrame))


class Processor(metaclass=ABCMeta):
    """
    Base class for fluorescence data processors.

    Processors transform fluorescence data (normalization, smoothing, etc.) and work
    with both Polars and Pandas DataFrames automatically. The `process()` method
    detects the input type and applies the appropriate implementation.

    For Pandas, there's also `process_scoped()` for scope-aware processing.
    """

    scope: ClassVar[ScopeType] = "limited"

    def process(self, data: DataFrameType) -> DataFrameType:
        """
        Process the data, auto-detecting whether it's Polars or Pandas.

        Parameters
        ----------
        data : pl.LazyFrame, pl.DataFrame, or pd.DataFrame
            The fluorescence data to process.

        Returns
        -------
        Same type as input
            The processed data.
        """
        if _is_polars(data):
            return self._process_polars(data)
        elif _is_pandas(data):
            return self._process_pandas(data)
        else:
            raise TypeError(f"Unsupported data type: {type(data)}")

    def process_scoped(self, data: "pd.DataFrame", scope: ScopeType) -> "pd.DataFrame":
        """
        Process Pandas data only if scope matches this processor's scope.

        This is useful for writing scope-agnostic code, provided that you call this
        for every scope before using the data.

        Parameters
        ----------
        data : pd.DataFrame
            The fluorescence data to process.
        scope : "all" or "limited"
            - "all": the entire welldata array.
            - "limited": all time points, but limited to the filter sets and samples
              being plotted.

        Returns
        -------
        pd.DataFrame
            The processed data (or unchanged if scope doesn't match).
        """
        if scope == self.scope:
            return self._process_pandas(data)
        return data

    @abstractmethod
    def _process_polars(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """Process Polars data. Must be implemented by subclasses."""
        ...

    @abstractmethod
    def _process_pandas(self, data: "pd.DataFrame") -> "pd.DataFrame":
        """Process Pandas data. Must be implemented by subclasses."""
        ...

    @abstractmethod
    def ylabel(self, previous_label: str | None = None) -> str:
        """Return a y-axis label describing this processor's transformation."""
        ...


# Backward compatibility aliases
PolarsProcessor = Processor
PandasProcessor = Processor


# --- Helper functions for Polars ---


def polars_from_filterdata(dr: "FilterDataReading", start_time: float | None = None) -> pl.LazyFrame:
    """Convert FilterDataReading to Polars LazyFrame."""

    d = pl.DataFrame(
        {
            "filter_set": dr.filter_set.lowerform,
            "stage": dr.stage,
            "cycle": dr.cycle,
            "step": dr.step,
            "point": dr.point,
            "well": [
                f"{r}{c}"
                for r in "ABCDEFGHIJKLMNOPQRSTUVWXYZ"[: dr.plate_rows]
                for c in range(1, dr.plate_cols + 1)
            ],
            "row": [i for i in range(dr.plate_rows) for _ in range(dr.plate_cols)],
            "column": [j for _ in range(dr.plate_rows) for j in range(dr.plate_cols)],
            "timestamp": dr.timestamp,
            "fluorescence": dr.well_fluorescence,
            "sample_temperature": dr.well_temperatures,
            "exposure": dr.exposure,
        }
    ).lazy()

    block_width = dr.plate_cols // len(dr.temperatures)
    d = d.with_columns(
        zone=1 + (pl.col("column") // block_width),
        timestamp=(pl.col("timestamp") * 1000).cast(pl.Datetime(time_unit="ms", time_zone="UTC")),
    )
    if start_time is not None:
        start_time_dt = datetime.fromtimestamp(start_time, tz=timezone.utc)
        d = d.with_columns(
            (pl.col("timestamp") - pl.lit(start_time_dt)).alias("time_since_start"),
        )
    return d


def match_expr(
    stage: int | Sequence[int] | range | None = None,
    cycle: int | Sequence[int] | range | None = None,
    step: int | Sequence[int] | range | None = None,
    point: int | Sequence[int] | range | None = None,
    expr: pl.Expr | None = None,
) -> pl.Expr:
    """Build a Polars filter expression from stage/cycle/step/point constraints."""
    fexpr: pl.Expr | bool = True
    match stage:
        case int():
            fexpr = fexpr & (pl.col("stage") == stage)
        case x if isinstance(x, Sequence):
            fexpr = fexpr & pl.col("stage").is_in(list(stage))
        case range():
            fexpr = fexpr & pl.col("stage").is_in(list(stage))
    match cycle:
        case int():
            fexpr = fexpr & (pl.col("cycle") == cycle)
        case x if isinstance(x, Sequence):
            fexpr = fexpr & pl.col("cycle").is_in(list(cycle))
        case range():
            fexpr = fexpr & pl.col("cycle").is_in(list(cycle))
    match step:
        case int():
            fexpr = fexpr & (pl.col("step") == step)
        case x if isinstance(x, Sequence):
            fexpr = fexpr & pl.col("step").is_in(list(step))
        case range():
            fexpr = fexpr & pl.col("step").is_in(list(step))
    match point:
        case int():
            fexpr = fexpr & (pl.col("point") == point)
        case x if isinstance(x, Sequence):
            fexpr = fexpr & pl.col("point").is_in(list(point))
        case range():
            fexpr = fexpr & pl.col("point").is_in(list(point))
    if expr is not None:
        fexpr = fexpr & expr
    return fexpr


def match_expr_single(
    stage: int | None = None, cycle: int | None = None, step: int | None = None, point: int | None = None
) -> pl.Expr:
    """Build a Polars filter expression for single values."""
    return match_expr(stage=stage, cycle=cycle, step=step, point=point)


class FilterDict(TypedDict, total=False):
    stage: int | Sequence[int] | range | None
    cycle: int | Sequence[int] | range | None
    step: int | Sequence[int] | range | None
    point: int | Sequence[int] | range | None
    expr: pl.Expr | None


# --- Processor implementations ---


class NormRaw(Processor):
    """
    A Processor that takes no arguments and passes through raw fluorescence values.
    """

    def _process_polars(self, data: pl.LazyFrame) -> pl.LazyFrame:
        return data

    def _process_pandas(self, data: "pd.DataFrame") -> "pd.DataFrame":
        return data

    def ylabel(self, previous_label: str | None = None) -> str:
        if previous_label is None or previous_label == "fluorescence":
            return "fluorescence (AU)"
        return re.sub(r"\(([^)]+)\)", r"(\1, AU)", previous_label)


@dataclass(init=False)
class NormToMeanPerWell(Processor):
    """
    A Processor that divides the fluorescence reading for each (filterset, well) pair
    by the mean value of that pair within a particular selection of data.

    The easiest way to use this is to give a particular stage (all data in that
    stage will be used), or a stage and set of cycles (those cycles in that stage
    will be used).  For example:

    - To normalize to the mean stage 8 values, use `NormToMeanPerWell(stage=8)`.
    - To normalize to the first 5 cycles of stage 2, use
        NormToMeanPerWell(stage=2, cycle=range(1, 6)).

    For Polars, use `expr` for custom filter expressions.
    For Pandas, use `selection` for arbitrary indexing.
    """

    # Polars selection (filter expression)
    _polars_selection: pl.Expr | None
    # Pandas selection (index tuple)
    _pandas_selection: Any
    scope: ClassVar[ScopeType] = "limited"

    def __init__(
        self,
        stage: int | slice | Sequence[int] | range | None = None,
        step: int | slice | Sequence[int] | range | None = None,
        cycle: int | slice | Sequence[int] | range | None = None,
        point: int | slice | Sequence[int] | range | None = None,
        *,
        expr: pl.Expr | None = None,
        selection: Any | None = None,
    ):
        # Build Polars expression
        polars_stage = _convert_slice_to_range(stage) if isinstance(stage, slice) else stage
        polars_step = _convert_slice_to_range(step) if isinstance(step, slice) else step
        polars_cycle = _convert_slice_to_range(cycle) if isinstance(cycle, slice) else cycle
        polars_point = _convert_slice_to_range(point) if isinstance(point, slice) else point
        self._polars_selection = match_expr(
            stage=polars_stage, step=polars_step, cycle=polars_cycle, point=polars_point, expr=expr
        )

        # Build Pandas selection
        if selection is not None:
            self._pandas_selection = selection
        else:
            self._pandas_selection = _build_pandas_selection(stage, step, cycle)

    def _process_polars(self, data: pl.LazyFrame) -> pl.LazyFrame:
        return data.with_columns(
            pl.col("processed_fluorescence")
            / pl.col("processed_fluorescence").filter(self._polars_selection).mean().over("well", "filter_set")
        )

    def _process_pandas(self, data: "pd.DataFrame") -> "pd.DataFrame":
        normdata = data.copy()
        means = (
            data.loc[(slice(None), *self._pandas_selection), (slice(None), "fl")].groupby("filter_set").mean()
        )
        normdata.loc[:, (slice(None), "fl")] /= means
        return normdata

    def ylabel(self, previous_label: str | None = None) -> str:
        if previous_label is None or previous_label == "fluorescence":
            return "fluorescence (norm. to mean)"
        return re.sub(r"\(([^)]+)\)", r"(\1, norm. to mean)", previous_label)


@dataclass(init=False)
class NormToMaxPerWell(Processor):
    """
    A Processor that divides the fluorescence reading for each (filterset, well) pair
    by the max value of that pair within a particular selection of data.

    See NormToMeanPerWell for usage examples.
    """

    _polars_selection: pl.Expr | None
    _pandas_selection: Any
    scope: ClassVar[ScopeType] = "limited"

    def __init__(
        self,
        stage: int | slice | Sequence[int] | range | None = None,
        step: int | slice | Sequence[int] | range | None = None,
        cycle: int | slice | Sequence[int] | range | None = None,
        point: int | slice | Sequence[int] | range | None = None,
        *,
        expr: pl.Expr | None = None,
        selection: Any | None = None,
    ):
        polars_stage = _convert_slice_to_range(stage) if isinstance(stage, slice) else stage
        polars_step = _convert_slice_to_range(step) if isinstance(step, slice) else step
        polars_cycle = _convert_slice_to_range(cycle) if isinstance(cycle, slice) else cycle
        polars_point = _convert_slice_to_range(point) if isinstance(point, slice) else point
        self._polars_selection = match_expr(
            stage=polars_stage, step=polars_step, cycle=polars_cycle, point=polars_point, expr=expr
        )

        if selection is not None:
            self._pandas_selection = selection
        else:
            self._pandas_selection = _build_pandas_selection(stage, step, cycle)

    def _process_polars(self, data: pl.LazyFrame) -> pl.LazyFrame:
        return data.with_columns(
            pl.col("processed_fluorescence")
            / pl.col("processed_fluorescence").filter(self._polars_selection).max().over("well", "filter_set")
        )

    def _process_pandas(self, data: "pd.DataFrame") -> "pd.DataFrame":
        normdata = data.copy()
        maxes = data.loc[(slice(None), *self._pandas_selection), (slice(None), "fl")].groupby("filter_set").max()
        normdata.loc[:, (slice(None), "fl")] /= maxes
        return normdata

    def ylabel(self, previous_label: str | None = None) -> str:
        if previous_label is None or previous_label == "fluorescence":
            return "fluorescence (norm. to max)"
        return re.sub(r"\(([^)]+)\)", r"(\1, norm. to max)", previous_label)


@dataclass(init=False)
class SubtractByMeanPerWell(Processor):
    """
    A Processor that subtracts the fluorescence reading for each (filterset, well) pair
    by the mean value of that pair within a particular selection of data.

    See NormToMeanPerWell for usage examples.
    """

    _polars_selection: pl.Expr | None
    _pandas_selection: Any
    scope: ClassVar[ScopeType] = "limited"

    def __init__(
        self,
        stage: int | slice | Sequence[int] | range | None = None,
        step: int | slice | Sequence[int] | range | None = None,
        cycle: int | slice | Sequence[int] | range | None = None,
        point: int | slice | Sequence[int] | range | None = None,
        *,
        expr: pl.Expr | None = None,
        selection: Any | None = None,
    ):
        polars_stage = _convert_slice_to_range(stage) if isinstance(stage, slice) else stage
        polars_step = _convert_slice_to_range(step) if isinstance(step, slice) else step
        polars_cycle = _convert_slice_to_range(cycle) if isinstance(cycle, slice) else cycle
        polars_point = _convert_slice_to_range(point) if isinstance(point, slice) else point
        self._polars_selection = match_expr(
            stage=polars_stage, step=polars_step, cycle=polars_cycle, point=polars_point, expr=expr
        )

        if selection is not None:
            self._pandas_selection = selection
        else:
            self._pandas_selection = _build_pandas_selection(stage, step, cycle)

    def _process_polars(self, data: pl.LazyFrame) -> pl.LazyFrame:
        return data.with_columns(
            pl.col("processed_fluorescence")
            - pl.col("processed_fluorescence").filter(self._polars_selection).mean().over("well", "filter_set")
        )

    def _process_pandas(self, data: "pd.DataFrame") -> "pd.DataFrame":
        normdata = data.copy()
        means = (
            data.loc[(slice(None), *self._pandas_selection), (slice(None), "fl")].groupby("filter_set").mean()
        )
        normdata.loc[:, (slice(None), "fl")] -= means
        return normdata

    def ylabel(self, previous_label: str | None = None) -> str:
        if previous_label is None or previous_label == "fluorescence":
            return "fluorescence (subtr. by mean)"
        return re.sub(r"\(([^)]+)\)", r"(\1, subtr. by mean)", previous_label)


@dataclass
class SmoothWindowMean(Processor):
    """
    A Processor that smooths fluorescence readings using a rolling window mean.
    """

    window: int
    min_periods: int | None = None
    center: bool = False
    win_type: str | None = None  # Pandas only
    closed: str | None = None  # Pandas only
    scope: ClassVar[ScopeType] = "limited"

    def _process_polars(self, data: pl.LazyFrame) -> pl.LazyFrame:
        return data.with_columns(
            pl.col("processed_fluorescence").rolling_mean(
                window_size=self.window,
                min_periods=self.min_periods,
                center=self.center,
            )
        )

    def _process_pandas(self, data: "pd.DataFrame") -> "pd.DataFrame":
        return data.rolling(
            window=self.window,
            min_periods=self.min_periods,
            center=self.center,
            win_type=self.win_type,
            closed=self.closed,
        ).mean()

    def ylabel(self, previous_label: str | None = None) -> str:
        if previous_label is None or previous_label == "fluorescence":
            return f"fluorescence (window mean {self.window})"
        return re.sub(r"\(([^)]+)\)", rf"(\1, window mean {self.window})", previous_label)


@dataclass
class SmoothEMWMean(Processor):
    """
    A Processor that smooths fluorescence readings using an exponentially weighted
    moving average (EWMA).
    """

    com: float | None = None
    span: float | None = None
    halflife: float | str | timedelta | None = None
    alpha: float | None = None
    min_periods: int = 0
    adjust: bool = True  # Pandas only
    ignore_na: bool = False  # Pandas only
    scope: ClassVar[ScopeType] = "limited"

    def _process_polars(self, data: pl.LazyFrame) -> pl.LazyFrame:
        # Polars ewm_mean uses half_life (with underscore), not halflife
        half_life = self.halflife if not isinstance(self.halflife, timedelta) else None
        return data.with_columns(
            pl.col("processed_fluorescence").ewm_mean(
                com=self.com,
                span=self.span,
                half_life=half_life,
                alpha=self.alpha,
            )
        )

    def _process_pandas(self, data: "pd.DataFrame") -> "pd.DataFrame":
        return data.ewm(
            com=self.com,
            span=self.span,
            halflife=self.halflife,
            alpha=self.alpha,
            min_periods=self.min_periods,
            adjust=self.adjust,
            ignore_na=self.ignore_na,
        ).mean()

    def ylabel(self, previous_label: str | None = None) -> str:
        if previous_label is None or previous_label == "fluorescence":
            return "fluorescence (EMW-smoothed)"
        return re.sub(r"\(([^)]+)\)", r"(\1, EMW-smoothed)", previous_label)


# --- Processing functions ---


def polars_process(
    data: pl.LazyFrame, processors: Sequence[Processor] | Processor | pl.Expr, ylabel: str | None = None
) -> pl.LazyFrame | tuple[pl.LazyFrame, str]:
    """
    Apply processors to Polars data.

    Parameters
    ----------
    data : pl.LazyFrame
        The fluorescence data.
    processors : Processor, list of Processors, or pl.Expr
        The processor(s) to apply.
    ylabel : str, optional
        If provided, return (data, updated_ylabel) tuple.

    Returns
    -------
    pl.LazyFrame or (pl.LazyFrame, str)
        Processed data, optionally with updated ylabel.
    """
    data = data.with_columns(pl.col("fluorescence").alias("processed_fluorescence"))
    if not isinstance(processors, Sequence):
        processors = [processors]
    for processor in processors:
        if isinstance(processor, pl.Expr):
            data = data.with_columns(processor)
        else:
            data = processor._process_polars(data)
            if ylabel is not None:
                ylabel = processor.ylabel(ylabel)
    if ylabel is not None:
        return data, ylabel
    else:
        return data


def pandas_process(
    data: "pd.DataFrame",
    processors: Sequence[Processor] | Processor,
    scope: ScopeType = "limited",
    ylabel: str | None = None,
) -> "pd.DataFrame" | tuple["pd.DataFrame", str]:
    """
    Apply processors to Pandas data.

    Parameters
    ----------
    data : pd.DataFrame
        The fluorescence data.
    processors : Processor or list of Processors
        The processor(s) to apply.
    scope : "all" or "limited"
        The scope for process_scoped.
    ylabel : str, optional
        If provided, return (data, updated_ylabel) tuple.

    Returns
    -------
    pd.DataFrame or (pd.DataFrame, str)
        Processed data, optionally with updated ylabel.
    """
    if not isinstance(processors, Sequence):
        processors = [processors]
    for processor in processors:
        data = processor.process_scoped(data, scope)
        if ylabel is not None:
            ylabel = processor.ylabel(ylabel)
    if ylabel is not None:
        return data, ylabel
    else:
        return data


# --- Helper functions ---


def _convert_slice_to_range(s: slice | Any) -> range | Any:
    """Convert a slice to a range for Polars compatibility."""
    if isinstance(s, slice):
        start = s.start if s.start is not None else 0
        stop = s.stop if s.stop is not None else 1000000  # Large default
        step = s.step if s.step is not None else 1
        return range(start, stop, step)
    return s


def _build_pandas_selection(
    stage: int | slice | Sequence[int] | range | None,
    step: int | slice | Sequence[int] | range | None,
    cycle: int | slice | Sequence[int] | range | None,
) -> tuple:
    """Build a Pandas selection tuple from stage/step/cycle."""
    if stage is None:
        stage = slice(None)
    elif isinstance(stage, (int, range)):
        stage = [stage] if isinstance(stage, int) else list(stage)
    if step is None:
        step = slice(None)
    elif isinstance(step, (int, range)):
        step = [step] if isinstance(step, int) else list(step)
    if cycle is None:
        cycle = slice(None)
    elif isinstance(cycle, (int, range)):
        cycle = [cycle] if isinstance(cycle, int) else list(cycle)

    return (stage, cycle, step)


def norm_zero_to_one_per_well(zero_filter: FilterDict, one_filter: FilterDict) -> pl.Expr:
    """
    Create a Polars expression that normalizes fluorescence to a 0-1 range.

    Parameters
    ----------
    zero_filter : FilterDict
        Filter for data points to use as the "zero" baseline.
    one_filter : FilterDict
        Filter for data points to use as the "one" level.

    Returns
    -------
    pl.Expr
        Expression that computes normalized fluorescence.
    """
    norm_fl_a = pl.col("fluorescence") - pl.col("fluorescence").filter(match_expr(**zero_filter)).mean().over(
        "well", "filter_set"
    )
    return norm_fl_a / norm_fl_a.filter(match_expr(**one_filter)).mean().over("well", "filter_set")
