from dataclasses import dataclass
from .data import FilterDataReading
from datetime import datetime, timezone
from typing import ClassVar, Sequence
from typing import TypedDict
import re
from abc import ABCMeta, abstractmethod
import polars as pl


class PolarsProcessor(metaclass=ABCMeta):
    @abstractmethod
    def process(self, data: pl.LazyFrame) -> pl.LazyFrame:  # pragma: no cover
        ...

    @abstractmethod
    def ylabel(self, previous_label: str | None = None) -> str:  # pragma: no cover
        ...


def polars_from_filterdata(dr: FilterDataReading, start_time: float | None = None) -> "pl.LazyFrame":
    import polars as pl

    d = pl.DataFrame(
        {
            "filter_set": dr.filter_set.lowerform,
            "stage": dr.stage,
            "cycle": dr.cycle,
            "step": dr.step,
            "point": dr.point,
            "well": [f"{r}{c}" for r in "ABCDEFGHIJKLMNOPQRSTUVWXYZ"[:dr.plate_rows] for c in range(1, dr.plate_cols + 1)],
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
        zone = 1 + (pl.col("column") // block_width),
        timestamp = (pl.col("timestamp")*1000).cast(pl.Datetime(time_unit="ms", time_zone="UTC"))
    )
    if start_time is not None:
        start_time = datetime.fromtimestamp(start_time, tz=timezone.utc)
        d = d.with_columns(
            (pl.col("timestamp") - pl.lit(start_time)).alias("time_since_start"),
        )
    return d


def match_expr(
    stage: int | Sequence[int] | range | None = None,
    cycle: int | Sequence[int] | range | None = None,
    step: int | Sequence[int] | range | None = None,
    point: int | Sequence[int] | range | None = None,
    expr: pl.Expr | None = None,
):
    fexpr = True
    match stage:
        case int():
            fexpr = fexpr & (pl.col("stage") == stage)
        case x if isinstance(x, Sequence):
            fexpr = fexpr & pl.col("stage").is_in(stage)
        case range():
            fexpr = fexpr & pl.col("stage").is_in(range(stage.start, stage.stop))
    match cycle:
        case int():
            fexpr = fexpr & (pl.col("cycle") == cycle)
        case x if isinstance(x, Sequence):
            fexpr = fexpr & pl.col("cycle").is_in(cycle)
        case range():
            fexpr = fexpr & pl.col("cycle").is_in(range(cycle.start, cycle.stop))
    match step:
        case int():
            fexpr = fexpr & (pl.col("step") == step)
        case x if isinstance(x, Sequence):
            fexpr = fexpr & pl.col("step").is_in(step)
        case range():
            fexpr = fexpr & pl.col("step").is_in(range(step.start, step.stop))
    match point:
        case int():
            fexpr = fexpr & (pl.col("point") == point)
        case x if isinstance(x, Sequence):
            fexpr = fexpr & pl.col("point").is_in(point)
        case range():
            fexpr = fexpr & pl.col("point").is_in(range(point.start, point.stop))
    if expr is not None:
        fexpr = fexpr & expr
    return fexpr

def match_expr_single(stage: int | None = None, cycle: int | None = None, step: int | None = None, point: int | None = None):
    return match_expr(stage=stage, cycle=cycle, step=step, point=point)

class FilterDict(TypedDict):
    stage: int | Sequence[int] | range | None
    cycle: int | Sequence[int] | range | None
    step: int | Sequence[int] | range | None
    point: int | Sequence[int] | range | None
    expr: pl.Expr | None = None

class NormRaw(PolarsProcessor):
    def process(self, data: pl.LazyFrame) -> pl.LazyFrame:
        return data

    def ylabel(self, previous_label: str | None = None) -> str:
        if previous_label == "fluorescence":
            return "fluorescence"
        return re.sub(r"\(([^)]+)\)", r"(\1, raw)", previous_label)

@dataclass(init=False)
class NormToMeanPerWell(PolarsProcessor):
    """
    A Processor that divides the fluorescence reading for each (filterset, well) pair
    by the mean value of that pair within a particular selection of data.

    The easiest way to use this is to give a particular stage (all data in that
    stage will be used), or a stage and set of cycles (those cycles in that stage
    will be used).  For example:

    - To normalize to the mean stage 8 values, use `NormToMeanPerWell(stage=8)`.
    - To normalize to the first 5 cycles of stage 2, use
        NormToMeanPerWell(stage=2, cycle=slice(1, 6)).

    `selection` allows arbitrary Pandas indexing (without the filter_set level
    of the MultiIndex) for unusual cases.
    """

    selection: pl.Expr | None
    scope: ClassVar[str] = "limited"

    def __init__(
        self,
        stage: int | slice | Sequence[int] | None = None,
        step: int | slice | Sequence[int] | None = None,
        cycle: int | slice | Sequence[int] | None = None,
        point: int | slice | Sequence[int] | None = None,
        *,
        expr: pl.Expr | None = None,
    ):
        self.selection = match_expr(stage=stage, step=step, cycle=cycle, point=point, expr=expr)

    def ylabel(self, previous_label: str | None = None) -> str:
        if previous_label == "fluorescence":
            return "fluorescence (norm. to mean)"
        return re.sub(r"\(([^)]+)\)", r"(\1, norm. to mean)", previous_label)

    def process(self, data: pl.LazyFrame) -> pl.LazyFrame:
        return data.with_columns(
            pl.col("processed_fluorescence") / pl.col("processed_fluorescence").filter(self.selection).mean().over("well", "filter_set")
        )


@dataclass(init=False)
class NormToMaxPerWell(PolarsProcessor):
    """
    A Processor that divides the fluorescence reading for each (filterset, well) pair
    by the max value of that pair within a particular selection of data.
    """

    selection: pl.Expr | None
    scope: ClassVar[str] = "limited"

    def __init__(
        self,
        stage: int | slice | Sequence[int] | None = None,
        step: int | slice | Sequence[int] | None = None,
        cycle: int | slice | Sequence[int] | None = None,
        point: int | slice | Sequence[int] | None = None,
        *,
        expr: pl.Expr | None = None,
    ):
        self.selection = match_expr(stage=stage, step=step, cycle=cycle, point=point, expr=expr)

    def ylabel(self, previous_label: str | None = None) -> str:
        if previous_label == "fluorescence":
            return "fluorescence (norm. to max)"
        return re.sub(r"\(([^)]+)\)", r"(\1, norm. to max)", previous_label)

    def process(self, data: pl.LazyFrame) -> pl.LazyFrame:
        return data.with_columns(
            pl.col("processed_fluorescence") / pl.col("processed_fluorescence").filter(self.selection).max().over("well", "filter_set")
        )

@dataclass(init=False)
class SubtractByMeanPerWell(PolarsProcessor):
    """
    A Processor that subtracts the fluorescence reading for each (filterset, well) pair
    by the mean value of that pair within a particular selection of data.
    """

    selection: pl.Expr | None
    scope: ClassVar[str] = "limited"

    def __init__(
        self,
        stage: int | slice | Sequence[int] | None = None,
        step: int | slice | Sequence[int] | None = None,
        cycle: int | slice | Sequence[int] | None = None,
        point: int | slice | Sequence[int] | None = None,
        *,
        expr: pl.Expr | None = None,
    ):
        self.selection = match_expr(stage=stage, step=step, cycle=cycle, point=point, expr=expr)

    def ylabel(self, previous_label: str | None = None) -> str:
        if previous_label == "fluorescence":
            return "fluorescence (subtr. by mean)"
        return re.sub(r"\(([^)]+)\)", r"(\1, subtr. by mean)", previous_label)

    def process(self, data: pl.LazyFrame) -> pl.LazyFrame:
        return data.with_columns(
            pl.col("processed_fluorescence") - pl.col("processed_fluorescence").filter(self.selection).mean().over("well", "filter_set")
        )

@dataclass
class SmoothWindowMean(PolarsProcessor):
    """
    A Processor that smooths fluorescence readings using Pandas' Rolling,
    and mean.
    """

    window: int
    min_samples: int | None = None
    center: bool = False
    scope: ClassVar = "limited"

    def process(self, data: pl.LazyFrame) -> pl.LazyFrame:
        return data.with_columns(
            pl.col("processed_fluorescence").rolling_mean(
                window_size=self.window,
                min_periods=self.min_samples,
                center=self.center,
            )
        )

    def ylabel(self, previous_label: str | None = None) -> str:
        if previous_label == "fluorescence":
            return f"fluorescence (window mean {self.window})"
        return re.sub(
            r"\(([^)]+)\)", rf"(\1, window mean {self.window})", previous_label
        )


@dataclass
class SmoothEMWMean(PolarsProcessor):
    """
    A Processor that smooths fluorescence readings using Pandas' Exponential Moving Window
    (ewm / exponentially weighted moving-average).
    """

    com: float | None = None
    span: float | None = None
    halflife: float | str | None = None
    alpha: float | None = None
    min_periods: int = 0
    adjust: bool = True
    ignore_na: bool = False

    def process(self, data: pl.LazyFrame) -> pl.LazyFrame:
        return data.with_columns(
            pl.col("processed_fluorescence").ewm_mean(
                com=self.com,
                span=self.span,
                half_life=self.halflife,
                alpha=self.alpha,
            )
        )

    def ylabel(self, previous_label: str | None = None) -> str:
        if previous_label == "fluorescence":
            return "fluorescence (EMW-smoothed)"
        return re.sub(r"\(([^)]+)\)", r"(\1, EMW-smoothed)", previous_label)

def norm_zero_to_one_per_well(zero_filter: FilterDict, one_filter: FilterDict):
    norm_fl_a =  pl.col("fluorescence") - pl.col("fluorescence").filter(match_expr(**zero_filter)).mean().over("well", "filter_set")
    return (
        norm_fl_a / norm_fl_a.filter(match_expr(**one_filter)).mean().over("well", "filter_set")
    )

def polars_process(data: pl.LazyFrame, processors, ylabel: str | None = None):
    data = data.with_columns(pl.col("fluorescence").alias("processed_fluorescence"))
    if not isinstance(processors, Sequence):
        processors = [processors]
    for processor in processors:
        if isinstance(processor, pl.Expr):
            data = data.with_columns(processor)
        else:
            data = processor.process(data)
            if ylabel is not None:
                ylabel = processor.ylabel(ylabel)
    if ylabel is not None:
        return data, ylabel
    else:
        return data