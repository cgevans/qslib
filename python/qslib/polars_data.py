from .data import FilterDataReading
from datetime import datetime
import numpy as np
from typing import Literal, Sequence
from typing import TypedDict


try:
    import polars as pl
except ImportError:
    raise ImportError("polars is not installed")


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
        timestamp = (pl.col("timestamp")*1000).cast(pl.Datetime(time_unit="ms"))
    )
    if start_time is not None:
        start_time = datetime.fromtimestamp(start_time)
        d = d.with_columns(
            (pl.col("timestamp") - pl.lit(start_time)).alias("time_since_start"),
        )
    return d


def match_expr(
    stages: int | Sequence[int] | range | None = None,
    cycles: int | Sequence[int] | range | None = None,
    steps: int | Sequence[int] | range | None = None,
    points: int | Sequence[int] | range | None = None,
):
    fexpr = True
    match stages:
        case int():
            fexpr = fexpr & (pl.col("stage") == stages)
        case x if isinstance(x, Sequence):
            fexpr = fexpr & pl.col("stage").is_in(stages)
        case range():
            fexpr = fexpr & pl.col("stage").is_in(range(stages.start, stages.stop))
    match cycles:
        case int():
            fexpr = fexpr & (pl.col("cycle") == cycles)
        case x if isinstance(x, Sequence):
            fexpr = fexpr & pl.col("cycle").is_in(cycles)
        case range():
            fexpr = fexpr & pl.col("cycle").is_in(range(cycles.start, cycles.stop))
    match steps:
        case int():
            fexpr = fexpr & (pl.col("step") == steps)
        case x if isinstance(x, Sequence):
            fexpr = fexpr & pl.col("step").is_in(steps)
        case range():
            fexpr = fexpr & pl.col("step").is_in(range(steps.start, steps.stop))
    match points:
        case int():
            fexpr = fexpr & (pl.col("point") == points)
        case x if isinstance(x, Sequence):
            fexpr = fexpr & pl.col("point").is_in(points)
        case range():
            fexpr = fexpr & pl.col("point").is_in(range(points.start, points.stop))
    return fexpr

def match_expr_single(stage: int | None = None, cycle: int | None = None, step: int | None = None, point: int | None = None):
    return match_expr(stages=stage, cycles=cycle, steps=step, points=point)

class FilterDict(TypedDict):
    stage: int | Sequence[int] | range | None
    cycle: int | Sequence[int] | range | None
    step: int | Sequence[int] | range | None
    point: int | Sequence[int] | range | None


@pl.api.register_lazyframe_namespace("welldata")
class WellData:
    def __init__(self, df: pl.LazyFrame):
        self._df = df

    def norm_to_mean_per_well(self, **norm_filter: FilterDict):
        return self._df.with_columns(
            (
                pl.col("fluorescence")
                / pl.col("fluorescence").filter(match_expr(**norm_filter)).mean().over("well", "filter_set")
            ).alias("normed_fluorescence")
        )

    def norm_zero_to_one_per_well(self, zero_filter: FilterDict, one_filter: FilterDict):
        d = self._df.with_columns(
            (
                pl.col("fluorescence")
                - pl.col("fluorescence").filter(match_expr(**zero_filter)).mean().over("well", "filter_set")
            ).alias("normed_fluorescence")
        )
        d = d.with_columns(
            (
                pl.col("normed_fluorescence")
                / pl.col("normed_fluorescence").filter(match_expr(**one_filter)).mean().over("well", "filter_set")
            )
        )
        return d

    def with_time_since_mark(self, stage: int | None = None, cycle: int | None = None, step: int | None = None, point: int | None = None, units: Literal['seconds','minutes','hours',None] = None):
        d = self._df.with_columns(
            (pl.col("timestamp") - pl.col("timestamp").filter(match_expr_single(stage, cycle, step, point)).first()
             ).alias("time_since_mark"))
        if units is None:
            return d
        d = d.with_columns(
            pl.col("time_since_mark").dt.total_milliseconds().cast(float) / 1000
        )
        match units:
            case 'seconds':
                return d
            case 'minutes':
                return d.with_columns(
                    pl.col("time_since_mark") / 60
                )
            case 'hours':
                return d.with_columns(
                    pl.col("time_since_mark") / 3600
                )
            case _:
                raise ValueError(f"Invalid unit: {units}")
