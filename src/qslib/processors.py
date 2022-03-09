# SPDX-FileCopyrightText: 2021-2022 Constantine Evans <const@costi.eu>
#
# SPDX-License-Identifier: AGPL-3.0-only

from __future__ import annotations

import re
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, ClassVar, Literal, Sequence

import attrs
import pandas as pd

ScopeType = Literal["all", "limited"]


class Processor(metaclass=ABCMeta):
    @abstractmethod
    def process_scoped(
        self, data: pd.DataFrame, scope: ScopeType
    ) -> pd.DataFrame:  # pragma: no cover
        """
        Filter the data, and return it (possibly not a copy), *if scope is the
        minimum necessary scope for this normalization type*.  Otherwise, just
        return the same data.

        This is useful for writing scope-agnostic code, provided that you call this for
        every scope before using the data.

        The values for scope are:

        - "all": the entire welldata array.
        - "limited": all time points, but limited to the filter sets and samples being plotted.
        """
        ...

    @abstractmethod
    def process(self, data: pd.DataFrame) -> pd.DataFrame:  # pragma: no cover
        ...

    @abstractmethod
    def ylabel(self, previous_label: str | None = None) -> str:  # pragma: no cover
        ...


class NormRaw(Processor):
    """
    A Processor that takes no arguments, and simply passes through raw
    fluorescence values.
    """

    def process_scoped(self, data: pd.DataFrame, scope: ScopeType) -> pd.DataFrame:
        return data

    def process(self, data: pd.DataFrame) -> pd.DataFrame:  # pragma: no cover
        return data

    def ylabel(self, previous_label: str | None = None) -> str:
        if previous_label is None:
            return "fluorescence (AU)"
        return re.sub(r"\(([^)]+)\)", r"(\1, AU)", previous_label)


@attrs.define()
class SmoothWindowMean(Processor):
    """
    A Processor that smooths fluorescence readings using Pandas' Rolling,
    and mean.
    """

    window: int
    min_periods: int | None = None
    center: bool = False
    win_type: str | None = None
    closed: str | None = None
    scope: ClassVar[ScopeType] = "limited"

    def __attrs_post_init__(self):
        pass

    def process_scoped(self, data: pd.DataFrame, scope: ScopeType) -> pd.DataFrame:
        if scope == self.scope:
            return self.process(data)
        else:
            return data

    def process(self, data: pd.DataFrame) -> pd.DataFrame:
        return data.rolling(
            window=self.window,
            min_periods=self.min_periods,
            center=self.center,
            win_type=self.win_type,
            closed=self.closed,
        ).mean()

    def ylabel(self, previous_label: str | None = None) -> str:
        if previous_label is None:
            return f"fluorescence (window mean {self.window})"
        return re.sub(
            r"\(([^)]+)\)", rf"(\1, window mean {self.window})", previous_label
        )


@attrs.define()
class SmoothEMWMean(Processor):
    """
    A Processor that smooths fluorescence readings using Pandas' Exponential Moving Window
    (ewm / exponentially weighted moving-average).
    """

    com: float | None = None
    span: float | None = None
    halflife: float | str | timedelta | None = None
    alpha: float | None = None
    min_periods: int = 0
    adjust: bool = True
    ignore_na: bool = False
    scope: ClassVar[ScopeType] = "limited"

    def __attrs_post_init__(self):
        pass

    def ylabel(self, previous_label: str | None = None) -> str:
        if previous_label is None:
            return f"fluorescence (EMW-smoothed)"
        return re.sub(r"\(([^)]+)\)", rf"(\1, EMW-smoothed)", previous_label)

    def process_scoped(self, data: pd.DataFrame, scope: ScopeType) -> pd.DataFrame:
        if scope == self.scope:
            return self.process(data)
        else:
            return data

    def process(self, data: pd.DataFrame) -> pd.DataFrame:
        return data.ewm(
            com=self.com,
            span=self.span,
            halflife=self.halflife,
            alpha=self.alpha,
            min_periods=self.min_periods,
            adjust=self.adjust,
            ignore_na=self.ignore_na,
        ).mean()


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
        NormToMeanPerWell(stage=2, cycle=slice(1, 6)).

    `selection` allows arbitrary Pandas indexing (without the filter_set level
    of the MultiIndex) for unusual cases.
    """

    selection: Any
    scope: ClassVar[ScopeType] = "limited"

    def __init__(
        self,
        stage: int | slice | Sequence[int] | None = None,
        step: int | slice | Sequence[int] | None = None,
        cycle: int | slice | Sequence[int] | None = None,
        *,
        selection: Any | None = None,
    ):
        if selection is not None:
            if stage is not None:
                raise ValueError("Selection already set, can't specify stage.")
            if step is not None:
                raise ValueError("Selection already set, can't specify step.")
            if cycle is not None:
                raise ValueError("Selection already set, can't specify cycle.")
            self.selection = selection
        if stage is None:
            stage = slice(None)
        elif isinstance(stage, int):
            stage = [stage]
        if step is None:
            step = slice(None)
        elif isinstance(step, int):
            step = [step]
        if cycle is None:
            cycle = slice(None)
        elif isinstance(cycle, int):
            cycle = [cycle]

        self.selection = (stage, step, cycle)

    def process_scoped(self, data: pd.DataFrame, scope: ScopeType) -> pd.DataFrame:
        if scope == self.scope:
            return self.process(data)
        else:
            return data

    def process(self, data: pd.DataFrame) -> pd.DataFrame:
        normdata = data.copy()

        means = (
            data.loc[(slice(None), *self.selection), (slice(None), "fl")]
            .groupby("filter_set")
            .mean()
        )

        normdata.loc[:, (slice(None), "fl")] /= means

        return normdata

    def ylabel(self, previous_label: str | None = None) -> str:
        if previous_label is None:
            return f"fluorescence (norm. to mean)"
        return re.sub(r"\(([^)]+)\)", rf"(\1, norm. to mean)", previous_label)


@dataclass(init=False)
class SubtractByMeanPerWell(Processor):
    """
    A Processor that subtracts the fluorescence reading for each (filterset, well) pair
    by the mean value of that pair within a particular selection of data.

    The easiest way to use this is to give a particular stage (all data in that
    stage will be used), or a stage and set of cycles (those cycles in that stage
    will be used).  For example:

    - To subtract the mean stage 8 values, use `NormToMeanPerWell(stage=8)`.
    - To subtract the mean of the first 5 cycles of stage 2, use
        NormToMeanPerWell(stage=2, cycle=slice(1, 6)).

    `selection` allows arbitrary Pandas indexing (without the filter_set level
    of the MultiIndex) for unusual cases.
    """

    selection: Any
    scope: ClassVar[ScopeType] = "limited"

    def __init__(
        self,
        stage: int | slice | Sequence[int] | None = None,
        step: int | slice | Sequence[int] | None = None,
        cycle: int | slice | Sequence[int] | None = None,
        *,
        selection: Any | None = None,
    ):
        if selection is not None:
            if stage is not None:
                raise ValueError("Selection already set, can't specify stage.")
            if step is not None:
                raise ValueError("Selection already set, can't specify step.")
            if cycle is not None:
                raise ValueError("Selection already set, can't specify cycle.")
            self.selection = selection
        if stage is None:
            stage = slice(None)
        elif isinstance(stage, int):
            stage = [stage]
        if step is None:
            step = slice(None)
        elif isinstance(step, int):
            step = [step]
        if cycle is None:
            cycle = slice(None)
        elif isinstance(cycle, int):
            cycle = [cycle]

        self.selection = (stage, step, cycle)

    def process_scoped(self, data: pd.DataFrame, scope: ScopeType) -> pd.DataFrame:
        if scope == self.scope:
            return self.process(data)
        else:
            return data

    def process(self, data: pd.DataFrame) -> pd.DataFrame:
        normdata = data.copy()

        means = (
            data.loc[(slice(None), *self.selection), (slice(None), "fl")]
            .groupby("filter_set")
            .mean()
        )

        normdata.loc[:, (slice(None), "fl")] -= means

        return normdata

    def ylabel(self, previous_label: str | None = None) -> str:
        if previous_label is None:
            return f"fluorescence (subtr. by mean)"
        return re.sub(r"\(([^)]+)\)", rf"(\1, subtr. by mean)", previous_label)


@dataclass
class NormToMaxPerWell(Processor):
    """
    A Processor that divides the fluorescence reading for each (filterset, well) pair
    by the max value of that pair within a particular selection of data.

    The easiest way to use this is to give a particular stage (all data in that
    stage will be used), or a stage and set of cycles (those cycles in that stage
    will be used).  For example:

    - To normalize to the mean stage 8 values, use `NormToMeanPerWell(stage=8)`.
    - To normalize to the first 5 cycles of stage 2, use
        NormToMeanPerWell(stage=2, cycle=slice(1, 6)).

    `selection` allows arbitrary Pandas indexing (without the filter_set level
    of the MultiIndex) for unusual cases.
    """

    selection: Any
    scope: ClassVar[ScopeType] = "limited"

    def __init__(
        self,
        stage: int | slice | Sequence[int] | None = None,
        step: int | slice | Sequence[int] | None = None,
        cycle: int | slice | Sequence[int] | None = None,
        *,
        selection: Any | None = None,
    ):
        if selection is not None:
            if stage is not None:
                raise ValueError("Selection already set, can't specify stage.")
            if step is not None:
                raise ValueError("Selection already set, can't specify step.")
            if cycle is not None:
                raise ValueError("Selection already set, can't specify cycle.")
            self.selection = selection
        if stage is None:
            stage = slice(None)
        elif isinstance(stage, int):
            stage = [stage]
        if step is None:
            step = slice(None)
        elif isinstance(step, int):
            step = [step]
        if cycle is None:
            cycle = slice(None)
        elif isinstance(cycle, int):
            cycle = [cycle]

        self.selection = (stage, step, cycle)

    def process_scoped(self, data: pd.DataFrame, scope: ScopeType) -> pd.DataFrame:
        if scope == self.scope:
            return self.process(data)
        else:
            return data

    def process(self, data: pd.DataFrame) -> pd.DataFrame:
        normdata = data.copy()

        means = (
            data.loc[(slice(None), *self.selection), (slice(None), "fl")]
            .groupby("filter_set")
            .max()
        )

        normdata.loc[:, (slice(None), "fl")] /= means

        return normdata

    def ylabel(self, previous_label: str | None = None) -> str:
        if previous_label is None:
            return f"fluorescence (norm. to max)"
        return re.sub(r"\(([^)]+)\)", rf"(\1, norm. to max)", previous_label)
