from __future__ import annotations

from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from typing import Any, ClassVar, Literal, Sequence

import pandas as pd

ScopeType = Literal["all", "limited"]


class Normalizer(metaclass=ABCMeta):
    @abstractmethod
    def normalize_scoped(self, data: pd.DataFrame, scope: ScopeType) -> pd.DataFrame:
        """
        Filter the data, and return it (possibly not a copy), *if scope is the
        minimum necessary scope for this normalization type*.  Otherwise, just
        return the same data.

        This is useful for writing scope-agnostic code, provided that you call this for
        every scope before using the data.
        """
        ...

    @abstractmethod
    def normalize(self, data: pd.DataFrame) -> pd.DataFrame:
        ...


class NormRaw(Normalizer):
    """
    A Normalizer that takes no arguments, and simply passes through raw
    fluorescence values.
    """

    def normalize_scoped(self, data: pd.DataFrame, scope: ScopeType) -> pd.DataFrame:
        return data

    def normalize(self, data: pd.DataFrame) -> pd.DataFrame:
        return data


@dataclass(init=False)
class NormToMeanPerWell(Normalizer):
    """
    A Normalizer that divides the fluorescence reading for each (filterset, well) pair
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

    def normalize_scoped(self, data: pd.DataFrame, scope: ScopeType) -> pd.DataFrame:
        if scope == self.scope:
            return self.normalize(data)
        else:
            return data

    def normalize(self, data: pd.DataFrame) -> pd.DataFrame:
        normdata = data.copy()

        means = (
            data.loc[(slice(None), *self.selection), (slice(None), "fl")]
            .groupby("filter_set")
            .mean()
        )

        normdata.loc[:, (slice(None), "fl")] /= means

        return normdata


@dataclass
class NormToMaxPerWell(Normalizer):
    """
    A Normalizer that divides the fluorescence reading for each (filterset, well) pair
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

    def normalize_scoped(self, data: pd.DataFrame, scope: ScopeType) -> pd.DataFrame:
        if scope == self.scope:
            return self.normalize(data)
        else:
            return data

    def normalize(self, data: pd.DataFrame) -> pd.DataFrame:
        normdata = data.copy()

        means = (
            data.loc[(slice(None), *self.selection), (slice(None), "fl")]
            .groupby("filter_set")
            .max()
        )

        normdata.loc[:, (slice(None), "fl")] /= means

        return normdata
