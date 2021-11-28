from __future__ import annotations
from dataclasses import dataclass
from typing import Any, ClassVar, Collection, Literal, Mapping, Sequence, cast
import pandas as pd
from .data import FilterSet
from abc import abstractmethod, abstractproperty, ABCMeta
from .plate_setup import PlateSetup
from .tcprotocol import Protocol
import matplotlib.pyplot as plt

ScopeType = Literal["all", "limited"]


def _normalize_filters(
    filters: str | FilterSet | Collection[str | FilterSet],
) -> list[FilterSet]:
    if isinstance(filters, str) or isinstance(filters, FilterSet):
        filters = [filters]
    return [FilterSet.fromstring(filter) for filter in filters]


class Normalizer(metaclass=ABCMeta):
    @abstractmethod
    def normalize_scoped(self, data: pd.DataFrame, scope: ScopeType) -> pd.DataFrame:
        """
        Filter the data, and return it (possibly not a copy), *if scope is the
        minimum necessary scope for this normalization type.  Otherwise, just
        return the same data.

        This is useful for writing scope-agnostic code, provided that you call this for
        every scope before using the data.
        """
        ...

    @abstractmethod
    def normalize(self, data: pd.DataFrame) -> pd.DataFrame:
        ...


class NormRaw(Normalizer):
    def normalize_scoped(self, data: pd.DataFrame, scope: ScopeType) -> pd.DataFrame:
        return data

    def normalize(self, data: pd.DataFrame) -> pd.DataFrame:
        return data


@dataclass(init=False)
class NormToMeanPerWell(Normalizer):
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
    stage: int | Sequence[int]


class FlPlotting(metaclass=ABCMeta):
    plate_setup: PlateSetup
    protocol: Protocol

    @property
    @abstractmethod
    def welldata(self) -> pd.DataFrame:
        ...

    @property
    @abstractmethod
    def all_filters(self) -> Collection[FilterSet]:
        ...

    def _find_anneal_stages(self) -> list[int]:
        anneal_stages = []
        for i, stage in enumerate(self.protocol.stages):
            if any(
                getattr(step, "collect", False)
                and (getattr(step, "temp_increment", 0.0) < 0.0)
                for step in stage.steps
            ):
                anneal_stages.append(i + 1)
        return anneal_stages

    def _find_melt_stages(
        self, anneal_stages: Sequence[int] | None
    ) -> tuple[list[int], list[int]]:
        melt_stages = []
        for i, stage in enumerate(self.protocol.stages):
            if any(
                getattr(step, "collect", False)
                and (getattr(step, "temp_increment", 0.0) > 0.0)
                for step in stage.steps
            ):
                melt_stages.append(i + 1)

        if (
            (anneal_stages is not None)
            and (len(anneal_stages) > 0)
            and (len(melt_stages) > 0)
            and (max(anneal_stages) + 1 < min(melt_stages))
        ):
            between_stages = [
                x for x in range(max(anneal_stages) + 1, min(melt_stages))
            ]
        else:
            between_stages = []

        return melt_stages, between_stages

    def plot_annealmelt(
        self,
        filters: str | FilterSet | Collection[str | FilterSet] | None,
        samples,
        anneal_stages: int | Sequence[int] | None = None,
        melt_stages: int | Sequence[int] | None = None,
        normalization: Normalizer = NormRaw(),
        ax: plt.Axes | None = None,
        legend: bool = True,
        figure_opts: Mapping[str, Any] | None = None,
    ):
        """
        Plots anneal/melt curves.


        filters
            If

        """

        if filters is None:
            filters = self.all_filters

        filters = _normalize_filters(filters)

        if anneal_stages is None:
            anneal_stages = self._find_anneal_stages()
        elif isinstance(anneal_stages, int):
            anneal_stages = [anneal_stages]

        if melt_stages is None:
            melt_stages, between_stages = self._find_melt_stages(anneal_stages)
            anneal_stages = anneal_stages + between_stages
        elif isinstance(melt_stages, int):
            melt_stages = [melt_stages]

        if ax is None:
            ax = cast(
                plt.Axes,
                plt.figure(
                    **({} if figure_opts is None else figure_opts)
                ).add_subplot(),
            )

        data = normalization.normalize_scoped(self.welldata, "all")

        all_wells = self.plate_setup.get_wells(samples)

        reduceddata = data.loc[[f.lowerform for f in filters], all_wells]

        reduceddata = normalization.normalize_scoped(reduceddata, "limited")

        for filter in filters:
            filterdat: pd.DataFrame = reduceddata.loc[filter.lowerform, :]  # type: ignore

            annealdat: pd.DataFrame = filterdat.loc[anneal_stages, :]  # type: ignore
            meltdat: pd.DataFrame = filterdat.loc[melt_stages, :]  # type: ignore

            for sample in samples:
                wells = self.plate_setup.get_wells(sample)

                for well in wells:
                    color = next(ax._get_lines.prop_cycler)["color"]

                    if len(wells) > 1:
                        label = f"{sample} ({well})"
                    else:
                        label = sample

                    anneallines = ax.plot(
                        annealdat.loc[:, (well, "st")],
                        annealdat.loc[:, (well, "fl")],
                        color=color,
                        label=label,
                    )

                    meltlines = ax.plot(
                        meltdat.loc[:, (well, "st")],
                        meltdat.loc[:, (well, "fl")],
                        color=color,
                        linestyle="dashed",
                    )

        ax.set_xlabel("temperature (°C)")

        # FIXME: consider normalization
        ax.set_ylabel("fluorescence")

        if legend:
            ax.legend()

        return ax

    def plot_time(
        self,
        filters: str | FilterSet | Collection[str | FilterSet] | None,
        samples,
        stages=None,
        normalization: Normalizer = NormRaw(),
        ax: plt.Axes | None = None,
        legend: bool = True,
        figure_opts: Mapping[str, Any] | None = None,
    ):
        raise NotImplemented

    def plot_temperatures(self):
        raise NotImplemented
