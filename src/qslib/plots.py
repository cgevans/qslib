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
        `NormToMeanPerWell(stage=2, cycle=slice(1, 6)).

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
    stage: int | Sequence[int]


class FlPlotting(metaclass=ABCMeta):
    plate_setup: PlateSetup
    protocol: Protocol
    name: str

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

    def plot_anneal_melt(
        self,
        samples: str | Sequence[str],
        filters: str | FilterSet | Collection[str | FilterSet] | None = None,
        anneal_stages: int | Sequence[int] | None = None,
        melt_stages: int | Sequence[int] | None = None,
        between_stages: int | Sequence[int] | None = None,
        normalization: Normalizer = NormRaw(),
        ax: plt.Axes | None = None,
        legend: bool = True,
        figure_kw: Mapping[str, Any] | None = None,
    ) -> plt.Axes:
        """
        Plots anneal/melt curves.

        This uses solid lines for the anneal, dashed lines for the melt, and
        dotted lines for anything "between" the anneal and melt (for example, a temperature
        hold).

        Line labels are intended to provide full information when read in combination with
        the axes title.  They will only include information that does not apply to all
        lines.  For example, if every line is from the same filter set, but different
        samples, then only the sample will be shown.  If every line is from the same sample,
        but different filter sets, then only the filter set will be shown. Wells are shown
        if a sample has multiple wells.

        samples
            Either a reference to a single sample (a string), or a list of sample names.
            Well names may also be included, in which case each well will be treated without
            regard to the sample name that may refer to it.  Note this means you cannot
            give your samples names that correspond with well references.

        filters
            Optional. A filterset (string or `FilterSet`) or list of filtersets to include in the
            plot.  Multiple filtersets will be plotted *on the same axes*.  Optional;
            if None, then all filtersets with data in the experiment will be included.

        anneal_stages, melt_stages, between_stages: int | Sequence[int] | None
            Optional. A stage or list of stages (integers, starting from 1), corresponding to the
            anneal, melt, and stages between the anneal and melt (if any).  Any of these
            may be None, in which case the function will try to determine the correct values
            automatically.

        normalization
            Optional. A Normalizer instance to apply to the data.  By default, this is NormRaw, which
            passes through raw fluorescence values.  NormToMeanPerWell also works well.

        ax
            Optional.  An axes to put the plot on.  If not provided, the function will
            create a new figure.

        legend
            Optional.  Determines whether a legend is included.

        figure_kw
            Optional.  A dictionary of options passed through as keyword options to
            the figure creation.  Only applies if ax is None.

        """

        if filters is None:
            filters = self.all_filters

        if isinstance(samples, str):
            samples = [samples]

        filters = _normalize_filters(filters)

        if anneal_stages is None:
            anneal_stages = self._find_anneal_stages()
        elif isinstance(anneal_stages, int):
            anneal_stages = [anneal_stages]

        if melt_stages is None:
            melt_stages, between_stages = self._find_melt_stages(anneal_stages)
        elif isinstance(melt_stages, int):
            melt_stages = [melt_stages]

        if between_stages is None:
            between_stages = []
        elif isinstance(between_stages, int):
            between_stages = [between_stages]

        if ax is None:
            ax = cast(
                plt.Axes,
                plt.figure(**({} if figure_kw is None else figure_kw)).add_subplot(),
            )

        data = normalization.normalize_scoped(self.welldata, "all")

        all_wells = self.plate_setup.get_wells(samples)

        reduceddata = data.loc[[f.lowerform for f in filters], all_wells]

        reduceddata = normalization.normalize_scoped(reduceddata, "limited")

        for filter in filters:
            filterdat: pd.DataFrame = reduceddata.loc[filter.lowerform, :]  # type: ignore

            annealdat: pd.DataFrame = filterdat.loc[anneal_stages, :]  # type: ignore
            meltdat: pd.DataFrame = filterdat.loc[melt_stages, :]  # type: ignore

            if len(between_stages) > 0:
                betweendat: pd.DataFrame = filterdat.loc[between_stages, :]  # type: ignore

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

                    if len(between_stages) > 0:
                        betweenlines = ax.plot(
                            betweendat.loc[:, (well, "st")],
                            betweendat.loc[:, (well, "fl")],
                            color=color,
                            linestyle="dotted",
                        )

        ax.set_xlabel("temperature (°C)")

        # FIXME: consider normalization
        ax.set_ylabel("fluorescence")

        if legend:
            ax.legend()

        ax.set_title(
            _gen_axtitle(
                self.name,
                anneal_stages + between_stages + melt_stages,
                samples,
                wells,
                filters,
            )
        )

        return ax

    def plot_over_time(
        self,
        samples: str | Sequence[str],
        filters: str | FilterSet | Collection[str | FilterSet] | None = None,
        stages: slice | int | Sequence[int] = slice(None),
        normalization: Normalizer = NormRaw(),
        ax: plt.Axes | Sequence[plt.Axes] | None = None,
        legend: bool = True,
        temperatures: Literal[False, "axes", "inset", "twin"] = False,
        figure_kw: Mapping[str, Any] | None = None,
    ) -> Sequence[plt.Axes]:
        """
        Plots fluorescence over time, optionally with temperatures over time.

        Line labels are intended to provide full information when read in combination with
        the axes title.  They will only include information that does not apply to all
        lines.  For example, if every line is from the same filter set, but different
        samples, then only the sample will be shown.  If every line is from the same sample,
        but different filter sets, then only the filter set will be shown. Wells are shown
        if a sample has multiple wells.

        samples
            Either a reference to a single sample (a string), or a list of sample names.
            Well names may also be included, in which case each well will be treated without
            regard to the sample name that may refer to it.  Note this means you cannot
            give your samples names that correspond with well references.

        filters
            Optional. A filterset (string or `FilterSet`) or list of filtersets to include in the
            plot.  Multiple filtersets will be plotted *on the same axes*.  Optional;
            if None, then all filtersets with data in the experiment will be included.

        stages
            Optional.  A stage, list of stages, or slice (all using integers starting
            from 1), to include in the plot.  By default, all stages are plotted.
            For example, to plot stage 2, use `stages=2`; to plot stages 2 and 4, use
            `stages=[2, 4]`, to plot stages 3 through 15, use `stages=slice(3, 16)`
            (Python ranges are exclusive on the end).  Note that is a slice, you
            can use `None` instead of a number to denote the beginning/end.

        normalization
            Optional. A Normalizer instance to apply to the data.  By default, this is NormRaw, which
            passes through raw fluorescence values.  NormToMeanPerWell also works well.

        temperatures
            Optional (default False).  Several alternatives for displaying temperatures.
            "axes" uses a separate axes (created if ax is not provided, otherwise ax must
            be a list of two axes).

            Temperatures are from Experiment.temperature, and are thus the temperatures
            as recorded during the run, not the set temperatures.  Note that this has a
            *very* large number of data points, something that should be dealt with at
            some point.

        ax
            Optional.  An axes to put the plot on.  If not provided, the function will
            create a new figure.  If `temperatures="axes"`, however, you must provide
            a list or tuple of *two* axes, the first for fluorescence, the second
            for temperature.

        legend
            Optional.  Determines whether a legend is included.

        figure_kw
            Optional.  A dictionary of options passed through as keyword options to
            the figure creation.  Only applies if ax is None.
        """

        if filters is None:
            filters = self.all_filters

        filters = _normalize_filters(filters)

        if isinstance(samples, str):
            samples = [samples]

        if isinstance(stages, int):
            stages = []

        fig = None

        if ax is None:
            if temperatures:
                fig, ax = plt.subplots(
                    2,
                    1,
                    sharex="all",
                    gridspec_kw={"height_ratios": [3, 1]},
                    **({} if figure_kw is None else figure_kw),
                )
            else:
                fig, ax = plt.subplots(1, 1, **({} if figure_kw is None else figure_kw))
                ax = [ax]

        elif (not isinstance(ax, Sequence)) or isinstance(ax, plt.Axes):
            ax = [ax]

        ax = cast(Sequence[plt.Axes], ax)

        data = normalization.normalize_scoped(self.welldata, "all")

        all_wells = self.plate_setup.get_wells(samples) + ["time"]

        reduceddata = data.loc[[f.lowerform for f in filters], all_wells]

        reduceddata = normalization.normalize_scoped(reduceddata, "limited")

        for filter in filters:
            filterdat: pd.DataFrame = reduceddata.loc[filter.lowerform, :]  # type: ignore

            for sample in samples:
                wells = self.plate_setup.get_wells(sample)

                for well in wells:
                    color = next(ax[0]._get_lines.prop_cycler)["color"]

                    label = _gen_label(sample, well, filter, samples, wells, filters)

                    lines = ax[0].plot(
                        filterdat.loc[stages, ("time", "hours")],
                        filterdat.loc[stages, (well, "fl")],
                        color=color,
                        label=label,
                    )

        ax[-1].set_xlabel("time (hours)")

        # FIXME: consider normalization
        ax[0].set_ylabel("fluorescence")

        if legend:
            ax[0].legend()

        if temperatures == "axes":
            if len(ax) < 2:
                raise ValueError("Temperature axes requires at least two axes in ax")

            xlims = ax[0].get_xlim()

            times = reduceddata.loc[(slice(None), stages), ("time", "hours")]
            tmin, tmax = times.min(), times.max()

            reltemps = self.temperatures.loc[
                lambda x: (tmin <= x[("time", "hours")])
                & (x[("time", "hours")] <= tmax),
                :,
            ]

            for x in range(1, 7):
                ax[1].plot(
                    reltemps.loc[:, ("time", "hours")],
                    reltemps.loc[:, ("sample", x)],
                )

            ax[0].set_xlim(xlims)
            ax[1].set_xlim(xlims)

            ax[1].set_ylabel("temperature (°C)")

        ax[0].set_title(_gen_axtitle(self.name, stages, samples, wells, filters))

        return ax

    def plot_temperatures(self):
        raise NotImplemented


def _gen_label(sample, well, filter, samples, wells, filters) -> str:
    label = ""
    if len(samples) > 1:
        label = str(sample)
    if len(wells) > 1:
        if len(label) > 0:
            label += f" ({well})"
        else:
            label = str(well)
    if len(filters) > 1:
        if len(label) > 0:
            label += f", {filter}"
        else:
            label = str(filter)

    return label


def _gen_axtitle(expname, stages, samples, wells, filters) -> str:
    elems = []
    if len(samples) == 1:
        elems += samples
    if len(filters) == 1:
        elems += [str(f) for f in filters]

    val = expname
    if stages != slice(None):
        val += f", stages {stages}"

    if len(elems) > 0:
        val += ": " + ", ".join(elems)

    return val