from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
import dataclasses
from itertools import zip_longest
from typing import (
    Any,
    ClassVar,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
)

from .base import RunStatus

from ._version import version as __version__  # type: ignore

from qslib.data import FilterSet
from . import parser as qp
import textwrap
import xml.etree.ElementTree as ET
import pandas as pd
import numpy as np
from .util import *
import logging
from copy import deepcopy

log = logging.getLogger(__name__)


def _temperature_str(temperatures: Union[float, Iterable[float]]) -> str:
    """Tries to do the right thing in generating a string for a temperature or
    list of temperatures."""
    if isinstance(temperatures, Iterable):
        temperatures = list(temperatures)
        if len(temperatures) == 1 or len(set(temperatures)) == 1:
            return f"{temperatures[0]:.02f}°C"
        else:
            return "[" + ", ".join(f"{x:.02f}" for x in temperatures) + "]" + "°C"
    else:
        return f"{temperatures:.02f}°C"


def _durformat(time: Union[int, float]) -> str:
    """Convert time in seconds to a nice string"""
    s = ""
    # <= 2 minutes: stay as seconds
    if time <= 2 * 60:
        s = "{}s".format(int(time)) + s
        return s

    if (time % 60) != 0:
        s = "{}s".format(int(time % 60)) + s
    rtime = int(time // 60)
    # <= 2 hours: stay as minutes
    if time <= 2 * 60 * 60:
        s = "{}m".format(rtime) + s
        return s

    if (rtime % 60) != 0:
        s = "{}m".format(rtime % 60) + s
    rtime = rtime // 60
    # <= 3 days: stay as hours
    if time <= 3 * 24 * 3600:
        if (rtime % 24) != 0:
            s = "{}h".format(rtime) + s
        return s

    s = "{}h".format(rtime % 24) + s
    rtime = rtime // 24
    # days without bound
    s = "{}d".format(rtime) + s
    return s


class XMLable(ABC):
    @abstractmethod
    def to_xml(self) -> ET.Element:
        ...


class ProtoCommand(ABC):
    @abstractmethod
    def to_command(self, **kwargs) -> str:
        ...


class BaseStep(ABC):
    @property
    @abstractmethod
    def body(self) -> Iterable[ProtoCommand]:
        ...

    @property
    @abstractmethod
    def identifier(self) -> int | str | None:
        ...

    @property
    @abstractmethod
    def repeat(self) -> int:
        ...

    def info_str(self, index=None, repeats=1) -> str:
        if index is not None:
            s = f"{index}. "
        else:
            s = f"- "
        s += f"Step{' '+str(self.identifier) if self.identifier is not None else ''} of commands:\n"
        s += "\n".join(f"  {i+1}. " + c.to_command() for i, c in enumerate(self.body))
        return s

    def to_command(self, *, stepindex, **kwargs):
        s = "STEP "
        if self.repeat != 1:
            s += f"-repeat={self.repeat} "
        if self.identifier is not None:
            s += f"{self.identifier} "
        else:
            s += f"{stepindex} "
        s += "<multiline.step>\n"
        for com in self.body:
            s += f"\t{com.to_command(**kwargs)}\n"
        s += "</multiline.step>"
        return s


@dataclass
class Stage(XMLable):
    _steps: list[BaseStep] = field(repr=False, init=False)
    steps: list[BaseStep] | Step  # type: ignore
    repeat: int = 1
    index: int | None = None
    label: str | None = None
    _class: str = "Stage"

    def __eq__(self, other: Stage):
        if self.__class__ != other.__class__:
            return False
        if (
            (self.index is not None)
            and (other.index is not None)
            and self.index != other.index
        ):
            return False
        if (
            (self.label is not None)
            and (other.label is not None)
            and self.label != other.label
        ):
            return False
        return self._steps == other._steps

    @property
    def steps(self) -> list[BaseStep]:
        return self._steps

    @steps.setter
    def steps(self, steps: Iterable[BaseStep] | BaseStep):  # type: ignore
        self._steps = [steps] if isinstance(steps, BaseStep) else list(steps)

    def __postinit__(self):
        self.steps = self._steps

    def __repr__(self) -> str:
        s = f"Stage(steps="
        if len(self.steps) == 0:
            s += "[]"
        elif len(self.steps) == 1:
            s += repr(self.steps[0])
        else:
            s += "[" + ", ".join(repr(f) for f in self.steps) + "]"
        if self.repeat != 1:
            s += f", repeat={self.repeat}"
        if self.index:
            s += f", index={self.index}"
        if self.label:
            s += f", label={self.label}"
        s += ")"
        return s

    def dataframe(
        self, start_time: float = 0, previous_temperatures: list[float] | None = None
    ) -> pd.DataFrame:
        """
        Create a dataframe of the steps in this stage.

        Parameters
        ----------

        start_time
            The initial start time, in seconds, of the stage (before
            the ramp to the first step).  Default is 0.

        previous_temperatures
            A list of temperatures at the end of the previous stage, to allow
            calculation of ramp time.  If `None`, the ramp is assumed to take
            no time.
        """

        durations = np.array(
            [
                step.duration_at_cycle(i)
                for i in range(1, self.repeat + 1)
                for step in self._steps
            ]
        )
        temperatures = np.array(
            [
                step.temperatures_at_cycle(i)
                for i in range(1, self.repeat + 1)
                for step in self._steps
            ]
        )
        # ramp_rates = np.array(
        #    [step.ramp_rate for _ in range(1, self.repeat + 1) for step in self.body]
        # )
        collect_data = np.array(
            [step.collect for _ in range(1, self.repeat + 1) for step in self._steps]
        )

        # FIXME: is this how ramp rates actually work?

        # ramp_durations = np.zeros(len(durations))
        # if previous_temperatures is not None:
        #    ramp_durations[0] = (
        #        np.max(np.abs(temperatures[0] - previous_temperatures)) / ramp_rates[0]
        #    )
        #
        # ramp_durations[1:] = (
        #    np.max(np.abs(temperatures[1:] - temperatures[:-1]), axis=1)
        #    / ramp_rates[1:]
        # )

        tot_durations = durations  # + ramp_durations

        start_times = start_time + np.zeros(len(durations))
        start_times[0] = start_time  # + ramp_durations[0]
        start_times[1:] = start_time + np.cumsum(
            tot_durations[:-1]
        )  # + ramp_durations[1:]

        end_times = start_time + np.cumsum(tot_durations)

        data = pd.DataFrame(
            {
                "start_time": start_times,
                "end_time": end_times,
                "collect_data": collect_data,
            }
        )

        data["temperature_avg"] = np.average(temperatures, axis=1)

        for i in range(
            0, temperatures.shape[1]
        ):  # pylint: disable=unsubscriptable-object
            data["temperature_{}".format(i + 1)] = temperatures[:, i]

        data["cycle"] = [
            c for c in range(1, self.repeat + 1) for s in range(1, len(self._steps) + 1)
        ]
        data["step"] = [
            s for c in range(1, self.repeat + 1) for s in range(1, len(self._steps) + 1)
        ]

        data.set_index(["cycle", "step"], inplace=True)

        return data

    def to_command(self, stageindex=None, **kwargs):
        s = "STAGe "
        if self.repeat != 1:
            s += f"-repeat={self.repeat} "
        if self.index is not None:
            s += f"{self.index} "
        else:
            s += f"{stageindex} "
        if self.label:
            s += self.label + " "
        else:
            s += f"STAGE_{self.index or stageindex} "
        s += "<multiline.stage>\n"
        for i, step in enumerate(self._steps):
            s += (
                textwrap.indent(f"{step.to_command(stepindex=i+1, **kwargs)}", "\t")
                + "\n"
            )
        s += "</multiline.stage>"
        return s

    @classmethod
    def _from_command_dict(cls, d) -> Stage:
        s = cls([])
        if len(d["args"]) >= 1:
            s.index = d["args"][0]
        if len(d["args"]) >= 2:
            s.label = d["args"][1]
        for k, v in d["opts"].items():
            setattr(s, k.lower(), v)

        s._steps = [Step._from_command_dict(step) for step in d["body"]]
        return s

    @classmethod
    def from_xml(cls, e: ET.Element) -> Stage:
        rep = int(e.findtext("NumOfRepetitions") or 1)
        startcycle = int(cast(str, e.findtext("StartingCycle")))
        ade = e.findtext("AutoDeltaEnabled") == "true"
        steps = [
            Step.from_xml(x, etc=startcycle, ehtc=startcycle, he=ade)
            for x in e.findall("TCStep")
        ]
        return cls(steps, rep)

    def to_xml(self) -> ET.Element:
        e = ET.Element("TCStage")
        ET.SubElement(e, "StageFlag").text = "CYCLING"
        ET.SubElement(e, "NumOfRepetitions").text = str(int(self.repeat))
        scycle: set[int] = set()
        for s in self._steps:
            scycle.add(s.temp_incrementcycle)
            scycle.add(s.time_incrementcycle)
            assert isinstance(s, XMLable)
            e.append(s.to_xml())
        if len(scycle) > 1:
            log.warn("Approx")
        ET.SubElement(e, "StartingCycle").text = str(next(iter(scycle)))
        ET.SubElement(e, "AutoDeltaEnabled").text = "true"

        return e

    def info_str(self, index) -> str:
        if self.repeat > 1:
            adds = "s"
        else:
            adds = ""
        stagestr = f"{index}. Stage with {self.repeat} cycle{adds}"
        stepstrs = [
            textwrap.indent(f"{step.info_str(i+1, self.repeat)}", "  ")
            for i, step in enumerate(self._steps)
        ]
        try:
            tot_dur = sum(x.total_duration(self.repeat) for x in self._steps)  # type: ignore
            stagestr += f" (total duration {_durformat(tot_dur)})"
        except KeyError:
            pass
        if len(stepstrs) > 1:
            stagestr += " of:\n" + "\n".join(stepstrs)
        else:
            stagestr += " of " + " ".join(stepstrs[0].split()[1:])
        return stagestr

    def __str__(self) -> str:
        return self.info_str(None)

    @classmethod
    def fromdict(cls, d: dict[str, Any]) -> "Stage":
        s = [cast(BaseStep, Step(**x)) for x in d["_steps"]]
        del d["_steps"]
        return cls(s, **d)


def _oxfordlist(iterable: Iterable[str]) -> str:
    x = iter(iterable)
    try:
        s = next(x)  # we know the first will be there
    except StopIteration:
        return ""
    try:
        maybeult = next(x)
    except StopIteration:
        return s
    try:
        nextult = next(x)
        s = s + ", " + maybeult
        maybeult = nextult
    except StopIteration:
        return s + " and " + maybeult
    while True:
        try:
            nextult = next(x)
            s = s + ", " + maybeult
            maybeult = nextult
        except StopIteration:
            return s + ", and " + maybeult


ALLTEMPS = ["temperature_{}".format(i) for i in range(1, 7)]


@dataclass
class Protocol(XMLable):
    """A run protocol for the QuantStudio.  Protocols encapsulate the temperature and camera
    controls for an entire run.  They are composed of :any:`Stage`s, which may repeat for a
    number of cycles, and the stages are in turn composed of Steps, which may be created for
    usual cases with :any:`Step`, or from SCPI commands (TODO: implement).  Steps may repeat
    their contents as well, but this is not yet implemeted.

    Parameters
    ----------
    stages: Iterable[Stage]
        The stages of the protocol, likely :any:`Stage`.
    name: str | None
        A protocol name. If not set, a timestamp will be used, unlike AB's uuid.
    volume: floa
        The sample volume, in µL.
    runmode: str | None
        The run mode.
    covertemperature: float (default 105.0)
        The cover temperature
    filters: Sequence[str]
        A list of default filters that can be used by any collection commands
        that don't specify their own.
    """

    stages: Iterable[Stage]
    name: str = field(default_factory=lambda: "Prot_" + _nowuuid())
    volume: float = 50.0
    runmode: str = "standard"  # standard or fast... need to deal with this
    filters: list[str | FilterSet] = field(default_factory=lambda: [])
    covertemperature: float = 105.0
    _class: str = "Protocol"

    def to_command(self):
        s = "PROTocol "
        if self.volume is not None:
            s += f"-volume={self.volume} "
        if self.runmode is not None:
            s += f"-runmode={self.runmode} "
        s += self.name + " "
        s += "<quote.message>\n"
        for i, stage in enumerate(self.stages):
            s += (
                textwrap.indent(
                    stage.to_command(filters=self.filters, stageindex=i + 1), "\t"
                )
                + "\n"
            )
        s += "</quote.message>"
        return s

    @classmethod
    def from_command(cls, s):
        return cls._from_command_dict(qp.command.parseString(s)[0])

    @classmethod
    def _from_command_dict(cls, d):
        assert d["command"].lower() in ["prot", "protocol"]
        p = cls([])
        assert len(d["args"]) == 1
        p.name = d["args"][0]
        for k, v in d["opts"].items():
            setattr(p, k.lower(), v)

        p.stages = [Stage._from_command_dict(x) for x in d["body"]]

        return p

    @property
    def dataframe(self) -> pd.DataFrame:
        "A DataFrame of the temperature protocol."
        dataframes: list[pd.DataFrame] = []

        stage_start_time = 0.0
        stagenum = 1
        previous_temperatures = None

        for stage in self.stages:
            dataframe = stage.dataframe(stage_start_time, previous_temperatures)
            stagenum += 1
            previous_temperatures = dataframe.iloc[-1].loc[ALLTEMPS].to_numpy()
            stage_start_time = cast(float, dataframe["end_time"].iloc[-1])
            dataframes.append(dataframe)

        return pd.concat(
            dataframes,
            keys=range(1, len(dataframes) + 1),
            names=["stage", "step", "cycle"],
        )

    @property
    def all_points(self) -> pd.DataFrame:
        d = self.dataframe
        dd = (
            d.loc[:, "temperature_avg":]
            .iloc[np.repeat(np.arange(0, len(d)), 2)]
            .reset_index()
        )
        times = np.empty(len(dd))
        times[::2] = np.array(d.loc[:, "start_time"])
        times[1::2] = np.array(d.loc[:, "end_time"])
        dd.insert(0, "time", times)
        return dd

    @property
    def all_times(self) -> np.ndarray:
        "An array of all start and end times of each step, interleaved."
        d = self.dataframe
        alltimes = np.empty(len(d) * 2)
        alltimes[:-1:2] = d["start_time"]
        alltimes[1::2] = d["end_time"]
        return alltimes

    def copy(self) -> Protocol:
        return deepcopy(self)

    @property
    def all_temperatures(self) -> np.ndarray:
        "An array of temperature settings at `all_times`."
        d = self.dataframe
        return np.repeat(d["temperature_avg":], 2)

    def tcplot(
        self, ax: Optional["plt.Axes"] = None
    ) -> Tuple["plt.Axes", Tuple[List["plt.Line2D"], List["plt.Line2D"]]]:
        "A plot of the temperature and data collection points."
        #    try:

        #    except ModuleNotFoundError:
        #        raise "

        import matplotlib.pyplot as plt

        if ax is None:
            _, ax = plt.subplots()

        d = self.dataframe

        all_points = self.all_points

        # FIXME: 0 to 6 is specific
        p1 = [
            ax.plot(
                all_points["time"] / 3600.0,
                all_points[f"temperature_{i}"],
                label=f"{i}",
            )[0]
            for i in range(1, 7)
        ]

        dc = d.loc[d.collect_data, :]
        p2 = [
            ax.plot(
                cast("pd.Series['float']", dc["end_time"]) / 3600.0,
                dc[f"temperature_{i+1}"],
                ".",
                color=p.get_color(),
            )[0]
            for i, p in enumerate(p1)
        ]

        ax.legend(title="zone")
        ax.set_xlabel("experiment time (hrs) (points collect data)")
        ax.set_ylabel("temperature (°C)")
        ax.set_title("Temperature Protocol")

        return (ax, (p1, p2))

    @classmethod
    def from_xml(cls, e: ET.Element) -> Protocol:
        svol = e.findtext("SampleVolume")
        runmode = e.findtext("RunMode")
        covertemperature = float(e.findtext("CoverTemperature"))
        protoname = e.findtext("ProtocolName")
        filter_e = e.find("CollectionProfile")
        filters = []
        if filter_e:
            for x in filter_e.findall("CollectionCondition/FilterSet"):
                filters.append(x.attrib["Excitation"] + "-" + x.attrib["Emission"])
        stages = [Stage.from_xml(x) for x in e.findall("TCStage")]
        return Protocol(stages, protoname, svol, runmode, filters, covertemperature)

    def to_xml(
        self, e: ET.Element | None = None, covertemperature=105.0
    ) -> tuple[ET.ElementTree, ET.ElementTree]:
        te = ET.ElementTree(ET.Element("TCProtocol"))
        tqe = ET.ElementTree(ET.Element("QSTCProtocol"))

        e = te.getroot()
        qe = tqe.getroot()

        _set_or_create(e, "FileVersion").text = "2.0"
        _set_or_create(e, "ProtocolName").text = self.name
        _set_or_create(qe, "QSLibNote").text = (
            "This protocol was"
            " generated by QSLib. It may be only an approximation or"
            " placeholder for the real protocol, contained as"
            " an SCPI command in QSLibProtocolCommand."
        )
        _set_or_create(qe, "QSLibProtocolCommand").text = self.to_command()
        _set_or_create(qe, "QSLibProtocol").text = str(dataclasses.asdict(self))
        _set_or_create(qe, "QSLibVerson").text = __version__
        _set_or_create(e, "CoverTemperature").text = str(covertemperature)
        if self.volume is not None:
            _set_or_create(e, "SampleVolume").text = str(self.volume)
        if self.runmode is not None:
            _set_or_create(e, "RunMode").text = str(self.runmode)
        if self.filters:
            x = _set_or_create(e, "CollectionProfile", ProfileId="1")
            for f in self.filters:
                if not isinstance(f, FilterSet):
                    f = FilterSet.fromstring(f)
                x.append(f.to_xml())
        for s in self.stages:
            e.append(s.to_xml())
        return te, tqe

    def __str__(self) -> str:
        begin = f"Run Protocol {self.name}"
        extras = []
        if self.volume:
            extras.append(f"sample volume {self.volume} µL")
        if self.runmode:
            extras.append(f"run mode {self.runmode}")
        if extras:
            begin += " with " + _oxfordlist(extras)
        begin += ":\n"
        if self.filters:
            begin += (
                f"(default filters "
                + _oxfordlist(FilterSet.fromstring(f).lowerform for f in self.filters)
                + ")\n\n"
            )
        else:
            begin += "\n"
        stagestrs = [
            textwrap.indent(stage.info_str(i + 1), "  ")
            for i, stage in enumerate(self.stages)
        ]

        return begin + "\n".join(stagestrs)

    @classmethod
    def fromdict(cls, d: dict[str, Any]) -> "Protocol":
        s = [Stage(**x) for x in d["stages"]]
        del d["stages"]
        return cls(s, **d)

    def check_compatible(self, new: Protocol, status: RunStatus):
        # Sample sample volume? (FIXME: We can't change it right now.)
        # assert self.volume == new.volume
        # assert self.name == new.name

        for i, (oldstage, newstage) in enumerate(zip_longest(self.stages, new.stages)):
            oldstage: Stage
            newstage: Stage
            if (
                i + 1 < status.stage
            ):  # If the stage has already passed, we must be equal
                assert oldstage == newstage
            elif (
                i + 1 == status.stage
            ):  # Current stage.  Only change is # cycles, >= current
                if newstage.repeat < status.cycle:
                    raise ValueError
                oldstage.repeat = newstage.repeat  # for comparison
                assert oldstage == newstage
            else:
                continue


@dataclass
class Exposure(ProtoCommand):
    # We don't support persistent... it doesn't seem safe
    settings: Mapping[FilterSet, Sequence[int]]
    state: str = "HoldAndCollect"

    def to_command(self, **kwargs) -> str:
        settingstrings = [
            k.hacform + "," + ",".join(str(x) for x in v)
            for k, v in self.settings.items()
        ]

        return f"EXP -state={self.state} " + " ".join(settingstrings)


@dataclass
class Ramp(ProtoCommand):
    temperature: Sequence[float]
    increment: float | None = None
    incrementcycle: int | None = None
    incrementstep: int | None = None
    rate: float | None = None
    cover: float | None = None
    _argfields: ClassVar[tuple[str, ...]] = (
        "increment",
        "incrementcycle",
        "incrementstep",
        "rate",
        "cover",
    )

    def to_command(self, **kwargs) -> str:
        p = []
        p.append("RAMP")
        for f in self._argfields:
            if (v := getattr(self, f)) is not None:
                p.append(f"-{f}={v}")
        p += self.temperature
        return " ".join(str(x) for x in p)


@dataclass
class HACFILT(ProtoCommand):
    filters: Sequence[FilterSet] | None

    def to_command(
        self, filters: Iterable[FilterSet | str] | None = None, **kwargs
    ) -> str:
        if self.filters is None:
            assert filters is not None
            filters = [
                FilterSet.fromstring(f) if isinstance(f, str) else f for f in filters
            ]
        else:
            filters = self.filters
        return "HACFILT " + " ".join([f.hacform for f in filters])

    def __init__(self, filters: Iterable[FilterSet | str]) -> None:
        self.filters = [
            FilterSet.fromstring(f) if isinstance(f, str) else f for f in filters
        ]


@dataclass
class HoldAndCollect(ProtoCommand):
    time: int
    increment: float | None = None
    incrementcycle: int | None = None
    incrementstep: int | None = None
    tiff: bool = False
    quant: bool = True
    pcr: bool = False
    _argfields: ClassVar[tuple[str, ...]] = (
        "increment",
        "incrementcycle",
        "incrementstep",
        "tiff",
        "quant",
        "pcr",
    )

    def to_command(self, **kwargs) -> str:
        p = []
        p.append("HoldAndCollect")
        for f in self._argfields:
            if (v := getattr(self, f)) is not None:
                p.append(f"-{f}={v}")
        p.append(self.time)
        return " ".join(str(x) for x in p)


@dataclass
class Hold(ProtoCommand):
    time: int | None
    increment: float | None = None
    incrementcycle: int | None = None
    incrementstep: int | None = None
    _argfields: ClassVar[tuple[str, ...]] = (
        "increment",
        "incrementcycle",
        "incrementstep",
    )

    def to_command(self, **kwargs) -> str:
        p = []
        p.append("HOLD")
        for f in self._argfields:
            if (v := getattr(self, f)) is not None:
                p.append(f"-{f}={v}")
        p.append(str(self.time))
        return " ".join(str(x) for x in p)


@dataclass
class Step(BaseStep, XMLable):
    """
    A normal protocol step.

    Parameters
    ----------

    time: int
        The step time setting, in seconds.
    temperature: float | Sequence[float]
        The temperature hold setting, either as a float (all zones the same) or a sequence
        (of correct length) of floats setting the temperature for each zone.
    collect: bool
        Collect fluorescence data?
    temp_increment: float
        Amount to increment all zone temperatures per cycle on and after :any:`temp_incrementcycle`.
    temp_incrementcycle: int (default 2)
        First cycle to start the increment changes. Note that the default in QSLib is 2, not 1 (as in AB's software),
        so that leaving this alone makes sense (the first cycle will be at :any:`temperature`, the next at
        :code:`temperature + temp_incrementcycle`.
    time_increment : float
    time_incrementcycle : int
        The same settings for time per cycle.
    filters : Sequence[FilterSet | str] (default empty)
        A list of filter pairs to collect, either using :any:`FilterSet` or a string like "x1-m4".  If collect is
        True and this is empty, then the filters will be set by the Protocol.

    Notes
    -----

    This currently does not support step-level repeats, which do exist on the machine.
    """

    time: int
    temperature: float | Sequence[float]
    collect: bool = False
    temp_increment: float = 0.0
    temp_incrementcycle: int = 2
    time_increment: int = 0
    time_incrementcycle: int = 2
    filters: Sequence[FilterSet | str] = tuple()
    pcr: bool = False
    quant: bool = True
    tiff: bool = False
    _class: str = "Step"

    def __eq__(self, other: Step):
        if self.__class__ != other.__class__:
            return False
        if self.temperature_list != other.temperature_list:
            return False
        # FIXME
        if (self.filters) and (other.filters) and self._filtersets != other._filtersets:
            return False
        if self.time != other.time:
            return False
        if self.collect != other.collect:
            return False
        if self.temp_increment != other.temp_increment:
            return False
        if self.temp_incrementcycle != other.temp_incrementcycle:
            return False
        if self.time_increment != other.time_increment:
            return False
        if self.time_incrementcycle != other.time_incrementcycle:
            return False
        if self.pcr != other.pcr:
            return False
        if self.quant != other.quant:
            return False
        if self.tiff != other.tiff:
            return False
        return True

    @property
    def _filtersets(self):
        return [FilterSet.fromstring(x) for x in self.filters]

    def info_str(self, index, repeats: int = 1) -> str:
        "String describing the step."

        tempstr = _temperature_str(self.temperatures_at_cycle(1))
        if (repeats > 1) and (self.temp_increment != 0.0):
            t = _temperature_str(self.temperatures_at_cycle(repeats))
            tempstr += f" to {t}"

        elems = [f"{tempstr} for {_durformat(self.time)}/cycle"]
        if self.temp_increment != 0.0:
            elems.append(f"{self.temp_increment}°C/cycle")
            if self.temp_incrementcycle > 1:
                elems[-1] += f" from cycle {self.temp_incrementcycle}"
        if self.time_increment != 0.0:
            elems.append(f"{_durformat(self.time_increment)}/cycle")
            if self.time_incrementcycle != 2:
                elems[-1] += f" from cycle {self.time_incrementcycle}"
        # if self.ramp_rate != 1.6:
        #    elems.append(f"{self.ramp_rate} °C/s ramp")
        s = f"{index}. " + ", ".join(elems)

        if self.collect:
            s += " (collects "
            if self.filters:
                s += ", ".join(FilterSet.fromstring(f).lowerform for f in self.filters)
            else:
                s += "default"
            if self.pcr:
                s += ", pcr on"
            if not self.quant:
                s += ", quant off"
            if self.tiff:
                s += ", keeps images"
            s += ")"

        return s

    def total_duration(self, repeats: int = 1):
        return sum(self.duration_at_cycle(c) for c in range(1, repeats + 1))

    def duration_at_cycle(self, cycle: int) -> float:  # cycle from 1
        "Duration of the step (excluding ramp) at `cycle` (from 1)"
        inccycles = max(0, cycle + 1 - self.time_incrementcycle)
        return self.time + inccycles * self.time_increment
        # FIXME: is this right?

    def temperatures_at_cycle(self, cycle: int) -> list[float]:
        "Temperatures of the step at `cycle` (from 1)"
        inccycles = max(0, cycle + 1 - self.temp_incrementcycle)
        return [
            x + inccycles * self.temp_increment
            # FIXME: This actually applies to first cycle... why!?
            for x in self.temperature_list
        ]

    @property
    def repeat(self) -> int:
        return 1

    @property
    def identifier(self) -> None:
        return None

    @property
    def temperature_list(self) -> list[float]:
        if isinstance(self.temperature, Sequence):
            return list(self.temperature)
        else:
            return 6 * [self.temperature]

    @property
    def body(self) -> list[ProtoCommand]:
        if self.collect:
            return [
                Ramp(
                    self.temperature_list, self.temp_increment, self.temp_incrementcycle
                ),
                HACFILT(self.filters),
                HoldAndCollect(
                    self.time,
                    self.time_increment,
                    self.time_incrementcycle,
                    None,
                    self.tiff,
                    self.quant,
                    self.pcr,
                ),
            ]
        else:
            return [
                Ramp(
                    self.temperature_list, self.temp_increment, self.temp_incrementcycle
                ),
                Hold(
                    self.time,
                    self.time_increment,
                    self.time_incrementcycle,
                ),
            ]

    @classmethod
    def from_xml(cls, e: ET.Element, *, etc=1, ehtc=1, he=False) -> Step:
        collect = bool(int(e.findtext("CollectionFlag")))
        ts = [float(x.text) for x in e.findall("Temperature")]
        ht = int(e.findtext("HoldTime"))
        et = float(e.findtext("ExtTemperature"))
        eht = int(e.findtext("ExtHoldTime"))
        if not he:
            et = 0
            eht = 0
        return Step(ht, ts, collect, et, etc, eht, ehtc, [], True)

    def to_xml(self) -> ET.Element:
        e = ET.Element("TCStep")
        ET.SubElement(e, "CollectionFlag").text = str(
            int(self.collect)
        )  # FIXME: approx
        for t in self.temperature_list:
            ET.SubElement(e, "Temperature").text = str(t)
        ET.SubElement(e, "HoldTime").text = str(self.time)
        # FIXME: does not contain cycle starts, because AB format can't handle
        ET.SubElement(e, "ExtTemperature").text = str(self.temp_increment)
        ET.SubElement(e, "ExtHoldTime").text = str(self.time_increment)
        # FIXME: RampRate, RampRateUnit
        ET.SubElement(e, "RampRate").text = "1.6"
        ET.SubElement(e, "RampRateUnit").text = "DEGREES_PER_SECOND"
        return e

    @classmethod
    def _from_command_dict(cls, d):
        coms = [x["command"] for x in d["body"]]
        if coms == ["RAMP", "HACFILT", "HoldAndCollect"]:
            c = cls(
                d["body"][2]["args"][0],
                d["body"][0]["args"],
                time_incrementcycle=1,
                temp_incrementcycle=1,
            )
            c.collect = True
            if "args" in d["body"][1].keys():
                c.filters = [FilterSet.fromstring(x) for x in d["body"][1]["args"]]
            else:
                c.filters = []
            for k, v in d["body"][2]["opts"].items():
                k = k.lower()
                if "increment" in k:
                    k = "time_" + k
                setattr(c, k.lower(), v)
            for k, v in d["body"][0]["opts"].items():
                k = k.lower()
                if "increment" in k:
                    k = "temp_" + k
                setattr(c, k.lower(), v)
        elif coms == ["RAMP", "HOLD"]:
            c = cls(
                d["body"][1]["args"][0],
                d["body"][0]["args"],
                time_incrementcycle=1,
                temp_incrementcycle=1,
            )
            c.collect = False
            for k, v in d["body"][1]["opts"].items():
                k = k.lower()
                if "increment" in k:
                    k = "time_" + k
                setattr(c, k.lower(), v)
            for k, v in d["body"][0]["opts"].items():
                k = k.lower()
                if "increment" in k:
                    k = "temp_" + k
                setattr(c, k.lower(), v)
        else:
            raise ValueError
        return c

    @classmethod
    def fromdict(cls, d: dict[str, Any]) -> "Step":
        return cls(**d)
