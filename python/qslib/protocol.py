# SPDX-FileCopyrightText: 2021 - 2023 Constantine Evans <qslib@mb.costi.net>
#
# SPDX-License-Identifier: EUPL-1.2

from __future__ import annotations

import logging
import math
import re
import textwrap
import warnings
import xml.etree.ElementTree as ET
from abc import ABC, abstractmethod
from copy import deepcopy
from dataclasses import dataclass
from itertools import zip_longest
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Collection,
    Generic,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
    overload,
)
from typing_extensions import TypeAlias

import attr
import numpy as np
import pandas as pd
import pint

from qslib import scpi_commands
from qslib.data import FilterSet

from ._util import _nowuuid, _set_or_create
from .base import RunStatus
from .scpi_commands import SCPICommand, SCPICommandLike
from .version import __version__

if TYPE_CHECKING:  # pragma: no cover
    import matplotlib.pyplot as plt

from pint.facets.plain import PlainUnit

FloatQuantity = pint.Quantity
IntQuantity = pint.Quantity
PlainQuantity: TypeAlias = pint.Quantity
ArrayQuantity: TypeAlias = pint.Quantity

NZONES = 6

UR: pint.UnitRegistry = pint.UnitRegistry(
    autoconvert_offset_to_baseunit=True, auto_reduce_dimensions=True
)

Q_ = UR.Quantity

_ZERO_SECONDS = Q_("0 seconds")
_SECONDS = Q_("0 seconds").u
_DEGC = Q_("0 °C").u

log = logging.getLogger(__name__)


def _check_unit_or_fail(val: PlainQuantity, unit: str | PlainUnit) -> None:
    if not val.check(unit):
        raise pint.DimensionalityError(val.u, unit)


def _wrap_seconds(val: int | float | str | PlainQuantity) -> PlainQuantity:
    if isinstance(val, str):
        uv = Q_(val)
        _check_unit_or_fail(uv, _SECONDS)
    elif isinstance(val, PlainQuantity):
        uv = val
        _check_unit_or_fail(uv, _SECONDS)
    else:
        uv = Q_(val, _SECONDS)
    return uv


def _maybe_wrap_seconds(
    val: int | float | str | PlainQuantity | None,
) -> PlainQuantity | None:
    if val is None:
        return None
    elif isinstance(val, str):
        uv = Q_(val)
        _check_unit_or_fail(uv, _SECONDS)
    elif isinstance(val, PlainQuantity):
        uv = val
        _check_unit_or_fail(uv, _SECONDS)
    else:
        uv = Q_(val, _SECONDS)
    return uv


def _wrap_degC(val: int | float | str | PlainQuantity) -> PlainQuantity:
    if isinstance(val, str):
        uv = Q_(val)
        _check_unit_or_fail(uv, _DEGC)
    elif isinstance(val, PlainQuantity):
        uv = val
        _check_unit_or_fail(uv, _DEGC)
    else:
        uv = Q_(val, _DEGC)
    return uv


def _wrap_delta_degC(val: int | float | str | PlainQuantity) -> PlainQuantity:
    if isinstance(val, str):
        uv = Q_(val)
        uv = _ensure_delta_temperature(uv)
    elif isinstance(val, PlainQuantity):
        uv = val
        uv = _ensure_delta_temperature(uv)
    else:
        uv = Q_(val, "delta_degC")
    return uv


def _ensure_delta_temperature(val: PlainQuantity) -> PlainQuantity:
    _check_unit_or_fail(val, "delta_degC")

    # Are we multiplicative?
    if not UR._units[str(val.u)].is_multiplicative:
        return Q_(val.m, "delta_" + str(val.u))

    return val


_ZEROTEMPDELTA = Q_(0.0, "delta_degC")


def _wrap_delta_degC_or_zero(
    val: int | float | str | PlainQuantity | None,
) -> PlainQuantity:
    if val is None:
        return _ZEROTEMPDELTA
    else:
        return _wrap_delta_degC(val)


def _wrap_degC_or_none(
    val: int | float | str | PlainQuantity | None,
) -> PlainQuantity | None:
    if val is None:
        return None
    else:
        return _wrap_degC(val)


def _wrapunitmaybelist_degC(
    val: (
        int | float | str | PlainQuantity | Sequence[int | float | str | PlainQuantity]
    ),
) -> PlainQuantity:
    unit: pint.Unit = UR.Unit("degC")

    if isinstance(val, PlainQuantity):
        uv = val
        _check_unit_or_fail(uv, unit)
    elif isinstance(val, str):
        uv = UR(val)
        _check_unit_or_fail(uv, unit)
    elif isinstance(val, Sequence):
        m = []
        for x in val:
            if isinstance(x, PlainQuantity):
                m.append(x.to(unit).magnitude)
            elif isinstance(x, str):
                m.append(Q_(x).to(unit).magnitude)
            else:
                m.append(x)
        return Q_(m, unit)
    else:
        uv = Q_(val, unit)
    return uv


def _durformat(time: PlainQuantity) -> str:
    """Convert time in seconds to a nice string"""
    time_s: int = time.to(UR.seconds).magnitude
    s = ""
    # <= 2 minutes: stay as seconds
    if time_s <= 2 * 60:
        s = "{}s".format(int(time_s)) + s
        return s

    if (time_s % 60) != 0:
        s = "{}s".format(int(time_s % 60)) + s
    rtime = int(time_s // 60)
    # <= 2 hours: stay as minutes
    if time_s <= 2 * 60 * 60:
        s = "{}m".format(rtime) + s
        return s

    if (rtime % 60) != 0:
        s = "{}m".format(rtime % 60) + s
    rtime = rtime // 60
    # <= 3 days: stay as hours
    if time_s <= 3 * 24 * 3600:
        if (rtime % 24) != 0:
            s = "{}h".format(rtime) + s
        return s

    s = "{}h".format(rtime % 24) + s
    rtime = rtime // 24
    # days without bound
    s = "{}d".format(rtime) + s
    return s


T = TypeVar("T")


class ProtoCommand(ABC):
    @abstractmethod
    def to_scpicommand(self, **kwargs: Any) -> SCPICommand:  # pragma: no cover
        ...

    def __init__(self, *args: Any, **kwargs: Any) -> None:  # pragma: no cover
        ...

    @classmethod
    @abstractmethod
    def from_scpicommand(cls: Type[T], sc: SCPICommand) -> T:  # pragma: no cover
        ...

    _names: ClassVar[Sequence[str]] = tuple()


@attr.define
class Ramp(ProtoCommand):
    """Ramps temperature to a new setting."""

    temperature: PlainQuantity = attr.field(  # [np.ndarray]
        converter=_wrapunitmaybelist_degC, on_setattr=attr.setters.convert
    )
    increment: PlainQuantity = attr.field(  # [float]
        converter=_wrap_delta_degC_or_zero,
        on_setattr=attr.setters.convert,
        default=_ZEROTEMPDELTA,
    )
    incrementcycle: int = 1
    incrementstep: int = 1
    rate: float = 100.0  # This is a percent
    cover: PlainQuantity | None = attr.field(  # [float]
        converter=_wrap_degC_or_none,
        on_setattr=attr.setters.convert,
        default=None,
    )
    _names: ClassVar[Sequence[str]] = ("RAMP",)

    def to_scpicommand(self, **kwargs: None) -> SCPICommand:
        opts = {}

        if self.increment != _ZEROTEMPDELTA:
            opts["increment"] = self.increment.to("delta_degC").magnitude
        if self.incrementcycle != 1:
            opts["incrementcycle"] = self.incrementcycle
        if self.incrementstep != 1:
            opts["incrementstep"] = self.incrementstep
        if self.rate != 100.0:
            opts["rate"] = self.rate
        if self.cover is not None:
            opts["cover"] = self.cover.to("degC").magnitude

        return SCPICommand(
            "RAMP", *self.temperature.to("degC").magnitude, comment=None, **opts
        )

    @classmethod
    def from_scpicommand(cls, sc: SCPICommand) -> Ramp:
        return Ramp(Q_(sc.args, "degC"), **sc.opts)  # type: ignore


@dataclass
class Exposure(ProtoCommand):
    """Modifies exposure settings."""

    # We don't support persistent... it doesn't seem safe
    settings: Sequence[Tuple[FilterSet, Sequence[int]]]
    state: str = "HoldAndCollect"
    _names: ClassVar[Sequence[str]] = ("EXP", "EXPOSURE")

    def to_scpicommand(self, **kwargs: None) -> SCPICommand:
        [k.hacform + "," + ",".join(str(x) for x in v) for k, v in self.settings]

        return SCPICommand(
            "EXP",
            *[[k.hacform] + [str(x) for x in v] for k, v in self.settings],
            state=self.state,
        )

    @classmethod
    def from_scpicommand(cls, sc: SCPICommand) -> Exposure:
        filts = [
            (FilterSet.fromstring(f"{x[1]},{x[0]},{x[2]}"), [int(y) for y in x[3:]])
            for x in cast(Sequence[str], sc.args)
        ]
        return Exposure(filts, **sc.opts)  # type: ignore


def _filtersequence(x: Sequence[str | FilterSet]) -> Sequence[FilterSet]:
    return [FilterSet.fromstring(f) for f in x]


@attr.define
class HACFILT(ProtoCommand):
    """Sets filters for :class:`HoldAndCollect` ."""

    filters: Sequence[FilterSet] = attr.field(
        converter=_filtersequence,
        on_setattr=attr.setters.convert,
    )
    _default_filters: Sequence[FilterSet] = attr.field(factory=lambda: [])
    _names: ClassVar[Sequence[str]] = ("HoldAndCollectFILTer", "HACFILT")

    def to_scpicommand(
        self, default_filters: Sequence[FilterSet] | None = None, **kwargs: None
    ) -> SCPICommand:
        if default_filters is None:
            default_filters = []
        if not default_filters and not self.filters:
            raise ValueError("Protocol must have default filters set.")
        if not self.filters:
            comment = "qslib:default_filters"
        else:
            comment = None
        return SCPICommand(
            "HACFILT",
            *(f.hacform for f in (self.filters if self.filters else default_filters)),
            comment=comment,
        )

    @classmethod
    def from_scpicommand(cls, sc: SCPICommand) -> HACFILT:
        c = HACFILT([FilterSet.fromstring(x) for x in cast(Iterable[str], sc.args)])
        if sc.comment and "qslib:default_filters" in sc.comment:
            c._default_filters = c.filters
            c.filters = []
        return c


def _quantity_to_seconds_int(q: PlainQuantity | int) -> int:
    if isinstance(q, PlainQuantity):
        return int(q.m_as("s"))
    else:
        return q


def _maybe_quantity_to_seconds_int(q: PlainQuantity | int | None) -> int | None:
    if isinstance(q, PlainQuantity):
        return int(q.m_as("s"))
    else:
        return q


@attr.define()
class HoldAndCollect(ProtoCommand):
    """A protocol hold (for a time) and collect (set by HACFILT) command."""

    time: PlainQuantity = attr.field(
        converter=_wrap_seconds, on_setattr=attr.setters.convert
    )
    increment: PlainQuantity = attr.field(
        default=_ZERO_SECONDS, converter=_wrap_seconds, on_setattr=attr.setters.convert
    )
    incrementcycle: int = 1
    incrementstep: int = 1
    tiff: bool = False
    quant: bool = True
    pcr: bool = False
    _names: ClassVar[Sequence[str]] = ("HoldAndCollect",)

    def to_scpicommand(self, **kwargs: None) -> SCPICommand:
        opts = {}
        if self.increment != _ZERO_SECONDS:
            opts["increment"] = self.increment.m_as(_SECONDS)
        if self.incrementcycle != HoldAndCollect.incrementcycle:
            opts["incrementcycle"] = self.incrementcycle
        if self.incrementstep != HoldAndCollect.incrementstep:
            opts["incrementstep"] = self.incrementstep
        opts["tiff"] = self.tiff
        opts["quant"] = self.quant
        opts["pcr"] = self.pcr
        return SCPICommand(
            "HoldAndCollect",
            int(self.time.m_as(_SECONDS)),
            comment=None,
            **opts,
        )

    @classmethod
    def from_scpicommand(cls, sc: SCPICommand) -> HoldAndCollect:
        return HoldAndCollect(sc.args[0], **sc.opts)  # type: ignore


@attr.define()
class Hold(ProtoCommand):
    """A protocol hold (for a time) command."""

    time: PlainQuantity | None = attr.field(
        converter=_maybe_wrap_seconds, on_setattr=attr.setters.convert
    )
    increment: PlainQuantity = attr.field(
        converter=_wrap_seconds, on_setattr=attr.setters.convert, default=_ZERO_SECONDS
    )
    incrementcycle: int = 1
    incrementstep: int = 1
    _names: ClassVar[Sequence[str]] = ("HOLD",)

    def to_scpicommand(self, **kwargs: None) -> SCPICommand:
        opts = {}
        if self.increment != _ZERO_SECONDS:
            opts["increment"] = self.increment.m_as(_SECONDS)
        if self.incrementcycle != 1:
            opts["incrementcycle"] = self.incrementcycle
        if self.incrementstep != 1:
            opts["incrementstep"] = self.incrementstep
        return SCPICommand(
            "HOLD",
            self.time.m_as(_SECONDS) if self.time is not None else "",
            comment=None,
            **opts,
        )

    @classmethod
    def from_scpicommand(cls, sc: SCPICommand) -> Hold:
        return Hold(sc.args[0], **sc.opts)  # type: ignore


class XMLable(ABC):
    @abstractmethod
    def to_xml(self, **kwargs: Any) -> ET.Element: ...


G = TypeVar("G")


class _NumOrRefIndexer(Generic[G]):
    _list: list[G]

    def __init__(self, val_list: list[G]):
        self._list = val_list

    @overload
    def _translate_key(self, key: int | str) -> int: ...

    @overload
    def _translate_key(self, key: slice) -> slice: ...

    def _translate_key(self, key: int | str | slice) -> int | slice:
        if isinstance(key, int):
            return key - 1
        elif isinstance(key, str):
            raise NotImplementedError
        elif isinstance(key, slice):
            return slice(key.start - 1, key.stop, key.step)

    def __getitem__(self, key: int | str | slice) -> G | list[G]:
        return self._list[self._translate_key(key)]

    @overload
    def __setitem__(self, key: int | str, val: G) -> None: ...

    @overload
    def __setitem__(self, key: slice, val: Sequence[G]) -> None: ...

    def __setitem__(self, key, val):
        self._list.__setitem__(self._translate_key(key), val)

    def __delitem__(self, key: int | str | slice) -> None:
        del self._list[self._translate_key(key)]

    def __call__(self, key: int | str | slice) -> G | list[G]:
        return self[key]

    def append(self, val: G) -> None:
        return self._list.append(val)

    def __iadd__(self, val: Iterable[G]) -> None:
        self._list += val


class CustomStep(ProtoCommand, XMLable):
    """A protocol step composed of SCPI/protocol commands."""

    _body: list[ProtoCommand]
    _identifier: int | str | None = None
    repeat: int = 1
    _names: ClassVar[Sequence[str]] = ["STEP"]

    def __init__(
        self,
        body: Sequence[ProtoCommand],
        identifier: int | str | None = None,
        repeat: int = 1,
    ):
        self._body = list(body)
        self._identifier = identifier
        self.repeat = repeat

    def info_str(self, index: None | int = None, repeats: int = 1) -> str:
        if index is not None:
            s = f"{index}. "
        else:
            s = "- "
        s += f"Step{' '+str(self._identifier) if self._identifier is not None else ''} of commands:\n"
        s += "\n".join(
            f"  {i+1}. " + c.to_scpicommand().to_string()
            for i, c in enumerate(self._body)
        )
        return s

    def duration_at_cycle_point(
        self, cycle: int, point: int = 1
    ) -> IntQuantity:  # cycle from 1
        return Q_(0, "second")

    def temperatures_at_cycle(self, cycle: int) -> list[ArrayQuantity]:
        return [Q_(np.array(6 * [math.nan]), "degC")]

    def temperatures_at_cycle_point(self, cycle: int, point: int) -> ArrayQuantity:
        return Q_(np.array(6 * [math.nan]), "degC")

    def total_duration(self, repeat: int = 1) -> IntQuantity:
        return 0 * UR.seconds

    @property
    def body(self) -> list[ProtoCommand]:
        return self._body

    @body.setter
    def body(self, v: Sequence[ProtoCommand]) -> None:
        self._body = list(v)

    @property
    def identifier(self) -> int | str | None:
        return self._identifier

    @identifier.setter
    def identifier(self, v: int | str | None) -> None:
        self._identifier = v

    @property
    def collects(self) -> bool:
        return False

    def to_xml(self, **kwargs: Any) -> ET.Element:
        assert not kwargs
        e = ET.Element("TCStep")
        ET.SubElement(e, "CollectionFlag").text = str(
            int(self.collects)
        )  # FIXME: approx
        for t in range(0, 6):  # FIXME
            ET.SubElement(e, "Temperature").text = "30.0"
        ET.SubElement(e, "HoldTime").text = "1"
        # FIXME: does not contain cycle starts, because AB format can't handle
        ET.SubElement(e, "ExtTemperature").text = str(0)
        ET.SubElement(e, "ExtHoldTime").text = str(0)
        # FIXME: RampRate, RampRateUnit
        ET.SubElement(e, "RampRate").text = "1.6"
        ET.SubElement(e, "RampRateUnit").text = "DEGREES_PER_SECOND"
        return e

    def to_scpicommand(self, *, stepindex: int = -1, **kwargs: Any) -> SCPICommand:
        opts = {}
        args: list[int | str | None | Sequence[SCPICommand]] = []
        if self.repeat != 1:
            opts["repeat"] = self.repeat
        if self._identifier:
            args.append(self.identifier)
        else:
            args.append(stepindex)

        args.append([com.to_scpicommand(**kwargs) for com in self.body])

        return SCPICommand("STEP", *args, **opts)  # type: ignore

    @classmethod
    def from_scpicommand(cls, sc: SCPICommand) -> CustomStep:
        # Try a normal step:
        try:
            return Step.from_scpicommand(sc)
        except ValueError:
            return cls(
                [
                    cast(ProtoCommand, x.specialize())
                    for x in cast(Sequence[SCPICommand], sc.args[1])
                ],
                identifier=cast(Union[int, str], sc.args[0]),
                **sc.opts,  # type: ignore
            )


def _filterlist(y: Iterable[FilterSet | str]) -> Sequence[FilterSet]:
    return [FilterSet.fromstring(x) for x in y]


@attr.define
class Step(CustomStep, XMLable):
    """
    A normal protocol step, of a hold and possible collection.

    Parameters
    ----------

    time: int
        The step time setting, in seconds.
    temperature: float | Sequence[float]
        The temperature hold setting, either as a float (all zones the same) or a sequence
        (of correct length) of floats setting the temperature for each zone.
    collect
        Collect fluorescence data?  If None (default), collect only if the Step has an explicit
        filters setting.
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

    time: PlainQuantity = attr.field(
        converter=_wrap_seconds, on_setattr=attr.setters.convert
    )
    temperature: PlainQuantity = attr.field(
        converter=_wrapunitmaybelist_degC, on_setattr=attr.setters.convert
    )
    collect: bool | None = None
    temp_increment: FloatQuantity = attr.field(
        default=_ZEROTEMPDELTA,
        converter=_wrap_delta_degC,
        on_setattr=attr.setters.convert,
    )
    temp_incrementcycle: int = 2
    temp_incrementpoint: int | None = None
    time_increment: IntQuantity = attr.field(
        default=_ZERO_SECONDS,
        converter=_wrap_seconds,
        on_setattr=attr.setters.convert,
    )
    time_incrementcycle: int = 2
    time_incrementpoint: int | None = None
    filters: Sequence[FilterSet] = attr.field(
        default=tuple(),
        converter=_filterlist,
        on_setattr=attr.setters.convert,
    )
    pcr: bool = False
    quant: bool = True
    tiff: bool = False
    repeat: int = 1
    _default_filters: Sequence[FilterSet] = attr.field(default=tuple())
    _classname: ClassVar[str] = "Step"

    def __eq__(self, other: object) -> bool:  # FIXME: add other stuff
        if not isinstance(other, Step):
            return False
        if self.__class__ != other.__class__:
            return False
        if np.all(self.temperature_list != other.temperature_list):
            return False
        # FIXME
        if (self.filters) and (other.filters) and self._filtersets != other._filtersets:
            return False
        if self.time != other.time:
            return False
        if self.collects != other.collects:
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
    def collects(self) -> bool:
        if self.collect is None:
            return len(self.filters) > 0
        return self.collect

    @property
    def _filtersets(self) -> list[FilterSet]:
        return [FilterSet.fromstring(x) for x in self.filters]

    @property
    def _machine_temp_incrementpoint(self) -> int:
        if self.temp_incrementpoint is None:
            return self.repeat + 1
        else:
            return self.temp_incrementpoint

    @property
    def _machine_time_incrementpoint(self) -> int:
        if self.time_incrementpoint is None:
            return self.repeat + 1
        else:
            return self.time_incrementpoint

    def info_str(self, index: None | int = None, repeats: int = 1) -> str:
        "String describing the step."

        temps_c1p1 = self.temperatures_at_cycle_point(1, 1)
        if self.repeat is not None:
            temps_c1pe = self.temperatures_at_cycle_point(1, self.repeat)
        else:
            temps_c1pe = temps_c1p1

        if not np.all(temps_c1pe == temps_c1p1):
            tempstr = f"({_temp_format(temps_c1p1)} to {_temp_format(temps_c1pe)})"
        else:
            tempstr = _temp_format(temps_c1p1)

        temps_cep1 = self.temperatures_at_cycle_point(repeats, 1)
        if self.repeat is not None:
            temps_cepe = self.temperatures_at_cycle_point(repeats, self.repeat)
        else:
            temps_cepe = temps_cep1

        if not np.all(temps_c1p1 == temps_cep1) or not np.all(temps_cep1 == temps_cepe):
            if not np.all(temps_cep1 == temps_cepe):
                tempstr += (
                    f" to ({_temp_format(temps_cep1)} to {_temp_format(temps_cepe)})"
                )
            else:
                tempstr += f" to {_temp_format(temps_cep1)}"

        time_c1p1 = self.duration_at_cycle_point(1, 1)
        time_c1pe = self.duration_at_cycle_point(
            1, self.repeat if self.repeat is not None else 1
        )
        time_cep1 = self.duration_at_cycle_point(repeats, 1)
        time_cepe = self.duration_at_cycle_point(
            repeats, self.repeat if self.repeat is not None else 1
        )

        if self.repeat > 1:
            if np.all(time_c1p1 == time_c1pe) and np.all(time_c1p1 == time_cep1):
                tempstr += (
                    f" for {self.repeat} points at {_durformat(time_c1p1)}/point"
                    f" ({_durformat(time_c1p1*self.repeat)}/cycle)"
                )
            elif np.all(time_c1p1 == time_c1pe):
                tempstr += (
                    f" for {self.repeat} points at {_durformat(time_c1p1)}/point to {_durformat(time_cep1)}/point"
                    f" ({_durformat(time_c1p1*self.repeat)}/cycle to {_durformat(time_cep1*self.repeat)}/cycle)"
                )
            elif np.all(time_c1p1 == time_cep1):
                tempstr += (
                    f" for {self.repeat} points at ({_durformat(time_c1p1)}/point to {_durformat(time_cep1)}/point) "
                    f"({_durformat(self.duration_of_cycle(1))}/cycle)"
                )
            else:
                tempstr += (
                    f" for {self.repeat} points at ({_durformat(time_c1p1)}/point to {_durformat(time_c1pe)}/point) to "
                    f"({_durformat(time_cep1)}/point to {_durformat(time_cepe)}/point)"
                    f" ({_durformat(self.duration_of_cycle(1))}/cycle to "
                    f"{_durformat(self.duration_of_cycle(repeats))}/cycle)"
                )

        else:
            if np.all(time_c1p1 == time_cep1):
                tempstr += f" for {_durformat(time_c1p1)}/cycle"
            else:
                tempstr += f" for {_durformat(time_c1p1)}/cycle to {_durformat(time_cep1)}/cycle"

        elems = [tempstr]
        if self.temp_increment != 0.0:
            if (self.repeat > 1) and (self._machine_temp_incrementpoint < self.repeat):
                elems.append(f"{self.temp_increment:+~}/point")
                if self._machine_temp_incrementpoint != 2:
                    elems[-1] += f" from point {self._machine_temp_incrementpoint}"
            if (repeats > 1) and (self.temp_incrementcycle < repeats):
                elems.append(f"{self.temp_increment:+~}/cycle")
                if self.temp_incrementcycle != 2:
                    elems[-1] += f" from cycle {self.temp_incrementcycle}"
        if self.time_increment != 0.0:
            if (self.repeat > 1) and (self._machine_time_incrementpoint < self.repeat):
                elems.append(f"{_durformat(self.time_increment)}/point")
                if self._machine_time_incrementpoint != 2:
                    elems[-1] += f" from cycle {self._machine_time_incrementpoint}"
            if (repeats > 1) and (self.time_incrementcycle < repeats):
                elems.append(f"{_durformat(self.time_increment)}/cycle")
                if self.time_incrementcycle != 2:
                    elems[-1] += f" from cycle {self.time_incrementcycle}"
        # if self.ramp_rate != 1.6:
        #    elems.append(f"{self.ramp_rate} °C/s ramp")
        s = f"{index}. " + ", ".join(elems)

        if self.collects:
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

    def total_duration(self, repeats: int = 1) -> PlainQuantity:
        return sum(
            (self.duration_of_cycle(c) for c in range(1, repeats + 1)), 0 * UR.seconds
        )

    def duration_at_cycle_point(self, cycle: int, point: int = 1) -> PlainQuantity:
        "Durations of the step at `cycle` (from 1)"
        inccycles = max(0, cycle + 1 - self.time_incrementcycle)
        incpoints = max(0, point + 1 - self._machine_time_incrementpoint)
        return self.time + (inccycles + incpoints) * self.time_increment

    def duration_of_cycle(self, cycle: int) -> PlainQuantity:
        return sum(self.durations_at_cycle(cycle), 0 * UR.seconds)

    def durations_at_cycle(self, cycle: int) -> list[PlainQuantity]:  # cycle from 1
        "Duration of the step (excluding ramp) at `cycle` (from 1)"
        return [
            self.duration_at_cycle_point(cycle, point)
            for point in range(1, self.repeat + 1)
        ]

    def temperatures_at_cycle_point(self, cycle: int, point: int) -> ArrayQuantity:
        "Temperatures of the step at `cycle` (from 1)"
        inccycles = max(0, cycle + 1 - self.temp_incrementcycle)
        incpoints = max(0, point + 1 - self._machine_temp_incrementpoint)
        return self.temperature_list + (inccycles + incpoints) * self.temp_increment

    def temperatures_at_cycle(self, cycle: int) -> list[ArrayQuantity]:
        "Temperatures of the step at `cycle` (from 1)"
        return [
            self.temperatures_at_cycle_point(cycle, point)
            for point in range(1, self.repeat + 1)
        ]

    @property
    def identifier(self) -> int | str | None:
        return None

    @identifier.setter
    def identifier(self, v: Any) -> None:
        raise ValueError

    @property
    def temperature_list(self) -> ArrayQuantity:
        mag = self.temperature.to("degC").magnitude  # FIXME
        if isinstance(mag, np.ndarray):
            return Q_(mag, "degC")
        else:
            return Q_(NZONES * [mag], "degC")

    @property
    def body(self) -> list[ProtoCommand]:
        if self.collects:
            return [
                Ramp(
                    self.temperature_list,
                    self.temp_increment,
                    self.temp_incrementcycle,
                    self._machine_temp_incrementpoint,
                ),
                HACFILT(self.filters),
                HoldAndCollect(
                    self.time,
                    self.time_increment,
                    self.time_incrementcycle,
                    self._machine_time_incrementpoint,
                    self.tiff,
                    self.quant,
                    self.pcr,
                ),
            ]
        else:
            return [
                Ramp(
                    self.temperature_list,
                    self.temp_increment,
                    self.temp_incrementcycle,
                    self._machine_temp_incrementpoint,
                ),
                Hold(
                    self.time,
                    self.time_increment,
                    self.time_incrementcycle,
                    self._machine_time_incrementpoint,
                ),
            ]

    @body.setter
    def body(self, v: Any) -> None:
        raise ValueError

    @classmethod
    def from_xml(
        cls, e: ET.Element, *, etc: int = 1, ehtc: int = 1, he: bool = False
    ) -> Step:
        collect = bool(int(e.findtext("CollectionFlag") or 0))
        ts: ArrayQuantity = Q_(
            [float(x.text or math.nan) for x in e.findall("Temperature")], "degC"
        )
        ht: IntQuantity = int(e.findtext("HoldTime") or 0) * UR.seconds
        et: FloatQuantity = float(e.findtext("ExtTemperature") or 0.0) * UR.delta_degC
        eht: IntQuantity = int(e.findtext("ExtHoldTime") or 0) * UR.seconds
        if not he:
            et = _ZEROTEMPDELTA
            eht = 0 * UR.seconds

        return Step(
            time=ht,
            temperature=ts,
            collect=collect,
            temp_increment=et,
            temp_incrementcycle=etc,
            temp_incrementpoint=1,
            time_increment=eht,
            time_incrementcycle=ehtc,
            time_incrementpoint=1,
            filters=[],
            pcr=True,
        )

    def to_xml(self, **kwargs: Any) -> ET.Element:
        assert not kwargs
        e = ET.Element("TCStep")
        ET.SubElement(e, "CollectionFlag").text = str(
            int(self.collects)
        )  # FIXME: approx
        for t in self.temperature_list.to("°C").magnitude:
            ET.SubElement(e, "Temperature").text = str(t)
        ET.SubElement(e, "HoldTime").text = str(int(self.time.to("seconds").magnitude))
        # FIXME: does not contain cycle starts, because AB format can't handle
        ET.SubElement(e, "ExtTemperature").text = str(
            self.temp_increment.to("delta_degC").magnitude
        )
        ET.SubElement(e, "ExtHoldTime").text = str(
            int(self.time_increment.to("seconds").magnitude)
        )
        # FIXME: RampRate, RampRateUnit
        ET.SubElement(e, "RampRate").text = "1.6"
        ET.SubElement(e, "RampRateUnit").text = "DEGREES_PER_SECOND"
        return e

    @classmethod
    def from_scpicommand(cls, sc: SCPICommand) -> Step:
        coms = [x.specialize() for x in cast(Sequence[SCPICommand], sc.args[1])]

        # FIXME: When willing to require Python 3.10, this can be cleaned up with a match statement.

        h: Hold | HoldAndCollect

        repeat = cast(int, sc.opts.get("repeat", 1))  # FIXME: check cast

        com_classes = [x.__class__ for x in coms]
        if com_classes == [Ramp, HACFILT, HoldAndCollect]:
            r = cast(Ramp, coms[0])
            hcf = cast(HACFILT, coms[1])
            h = cast(HoldAndCollect, coms[2])
            c = cls(
                h.time,
                r.temperature,
                time_incrementcycle=1,
                temp_incrementcycle=1,
                repeat=repeat,
            )
            c.collect = True
            if hcf._default_filters:
                c.filters = []
                c.collect = True
                c._default_filters = hcf._default_filters
            else:
                c.filters = hcf.filters
            c.time_increment = h.increment
            c.temp_increment = r.increment
            c.time_incrementcycle = h.incrementcycle
            c.time_incrementpoint = (
                h.incrementstep if h.incrementstep <= repeat else None
            )
            c.temp_incrementcycle = r.incrementcycle
            c.temp_incrementpoint = (
                r.incrementstep if r.incrementstep <= repeat else None
            )

        elif com_classes == [Ramp, Hold]:
            r = cast(Ramp, coms[0])
            h = cast(Hold, coms[1])
            if h.time is None:
                raise ValueError
            c = cls(
                h.time,
                r.temperature,
                time_incrementcycle=1,
                temp_incrementcycle=1,
                repeat=repeat,
            )
            c.collect = False
            c.time_increment = h.increment
            c.temp_increment = r.increment
            c.time_incrementcycle = h.incrementcycle
            c.time_incrementpoint = (
                h.incrementstep if h.incrementstep <= repeat else None
            )
            c.temp_incrementcycle = r.incrementcycle
            c.temp_incrementpoint = (
                r.incrementstep if r.incrementstep <= repeat else None
            )
        else:
            raise ValueError
        return c

    @classmethod
    def fromdict(cls, d: dict[str, Any]) -> "Step":
        return cls(**d)


def _temp_format(x: PlainQuantity | ArrayQuantity) -> str:
    if isinstance(x.m, np.ndarray):
        if len(set(x.m)) == 1:
            return f"{x.m[0]:.2f}{x.u:~}"
        else:
            return f"[{', '.join(f'{x:.2f}' for x in x.m)}]{x.u:~}"
    else:
        return f"{x.m:.2f}{x.u:~}"


def _bsl(x: Iterable[CustomStep] | CustomStep) -> Sequence[CustomStep]:
    return list(x) if isinstance(x, Iterable) else [x]


def _maybelist_cs(x: CustomStep | Sequence[CustomStep]) -> Sequence[CustomStep]:
    return [x] if not isinstance(x, Sequence) else list(x)


@attr.define
class Stage(XMLable, ProtoCommand):
    """A Stage in a protocol, composed of :class:`Step` s with a possible repeat."""

    steps: Sequence[CustomStep] = attr.field(converter=_maybelist_cs)
    repeat: int = 1
    index: int | None = None
    label: str | None = None
    _default_filters: Sequence[FilterSet] = attr.field(default=tuple())
    _classname: ClassVar[str] = "Stage"
    _names: ClassVar[Sequence[str]] = ("STAGe",)

    @property
    def step(self) -> _NumOrRefIndexer[CustomStep]:
        return _NumOrRefIndexer(list(self.steps))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Stage):
            return False
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
        return self.steps == other.steps

    @classmethod
    def stepped_ramp(
        cls: Type[Stage],
        from_temperature: float | str | FloatQuantity | Sequence[float],
        to_temperature: float | str | FloatQuantity | Sequence[float],
        total_time: int | str | IntQuantity,
        *,
        n_steps: int | None = None,
        temperature_step: float | str | FloatQuantity | None = None,
        points_per_step: int = 1,
        collect: bool | None = None,
        filters: Sequence[str | FilterSet] = tuple(),
        start_increment: bool = False,
    ) -> Stage:
        """Hold at a series of temperatures, from one to another.

        Parameters
        ----------
        from_temperature
            Initial temperature/s (inclusive).  If None, uses the final temperature of
            the previous stage (FIXME: None is not currently handled).
        to_temperature
            Final temperature/s (inclusive).
        total_time
            Total time for the stage
        n_steps
            Number of steps.  If None, uses 1.0 Δ°C steps, or, if
            doing a multi-temperature change, uses maximum step
            of 1.0 Δ°C.  If n_steps is specified, it is the number of
            temperature *steps* to take.  Normally, since there is
            an initial cycle of the starting temperatures, this means
            there will be `n_steps + 1` cycles.  If start_increment is True,
            and the initial cycle is already stepped away from the starting
            temperature, then there will be only `n_steps` cycles.
        temperature_step
            Step temperature change (optional).  Must be None,
            or correctly match calculation, if n_steps is not
            None.  If both this and n_steps are None, default
            is 1.0 Δ°C steps.  If temperature step does not
            exactly fit range, it will be adjusted, with a warning
            if the change is more than 5%.  Sign is ignored.  If
            doing a multi-temperature change, then this is the
            maximum temperature step.
        collect
            Collect data?  If None, collects data if filters is set explicitly.
        filters
            Filters to collect.
        start_increment
            If False (default), start at the `from_temperature`, holding there
            for the same hold time as every other temperature.  If True, start
            one step away from the `from_temperature`.  This is useful, for
            example, if the previous stage held at a particular temperature,
            and you now want to step *away* from that temperature.  When True,
            note the remarks about n_steps above.

        Returns
        -------
        Stage
            The resulting stage.
        """

        from_temperature = _wrapunitmaybelist_degC(from_temperature)
        to_temperature = _wrapunitmaybelist_degC(to_temperature)

        delta = to_temperature - from_temperature

        multistep = False

        if hasattr(delta, "shape") and len(delta.shape) > 0:
            max_delta = delta.max()  # type: FloatQuantity
        else:
            max_delta = delta

        total_time = cast(IntQuantity, _wrap_seconds(total_time).to("seconds"))

        if n_steps is None:
            if temperature_step is None:
                temperature_step = Q_(1.0, "delta_degC")
                autoset_step = True
            else:
                temperature_step = abs(_wrap_delta_degC(temperature_step))  # type: ignore
                autoset_step = False

            n_steps = max(
                abs(round((max_delta / temperature_step).to("").magnitude))
                + (0 if start_increment else 1),
                1,
            )

            real_max_temperature_step = abs(
                max_delta / (n_steps - (0 if start_increment else 1))
            )

            change = (
                ((real_max_temperature_step - temperature_step) / temperature_step)
                .to("")
                .magnitude
            )

            if (abs(change) > 0.05) and not autoset_step:
                warnings.warn(
                    f"Desired temperature step {temperature_step} differs by {100*change}% "
                    f"from actual {real_max_temperature_step}."
                )

        elif temperature_step is not None:
            temperature_step = abs(_wrap_delta_degC(temperature_step))  # type: ignore
            if (
                abs(round((max_delta / temperature_step).to("").magnitude))
                + (0 if start_increment else 1)
                != n_steps
            ):
                raise ValueError(
                    "Both n_steps and temperature_step set, and calculated steps don't match set steps."
                )

        temp_increment = (
            (to_temperature - from_temperature)
            / (n_steps - (0 if start_increment else 1))
        ).round(4)

        # If the temp_increment is entirely equal, we are not multistep, and we should
        # have only a single temp_increment.

        if hasattr(temp_increment, "shape") and len(temp_increment.shape) > 0:
            if (temp_increment != temp_increment[0]).any():
                multistep = True
            else:
                temp_increment = temp_increment[0]

        step_time = cast(IntQuantity, total_time / n_steps).round()

        if not multistep:
            return cls(
                [
                    Step(
                        step_time / points_per_step,
                        from_temperature,
                        collect=collect,
                        temp_increment=temp_increment,
                        filters=filters,
                        temp_incrementcycle=(1 if start_increment else 2),
                        repeat=points_per_step,
                    )
                ],
                repeat=n_steps,
            )

        # MULTISTEP!

        return cls(
            [
                Step(
                    step_time / points_per_step,
                    from_temperature
                    + (step_i + (1 if start_increment else 0)) * temp_increment,
                    collect=collect,
                    filters=filters,
                    repeat=points_per_step,
                )
                for step_i in range(0, n_steps)
            ]
        )

    @classmethod
    def hold_at(
        cls: Type[Stage],
        temperature: float | str | Sequence[float],
        total_time: int | str | IntQuantity,
        step_time: int | str | IntQuantity | None = None,
        collect: bool | None = None,
        filters: Sequence[str | FilterSet] = tuple(),
    ) -> Stage:
        """Hold at a temperature for a set amount of time, with steps of a configurable fixed time.


        Parameters
        ----------
        temperatures
            The temperature or temperatures to hold.  If not strings or quantities, the value/values are
            interpreted as °C.
        total_time
            Desired total time for the stage.  If this is not a multiple of `step_time`, it may not be the actual
            total time for the stage.  The function will emit a warning if the difference is more than 10%.  If
            an integer, value is interpreted as seconds.
        step_time
            If None (default), the stage will have one step.  Otherwise, it will have steps of this time.  If
            an integer, value is interpreted as seconds.
        collect
            Whether or not each step should collect fluorescence data.  If None (default), collects data
            if filters is explicitly set.
        filters
            A list of filters to collect.  If empty, and collect is True, then each step will collect the
            default filters for the :class:`Protocol`.

        Returns
        -------
        Stage
            The resulting Stage

        Raises
        ------
        ValueError
            If step time is larger than total time.
        """
        if step_time is None:
            step_time = total_time

        step_time = _wrap_seconds(step_time)
        total_time = _wrap_seconds(total_time)

        if step_time > total_time:
            raise ValueError(
                f"Step time {step_time} > total time {total_time}.  Did you mix up the parameter order?"
            )

        repeat = round((total_time / step_time).to("").magnitude)

        real_total_time = repeat * step_time

        chg = ((real_total_time - total_time) / total_time).to("").magnitude

        if abs(chg) > 0.1:
            warnings.warn(
                f"Stage will have total time {real_total_time}, with {repeat} steps, "
                f"{100*chg} different from desired time {total_time}."
            )

        return cls(
            [
                Step(
                    step_time,
                    _wrapunitmaybelist_degC(temperature),
                    collect=collect,
                    filters=filters,
                )
            ],
            repeat=repeat,
        )

    hold_for = hold_at

    def __repr__(self) -> str:
        s = "Stage(steps="
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
                step.duration_at_cycle_point(cycle, point).to("seconds").magnitude
                for cycle in range(1, self.repeat + 1)
                for step in self.steps
                for point in range(1, step.repeat + 1)
            ]
        )
        temperatures = np.array(
            [
                step.temperatures_at_cycle_point(cycle, point).to("°C").magnitude
                for cycle in range(1, self.repeat + 1)
                for step in self.steps
                for point in range(1, step.repeat + 1)
            ]
        )
        ramp_rates = [
            1.6 # FIXME: should this be 1.56?
            for _ in range(1, self.repeat + 1)
            for step in self.steps
            for point in range(1, step.repeat + 1)
        ]
        # np.array(
        #    [step.ramp_rate for _ in range(1, self.repeat + 1) for step in self.body]
        # )
        collect_data = np.array(
            [
                step.collects
                for _ in range(1, self.repeat + 1)
                for step in self.steps
                for _ in range(1, step.repeat + 1)
            ]
        )

        # FIXME: is this how ramp rates actually work?

        ramp_durations = np.zeros(len(durations))
        if previous_temperatures is not None:
            ramp_durations[0] = (
                np.max(np.abs(temperatures[0] - previous_temperatures)) / ramp_rates[0]
            )
            ramp_durations[1:] = (
                np.max(np.abs(temperatures[1:] - temperatures[:-1]), axis=1)
                / ramp_rates[1:]
            )

        tot_durations = durations + ramp_durations

        start_times = start_time + np.zeros(len(durations))
        start_times[0] = start_time + ramp_durations[0]
        start_times[1:] = (
            start_time + np.cumsum(tot_durations[:-1]) + ramp_durations[1:]
        )

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
            c
            for c in range(1, self.repeat + 1)
            for (s, step) in enumerate(self.steps)
            for point in range(1, step.repeat + 1)
        ]
        data["step"] = [
            s + 1
            for c in range(1, self.repeat + 1)
            for (s, step) in enumerate(self.steps)
            for point in range(1, step.repeat + 1)
        ]
        data["point"] = [
            point
            for c in range(1, self.repeat + 1)
            for (s, step) in enumerate(self.steps)
            for point in range(1, step.repeat + 1)
        ]

        data.set_index(["cycle", "step", "point"], inplace=True)

        return data

    def to_scpicommand(
        self, stageindex: int | str | None = None, **kwargs: Any
    ) -> SCPICommand:
        opts = {}
        args: list[int | str | list[SCPICommand]] = []
        if self.repeat != 1:
            opts["repeat"] = self.repeat
        if self.index:
            index_to_use: int | str = self.index
        elif stageindex:
            index_to_use = stageindex
        else:
            raise ValueError("No index.")
        args.append(index_to_use)
        args.append(self.label or f"STAGE_{index_to_use}")
        args.append(
            [
                step.to_scpicommand(stepindex=i + 1, **kwargs)
                for i, step in enumerate(self.steps)
            ]
        )

        return SCPICommand("STAGe", *args, comment=None, **opts)

    @classmethod
    def from_scpicommand(cls, sc: SCPICommand, **kwargs: Any) -> Stage:
        c = cls(
            [
                cast(CustomStep, x.specialize(**kwargs))
                for x in cast(Sequence[SCPICommand], sc.args[2])
            ],
            index=cast(int, sc.args[0]),
            label=cast(Optional[str], sc.args[1]),
            **sc.opts,  # type: ignore
        )

        dfilt: list[FilterSet] = []

        for s in c.steps:
            if isinstance(s, Step) and s._default_filters:
                ndf = s._default_filters
                if len(dfilt) == 0:
                    dfilt = list(ndf)
                if dfilt != ndf:
                    raise ValueError("Inconsistent default filters")
        c._default_filters = dfilt

        return c

    @classmethod
    def from_xml(cls, e: ET.Element) -> Stage:
        rep = int(e.findtext("NumOfRepetitions") or 1)
        startcycle = int(cast(str, e.findtext("StartingCycle")))
        ade = e.findtext("AutoDeltaEnabled") == "true"
        steps: list[CustomStep] = [
            Step.from_xml(x, etc=startcycle, ehtc=startcycle, he=ade)
            for x in e.findall("TCStep")
        ]
        return cls(steps, rep)

    def to_xml(self, **kwargs: Any) -> ET.Element:
        assert not kwargs
        e = ET.Element("TCStage")
        ET.SubElement(e, "StageFlag").text = "CYCLING"
        ET.SubElement(e, "NumOfRepetitions").text = str(int(self.repeat))
        scycle: set[int] = set()
        for s in self.steps:
            if isinstance(s, Step):
                scycle.add(s.temp_incrementcycle)
                scycle.add(s.time_incrementcycle)
            if isinstance(s, XMLable):
                e.append(s.to_xml())
        if len(scycle) > 1:
            log.warn("Approx")
        if scycle:
            ET.SubElement(e, "StartingCycle").text = str(next(iter(scycle)))
        ET.SubElement(e, "AutoDeltaEnabled").text = "true"

        return e

    def info_str(self, index: int | None = None) -> str:
        if self.repeat > 1:
            adds = "s"
        else:
            adds = ""
        stagestr = f"{index}. Stage with {self.repeat} cycle{adds}"
        stepstrs = [
            textwrap.indent(f"{step.info_str(i+1, self.repeat)}", "    ")
            for i, step in enumerate(self.steps)
        ]
        try:
            tot_dur = sum(
                (x.total_duration(self.repeat) for x in self.steps),
                _ZERO_SECONDS,
            )
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
        s = [cast(CustomStep, Step(**x)) for x in d["_steps"]]
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


@attr.define
class Protocol(ProtoCommand):
    """A run protocol for the QuantStudio.  Protocols encapsulate the temperature and camera
    controls for an entire run.  They are composed of :class:`Stage`s, which may repeat for a
    number of cycles, and the stages are in turn composed of Steps, which may be created for
    usual cases with :class:`Step`, or from SCPI commands.  Steps may repeat
    their contents as well, but this is not yet implemeted.

    Parameters
    ----------
    stages: Iterable[Stage]
        The stages of the protocol, likely :class:`Stage`.
    stage: _NumOrRefIndexer[Stage]
        A more convenient way of accessing the stages of the protocol, with
        numbering that matches the machine.
    name: str | None
        A protocol name. If not set, a timestamp will be used, unlike AB's uuid.
    volume: float
        The sample volume, in µL.
    runmode: str | None
        The run mode.
    covertemperature: float (default 105.0)
        The cover temperature
    filters: Sequence[str]
        A list of default filters that can be used by any collection commands
        that don't specify their own.
    prerun: Sequence[SCPICommand]
        Sets PRERUN.  *DO NOT USE THIS UNLESS YOU KNOW WHAT YOU ARE DOING*.
    postrun: Sequence[SCPICommand]
        Sets POSTRUN. *DO NOT USE THIS UNLESS YOU KNOW WHAT YOU ARE DOING*.
    """

    stages: list[Stage] = attr.field(factory=list)
    name: str = attr.field(factory=lambda: "Prot_" + _nowuuid())
    volume: float = 50.0
    runmode: str = "standard"  # standard or fast... need to deal with this
    filters: Sequence[FilterSet] = attr.field(
        factory=lambda: [],
        converter=_filterlist,
        on_setattr=attr.setters.convert,
    )
    covertemperature: float = 105.0
    prerun: Sequence[SCPICommandLike] = attr.field(factory=list)
    postrun: Sequence[SCPICommandLike] = attr.field(factory=list)
    _classname: str = "Protocol"
    _names: ClassVar[Sequence[str]] = ("PROTocol", "PROT")

    @property
    def stage(self) -> _NumOrRefIndexer[Stage]:
        """
        A more convenient view of :any:`Protocol.stages`.  This allows one-indexed access,
        such that `protocol.stage[5] == protocol.stages[6]` is stage 5 of the protocol, in
        the interpretation of tha machine.  Indexing can use slices, and is *inclusive*, so
        `protocol.stage[5:6]` returns stages 5 and 6.  Getting, setting, and appending
        stages are all supported through this interface.
        """
        return _NumOrRefIndexer(self.stages)

    def to_scpicommand(self, **kwargs: Any) -> SCPICommand:
        assert not kwargs
        args: list[int | str | Sequence[SCPICommand]] = []
        opts: dict[str, int | float | str] = {}
        if self.volume is not None:
            opts["volume"] = self.volume
        if self.runmode is not None:
            opts["runmode"] = self.runmode
        args.append(self.name)
        stages: list[SCPICommand] = []
        if self.prerun:
            stages.append(
                SCPICommand("PRERun", [s.to_scpicommand() for s in self.prerun])
            )

        stages += [
            stage.to_scpicommand(
                filters=self.filters, stageindex=i + 1, default_filters=self.filters
            )
            for i, stage in enumerate(self.stages)
        ]

        if self.postrun:
            stages.append(
                SCPICommand("POSTRun", [s.to_scpicommand() for s in self.postrun])
            )

        args.append(stages)

        return SCPICommand("PROTocol", *args, comment=None, **opts)

    @classmethod
    def from_scpicommand(cls: Type[Protocol], sc: SCPICommand) -> Protocol:
        stage_commands = [x for x in cast(Sequence[SCPICommand], sc.args[1])]

        if stage_commands[0].command.upper() == "PRERUN":
            prerun = cast(Sequence[SCPICommand], stage_commands[0].args[0])
            assert isinstance(prerun, Sequence)
            del stage_commands[0]
        else:
            prerun = []
        if stage_commands[-1].command.upper() == "POSTRUN":
            postrun = cast(Sequence[SCPICommand], stage_commands[-1].args[0])
            assert isinstance(postrun, Sequence)
            del stage_commands[-1]
        else:
            postrun = []

        stages = [cast(Stage, x.specialize()) for x in stage_commands]
        for s in stages:
            if not isinstance(s, Stage):
                raise ValueError

        name = sc.args[0]
        if not isinstance(name, str):
            raise ValueError

        opts = sc.opts

        c = cls(stages, name, prerun=prerun, postrun=postrun, **opts)  # type: ignore

        dfilt: list[FilterSet] = []

        for s in c.stages:
            if hasattr(s, "_default_filters") and s._default_filters:
                ndf = s._default_filters
                if len(dfilt) == 0:
                    dfilt = list(ndf)
                if dfilt != ndf:
                    raise ValueError("Inconsistent default filters")
        c.filters = dfilt

        return c

    @property
    def dataframe(self) -> pd.DataFrame:
        "A DataFrame of the temperature protocol."
        dataframes: list[pd.DataFrame] = []

        stage_start_time = 0.0
        stagenum = 1
        previous_temperatures = [25.0] * 6

        for stage in self.stages:
            dataframe = stage.dataframe(stage_start_time, list(previous_temperatures))
            stagenum += 1
            previous_temperatures = dataframe.iloc[-1].loc[ALLTEMPS].to_numpy()
            stage_start_time = cast(float, dataframe["end_time"].iloc[-1])
            dataframes.append(dataframe)

        return pd.concat(
            dataframes,
            keys=range(1, len(dataframes) + 1),
            names=["stage", "step", "cycle", "point"],
        )

    @property
    def all_points(self) -> pd.DataFrame:
        d = self.dataframe
        dd = (
            d.loc[:, "temperature_avg":]  # type: ignore
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

    @property
    def all_filters(self) -> Collection[FilterSet]:
        "A list of all filters used at some point in the protocol."
        filters = {FilterSet.fromstring(f) for f in self.filters}

        for stage in self.stages:
            for step in stage.steps:
                if isinstance(step, Step) and step.collect:
                    filters |= {FilterSet.fromstring(f) for f in step.filters}

        return filters

    def copy(self) -> Protocol:
        """Returns a new copy (recursively) of the protocol."""
        return deepcopy(self)

    @property
    def all_temperatures(self) -> np.ndarray:
        "An array of temperature settings at `all_times`."
        d = self.dataframe
        return np.repeat(d["temperature_avg":], 2)  # type: ignore

    def plot_protocol(
        self, ax: Optional[plt.Axes] = None
    ) -> Tuple[plt.Axes, Tuple[List[plt.Line2D], List[plt.Line2D]]]:
        "A plot of the temperature and data collection points."

        import matplotlib.pyplot as plt

        #    try:
        #    except ModuleNotFoundError:
        #        raise "

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
        svol = float(e.findtext("SampleVolume") or 50.0)
        runmode = e.findtext("RunMode") or "standard"
        covertemperature = float(e.findtext("CoverTemperature") or 105.0)
        protoname = e.findtext("ProtocolName")
        if protoname is None:
            raise ValueError
        filter_e = e.find("CollectionProfile")
        filters = []
        if filter_e:
            for x in filter_e.findall("CollectionCondition/FilterSet"):
                filters.append(x.attrib["Excitation"] + "-" + x.attrib["Emission"])
        stages = [Stage.from_xml(x) for x in e.findall("TCStage")]
        return Protocol(stages, protoname, svol, runmode, filters, covertemperature)

    def to_xml(
        self, covertemperature: float = 105.0
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
        _set_or_create(qe, "QSLibProtocolCommand").text = (
            self.to_scpicommand().to_string()
        )
        _set_or_create(qe, "QSLibProtocol").text = str(attr.asdict(self))
        _set_or_create(qe, "QSLibVerson").text = __version__
        _set_or_create(e, "CoverTemperature").text = str(covertemperature)
        _set_or_create(e, "SampleVolume").text = str(self.volume or 50)
        _set_or_create(e, "RunMode").text = str(self.runmode or "Standard")
        _set_or_create(e, "UserName").text = ""
        _set_or_create(e, "TubeType").text = "0"
        _set_or_create(e, "BlockID").text = "18"  # FIXME
        _set_or_create(e, "Delay").text = "0.0"
        _set_or_create(e, "ExtendedPCRCycles").text = "0"
        _set_or_create(e, "ExtendedHoldTemp").text = "0"
        _set_or_create(e, "ExtendedHoldTime").text = "0"
        if self.filters:
            x = _set_or_create(e, "CollectionProfile", ProfileId="1")
            for f in self.filters:
                if not isinstance(f, FilterSet):
                    f = FilterSet.fromstring(f)
                x.append(f.to_xml())
        for s in self.stages:
            e.append(s.to_xml())
        return te, tqe

    def info(self) -> str:
        """Generate a (markdown) text protocol description."""
        return str(self)

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
                "(default filters "
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

    def _repr_markdown_(self) -> str:
        return str(self)

    @classmethod
    def fromdict(cls, d: dict[str, Any]) -> "Protocol":
        s = [Stage(**x) for x in d["stages"]]
        del d["stages"]
        return cls(s, **d)

    def check_compatible(self, new: Protocol, status: RunStatus) -> bool:
        """Checks compatibility for changing a running protocol to a new one.

        Raises ValueError if incompatible, returns True if compatible.

        Parameters
        ----------
        new
            New protocol.
        status
            Current run status.

        Raises
        ------
        ValueError
            Protocols are incompatible.
        """
        # Sample sample volume? (FIXME: We can't change it right now.)
        # assert self.volume == new.volume
        # assert self.name == new.name

        for i, (oldstage, newstage) in enumerate(zip_longest(self.stages, new.stages)):
            if (
                i + 1 < status.stage
            ):  # If the stage has already passed, we must be equal
                if oldstage != newstage:
                    raise ValueError
            elif (
                i + 1 == status.stage
            ):  # Current stage.  Only change is # cycles, >= current
                if newstage.repeat < status.cycle:
                    raise ValueError
                oldstage.repeat = newstage.repeat  # for comparison
                if oldstage != newstage:
                    raise ValueError
            else:
                continue

        return True

    def validate(self, fix: bool = True):
        for i, stage in enumerate(self.stages):
            if (stage.index is not None) and (stage.index != i + 1):
                if fix:
                    log.warn(
                        "Stage %s is at index %d of protocol, but has set index %d. Fixing.",
                        stage,
                        i + 1,
                        stage.index,
                    )
                    stage.index = i + 1
                else:
                    raise ValueError(
                        "Stage %s is at index %d of protocol, but has set index %d."
                        % (stage, i + 1, stage.index)
                    )

            if stage.label is not None:
                m = re.match(r"STAGE_(\d+)", stage.label)
                if m and (int(m[1]) != i + 1):
                    if fix:
                        log.warn(
                            "Stage %s has label %s, which implies index %d, but is at index %d of protocol. Fixing.",
                            stage,
                            stage.label,
                            int(m[1]),
                            i + 1,
                        )
                        stage.label = f"STAGE_{i+1}"
                    else:
                        raise ValueError(
                            "Stage %s has label %s, which implies index %d, but is at index %d of protocol."
                            % (stage, stage.label, int(m[1]), i + 1)
                        )


for c in cast(
    Sequence[Protocol],
    [Ramp, Exposure, HACFILT, HoldAndCollect, Hold, CustomStep, Stage, Protocol],
):
    for n in c._names:
        scpi_commands._scpi_command_classes[n.upper()] = cast(SCPICommandLike, c)
