# SPDX-FileCopyrightText: 2021 - 2023 Constantine Evans <qslib@mb.costi.net>
#
# SPDX-License-Identifier: EUPL-1.2

from __future__ import annotations

import os
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from glob import glob
from os import PathLike
from pathlib import Path
from typing import Any, List, Literal, Optional, Sequence, TypeVar, Union, cast, TYPE_CHECKING

import numpy as np
import numpy.typing as npt
import pandas as pd

from .plate_setup import _WELLNAMES_96, _WELLNAMES_384

if TYPE_CHECKING:
    pass

_UPPERS = "ABCDEFGHIJKLMNOP"


def _find_text_or_raise(e: ET.ElementTree | ET.Element, path: str) -> str:
    "Find the text of the element at path and return it, or raise an error."
    x = e.find(path)
    if x is None:
        raise ValueError(f"{path} not found in {x}.")
    else:
        t = x.text
        if t is None:
            raise ValueError(f"{path} has no text.")
        else:
            return t


def _get_text_or_raise(e: ET.Element) -> str:
    "Get the text of the element or raise an error."
    t = e.text
    if t is None:
        raise ValueError(f"{e} has no text.")
    else:
        return t


@dataclass(frozen=True, order=True, eq=True)
class FilterSet:
    """Representation of a filter set, potentially including the "quant"
    parameter used by HACFILT in SCPI protocols."""

    ex: int
    em: int
    quant: bool = True

    @classmethod
    def fromstring(cls, string: str | FilterSet | Sequence) -> FilterSet:
        if isinstance(string, FilterSet):
            return string
        # fixme: do validation
        if not isinstance(string, str):
            string = ",".join(string)

        if string.startswith("x"):
            return cls(int(string[1]), int(string[4]))
        elif string.startswith("M"):
            return cls(int(string[4]), int(string[1]))
        elif string.startswith("m"):
            return cls(int(string[4]), int(string[1]), string.endswith(",quant"))
        else:
            raise ValueError

    @property
    def lowerform(self) -> str:
        return f"x{self.ex}-m{self.em}"

    @property
    def upperform(self) -> str:
        return f"M{self.em}_X{self.ex}"

    @property
    def hacform(self) -> str:
        return f"m{self.em},x{self.ex}" + (",quant" if self.quant else "")

    def to_xml(self) -> ET.Element:
        e = ET.Element("CollectionCondition")
        ET.SubElement(e, "FilterSet", Emission=f"m{self.em}", Excitation=f"x{self.ex}")
        ET.SubElement(e, "Frames").text = "0"
        return e

    def __str__(self) -> str:
        return self.lowerform + ("-noquant" if not self.quant else "")


class FilterDataReading:
    stage: int
    step: int
    cycle: int
    point: int
    timestamp: float | None
    exposure: int
    filter_set: FilterSet
    well_fluorescence: npt.NDArray[np.float64]
    temperatures: npt.NDArray[np.float64]
    set_temperatures: npt.NDArray[np.float64] | None
    plate_rows: int = 8
    plate_cols: int = 12

    def __repr__(self):
        return (
            f"FilterDataReading(stage={self.stage}, cycle={self.cycle}, "
            f'step={self.step}, point={self.point}, filter_set="{self.filter_set}", '
            f"timestamp={self.timestamp}, ...)"
        )

    @classmethod
    def from_file(
        cls,
        path: str | PathLike[str],
        sds_dir: str | PathLike[str] | None = None,
        set_temperatures: Union[Literal["auto"], None, List[float]] = "auto",
        timestamp_dict: Optional[dict[tuple[int, int, int, int, int], float]] = None,
    ) -> FilterDataReading:
        p = ET.parse(path).find(".//PlateData")
        if p is None:
            raise ValueError(f"File {path} is not a valid filter data file.")
        return cls(p, sds_dir=sds_dir, set_temperatures=set_temperatures, timestamp_dict=timestamp_dict)

    def __init__(
        self,
        pde: ET.Element,
        timestamp: Optional[float] = None,
        sds_dir: Optional[str | PathLike[str]] = None,
        set_temperatures: Union[Literal["auto"], None, List[float]] = "auto",
        timestamp_dict: Optional[dict[tuple[int, int, int, int, int], float]] = None,
    ):
        attribs = {
            cast(str, k.text).lower(): cast(str, v.text)
            for k, v in zip(
                pde.findall("Attribute/key"),
                pde.findall("Attribute/value"),
            )
        }

        self.temperatures = np.array(
            [float(x) for x in attribs["temperature"].split(",")], dtype=np.float64
        )

        self.stage = int(attribs["stage"])
        self.step = int(attribs["step"])
        self.cycle = int(attribs["cycle"])
        self.point = int(attribs["point"])
        self.exposure = int(attribs["exposure"])
        self.filter_set = FilterSet.fromstring(attribs["filter_set"])

        if set_temperatures == "auto":
            self.set_temperatures: Optional[np.ndarray] = cast(
                np.ndarray, self.temperatures
            ).round(2)
        elif set_temperatures is not None:
            assert len(set_temperatures) == len(self.temperatures)
            self.set_temperatures = np.array(set_temperatures)
        else:
            self.set_temperatures = None

        self.plate_rows = int(cast(str, cast(ET.Element, pde.find("Rows")).text))
        self.plate_cols = int(cast(str, cast(ET.Element, pde.find("Cols")).text))

        assert self.plate_cols % len(self.temperatures) == 0

        # todo: handle other cases
        assert self.plate_rows * self.plate_cols in (96, 384)
        assert len(self.temperatures) in (1, 2, 3, 6)

        wfs = cast(ET.Element, pde.find("WellData")).text
        if wfs is None:
            raise ValueError("WellData is empty.")

        self.well_fluorescence = cast(
            np.ndarray,
            np.fromstring(wfs, sep=" "),
        )

        self.timestamp = timestamp

        if timestamp is None and sds_dir is not None:
            qs = glob(
                os.path.join(
                    sds_dir, "quant", self.filename_reading_string + "_E*.quant"
                )
            )
            qs.sort()
            self.set_timestamp_by_quantdata(open(qs[-1]).read())

        if timestamp_dict is not None:
            self.timestamp = timestamp_dict[
                (self.stage, self.cycle, self.step, self.point, self.filter_set.em, self.filter_set.ex)
            ]

    def set_timestamp_by_quantdata(self, qstring: str) -> float:
        qss = qstring.split("\n\n")[3].split("\n")
        assert len(qss) == 3
        assert qss[0] == "[conditions]"
        qd = {k: v for k, v in zip(qss[1].split("\t"), qss[2].split("\t"))}
        self.timestamp = float(qd["Time"])
        return self.timestamp

    @property
    def filename_reading_string(self) -> str:
        return (
            f"S{self.stage:02}_C{self.cycle:03}_T{self.step:02}_"
            f"P{self.point:04}_{self.filter_set.upperform}"
        )

    @property
    def well_temperatures(self) -> np.ndarray:
        zones = self.plate_cols / len(self.temperatures)
        assert zones == int(zones)
        return cast(
            np.ndarray,
            self.temperatures[
                np.tile(
                    np.arange(0, len(self.temperatures)).repeat(int(zones)),
                    self.plate_rows,
                )
            ],
        )

    @property
    def well_set_temperatures(self) -> np.ndarray:
        if self.set_temperatures is None:
            raise ValueError("No set temperatures available.")
        zones = self.plate_cols / len(self.set_temperatures)
        assert zones == int(zones)
        zones = int(zones)
        if self.set_temperatures is not None:
            return cast(
                np.ndarray,
                self.set_temperatures[
                    np.tile(
                        np.arange(0, len(self.set_temperatures)).repeat(zones),
                        self.plate_rows,
                    )
                ],
            )
        else:
            return np.full(self.plate_cols * self.plate_rows, None)

    @property
    def plate_temperatures(self) -> np.ndarray:
        return self.well_temperatures.reshape(self.plate_rows, self.plate_cols)

    @property
    def plate_set_temperatures(self) -> np.ndarray:
        return self.well_set_temperatures.reshape(self.plate_rows, self.plate_cols)

    @property
    def plate_fluorescence(self) -> np.ndarray:
        return self.well_fluorescence.reshape(self.plate_rows, self.plate_cols)

    def to_lineprotocol(
        self, run_name: Optional[str] = None, sample_array=None
    ) -> List[str]:
        lines = []
        gs = f"filterdata,filter_set={self.filter_set}"
        assert self.timestamp
        es = " {}".format(int(self.timestamp * 1e9))

        wr = [
            (_UPPERS[rn], rn, c)
            for rn in range(0, self.plate_rows)
            for c in range(1, self.plate_cols + 1)
        ]

        for (r, rn, c), f, tr, ts in zip(
            wr,
            self.well_fluorescence,
            self.well_temperatures,
            self.well_set_temperatures,
        ):
            s = (
                f",row={r},col={c:02} fluorescence={f},temperature_read={tr}"
                f",stage={self.stage:02}i,cycle={self.cycle:03}i"
                f",step={self.step:02}i,point={self.point:04}i"
            )
            if sample_array is not None:
                s += f',sample="{sample_array[rn,c-1]}"'
            if run_name is not None:
                s += f',run_name="{run_name}"'
            if self.set_temperatures is not None:
                s += f",temperature_set={ts}"
            lines.append(gs + s + es)

        return lines


def df_from_readings(
    readings: List[FilterDataReading], start_time: float | None = None
) -> pd.DataFrame:
    f = [
        np.concatenate(
            (
                np.array([r.timestamp]),
                r.well_fluorescence,
                r.well_temperatures,
                r.well_set_temperatures,
                np.array([r.exposure]),
            )
        )
        for r in readings
    ]

    indices = pd.MultiIndex.from_tuples(
        [(r.filter_set.lowerform, r.stage, r.cycle, r.step, r.point) for r in readings],
        names=["filter_set", "stage", "cycle", "step", "point"],
    )

    # fr = readings[0]

    # wr = [
    #     f"{r}{c:02}"
    #     for r in _UPPERS[0 : fr.plate_rows]
    #     for c in range(1, fr.plate_cols + 1)
    # ]

    a = pd.DataFrame(
        f,
        index=indices,
        columns=pd.MultiIndex.from_tuples(
            [(cast(str, "time"), cast(str, "timestamp"))]
            + [
                (f"{r}{c}", v)
                for v in ["fl", "rt", "st"]
                for r in _UPPERS[0 : readings[0].plate_rows]
                for c in range(1, readings[0].plate_cols + 1)
            ]
            + [("exposure", "exposure")]
        ),
    )

    if start_time is not None:
        a[("time", "seconds")] = a[("time", "timestamp")] - start_time
        a[("time", "hours")] = a[("time", "seconds")] / 3600.0

    a.sort_index(inplace=True)

    return a.reindex(
        labels=pd.MultiIndex.from_tuples(
            [(cast(str, "time"), v) for v in ["seconds", "hours", "timestamp"]]
            + [
                (f"{r}{c}", v)
                for r in _UPPERS[0 : readings[0].plate_rows]
                for c in range(1, readings[0].plate_cols + 1)
                for v in ["fl", "rt", "st"]
            ]
            + [("exposure", "exposure")]
        ),
        axis="columns",  # type: ignore
    )

def _filterdata_df_v2(
    jsdata: dict,
    plate_type: int,
    quant_files_path: Path | None = None,
    start_time: float | None = None,
):
    dfd: dict[str, list[Any]] = {
        "filter_set": [],
        "stage": [],
        "cycle": [],
        "step": [],
        "point": [],
        "exposure": [],
    }
    dft = []

    wellnames = _WELLNAMES_96 if plate_type == 96 else _WELLNAMES_384

    for w in wellnames:
        dfd[w] = []

    for x in jsdata:
        cp = x["collectionPoint"]
        for y in x["filterData"]:
            dfd["filter_set"].append(y["filterSet"].lower().replace("_", "-"))
            dfd["stage"].append(cp["stage"])
            dfd["cycle"].append(cp["cycle"])
            dfd["step"].append(cp["step"])
            dfd["point"].append(cp["point"])
            dfd["exposure"].append(y["exposure"])
            for w, v in zip(wellnames, y["wellFluorescences"], strict=True):
                dfd[w].append(v)
            dft.append(x["zoneTemperatures"])

    fdd = pd.DataFrame(dfd)
    fdd.set_index(["filter_set", "stage", "cycle", "step", "point"], inplace=True)
    fdd.columns = pd.MultiIndex.from_tuples(
        [("exposure", "exposure")] + [(x, "fl") for x in wellnames]
    )

    wrt = pd.DataFrame(
        np.array(dft).repeat(int(plate_type / len(dft[0])), axis=1),
        columns=pd.MultiIndex.from_tuples([(x, "rt") for x in wellnames]),
        index=fdd.index,
    )

    if quant_files_path is not None:
        timestamps = []
        for filter_set, stage, cycle, step, point in fdd.index:
            filename = (
                f"S{stage:02}_C{cycle:03}_T{step:02}_"
                f"P{point:04}_{FilterSet.fromstring(filter_set).upperform}"
                "_E1.quant"  # fixme: make consistent
            )
            qstring = (quant_files_path / filename).open().read()
            qss = qstring.split("\n\n")[3].split("\n")
            assert len(qss) == 3
            assert qss[0] == "[conditions]"
            qd = {k: v for k, v in zip(qss[1].split("\t"), qss[2].split("\t"))}
            timestamp = float(qd["Time"])
            timestamps.append(timestamp)
        fdd["time", "timestamp"] = timestamps
        if start_time is not None:
            fdd[("time", "seconds")] = fdd[("time", "timestamp")] - start_time
            fdd[("time", "hours")] = fdd[("time", "seconds")] / 3600.0

    return fdd.join(wrt).sort_index(axis=1)


def _parse_strlist(s):
    if s == "[]":
        return []
    return [d for d in s[1:-1].split(", ")]


T = TypeVar("T")


def _parse_multicomponent_data_v1(root: ET.ElementTree):
    n_wells = int(_find_text_or_raise(root, "WellCount"))
    if n_wells == 96:
        wellnames = _WELLNAMES_96
    elif n_wells == 384:
        wellnames = _WELLNAMES_384
    else:
        raise ValueError(
            f"Unsupported number of wells in multicomponent data: {n_wells}"
        )

    cycle_count = int(_find_text_or_raise(root, "CycleCount"))

    welldyes = {
        int(dd.attrib["WellIndex"]): _parse_strlist(
            _find_text_or_raise(dd, "DyeList")
        )  # fixme
        for dd in root.findall("DyeData")
    }

    wellcycdata = {
        int(d.attrib["WellIndex"]): {
            dye: np.fromstring(_get_text_or_raise(sd)[1:-1], sep=",")
            for dye, sd in zip(
                welldyes[int(d.attrib["WellIndex"])],
                d.findall("CycleData"),
            )
        }
        for d in root.findall("SignalData")
    }

    cycdataframes = []
    for k, v in wellcycdata.items():
        df = pd.DataFrame(v)
        df["collection_cycle"] = df.index + 1
        df["well"] = wellnames[k]
        cycdataframes.append(df)
    mcd = pd.concat(cycdataframes).set_index(["well", "collection_cycle"])

    temperatures = pd.Series(
        np.fromstring(_find_text_or_raise(root, "SampleTemperatures"), sep="\t"),
        index=pd.MultiIndex.from_product(
            [wellnames, range(1, cycle_count + 1)], names=["well", "collection_cycle"]
        ),
        name="temperature",
    )

    cps = pd.DataFrame.from_records(
        [
            [
                int(y)
                for y in cast(
                    re.Match[str],
                    re.match(r"\[Stg:(\d+) Cyc:(\d+) Stp:(\d+) Pt:(\d+)\]", x),
                ).groups()
            ]
            for x in _parse_strlist(_find_text_or_raise(root, "CollectionPoints"))
        ],
        columns=[
            "stage",
            "cycle",
            "step",
            "point",
        ],
        index=pd.Index(range(1, cycle_count + 1), name="collection_cycle"),
    )

    return mcd.join(temperatures).join(cps)


def _parse_multicomponent_data_v2(jd: dict, plate_type: int):
    if plate_type == 96:
        wellnames = _WELLNAMES_96
    elif plate_type == 384:
        wellnames = _WELLNAMES_384
    else:
        raise ValueError(
            f"Unsupported number of wells in multicomponent data: {plate_type}"
        )

    cycle_count = len(jd["collectionPoints"])

    wellcycdata = {
        int(d["wellIndex"]): {
            dd["dyeName"]: np.array(dd["fluorescences"]) for dd in d["dyeData"]
        }
        | {"temperature": d["temperatures"]}
        for d in jd["wellData"]
    }

    # FIXME: bubble data

    cycdataframes = []
    for k, v in wellcycdata.items():
        df = pd.DataFrame(v)
        df["collection_cycle"] = df.index + 1
        df["well"] = wellnames[k]
        cycdataframes.append(df)
    mcd = pd.concat(cycdataframes).set_index(["well", "collection_cycle"])

    cps = pd.DataFrame.from_records(
        jd["collectionPoints"],
        index=pd.Index(range(1, cycle_count + 1), name="collection_cycle"),
    )

    return mcd.join(cps)


def _parse_analysis_result(contents: str, plate_type: int):
    wellnames = _WELLNAMES_96 if plate_type == 96 else _WELLNAMES_384

    a = [x.splitlines() for x in re.split(r"\n(?=\d)", contents)]

    colnames = a[0][1].split("\t")
    ard_d: dict[str, list[Any]] = {y: [] for y in colnames}

    # ard_d |= {
    #     "Std Curve Results": [],
    #     "Std Curve Results X Values": [],
    #     "Std Curve Results Y Values": [],
    #     "Rn values": [],
    #     "Delta Rn values": [],
    # }
    # FIXME: will fail if there are unexpected columns

    for x in a[1:]:
        for k, v in zip(colnames, x[0].split("\t")):
            if v == "":
                v = np.nan
            # FIXME:
            else:
                try:
                    v = int(v)
                except ValueError:
                    try:
                        v = float(v)
                    except ValueError:
                        pass
            if k not in ard_d:
                ard_d[k] = []
            ard_d[k].append(v)
        for y in x[1:]:
            z = y.split("\t")
            k = z[0]
            v = z[1:]
            if k not in ard_d:
                ard_d[k] = []
            ard_d[k].append(v)

    d = pd.DataFrame(ard_d)
    d.rename({"Well": "WellIndex"}, axis=1, inplace=True)
    d["Well"] = np.array(wellnames)[d["WellIndex"]]
    d.set_index(["Well"], inplace=True)

    return (d, None)  # fixme: parse ampl data
