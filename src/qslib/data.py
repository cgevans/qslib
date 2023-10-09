# SPDX-FileCopyrightText: 2021-2022 Constantine Evans <const@costi.eu>
#
# SPDX-License-Identifier: AGPL-3.0-only

from __future__ import annotations

import os
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from glob import glob
from os import PathLike
from typing import List, Literal, Optional, Sequence, Union, cast

import numpy as np
import numpy.typing as npt
import pandas as pd

from .plate_setup import _WELLNAMES_96, _WELLNAMES_384

_UPPERS = "ABCDEFGHIJKLMNOP"


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
        cls, path: str | PathLike[str], sds_dir: str | PathLike[str] | None
    ) -> FilterDataReading:
        p = ET.parse(path).find(".//PlateData")
        if p is None:
            raise ValueError(f"File {path} is not a valid filter data file.")
        return cls(p, sds_dir=sds_dir)

    def __init__(
        self,
        pde: ET.Element,
        timestamp: Optional[float] = None,
        sds_dir: Optional[str | PathLike[str]] = None,
        set_temperatures: Union[Literal["auto"], None, List[float]] = "auto",
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


def _filterdata_df_v2(jsdata: dict):
    dfd = {
        "filter_set": [],
        "stage": [],
        "cycle": [],
        "step": [],
        "point": [],
        "exposure": [],
    }
    dft = []
    for w in _WELLNAMES_384:
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
            for i, w in enumerate(_WELLNAMES_384):
                dfd[w].append(y["wellFluorescences"][i])
            dft.append(x["zoneTemperatures"])

    fdd = pd.DataFrame(dfd)
    fdd.set_index(["filter_set", "stage", "cycle", "step", "point"], inplace=True)
    fdd.columns = pd.MultiIndex.from_tuples(
        [("exposure", "exposure")] + [(x, "fl") for x in _WELLNAMES_384]
    )

    wrt = pd.DataFrame(
        np.array(dft).repeat(384 / 6, axis=1),
        columns=pd.MultiIndex.from_tuples([(x, "rt") for x in _WELLNAMES_384]),
        index=fdd.index,
    )

    return fdd.join(wrt).sort_index(axis=1)


def _parse_strlist(s):
    if s == "[]":
        return []
    return [d for d in s[1:-1].split(", ")]


def _parse_multicomponent_data(root: ET.Element):
    n_wells = int(root.find("WellCount").text)
    if n_wells == 96:
        wellnames = _WELLNAMES_96
    elif n_wells == 384:
        wellnames = _WELLNAMES_384
    else:
        raise ValueError(
            f"Unsupported number of wells in multicomponent data: {n_wells}"
        )

    cycle_count = int(root.find("CycleCount").text)

    welldyes = {
        int(dd.attrib["WellIndex"]): _parse_strlist(dd.find("DyeList").text)
        for dd in root.findall("DyeData")
    }

    wellcycdata = {
        int(d.attrib["WellIndex"]): {
            dye: np.fromstring(sd.text[1:-1], sep=",")
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
        df["cycle"] = df.index + 1
        df["well"] = wellnames[k]
        cycdataframes.append(df)
    mcd = pd.concat(cycdataframes).set_index(["well", "cycle"])

    temperatures = pd.Series(
        np.fromstring(root.find("SampleTemperatures").text, sep="\t"),
        index=pd.MultiIndex.from_product(
            [wellnames, range(1, cycle_count + 1)], names=["well", "cycle"]
        ),
        name="temperature",
    )

    cps = pd.DataFrame.from_records(
        [
            [
                int(y)
                for y in re.match(
                    r"\[Stg:(\d+) Cyc:(\d+) Stp:(\d+) Pt:(\d+)\]", x
                ).groups()
            ]
            for x in _parse_strlist(root.find("CollectionPoints").text)
        ],
        columns=[
            "collected_stage",
            "collected_cycle",
            "collected_step",
            "collected_point",
        ],
        index=pd.Index(range(1, cycle_count + 1), name="cycle"),
    )

    return mcd.join(temperatures).join(cps)


def _parse_analysis(contents: str):
    a = [x.splitlines() for x in re.split(r"\n(?=\d)", contents)]
    d = (
        pd.DataFrame.from_records(
            [x[0].split("\t") for x in a[1:]], columns=a[0][1].split("\t")
        )
        .replace("", np.nan)
        .astype(
            {
                "Well": int,
                "Sample Name": "string",
                "Detector": "string",
                "Task": "string",
                "Ct": float,
                "Avg Ct": float,
                "Ct SD": float,
                "Delta Ct": float,
                "Qty": float,
                "Avg Qty": float,
                "Qty SD": float,
            }
        )
        .rename(columns={"Well": "WellIndex"})
    )
    d["Well"] = np.array(_WELLNAMES_384)[d["WellIndex"]]  # fixme
    d = d.astype({"Well": "string"}).set_index(["Well"])
    return d
