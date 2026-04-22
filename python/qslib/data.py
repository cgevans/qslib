# SPDX-FileCopyrightText: 2021 - 2023 Constantine Evans <qslib@mb.costi.net>
#
# SPDX-License-Identifier: EUPL-1.2

from __future__ import annotations

import re
import xml.etree.ElementTree as ET

from pathlib import Path
from typing import Any, TypeVar, cast, TYPE_CHECKING

import numpy as np
import pandas as pd

from ._qslib import FilterSet
from .plate_setup import _WELLNAMES_96, _WELLNAMES_384

if TYPE_CHECKING:
    pass


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


def _filterset_to_xml(fs: FilterSet) -> ET.Element:
    """Convert a FilterSet to an XML CollectionCondition element."""
    e = ET.Element("CollectionCondition")
    ET.SubElement(e, "FilterSet", Emission=f"m{fs.em}", Excitation=f"x{fs.ex}")
    ET.SubElement(e, "Frames").text = "0"
    return e


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
        from ._qslib import QuantFile
        timestamps = []
        for filter_set, stage, cycle, step, point in fdd.index:
            filename = (
                f"S{stage:02}_C{cycle:03}_T{step:02}_"
                f"P{point:04}_{FilterSet.fromstring(filter_set).upperform}"
                "_E1.quant"  # fixme: make consistent
            )
            with (quant_files_path / filename).open() as f:
                qstring = f.read()
            qf = QuantFile.parse(qstring)
            timestamp = qf.conditions.timestamp
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
        np.array(_find_text_or_raise(root, "SampleTemperatures").split(), dtype=np.float64),
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
