# SPDX-FileCopyrightText: 2021 - 2023 Constantine Evans <const@costi.net>
# SPDX-FileCopyrightText: 2021 - 2023 Constantine Evans <qslib@mb.costi.net>
#
# SPDX-License-Identifier: EUPL-1.2

"""Code for handling plate setup."""
from __future__ import annotations

import xml.etree.ElementTree as ET
from dataclasses import dataclass
from io import BytesIO
from typing import (
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Literal,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
    TYPE_CHECKING
)
from uuid import uuid1

import attrs
import numpy as np
import pandas as pd
import tabulate


if TYPE_CHECKING:
    from qslib.machine import Machine
    from kithairon import PickList, Labware
    from typing_extensions import Self


_ROWALPHAS = "ABCDEFGHIJKLMNOP"
_ROWALPHAS_96 = "ABCDEFGH"

_WELLNAMES_96 = [x + str(y) for x in _ROWALPHAS_96 for y in range(1, 13)]
_WELLNAMES_384 = [x + str(y) for x in _ROWALPHAS for y in range(1, 25)]

_WELLNAMESET_96 = set(_WELLNAMES_96)
_WELLNAMESET_384 = set(_WELLNAMES_384)

_WELLALPHREF_96 = [(x, f"{y}") for x in _ROWALPHAS_96 for y in range(1, 13)]
_WELLALPHREF_384 = [(x, f"{y}") for x in _ROWALPHAS for y in range(1, 25)]


def _process_color_from_str_int(x: str) -> Tuple[int, int, int, int]:
    """From a string that represents a signed int32 (this choice make no sense),
    interpret it as a unsigned 32-bit integer, then unpack the bits to get R,G,B,A."""

    color_bytes: Tuple[int, int, int, int] = tuple(
        int(b) for b in int(x).to_bytes(4, "little", signed=True)
    )  # type: ignore

    return color_bytes


def _color_to_str_int(x: Tuple[int, int, int, int]) -> str:
    return str(int.from_bytes(bytes(x), "little", signed=True))


def _str_or_list_to_list(v: str | Sequence[str]) -> list[str]:
    if isinstance(v, str):
        return [v]
    return list(v)


@attrs.define(init=False)
class Sample:
    name: str
    color: Tuple[int, int, int, int] = attrs.field(default=(255, 0, 0, 255))
    properties: dict[str, str] = attrs.field(factory=dict)
    description: str | None = None
    wells: list[str] = attrs.field(
        factory=list, converter=_str_or_list_to_list, on_setattr=attrs.setters.convert
    )

    def __init__(
        self,
        name: str,
        uuid: str | None = None,
        color: Tuple[int, int, int, int] = (255, 0, 0, 255),
        properties: dict[str, str] | None = None,
        description: str | None = None,
        wells: str | list[str] | None = None,
    ) -> None:
        if properties is None:
            properties = dict()
        if "SP_UUID" not in properties:
            if not uuid:
                uuid = uuid1().hex
            properties["SP_UUID"] = uuid
        if wells is None:
            wells = list()
        self.__attrs_init__(name, color, properties, description, wells=wells)  # type: ignore

    @property
    def uuid(self) -> str:
        return self.properties["SP_UUID"]

    @uuid.setter
    def uuid(self, val: str) -> None:
        self.properties["SP_UUID"] = val

    @classmethod
    def from_platesetup_sample(cls, se: ET.Element) -> Sample:
        name = se.findtext("Name") or "Unnamed"
        color = _process_color_from_str_int(se.findtext("Color") or "-1")

        keys = [
            x.text for x in se.findall("CustomProperty/Property") if x.text is not None
        ]
        values = [
            x.text for x in se.findall("CustomProperty/Value") if x.text is not None
        ]
        properties = {key: value for key, value in zip(keys, values)}

        return cls(
            name=name,
            color=color,
            properties=properties,
            description=se.findtext("Description"),
        )

    def to_xml(self) -> ET.Element:
        x = ET.Element("Sample")
        ET.SubElement(x, "Name").text = self.name
        ET.SubElement(x, "Color").text = _color_to_str_int(self.color)
        if self.description:
            ET.SubElement(x, "Description").text = self.description
        u = ET.SubElement(x, "CustomProperty")
        for key in self.properties.keys():
            ET.SubElement(u, "Property").text = key
        ET.SubElement(u, "Property").text = "SP_UUID"
        for value in self.properties.values():
            ET.SubElement(u, "Value").text = value
        ET.SubElement(u, "Value").text = self.uuid
        return x


@attrs.define()
class _SampleWellsView(Mapping[str, list[str]]):
    samples_by_name: Dict[str, Sample]

    def __getitem__(self, name: str) -> list[str]:
        return self.samples_by_name[name].wells

    def __setitem__(self, name: str, wells: str | list[str]) -> None:
        if isinstance(wells, str):
            wells = [wells]
        try:
            self.samples_by_name[name].wells = wells
        except KeyError:
            self.samples_by_name[name] = Sample(name=name, wells=wells)

    def __len__(self) -> int:
        return len(self.samples_by_name)

    def __iter__(self) -> Iterator[str]:
        return iter(self.samples_by_name)


@dataclass
class PlateSetup:
    samples_by_name: Dict[str, Sample]
    plate_type: Literal[96, 384] = 96

    @property
    def sample_wells(self):
        return _SampleWellsView(self.samples_by_name)

    @classmethod
    def from_platesetup_xml(cls, platexml: ET.Element) -> PlateSetup:  # type: ignore
        pt = platexml.find("PlateKind/Type")
        if pt is None:
            raise ValueError
        qs_platetype = pt.text
        if qs_platetype == "TYPE_8X12":
            plate_type = 96  # type: Literal[96, 384]
        elif qs_platetype == "TYPE_16X24":
            plate_type = 384
        else:
            raise ValueError

        sample_fvs = platexml.findall(
            "FeatureMap/Feature/Id[.='sample']/../../FeatureValue"
        )

        samples_by_name: Dict[str, Sample] = dict()
        samples_by_uuid: Dict[str, Sample] = dict()

        sample_wells: Dict[str, list[str]] = dict()

        wn = _WELLNAMES_96 if plate_type == 96 else _WELLNAMES_384

        for fv in sample_fvs:
            if x := fv.findtext("Index"):
                idx = int(x)
            else:
                raise ValueError
            if y := fv.find("FeatureItem/Sample"):
                sample = Sample.from_platesetup_sample(y)
            if sample.name in samples_by_name.keys():
                assert sample == samples_by_name[sample.name]
                assert sample == samples_by_uuid[sample.uuid]
                sample_wells[sample.name].append(wn[idx])
            else:
                assert sample.uuid not in samples_by_uuid.keys()
                samples_by_name[sample.name] = sample
                samples_by_uuid[sample.uuid] = sample
                sample_wells[sample.name] = [wn[idx]]

        return cls(sample_wells, samples_by_name, plate_type=plate_type)

    def __init__(
        self,
        sample_wells: Mapping[str, str | List[str]] | None = None,
        samples: Iterable[Sample] | Mapping[str, Sample] = tuple(),
        plate_type: Literal[96, 384] = 96,
    ) -> None:
        assert plate_type in (96, 384)
        self.plate_Type = plate_type

        if isinstance(samples, Mapping):
            self.samples_by_name = dict(samples)
        else:
            self.samples_by_name = {s.name: s for s in samples}

        if sample_wells is not None:
            for name, wells in sample_wells.items():
                if name in self.samples_by_name:
                    if isinstance(wells, str):
                        wells = [wells]
                    self.samples_by_name[name].wells = wells
                else:
                    self.samples_by_name[name] = Sample(name, wells=wells)

    @property
    def well_sample(self):
        well_sample_name = pd.Series(
            np.full(8 * 12, None, object),
            index=_WELLNAMES_96 if self.plate_Type == 96 else _WELLNAMES_384,
        )
        for s, ws in self.sample_wells.items():
            for w in ws:
                well_sample_name.loc[w] = s
        return well_sample_name

    def get_wells(self, samples_or_wells: str | Sequence[str]) -> list[str]:
        """
        Given a sample, well, or list of the two, returns the corresponding
        wells.  Note that this relies on samples not having well-like names.
        """
        wells = []

        if isinstance(samples_or_wells, str):
            samples_or_wells = [samples_or_wells]

        for sw in samples_or_wells:
            if sw.upper() in (
                _WELLNAMESET_96 if self.plate_Type == 96 else _WELLNAMESET_384
            ):
                wells.append(sw.upper())
            else:
                wells += self.sample_wells[sw]

        return wells

    def get_descriptive_string(self, name: str) -> str:
        if (w := name.upper()) in (
            _WELLNAMESET_96 if self.plate_Type == 96 else _WELLNAMESET_384
        ):
            return w
        sample = self.samples_by_name[name]
        return sample.description or sample.name

    def well_samples_as_array(self) -> np.ndarray[Any, Any]:
        return self.well_sample.to_numpy().reshape((8, 12))

    def to_lineprotocol(self, timestamp: int, run_name: str | None = None) -> list[str]:
        if run_name:
            rts = f',run_name="{run_name}"'
        else:
            rts = ""
        return [
            f'platesetup,row={r},col={c} sample="{s}"{rts} {timestamp}'
            for ((r, c), s) in zip(
                _WELLALPHREF_96 if self.plate_type == 96 else _WELLALPHREF_384,
                self.well_sample,
            )
        ]

    @classmethod
    def from_array(
        cls, array: Union[np.ndarray, pd.DataFrame], *, make_unique: bool = False
    ) -> PlateSetup:
        raise NotImplementedError

    @classmethod
    def from_tsv(cls, tsvstr: str) -> PlateSetup:
        raise NotImplementedError

    def to_table(
        self,
        headers: Sequence[Union[str, int]] = list(range(1, 13)),
        tablefmt: str = "orgtbl",
        protect_markdown: bool = False,
        showindex: Sequence[str] | None = None,
        **kwargs: Any,
    ) -> str:
        if showindex is None:
            showindex = _ROWALPHAS_96 if self.plate_Type == 96 else _ROWALPHAS
        ws = self.well_samples_as_array()
        if protect_markdown:
            ws = np.vectorize(lambda x: f"`{x}`" if x is not None else None)(ws)

        return tabulate.tabulate(
            ws,
            tablefmt=tablefmt,
            headers=[str(x) for x in headers],
            showindex=showindex,
            **kwargs,
        )

    def update_xml(self, root: ET.Element) -> None:
        samplemap = root.find("FeatureMap/Feature/Id[.='sample']/../..")
        e: Optional[ET.Element]

        e = ET.SubElement(root, "PlateKind")
        ET.SubElement(e, "Type").text = (
            "TYPE_8X12" if self.plate_Type == 96 else "TYPE_16X24"
        )
        ET.SubElement(e, "Name").text = (
            "96-Well Plate (8x12)"
            if self.plate_Type == 96
            else "384-Well Plate (16x24)"
        )
        ET.SubElement(e, "RowCount").text = "8" if self.plate_Type == 96 else "16"
        ET.SubElement(e, "ColumnCount").text = "12" if self.plate_Type == 96 else "24"

        if not samplemap:
            e = ET.SubElement(root, "FeatureMap")
            v = ET.SubElement(e, "Feature")
            ET.SubElement(v, "Id").text = "sample"
            ET.SubElement(v, "Name").text = "sample"
            samplemap = e
        ws = np.array(self.well_sample)
        for welli in range(0, self.plate_Type):
            if ws[welli]:
                e = samplemap.find(f"FeatureValue/Index[.='{welli}']/../FeatureItem")
                if not e:
                    e = ET.SubElement(samplemap, "FeatureValue")
                    ET.SubElement(e, "Index").text = str(welli)
                    e = ET.SubElement(e, "FeatureItem")
                if s := e.find("Sample"):
                    e.remove(s)
                e.append(self.samples_by_name[ws[welli]].to_xml())
            else:
                if e := samplemap.find(f"FeatureValue/Index[.='{welli}']/.."):
                    samplemap.remove(e)

    @classmethod
    def from_machine(
        cls, c: Machine, runtitle: str | None = None
    ) -> PlateSetup:
        s = c.get_sds_file("plate_setup.xml", runtitle=runtitle)
        x = ET.parse(BytesIO(s), parser=None)
        return cls.from_platesetup_xml(x.getroot())

    def __repr__(self) -> str:
        return f"PlateSetup(samples {self.sample_wells.keys()}))"

    def __str__(self) -> str:
        s = ""
        if self.sample_wells:
            s += "Plate setup:\n\n"
            for sample, wells in self.sample_wells.items():
                s += f" - {sample}: {wells}\n"
        return s

    def _repr_markdown_(self) -> str:
        if len(self.sample_wells) < 12:
            return str(self)
        else:
            return self.to_table(tablefmt="pipe", protect_markdown=True)

    @classmethod
    def from_picklist(cls, picklist: 'PickList' | str, plate_name: str | None = None, labware: 'Labware' | None = None) -> 'Self':
        """Create a PlateSetup from a Kithairon PickList.

        Parameters
        ----------
        picklist: PickList or str
            The picklist to read.  If a string, it is treated as a path to a CSV picklist.
        plate_name: str or None
            The destination plate that the PlateSetup is for.  If None, and there is only one destination plate, that one is used.  If there are multiple destination plates, the user must specify one.
        labware: Labware or None
            The Kithairon labware to use.  If None, the default labware is used.

        Raises
        ------
        ValueError
            If:
            - There is more than one destination plate and no plate_name is specified.
            - There are multiple sample names in a single well.
            - The destination plate is not a destination plate type in the Labware.
            - The destination plate shape is not 96 or 384 (taken from Labware)
        """
        try:
            import polars as pl
        except ImportError:
            raise ValueError("Polars is required to use this method")
        try:
            import kithairon as kt
        except ImportError:
            raise ValueError("Kithairon is required to use this method")

        if isinstance(picklist, str):
            picklist = kt.PickList.read_csv(picklist)

        destplates = picklist.data.select("Destination Plate Name", "Destination Plate Type").unique()

        if plate_name is None:
            if len(destplates) > 1:
                raise ValueError("Multiple destination plates found; please specify one: " + ", ".join(destplates["Destination Plate Name"].to_list()))
            plate_name = destplates["Destination Plate Name"][0]
            plate_type = destplates["Destination Plate Type"][0]
        else:
            if plate_name not in destplates["Destination Plate Name"].to_list():
                raise ValueError(f"Destination plate {plate_name} not a destination.  Destinations are: " + ", ".join(destplates["Destination Plate Name"].to_list()))
            plate_type = destplates.filter(pl.col("Destination Plate Name") == plate_name)["Destination Plate Type"][0]

        if labware is None:
            lw = kt.labware.get_default_labware()
        else:
            lw = labware

        try:
            plate = lw[plate_type]
        except KeyError:
            raise ValueError(f"Labware does not have a plate type {plate_type}")

        if plate.shape == (8, 12):
            plate_type = 96
        elif plate.shape == (16, 24):
            plate_type = 384
        else:
            raise ValueError(f"Plate shape {plate.shape} not supported")
        
        if plate.usage != "DEST":
            raise ValueError(f"Plate {plate_name} is not a destination plate type")
        
        plate = picklist.data.filter(pl.col("Destination Plate Name") == plate_name)
        
        u = pl.col("Destination Sample Name").unique()
        sample_names = plate.group_by("Destination Well").agg(
            u.alias("sample_names"), u.len().alias("n_sample_names")
        )

        errs = sample_names.filter(pl.col("n_sample_names") > 1).sort("Destination Well")
        if not errs.is_empty():
            errstring = ", ".join(f"{r['Destination Well']}: {r['sample_names']}" for r in errs.iter_rows(named=True))
            raise ValueError(f"Multiple sample names found in well(s): {errstring}")

        return cls(
            samples=[
                Sample(r["sample_names"][0], wells=r["Destination Well"])
                for r in sample_names.iter_rows(named=True)
            ],
            plate_type=plate_type
        )
