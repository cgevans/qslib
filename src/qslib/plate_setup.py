# SPDX-FileCopyrightText: 2021-2022 Constantine Evans <const@costi.eu>
#
# SPDX-License-Identifier: AGPL-3.0-only

"""Code for handling plate setup."""
from __future__ import annotations

import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from io import BytesIO
from typing import (
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
)
from uuid import uuid1

import attrs
import numpy as np
import pandas as pd
import tabulate

from .qsconnection_async import QSConnectionAsync

_WELLNAMES = [x + str(y) for x in "ABCDEFGH" for y in range(1, 13)]

_WELLNAMESET = set(_WELLNAMES)

_WELLALPHREF = [(x, f"{y}") for x in "ABCDEFGH" for y in range(1, 13)]


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
        if not "SP_UUID" in properties:
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

    @property
    def sample_wells(self):
        return _SampleWellsView(self.samples_by_name)

    @classmethod
    def from_platesetup_xml(cls, platexml: ET.Element) -> PlateSetup:  # type: ignore
        # assert platexml.find("PlateKind/Type").text == "TYPE_8X12"

        sample_fvs = platexml.findall(
            "FeatureMap/Feature/Id[.='sample']/../../FeatureValue"
        )

        samples_by_name: Dict[str, Sample] = dict()
        samples_by_uuid: Dict[str, Sample] = dict()

        sample_wells: Dict[str, list[str]] = dict()

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
                sample_wells[sample.name].append(_WELLNAMES[idx])
            else:
                assert sample.uuid not in samples_by_uuid.keys()
                samples_by_name[sample.name] = sample
                samples_by_uuid[sample.uuid] = sample
                sample_wells[sample.name] = [_WELLNAMES[idx]]

        return cls(sample_wells, samples_by_name)

    def __init__(
        self,
        sample_wells: Mapping[str, str | List[str]] | None = None,
        samples: Iterable[Sample] | Mapping[str, Sample] = tuple(),
    ) -> None:
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
        well_sample_name = pd.Series(np.full(8 * 12, None, object), index=_WELLNAMES)
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
            if sw.upper() in _WELLNAMESET:
                wells.append(sw.upper())
            else:
                wells += self.sample_wells[sw]

        return wells

    def get_descriptive_string(self, name: str) -> str:
        if (w := name.upper()) in _WELLNAMESET:
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
            for ((r, c), s) in zip(_WELLALPHREF, self.well_sample)
        ]

    @classmethod
    def from_array(
        cls, array: Union[np.ndarray, pd.DataFrame], *, make_unique: bool = False
    ) -> PlateSetup:
        raise NotImplemented

    @classmethod
    def from_tsv(cls, tsvstr: str) -> PlateSetup:
        raise NotImplemented

    def to_table(
        self,
        headers: Sequence[Union[str, int]] = list(range(1, 13)),
        tablefmt: str = "orgtbl",
        showindex: Sequence[str] = tuple("ABCDEFGH"),
        **kwargs: Any,
    ) -> str:
        return tabulate.tabulate(
            self.well_samples_as_array(),
            tablefmt=tablefmt,
            headers=[str(x) for x in headers],
            showindex=showindex,
            **kwargs,
        )

    def update_xml(self, root: ET.Element) -> None:
        samplemap = root.find("FeatureMap/Feature/Id[.='sample']/../..")
        e: Optional[ET.Element]
        if not samplemap:
            e = ET.SubElement(root, "FeatureMap")
            v = ET.SubElement(e, "Feature")
            ET.SubElement(v, "Id").text = "sample"
            ET.SubElement(v, "Name").text = "sample"
            samplemap = e
        ws = np.array(self.well_sample)
        for welli in range(0, 96):
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
    async def from_machine(
        cls, c: QSConnectionAsync, runtitle: Optional[str] = None
    ) -> PlateSetup:
        s = await c.get_sds_file("plate_setup.xml", runtitle=runtitle)
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
            return self.to_table(tablefmt="pipe")
