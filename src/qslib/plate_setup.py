"""Code for handling plate setup."""
from __future__ import annotations
from uuid import uuid4

from dataclasses import dataclass, field
from typing import Optional, Sequence, Tuple, List, Dict, Union
from lxml import etree
import numpy as np
import pandas as pd
import tabulate
from io import BytesIO
from .qsconnection_async import QSConnectionAsync

_WELLNAMES = [x + str(y) for x in "ABCDEFGH" for y in range(1, 13)]

_WELLALPHREF = [(x, f"{y:02d}") for x in "ABCDEFGH" for y in range(1, 13)]


def _process_color_from_str_int(x: str) -> Tuple[int, int, int, int]:
    """From a string that represents a signed int32 (this choice make no sense),
    interpret it as a unsigned 32-bit integer, then unpack the bits to get R,G,B,A."""

    color_bytes: Tuple[int, int, int, int] = tuple(
        int(b) for b in int(x).to_bytes(4, "little", signed=True)
    )  # type: ignore

    return color_bytes


@dataclass
class Sample:
    name: str
    uuid: str = field(default_factory=(lambda: uuid4().hex))
    color: Tuple[int, int, int, int] = field(default=(255, 0, 0, 255))

    @classmethod
    def from_platesetup_sample(cls, se: etree.Element) -> Sample:  # type: ignore
        return cls(
            se.find("Name").text,
            se.xpath("(CustomProperty/Property[text()='SP_UUID'])[1]/../Value/text()")[
                0
            ],
            _process_color_from_str_int(se.find("Color").text),
        )


@dataclass
class PlateSetup:
    samples_by_name: Dict[str, Sample]
    sample_wells: Dict[str, List[str]]

    @classmethod
    def from_platesetup_xml(cls, platexml: etree.Element) -> PlateSetup:  # type: ignore
        assert platexml.find("PlateKind/Type").text == "TYPE_8X12"

        sample_fvs = platexml.xpath(
            "FeatureMap/Feature/Id[text()='sample']/../../FeatureValue"
        )

        samples_by_name: Dict[str, Sample] = dict()
        samples_by_uuid: Dict[str, Sample] = dict()

        sample_wells: Dict[str, List[str]] = dict()

        for fv in sample_fvs:
            idx = int(fv.find("Index").text)
            sample = Sample.from_platesetup_sample(fv.find("FeatureItem/Sample"))
            if sample.name in samples_by_name.keys():
                assert sample == samples_by_name[sample.name]
                assert sample == samples_by_uuid[sample.uuid]
                sample_wells[sample.name].append(_WELLNAMES[idx])
            else:
                assert sample.uuid not in samples_by_uuid.keys()
                samples_by_name[sample.name] = sample
                samples_by_uuid[sample.uuid] = sample
                sample_wells[sample.name] = [_WELLNAMES[idx]]

        return cls(samples_by_name, sample_wells)

    @property
    def well_sample(self):
        well_sample_name = pd.Series(np.full(8 * 12, None, object), index=_WELLNAMES)
        for s, ws in self.sample_wells.items():
            for w in ws:
                well_sample_name.loc[w] = s
        return well_sample_name

    def well_samples_as_array(self) -> np.ndarray:
        return self.well_sample.to_numpy().reshape((8, 12))

    def to_lineprotocol(self, timestamp, run_name=None):
        if run_name:
            rts = f',run_name="{run_name}"'
        else:
            rts = ""
        return [
            f'platesetup,row={r},col={c} sample="{s}"{rts} {timestamp}'
            for ((r, c), s) in zip(_WELLALPHREF, self.well_sample)
        ]

    def to_table(
        self,
        headers: Sequence[Union[str, int]] = list(range(1, 13)),
        tablefmt: str = "orgtbl",
        showindex: Sequence[str] = tuple("ABCDEFGH"),
        **kwargs,
    ) -> str:
        return tabulate.tabulate(
            self.well_samples_as_array(),
            tablefmt=tablefmt,
            headers=[str(x) for x in headers],
            showindex=showindex,
            **kwargs,
        )

    @classmethod
    async def from_machine(
        cls, c: QSConnectionAsync, runtitle: Optional[str] = None
    ) -> PlateSetup:
        s = await c.get_sds_file("plate_setup.xml", runtitle=runtitle)
        x = etree.parse(BytesIO(s), parser=None)
        return cls.from_platesetup_xml(x)

    def __repr__(self) -> str:
        return f"PlateSetup(samples {self.sample_wells.keys()}))"

    def __str__(self) -> str:
        s = ""
        if self.sample_wells:
            s += "Plate samples:\n"
            for sample, wells in self.sample_wells.items():
                s += f"- {sample}: {wells}\n"
        return s
