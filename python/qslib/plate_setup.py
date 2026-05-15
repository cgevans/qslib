# SPDX-FileCopyrightText: 2021 - 2023 Constantine Evans <const@costi.net>
# SPDX-FileCopyrightText: 2021 - 2023 Constantine Evans <qslib@mb.costi.net>
#
# SPDX-License-Identifier: EUPL-1.2

"""Code for handling plate setup."""

from __future__ import annotations

import html as html_mod
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Iterator, List, Literal, Mapping, Sequence, Tuple, TYPE_CHECKING

import numpy as np
import polars as pl
import tabulate

from ._qslib import PlateSetup as RustPlateSetup, Sample

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


_SORT_WELLS_ROW_OUTER = (
    pl.col("well")
    .str.extract_groups(r"^(?<row>\w)(?<col>\d{1,2})$")
    .struct.with_fields(pl.field("row"), pl.field("col").cast(pl.Int32))
)


def _color_to_str(x: Tuple[int, int, int, int]) -> str:
    return f"#{x[0]:02x}{x[1]:02x}{x[2]:02x}{x[3]:02x}"


_SAMPLE_SCHEMA = {
    "name": pl.String,
    "color": pl.String,
    "description": pl.String,
    "wells": pl.List(pl.String),
    "uuid": pl.String,
    "properties": pl.Struct,
}


class _SampleWellsView(Mapping[str, list[str]]):
    def __init__(self, samples_by_name: Dict[str, Sample]) -> None:
        self.samples_by_name = samples_by_name

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

    def to_polars(self) -> pl.DataFrame:
        return pl.from_records([s.to_record() for s in self.samples_by_name.values()], schema=_SAMPLE_SCHEMA)

    @property
    def sample_wells(self):
        return _SampleWellsView(self.samples_by_name)

    @classmethod
    def from_xml_string(cls, xml: str | bytes) -> PlateSetup:
        """Create a PlateSetup from an XML string (or bytes).

        Uses Rust XML parsing internally.
        """
        if isinstance(xml, bytes):
            xml = xml.decode("utf-8")
        rust_ps = RustPlateSetup.from_xml_string(xml)
        samples = rust_ps.get_samples_with_wells()
        plate_type: Literal[96, 384] = rust_ps.plate_type_int  # type: ignore

        return cls(sample_wells=None, samples=samples, plate_type=plate_type)

    def __init__(
        self,
        sample_wells: Mapping[str, str | List[str]] | None = None,
        samples: Iterable[Sample] | Mapping[str, Sample] = tuple(),
        plate_type: Literal[96, 384] = 96,
    ) -> None:
        assert plate_type in (96, 384)
        self.plate_type = plate_type

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
    def well_samples(self) -> pl.Series:
        return self.to_polars_by_well(full=False)["name"]

    def to_polars_by_well(self, full: bool = False) -> pl.DataFrame:
        well_sample_name = pl.DataFrame({"well": _WELLNAMES_96 if self.plate_type == 96 else _WELLNAMES_384})
        w = well_sample_name.join(
            self.to_polars().rename({"wells": "well"}).explode("well"),
            on="well",
            how="left",
            maintain_order="left",
        )
        if full:
            return w
        else:
            return w.select("well", "name")

    def get_wells(self, samples_or_wells: str | Sequence[str]) -> list[str]:
        """
        Given a sample, well, or list of the two, returns the corresponding
        wells.  Note that this relies on samples not having well-like names.
        """
        wells = []

        if isinstance(samples_or_wells, str):
            samples_or_wells = [samples_or_wells]

        for sw in samples_or_wells:
            if sw.upper() in (_WELLNAMESET_96 if self.plate_type == 96 else _WELLNAMESET_384):
                wells.append(sw.upper())
            else:
                wells += self.sample_wells[sw]

        return wells

    def get_descriptive_string(self, name: str) -> str:
        if (w := name.upper()) in (_WELLNAMESET_96 if self.plate_type == 96 else _WELLNAMESET_384):
            return w
        sample = self.samples_by_name[name]
        return sample.description or sample.name

    def sample_names_as_array(self) -> np.ndarray[Any, Any]:
        n = self.well_samples.to_numpy()
        if self.plate_type == 96:
            return n.reshape((8, 12))
        elif self.plate_type == 384:
            return n.reshape((16, 24))
        else:
            raise ValueError(f"Plate type {self.plate_type} not supported")

    def to_lineprotocol(self, timestamp: int, run_name: str | None = None) -> list[str]:
        if run_name:
            rts = f',run_name="{run_name}"'
        else:
            rts = ""
        return [
            f'platesetup,row={r},col={c} sample="{s}"{rts} {timestamp}'
            for ((r, c), s) in zip(
                _WELLALPHREF_96 if self.plate_type == 96 else _WELLALPHREF_384,
                self.well_samples,
            )
        ]

    @classmethod
    def from_array(
        cls,
        array: np.ndarray,  # TODO: tests for this
    ) -> PlateSetup:
        """Given an (8,12) or (16,24) array of sample names, create a PlateSetup.
        Interprets None, "None", and "null" as empty wells."""
        if array.shape != (8, 12) and array.shape != (16, 24):
            raise ValueError("Array must be (8,12) or (16,24)")
        if array.shape == (8, 12):
            plate_type = 96
        elif array.shape == (16, 24):
            plate_type = 384
        else:
            raise ValueError(f"Array shape {array.shape} must be (8,12) or (16,24)")

        wn = _WELLNAMES_96 if plate_type == 96 else _WELLNAMES_384

        sample_wells: dict[str, list[str]] = {}
        for idx, s in enumerate(array.flatten()):
            if s is not None and s != "None" and s != "null":
                sample_wells.setdefault(s, []).append(wn[idx])

        return cls(sample_wells, plate_type=plate_type)

    def to_table(
        self,
        format: Literal["markdown", "orgtbl", "html"] = "markdown",
        showindex: Sequence[str] | None = None,
        showcolors: bool | Literal["auto"] = "auto",
    ) -> str:
        if showindex is None:
            showindex = _ROWALPHAS_96 if self.plate_type == 96 else _ROWALPHAS
        ws = self.to_polars_by_well(full=True)

        def fmt_header(x: str) -> str:
            if not x:
                return ""
            if format == "markdown":
                return f"**{x}**"
            elif format == "orgtbl":
                return f"{x}"
            elif format == "html":
                return f"<b>{x}</b>"

        tablefmt = format

        if format == "markdown":
            ws = ws.with_columns(
                pl.when(pl.col("name").is_not_null())
                .then(pl.col("name").str.replace(".*", "`$0`"))
                .otherwise(pl.lit(""))
                .alias("name")
            )
            tablefmt = "pipe"

        if format == "html" and (showcolors == "auto" or showcolors):

            def color_style(row):
                color = row["color"]
                return f"color: {color};" if color else ""

            ws = ws.with_columns(
                pl.when(pl.col("name").is_not_null()).then(
                    pl.struct(["name", "color"])
                    .map_elements(
                        lambda r: f'<span style="{color_style(r)}">{html_mod.escape(str(r["name"]))}</span>',
                        return_dtype=pl.String,
                    )
                    .alias("name")
                )
            )
            tablefmt = "unsafehtml"

        headers = [""] + (list(range(1, 13)) if self.plate_type == 96 else list(range(1, 25)))

        ws = ws["name"].to_numpy().reshape((8, 12) if self.plate_type == 96 else (16, 24))

        ws_with_rownames = np.insert(ws, 0, [fmt_header(x) for x in showindex], axis=1)

        return tabulate.tabulate(
            ws_with_rownames,
            tablefmt=tablefmt,
            headers=[fmt_header(x) for x in headers],
            showindex=showindex,
        )

    def to_xml_string(self, existing_xml: str | None = None) -> str:
        """Serialize this PlateSetup to an XML string.

        If existing_xml is provided, the existing XML structure is preserved
        and only the sample data is updated.  Otherwise a new XML structure is created.

        Uses Rust XML handling internally.
        """
        if existing_xml is not None:
            rust_ps = RustPlateSetup.from_xml_string(existing_xml)
            rust_ps.set_samples(self.samples_by_name)
        else:
            rust_ps = RustPlateSetup.from_samples_and_wells(
                self.plate_type, {name: (sample, list(sample.wells)) for name, sample in self.samples_by_name.items()}
            )

        return rust_ps.to_xml_string()

    @classmethod
    def from_machine(cls, c: Machine, runtitle: str | None = None) -> PlateSetup:
        s = c.get_sds_file("plate_setup.xml", runtitle=runtitle)
        return cls.from_xml_string(s)

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
            return self.to_table(format="markdown")

    @classmethod
    def from_picklist(
        cls, picklist: "PickList" | str, plate_name: str | None = None, labware: "Labware" | None = None
    ) -> "Self":
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
                raise ValueError(
                    "Multiple destination plates found; please specify one: "
                    + ", ".join(destplates["Destination Plate Name"].to_list())
                )
            plate_name = destplates["Destination Plate Name"][0]
            plate_type = destplates["Destination Plate Type"][0]
        else:
            if plate_name not in destplates["Destination Plate Name"].to_list():
                raise ValueError(
                    f"Destination plate {plate_name} not a destination.  Destinations are: "
                    + ", ".join(destplates["Destination Plate Name"].to_list())
                )
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
        sample_names = plate.group_by("Destination Well").agg(u.alias("sample_names"), u.len().alias("n_sample_names"))

        errs = sample_names.filter(pl.col("n_sample_names") > 1).sort("Destination Well")
        if not errs.is_empty():
            errstring = ", ".join(f"{r['Destination Well']}: {r['sample_names']}" for r in errs.iter_rows(named=True))
            raise ValueError(f"Multiple sample names found in well(s): {errstring}")

        return cls(
            samples=[
                Sample(r["sample_names"][0], wells=r["Destination Well"]) for r in sample_names.iter_rows(named=True)
            ],
            plate_type=plate_type,
        )
