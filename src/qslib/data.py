from __future__ import annotations
from typing import Optional, Union, List, cast
from typing_extensions import Literal
from dataclasses import dataclass
from lxml import etree
import numpy as np
import pandas as pd

_UPPERS = "ABCDEFGHIJKLMNOP"


@dataclass
class FilterSet:
    ex: int
    em: int

    @classmethod
    def fromstring(cls, string: str) -> FilterSet:
        # fixme: do validation
        if string.startswith("x"):
            return cls(int(string[1]), int(string[4]))
        elif string.startswith("M"):
            return cls(int(string[4]), int(string[1]))
        else:
            raise ValueError

    @property
    def lowerform(self) -> str:
        return f"x{self.ex}-m{self.em}"

    @property
    def upperform(self) -> str:
        return f"M{self.em}_X{self.ex}"

    def __str__(self) -> str:
        return self.lowerform


class FilterDataReading:
    def __init__(
        self,
        filterxmlstring: Union[str, bytes],
        time: Optional[float] = None,
        set_temperatures: Union[Literal["auto"], None, List[float]] = "auto",
    ):
        if isinstance(filterxmlstring, str):
            filterxmlstring = filterxmlstring.encode()

        fxml = etree.fromstring(filterxmlstring)

        self.attribs = {
            k.lower(): v
            for k, v in zip(
                fxml.xpath("//Attribute/key/text()"),
                fxml.xpath("//Attribute/value/text()"),
            )
        }

        self.attribs["temperature"] = np.array(
            [float(x) for x in self.attribs["temperature"].split(",")]
        )
        if set_temperatures == "auto":
            self.set_temperatures = self.attribs["temperature"].round(2)
        elif set_temperatures is not None:
            assert len(set_temperatures) == len(self.attribs["temperature"])
            self.set_temperatures = set_temperatures
        else:
            self.set_temperatures = None

        self.plate_rows = int(fxml.xpath("//PlateData/Rows/text()")[0])
        self.plate_cols = int(fxml.xpath("//PlateData/Cols/text()")[0])

        assert self.plate_cols % len(self.attribs["temperature"]) == 0

        # todo: handle other cases
        assert self.plate_rows * self.plate_cols == 96
        assert len(self.attribs["temperature"]) == 6

        self.well_fluorescence = np.fromstring(
            fxml.xpath("//WellData/text()")[0], sep=" "
        )

        self.time = time

    def set_time_by_quantdata(self, qstring: str) -> float:
        qss = qstring.split("\n\n")[3].split("\n")
        assert len(qss) == 3
        assert qss[0] == "[conditions]"
        qd = {k: v for k, v in zip(qss[1].split("\t"), qss[2].split("\t"))}
        self.time = float(qd["Time"])
        return self.time

    @property
    def filter_set(self) -> FilterSet:
        return FilterSet.fromstring(self.attribs["filter_set"])

    @property
    def temperature(self) -> np.ndarray:
        return cast(np.ndarray, self.attribs["temperature"])

    @property
    def filename_reading_string(self) -> str:
        return (
            f"S{self.stage:02}_C{self.cycle:03}_T{self.step:02}_"
            f"P{self.point:04}_{self.filter_set.upperform}"
        )

    @property
    def well_temperatures(self) -> np.ndarray:
        return self.temperature[
            np.tile(
                np.arange(0, len(self.attribs["temperature"])).repeat(
                    self.plate_cols / len(self.temperature)
                ),
                self.plate_rows,
            )
        ]

    @property
    def well_set_temperatures(self) -> np.ndarray:
        if self.set_temperatures is not None:
            return self.set_temperatures[
                np.tile(
                    np.arange(0, len(self.set_temperatures)).repeat(
                        self.plate_cols / len(self.set_temperatures)
                    ),
                    self.plate_rows,
                )
            ]
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

    @property
    def stage(self) -> int:
        return int(self.attribs["stage"])

    @property
    def cycle(self) -> int:
        return int(self.attribs["cycle"])

    @property
    def step(self) -> int:
        return int(self.attribs["step"])

    @property
    def point(self) -> int:
        return int(self.attribs["point"])

    @property
    def exposure(self) -> float:
        return float(self.attribs["exposure"])

    def to_lineprotocol(self) -> List[str]:
        lines = []
        gs = (
            f"filterdata,filter_set={self.filter_set},stage={self.stage:02},"
            f"cycle={self.cycle:03},step={self.step:02},point={self.point:04},"
        )
        assert self.time
        es = " {}".format(int(self.time * 1e9))

        wr = [
            (r, c)
            for r in _UPPERS[0 : self.plate_rows]
            for c in range(1, self.plate_cols + 1)
        ]

        for (r, c), f, tr, ts in zip(
            wr,
            self.well_fluorescence,
            self.well_temperatures,
            self.well_set_temperatures,
        ):
            s = f"row={r},col={c:02} fluorescence={f},temperature_read={tr}"
            if self.set_temperatures is not None:
                s += f",temperature_set={ts}"
            lines.append(gs + s + es)

        return lines


class FilterDataCollection(pd.DataFrame):  # type: ignore
    @property
    def _constructor(self) -> type:
        return FilterDataCollection

    @property
    def _constructor_sliced(self) -> pd.Series:
        return pd.Series

    @classmethod
    def from_readings(cls, readings: List[FilterDataReading]) -> pd.DataFrame:
        f = [
            np.concatenate(
                (
                    np.array([r.time]),
                    r.well_fluorescence,
                    r.well_temperatures,
                    r.well_set_temperatures,
                    np.array([r.exposure]),
                )
            )
            for r in readings
        ]

        indices = pd.MultiIndex.from_tuples(
            [
                (r.filter_set.lowerform, r.stage, r.cycle, r.step, r.point)
                for r in readings
            ],
            names=["filter_set", "stage", "cycle", "step", "point"],
        )

        fr = readings[0]

        wr = [
            f"{r}{c:02}"
            for r in _UPPERS[0 : fr.plate_rows]
            for c in range(1, fr.plate_cols + 1)
        ]

        a = cls(
            f,
            index=indices,
            columns=["time"]
            + ["f_" + r for r in wr]
            + ["tr_" + r for r in wr]
            + ["ts_" + r for r in wr]
            + ["exposure"],
        )

        a.sort_index(inplace=True)

        return a
