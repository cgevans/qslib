# SPDX-FileCopyrightText: 2021-2022 Constantine Evans <const@costi.eu>
# SPDX-License-Identifier: AGPL-3.0-only

import pathlib
from os import PathLike

import numpy as np
import pytest

import qslib.protocol as tc
from qslib import Experiment
from qslib.protocol import UR, Exposure, FilterSet, Protocol, Stage, Step
from qslib.scpi_commands import SCPICommand  # noqa

PROTSTRING = """PROTOCOL -volume=30 -runmode=standard testproto <multiline.protocol>
\tSTAGE 1 STAGE_1 <multiline.stage>
\t\tSTEP 1 <multiline.step>
\t\t\tRAMP -incrementcycle=2 -incrementstep=2 80 80 80 80 80 80
\t\t\tHOLD -incrementcycle=2 -incrementstep=2 300
\t\t</multiline.step>
\t</multiline.stage>
\tSTAGE -repeat=27 2 STAGE_2 <multiline.stage>
\t\tSTEP 1 <multiline.step>
\t\t\tRAMP -increment=-1 -incrementcycle=2 -incrementstep=2 80 80 80 80 80 80
\t\t\tHOLD -incrementcycle=2 -incrementstep=2 147600
\t\t</multiline.step>
\t</multiline.stage>
\tSTAGE -repeat=5 3 STAGE_3 <multiline.stage>
\t\tSTEP 1 <multiline.step>
\t\t\tRAMP -incrementcycle=2 -incrementstep=2 53 53 53 53 53 53
\t\t\tHACFILT m4,x1,quant m5,x3,quant # qslib:default_filters
\t\t\tHOLDANDCOLLECT -incrementcycle=2 -incrementstep=2 -tiff=False -quant=True -pcr=False 120
\t\t</multiline.step>
\t</multiline.stage>
\tSTAGE -repeat=20 4 STAGE_4 <multiline.stage>
\t\tSTEP 1 <multiline.step>
\t\t\tRAMP -incrementcycle=2 -incrementstep=2 51.2 50.84 50.480000000000004 50.12 49.76 49.4
\t\t\tHACFILT m4,x1,quant m5,x3,quant # qslib:default_filters
\t\t\tHOLDANDCOLLECT -incrementcycle=2 -incrementstep=2 -tiff=False -quant=True -pcr=False 64800000
\t\t</multiline.step>
\t</multiline.stage>
\tSTAGE -repeat=20 5 STAGE_5 <multiline.stage>
\t\tSTEP 1 <multiline.step>
\t\t\tRAMP -incrementcycle=2 -incrementstep=2 51.2 50.84 50.480000000000004 50.12 49.76 49.4
\t\t\tHACFILT m4,x1,quant m5,x3,quant
\t\t\tHOLDANDCOLLECT -incrementcycle=2 -incrementstep=2 -tiff=False -quant=True -pcr=False 86400
\t\t</multiline.step>
\t</multiline.stage>
\tSTAGE -repeat=100 6 STAGE_6 <multiline.stage>
\t\tSTEP 1 <multiline.step>
\t\t\tRAMP -incrementcycle=2 -incrementstep=2 51.2 50.84 50.480000000000004 50.12 49.76 49.4
\t\t\tHACFILT m4,x1,quant m5,x3,quant
\t\t\tHOLDANDCOLLECT -incrementcycle=2 -incrementstep=2 -tiff=False -quant=True -pcr=False 1200
\t\t</multiline.step>
\t</multiline.stage>
</multiline.protocol>
"""  # noqa


# def test_temperature_lists() -> None:
#     a = 52.3
#     a_l = [52.3] * 6
#     a_a = np.array(a_l)

#     d_a = np.linspace(40, 46, 6)
#     d_l = list(d_a)

#     assert (
#         tc._temperature_str(a)
#         == tc._temperature_str(a_l)
#         == tc._temperature_str(a_a)
#         == "52.30°C"
#     )
#     assert tc._temperature_str(d_a) == tc._temperature_str(d_l)


def test_proto() -> None:
    temperatures = list(np.linspace(51.2, 49.4, num=6))
    prot = Protocol(
        name="testproto",
        stages=[
            Stage(Step("5 minutes", 80)),
            Stage(Step(41 * 60 * 60, 80, temp_increment=-1), repeat=27),
            Stage(
                Step(
                    2 * 60,
                    53,
                    collect=True,
                ),
                repeat=5,
            ),
            Stage(Step(5 * 60 * 60 * 60 * 60, temperatures, collect=True), repeat=20),
            Stage(
                Step(
                    "24 hours", temperatures, collect=True, filters=["x1-m4", "x3-m5"]
                ),
                repeat=20,
            ),
            Stage(
                Step(20 * 60, temperatures, collect=True, filters=["x1-m4", "x3-m5"]),
                repeat=100,
            ),
        ],
        filters=["x1-m4", "x3-m5"],
        volume=30,
    )

    prot_explicitfilter = Protocol(
        name="testproto",
        stages=[
            Stage(Step(5 * 60, 80)),
            Stage(Step(41 * 60 * 60, 80, temp_increment=-1), repeat=27),
            Stage(
                Step(
                    UR.Quantity(2, "minutes"),
                    "53 degC",
                    filters=["x1-m4", "x3-m5"],
                    collect=True,
                ),
                repeat=5,
            ),
            Stage(
                Step(
                    5 * 60 * 60 * 60 * 60,
                    UR.Quantity(temperatures, "degC"),
                    collect=True,
                    filters=["x1-m4", "x3-m5"],
                ),
                repeat=20,
            ),
            Stage(
                Step(
                    "24 hours",
                    [f"{x} degC" for x in temperatures],
                    collect=True,
                    filters=["x1-m4", FilterSet.fromstring("x3-m5")],
                ),
                repeat=20,
            ),
            Stage(
                Step(
                    20 * 60,
                    [UR.Quantity(x, "degC") for x in temperatures],
                    collect=True,
                    filters=["x1-m4", "x3-m5"],
                ),
                repeat=100,
            ),
        ],
        volume=30,
    )

    assert prot.to_scpicommand().to_string() == PROTSTRING
    assert prot.to_scpicommand() == prot_explicitfilter.to_scpicommand()

    assert str(prot) != str(prot_explicitfilter)

    prot_fromstring = Protocol.from_scpicommand(SCPICommand.from_string(PROTSTRING))

    assert prot_explicitfilter.to_scpicommand() == prot_fromstring.to_scpicommand()


def test_refaccess():
    exp = []


def test_exp_saveload_proto(tmp_path: pathlib.Path) -> None:
    temperatures = list(np.linspace(51.2, 49.4, num=6))
    prot = Protocol(
        name="testproto",
        stages=[
            Stage(Step(5 * 60, 80)),
            Stage(
                Step(
                    60,
                    80,
                    temp_increment=-1,
                    temp_incrementcycle=3,
                    time_incrementcycle=4,
                ),
                repeat=27,
            ),
            Stage(
                Step(
                    2 * 60,
                    53,
                    collect=True,
                ),
                repeat=5,
            ),
            Stage(Step(5 * 60, temperatures, collect=True), repeat=20),
            Stage(
                Step(10 * 60, temperatures, collect=True, filters=["x1-m4", "x3-m5"]),
                repeat=20,
            ),
            Stage(
                Step(20 * 60, temperatures, collect=True, filters=["x1-m4", "x3-m5"]),
                repeat=100,
            ),
        ],
        filters=["x1-m4", "x3-m5"],
        volume=30,
    )
    exp = Experiment(protocol=prot)
    exp.save_file(tmp_path / "test_proto.eds")
    exp2 = Experiment.from_file(tmp_path / "test_proto.eds")

    # FIXME: for now, we don't do a great job with save/load for default filters
    assert (
        exp.protocol.to_scpicommand().to_string()
        == exp2.protocol.to_scpicommand().to_string()
    )


def test_stepped_ramp_down():
    srstage = Stage.stepped_ramp(60.0, 40.0, total_time=60 * 21)

    df = srstage.dataframe()

    assert len(df) == 21
    assert df.iloc[-1, :]["temperature_1"] == 40.0
    assert df.iloc[0, :]["temperature_1"] == 60.0
    assert df.iloc[1, :]["temperature_1"] == 59.0

    assert srstage == Stage(Step(60, 60.0, temp_increment=-1.0), 21)


def test_stepped_ramp_up():
    srstage = Stage.stepped_ramp(40.0, 60.0, 60 * 41, n_steps=41)

    df = srstage.dataframe()

    assert len(df) == 41
    assert df.iloc[-1, :]["temperature_1"] == 60.0
    assert df.iloc[0, :]["temperature_1"] == 40.0
    assert df.iloc[1, :]["temperature_1"] == 40.5

    assert srstage == Stage(Step(60, 40.0, temp_increment=0.5), 41)


def test_exposure_roundtrip():
    e = Exposure([(FilterSet(1, 4), [500, 2000])])
    s = e.to_scpicommand().to_string()
    assert SCPICommand.from_string(s).specialize() == e
