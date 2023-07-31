# SPDX-FileCopyrightText: 2021-2022 Constantine Evans <const@costi.eu>
# SPDX-License-Identifier: AGPL-3.0-only

import pathlib

import numpy as np

from qslib import Experiment
from qslib.protocol import Q_, UR, Exposure, FilterSet, Protocol, Stage, Step
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
    pass


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


def test_stepped_ramp_multi_same_increment():
    p1 = Protocol(
        [
            Stage.stepped_ramp(
                [50, 50, 50, 40, 40, 40], [40, 40, 40, 30, 30, 30], "11 min"
            ),
            Stage.stepped_ramp(
                [40, 40, 40, 30, 30, 30],
                [30, 30, 30, 20, 20, 20.0],
                "10 min",
                start_increment=True,
            ),
            # Stage.stepped_ramp(None, [20,20,20,10,10,10], "9 min", start_increment=True), # FIXME: implement this
        ]
    )

    str(p1)

    p1.to_scpicommand()

    assert (
        (p1.dataframe.loc[:, "temperature_1":"temperature_6"].diff()[1:] == -1)
        .all()
        .all()
    )
    assert (p1.dataframe.start_time.diff()[1:] == 60.625).all()
    assert (p1.dataframe.end_time.diff()[1:] == 60.625).all()


def test_delta_unit_conversion():
    assert (
        Stage.stepped_ramp(
            "50 °C", "20 °C", total_time="30 min", temperature_step="2 °C"
        )
        == Stage.stepped_ramp(
            "50 °C", "20 °C", total_time="30 min", temperature_step="2 delta_degC"
        )
        == Stage.stepped_ramp(
            "50 °C", "20 °C", total_time="30 min", temperature_step="3.6 degF"
        )
    )


def test_hold():
    h = Stage.hold_at("60 °C", "1 hour", "10 minutes")
    assert h == Stage(Step("10 min", "60 °C"), 6)

    assert Stage.hold_at("50 °C", total_time="1 hour").steps[0].duration_at_cycle(
        0
    ) == Q_("1 hour")
