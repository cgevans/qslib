# SPDX-FileCopyrightText: 2021-2023 Constantine Evans <qslib@mb.costi.net>
# SPDX-License-Identifier: EUPL-1.2

import pathlib

import numpy as np

from qslib import Experiment
from qslib.protocol import Q_, UR, Exposure, FilterSet, Protocol, Stage, Step, _durformat
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

    assert Stage.hold_at("50 °C", total_time="1 hour").steps[0].duration_at_cycle_point(
        0
    ) == Q_("1 hour")

def test_protocol_equality():
    """Test that protocols are equal if they have the same stages, filters, and 
       volume, even if the names are different"""
    prot = Protocol(
        [
            Stage.hold_at(25, "3min", "30s", collect=True)
        ], filters=["x1-m1", "x3-m5", "x2-m2"]
    )
    prot2 = Protocol(
        [
            Stage.hold_at(25, "3min", "30s", collect=True)
        ], filters=["x1-m1", "x3-m5", "x2-m2"], name="prot2"
    )
    assert prot == prot2

    prot2.volume = 40
    assert prot != prot2


def test_durformat_basic_units():
    """Test _durformat with basic time units"""
    # Seconds
    assert _durformat(Q_(1, "seconds")) == "1s"
    assert _durformat(Q_(30, "seconds")) == "30s"
    assert _durformat(Q_(59, "seconds")) == "59s"
    assert _durformat(Q_(120, "seconds")) == "120s"  # 2 minutes, stays as seconds
    
    # Minutes (> 2 minutes)
    assert _durformat(Q_(180, "seconds")) == "3m"  # 3 minutes
    assert _durformat(Q_(3600, "seconds")) == "60m"  # 1 hour
    assert _durformat(Q_(7200, "seconds")) == "120m"  # 2 hours, stays as minutes
    
    # Hours (> 2 hours, <= 3 days)
    assert _durformat(Q_(10800, "seconds")) == "3h"  # 3 hours
    assert _durformat(Q_(86400, "seconds")) == "24h"  # 1 day exactly
    assert _durformat(Q_(172800, "seconds")) == "48h"  # 2 days exactly
    assert _durformat(Q_(259200, "seconds")) == "72h"  # 3 days exactly


def test_durformat_exact_day_durations():
    """Test _durformat with exact day durations that were previously broken"""
    # These were returning empty strings before the fix
    assert _durformat(Q_(86400, "seconds")) == "24h"  # 1 day
    assert _durformat(Q_(172800, "seconds")) == "48h"  # 2 days
    assert _durformat(Q_(259200, "seconds")) == "72h"  # 3 days
    
    # Days > 3 days should show in days format
    assert _durformat(Q_(345600, "seconds")) == "4d"  # 4 days
    assert _durformat(Q_(432000, "seconds")) == "5d"  # 5 days
    assert _durformat(Q_(604800, "seconds")) == "7d"  # 1 week


def test_durformat_mixed_durations():
    """Test _durformat with mixed time components"""
    # Seconds + minutes
    assert _durformat(Q_(61, "seconds")) == "61s"  # 1m1s, but <= 2 minutes
    assert _durformat(Q_(181, "seconds")) == "3m1s"  # 3m1s
    
    # Minutes + hours
    assert _durformat(Q_(3661, "seconds")) == "61m1s"  # 1h1m1s, but <= 2 hours
    assert _durformat(Q_(10861, "seconds")) == "3h1m1s"  # 3h1m1s
    
    # Hours + days (> 3 days)
    assert _durformat(Q_(349200, "seconds")) == "4d1h"  # 4d1h
    assert _durformat(Q_(349260, "seconds")) == "4d1h1m"  # 4d1h1m
    assert _durformat(Q_(349261, "seconds")) == "4d1h1m1s"  # 4d1h1m1s


def test_durformat_boundary_conditions():
    """Test _durformat at boundary conditions"""
    # Exactly at boundaries
    assert _durformat(Q_(120, "seconds")) == "120s"  # Exactly 2 minutes
    assert _durformat(Q_(121, "seconds")) == "2m1s"  # Just over 2 minutes
    
    assert _durformat(Q_(7200, "seconds")) == "120m"  # Exactly 2 hours
    assert _durformat(Q_(7201, "seconds")) == "2h1s"  # Just over 2 hours
    
    assert _durformat(Q_(259200, "seconds")) == "72h"  # Exactly 3 days
    assert _durformat(Q_(259201, "seconds")) == "3d1s"  # Just over 3 days


def test_durformat_no_empty_strings():
    """Test that _durformat never returns empty strings"""
    # Test a range of values that could potentially cause issues
    test_values = [
        1, 60, 120, 3600, 7200, 86400, 172800, 259200, 345600, 432000,
        86401, 90000, 349200, 349261  # Mixed values
    ]
    
    for seconds in test_values:
        result = _durformat(Q_(seconds, "seconds"))
        assert result != "", f"Empty string returned for {seconds} seconds"
        assert len(result.strip()) > 0, f"Whitespace-only string for {seconds} seconds"
        assert result.replace("d", "").replace("h", "").replace("m", "").replace("s", "").isdigit() or \
               any(c.isdigit() for c in result), f"No digits in result '{result}' for {seconds} seconds"


def test_durformat_stage_integration():
    """Test _durformat integration with Stage.info_str()"""
    # Test that stages with exact day durations show proper total duration
    step_1day = Step(temperature=95.0, time=86400, repeat=1)  # 1 day
    stage_1day = Stage([step_1day], repeat=1)
    info_1day = stage_1day.info_str(1)
    assert "total duration 24h" in info_1day
    assert "total duration )" not in info_1day  # No empty duration
    
    step_2day = Step(temperature=95.0, time=172800, repeat=1)  # 2 days
    stage_2day = Stage([step_2day], repeat=1)
    info_2day = stage_2day.info_str(1)
    assert "total duration 48h" in info_2day
    
    step_4day = Step(temperature=95.0, time=345600, repeat=1)  # 4 days
    stage_4day = Stage([step_4day], repeat=1)
    info_4day = stage_4day.info_str(1)
    assert "total duration 4d" in info_4day