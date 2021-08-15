from os import PathLike
import pytest  # noqa
import qslib.tcprotocol as tc
import numpy as np
from qslib.common import Stage, Step, Protocol, Experiment
import pathlib

PROTSTRING = """PROTocol -volume=30 -runmode=standard testproto <quote.message>
\tSTAGe 1 STAGE_1 <multiline.stage>
\t\tSTEP 1 <multiline.step>
\t\t\tRAMP -increment=0.0 -incrementcycle=2 80 80 80 80 80 80
\t\t\tHOLD -increment=0 -incrementcycle=2 300
\t\t</multiline.step>
\t</multiline.stage>
\tSTAGe -repeat=27 2 STAGE_2 <multiline.stage>
\t\tSTEP 1 <multiline.step>
\t\t\tRAMP -increment=-1 -incrementcycle=2 80 80 80 80 80 80
\t\t\tHOLD -increment=0 -incrementcycle=2 60
\t\t</multiline.step>
\t</multiline.stage>
\tSTAGe -repeat=5 3 STAGE_3 <multiline.stage>
\t\tSTEP 1 <multiline.step>
\t\t\tRAMP -increment=0.0 -incrementcycle=2 53 53 53 53 53 53
\t\t\tHACFILT m4,x1,quant m5,x3,quant
\t\t\tHoldAndCollect -increment=0 -incrementcycle=2 -tiff=False -quant=True -pcr=False 120
\t\t</multiline.step>
\t</multiline.stage>
\tSTAGe -repeat=20 4 STAGE_4 <multiline.stage>
\t\tSTEP 1 <multiline.step>
\t\t\tRAMP -increment=0.0 -incrementcycle=2 51.2 50.84 50.480000000000004 50.12 49.76 49.4
\t\t\tHACFILT m4,x1,quant m5,x3,quant
\t\t\tHoldAndCollect -increment=0 -incrementcycle=2 -tiff=False -quant=True -pcr=False 300
\t\t</multiline.step>
\t</multiline.stage>
\tSTAGe -repeat=20 5 STAGE_5 <multiline.stage>
\t\tSTEP 1 <multiline.step>
\t\t\tRAMP -increment=0.0 -incrementcycle=2 51.2 50.84 50.480000000000004 50.12 49.76 49.4
\t\t\tHACFILT m4,x1,quant m5,x3,quant
\t\t\tHoldAndCollect -increment=0 -incrementcycle=2 -tiff=False -quant=True -pcr=False 600
\t\t</multiline.step>
\t</multiline.stage>
\tSTAGe -repeat=100 6 STAGE_6 <multiline.stage>
\t\tSTEP 1 <multiline.step>
\t\t\tRAMP -increment=0.0 -incrementcycle=2 51.2 50.84 50.480000000000004 50.12 49.76 49.4
\t\t\tHACFILT m4,x1,quant m5,x3,quant
\t\t\tHoldAndCollect -increment=0 -incrementcycle=2 -tiff=False -quant=True -pcr=False 1200
\t\t</multiline.step>
\t</multiline.stage>
</quote.message>"""  # noqa


def test_temperature_lists() -> None:
    a = 52.3
    a_l = [52.3] * 6
    a_a = np.array(a_l)

    d_a = np.linspace(40, 46, 6)
    d_l = list(d_a)

    assert (
        tc._temperature_str(a)
        == tc._temperature_str(a_l)
        == tc._temperature_str(a_a)
        == "52.30°C"
    )
    assert tc._temperature_str(d_a) == tc._temperature_str(d_l)


def test_proto() -> None:
    temperatures = list(np.linspace(51.2, 49.4, num=6))
    prot = Protocol(
        name="testproto",
        stages=[
            Stage(Step(5 * 60, 80)),
            Stage(Step(60, 80, temp_increment=-1), repeat=27),
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

    prot_explicitfilter = Protocol(
        name="testproto",
        stages=[
            Stage(Step(5 * 60, 80)),
            Stage(Step(60, 80, temp_increment=-1), repeat=27),
            Stage(
                Step(
                    2 * 60,
                    53,
                    filters=["x1-m4", "x3-m5"],
                    collect=True,
                ),
                repeat=5,
            ),
            Stage(
                Step(5 * 60, temperatures, collect=True, filters=["x1-m4", "x3-m5"]),
                repeat=20,
            ),
            Stage(
                Step(10 * 60, temperatures, collect=True, filters=["x1-m4", "x3-m5"]),
                repeat=20,
            ),
            Stage(
                Step(20 * 60, temperatures, collect=True, filters=["x1-m4", "x3-m5"]),
                repeat=100,
            ),
        ],
        volume=30,
    )

    assert prot.to_command() == PROTSTRING
    assert prot.to_command() == prot_explicitfilter.to_command()

    prot_fromstring = Protocol.from_command(PROTSTRING)

    assert prot_explicitfilter == prot_fromstring

def test_exp_saveload_proto(tmp_path: pathlib.Path):
    temperatures = list(np.linspace(51.2, 49.4, num=6))
    prot = Protocol(
        name="testproto",
        stages=[
            Stage(Step(5 * 60, 80)),
            Stage(Step(60, 80, temp_increment=-1), repeat=27),
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
    assert exp.protocol.to_command() == exp2.protocol.to_command()
