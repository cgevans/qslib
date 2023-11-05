# SPDX-FileCopyrightText: 2021-2023 Constantine Evans <qslib@mb.costi.net>
# SPDX-License-Identifier: EUPL-1.2

import asyncio
import uuid

import pytest

from qslib import AccessLevel, Experiment, Machine, PlateSetup, Protocol, Stage
from qslib.experiment import MachineBusyError
from qslib.qs_is_protocol import (
    AccessLevelExceeded,
    AuthError,
    InsufficientAccess,
    InvocationError,
)


@pytest.mark.asyncio
async def test_real_insufficientaccess():
    m = Machine("localhost", password="correctpassword")
    with m:
        with pytest.raises(InsufficientAccess):
            m.run_command("MACRO USER?")


@pytest.mark.asyncio
async def test_real_accesslevelexceeded():
    m = Machine("localhost", max_access_level="Full", password="correctpassword")
    with m:
        with pytest.raises(AccessLevelExceeded):
            m.set_access_level(AccessLevel.Full)


@pytest.mark.asyncio
async def test_real_autherror():
    m = Machine("localhost", password="aninvalidpassword")
    with pytest.raises(AuthError):
        with m:
            m.run_command("HELP?")


def test_real_invocationerror():
    m = Machine("localhost", password="correctpassword")
    with m:
        with pytest.raises(InvocationError):
            m.run_command("HELP? A B")


# @pytest.mark.asyncio
# async def test_real_fakeexperiment():
#     exp = Experiment.from_file("tests/test.eds")
#     # We need better sample arrangements:
#     exp.sample_wells["Sample 1"] = ["A7", "A8"]
#     exp.sample_wells["Sample 2"] = ["A9", "A10"]
#     exp.sample_wells["othersample"] = ["B7"]

#     with Machine("localhost") as m:
#         with m.at_access("Controller"):
#             exp._populate_folder(m)


@pytest.mark.asyncio
async def test_real_experiment():
    # mon = Collector(
    #     Config(machine=MachineConfig(host="localhost", password="correctpassword"))
    # )
    # confut: Future[bool] = asyncio.Future()
    # task = mon.monitor(confut)
    # asyncio.tasks.create_task(task)
    # await confut

    proto = Protocol(
        [
            Stage.stepped_ramp(
                50, [30, 31, 32, 33, 34, 35], 120, n_steps=5, collect=True
            )
        ],
        filters=["x1-m4", "x3-m5"],
    )

    exp = Experiment(uuid.uuid1().hex, proto, PlateSetup({"s": "A1"}))

    m = Machine("localhost", max_access_level="Controller", password="correctpassword")

    exp.run(m, require_drawer_check=False)

    with pytest.raises(
        MachineBusyError, match=r"Machine localhost:[^ ]+ is currently busy: .*"
    ):
        exp.run(m)

    exp.sync_from_machine(m)

    exp.pause_now()

    await asyncio.sleep(3)

    proto2 = Protocol(
        [
            Stage.stepped_ramp(
                50, [30, 31, 32, 33, 34, 35], 120, n_steps=5, collect=True
            ),
            Stage.stepped_ramp(30, 50, 120, n_steps=5, collect=True),
        ],
        filters=["x1-m4", "x3-m5"],
    )

    assert proto != proto2
    assert proto != 5

    exp.change_protocol(proto2)

    exp2 = Experiment.from_running(m)

    # assert exp.protocol == exp2.protocol
    assert exp.name == exp2.name

    exp.resume()

    exp.get_status()

    exp.sync_from_machine(m)

    rs = m.run_status()
    while rs.name != "-":
        await asyncio.sleep(2)
        rs = m.run_status()

    await asyncio.sleep(2)

    exp.sync_from_machine(m)

    exp3 = Experiment.from_machine(m, exp.name)

    assert exp2.name == exp3.name

    rs = m.run_status()

    assert rs.name == "-"

    with m:
        with m.at_access("Controller"):
            await m.connection.compile_eds(exp.name)

    # with pytest.raises(AlreadyExistsError, match=f"Run {exp.name} exists.*"):
    #     exp.runstate = "INIT"
    #     exp.run("localhost")
