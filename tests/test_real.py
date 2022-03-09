# SPDX-FileCopyrightText: 2021-2022 Constantine Evans <const@costi.eu>
# SPDX-License-Identifier: AGPL-3.0-only

import asyncio
import uuid
from asyncio.futures import Future

import pytest
import pytest_asyncio

from qslib import *
from qslib.experiment import AlreadyExistsError, MachineBusyError
from qslib.qs_is_protocol import (
    AccessLevelExceeded,
    AuthError,
    InsufficientAccess,
    InvocationError,
)


@pytest.mark.asyncio
async def test_real_insufficientaccess():
    m = Machine("localhost")
    with m:
        with pytest.raises(InsufficientAccess):
            m.run_command("MACRO USER?")


@pytest.mark.asyncio
async def test_real_accesslevelexceeded():
    m = Machine("localhost", max_access_level="Full")
    with m:
        with pytest.raises(AccessLevelExceeded):
            m.set_access_level(AccessLevel.Full)


@pytest.mark.asyncio
async def test_real_autherror():
    m = Machine("localhost", password="aninvalidpassword")
    with pytest.raises(AuthError):
        with m:
            m.run_command("HELP?")


@pytest.mark.asyncio
def test_real_invocationerror():
    m = Machine("localhost")
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

    from qslib.monitor import Collector, Config

    mon = Collector(Config())

    confut: Future[bool] = asyncio.Future()
    task = mon.monitor(confut)

    ttask = asyncio.tasks.create_task(task)

    await confut

    proto = Protocol(
        [
            Stage.stepped_ramp(
                50, [30, 31, 32, 33, 34, 35], 120, n_steps=5, collect=True
            )
        ],
        filters=["x1-m4", "x3-m5"],
    )

    exp = Experiment(uuid.uuid1().hex, proto, PlateSetup({"s": "A1"}))

    m = Machine("localhost", port=7000, max_access_level="Controller")

    exp.run("localhost", require_drawer_check=False)

    with pytest.raises(
        MachineBusyError, match=r"Machine localhost:7000 is currently busy: .*"
    ):
        exp.run("localhost")

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

    exp2 = Experiment.from_running("localhost")

    # assert exp.protocol == exp2.protocol
    assert exp.name == exp2.name

    exp.resume()

    exp.get_status()

    exp.sync_from_machine(m)

    with m:
        with m.at_access("Controller"):
            m.run_command(f'TextWAit -timeout=7200 Run Ended \\"{exp.name}\\"')

    await asyncio.sleep(2)

    exp.sync_from_machine(m)

    exp3 = Experiment.from_machine("localhost", exp.name)

    assert exp2.name == exp3.name

    rs = m.run_status()

    assert rs.name == "-"

    with m:
        with m.at_access("Controller"):
            await m.connection.compile_eds(exp.name)

    # with pytest.raises(AlreadyExistsError, match=f"Run {exp.name} exists.*"):
    #     exp.runstate = "INIT"
    #     exp.run("localhost")
