# SPDX-FileCopyrightText: 2021-2023 Constantine Evans <qslib@mb.costi.net>
# SPDX-License-Identifier: EUPL-1.2

import asyncio
import os
import uuid

import pytest

from qslib import AccessLevel, Experiment, Machine, PlateSetup, Protocol, Stage
from qslib.experiment import MachineBusyError
from qslib.machine import AlreadyCollectedError


# Machine configuration from environment variables
TEST_MACHINE = os.environ.get("QSLIB_TEST_MACHINE", "localhost")
TEST_PORT = int(os.environ.get("QSLIB_TEST_PORT", "7443"))
TEST_PASSWORD = os.environ.get("QSLIB_TEST_PASSWORD", "")
TEST_SSL = os.environ.get("QSLIB_TEST_SSL", "true").lower() in ("true", "1", "yes")

requires_machine = pytest.mark.skipif(
    os.environ.get("QSLIB_TEST_MACHINE") is None,
    reason="No test machine configured (set QSLIB_TEST_MACHINE env var)"
)


@requires_machine
@pytest.mark.asyncio
async def test_real_insufficientaccess():
    m = Machine(TEST_MACHINE, port=TEST_PORT, password=TEST_PASSWORD, ssl=TEST_SSL)
    with m:
        with pytest.raises(Exception):
            m.run_command("MACRO USER?")


@requires_machine
@pytest.mark.asyncio
async def test_real_accesslevelexceeded():
    m = Machine(TEST_MACHINE, port=TEST_PORT, max_access_level="Full", password=TEST_PASSWORD, ssl=TEST_SSL)
    with m:
        with pytest.raises(Exception):
            m.set_access_level(AccessLevel.Full)


@requires_machine
def test_above_self_access_limit():
    m = Machine(TEST_MACHINE, port=TEST_PORT, max_access_level="Observer", password=TEST_PASSWORD, ssl=TEST_SSL)
    with pytest.raises(ValueError):
        m.set_access_level(AccessLevel.Controller)
    with m:
        assert m.access_level == AccessLevel.Observer


@requires_machine
@pytest.mark.asyncio
async def test_real_autherror():
    m = Machine(TEST_MACHINE, port=TEST_PORT, password="aninvalidpassword", ssl=TEST_SSL)
    with pytest.raises(Exception):
        with m:
            m.run_command("HELP?")


@requires_machine
def test_real_invocationerror():
    m = Machine(TEST_MACHINE, port=TEST_PORT, password=TEST_PASSWORD, ssl=TEST_SSL)
    with m:
        with pytest.raises(Exception):
            m.run_command("HELP? A B")


@requires_machine
@pytest.mark.parametrize("encoding", ["base64", "plain"])
def test_file_read_write_default(encoding):
    m = Machine(TEST_MACHINE, port=TEST_PORT, password=TEST_PASSWORD, ssl=TEST_SSL)

    m.write_file("public_run_complete:test.txt", "Hello, world!")

    assert "public_run_complete:test.txt" in m.list_files("public_run_complete:")

    data = m.read_file("public_run_complete:test.txt", encoding=encoding)
    assert data == b"Hello, world!"

    assert data == m.read_file("test.txt", context="public_run_complete", encoding=encoding)
    assert data == m.read_file("test.txt", context="public_run_complete:", encoding=encoding)

    with m.at_access(AccessLevel.Controller):
        m.run_command("FILE:REMOVE public_run_complete:test.txt")

    assert "public_run_complete:test.txt" not in m.list_files("public_run_complete:")


# @pytest.mark.asyncio
# async def test_real_fakeexperiment():
#     exp = Experiment.from_file("tests/test.eds")
#     # We need better sample arrangements:
#     exp.sample_wells["Sample 1"] = ["A7", "A8"]
#     exp.sample_wells["Sample 2"] = ["A9", "A10"]
#     exp.sample_wells["othersample"] = ["B7"]

#     with Machine(TEST_MACHINE) as m:
#         with m.at_access("Controller"):
#             exp._populate_folder(m)


@requires_machine
@pytest.mark.asyncio
async def test_real_experiment():
    proto = Protocol(
        [
            Stage.stepped_ramp(
                50, [30, 31, 32, 33, 34, 35], 240, n_steps=10, collect=True
            )
        ],
        filters=["x1-m4", "x3-m5"],
    )

    exp = Experiment(uuid.uuid1().hex, proto, PlateSetup({"s": "A1"}))

    m = Machine(TEST_MACHINE, port=TEST_PORT, max_access_level="Controller", password=TEST_PASSWORD, ssl=TEST_SSL)

    exp.run(m, require_drawer_check=False)


    exp.pause_now()

    with pytest.raises(
        MachineBusyError, match=rf"Machine {TEST_MACHINE}:[^ ]+ is currently busy: .*"
    ):
        exp.run(m)


    exp.sync_from_machine(m)

    proto2 = Protocol(
        [
            Stage.stepped_ramp(
                50, [30, 31, 32, 33, 34, 35], 240, n_steps=10, collect=True
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
        await asyncio.sleep(1)
        rs = m.run_status()


    exp.sync_from_machine(m)

    exp3 = Experiment.from_machine(m, exp.name)

    assert exp2.name == exp3.name

    rs = m.run_status()

    assert rs.name == "-"

    with m:
        with m.at_access("Controller"):
            m.compile_eds(exp.name)

    exp_from_eds = Experiment.from_machine(m, exp.name)
    assert exp_from_eds.name == exp.name

    with pytest.raises(AlreadyCollectedError):
        m.compile_eds(exp.name)


@requires_machine
def test_block_temp():
    m = Machine(TEST_MACHINE, port=TEST_PORT, password=TEST_PASSWORD, ssl=TEST_SSL)
    with m:
        assert m.block == (False, 25.0)
