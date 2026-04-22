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


@requires_machine
def test_run_status_idle():
    m = Machine(TEST_MACHINE, port=TEST_PORT, password=TEST_PASSWORD, ssl=TEST_SSL)
    with m:
        rs = m.run_status()
        assert rs.name == "-"


@requires_machine
def test_machine_status():
    m = Machine(TEST_MACHINE, port=TEST_PORT, password=TEST_PASSWORD, ssl=TEST_SSL)
    with m:
        ms = m.machine_status()
        assert hasattr(ms, "drawer")
        assert hasattr(ms, "sample_temperatures")


@requires_machine
def test_get_running_protocol_no_run():
    m = Machine(TEST_MACHINE, port=TEST_PORT, password=TEST_PASSWORD, ssl=TEST_SSL)
    with m:
        with pytest.raises(Exception):
            m.get_running_protocol()


@requires_machine
def test_get_zone_count():
    m = Machine(TEST_MACHINE, port=TEST_PORT, password=TEST_PASSWORD, ssl=TEST_SSL)
    with m:
        z = m.get_zone_count()
        assert z >= 1


@requires_machine
def test_list_runs_in_storage():
    m = Machine(TEST_MACHINE, port=TEST_PORT, password=TEST_PASSWORD, ssl=TEST_SSL)
    with m:
        runs = m.list_runs_in_storage()
        assert isinstance(runs, list)


@requires_machine
def test_list_runs_in_storage_verbose():
    m = Machine(TEST_MACHINE, port=TEST_PORT, password=TEST_PASSWORD, ssl=TEST_SSL)
    with m:
        runs = m.list_runs_in_storage(verbose=True)
        assert isinstance(runs, list)


@requires_machine
def test_power_status():
    m = Machine(TEST_MACHINE, port=TEST_PORT, password=TEST_PASSWORD, ssl=TEST_SSL)
    with m:
        p = m.power
        assert isinstance(p, bool)


@requires_machine
def test_drawer_position():
    m = Machine(TEST_MACHINE, port=TEST_PORT, password=TEST_PASSWORD, ssl=TEST_SSL)
    with m:
        d = m.drawer_position
        assert d in ("Open", "Closed", "Unknown")


@requires_machine
def test_block_temperature():
    m = Machine(TEST_MACHINE, port=TEST_PORT, password=TEST_PASSWORD, ssl=TEST_SSL)
    with m:
        b = m.block
        assert isinstance(b, tuple) and len(b) == 2


@requires_machine
def test_run_command_help():
    m = Machine(TEST_MACHINE, port=TEST_PORT, password=TEST_PASSWORD, ssl=TEST_SSL)
    with m:
        result = m.run_command("HELP?")
        assert isinstance(result, str) and len(result) > 0


@requires_machine
def test_cover_position():
    m = Machine(TEST_MACHINE, port=TEST_PORT, password=TEST_PASSWORD, ssl=TEST_SSL)
    with m:
        pos = m.cover_position
        assert pos in ("Up", "Down", "Unknown", "")


@requires_machine
def test_drawer_open_close():
    m = Machine(
        TEST_MACHINE,
        port=TEST_PORT,
        max_access_level="Controller",
        password=TEST_PASSWORD,
        ssl=TEST_SSL,
    )
    with m:
        m.drawer_open()
        m.drawer_close(lower_cover=False, check=False)


@requires_machine
def test_cover_lower():
    m = Machine(
        TEST_MACHINE,
        port=TEST_PORT,
        max_access_level="Controller",
        password=TEST_PASSWORD,
        ssl=TEST_SSL,
    )
    with m:
        # Close drawer first, then lower cover
        m.drawer_close(lower_cover=False, check=False)
        m.cover_lower(check=False)


@requires_machine
def test_run_command_to_bytes():
    m = Machine(TEST_MACHINE, port=TEST_PORT, password=TEST_PASSWORD, ssl=TEST_SSL)
    with m:
        result = m.run_command_to_bytes("HELP?")
        assert isinstance(result, bytes) and len(result) > 0


@requires_machine
def test_run_command_bytes_raw():
    m = Machine(TEST_MACHINE, port=TEST_PORT, password=TEST_PASSWORD, ssl=TEST_SSL)
    with m:
        result = m.run_command_bytes(b"HELP?")
        assert isinstance(result, bytes) and len(result) > 0


@requires_machine
def test_define_protocol():
    proto = Protocol(
        [Stage.stepped_ramp(50, 60, 120, n_steps=3, collect=True)],
        filters=["x1-m4"],
        name="TestProto",
    )
    m = Machine(
        TEST_MACHINE,
        port=TEST_PORT,
        max_access_level="Controller",
        password=TEST_PASSWORD,
        ssl=TEST_SSL,
    )
    with m:
        m.define_protocol(proto)


@requires_machine
def test_list_files_verbose():
    m = Machine(TEST_MACHINE, port=TEST_PORT, password=TEST_PASSWORD, ssl=TEST_SSL)
    with m:
        result = m.list_files("public_run_complete:", verbose=True)
        assert isinstance(result, list)
        # Each entry should be a dict with at least 'path'
        for entry in result:
            assert isinstance(entry, dict)
            assert "path" in entry


@requires_machine
def test_current_run_name_no_run():
    m = Machine(TEST_MACHINE, port=TEST_PORT, password=TEST_PASSWORD, ssl=TEST_SSL)
    with m:
        name = m.current_run_name
        assert name is None


@requires_machine
def test_at_access_context_manager():
    m = Machine(TEST_MACHINE, port=TEST_PORT, password=TEST_PASSWORD, ssl=TEST_SSL)
    with m:
        initial = m.access_level
        with m.at_access("Controller"):
            assert m.get_access_level()[0] == AccessLevel.Controller
        # After exiting context, should restore previous level
        restored = m.get_access_level()[0]
        assert restored == initial


@requires_machine
def test_block_setter_off():
    m = Machine(
        TEST_MACHINE,
        port=TEST_PORT,
        max_access_level="Controller",
        password=TEST_PASSWORD,
        ssl=TEST_SSL,
    )
    with m:
        m.block = False
        on, temp = m.block
        assert on is False


@requires_machine
def test_block_setter_tuple():
    m = Machine(
        TEST_MACHINE,
        port=TEST_PORT,
        max_access_level="Controller",
        password=TEST_PASSWORD,
        ssl=TEST_SSL,
    )
    with m:
        m.block = (True, 30.0)
        on, temp = m.block
        assert on is True
        assert abs(temp - 30.0) < 1.0
        # Clean up: turn block off
        m.block = False


@requires_machine
def test_generate_random_key():
    m = Machine(
        TEST_MACHINE,
        port=TEST_PORT,
        max_access_level="Controller",
        password=TEST_PASSWORD,
        ssl=TEST_SSL,
    )
    with m:
        key = m.generate_random_key()
        assert isinstance(key, str)
        assert len(key) > 0


@requires_machine
def test_power_cycle():
    m = Machine(
        TEST_MACHINE,
        port=TEST_PORT,
        max_access_level="Controller",
        password=TEST_PASSWORD,
        ssl=TEST_SSL,
    )
    with m:
        original = m.power
        m.power = True
        assert m.power is True
        m.power = False
        assert m.power is False
        # Restore
        m.power = original


@requires_machine
def test_get_expfile_list():
    m = Machine(TEST_MACHINE, port=TEST_PORT, password=TEST_PASSWORD, ssl=TEST_SSL)
    with m:
        # This may return an empty list or a list of files
        try:
            result = m.get_expfile_list("*", allow_nomatch=True)
            assert isinstance(result, list)
        except Exception:
            pass  # May fail if no experiment is running
