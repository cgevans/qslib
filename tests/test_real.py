import asyncio
import uuid
from asyncio.futures import Future

import pytest
import pytest_asyncio

from qslib import *
from qslib.experiment import MachineBusyError, AlreadyExistsError


@pytest.mark.asyncio
async def test_real_experiment():

    from qslib.monitor import Collector, Config

    mon = Collector(Config())

    confut: Future[bool] = asyncio.Future()
    task = mon.monitor(confut)

    asyncio.tasks.create_task(task)

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

    exp.run("localhost")

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

    await asyncio.sleep(10)

    exp.sync_from_machine(m)

    exp.sync_from_machine(m)

    exp3 = Experiment.from_machine("localhost", exp.name)

    assert exp2.name == exp3.name

    # with pytest.raises(AlreadyExistsError, match=f"Run {exp.name} exists.*"):
    #     exp.runstate = "INIT"
    #     exp.run("localhost")
