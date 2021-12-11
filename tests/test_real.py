from asyncio.futures import Future
import pytest
import pytest_asyncio
import uuid

from qslib.common import *
from qslib.monitor import Collector, Config

import asyncio


@pytest.mark.asyncio
async def test_experiment():

    mon = Collector(Config())

    confut: Future[bool] = asyncio.Future()
    task = mon.monitor(confut)

    asyncio.tasks.create_task(task)

    await confut

    proto = Protocol(
        [Stage.stepped_ramp(50, 30, 120, 5, True)], filters=["x1-m4", "x3-m5"]
    )

    exp = Experiment(uuid.uuid1().hex, proto)

    m = Machine("localhost", port=7000, max_access_level="Controller")

    with m:
        exp.run(m)

    exp.sync_from_machine(m)

    proto2 = Protocol(
        [
            Stage.stepped_ramp(50, 30, 120, 5, True),
            Stage.stepped_ramp(30, 50, 120, 5, True),
        ],
        filters=["x1-m4", "x3-m5"],
    )

    assert proto != proto2
    assert proto != 5

    with m:
        exp.change_protocol(proto2, m)

    await asyncio.sleep(10)
