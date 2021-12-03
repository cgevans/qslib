import asyncio
import pytest
import pytest_asyncio
from qslib.common import Machine
import re
import logging


async def _fakeserver_runner(sr: asyncio.StreamReader, sw: asyncio.StreamWriter):
    sw.write(
        b"READy -session=12345 -product=QuantStudio3_5 -version=1.3.0 -build=001 -capabilities=Index\n"
    )
    await sw.drain()

    while not sr.at_eof():
        line = await sr.readline()
        print(line)
        if x := re.match(rb"(\d+) RUNTitle?", line):
            sw.write(b"OK " + x.group(1) + b" aoeu\n")
        elif x := re.match(rb"(\d+) ACC", line):
            sw.write(b"OK " + x.group(1) + b"\n")
        elif x := re.match(rb"(\d+) QUIT", line):
            sw.write(b"OK " + x.group(1) + b"\n")
            await sw.drain()
            sw.close()
            return
        await sw.drain()


@pytest.mark.asyncio
async def test_runtitle():
    srv = await asyncio.start_server(_fakeserver_runner, "localhost", 53533)

    async with srv:
        with Machine("localhost", port=53533) as m:
            assert m.current_run_name == "aoeu"
