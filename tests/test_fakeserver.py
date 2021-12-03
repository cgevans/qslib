import asyncio
import pytest
import pytest_asyncio
from qslib.common import Machine
import re
import logging


def crcb(crlist):
    async def _fakeserver_runner(sr: asyncio.StreamReader, sw: asyncio.StreamWriter):
        acc = "Guest"

        sw.write(
            b"READy -session=12345 -product=QuantStudio3_5 -version=1.3.0 -build=001 -capabilities=Index\n"
        )
        await sw.drain()

        while not sr.at_eof():
            line = await sr.readline()
            print(line)
            if x := re.match(rb"(\d+) ACC (\w+)", line, re.IGNORECASE):
                sw.write(b"OK " + x.group(1) + b"\n")
                acc = x.group(2).decode()
                continue

            if x := re.match(rb"(\d+) ACC?", line, re.IGNORECASE):
                sw.write(
                    b"OK "
                    + x.group(1)
                    + f" -stealth=False -exclusive=False {acc}".encode()
                    + b"\n"
                )
                continue

            if x := re.match(rb"(\d+) QUIT", line, re.IGNORECASE):
                sw.write(b"OK " + x.group(1) + b"\n")
                await sw.drain()
                sw.close()
                return

            if x := re.match(rb"(\d+) TESTKILLSERVER", line):
                sw.close()
                return

            for com, resp in crlist.items():
                if x := re.match(rb"(\d+) " + re.escape(com.encode()), line):
                    sw.write(b"OK " + x.group(1) + b" " + resp.encode() + b"\n")
                    await sw.drain()
                    break

    return _fakeserver_runner


@pytest.mark.asyncio
async def test_connection():
    srv = await asyncio.start_server(crcb({}), "localhost", 53533)

    async with srv:
        m = Machine("localhost", port=53533)

        assert m.connected is False

        m.connect()

        assert m.connected is True

        m.disconnect()

        assert m.connected is False

        m = Machine("localhost", port=53533, connect_now=True)

        assert m.connected is True

        with pytest.raises(ConnectionError):
            m.run_command_bytes(b"TESTKILLSERVER")

        assert m.connected is False


@pytest.mark.asyncio
async def test_acc_level_set():
    srv = await asyncio.start_server(crcb({}), "localhost", 53533)

    async with srv:
        m = Machine("localhost", port=53533)

        with m:
            with m.at_access("Observer"):
                assert m.get_access_level() == ("Observer", False, False)

            m.set_access_level("Observer")


@pytest.mark.asyncio
async def test_runtitle():
    srv = await asyncio.start_server(crcb({"RUNTitle?": "aoeu"}), "localhost", 53533)

    async with srv:
        with Machine("localhost", port=53533) as m:
            assert m.current_run_name == "aoeu"


@pytest.mark.asyncio
async def test_runtitle_not_running():
    srv = await asyncio.start_server(crcb({"RUNTitle?": "-"}), "localhost", 53533)

    async with srv:
        with Machine("localhost", port=53533) as m:
            assert m.current_run_name == None