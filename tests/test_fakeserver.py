# SPDX-FileCopyrightText: 2021-2023 Constantine Evans <qslib@mb.costi.net>
# SPDX-License-Identifier: EUPL-1.2

import asyncio
import re

import pytest

from qslib import Machine
from qslib._qslib import QSConnection


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

            if x := re.match(rb"(\d+) TESTERRORSERVER", line):
                sw.write(b"ERRor " + x.group(1) + b" testerror\n")
                await sw.drain()

            if x := re.match(rb"(\d+) TESTUNKNOWNRESPSERVER", line):
                sw.write(b"UNKNOWNCOMMAND " + x.group(1) + b" testerror\n")
                await sw.drain()
                sw.write(b"OK " + x.group(1) + b"\n")
                await sw.drain()

            if x := re.match(rb"(\d+) DOUBLEOK", line):
                sw.write(b"OK 12345 doubleok\n")
                await sw.drain()
                sw.write(b"OK " + x.group(1) + b"\n")
                await sw.drain()

            if x := re.match(rb"(\d+) TESTNEXTSERVER", line):
                sw.write(b"NEXT " + x.group(1) + b"\n")
                await sw.drain()
                sw.write(b"MESSage testservermessage ueao\n")
                await sw.drain()
                sw.write(b"MESSage testservermessage 123456789.021 ueao\n")
                await sw.drain()
                sw.write(b"OK " + x.group(1) + b"\n")
                await sw.drain()

            if x := re.match(rb"TESTNEXTSERVER", line):
                sw.write(b"NEXT " + line[:-1] + b"\n")
                await sw.drain()
                sw.write(b"MESSage testservermessage ueao\n")
                await sw.drain()
                sw.write(b"MESSage testservermessage 123456789.021 ueao\n")
                await sw.drain()
                sw.write(b"OK " + line[:-1] + b" return message" + b"\n")
                await sw.drain()

            if x := re.match(rb"(\d+) TESTNEXTSERVERDELAY", line):
                sw.write(b"NEXT " + x.group(1) + b"\n")
                await sw.drain()
                sw.write(b"MESSage testservermessage ueao\n")
                await asyncio.sleep(1)
                await sw.drain()
                sw.write(b"OK " + x.group(1) + b"\n")
                await sw.drain()

            for com, resp in crlist.items():
                if x := re.match(rb"(\d+) " + re.escape(com.encode()), line):
                    sw.write(b"OK " + x.group(1) + b" " + resp.encode() + b"\n")
                    await sw.drain()
                    break

    return _fakeserver_runner


# # @pytest.mark.parametrize("ssl", [True, False])
# @pytest.mark.asyncio
# async def test_responses():
#     srv = await asyncio.start_server(crcb({}), "localhost", 53533)

#     async with srv:
#         m = Machine("localhost", port=53533, ssl=False)

#         m.connect()

#         with pytest.raises(Exception):
#             m.run_command("TESTERRORSERVER")

#         m.run_command("TESTUNKNOWNRESPSERVER")
#         # FIXME: this should check logs

#         m.run_command_to_ack("TESTNEXTSERVER")

#         m.run_command("TESTNEXTSERVER")

#         m.run_command("TESTNEXTSERVERDELAY")

#         m.disconnect()


# @pytest.mark.asyncio
# async def test_connection():
#     srv = await asyncio.start_server(crcb({}), "localhost", 53533)

#     async with srv:
#         m = Machine("localhost", port=53533, ssl=False)

#         assert m.connected is False

#         m.connect()

#         assert m.connected is True

#         m.disconnect()

#         assert m.connected is False

#         m = Machine("localhost", port=53533, ssl=False)

#         m.connect()

#         assert m.connected is True

#         assert m._connection is not None

#         m._connection._protocol.waiting_commands.append((b"12345", None))

#         with pytest.raises(ConnectionError):
#             m.run_command_bytes(b"TESTKILLSERVER")

#         assert m.connected is False


# @pytest.mark.asyncio
# async def test_acc_level_set():
#     srv = await asyncio.start_server(crcb({}), "localhost", 53533)

#     async with srv:
#         m = Machine("localhost", port=53533, ssl=False)

#         with m:
#             with m.at_access("Observer"):
#                 assert m.get_access_level() == ("Observer", False, False)

#             m.set_access_level("Observer")


# @pytest.mark.asyncio
# async def test_runtitle():
#     srv = await asyncio.start_server(crcb({"RUNTitle?": "aoeu"}), "localhost", 53533)

#     async with srv:
#         with Machine("localhost", port=53533, ssl=False) as m:
#             assert m.current_run_name == "aoeu"


# @pytest.mark.asyncio
# async def test_runtitle_not_running():
#     srv = await asyncio.start_server(crcb({"RUNTitle?": "-"}), "localhost", 53533)

#     async with srv:
#         with Machine("localhost", port=53533, ssl=False) as m:
#             assert m.current_run_name is None


# @pytest.mark.asyncio
# async def test_quote():
#     msg = "<quote>a\nu\n\n \n <quote.2>C\n</quote.2>\n  </quote>"
#     srv = await asyncio.start_server(crcb({"TESTQUOTE": msg}), "localhost", 53533)

#     async with srv:
#         with Machine("localhost", port=53533, ssl=False) as m:
#             assert m.run_command("TESTQUOTE") == msg


# @pytest.mark.skip  # FIXME
# @pytest.mark.asyncio
# async def test_invalid_quote():
#     msg = "<quote>a\nu\n</quote.2>\n  </quote>"
#     srv = await asyncio.start_server(crcb({"TESTQUOTE": msg}), "localhost", 53533)

#     async with srv:
#         with Machine("localhost", port=53533, ssl=False) as m:
#             with pytest.raises(ConnectionError):
#                 m.run_command("TESTQUOTE")


# @pytest.mark.asyncio
# async def test_nonuid_nonreturn():
#     srv = await asyncio.start_server(crcb({}), "localhost", 53533)

#     async with srv:
#         qsa = QSConnection("localhost", 53533, ssl=False)

#         assert qsa.connected is False

#         async with qsa:
#             assert b"return message\n" == await qsa.run_command(
#                 "TESTNEXTSERVER", uid=False, just_ack=False
#             )

#             qsa._protocol.waiting_commands.append((b"12345", None))
#             await qsa._protocol.run_command("DOUBLEOK")
