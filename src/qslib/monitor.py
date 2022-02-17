# SPDX-FileCopyrightText: 2021-2022 Constantine Evans <const@costi.eu>
#
# SPDX-License-Identifier: AGPL-3.0-only

from __future__ import annotations

import asyncio
import functools
import logging
import os
import re
import shutil
import time
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, TextIO, Tuple, Type, Union, cast

import numpy as np
import numpy.typing as npt
from influxdb_client import InfluxDBClient, Point  # , Point, WritePrecision
from influxdb_client.client.write_api import ASYNCHRONOUS
from nio.client import AsyncClient
from nio.client.async_client import AsyncClientConfig
from nio.responses import JoinedRoomsError

from qslib.base import RunStatus
from qslib.plate_setup import PlateSetup
from qslib.qs_is_protocol import CommandError
from qslib.qsconnection_async import FilterDataFilename, QSConnectionAsync
from qslib.scpi_commands import AccessLevel, ArgList

log = logging.getLogger("monitor")

LEDSTATUS = re.compile(
    rb"Temperature:([+\-\d.]+) Current:([+\-\d.]+) Voltage:([+\-\d.]+) JuncTemp:([+\-\d.]+)"
)


@dataclass
class LEDStatus:
    temperature: float
    current: float
    voltage: float
    junctemp: float


@dataclass(frozen=True)
class MatrixConfig:
    password: str
    user: str
    room: str
    host: str
    encryption: bool = False


@dataclass(frozen=True)
class InfluxConfig:
    token: str
    org: str
    bucket: str
    url: str


@dataclass(frozen=True)
class MachineConfig:
    password: Union[str, None] = None
    name: str = "localhost"
    host: str = "localhost"
    port: str = "7000"
    retries: int = 3
    compile: bool = False


@dataclass(frozen=True)
class SyncConfig:
    completed_directory: Union[str, None] = None
    in_progress_directory: Union[str, None] = None


@dataclass(frozen=True)
class Config:
    matrix: Union[MatrixConfig, None] = None
    influxdb: Union[InfluxConfig, None] = None
    machine: MachineConfig = MachineConfig()
    sync: SyncConfig = SyncConfig()


@dataclass
class RunState:
    name: Optional[str] = None
    stage: Optional[int | str] = None
    cycle: Optional[int] = None
    step: Optional[int] = None
    plate_setup: Optional[PlateSetup] = None

    async def refresh(self, c: QSConnectionAsync) -> None:
        runmsg = ArgList.from_string(await c.run_command("RunProgress?"))
        name = cast(str, runmsg.opts["RunTitle"])
        if name == "-":
            self.name = None
        else:
            self.name = re.sub(r"(<([\w.]+)>)?([^<]+)(</[\w.]+>)?", r"\3", name)
        stage = runmsg.opts["Stage"]
        if stage == "-":
            self.stage = None
        else:
            self.stage = cast(int, stage)
        cycle = runmsg.opts["Cycle"]
        if cycle == "-":
            self.cycle = None
        else:
            self.cycle = cast(int, cycle)
        step = runmsg.opts["Step"] if self.stage else None
        if step == "-":
            self.step = None
        else:
            self.step = cast(Optional[int], step)
        if self.name:
            try:
                self.plate_setup = await PlateSetup.from_machine(c)
            except CommandError:
                self.plate_setup = None

    @classmethod
    async def from_machine(cls: Type[RunState], c: QSConnectionAsync) -> RunState:
        n = cls.__new__(cls)
        await n.refresh(c)
        return n

    def statemsg(self, timestamp: str) -> str:
        s = f'run_state name="{self.name}"'
        if self.stage:
            s += f",stage={self.stage}i,cycle={self.cycle}i,step={self.step}i"
        else:
            s += f",stage=0i,cycle=0i,step=0i"  # FIXME: not great
        s += f" {timestamp}"
        return s


@dataclass
class MachineState:
    zone_targets: List[float]
    zone_controls: List[bool]
    cover_target: float
    cover_control: bool
    drawer: str

    async def refresh(self, c: QSConnectionAsync) -> None:
        targmsg = ArgList.from_string(await c.run_command("TBC:SETT?"))
        self.cover_target = cast(float, targmsg.opts["Cover"])
        self.zone_targets = cast(
            List[float], [targmsg.opts[f"Zone{i}"] for i in range(1, 7)]
        )

        contmsg = ArgList.from_string(await c.run_command("TBC:CONT?"))
        self.cover_control = cast(bool, contmsg.opts["Cover"])
        self.zone_controls = cast(
            List[bool], [contmsg.opts[f"Zone{i}"] for i in range(1, 7)]
        )

        self.drawer = await c.run_command("DRAW?")

    @classmethod
    async def from_machine(cls, c: QSConnectionAsync) -> MachineState:
        n = cast(MachineState, cls.__new__(cls))
        await n.refresh(c)
        return n

    # def targetmsg(timestamp):
    #    return ""


@dataclass
class State:
    run: RunState
    machine: MachineState

    @classmethod
    async def from_machine(cls, c: QSConnectionAsync) -> State:
        run = await RunState.from_machine(c)
        machine = await MachineState.from_machine(c)
        return cls(run, machine)


# def parse_fd_fn(x: str) -> Tuple[str, int, int, int, int]:
#    s = re.search(r"S(\d{2})_C(\d{3})_T(\d{2})_P(\d{4})_M(\d)_X(\d)_filterdata.xml$", x)
#    assert s is not None
#    return (f"x{s[6]}-m{s[5]}", int(s[1]), int(s[2]), int(s[3]), int(s[4]))


def index_to_filename_ref(i: Tuple[str, int, int, int, int]) -> str:
    x, s, c, t, p = i
    return f"S{s:02}_C{c:03}_T{t:02}_P{p:04}_M{x[4]}_X{x[1]}"


async def get_runinfo(c: QSConnectionAsync) -> State:
    state = await State.from_machine(c)
    return state


class Collector:
    def __init__(self, config: Config):
        self.config = config

        if self.config.influxdb:
            self.idbclient = InfluxDBClient(
                url=self.config.influxdb.url,
                token=self.config.influxdb.token,
                org=self.config.influxdb.org,
            )
            self.idbw = self.idbclient.write_api(write_options=ASYNCHRONOUS)
        else:
            self.idbw = None

        if self.config.matrix:
            self.matrix_config = AsyncClientConfig(
                encryption_enabled=self.config.matrix.encryption
            )

            self.matrix_client = AsyncClient(
                self.config.matrix.host,
                self.config.matrix.user,
                store_path="./matrix_store/",
                config=self.matrix_config,
            )

        log.info(config.sync)

        self.run_log_file: TextIO | None = None

    def inject(
        self, t: str | Iterable[str | Point] | Point, flush: bool = False
    ) -> None:
        if self.idbw:
            self.idbw.write(bucket=self.config.influxdb.bucket, record=t)  # type:ignore
            if flush:
                self.idbw.flush()
        else:
            pass

    async def matrix_announce(self, msg: str) -> None:
        assert self.config.matrix
        await self.matrix_client.room_send(
            room_id=self.config.matrix.room,
            message_type="m.room.message",
            content={"msgtype": "m.text", "body": msg},
            ignore_unverified_devices=True,
        )

        await self.matrix_client.sync()

    async def setup_new_rundir(
        self,
        connection: QSConnectionAsync,
        name: str,
        *,
        firstmsg: str | None = None,
        overwrite: bool = False,
    ) -> None:
        # name = name.replace(" ", "_")

        assert self.ipdir is not None

        if not self.ipdir.is_dir():
            log.error(f"Can't open in-progress directory {self.ipdir}.")
            return

        dirpath = self.ipdir / name

        if dirpath.exists() and (not overwrite):
            log.error(f"In-progress directory for {name} already exists.")
            return
        elif dirpath.exists():
            assert dirpath != self.ipdir
            shutil.rmtree(dirpath)

        dirpath.mkdir()
        zf = await connection.read_dir_as_zip(name, "experiment")
        zf.extractall(dirpath)

        (dirpath / "apldbio" / "sds" / "quant").mkdir(exist_ok=True)
        (dirpath / "apldbio" / "sds" / "filter").mkdir(exist_ok=True)
        (dirpath / "apldbio" / "sds" / "calibrations").mkdir(exist_ok=True)

        self.run_log_file = (dirpath / "apldbio" / "sds" / "messages.log").open("a")

        if firstmsg is not None:
            self.run_log_file.write(firstmsg)
            self.run_log_file.flush()

    @property
    def ipdir(self) -> Path | None:
        x = self.config.sync.in_progress_directory
        if x is None:
            return None
        else:
            return Path(x)

    def run_ip_path(self, name: str) -> Path:
        # name = name.replace(" ", "_")
        if (ipdir := self.ipdir) is None:
            raise ValueError
        return ipdir / name / "apldbio" / "sds"

    async def compile_eds(self, connection: QSConnectionAsync, name: str) -> None:
        # name = name.replace(" ", "_")

        # Wait 5 minutes in case machine compiles it (AB sofware run)
        await asyncio.sleep(300.0)

        try:
            await connection.set_access_level(AccessLevel.Controller)
            await connection.compile_eds(name)
        except FileNotFoundError as e:
            raise e
        finally:
            await connection.set_access_level(AccessLevel.Observer)

    async def sync_completed(self, connection: QSConnectionAsync, name: str) -> None:
        # name = name.replace(" ", "_")

        try:
            await self.compile_eds(connection, name)
        except FileNotFoundError:
            pass

        dir = Path(cast(str, self.config.sync.completed_directory))

        if not dir.is_dir():
            log.error(f"Can't sync completed EDS to invalid path {dir}.")
            return

        path = dir / (name + ".eds")

        if path.exists():
            log.error(f"Completed EDS already exists for {name}.")
            return

        try:
            with path.open("wb") as f:
                edsfile = await connection.read_file(f"public_run_complete:{name}.eds")
                f.write(edsfile)
        except Exception as e:
            log.error(f"Error synchronizing completed EDS {name}: {e}")
            return

        if self.ipdir:
            import shutil

            if (self.ipdir / name).exists():
                shutil.rmtree(self.ipdir / name)
            if (x := (self.ipdir / (name + ".eds"))).exists():
                x.unlink()

    async def docollect(
        self,
        args: Dict[str, Union[str, int, bool, float]],
        state: State,
        connection: QSConnectionAsync,
    ) -> None:
        if state.run.plate_setup:
            pa: npt.NDArray[
                np.object_
            ] | None = state.run.plate_setup.well_samples_as_array()
        else:
            pa = None

        run = cast(str, args["run"])

        if run.startswith('"'):
            run = run[1:-1]

        del args["run"]
        for k, v in args.items():
            if k != "run":
                args[k] = int(v)
        pl = [
            FilterDataFilename.fromstring(x)
            for x in await connection.get_expfile_list(
                "{run}/apldbio/sds/filter/S{stage:02}_C{cycle:03}"
                "_T{step:02}_P{point:04}_*_filterdata.xml".format(
                    run=run, **cast(Dict[str, int], args)
                ),
                allow_nomatch=True,
            )
        ]
        pl.sort()
        toget = [x for x in pl if x.is_same_point(pl[-1])]

        lp: List[str] = []
        files: list[tuple[str, bytes]] = []

        if (
            self.ipdir
            and (
                self.ipdir / run / "apldbio" / "sds" / "filter"  # .replace(" ", "_")
            ).exists()
        ):
            for fdf in toget:
                fdr, files_one = await connection.get_filterdata_one(
                    fdf, return_files=True
                )
                lp += fdr.to_lineprotocol(run_name=run, sample_array=pa)
                files += files_one
        else:
            for fdf in toget:
                lp += (await connection.get_filterdata_one(fdf)).to_lineprotocol(
                    run_name=run, sample_array=pa
                )

        self.inject(lp, flush=True)

        for path, data in files:
            fullpath = self.run_ip_path(run) / path
            with fullpath.open("wb") as f:
                f.write(data)

        if (
            self.ipdir
            and (
                self.ipdir / run / "apldbio" / "sds" / "filter"  # .replace(" ", "_")
            ).exists()
        ):
            saferun = run  # .replace(" ", "_")
            ipp = self.ipdir / saferun
            with zipfile.ZipFile(self.ipdir / (saferun + ".eds"), "w") as z:
                for root, _, zfiles in os.walk(ipp):
                    for zfile in zfiles:
                        fpath = os.path.join(root, zfile)
                        z.write(fpath, os.path.relpath(fpath, ipp))

    async def handle_run_msg(
        self: Collector,
        state: State,
        c: QSConnectionAsync,
        topic: bytes,
        message: bytes,
        timestamp: float | None,
    ) -> None:
        topic_str = topic.decode()
        message_str = message.decode()

        # Are we logging?
        if self.run_log_file is not None:
            self.run_log_file.write(f"{topic_str} {timestamp} {message_str}")
            self.run_log_file.flush()

        assert timestamp is not None
        timestamp = int(1e9 * timestamp)
        msg = ArgList.from_string(message_str)
        log.debug(msg)
        contents = msg.args
        action = cast(str, contents[0])
        if action == "Stage":
            assert isinstance(contents[1], (str, int))
            state.run.stage = contents[1]
            self.inject(
                Point("run_action")
                .tag("type", action.lower())
                .field(action.lower(), contents[1])
                .time(timestamp)
                .to_line_protocol()
            )
            self.inject(
                Point("run_status")
                .tag("type", action.lower())
                .field(action.lower(), contents[1])
                .time(timestamp)
                .to_line_protocol()
            )
        elif action == "Cycle":
            state.run.cycle = int(contents[1])
            self.inject(
                Point("run_action")
                .tag("type", action.lower())
                .field(action.lower(), contents[1])
                .time(timestamp)
                .to_line_protocol()
            )
            self.inject(
                Point("run_status")
                .tag("type", action.lower())
                .field(action.lower(), contents[1])
                .time(timestamp)
                .to_line_protocol()
            )
        elif action == "Step":
            state.run.step = int(contents[1])
            self.inject(
                Point("run_status")
                .tag("type", action.lower())
                .field(action.lower(), contents[1])
                .time(timestamp)
                .to_line_protocol()
            )
            self.inject(
                Point("run_action")
                .tag("type", action.lower())
                .field(action.lower(), contents[1])
                .time(timestamp)
                .to_line_protocol()
            )
        elif action == "Holding":
            self.inject(
                f'run_action,type=Holding holdtime={msg.opts["time"]} {timestamp}'  # noqa: E501
            )
        elif action == "Ramping":
            # TODO: check zones
            state.machine.zone_targets = [
                float(x) for x in cast(list[float], msg.opts["targets"][0])  # type: ignore
            ]
            self.inject(
                f'run_action,type={action} run_name="{state.run.name}" {timestamp}'  # noqa: E501
            )
        elif action == "Acquiring":
            self.inject(
                f'run_action,type={action} run_name="{state.run.name}" {timestamp}'  # noqa: E501
            )
        elif action in ["Error", "Ended", "Aborted", "Stopped", "Starting"]:
            self.inject(
                f'run_action,type={action} run_name="{state.run.name}" {timestamp}'  # noqa: E501
            )
            asyncio.tasks.create_task(
                self.matrix_announce(
                    f"{self.config.machine.name} status: {action} {' '.join(str(x) for x in contents[1:])}"  # noqa: E501
                )
            )
            if action == "Ended":
                self.run_log_file = None

                if self.config.machine.compile:
                    assert state.run.name
                    compdir = self.config.sync.completed_directory

                    if compdir != "":
                        # This will need to compile and sync
                        asyncio.tasks.create_task(
                            self.sync_completed(c, state.run.name)
                        )
                    else:
                        # No sync; just compile
                        asyncio.tasks.create_task(self.compile_eds(c, state.run.name))
            elif action == "Starting":
                if self.ipdir:
                    newname: str = cast(str, contents[1])
                    newname.strip('"')

                    asyncio.tasks.create_task(
                        self.setup_new_rundir(
                            c,
                            newname,
                            firstmsg=f"\n{topic_str} {timestamp} {message_str}",
                        )
                    )

        elif action == "Collected":
            self.inject(
                f'run_action,type={action} run_name="{state.run.name}" {timestamp}'  # noqa: E501
            )
            asyncio.tasks.create_task(self.docollect(msg.opts, state, c))
        else:
            self.inject(
                Point("run_action")
                .tag("type", f"Other")
                .tag("run_name", state.run.name)
                .field("message", " ".join(str(x) for x in contents))
                .time(timestamp)
            )

        await state.run.refresh(c)
        await state.machine.refresh(c)

        log.info(message_str)
        self.inject(state.run.statemsg(str(timestamp)))

        if state.run.plate_setup:
            self.inject(
                state.run.plate_setup.to_lineprotocol(timestamp, state.run.name)
            )

        if self.idbw:
            self.idbw.flush()

    async def handle_led(
        self, topic: bytes, message: bytes, timestamp: float | None
    ) -> None:
        # Are we logging?
        if self.run_log_file is not None:
            self.run_log_file.write(f"{topic.decode()} {timestamp} {message.decode()}")
            self.run_log_file.flush()
        assert timestamp is not None
        ls = LEDSTATUS.match(message)
        assert ls
        p = (
            Point("lamp")
            .field("temperature", float(ls[1].decode()))
            .field("current", float(ls[2].decode()))
            .field("voltage", float(ls[3].decode()))
            .field("junctemp", float(ls[4].decode()))
            .time(int(1e9 * timestamp))
        )
        self.inject(p, flush=True)

    async def handle_msg(
        self,
        state: State,
        c: QSConnectionAsync,
        topic: bytes,
        message: bytes,
        timestamp: float | None,
    ) -> None:
        # Are we logging?
        if self.run_log_file is not None:
            self.run_log_file.write(f"{topic.decode()} {timestamp} {message.decode()}")
            self.run_log_file.flush()
        assert timestamp is not None
        args = ArgList.from_string(message.decode()).opts
        log.debug(f"Handling message {topic.decode()} {message.decode()}")
        if topic == b"Temperature":
            recs = []
            for i, (s, b, t) in enumerate(
                zip(
                    # FIXME: parsing weirdness: these are single-element tuples
                    [float(x) for x in cast(list[float], args["sample"][0])],  # type: ignore
                    [float(x) for x in cast(list[float], args["block"][0])],  # type: ignore
                    state.machine.zone_targets,
                )
            ):
                recs.append(
                    f"temperature,loc=zones,zone={i} sample={s},block={b},target={t} {int(1e9 * timestamp)}"  # noqa: E501
                )
            recs.append(
                Point("temperature").tag("loc", "cover").field("cover", args["cover"])
            )
            recs.append(
                Point("temperature")
                .tag("loc", "heatsink")
                .field("heatsink", args["heatsink"])
            )
            self.inject(recs)
        elif topic == b"Time":
            p = Point("run_time")
            for key in ["elapsed", "remaining", "active"]:
                if key in args.keys():
                    p = p.field(key, args[key])
                p.time(int(1e9 * timestamp))
            self.inject(p)
        if self.idbw:
            self.idbw.flush()

    async def monitor(self, connected_fut: asyncio.Future[bool] | None = None) -> None:

        if self.config.matrix is not None:
            await self.matrix_client.login(self.config.matrix.password)
            joinedroomresp = await self.matrix_client.joined_rooms()
            if isinstance(joinedroomresp, JoinedRoomsError):
                log.error(joinedroomresp)
                joinedrooms = []
            else:
                joinedrooms = joinedroomresp.rooms
            if self.config.matrix.room not in joinedrooms:
                await self.matrix_client.join(self.config.matrix.room)

        async with QSConnectionAsync(
            host=self.config.machine.host,
            port=int(self.config.machine.port),
            password=self.config.machine.password,
        ) as c:
            log.info("monitor connected")
            # Are we currently *in* a run? If so, we'll need to get info.
            state = await get_runinfo(c)
            log.info(f"status info: {state}")

            self.inject(state.run.statemsg(str(time.time_ns())))

            if state.run.plate_setup:
                self.inject(
                    state.run.plate_setup.to_lineprotocol(
                        time.time_ns(), state.run.name
                    )
                )

            if self.idbw:
                self.idbw.flush()

            # Setup directory if run already started:
            if state.run.name and self.ipdir:
                await self.setup_new_rundir(c, state.run.name, overwrite=True)

            await c.run_command("SUBS -timestamp Temperature Time Run LEDStatus")
            log.debug("subscriptions made")

            for t in [b"Temperature", b"Time"]:
                c._protocol.topic_handlers[t] = functools.partial(
                    self.handle_msg, state, c
                )
            c._protocol.topic_handlers[b"Run"] = functools.partial(
                self.handle_run_msg, state, c
            )

            c._protocol.topic_handlers[b"LEDStatus"] = self.handle_led

            log.info(c._protocol.topic_handlers)

            if connected_fut is not None:
                connected_fut.set_result(True)

            ok = True
            while ok:
                await asyncio.wait((c._protocol.lostconnection,), timeout=60)

                # Have we lost the connection?
                if c._protocol.lostconnection.done():
                    log.error("Lost connection.")
                    ok = False

                # Are we actually fine?
                if time.time() - c._protocol.last_received <= 60.0:
                    continue

                # No, we have a sleep timeout.  Send a test command.
                try:
                    await asyncio.wait_for(c.run_command("ISTAT?"), 30.0)
                except TimeoutError:
                    log.error(
                        "No data received in 5 minutes and ISTAT? test timed out.  Trying to disconnect."
                    )
                    await c.disconnect()
                    raise TimeoutError

    async def reliable_monitor(
        self, connected_fut: asyncio.Future[bool] | None = None
    ) -> None:
        log.info("starting reconnectable monitoring")

        restart = True
        successive_failures = 0
        while restart:
            try:
                await self.monitor(connected_fut=connected_fut)
            except asyncio.exceptions.TimeoutError as e:
                successive_failures = 0
                log.warn(f"lost connection with timeout {e}")
            except OSError as e:
                log.error(f"connectio error {e}, retrying")
            except Exception as e:
                if self.config.machine.retries - successive_failures > 0:
                    log.error(
                        f"Error {repr(e)}, retrying {self.config.machine.retries-successive_failures} times"  # noqa: E501
                    )
                    successive_failures += 1
                else:
                    log.critical(f"giving up, error {e}")
                    restart = False
            log.debug("awaiting retry")
            await asyncio.sleep(30)
