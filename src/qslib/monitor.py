from __future__ import annotations
from dataclasses import dataclass
import os
import time
from typing import TextIO, Union, Dict, Tuple, cast, List, Optional, Any
import zipfile
from nio.client import AsyncClient
import re
import asyncio
from pathlib import Path
import shutil

from nio.client.async_client import AsyncClientConfig
from nio.responses import JoinedRoomsError
from qslib.base import AccessLevel

from qslib.qs_is_protocol import CommandError
from .parser import ArgList

from qslib.plate_setup import PlateSetup
from qslib.qsconnection_async import FilterDataFilename, QSConnectionAsync

import logging

from influxdb_client import InfluxDBClient, Point  # , Point, WritePrecision
from influxdb_client.client.write_api import ASYNCHRONOUS

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
    password: str
    name: str
    host: str
    port: str
    retries: int
    compile: bool = False


@dataclass(frozen=True)
class SyncConfig:
    completed_directory: Union[str, None] = None
    in_progress_directory: Union[str, None] = None


@dataclass(frozen=True)
class Config:
    matrix: Union[MatrixConfig, None]
    influxdb: InfluxConfig
    machine: MachineConfig
    sync: SyncConfig


@dataclass
class RunState:
    name: Optional[str] = None
    stage: Optional[int] = None
    cycle: Optional[int] = None
    step: Optional[int] = None
    plate_setup: Optional[PlateSetup] = None

    async def refresh(self, c: QSConnectionAsync):
        runmsg = ArgList(await c.run_command("RunProgress?"))
        print(runmsg)
        self.name = cast(str, runmsg.opts["RunTitle"])
        if self.name == "-":
            self.name = None
        self.stage = cast(int, runmsg.opts["Stage"])
        if self.stage == "-":
            self.stage = None
        self.cycle = cast(int, runmsg.opts["Cycle"])
        if self.cycle == "-":
            self.cycle = None
        self.step = cast(int, runmsg.opts["Step"] if self.stage else None)
        if self.step == "-":
            self.step = None
        if self.name:
            try:
                self.plate_setup = await PlateSetup.from_machine(c)
            except CommandError:
                self.plate_setup = None

    @classmethod
    async def from_machine(cls, c: QSConnectionAsync):
        n = cls.__new__(cls)
        await n.refresh(c)
        return n

    def statemsg(self, timestamp):
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

    async def refresh(self, c: QSConnectionAsync):
        targmsg = ArgList(await c.run_command("TBC:SETT?"))
        self.cover_target = cast(float, targmsg.opts["Cover"])
        self.zone_targets = cast(
            List[float], [targmsg.opts[f"Zone{i}"] for i in range(1, 7)]
        )

        contmsg = ArgList(await c.run_command("TBC:CONT?"))
        self.cover_control = cast(bool, contmsg.opts["Cover"])
        self.zone_controls = cast(
            List[bool], [contmsg.opts[f"Zone{i}"] for i in range(1, 7)]
        )

        self.drawer = await c.run_command("DRAW?")

    @classmethod
    async def from_machine(cls, c: QSConnectionAsync):
        n = cls.__new__(cls)
        await n.refresh(c)
        return n

    # def targetmsg(timestamp):
    #    return ""


@dataclass
class State:
    run: RunState
    machine: MachineState

    @classmethod
    async def from_machine(cls, c: QSConnectionAsync):
        n = cls.__new__(cls)
        n.run = await RunState.from_machine(c)
        n.machine = await MachineState.from_machine(c)
        return n


# def parse_fd_fn(x: str) -> Tuple[str, int, int, int, int]:
#    s = re.search(r"S(\d{2})_C(\d{3})_T(\d{2})_P(\d{4})_M(\d)_X(\d)_filterdata.xml$", x)
#    assert s is not None
#    return (f"x{s[6]}-m{s[5]}", int(s[1]), int(s[2]), int(s[3]), int(s[4]))


def index_to_filename_ref(i: Tuple[str, int, int, int, int]) -> str:
    x, s, c, t, p = i
    return f"S{s:02}_C{c:03}_T{t:02}_P{p:04}_M{x[4]}_X{x[1]}"


async def get_runinfo(c: QSConnectionAsync):
    state = await State.from_machine(c)
    return state


class Collector:
    def __init__(self, config: Config):
        self.config = config

        self.idbclient = InfluxDBClient(
            url=self.config.influxdb.url,
            token=self.config.influxdb.token,
            org=self.config.influxdb.org,
        )
        self.idbw = self.idbclient.write_api(write_options=ASYNCHRONOUS)

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

    def inject(self, t):
        self.idbw.write(bucket=self.config.influxdb.bucket, record=t)

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
    ):
        name = name.replace(" ", "_")

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
        name = name.replace(" ", "_")
        if (ipdir := self.ipdir) is None:
            raise ValueError
        return ipdir / name / "apldbio" / "sds"

    async def compile_eds(self, connection: QSConnectionAsync, name: str):
        name = name.replace(" ", "_")

        try:
            await connection.set_access_level(AccessLevel.Controller)
            await connection.compile_eds(name)
        finally:
            await connection.set_access_level(AccessLevel.Observer)

    async def sync_completed(self, connection: QSConnectionAsync, name: str):
        name = name.replace(" ", "_")

        await self.compile_eds(connection, name)

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
                edsfile = await connection.get_file(f"public_run_complete:{name}.eds")
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
            pa = state.run.plate_setup.well_samples_as_array()
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
                )
            )
        ]
        pl.sort()
        toget = [x for x in pl if x.is_same_point(pl[-1])]

        lp: List[str] = []
        files: list[tuple[str, bytes]] = []

        if (
            self.ipdir
            and (
                self.ipdir / run.replace(" ", "_") / "apldbio" / "sds" / "filter"
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

        self.idbw.write(bucket=self.config.influxdb.bucket, record=lp)
        self.idbw.flush()

        for path, data in files:
            fullpath = self.run_ip_path(run) / path
            with fullpath.open("wb") as f:
                f.write(data)

        if (
            self.ipdir
            and (
                self.ipdir / run.replace(" ", "_") / "apldbio" / "sds" / "filter"
            ).exists()
        ):
            saferun = run.replace(" ", "_")
            ipp = self.ipdir / saferun
            with zipfile.ZipFile(self.ipdir / (saferun + ".eds"), "w") as z:
                for root, subs, zfiles in os.walk(ipp):
                    for zfile in zfiles:
                        fpath = os.path.join(root, zfile)
                        z.write(fpath, os.path.relpath(fpath, ipp))

    async def handle_run_msg(
        self,
        state,
        c: QSConnectionAsync,
        topic: str,
        message: str,
        timestamp: float | None,
    ):

        # Are we logging?
        if self.run_log_file is not None:
            self.run_log_file.write(f"{topic} {timestamp} {message}")
            self.run_log_file.flush()

        assert timestamp is not None
        timestamp = int(1e9 * timestamp)
        msg = ArgList(message)
        log.debug(msg)
        contents = msg.args
        action = cast(str, contents[0])
        if action == "Stage":
            state.run.stage = contents[1]
            self.inject(
                Point("run_action")
                .tag("type", action.lower())
                .field(action.lower(), contents[1])
                .time(timestamp)
                .to_line_protocol()
            )
            self.inject(
                f"run_status,type={action.lower()} {action.lower()}={contents[1]}i {timestamp}"  # noqa: E501
            )
        elif action == "Cycle":
            state.run.cycle = contents[1]
            self.inject(
                Point("run_action")
                .tag("type", action.lower())
                .field(action.lower(), contents[1])
                .time(timestamp)
                .to_line_protocol()
            )
            self.inject(
                f"run_status,type={action.lower()} {action.lower()}={contents[1]}i {timestamp}"  # noqa: E501
            )
        elif action == "Step":
            state.run.step = contents[1]
            self.inject(
                f"run_status,type={action.lower()} {action.lower()}={contents[1]}i {timestamp}"  # noqa: E501
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
            state.machine.zone_targets = msg.opts["targets"]
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

                if cast(dict, self.config).get("machine", {}).get("compile", False):
                    compdir = (
                        cast(dict, self.config)
                        .get("sync", {})
                        .get("completed_directory", "")
                    )
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
                            firstmsg=f"\n{topic} {timestamp} {message}",
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

        log.info(message)
        self.inject(state.run.statemsg(timestamp))

        if state.run.plate_setup:
            self.inject(
                state.run.plate_setup.to_lineprotocol(timestamp, state.run.name)
            )

        self.idbw.flush()

    async def handle_led(self, topic: bytes, message: bytes, timestamp: float | None):
        # Are we logging?
        if self.run_log_file is not None:
            self.run_log_file.write(f"{topic.decode()} {timestamp} {message.decode()}")
            self.run_log_file.flush()
        assert timestamp is not None
        ls = LEDSTATUS.match(message)
        assert ls
        p = (
            Point("lamp")
            .field("temperature", ls[1])
            .field("current", ls[2])
            .field("voltage", ls[3])
            .field("junctemp", ls[4])
            .time(int(1e9 * timestamp))
        )
        self.inject(p)
        self.idbw.flush()

    async def handle_msg(
        self, state, c, topic: str, message: str, timestamp: float | None
    ):
        # Are we logging?
        if self.run_log_file is not None:
            self.run_log_file.write(f"{topic} {timestamp} {message}")
            self.run_log_file.flush()
        assert timestamp is not None
        timestamp = int(1e9 * timestamp)
        args = ArgList(message).opts
        log.debug(f"Handling message {topic} {message}")
        if topic == "Temperature":
            recs = []
            for i, (s, b, t) in enumerate(
                zip(
                    [float(x) for x in cast(str, args["sample"]).split(",")],
                    [float(x) for x in cast(str, args["block"]).split(",")],
                    state.machine.zone_targets,
                )
            ):
                recs.append(
                    f"temperature,loc=zones,zone={i} sample={s},block={b},target={t} {timestamp}"  # noqa: E501
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
        elif topic == "Time":
            p = Point("run_time")
            for t in ["elapsed", "remaining", "active"]:
                if t in args.keys():
                    p = p.field(t, args[t])
                p.time(timestamp)
            self.inject(p)
        self.idbw.flush()
        if topic not in ["Temperature", "Time", "Run", "LEDStatus"]:
            log.info(message)

    async def monitor(self):

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

            self.inject(state.run.statemsg(time.time_ns()))

            if state.run.plate_setup:
                self.inject(
                    state.run.plate_setup.to_lineprotocol(
                        time.time_ns(), state.run.name
                    )
                )

            self.idbw.flush()

            # Setup directory if run already started:
            if state.run.name and self.ipdir:
                await self.setup_new_rundir(c, state.run.name, overwrite=True)

            await c.run_command("SUBS -timestamp Temperature Time Run LEDStatus")
            log.debug("subscriptions made")

            for t in [b"Temperature", b"Time"]:
                c._protocol.topic_handlers[
                    t
                ] = lambda topic, message, timestamp: self.handle_msg(
                    state, c, topic.decode(), message.decode(), timestamp
                )
            c._protocol.topic_handlers[
                b"Run"
            ] = lambda topic, message, timestamp: self.handle_run_msg(
                state, c, topic.decode(), message.decode(), timestamp
            )

            c._protocol.topic_handlers[b"LEDStatus"] = self.handle_led

            log.info(c._protocol.topic_handlers)

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

    async def reliable_monitor(self):
        log.info("starting reconnectable monitoring")

        restart = True
        successive_failures = 0
        while restart:
            try:
                await self.monitor()
            except asyncio.exceptions.TimeoutError as e:
                successive_failures = 0
                log.warn(f"lost connection with timeout {e}")
            except OSError as e:
                log.error(f"connectio error {e}, retrying")
            except Exception as e:
                if self.config.machine.retries - successive_failures > 0:
                    log.error(
                        f"Error {e}, retrying {self.config.machine.retries-successive_failures} times"  # noqa: E501
                    )
                    successive_failures += 1
                else:
                    log.critical(f"giving up, error {e}")
                    restart = False
            log.debug("awaiting retry")
            await asyncio.sleep(30)
