from __future__ import annotations
from dataclasses import dataclass
import time
import typing as tp
from typing import Union, Dict, Tuple, cast, List, Optional
from nio.client import AsyncClient
import re
import asyncio

from nio.client.async_client import AsyncClientConfig

from qslib.qs_is_protocol import CommandError
from .parser import arglist

from qslib.plate_setup import PlateSetup
from qslib.qsconnection_async import QSConnectionAsync
import logging

from influxdb_client import InfluxDBClient, Point  # , Point, WritePrecision
from influxdb_client.client.write_api import ASYNCHRONOUS

log = logging.getLogger("monitor")

LEDSTATUS = re.compile(
    rb"Temperature:([+\-\d.]+) Current:([+\-\d.]+) Voltage:([+\-\d.]+) JuncTemp:([+\-\d.]+)"
)


@dataclass
class RunState:
    name: Optional[str] = None
    stage: Optional[int] = None
    cycle: Optional[int] = None
    step: Optional[int] = None
    plate_setup: Optional[PlateSetup] = None

    async def refresh(self, c: QSConnectionAsync):
        runmsg = arglist.parseString(await c.run_command("RunProgress?")).asDict()
        print(runmsg)
        self.name = cast(str, runmsg["arglist"]["opts"]["RunTitle"])
        if self.name == "-":
            self.name = None
        self.stage = cast(int, runmsg["arglist"]["opts"]["Stage"])
        if self.stage == "-":
            self.stage = None
        self.cycle = cast(int, runmsg["arglist"]["opts"]["Cycle"])
        if self.cycle == "-":
            self.cycle = None
        self.step = cast(int, runmsg["arglist"]["opts"]["Step"] if self.stage else None)
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
        targmsg = arglist.parseString(await c.run_command("TBC:SETT?"))
        self.cover_target = cast(float, targmsg["arglist"]["opts"]["Cover"])
        self.zone_targets = cast(
            List[float], [targmsg["arglist"]["opts"][f"Zone{i}"] for i in range(1, 7)]
        )

        contmsg = arglist.parseString(await c.run_command("TBC:CONT?"))
        self.cover_control = cast(bool, contmsg["arglist"]["opts"]["Cover"])
        self.zone_controls = cast(
            List[bool], [contmsg["arglist"]["opts"][f"Zone{i}"] for i in range(1, 7)]
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


def parse_fd_fn(x: str) -> Tuple[str, int, int, int, int]:
    s = re.search(r"S(\d{2})_C(\d{3})_T(\d{2})_P(\d{4})_M(\d)_X(\d)_filterdata.xml$", x)
    assert s is not None
    return (f"x{s[6]}-m{s[5]}", int(s[1]), int(s[2]), int(s[3]), int(s[4]))


def index_to_filename_ref(i: Tuple[str, int, int, int, int]) -> str:
    x, s, c, t, p = i
    return f"S{s:02}_C{c:03}_T{t:02}_P{p:04}_M{x[4]}_X{x[1]}"


async def get_runinfo(c: QSConnectionAsync):
    state = await State.from_machine(c)
    return state


class Collector:
    def __init__(self, config):
        self.config = config

        self.idbclient = InfluxDBClient(
            url=self.config["influxdb"]["url"],
            token=self.config["influxdb"]["token"],
            org=self.config["influxdb"]["org"],
        )
        self.idbw = self.idbclient.write_api(write_options=ASYNCHRONOUS)

        self.matrix_config = AsyncClientConfig(
            #encryption_enabled=True
        )

        self.matrix_client = AsyncClient(
            self.config["matrix"]["host"], self.config["matrix"]["user"],
            store_path='./matrix_store/', config=self.matrix_config
        )

    def inject(self, t):
        self.idbw.write(bucket=self.config["influxdb"]["bucket"], record=t)

    async def matrix_announce(self, msg: str) -> None:
        await self.matrix_client.room_send(
            room_id=self.config["matrix"]["room"],
            message_type="m.room.message",
            content={"msgtype": "m.text", "body": msg},
            ignore_unverified_devices=True
        )

        await self.matrix_client.sync()

    async def docollect(
        self,
        args: Dict[str, Union[str, int]],
        state: State,
        connection: QSConnectionAsync,
    ) -> None:
        if state.run.plate_setup:
            pa = state.run.plate_setup.well_samples_as_array()
        else:
            pa = None

        if state.run.name:
            name = state.run.name
        else:
            name = None

        run = cast(str, args["run"])
        del args["run"]
        for k, v in args.items():
            if k != "run":
                args[k] = int(v)
        pl = [
            parse_fd_fn(x)
            for x in await connection.get_expfile_list(
                "{run}/apldbio/sds/filter/S{stage:02}_C{cycle:03}"
                "_T{step:02}_P{point:04}_*_filterdata.xml".format(
                    run=run, **cast(Dict[str, int], args)
                )
            )
        ]
        pl.sort()
        toget = [x for x in pl if x[1:] == pl[-1][1:]]
        lp: List[str] = sum(
            [
                (await connection.get_filterdata_one(*x)).to_lineprotocol(
                    run_name=name, sample_array=pa
                )
                for x in toget
            ],
            [],
        )

        self.idbw.write(bucket=self.config["influxdb"]["bucket"], record=lp)
        self.idbw.flush()

    async def handle_run_msg(self, state, c, topic, message, timestamp):
        timestamp = int(1e9 * timestamp)
        msg = arglist.parseString(message)
        log.debug(msg)
        contents = msg["arglist"]["args"]
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
                f'run_action,type=Holding holdtime={msg["arglist"]["opts"]["time"]} {timestamp}'  # noqa: E501
            )
        elif action == "Ramping":
            # TODO: check zones
            state.machine.zone_targets = msg["arglist"]["opts"]["targets"]
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
                    f"{self.config['machine']['name']} status: {action} {' '.join(contents[1:])}"  # noqa: E501
                )
            )
        elif action == "Collected":
            self.inject(
                f'run_action,type={action} run_name="{state.run.name}" {timestamp}'  # noqa: E501
            )
            asyncio.tasks.create_task(self.docollect(msg["arglist"]["opts"], state, c))
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

    async def handle_led(self, topic, msg, timestamp):
        timestamp = int(1e9 * timestamp)
        vs = [float(x) for x in LEDSTATUS.match(msg).groups()]
        ks = ["temperature", "current", "voltage", "junctemp"]
        p = Point("lamp")
        for k, v in zip(ks, vs):
            p = p.field(k, v)
        p = p.time(timestamp)
        self.inject(p)
        self.idbw.flush()

    async def handle_msg(self, state, c, topic, msg, timestamp):
        timestamp = int(1e9 * timestamp)
        args = arglist.parseString(msg)["arglist"]["opts"]
        log.debug(f"Handling message {topic} {msg}")
        if topic == "Temperature":
            recs = []
            for i, (s, b, t) in enumerate(
                zip(
                    [float(x) for x in args["sample"].split(",")],
                    [float(x) for x in args["block"].split(",")],
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
            log.info(msg)

    async def monitor(self):
        await self.matrix_client.login(self.config["matrix"]["password"])

        async with QSConnectionAsync(
            host=self.config["machine"]["host"],
            port=self.config["machine"]["port"],
            password=self.config["machine"]["password"],
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

            await c.run_command("SUBS -timestamp Temperature Time Run LEDStatus")
            log.debug("subscriptions made")

            for t in [b"Temperature", b"Time"]:
                c._protocol.topic_handlers[t] = lambda t, m, timestamp: self.handle_msg(
                    state, c, t.decode(), m.decode(), timestamp
                )
            c._protocol.topic_handlers[
                b"Run"
            ] = lambda t, m, timestamp: self.handle_run_msg(
                state, c, t.decode(), m.decode(), timestamp
            )

            c._protocol.topic_handlers[b"LEDStatus"] = self.handle_led

            log.info(c._protocol.topic_handlers)

            await c._protocol.lostconnection
            log.error("Lost connection")

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
                if self.config["machine"].get("retries", 3) - successive_failures > 0:
                    log.error(
                        f"Error {e}, retrying {self.config['machine']['retries']-successive_failures} times"  # noqa: E501
                    )
                    successive_failures += 1
                else:
                    log.critical(f"giving up, error {e}")
                    restart = False
            log.debug("awaiting retry")
            await asyncio.sleep(15)
