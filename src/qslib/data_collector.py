import time
import typing as tp
from typing import Union, Dict, Tuple, cast, List
from nio.client import AsyncClient
import re
import asyncio
from .qsconnection_async import QSConnectionAsync
import logging

from . import parser
from influxdb_client import InfluxDBClient, Point  # , Point, WritePrecision
from influxdb_client.client.write_api import ASYNCHRONOUS

log = logging.getLogger("monitor")


def parse_fd_fn(x: str) -> Tuple[str, int, int, int, int]:
    s = re.search(r"S(\d{2})_C(\d{3})_T(\d{2})_P(\d{4})_M(\d)_X(\d)_filterdata.xml$", x)
    assert s is not None
    return (f"x{s[6]}-m{s[5]}", int(s[1]), int(s[2]), int(s[3]), int(s[4]))


def index_to_filename_ref(i: Tuple[str, int, int, int, int]) -> str:
    x, s, c, t, p = i
    return f"S{s:02}_C{c:03}_T{t:02}_P{p:04}_M{x[4]}_X{x[1]}"


class Collector:
    def __init__(self, config):
        self.config = config

        self.idbclient = InfluxDBClient(
            url=self.config["influxdb"]["url"],
            token=self.config["influxdb"]["token"],
            org=self.config["influxdb"]["org"],
        )
        self.idbw = self.idbclient.write_api(write_options=ASYNCHRONOUS)

        self.matrix_client = AsyncClient(
            self.config["matrix"]["host"], self.config["matrix"]["user"]
        )

    def inject(self, t):
        self.idbw.write(bucket=self.config["influxdb"]["bucket"], record=t)

    async def matrix_announce(self, msg: str) -> None:
        await self.matrix_client.room_send(
            room_id=self.config["matrix"]["room"],
            message_type="m.room.message",
            content={"msgtype": "m.text", "body": msg},
        )

        await self.matrix_client.sync()

    async def docollect(
        self,
        args: Dict[str, Union[str, int]],
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

        run = cast(str, args["run"])[1:-1]
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
        msg = cast(dict[str, tp.Any], parser.arglist.parseString(message.decode()))
        contents = msg["contents"]
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
                f'run_action,type=Holding holdtime={msg["args"]["time"]} {timestamp}'  # noqa: E501
            )
        elif action == "Ramping":
            # TODO: check zones
            state.machine.zone_targets = msg["args"]["targets"]
            self.inject(
                f'run_action,type={action} run_name="{state.run.name}" {timestamp}'  # noqa: E501
            )
        elif action == "Acquiring":
            self.inject(
                f'run_action,type={action} run_name="{state.run.name}" {timestamp}'  # noqa: E501
            )
        elif action == "Starting":
            self.inject(
                f'run_action,type="{action}" run_name="{state.run.name}" {timestamp}'  # noqa: E501
            )
            asyncio.tasks.create_task(
                self.matrix_announce(
                    f"{self.config['machine']['name']} status: {action} {' '.join(contents[1:])}"  # noqa: E501
                )
            )
        elif action == "Ended":
            self.inject(
                f'run_action,type={action} run_name="{state.run.name}" {timestamp}'  # noqa: E501
            )
            asyncio.tasks.create_task(
                self.matrix_announce(
                    f"{self.config['machine']['name']} status: {action} {' '.join(contents[1:])}"  # noqa: E501
                )
            )
        elif action == "Error":
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
            asyncio.tasks.create_task(self.docollect(msg["args"], state, c))
        else:
            self.inject(
                Point("run_action")
                .tag("type", f"Other")
                .tag("run_name", state.run.name)
                .field("message", " ".join(contents))
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

    async def handle_msg(self, state, c, topic, msg, timestamp):
        timestamp = int(1e9 * timestamp)
        args = parser.arglist.parseString(msg).asDict()
        if topic == "Temperature":
            recs = []
            for i, (s, b, t) in enumerate(
                zip(
                    args["sample"],
                    args["block"],
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

            await c.run_command("SUBS -timestamp Temperature Time Run")
            log.debug("subscriptions made")

            for t in ["Temperature", "Time"]:
                c._protocol.topic_handlers[t] = lambda t, m, timestamp: self.handle_msg(
                    state, c, t, m, timestamp
                )
            c._protocol.topic_handlers[
                "Run"
            ] = lambda t, m, timestamp: self.handle_run_msg(state, c, t, m, timestamp)

            await c._protocol.lostconnection

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
                await asyncio.sleep(30)
            except Exception as e:
                if self.config["machine"]["retries"] - successive_failures > 0:
                    log.error(
                        f"non-timeout error {e}, retrying {self.config['machine']['retries']-successive_failures} times"  # noqa: E501
                    )
                    successive_failures += 1
                    await asyncio.sleep(30)
                else:
                    log.critical(f"giving up, error {e}")
