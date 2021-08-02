from __future__ import annotations
from glob import glob
import io
import os
from posixpath import join
from typing import Any, IO
from .machine import Machine, RunStatus
from .tcprotocol import Protocol
import tempfile
import zipfile
import base64
import logging
import xml.etree.ElementTree as ET
import re
import pandas as pd
from .data import FilterDataCollection, FilterSet, FilterDataReading

TEMPLATE_NAME = "ruo"


class NotRunningError(ValueError):
    pass


def _find_or_raise(e: ET.ElementTree | ET.Element, n: str) -> ET.Element:
    x = e.find(n)
    if x is None:
        raise ValueError(f"{n} not found in {x}.")
    else:
        return x


log = logging.getLogger("experiment")


class Experiment:
    protocol: Protocol
    title: str
    info: dict[str, Any]
    machine: Machine | None

    @property
    def runtitle_safe(self) -> str:
        return self.title.replace(" ", "-")

    def _ensure_machine(self, machine: Machine | None) -> Machine:
        if machine:
            return machine
        elif c := getattr(self, "machine", None):
            return c
        else:
            log.warn("creating new machine")
            self.machine = Machine(
                self.info["host"], self.info["port"], self.info["password"]
            )
            return self.machine

    def _ensure_running(self, machine: Machine):
        crt = machine.current_run_title
        if crt != self.runtitle_safe:
            raise ValueError(
                f"This experiment is {self.runtitle_safe}, machine is running {crt}."
            )

    def _populate_folder(self):
        machine = self._ensure_machine(None)
        tempzip = io.BytesIO()
        self.save_eds(tempzip)
        exppath = self.runtitle_safe + "/"
        with machine.at_access("Controller"):
            machine.run_command_bytes(
                b"EXP:ZIPWRITE "
                + exppath.encode()
                + b" <message>\n"
                + base64.encodebytes(tempzip.getvalue())
                + b"</message>\n"
            )

    def run(self, machine: Machine | None = None):
        machine = self._ensure_machine(machine)
        with machine.at_access("Controller"):

            # Ensure machine state and power.
            machine.power = True
            machine.drawer_close(lower_cover=True)

            # Check existence of previous run folder
            if self.runtitle_safe + "/" in machine.run_command("EXP:LIST?"):
                raise ValueError("Experiment folder already exists on machine.")

            # Create and populate new folder.
            machine.run_command(f"EXP:NEW {self.runtitle_safe} {TEMPLATE_NAME}")

            self._populate_folder()

            machine.define_protocol(self.protocol)

            # Start the run
            machine.run_command_to_ack(f"RP {self.runtitle_safe} {self.protocol.name}")

    def collect_finished(self, machine: Machine | None = None):
        machine = self._ensure_machine(machine)

        # Ensure run is actually done.
        raise NotImplementedError

    def pause_now(self, machine: Machine | None = None):
        """
        If this experiment is running, pause it (immediately).

        Requires and takes exclusive Controller access on the machine.

        Raises
        ------
        NotRunningError if the experiment is not currently running
        """
        machine = self._ensure_machine(machine)
        self._ensure_running(machine)
        with machine.at_access("Controller", exclusive=True):
            machine.pause_current_run()

    def resume(self, machine: Machine | None = None):
        """
        If this experiment is running, resume it.

        Requires and takes exclusive Controller access on the machine.

        Raises
        ------
        NotRunningError if the experiment is not currently running
        """
        machine = self._ensure_machine(machine)
        self._ensure_running(machine)
        with machine.at_access("Controller", exclusive=True):
            machine.resume_current_run()

    def stop(self, machine: Machine | None = None):
        """
        If this experiment is running, stop it after the end of the current cycle.

        Requires and takes exclusive Controller access on the machine.

        Raises
        ------
        NotRunningError if the experiment is not currently running
        """
        machine = self._ensure_machine(machine)
        self._ensure_running(machine)
        with machine.at_access("Controller", exclusive=True):
            machine.stop_current_run()

    def abort(self, machine: Machine | None = None):
        """
        If this experiment is running, abort it, stopping it immediately.

        Requires and takes exclusive Controller access on the machine.

        Raises
        ------
        NotRunningError if the experiment is not currently running
        """
        machine = self._ensure_machine(machine)
        self._ensure_running(machine)
        with machine.at_access("Controller", exclusive=True):
            machine.abort_current_run()

    def get_status(self, machine: Machine | None = None) -> RunStatus:

        """
        Return the status of the experiment, if currently running.

        Requires Observer access on the machine.

        Raises
        ------
        NotRunningError if the experiment is not currently running
        """
        machine = self._ensure_machine(machine)
        self._ensure_running(machine)
        return RunStatus.from_machine(machine)

    def sync_from_machine(self, machine: Machine | None = None):
        machine = self._ensure_machine(machine)
        self._ensure_running(machine)
        raise NotImplementedError

    def change_protocol(self, protocol, machine: Machine | None = None):
        machine = self._ensure_machine(machine)
        self._ensure_running(machine)
        raise NotImplementedError

    def save_eds(self, filename_or_io, overwrite=False):
        if overwrite:
            mode = "w"
        else:
            mode = "x"

        self.update_files()

        with zipfile.ZipFile(filename_or_io, mode) as z:
            for root, subs, files in os.walk(self._dir_base):
                for file in files:
                    fpath = os.path.join(root, file)
                    z.write(fpath, os.path.relpath(fpath, self._dir_base))

    def __init__(self):
        self._tmp_dir_obj = tempfile.TemporaryDirectory()
        self._dir_base = self._tmp_dir_obj.name
        self._dir_eds = os.path.join(self._dir_base, "apldbio", "sds")

    def update_files(self):
        self._update_experiment_xml()
        self._update_tcprotocol_xml()
        self._update_platesetup_xml()

    def update_from_files(self):
        self._update_from_experiment_xml()
        self._update_from_tcprotocol_xml()
        self._update_from_platesetup_xml()
        self._update_from_log()
        self._update_data()
        # TODO: data

    @classmethod
    def from_eds(cls, file: str | os.PathLike[str] | IO[bytes]) -> "Experiment":
        exp = cls()

        with zipfile.ZipFile(file) as z:
            # Ensure that this actually looks like an EDS file:
            try:
                z.getinfo("apldbio/sds/experiment.xml")
            except KeyError:
                raise ValueError(f"{file} does not appear to be an EDS file.") from None

            z.extractall(exp._dir_base)

        exp.update_from_files()

        return exp

    @classmethod
    def from_running_experiment(cls, machine: Machine):
        exp = cls()

        crt = machine.current_run_title

        z = machine.read_dir_as_zip(crt, leaf="EXP")

        z.extractall(exp._dir_base)

        exp.update_from_files()

        return exp

    def _update_experiment_xml(self):
        exml = ET.parse(os.path.join(self._dir_eds, "experiment.xml"))

        _find_or_raise(exml, "Name").text = self.runtitle
        _find_or_raise(exml, "Operator").text = self.user

        exml.write(os.path.join(self._dir_eds, "experiment.xml"))

    def _update_from_experiment_xml(self):
        exml = ET.parse(os.path.join(self._dir_eds, "experiment.xml"))

        self.runtitle = _find_or_raise(exml, "Name").text
        self.user = _find_or_raise(exml, "Operator").text

    def _update_tcprotocol_xml(self):
        if self.protocol:
            exml = ET.ElementTree(self.protocol.to_xml())
            exml.write(os.path.join(self._dir_eds, "tcprotocol.xml"))

    def _update_from_tcprotocol_xml(self):
        exml = ET.parse(os.path.join(self._dir_eds, "tcprotocol.xml"))

        if (x := exml.find("QSLibProtocol")) is not None:
            self.protocol = Protocol.from_command(x.text)
        else:
            try:
                self.protocol = Protocol.from_xml(exml.getroot())
            except:
                self.protocol = None

    def _update_platesetup_xml(self):
        exml = ET.ElementTree(ET.Element("Plate"))
        exml.write(os.path.join(self._dir_eds, "plate_setup.xml"))

    def _update_from_platesetup_xml(self):
        pass
        # raise NotImplementedError

    def _update_data(self):
        fdp = os.path.join(self._dir_eds, "filterdata.xml")
        if os.path.isfile(fdp):
            fdx = ET.parse(fdp)
            fdrs = [
                FilterDataReading(x, sds_dir=self._dir_eds)
                for x in fdx.findall(".//PlateData")
            ]
            self.fdc = FilterDataCollection.from_readings(fdrs)
        elif fdfs := glob(os.path.join(self._dir_eds, "filter", "*_filterdata.xml")):
            fdrs = [
                FilterDataReading(
                    ET.parse(fdf).find(".//PlateData"), sds_dir=self._dir_eds
                )
                for fdf in fdfs
            ]
            self.fdc = FilterDataCollection.from_readings(fdrs)
        else:
            self.fdc = None

    def _update_from_log(self):
        if not os.path.isfile(os.path.join(self._dir_eds, "messages.log")):
            return
        log = open(os.path.join(self._dir_eds, "messages.log")).read()

        if m := re.search(r'^Run ([\d.]+) Starting "([^"]+)"$', log, re.MULTILINE):
            self.experiment_start = float(m[1])
        else:
            return

        tt = []

        for m in re.finditer(
            r"^Temperature ([\d.]+) -sample=([\d.,]+) -heatsink=([\d.,]+) "
            r"-cover=([\d.,]+) -block=([\d.,]+)$",
            log,
            re.MULTILINE,
        ):
            r = []
            r.append(float(m[1]) - self.experiment_start)
            r += [float(x) for x in m[2].split(",")]
            r.append(float(m[3]))
            r.append(float(m[4]))
            r += [float(x) for x in m[5].split(",")]
            tt.append(r)

        self.num_zones = int((len(tt[0]) - 3) / 2)

        self.temperatures = pd.DataFrame(
            tt,
            columns=pd.MultiIndex.from_tuples(
                [("time", "seconds")]
                + [("sample", n) for n in range(1, self.num_zones + 1)]
                + [("heatsink", "heatsink"), ("cover", "cover")]
                + [("block", n) for n in range(1, self.num_zones + 1)]
            ),
        )

        self.temperatures[("time", "hours")] = (
            self.temperatures[("time", "seconds")] / 3600.0
        )
        self.temperatures[("sample", "avg")] = self.temperatures["sample"].mean(axis=1)
        self.temperatures[("block", "avg")] = self.temperatures["block"].mean(axis=1)

    @property
    def rawdata(self):
        if self.fdc is not None:
            return _fdc_to_rawdata(self.fdc, self.experiment_start)  # type: ignore
        else:
            return None


def _redo_wells(wn: str) -> str:
    "change fluorescent well name format"
    return f"{wn[2]}{int(wn[3:])}"


def _fdc_to_rawdata(fdc: FilterDataCollection, start_time: float) -> pd.DataFrame:
    ret: pd.DataFrame = fdc.loc[:, slice("f_A01", "f_H12")].copy()
    ret.rename(columns=_redo_wells, inplace=True)
    for i in range(0, 6):
        ret[f"temperature_{i+1}"] = fdc[f"tr_A{(i+1)*2:02}"]
    ret["temperature_avg"] = ret.loc[:, slice("temperature_1", "temperature_6")].mean(
        axis=1
    )
    ret["timestamp"] = fdc["timestamp"]
    ret["exptime"] = ret["timestamp"] - start_time
    ret["exphrs"] = (ret["timestamp"] - start_time) / 60 / 60
    # ret['time'] = ret['time'].astype('datetime64[s]') # fixme: should we?

    return ret
