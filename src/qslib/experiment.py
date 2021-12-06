from __future__ import annotations

import base64
import dataclasses
import io
import logging
import os
import re
import tempfile
import xml.etree.ElementTree as ET
import zipfile
from dataclasses import InitVar, dataclass, field
from datetime import datetime
from glob import glob
from typing import IO, Any, Collection, Literal, Mapping, Sequence, Union, cast
from pathlib import Path

import numpy as np

from warnings import warn

from pandas.core.base import DataError

from .rawquant_compat import _fdc_to_rawdata

import matplotlib.pyplot as plt

import pandas as pd
import toml as toml
from qslib.base import AccessLevel
from qslib.plate_setup import PlateSetup

from .data import FilterSet, df_from_readings, FilterDataReading
from .machine import Machine, RunStatus
from .tcprotocol import Protocol, Stage, Step
from .util import *
from ._version import version as __version__  # type: ignore
from .normalization import Normalizer, NormRaw

TEMPLATE_NAME = "ruo"

MANIFEST_CONTENTS = f"""Manifest-Version: 1.0
Content-Type: Std
Implementation-Title: QSLib
Implementation-Version: {__version__}
Specification-Vendor: Applied Biosystems
Specification-Title: Experiment Document Specification
Specification-Version: 1.3.2
"""

log = logging.getLogger(__name__)


class AlreadyExistsError(ValueError):
    pass


@dataclass
class MachineError(Exception):
    host: str = field(init=False)
    port: int | str = field(init=False)
    machine: InitVar[Machine]

    def __post_init__(self, machine: Machine) -> None:
        self.host = machine.host
        self.port = machine.port


@dataclass
class NotRunningError(MachineError):
    run: str
    current: RunStatus

    def __str__(self) -> str:
        return (
            f"{self.run} is not running on {self.host}."
            f" Current status is {self.current}."
        )


@dataclass
class MachineBusyError(MachineError):
    current: RunStatus

    def __str__(self) -> str:
        return f"Machine {self.host}:{self.port} is currently busy: {self.current}."


@dataclass
class RunAlreadyExistsError(MachineError):
    name: str

    def __str__(self) -> str:
        return (
            f"Run {self.name} exists in machine working directory. This may be caused by "
            "a failed previous run.  TODO: write cleanup code."
        )


def _find_or_raise(e: ET.ElementTree | ET.Element, n: str) -> ET.Element:
    x = e.find(n)
    if x is None:
        raise ValueError(f"{n} not found in {x}.")
    else:
        return x


_TABLE_FORMAT = {"markdown": "pipe", "org": "orgtbl"}


class Experiment:
    """A QuantStudio experiment / EDS file

    This class can create, modify, load and save experiments in several ways, run them,
    and control and modify them while running.

    Experiments can be loaded from a file:

    >>> exp: Experiment = Experiment.from_file("experiment.eds")

    They can also be loaded from a running experiment:

    >>> machine = Machine("localhost", 7000, password="password")
    >>> exp = Experiment.from_running(machine)

    Or from the machine's storage:

    >>> exp = Experiment.from_machine_storage(machine, "experiment.eds")

    They can also be created from scratch:

    >>> exp = Experiment("an-experiment-name")
    >>> exp.protocol = Protocol([Stage([Step(time=60, temperature=60)])])
    >>> exp.plate_setup = PlateSetup({"sample_name": "A5"})

    And they can be run on a machine:

    >>> exp.run(machine)

    Data can be accessed in a few ways:

    The (hopefully) easiest way is with welldata, which has multi-indexes
    for both rows and columns.

    >>> exp.welldata.loc[('x1-m4', 4), [('time', 'hours'), ('A05', 'fl')]].plot()

    Or for a temperature curve:

    >>> exp.welldata.loc[('x1-m4', 4), [('A05', 'st'), ('A05', 'fl')]].plot()

    filterdata should still work normally.

    Notes
    -----

    There are a few differences in how QSLib and AB's software handles experiments.

    * AB's software considers the run as starting when the machine indicates "Run Starting".
      This is stored as :any:`runstarttime` in QSLib, but as it may include the lamp warmup
      (3 minutes) and other pre-actual-protocol time, QSLib instead prefers :any:`activestarttime`,
      which it sets from the beginning of the first real (not PRERUN) Stage, at which point
      the machine starts its own active clock and starts ramping to the first temperature.
      QSLib uses this as the start time reference in its data, and also includes the
      timestamp from the machine.
    * The machine has a specific language for run protocols. QSLib uses this language.
      AB's Design and Analysis software *does not*, instead using an XML format. Not
      everything in the machine's language is possible to express in the XML format (eg,
      disabling pcr analysis, saving images); the XML format has some concepts not present in
      the machine format, and is generally more complicated and harder to understand. QSLib uses
      and trusts the machine's protocol if at all possible, *even for files written by
      AB D&A* (it is stored in the log if the run has started).
    * QSLib will try to write a reasonable XML protocol for AB D&A to see, but it may
      by an approximation or simply wrong, if the actual protocol can't be expressed there.
      It will also store the actual protocol in tcprotocol.xml, and its own representation.
    * By default, creating a step with a per-cycle increment in QSLib starts the change
      *on cycle 2*, not cycle 1, as is the default in the software.
    * Immediate pause/resume, mid-run stage addition, and other functions are not supported
      by AB D&A and experiments using them may confuse the software later.
    * QSLib writes notes to XML files, and tries to create reasonable XML files for
      AB D&A, but may still cause problems. At the moment, it makes clear that its files
      are its own (setting software versions in experiment.xml and Manifest.mf).
    """

    protocol: Protocol
    """
    Temperature and reading protocol for the experiment.
    """
    name: str
    """
    Experiment name, also used as file name on the machine.
    """
    runstarttime: datetime | None
    """
    The run start time as a datetime. This is taken *directly from the log*, ignoring the software-set
    value and replacing it on save if possibe.  It is defined as the moment the machine
    records "Run Starting" in its log, using its timestamp.  This may be 3 minutes before
    the start of the protocol if the lamp needs to warm up.  It should be the same value as defined by
    AB's software.

    Use :any:`activestarttime` for a more accurate value.

    None if the file has not been updated since the start of the run
    """
    runendtime: datetime | None
    """
    The run end time as a datetime, taken from the log.  This is the end of the run, 
    """
    activestarttime: datetime | None
    """
    The actual beginning of the first stage of the run, defined as the first "Run Stage" message
    in the log after "Stage PRERUN". This is *not* what AB's software considers the start of a run.
    """
    activeendtime: datetime | None
    """
    The actual end of the main part of the run, indicated by "Stage POSTRun" or an abort.
    """
    createdtime: datetime
    """
    The run creation time.
    """
    modifiedtime: datetime
    """
    The last modification time. QSLib sets this on write. AB D&A may not.
    """
    runstate: Literal["INIT", "RUNNING", "COMPLETE", "ABORTED", "STOPPED", "UNKNOWN"]
    """
    Run state, possible values INIT, RUNNING, COMPLETE, ABORTED, STOPPED(?).
    """
    machine: Machine | None
    plate_setup: PlateSetup
    """
    Plate setup for the experiment.
    """
    writesoftware: str
    """
    A string describing the software and version used to write the file.
    """
    _welldata: pd.DataFrame | None

    temperatures: pd.DataFrame | None
    """
    A DataFrame of temperature readings, at one second resolution, during the experiment
    (and potentially slightly before and after, if included in the message log).

    Columns (as multi-index):

    ("time", ...) : float
        Time of temperature reading, for choices of "timestamp" (Unix timestamp in seconds), 
        "seconds" (seconds since the *active* start of the run), or "hours". The latter two may 
        be negative, and may not be set if the run never became active.

    ("sample", ...) : float
        Sample temperature for blocks 1, 2, ..., 6, and average in "avg".

    ("block", ...) : float
        Block temperature for blocks 1, 2, ..., 6, and average in "avg".

    ("other", "cover") : float
        Cover temperature

    ("other", "heatsink") : float
        Heatsink temperature

    """

    @property
    def all_filters(self) -> Collection[FilterSet]:
        return self.protocol.all_filters

    @property
    def welldata(self) -> pd.DataFrame:
        """
        A DataFrame with fluorescence reading information.

        Indices (multi-index) are (filter_set, stage, cycle, step, point), where filter_set
        is a string in familiar form (eg, "x1-m4") and the rest are int.

        Columns (as multi-index):

        ("time", ...) : float
            Time of the data collection, taken from the .quant file.  May differ for different
            filter sets.  Options are "timestamp" (unix timestamp in seconds), "seconds", and
            "hours" (the latter two from the *active* start of the run).

        (well, option) : float
            Data for a well, with well formatted like "A05".  Options are "rt" (read temperature
            from .quant file), "st" (more stable temperature), and "fl" (fluorescence).

        ("exposure", "exposure") : float
            Exposure time from filterdata.xml.  Misleading, because it only refers to the
            longest exposure of multiple exposures.
        """
        if self._welldata is not None:
            return self._welldata
        elif self.runstate == "INIT":
            raise ValueError("Run hasn't started yet: no data available.")
        else:
            raise ValueError("Experiment data is not available")

    def summary(self, format: str = "markdown", plate: str = "list") -> str:
        """Generate a summary of the experiment, with some formatting configuation. `str()`
        uses this with default parameters.

        Parameters
        ----------
        format : "markdown" or "org", optional
            Format of output, currently "markdown" or "org", and currently matters only
            when plate is "table". By default "markdown".  If an unknown value,
            passed as tablefmt to tabulate.
        plate : "list" or "table", optional
            Format of plate information. "list" gives a list of samples, "table" outputs a
            plate layout table (possibly quite wide). By default "list".

        Returns
        -------
        str
            Summary
        """
        s = f"# QS Experiment {self.name} ({self.runstate})\n\n"
        s += f"{self.protocol}\n\n"
        if plate == "list":
            s += f"{self.plate_setup}\n"
        elif plate == "table":
            s += f"{self.plate_setup.to_table(tablefmt=_TABLE_FORMAT.get(format, format))}\n\n"
        s += f"- Created: {self.createdtime}\n"
        if self.runstarttime:
            s += f"- Run Started: {self.runstarttime}\n"
        if self.runendtime:
            s += f"- Run Ended: {self.runendtime}\n"
        s += f"- Written by: {self.writesoftware}\n"
        s += f"- Read by: QSLib {__version__}\n"
        return s

    def info_html(self) -> str | ET.ElementTree | None:
        summary = self.summary(plate="table")

        import matplotlib.pyplot as plt

        fig, ax = plt.subplots(figsize=(21.0 / 2.54, 15.0 / 2.54))
        self.protocol.tcplot(ax)

        o = io.StringIO()
        fig.savefig(o, format="svg")
        o = o.getvalue()
        o = re.search("<svg.*</svg>", o, re.DOTALL)
        if o is None:
            raise Exception
        o = o[0]

        import markdown

        ost = markdown.markdown(summary, output_format="xhtml", extensions=["tables"])

        s = f"""
<!DOCTYPE html>
<html>
<head>
<title>{self.name}</title>
<style>
table {{
    font-size: 10pt;
    border-collapse: collapse;
    margin-left: auto;
    margin-right: auto;
}}
svg {{
    margin-left: auto;
    margin-right: auto;
}}
td:first-of-type {{
    font-weight: bold;
}}
table, th, td {{
    border: 1px solid black;
}}
</style>
</head>
<body>
{ost}

{o}
</body>
</html>
        """

        return s

    def __str__(self) -> str:
        return self.summary()

    def _repr_markdown_(self) -> str:
        if len(self.plate_setup.sample_wells) <= 12:
            return self.summary()
        else:
            return self.summary(plate="table")

    @property
    def runtitle_safe(self) -> str:
        """Run name with " " replaced by "-"."""
        return self.name.replace(" ", "-")

    @property
    def rawdata(self) -> pd.DataFrame:
        warn("rawdata is deprecated; use welldata instead")
        if (self.activestarttime is None) or (self.welldata is None):
            raise DataError
        return _fdc_to_rawdata(
            self.welldata,
            self.activestarttime.timestamp(),
        )

    @property
    def filterdata(self) -> pd.DataFrame:
        warn("filterdata is deprecated; use welldata instead")
        return self.rawdata

    def _ensure_machine(
        self, machine: Machine | str | None, password: str | None = None
    ) -> Machine:
        if isinstance(machine, Machine):
            self.machine = machine
            return machine
        elif isinstance(machine, str):
            self.machine = Machine(machine, password=password)
            return self.machine
        elif c := getattr(self, "machine", None):
            return c
        else:
            raise ValueError(
                "No stored machine info for this experiment: please provide some."
            )

    def _ensure_running(self, machine: Machine) -> RunStatus:
        crt = machine.run_status()
        if crt.name != self.runtitle_safe:
            raise NotRunningError(machine, self.runtitle_safe, crt)
        return crt

    def _populate_folder(self) -> None:
        machine = self._ensure_machine(None)
        tempzip = io.BytesIO()
        self.save_file(tempzip)
        exppath = self.runtitle_safe + "/"
        with machine.at_access("Controller"):
            machine.run_command_bytes(
                b"EXP:ZIPWRITE "
                + exppath.encode()
                + b" <message>\n"
                + base64.encodebytes(tempzip.getvalue())
                + b"</message>\n"
            )

    def run(
        self,
        machine: Machine | str | None = None,
        password=None,
        require_exclusive=True,
    ) -> None:
        """Load the run onto a machine, and start it.

        Parameters
        ----------
        machine : Machine, optional
            [description], by default None

        Raises
        ------
        MachineBusyError
            The machine isn't idle.
        AlreadyExistsError
            The machine already has a folder for this run in its working runs folder.
        """
        machine = self._ensure_machine(machine, password)
        log.info(f"Attempting to start {self.runtitle_safe} on {machine.host}.")

        # Ensure machine isn't running:
        if (x := machine.run_status()).state.upper() != "IDLE":
            raise MachineBusyError(machine, x)

        if self.runstate != "INIT":
            raise ValueError

        with machine.at_access("Controller", exclusive=require_exclusive):
            log.debug("Powering on machine and ensuring drawer/cover is closed.")
            # Ensure machine state and power.
            machine.power = True
            machine.drawer_close(lower_cover=True)

            # Check existence of previous run folder
            if self.runtitle_safe + "/" in machine.run_command("EXP:LIST?"):
                raise AlreadyExistsError(self.runtitle_safe)

            log.debug(f"Creating experiment folder {self.runtitle_safe}.")
            machine.run_command(f"EXP:NEW {self.runtitle_safe} {TEMPLATE_NAME}")

            log.debug(f"Populating experiment folder.")
            self._populate_folder()

            log.debug(f"Sending protocol.")
            machine.define_protocol(self.protocol)

            log.debug(f"Sending run command.")
            # Start the run
            machine.run_command_to_ack(
                f"RP -samplevolume={self.protocol.volume} -runmode={self.protocol.runmode}"
                f" {self.protocol.name} {self.runtitle_safe}"
            )  # TODO: user, cover
            log.info(f"Run {self.runtitle_safe} started on {machine.host}.")

    def create_new_copy(self) -> Experiment:
        """Create a copy of the experiment, with data and run information removed, suitable
        for rerunning.

        Returns
        -------
        Experiment
        """
        raise NotImplementedError

    def collect_finished(self, machine: Machine | None = None) -> None:
        """NOT YET IMPLEMENTED

        Collect the completed (aborted/etc) experiment from a machine, reliably.  This will
        search for and recover working data if the EDS file generation on the machine failed.

        Parameters
        ----------
        machine : Machine, optional
            [description], by default None

        Raises
        ------
        NotImplementedError
            [description]
        """
        machine = self._ensure_machine(machine)

        # Ensure run is actually done.
        raise NotImplementedError

    def pause_now(self, machine: Machine | None = None) -> None:
        """
        If this experiment is running, pause it (immediately).

        Requires and takes exclusive Controller access on the machine.

        Raises
        ------
        NotRunningError
            the experiment is not currently running
        """
        machine = self._ensure_machine(machine)
        self._ensure_running(machine)
        with machine.at_access("Controller", exclusive=True):
            machine.pause_current_run()

    def resume(self, machine: Machine | None = None) -> None:
        """
        If this experiment is running, resume it.

        Requires and takes exclusive Controller access on the machine.

        Raises
        ------
        NotRunningError
            the experiment is not currently running
        """
        machine = self._ensure_machine(machine)
        self._ensure_running(machine)
        with machine.at_access("Controller", exclusive=True):
            machine.resume_current_run()

    def stop(self, machine: Machine | None = None) -> None:
        """
        If this experiment is running, stop it after the end of the current cycle.

        Requires and takes exclusive Controller access on the machine.

        Raises
        ------
        NotRunningError
            the experiment is not currently running
        """
        machine = self._ensure_machine(machine)
        self._ensure_running(machine)
        with machine.at_access("Controller", exclusive=True):
            machine.stop_current_run()

    def abort(self, machine: Machine | None = None) -> None:
        """
        If this experiment is running, abort it, stopping it immediately.

        Requires and takes exclusive Controller access on the machine.

        Raises
        ------
        NotRunningError
            the experiment is not currently running
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
        NotRunningError
            the experiment is not currently running
        """
        machine = self._ensure_machine(machine)
        return self._ensure_running(machine)

    def sync_from_machine(
        self,
        machine: Machine | None = None,
        connect: bool = False,
        log_method: Literal["copy", "eval"] = "eval",
    ) -> None:
        """
        Try to synchronize the data in the experiment to the current state of the run on a
        machine, more efficiently than reloading everything.
        """
        machine = self._ensure_machine(machine)

        disconnect = False
        if connect and not machine.connected:
            disconnect = True
            machine.connect()

        try:
            # Get a list of all the files in the experiment folder
            machine_files = machine.list_files(
                f"experiments:{self.runtitle_safe}/", verbose=True, recursive=True
            )

            # Transfer anything we don't have
            for f in machine_files:
                name = os.path.basename(f["path"])
                sdspath = self._sdspath(
                    re.sub(".*/apldbio/sds/(.*)$", r"\1", f["path"])
                )
                logging.debug(f"checking {f['path']} mtime {f['mtime']} to {sdspath}")
                if os.path.exists(sdspath) and os.path.getmtime(sdspath) >= float(
                    f["mtime"]
                ):
                    logging.debug(f"{sdspath} has {os.path.getmtime(sdspath)}")
                    continue
                from pathlib import Path  # FIXME

                ldir = f["path"].split("/")[-2]
                if ldir != "sds":
                    cp = Path(self._dir_eds) / ldir
                    if not cp.exists():
                        cp.mkdir()
                if log_method == "copy" or (not f["path"].endswith("messages.log")):
                    with open(sdspath, "wb") as b:
                        b.write(machine.read_file(f["path"]))
                elif log_method == "eval":
                    with open(sdspath, "ab+") as b:

                        # Seeking to the end of the file gives us the size
                        curpos = b.seek(0, 1)

                        # If we're less than 20 bytes, just copy the file:
                        if curpos < 20:
                            b.seek(0)
                            b.write(machine.read_file(f["path"]))
                        else:
                            # Now seek 20 bytes back, and read those 20 bytes
                            b.seek(-20, 1)
                            checklocal = b.read(20)

                            assert b.seek(0, 1) == curpos

                            rdat = machine._get_log_from_byte(
                                self.runtitle_safe, curpos - 20
                            )

                            if rdat[0:20] == checklocal:
                                b.write(rdat[20:])
                            else:
                                log.error(
                                    "Log comparison failure in eval method,"
                                    f" falling back to copy. ('{checklocal}' ≠ '{rdat[0:20]}')"
                                )
                                b.seek(0)
                                b.write(machine.read_file(f["path"]))
                else:
                    raise NotImplementedError
                os.utime(sdspath, (f["atime"], f["mtime"]))

            # The message log is tricky. Ideally we'd use rsync or wc+tail. TODO
            self._update_from_files()
        finally:
            if disconnect:
                machine.disconnect()

    def change_protocol(
        self, new_protocol: Protocol, machine: Machine | None = None
    ) -> None:
        """
        For a running experiment and an updated protocol, check compatibility
        with the current run, and if possible, update the protocol in the experiment
        file and on the machine, changing the current run.

        Changes that should be possible:

        * Changing the number of cycles of the current run to a higher or lower value (but
          higher or equal to the current cycle number), allowing stages to be lengthened,
          shortened, or stopped.
        * Adding new stages after the current stage.
        * Arbitrarily changing any stage that hasn't started.

        For safest results, ensure power saving is turned off in the Android software.

        Parameters
        ----------
        new_protocol : Protocol
            [description]
        machine : Machine, optional
            [description], by default None

        """
        machine = self._ensure_machine(machine)
        self._ensure_running(machine)

        runstatus = machine.run_status()

        # Get running protocol and status.
        machine_proto = machine.get_running_protocol()

        # FIXME: can't just do this, may have extra default things added
        # if machine_proto != self.protocol:
        #    raise ValueError("Machine and experiment protocols differ.")

        # Check compatibility of changes.
        machine_proto.check_compatible(new_protocol, runstatus)

        new_protocol.name = machine_proto.name
        new_protocol.volume = machine_proto.volume
        new_protocol.runmode = machine_proto.runmode

        with machine.at_access(AccessLevel.Controller):
            # Make changes.
            machine.define_protocol(new_protocol)

            # Save changes in tcprotocol.xml
            self.protocol = new_protocol

            self._update_tcprotocol_xml()

            # Push new tcprotocol.xml
            machine.write_file(
                "${LogFolder}/tcprotocol.xml",
                open(self._sdspath("tcprotocol.xml"), "rb").read(),
            )

    def save_file(
        self, file: str | os.PathLike[str] | IO[bytes], overwrite: bool = False
    ) -> None:
        """
        Save an EDS file of the experiment. This *should* be readable by AB's software,
        but makes no attempt to hide that it was written by QSLib, and contains some other
        information. By default, this will refuse to overwrite an existing file.

        Parameters
        ----------
        file : str or IO[bytes]
            A filename or open binary IO.
        overwrite : bool, optional
            If True, overwrite any existing file without warning. Defaults to False.
        """
        if overwrite:
            mode = "w"
        else:
            mode = "x"

        self._update_files()

        with zipfile.ZipFile(file, mode) as z:
            for root, subs, files in os.walk(self._dir_base):
                for file in files:
                    fpath = os.path.join(root, file)
                    z.write(fpath, os.path.relpath(fpath, self._dir_base))

    def save_file_without_changes(
        self, file: str | IO[bytes], overwrite: bool = False
    ) -> None:
        """
        Save an EDS file of the experiment. Unlike :any:`save_file`, this will not
        update any parts of the file, so if it has not been modified elsewhere,
        it will be the same as when it was loaded. By default, this will refuse to
        overwrite an existing file.

        Parameters
        ----------
        file : str or IO[bytes]
            A filename or open binary IO.
        overwrite : bool, optional
            If True, overwrite any existing file without warning. Defaults to False.
        """
        if overwrite:
            mode = "w"
        else:
            mode = "x"

        with zipfile.ZipFile(file, mode) as z:
            for root, subs, files in os.walk(self._dir_base):
                for file in files:
                    fpath = os.path.join(root, file)
                    z.write(fpath, os.path.relpath(fpath, self._dir_base))

    @property
    def sample_wells(self) -> dict[str, list[str]]:
        return self.plate_setup.sample_wells

    @sample_wells.setter
    def sample_wells(self, new_sample_wells: dict[str, list[str]]):
        self.plate_setup.sample_wells = new_sample_wells

    def __init__(
        self,
        name: str | None = None,
        protocol: Protocol | None = None,
        plate_setup: PlateSetup | None = None,
        _create_xml: bool = True,
    ):
        self._tmp_dir_obj = tempfile.TemporaryDirectory()
        self._dir_base = self._tmp_dir_obj.name
        self._dir_eds = os.path.join(self._dir_base, "apldbio", "sds")

        self._protocol_from_qslib = None
        self._protocol_from_log = None
        self._protocol_from_xml = None
        self.runstarttime = None
        self.runendtime = None
        self.activestarttime = None
        self.activeendtime = None

        self.machine = None

        if name is not None:
            self.name = name
        else:
            self.name = _nowuuid()

        if protocol is None:
            self.protocol = Protocol([Stage([Step(60, 25)])])
        else:
            self.protocol = protocol

        if plate_setup:
            self.plate_setup = plate_setup
        else:
            self.plate_setup = PlateSetup()

        if _create_xml:
            self.createdtime = datetime.now()
            self.modifiedtime = self.createdtime
            self.runstarttime = None
            self.runendtime = None
            self.runstate = "INIT"
            self.user = None
            self.writesoftware = f"QSLib {__version__}"

            self._new_xml_files()
            self._update_files()

    def _new_xml_files(self) -> None:
        os.mkdir(os.path.join(self._dir_base, "apldbio"))
        os.mkdir(os.path.join(self._dir_base, "apldbio", "sds"))

        e = ET.ElementTree(ET.Element("Experiment"))

        ET.SubElement(e.getroot(), "Label").text = "ruo"
        tp = ET.SubElement(e.getroot(), "Type")
        ET.SubElement(tp, "Id").text = "Custom"
        ET.SubElement(tp, "Name").text = "Custom"
        ET.SubElement(tp, "Description").text = "Custom QSLib experiment"
        ET.SubElement(tp, "ResultPersisterName").text = "scAnalysisResultPersister"
        ET.SubElement(
            tp, "ContributedResultPersisterName"
        ).text = "mcAnalysisResultPersister"
        ET.SubElement(e.getroot(), "ChemistryType").text = "Other"
        ET.SubElement(e.getroot(), "TCProtocolMode").text = "Standard"
        ET.SubElement(e.getroot(), "DNATemplateType").text = "WET_DNA"
        ET.SubElement(e.getroot(), "InstrumentTypeId").text = "appletini"  # note cap!
        ET.SubElement(e.getroot(), "BlockTypeID").text = "18"
        ET.SubElement(e.getroot(), "PlateTypeID").text = "TYPE_8X12"

        e.write(open(os.path.join(self._dir_eds, "experiment.xml"), "wb"))

        p = ET.parse(
            io.BytesIO(
                b"""<Plate>
    <Name></Name>
    <BarCode></BarCode>
    <Description></Description>
    <Rows>8</Rows>
    <Columns>12</Columns>
    <PlateKind>
        <Name>96-Well Plate (8x12)</Name>
        <Type>TYPE_8X12</Type>
        <RowCount>8</RowCount>
        <ColumnCount>12</ColumnCount>
    </PlateKind>
    <FeatureMap>
        <Feature>
            <Id>marker-task</Id>
            <Name>marker-task</Name>
        </Feature>
    </FeatureMap>
    </Plate>
    """
            )
        )

        p.write(open(os.path.join(self._dir_eds, "plate_setup.xml"), "wb"))

        ET.ElementTree(ET.Element("TCProtocol")).write(
            open(os.path.join(self._dir_eds, "tcprotocol.xml"), "wb")
        )

        ET.ElementTree(ET.Element("QSLTCProtocol")).write(
            open(os.path.join(self._dir_eds, "qsl-tcprotocol.xml"), "wb")
        )

        with open(os.path.join(self._dir_eds, "Manifest.mf"), "w") as f:
            f.write(MANIFEST_CONTENTS)

        # File will not load in AB without this, even though it is
        # useless for our purposes.
        with open(self._sdspath("analysis_protocol.xml"), "w") as f:
            f.write(
                """<JaxbAnalysisProtocol>
    <Name>unnamed</Name>
    <JaxbAnalysisSettings>
        <Type>com.apldbio.sds.platform.core.analysis.IDetectorSettings</Type>
        <JaxbSettingValue>
            <Name>ConfidenceLevel</Name>
            <JaxbValueItem type="Double">
                <DoubleValue>99.0</DoubleValue>
            </JaxbValueItem>
        </JaxbSettingValue>
        <JaxbSettingValue>
            <Name>CreatedByUser</Name>
            <JaxbValueItem type="Boolean">
                <BooleanValue>true</BooleanValue>
            </JaxbValueItem>
        </JaxbSettingValue>
        <JaxbSettingValue>
            <Name>Threshold</Name>
            <JaxbValueItem type="Double">
                <DoubleValue>0.2</DoubleValue>
            </JaxbValueItem>
        </JaxbSettingValue>
        <JaxbSettingValue>
            <Name>AutoBaseline</Name>
            <JaxbValueItem type="Boolean">
                <BooleanValue>true</BooleanValue>
            </JaxbValueItem>
        </JaxbSettingValue>
        <JaxbSettingValue>
            <Name>BaselineStart</Name>
            <JaxbValueItem type="Integer">
                <IntValue>3</IntValue>
            </JaxbValueItem>
        </JaxbSettingValue>
        <JaxbSettingValue>
            <Name>ObjectName</Name>
            <JaxbValueItem type="String">
                <StringValue>AnalysisProtocol.DEFAULT_SETTINGS</StringValue>
            </JaxbValueItem>
        </JaxbSettingValue>
        <JaxbSettingValue>
            <Name>BaselineStop</Name>
            <JaxbValueItem type="Integer">
                <IntValue>15</IntValue>
            </JaxbValueItem>
        </JaxbSettingValue>
        <JaxbSettingValue>
            <Name>AutoCt</Name>
            <JaxbValueItem type="Boolean">
                <BooleanValue>true</BooleanValue>
            </JaxbValueItem>
        </JaxbSettingValue>
    </JaxbAnalysisSettings>
    <JaxbAnalysisSettings>
        <Type>com.apldbio.sds.platform.core.analysis.IWellSettings</Type>
        <JaxbSettingValue>
            <Name>BaselineStart</Name>
            <JaxbValueItem type="Integer">
                <IntValue>3</IntValue>
            </JaxbValueItem>
        </JaxbSettingValue>
        <JaxbSettingValue>
            <Name>ObjectName</Name>
            <JaxbValueItem type="String">
                <StringValue>AnalysisProtocol.DEFAULT_SETTINGS</StringValue>
            </JaxbValueItem>
        </JaxbSettingValue>
        <JaxbSettingValue>
            <Name>BaselineStop</Name>
            <JaxbValueItem type="Integer">
                <IntValue>15</IntValue>
            </JaxbValueItem>
        </JaxbSettingValue>
        <JaxbSettingValue>
            <Name>UseDetectorDefaults</Name>
            <JaxbValueItem type="Boolean">
                <BooleanValue>false</BooleanValue>
            </JaxbValueItem>
        </JaxbSettingValue>
    </JaxbAnalysisSettings>
    <JaxbAnalysisSettings>
        <Type>com.apldbio.sds.platform.core.analysis.ISignalSmoothingSettings</Type>
        <JaxbSettingValue>
            <Name>SignalSmoothing</Name>
            <JaxbValueItem type="Boolean">
                <BooleanValue>true</BooleanValue>
            </JaxbValueItem>
        </JaxbSettingValue>
        <JaxbSettingValue>
            <Name>ObjectName</Name>
            <JaxbValueItem type="String">
                <StringValue>AnalysisProtocol.DEFAULT_SETTINGS</StringValue>
            </JaxbValueItem>
        </JaxbSettingValue>
    </JaxbAnalysisSettings>
    <JaxbAnalysisSettings>
        <Type>com.apldbio.sds.platform.core.analysis.IAlgorithmSelectSettings</Type>
        <JaxbSettingValue>
            <Name>AlgorithmName</Name>
            <JaxbValueItem type="String">
                <StringValue>default</StringValue>
            </JaxbValueItem>
        </JaxbSettingValue>
        <JaxbSettingValue>
            <Name>ObjectName</Name>
            <JaxbValueItem type="String">
                <StringValue>AnalysisProtocol.DEFAULT_SETTINGS</StringValue>
            </JaxbValueItem>
        </JaxbSettingValue>
    </JaxbAnalysisSettings>
    <JaxbAnalysisSettings>
        <Type>com.apldbio.sds.platform.core.analysis.IAutoAnalysisSettings</Type>
        <JaxbSettingValue>
            <Name>AutoAnalysis</Name>
            <JaxbValueItem type="Boolean">
                <BooleanValue>false</BooleanValue>
            </JaxbValueItem>
        </JaxbSettingValue>
        <JaxbSettingValue>
            <Name>AutoSpectral</Name>
            <JaxbValueItem type="Boolean">
                <BooleanValue>false</BooleanValue>
            </JaxbValueItem>
        </JaxbSettingValue>
        <JaxbSettingValue>
            <Name>ObjectName</Name>
            <JaxbValueItem type="String">
                <StringValue>AnalysisProtocol.DEFAULT_SETTINGS</StringValue>
            </JaxbValueItem>
        </JaxbSettingValue>
    </JaxbAnalysisSettings>
    <JaxbAnalysisSettings>
        <Type>com.apldbio.sds.platform.core.analysis.IDataSelectSettings</Type>
        <JaxbSettingValue>
            <Name>StepNum</Name>
            <JaxbValueItem type="Integer">
                <IntValue>5</IntValue>
            </JaxbValueItem>
        </JaxbSettingValue>
        <JaxbSettingValue>
            <Name>StageNum</Name>
            <JaxbValueItem type="Integer">
                <IntValue>4</IntValue>
            </JaxbValueItem>
        </JaxbSettingValue>
        <JaxbSettingValue>
            <Name>MeltStageNum</Name>
            <JaxbValueItem type="Integer">
                <IntValue>0</IntValue>
            </JaxbValueItem>
        </JaxbSettingValue>
        <JaxbSettingValue>
            <Name>ObjectName</Name>
            <JaxbValueItem type="String">
                <StringValue>IDataSelectSettings.OBJECT_NAME</StringValue>
            </JaxbValueItem>
        </JaxbSettingValue>
        <JaxbSettingValue>
            <Name>PointNum</Name>
            <JaxbValueItem type="Integer">
                <IntValue>5</IntValue>
            </JaxbValueItem>
        </JaxbSettingValue>
    </JaxbAnalysisSettings>
    <JaxbAnalysisSettings>
        <Type>com.apldbio.sds.platform.core.analysis.ICrtSettings</Type>
        <JaxbSettingValue>
            <Name>CrtStartValue</Name>
            <JaxbValueItem type="Integer">
                <IntValue>1</IntValue>
            </JaxbValueItem>
        </JaxbSettingValue>
    </JaxbAnalysisSettings>
    <AnalysisSetting>
        <Type>NHC</Type>
        <SettingValue>
            <Name>enabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>true</BooleanValue>
        </SettingValue>
        <SettingValue>
            <Name>criteria</Name>
            <Type>String</Type>
            <StringValue>LESS_THAN 35.0</StringValue>
        </SettingValue>
        <SettingValue>
            <Name>omitEnabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>false</BooleanValue>
        </SettingValue>
    </AnalysisSetting>
    <AnalysisSetting>
        <Type>BPR</Type>
        <SettingValue>
            <Name>enabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>true</BooleanValue>
        </SettingValue>
        <SettingValue>
            <Name>criteria</Name>
            <Type>String</Type>
            <StringValue>GREATER_THAN 0.6</StringValue>
        </SettingValue>
        <SettingValue>
            <Name>omitEnabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>false</BooleanValue>
        </SettingValue>
    </AnalysisSetting>
    <AnalysisSetting>
        <Type>DRNMIN</Type>
        <SettingValue>
            <Name>enabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>true</BooleanValue>
        </SettingValue>
        <SettingValue>
            <Name>criteria</Name>
            <Type>String</Type>
            <StringValue>LESS_THAN 35.0</StringValue>
        </SettingValue>
        <SettingValue>
            <Name>criteria</Name>
            <Type>String</Type>
            <StringValue>LESS_THAN 0.2</StringValue>
        </SettingValue>
        <SettingValue>
            <Name>omitEnabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>false</BooleanValue>
        </SettingValue>
    </AnalysisSetting>
    <AnalysisSetting>
        <Type>FOS</Type>
        <SettingValue>
            <Name>enabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>true</BooleanValue>
        </SettingValue>
        <SettingValue>
            <Name>criteria</Name>
            <Type>String</Type>
            <StringValue>true</StringValue>
        </SettingValue>
        <SettingValue>
            <Name>omitEnabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>false</BooleanValue>
        </SettingValue>
    </AnalysisSetting>
    <AnalysisSetting>
        <Type>HSD</Type>
        <SettingValue>
            <Name>enabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>true</BooleanValue>
        </SettingValue>
        <SettingValue>
            <Name>criteria</Name>
            <Type>String</Type>
            <StringValue>GREATER_THAN 0.5</StringValue>
        </SettingValue>
        <SettingValue>
            <Name>omitEnabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>false</BooleanValue>
        </SettingValue>
    </AnalysisSetting>
    <AnalysisSetting>
        <Type>CC</Type>
        <SettingValue>
            <Name>enabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>true</BooleanValue>
        </SettingValue>
        <SettingValue>
            <Name>criteria</Name>
            <Type>String</Type>
            <StringValue>LESS_THAN 0.8</StringValue>
        </SettingValue>
        <SettingValue>
            <Name>omitEnabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>false</BooleanValue>
        </SettingValue>
    </AnalysisSetting>
    <AnalysisSetting>
        <Type>NA</Type>
        <SettingValue>
            <Name>enabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>true</BooleanValue>
        </SettingValue>
        <SettingValue>
            <Name>criteria</Name>
            <Type>String</Type>
            <StringValue>LESS_THAN 0.1</StringValue>
        </SettingValue>
        <SettingValue>
            <Name>omitEnabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>false</BooleanValue>
        </SettingValue>
    </AnalysisSetting>
    <AnalysisSetting>
        <Type>HRN</Type>
        <SettingValue>
            <Name>enabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>true</BooleanValue>
        </SettingValue>
        <SettingValue>
            <Name>criteria</Name>
            <Type>String</Type>
            <StringValue>GREATER_THAN 4.0</StringValue>
        </SettingValue>
        <SettingValue>
            <Name>omitEnabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>false</BooleanValue>
        </SettingValue>
    </AnalysisSetting>
    <AnalysisSetting>
        <Type>NS</Type>
        <SettingValue>
            <Name>enabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>true</BooleanValue>
        </SettingValue>
        <SettingValue>
            <Name>criteria</Name>
            <Type>String</Type>
            <StringValue>GREATER_THAN 1.0</StringValue>
        </SettingValue>
        <SettingValue>
            <Name>omitEnabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>false</BooleanValue>
        </SettingValue>
    </AnalysisSetting>
    <AnalysisSetting>
        <Type>EW</Type>
        <SettingValue>
            <Name>enabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>false</BooleanValue>
        </SettingValue>
        <SettingValue>
            <Name>criteria</Name>
            <Type>String</Type>
            <StringValue>true</StringValue>
        </SettingValue>
        <SettingValue>
            <Name>omitEnabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>false</BooleanValue>
        </SettingValue>
        <SettingValue>
            <Name>criteria4QS</Name>
            <Type>String</Type>
            <StringValue>LESS_THAN 100000.0</StringValue>
        </SettingValue>
    </AnalysisSetting>
    <AnalysisSetting>
        <Type>ORG</Type>
        <SettingValue>
            <Name>enabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>true</BooleanValue>
        </SettingValue>
        <SettingValue>
            <Name>criteria</Name>
            <Type>String</Type>
            <StringValue>true</StringValue>
        </SettingValue>
        <SettingValue>
            <Name>omitEnabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>false</BooleanValue>
        </SettingValue>
    </AnalysisSetting>
    <AnalysisSetting>
        <Type>EAF</Type>
        <SettingValue>
            <Name>enabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>true</BooleanValue>
        </SettingValue>
        <SettingValue>
            <Name>criteria</Name>
            <Type>String</Type>
            <StringValue>true</StringValue>
        </SettingValue>
        <SettingValue>
            <Name>omitEnabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>false</BooleanValue>
        </SettingValue>
    </AnalysisSetting>
    <AnalysisSetting>
        <Type>BAF</Type>
        <SettingValue>
            <Name>enabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>true</BooleanValue>
        </SettingValue>
        <SettingValue>
            <Name>criteria</Name>
            <Type>String</Type>
            <StringValue>true</StringValue>
        </SettingValue>
        <SettingValue>
            <Name>omitEnabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>false</BooleanValue>
        </SettingValue>
    </AnalysisSetting>
    <AnalysisSetting>
        <Type>TAF</Type>
        <SettingValue>
            <Name>enabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>true</BooleanValue>
        </SettingValue>
        <SettingValue>
            <Name>criteria</Name>
            <Type>String</Type>
            <StringValue>true</StringValue>
        </SettingValue>
        <SettingValue>
            <Name>omitEnabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>false</BooleanValue>
        </SettingValue>
    </AnalysisSetting>
    <AnalysisSetting>
        <Type>CAF</Type>
        <SettingValue>
            <Name>enabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>true</BooleanValue>
        </SettingValue>
        <SettingValue>
            <Name>criteria</Name>
            <Type>String</Type>
            <StringValue>true</StringValue>
        </SettingValue>
        <SettingValue>
            <Name>omitEnabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>false</BooleanValue>
        </SettingValue>
    </AnalysisSetting>
    <AnalysisSetting>
        <Type>ROXLOW</Type>
        <SettingValue>
            <Name>enabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>true</BooleanValue>
        </SettingValue>
        <SettingValue>
            <Name>criteria</Name>
            <Type>String</Type>
            <StringValue>LESS_THAN 20.0</StringValue>
        </SettingValue>
        <SettingValue>
            <Name>omitEnabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>false</BooleanValue>
        </SettingValue>
    </AnalysisSetting>
    <AnalysisSetting>
        <Type>ROXDROP</Type>
        <SettingValue>
            <Name>enabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>true</BooleanValue>
        </SettingValue>
        <SettingValue>
            <Name>criteria</Name>
            <Type>String</Type>
            <StringValue>GREATER_THAN 0.04</StringValue>
        </SettingValue>
        <SettingValue>
            <Name>omitEnabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>false</BooleanValue>
        </SettingValue>
    </AnalysisSetting>
</JaxbAnalysisProtocol>
            """
            )

    def _sdspath(self, path: str) -> str:
        return os.path.join(self._dir_eds, path)

    def _update_files(self) -> None:
        self._update_experiment_xml()
        self._update_tcprotocol_xml()
        self._update_platesetup_xml()

    def _update_from_files(self) -> None:
        p = Path(self._dir_eds)
        if (p / "experiment.xml").is_file():
            self._update_from_experiment_xml()
        if (p / "tcprotocol.xml").is_file():
            self._update_from_tcprotocol_xml()
        if (p / "plate_setup.xml").is_file():
            self._update_from_platesetup_xml()
        if (p / "messages.log").is_file():
            self._update_from_log()
        self._update_from_data()

        if self._protocol_from_xml:
            self.protocol = self._protocol_from_xml
        if self._protocol_from_log:
            self.protocol = self._protocol_from_log
        if self._protocol_from_qslib:
            self.protocol = self._protocol_from_qslib
        if self._protocol_from_xml:
            self.protocol.covertemperature = self._protocol_from_xml.covertemperature

    @classmethod
    def from_file(cls, file: str | os.PathLike[str] | IO[bytes]) -> Experiment:
        """Load an experiment from an EDS file.

        Returns
        -------
        file : str or os.PathLike[str] or IO[bytes]
            The filename or file handle to read.

        Raises
        ------
        ValueError
            if the file does not appear to be an EDS file (lacks an experiment.xml).
        """
        exp = cls(_create_xml=False)

        with zipfile.ZipFile(file) as z:
            # Ensure that this actually looks like an EDS file:
            try:
                z.getinfo("apldbio/sds/experiment.xml")
            except KeyError:
                raise ValueError(f"{file} does not appear to be an EDS file.") from None

            z.extractall(exp._dir_base)

        exp._update_from_files()

        return exp

    @classmethod
    def from_running(cls, machine: Machine) -> "Experiment":
        """Create an experiment from the one currently running on a machine.

        Parameters
        ----------
        machine : Machine
            the machine to connect to

        Returns
        -------
        Experiment
            a copy of the runnig experiment
        """
        exp = cls(_create_xml=False)

        crt = machine.current_run_name

        if not crt:
            raise ValueError("Nothing is currently running.")

        z = machine.read_dir_as_zip(crt, leaf="EXP")

        z.extractall(exp._dir_base)

        exp._update_from_files()

        return exp

    @classmethod
    def from_uncollected(
        cls, machine: Machine, name: str, move: bool = False
    ) -> Experiment:
        """Create an experiment from the uncollected (not yet compressed)
        storage.

        Parameters
        ----------
        machine : Machine
            the machine to connect to

        name: str
            the name of the run to collect.

        Returns
        -------
        Experiment
            a copy of the runnig experiment
        """
        exp = cls(_create_xml=False)

        crt = name

        if move:
            raise NotImplementedError

        if not crt:
            raise ValueError("Nothing is currently running.")

        z = machine.read_dir_as_zip(crt, leaf="EXP")

        z.extractall(exp._dir_base)

        exp._update_from_files()

        return exp

    @classmethod
    def from_machine_storage(cls, machine: Machine, path: str) -> Experiment:
        """Create an experiment from the one currently running on a machine.

        Parameters
        ----------
        machine : Machine
            the machine to connect to

        Returns
        -------
        Experiment
            a copy of the runnig experiment
        """
        exp = cls(_create_xml=False)

        o = machine.read_file(path, context="public_run_complete")

        z = zipfile.ZipFile(io.BytesIO(o))

        z.extractall(exp._dir_base)

        exp._update_from_files()

        return exp

    def _update_experiment_xml(self) -> None:
        exml = ET.parse(os.path.join(self._dir_eds, "experiment.xml"))

        _set_or_create(exml, "Name", text=self.name)
        if self.user:
            _set_or_create(exml, "Operator", text=self.user)
        else:
            if (e := exml.find("Operator")) is not None:
                exml.getroot().remove(e)
        _set_or_create(
            exml, "CreatedTime", str(int(self.createdtime.timestamp() * 1000))
        )
        _set_or_create(
            exml, "ModifiedTime", str(int(datetime.now().timestamp() * 1000))
        )
        if self.runstarttime:
            _set_or_create(
                exml, "RunStartTime", str(int(self.runstarttime.timestamp() * 1000))
            )
        if self.runendtime:
            _set_or_create(
                exml, "RunEndTime", str(int(self.runendtime.timestamp() * 1000))
            )

        _set_or_create(exml, "RunState", self.runstate)

        sinfo = exml.find(
            "ExperimentProperty[@type='RunInfo']/PropertyValue[@key='softwareVersion']/String"
        )
        if sinfo is None:
            if not (e := exml.find("ExperimentProperty[@type='RunInfo']")):
                e = ET.SubElement(exml.getroot(), "ExperimentProperty", type="RunInfo")
            e.append(sinfo := ET.Element("PropertyValue", key="softwareVersion"))
            sinfo = ET.SubElement(sinfo, "String")
        sinfo.text = f"QSLib {__version__}"

        ET.indent(exml)

        exml.write(os.path.join(self._dir_eds, "experiment.xml"))

    def _update_from_experiment_xml(self) -> None:
        exml = ET.parse(os.path.join(self._dir_eds, "experiment.xml"))

        self.name = exml.findtext("Name") or "unknown"
        self.user = _text_or_none(exml, "Operator")
        self.createdtime = datetime.fromtimestamp(
            float(_find_or_raise(exml, "CreatedTime").text) / 1000.0
        )  # type: ignore
        self.runstate = exml.findtext("RunState") or "UNKNOWN"  # type: ignore
        self.writesoftware = (
            exml.findtext(
                "ExperimentProperty[@type='RunInfo']/PropertyValue[@key='softwareVersion']/String"
            )
            or "UNKNOWN"
        )
        if x := exml.findtext("RunStartTime"):
            self.runstarttime = datetime.fromtimestamp(float(x) / 1000.0)
        if x := exml.findtext("RunEndTime"):
            self.runendtime = datetime.fromtimestamp(float(x) / 1000.0)

    def _update_tcprotocol_xml(self) -> None:
        if self.protocol:
            # exml = ET.parse(os.path.join(self._dir_eds, "tcprotocol.xml"))
            tcxml, qstcxml = self.protocol.to_xml()
            ET.indent(tcxml)
            ET.indent(qstcxml)
            tcxml.write(os.path.join(self._dir_eds, "tcprotocol.xml"))

            # Make new machine with stripped password:
            if self.machine:
                m2d = dataclasses.asdict(self.machine)
                m2d["password"] = None
                ET.SubElement(qstcxml.getroot(), "MachineConnection").text = toml.dumps(
                    m2d
                )

            qstcxml.write(os.path.join(self._dir_eds, "qsl-tcprotocol.xml"))

    def _update_from_tcprotocol_xml(self) -> None:
        exml = ET.parse(os.path.join(self._dir_eds, "tcprotocol.xml"))
        if os.path.isfile(os.path.join(self._dir_eds, "qsl-tcprotocol.xml")):
            qstcxml = ET.parse(os.path.join(self._dir_eds, "qsl-tcprotocol.xml"))

            if (x := qstcxml.find("QSLibProtocolCommand")) is not None:
                try:
                    self._protocol_from_qslib = Protocol.from_command(x.text)
                except ValueError:
                    self._protocol_from_qslib = None
            else:
                self._protocol_from_qslib = None

            if (x := qstcxml.findtext("MachineConnection")) and not self.machine:
                self.machine = Machine(toml.loads(x))
        try:
            self._protocol_from_xml = Protocol.from_xml(exml.getroot())
        except Exception as e:
            print(e)
            self._protocol_from_xml = None

    def _update_platesetup_xml(self) -> None:
        x = ET.parse(os.path.join(self._dir_eds, "plate_setup.xml"))
        self.plate_setup.update_xml(x.getroot())
        ET.indent(x)
        x.write(os.path.join(self._dir_eds, "plate_setup.xml"))

    def _update_from_platesetup_xml(self) -> None:
        x = ET.parse(os.path.join(self._dir_eds, "plate_setup.xml")).getroot()
        self.plate_setup = PlateSetup.from_platesetup_xml(x)

    def _update_from_data(self) -> None:
        fdp = os.path.join(self._dir_eds, "filterdata.xml")
        if os.path.isfile(fdp):
            fdx = ET.parse(fdp)
            fdrs = [
                FilterDataReading(x, sds_dir=self._dir_eds)
                for x in fdx.findall(".//PlateData")
            ]
            self._welldata = df_from_readings(
                fdrs, self.activestarttime.timestamp() if self.activestarttime else None
            )
        elif fdfs := glob(os.path.join(self._dir_eds, "filter", "*_filterdata.xml")):
            fdrs = [
                FilterDataReading(
                    ET.parse(fdf).find(".//PlateData"), sds_dir=self._dir_eds
                )
                for fdf in fdfs
            ]
            self._welldata = df_from_readings(
                fdrs, self.activestarttime.timestamp() if self.activestarttime else None
            )
        else:
            self._welldata = None

    def data_for_sample(self, sample: str) -> pd.DataFrame:
        """Convenience function to return data for a specific sample.

        Finds wells using :code:`self.plate_setup.sample_wells[sample]`, then
        returns :code:`self.welldata.loc[:, wells]`

        Parameters
        ----------
        sample : str
            sample name

        Returns
        -------
        pd.Dataframe
            Slice of welldata.  Will have multiple wells if sample is in multiple wells.
        """
        wells = ["time"] + [
            f"{x[0]}{int(x[1:])}" for x in self.plate_setup.sample_wells[sample]
        ]
        x = self.welldata.loc[:, wells]
        return x

    def _update_from_log(self) -> None:
        if not os.path.isfile(os.path.join(self._dir_eds, "messages.log")):
            return
        try:
            msglog = open(os.path.join(self._dir_eds, "messages.log"), "r").read()
        except UnicodeDecodeError as e:
            log.debug(
                "Decoding log failed. If <binary.reply> is present in log this may be the cause:"
                "if so contact Constantine (<const@costinet.org>).  Continuing with failed characters replaced with backslash notation"
                "Failure was in this area:\n"
                f"{e.object[e.start-500:e.end+500]}"
            )
            msglog = open(
                os.path.join(self._dir_eds, "messages.log"),
                "r",
                errors="backslashreplace",
            ).read()

        ms = re.finditer(
            r"^Run (?P<ts>[\d.]+) (?P<msg>\w+)(?: (?P<ext>\S+))?", msglog, re.MULTILINE
        )
        self.runstarttime = None
        self.runendtime = None
        self.prerunstart = None
        self.activestarttime = None
        self.activeendtime = None
        self.runstate = "INIT"
        for m in ms:
            ts = datetime.fromtimestamp(float(m["ts"]))
            if m["msg"] == "Starting":
                self.runstarttime = ts
            elif (m["msg"] == "Stage") and (m["ext"] == "PRERUN"):
                self.prerunstart = ts
                self.runstate = "RUNNING"
            elif (
                (m["msg"] == "Stage")
                and (self.activestarttime is None)
                and (self.prerunstart is not None)
            ):
                self.activestarttime = ts
            elif (m["msg"] == "Stage") and (m["ext"] == "POSTRun"):
                self.activeendtime = ts
            elif m["msg"] == "Ended":
                self.runendtime = ts
                self.runstate = "COMPLETE"
            elif m["msg"] == "Aborted":
                self.runstate = "ABORTED"
                self.activeendtime = ts
                self.runendtime = ts
            elif m["msg"] == "Stopped":
                self.runstate = "STOPPED"
                self.activeendtime = ts
                self.runendtime = ts

        tt = []

        try:
            # In normal runs, the protocol is there without the PROT command at the
            # beginning of the log as an info command, with quote.message.  Let's
            # try to grab it!
            if m := re.match(
                r"^Info (?:[\d.]+) (<quote.message>.*?</quote.message>)",
                msglog,
                re.DOTALL | re.MULTILINE,
            ):
                # We can get the prot name too, and sample volume!
                rp = re.search(
                    r"NEXT RP (?:-CoverTemperature=(?P<ct>[\d.]+) )?(?:-SampleVolume=(?P<sv>[\d.]+) )?([\w-]+) (?P<protoname>[\w-]+)",
                    msglog,
                    re.IGNORECASE,
                )
                if rp:
                    prot = Protocol.from_command(f"PROT {rp['protoname']} {m[1]}")
                    if rp[1]:
                        prot.volume = float(rp["sv"])
                else:
                    prot = Protocol.from_command(f"PROT unknown_name {m[1]}")
                self._protocol_from_log = prot
            else:
                self._protocol_from_log = None
        except ValueError:
            self._protocol_from_log = None

        for m in re.finditer(
            r"^Temperature ([\d.]+) -sample=([\d.,]+) -heatsink=([\d.,]+) "
            r"-cover=([\d.,]+) -block=([\d.,]+)$",
            msglog,
            re.MULTILINE,
        ):
            r = []
            r.append(float(m[1]))
            r += [float(x) for x in m[2].split(",")]
            r.append(float(m[3]))
            r.append(float(m[4]))
            r += [float(x) for x in m[5].split(",")]
            tt.append(r)

        if len(tt) > 0:
            self.num_zones = int((len(tt[0]) - 3) / 2)

            self.temperatures = pd.DataFrame(
                tt,
                columns=pd.MultiIndex.from_tuples(
                    [(cast(str, "time"), cast(Union[str, int], "timestamp"))]
                    + [("sample", n) for n in range(1, self.num_zones + 1)]
                    + [("other", "heatsink"), ("other", "cover")]
                    + [("block", n) for n in range(1, self.num_zones + 1)]
                ),
            )

            if self.activestarttime:
                self.temperatures[("time", "seconds")] = (
                    self.temperatures[("time", "timestamp")]
                    - self.activestarttime.timestamp()
                )
                self.temperatures[("time", "hours")] = (
                    self.temperatures[("time", "seconds")] / 3600.0
                )
            self.temperatures[("sample", "avg")] = self.temperatures["sample"].mean(
                axis=1
            )
            self.temperatures[("block", "avg")] = self.temperatures["block"].mean(
                axis=1
            )

        else:
            self.num_zones = None
            self.temperatures = None

    def _find_anneal_stages(self) -> list[int]:
        anneal_stages = []
        for i, stage in enumerate(self.protocol.stages):
            if any(
                getattr(step, "collect", False)
                and (getattr(step, "temp_increment", 0.0) < 0.0)
                for step in stage.steps
            ):
                anneal_stages.append(i + 1)
        return anneal_stages

    def _find_melt_stages(
        self, anneal_stages: Sequence[int] | None
    ) -> tuple[list[int], list[int]]:
        melt_stages = []
        for i, stage in enumerate(self.protocol.stages):
            if any(
                getattr(step, "collect", False)
                and (getattr(step, "temp_increment", 0.0) > 0.0)
                for step in stage.steps
            ):
                melt_stages.append(i + 1)

        if (
            (anneal_stages is not None)
            and (len(anneal_stages) > 0)
            and (len(melt_stages) > 0)
            and (max(anneal_stages) + 1 < min(melt_stages))
        ):
            between_stages = [
                x for x in range(max(anneal_stages) + 1, min(melt_stages))
            ]
        else:
            between_stages = []

        return melt_stages, between_stages

    def plot_anneal_melt(
        self,
        samples: str | Sequence[str] | None = None,
        filters: str | FilterSet | Collection[str | FilterSet] | None = None,
        anneal_stages: int | Sequence[int] | None = None,
        melt_stages: int | Sequence[int] | None = None,
        between_stages: int | Sequence[int] | None = None,
        normalization: Normalizer = NormRaw(),
        ax: plt.Axes | None = None,
        marker: str | None = None,
        legend: bool = True,
        figure_kw: Mapping[str, Any] | None = None,
        line_kw: Mapping[str, Any] | None = None,
    ) -> plt.Axes:
        """
        Plots anneal/melt curves.

        This uses solid lines for the anneal, dashed lines for the melt, and
        dotted lines for anything "between" the anneal and melt (for example, a temperature
        hold).

        Line labels are intended to provide full information when read in combination with
        the axes title.  They will only include information that does not apply to all
        lines.  For example, if every line is from the same filter set, but different
        samples, then only the sample will be shown.  If every line is from the same sample,
        but different filter sets, then only the filter set will be shown. Wells are shown
        if a sample has multiple wells.

        samples
            Either a reference to a single sample (a string), or a list of sample names.
            Well names may also be included, in which case each well will be treated without
            regard to the sample name that may refer to it.  Note this means you cannot
            give your samples names that correspond with well references.

        filters
            Optional. A filterset (string or `FilterSet`) or list of filtersets to include in the
            plot.  Multiple filtersets will be plotted *on the same axes*.  Optional;
            if None, then all filtersets with data in the experiment will be included.

        anneal_stages, melt_stages, between_stages: int | Sequence[int] | None
            Optional. A stage or list of stages (integers, starting from 1), corresponding to the
            anneal, melt, and stages between the anneal and melt (if any).  Any of these
            may be None, in which case the function will try to determine the correct values
            automatically.

        normalization
            Optional. A Normalizer instance to apply to the data.  By default, this is NormRaw, which
            passes through raw fluorescence values.  NormToMeanPerWell also works well.

        marker

        ax
            Optional.  An axes to put the plot on.  If not provided, the function will
            create a new figure.

        legend
            Optional.  Determines whether a legend is included.

        figure_kw
            Optional.  A dictionary of options passed through as keyword options to
            the figure creation.  Only applies if ax is None.

        line_kw
            Optional.  A dictionary of keywords passed to all three plotting commands.

        """

        if filters is None:
            filters = self.all_filters

        if isinstance(samples, str):
            samples = [samples]
        elif samples is None:
            samples = list(self.plate_setup.sample_wells.keys())

        filters = _normalize_filters(filters)

        if anneal_stages is None:
            anneal_stages = self._find_anneal_stages()
        elif isinstance(anneal_stages, int):
            anneal_stages = [anneal_stages]
        else:
            anneal_stages = list(anneal_stages)

        if melt_stages is None:
            melt_stages, between_stages = self._find_melt_stages(anneal_stages)
        elif isinstance(melt_stages, int):
            melt_stages = [melt_stages]
        else:
            melt_stages = list(melt_stages)

        if between_stages is None:
            between_stages = []
        elif isinstance(between_stages, int):
            between_stages = [between_stages]
        else:
            between_stages = list(between_stages)

        if ax is None:
            ax = cast(
                plt.Axes,
                plt.figure(**({} if figure_kw is None else figure_kw)).add_subplot(),
            )

        data = normalization.normalize_scoped(self.welldata, "all")

        all_wells = self.plate_setup.get_wells(samples)

        reduceddata = data.loc[[f.lowerform for f in filters], all_wells]

        reduceddata = normalization.normalize_scoped(reduceddata, "limited")

        for filter in filters:
            filterdat: pd.DataFrame = reduceddata.loc[filter.lowerform, :]  # type: ignore

            annealdat: pd.DataFrame = filterdat.loc[anneal_stages, :]  # type: ignore
            meltdat: pd.DataFrame = filterdat.loc[melt_stages, :]  # type: ignore

            if len(between_stages) > 0:
                betweendat: pd.DataFrame = filterdat.loc[between_stages, :]  # type: ignore

            for sample in samples:
                wells = self.plate_setup.get_wells(sample)

                for well in wells:
                    color = next(ax._get_lines.prop_cycler)["color"]

                    label = _gen_label(sample, well, filter, samples, wells, filters)

                    anneallines = ax.plot(
                        annealdat.loc[:, (well, "st")],
                        annealdat.loc[:, (well, "fl")],
                        color=color,
                        label=label,
                        marker=marker,
                        **(line_kw if line_kw is not None else {}),
                    )

                    meltlines = ax.plot(
                        meltdat.loc[:, (well, "st")],
                        meltdat.loc[:, (well, "fl")],
                        color=color,
                        linestyle="dashed",
                        marker=marker,
                        **(line_kw if line_kw is not None else {}),
                    )

                    if len(between_stages) > 0:
                        betweenlines = ax.plot(
                            betweendat.loc[:, (well, "st")],
                            betweendat.loc[:, (well, "fl")],
                            color=color,
                            linestyle="dotted",
                            marker=marker,
                            **(line_kw if line_kw is not None else {}),
                        )

        ax.set_xlabel("temperature (°C)")

        # FIXME: consider normalization
        ax.set_ylabel("fluorescence")

        if legend:
            ax.legend()

        ax.set_title(
            _gen_axtitle(
                self.name,
                anneal_stages + between_stages + melt_stages,
                samples,
                wells,
                filters,
            )
        )

        return ax

    def plot_over_time(
        self,
        samples: str | Sequence[str] | None = None,
        filters: str | FilterSet | Collection[str | FilterSet] | None = None,
        stages: slice | int | Sequence[int] = slice(None),
        normalization: Normalizer = NormRaw(),
        ax: plt.Axes | Sequence[plt.Axes] | None = None,
        legend: bool = True,
        temperatures: Literal[False, "axes", "inset", "twin"] = False,
        marker: str | None = None,
        figure_kw: Mapping[str, Any] | None = None,
        line_kw: Mapping[str, Any] | None = None,
    ) -> Sequence[plt.Axes]:
        """
        Plots fluorescence over time, optionally with temperatures over time.

        Line labels are intended to provide full information when read in combination with
        the axes title.  They will only include information that does not apply to all
        lines.  For example, if every line is from the same filter set, but different
        samples, then only the sample will be shown.  If every line is from the same sample,
        but different filter sets, then only the filter set will be shown. Wells are shown
        if a sample has multiple wells.

        samples
            Either a reference to a single sample (a string), or a list of sample names.
            Well names may also be included, in which case each well will be treated without
            regard to the sample name that may refer to it.  Note this means you cannot
            give your samples names that correspond with well references.

        filters
            Optional. A filterset (string or `FilterSet`) or list of filtersets to include in the
            plot.  Multiple filtersets will be plotted *on the same axes*.  Optional;
            if None, then all filtersets with data in the experiment will be included.

        stages
            Optional.  A stage, list of stages, or slice (all using integers starting
            from 1), to include in the plot.  By default, all stages are plotted.
            For example, to plot stage 2, use `stages=2`; to plot stages 2 and 4, use
            `stages=[2, 4]`, to plot stages 3 through 15, use `stages=slice(3, 16)`
            (Python ranges are exclusive on the end).  Note that is a slice, you
            can use `None` instead of a number to denote the beginning/end.

        normalization
            Optional. A Normalizer instance to apply to the data.  By default, this is NormRaw, which
            passes through raw fluorescence values.  NormToMeanPerWell also works well.

        temperatures
            Optional (default False).  Several alternatives for displaying temperatures.
            "axes" uses a separate axes (created if ax is not provided, otherwise ax must
            be a list of two axes).

            Temperatures are from Experiment.temperature, and are thus the temperatures
            as recorded during the run, not the set temperatures.  Note that this has a
            *very* large number of data points, something that should be dealt with at
            some point.

        ax
            Optional.  An axes to put the plot on.  If not provided, the function will
            create a new figure.  If `temperatures="axes"`, however, you must provide
            a list or tuple of *two* axes, the first for fluorescence, the second
            for temperature.

        legend
            Optional.  Determines whether a legend is included.

        figure_kw
            Optional.  A dictionary of options passed through as keyword options to
            the figure creation.  Only applies if ax is None.

        line_kw
            Optional.  A dictionary of keywords passed to fluorescence plot commands.

        """

        if filters is None:
            filters = self.all_filters

        filters = _normalize_filters(filters)

        if isinstance(samples, str):
            samples = [samples]
        elif samples is None:
            samples = list(self.plate_setup.sample_wells.keys())

        if isinstance(stages, int):
            stages = [stages]

        fig = None

        if ax is None:
            if temperatures:
                fig, ax = plt.subplots(
                    2,
                    1,
                    sharex="all",
                    gridspec_kw={"height_ratios": [3, 1]},
                    **({} if figure_kw is None else figure_kw),
                )
            else:
                fig, ax = plt.subplots(1, 1, **({} if figure_kw is None else figure_kw))
                ax = [ax]

        elif (not isinstance(ax, (Sequence, np.ndarray))) or isinstance(ax, plt.Axes):
            ax = [ax]

        ax = cast(Sequence[plt.Axes], ax)

        data = normalization.normalize_scoped(self.welldata, "all")

        all_wells = self.plate_setup.get_wells(samples) + ["time"]

        reduceddata = data.loc[[f.lowerform for f in filters], all_wells]

        reduceddata = normalization.normalize_scoped(reduceddata, "limited")

        for filter in filters:
            filterdat: pd.DataFrame = reduceddata.loc[filter.lowerform, :]  # type: ignore

            for sample in samples:
                wells = self.plate_setup.get_wells(sample)

                for well in wells:
                    color = next(ax[0]._get_lines.prop_cycler)["color"]

                    label = _gen_label(sample, well, filter, samples, wells, filters)

                    lines = ax[0].plot(
                        filterdat.loc[stages, ("time", "hours")],
                        filterdat.loc[stages, (well, "fl")],
                        color=color,
                        label=label,
                        marker=marker,
                        **(line_kw if line_kw is not None else {}),
                    )

        ax[-1].set_xlabel("time (hours)")

        # FIXME: consider normalization
        ax[0].set_ylabel("fluorescence")

        if legend:
            ax[0].legend()

        if temperatures == "axes":
            if len(ax) < 2:
                raise ValueError("Temperature axes requires at least two axes in ax")

            xlims = ax[0].get_xlim()

            tmin, tmax = np.inf, 0.0
            for i, fr in reduceddata.groupby("filter_set", as_index=False):
                d = fr.loc[i, ("time", "hours")].loc[stages]
                tmin = min(tmin, d.min())
                tmax = max(tmax, d.max())

            reltemps = self.temperatures.loc[
                lambda x: (tmin <= x[("time", "hours")])
                & (x[("time", "hours")] <= tmax),
                :,
            ]

            for x in range(1, 7):
                ax[1].plot(
                    reltemps.loc[:, ("time", "hours")],
                    reltemps.loc[:, ("sample", x)],
                )

            ax[0].set_xlim(xlims)
            ax[1].set_xlim(xlims)

            ax[1].set_ylabel("temperature (°C)")

        ax[0].set_title(_gen_axtitle(self.name, stages, samples, wells, filters))

        return ax

    def plot_temperatures(self):
        raise NotImplemented


def _normalize_filters(
    filters: str | FilterSet | Collection[str | FilterSet],
) -> list[FilterSet]:
    if isinstance(filters, str) or isinstance(filters, FilterSet):
        filters = [filters]
    return [FilterSet.fromstring(filter) for filter in filters]


def _gen_label(sample, well, filter, samples, wells, filters) -> str:
    label = ""
    if len(samples) > 1:
        label = str(sample)
    if len(wells) > 1:
        if len(label) > 0:
            label += f" ({well})"
        else:
            label = str(well)
    if len(filters) > 1:
        if len(label) > 0:
            label += f", {filter}"
        else:
            label = str(filter)

    return label


def _gen_axtitle(expname, stages, samples, wells, filters) -> str:
    elems = []
    if len(samples) == 1:
        elems += samples
    if len(filters) == 1:
        elems += [str(f) for f in filters]

    val = expname
    if stages != slice(None):
        val += f", stage {_pp_seqsliceint(stages)}"

    if len(elems) > 0:
        val += ": " + ", ".join(elems)

    return val
