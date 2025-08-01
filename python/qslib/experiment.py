# SPDX-FileCopyrightText: 2021 - 2023 Constantine Evans <qslib@mb.costi.net>
#
# SPDX-License-Identifier: EUPL-1.2

"""Experiment class and related.
"""
from __future__ import annotations

import base64
import io
import json
import logging
import os
import re
import tempfile
import polars as pl
import xml.etree.ElementTree as ET
import zipfile
from dataclasses import InitVar, dataclass, field
from datetime import datetime, timezone
from glob import glob
from pathlib import Path
from typing import (
    IO,
    TYPE_CHECKING,
    Any,
    Callable,
    Collection,
    List,
    Literal,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
)
from warnings import warn

import numpy as np
import pandas as pd
import toml as toml

from qslib.plate_setup import PlateSetup, _SampleWellsView
from qslib.scpi_commands import AccessLevel, SCPICommand

from ._analysis_protocol_text import _ANALYSIS_PROTOCOL_TEXT
from ._util import _nowuuid, _pp_seqsliceint, _set_or_create, cached_method
from .base import RunStatus
from .data import (
    FilterDataReading,
    FilterSet,
    _filterdata_df_v2,
    _parse_analysis_result,
    _parse_multicomponent_data_v1,
    _parse_multicomponent_data_v2,
    df_from_readings,
)
from .machine import Machine
from .processors import NormRaw, Processor
from .protocol import Protocol, Stage, Step
from .version import __version__

if TYPE_CHECKING:  # pragma: no cover
    from matplotlib.axes import Axes
    from matplotlib.lines import Line2D
    import polars as pl

# Let's just assume all of these are problematic, for now.
INVALID_NAME_RE = re.compile(r"[\[\]{}!/:;@&=+$,?#|\\]")


def _safe_exp_name(name: str) -> str:
    m = INVALID_NAME_RE.search(name)

    if m:
        raise ValueError(f"Invalid characters ({m[0]}) in run name: {name}.")

    return name.replace(" ", "_")


TEMPLATE_NAME = "ruo"

_MANIFEST_CONTENTS = f"""Manifest-Version: 1.0
Content-Type: Std
Implementation-Title: QSLib
Implementation-Version: {__version__}
Specification-Vendor: Applied Biosystems
Specification-Title: Experiment Document Specification
Specification-Version: 1.3.2
"""

log = logging.getLogger(__name__)


MachineReference = Union[str, Machine]


@dataclass
class AlreadyStartedError(ValueError):
    """The experiment has already been started."""

    name: str
    state: str

    def __str__(self) -> str:
        return (
            f"Experiment {self.name} is recorded as already having started."
            f"Current state of the object is {self.state}."
        )


@dataclass
class MachineError(Exception):
    """Base class for an error from a machine."""

    host: str = field(init=False)
    port: int | str | None = field(init=False)
    machine: InitVar[Machine]

    def __post_init__(self, machine: Machine) -> None:
        self.host = machine.host
        self.port = machine.port


@dataclass
class NotRunningError(MachineError):
    """The named experiment is not currently running."""

    run: str
    current: RunStatus

    def __str__(self) -> str:
        return (
            f"{self.run} is not running on {self.host}."
            f" Current status is {self.current}."
        )


@dataclass
class MachineBusyError(MachineError):
    "The machine is busy."
    current: RunStatus

    def __str__(self) -> str:
        return f"Machine {self.host}:{self.port} is currently busy: {self.current}."


@dataclass
class AlreadyExistsError(MachineError):
    "A run already exists with the same name."
    name: str

    def __str__(self) -> str:
        return f"Run {self.name} exists."


@dataclass
class AlreadyExistsWorkingError(AlreadyExistsError):
    "A run already exists in uncollected (experiment:) with the same name."
    name: str

    def __str__(self) -> str:
        return (
            f"Run {self.name} exists in machine working directory. This may be caused by "
            'a failed previous run.  Start run with `overwrite="incomplete"` or `True` to overwrite.'
        )


@dataclass
class AlreadyExistsCompleteError(AlreadyExistsError):
    "A run already exists in uncollected (experiment:) with the same name."
    name: str

    def __str__(self) -> str:
        return (
            f"Run {self.name} is in completed run storage. This may be caused by "
            "a failed previous run.  Start run with `overwrite=True` to overwrite."
        )


def _find_or_raise(e: ET.ElementTree | ET.Element, path: str) -> ET.Element:
    "Find an element at path and return it, or raise an error."
    x = e.find(path)
    if x is None:
        raise ValueError(f"{path} not found in {x}.")
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

    >>> machine = Machine("localhost", password="password")
    >>> exp = Experiment.from_running(machine)

    Or from the machine's storage:

    >>> exp = Experiment.from_machine(machine, "experiment")

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
    _filter_data: pd.DataFrame | None = None
    _multicomponent_data: pd.DataFrame | None = None
    _analysis_result: pd.DataFrame | None = None
    _amplification_data: pd.DataFrame | None = None


    num_zones: int
    """The number of temperature zones (excluding cover), or -1 if not known."""
    spec_major_version: int = 1

    @property
    def all_filters(self) -> Collection[FilterSet]:
        """All filters used at some point in the experiment.

        If the experiment has data, this is based on the existing data.  Otherwise, it is based
        on the experiment protocol.
        """
        if self._filter_data is not None:
            return [
                FilterSet.fromstring(f)
                for f in self.welldata.index.get_level_values(0).unique()
            ]
        return self.protocol.all_filters

    @property
    def filter_strings(self) -> list[str]:
        """All filters, as x?-m? strings, used at some point in the experiment."""
        return [str(f) for f in self.all_filters]

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
        try:
            return self.filter_data
        except Exception as e:
            if self.runstate == "INIT":
                raise ValueError("Run hasn't started yet: no data available.")
            else:
                raise ValueError("Experiment data is not available")


    def summary(self, format: str = "markdown", plate: str = "list") -> str:
        return self.info(format, plate)

    def info(self, format: str = "markdown", plate: str = "list") -> str:
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
        if self.runstate != "INIT":
            s += "- Available data types: " + ", ".join(self.available_data()) + "\n"
        s += f"- Written by: {self.writesoftware}\n"
        s += f"- Read by: QSLib {__version__}\n"
        return s

    def available_data(self) -> list[str]:
        d = []
        if self._filter_data is not None:
            d.append("filter_data")
        if self._multicomponent_data is not None:
            d.append("multicomponent_data")
        if self._amplification_data is not None:
            d.append("amplification_data")
        if self._analysis_result is not None:
            d.append("analysis_result")
        if self.temperatures is not None:
            d.append("temperatures")
        return d

    def info_html(self) -> str:
        """Create a self-contained HTML summary (returned as a string, but very large) of the experiment."""
        summary = self.info(plate="table")

        import matplotlib.pyplot as plt

        fig, ax = plt.subplots(figsize=(21.0 / 2.54, 15.0 / 2.54))
        self.protocol.plot_protocol(ax)

        o = io.StringIO()
        fig.savefig(o, format="svg")
        inner = re.search("<svg.*</svg>", o.getvalue(), re.DOTALL)
        if inner is None:
            raise Exception
        innersvg = inner[0]

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

{innersvg}
</body>
</html>
        """

        return s

    def __str__(self) -> str:
        return self.info()

    def _repr_markdown_(self) -> str:
        if len(self.plate_setup.sample_wells) <= 12:
            return self.info()
        else:
            return self.info(plate="table")

    @property
    def runtitle_safe(self) -> str:
        """Run name with " " replaced by "_"; raises ValueError if name has other problematic characters."""
        return _safe_exp_name(self.name)

    @property
    def raw_data(self) -> pd.DataFrame:
        return self.welldata

    def _ensure_machine(
        self,
        machine: MachineReference | None,
        password: str | None = None,
        needed_level: AccessLevel = AccessLevel.Observer,
    ) -> Machine:
        if isinstance(machine, Machine):
            self.machine = machine
            self.machine.max_access_level = max(
                self.machine.max_access_level, needed_level
            )
            return machine
        elif isinstance(machine, str):
            self.machine = Machine(
                machine,
                password=password,
                max_access_level=needed_level,
            )
            return self.machine
        elif hasattr(self, "machine") and (c := self.machine):
            c.max_access_level = max(c.max_access_level, needed_level)
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

    def _populate_folder(self, machine: Machine) -> None:
        tempzip = io.BytesIO()
        self.save_file(tempzip)
        exppath = self.runtitle_safe + "/"
        machine.run_command_bytes(
            b"EXP:ZIPWRITE "
            + exppath.encode()
            + b" <message>\n"
            + base64.encodebytes(tempzip.getvalue())
            + b"</message>\n"
        )

    def run(
        self,
        machine: MachineReference | None = None,
        overwrite: Literal[True, False, "incomplete"] = False,
        require_exclusive: bool = False,
        require_drawer_check: bool = True,
    ) -> None:
        """Load the run onto a machine, and start it.

        Parameters
        ----------
        machine
            The machine to run on, by default None, in which case the machine
            associated with the run (if any) is used.
        overwrite: bool or "incomplete", optional
            Whether to overwrite files if a run with the same name already exists.
            If "incomplete", only overwrite if the existing run is an incomplete
            folder (in the experiments: working directory). By default False.
        require_exclusive: bool, optional
            Whether to require exclusive access to the machine, by default False.
        require_drawer_check: bool, optional
            Whether to ensure the drawer is closed before starting.  Note that
            a close command will be sent regardless of this setting.  By default True.

        Raises
        ------
        MachineBusyError
            The machine isn't idle.
        AlreadyExistsError (either AlreadyExistsWorkingError or AlreadyExistsCompleteError)
            The machine already has a folder or eds file with this run name.
        """
        machine = self._ensure_machine(machine, needed_level=AccessLevel.Controller)
        log.info("Attempting to start %s on %s.", self.runtitle_safe, machine.host)

        with machine.ensured_connection():
            # Ensure machine isn't running:
            if (x := machine.run_status()).state.upper() != "IDLE":
                raise MachineBusyError(machine, x)

            if self.runstate != "INIT":
                raise AlreadyStartedError(self.runtitle_safe, self.runstate)

            with machine.at_access(AccessLevel.Controller, exclusive=require_exclusive):
                # Check existence of previous run folder in experiments:
                if self.runtitle_safe + "/" in machine.run_command("EXP:LIST?"):
                    if not overwrite:
                        raise AlreadyExistsWorkingError(machine, self.runtitle_safe)
                    else:
                        log.warning(
                            f"Found incomplete run folder, deleting per overwrite={overwrite}."
                        )
                        machine.run_command(f"EXP:DELETE {self.runtitle_safe}")

                # Check existence of previous completed run:
                compl_runs = machine.list_files("public_run_complete:", verbose=False)
                reds = f"public_run_complete:{self.runtitle_safe}.eds"
                if reds in compl_runs:
                    if (not overwrite) or (overwrite == "incomplete"):
                        raise AlreadyExistsCompleteError(machine, self.runtitle_safe)
                    else:
                        log.warning(
                            f"Found complete run, but overwriting per overwrite={overwrite}."
                        )
                        machine.run_command(f"FILE:REMOVE {reds}")

                log.debug("Powering on machine and ensuring drawer/cover is closed.")
                # Ensure machine state and power.
                machine.power = True

                machine.drawer_close(lower_cover=True, check=require_drawer_check)

                log.debug("Creating experiment folder %s.", self.runtitle_safe)
                machine.run_command(f"EXP:NEW {self.runtitle_safe} {TEMPLATE_NAME}")

                log.debug("Setting up experiment.xml")
                self.runstarttime = datetime.now()
                self.runstate = "RUNNING"
                self._update_files()

                log.debug("Populating experiment folder.")
                self._populate_folder(machine)

                log.debug("Sending protocol.")
                machine.define_protocol(self.protocol)

                log.debug("Sending run command.")
                # Start the run
                machine.run_command_to_ack(
                    f"RP -samplevolume={self.protocol.volume} -runmode={self.protocol.runmode}"
                    f" {self.protocol.name} {self.runtitle_safe}"
                )  # TODO: user, cover
                log.info("Run %s started on %s.", self.runtitle_safe, machine.host)

    def pause_now(self, machine: MachineReference | None = None) -> None:
        """
        If this experiment is running, pause it (immediately).

        Requires and takes exclusive Controller access on the machine.

        Raises
        ------
        NotRunningError
            the experiment is not currently running
        """
        machine = self._ensure_machine(machine, needed_level=AccessLevel.Controller)
        with machine.ensured_connection(AccessLevel.Controller):
            self._ensure_running(machine)
            machine.pause_current_run()

    def resume(self, machine: MachineReference | None = None) -> None:
        """
        If this experiment is running, resume it.

        Requires and takes exclusive Controller access on the machine.

        Raises
        ------
        NotRunningError
            the experiment is not currently running
        """
        machine = self._ensure_machine(machine, needed_level=AccessLevel.Controller)
        with machine.ensured_connection(AccessLevel.Controller):
            self._ensure_running(machine)
            machine.resume_current_run()

    def stop(self, machine: MachineReference | None = None) -> None:
        """
        If this experiment is running, stop it after the end of the current cycle.

        Requires and takes exclusive Controller access on the machine.

        Raises
        ------
        NotRunningError
            the experiment is not currently running
        """
        machine = self._ensure_machine(machine, needed_level=AccessLevel.Controller)
        with machine.ensured_connection(AccessLevel.Controller):
            self._ensure_running(machine)
            machine.stop_current_run()

    def abort(self, machine: MachineReference | None = None) -> None:
        """
        If this experiment is running, abort it, stopping it immediately.

        Requires and takes exclusive Controller access on the machine.

        Raises
        ------
        NotRunningError
            the experiment is not currently running
        """
        machine = self._ensure_machine(machine)
        with machine.ensured_connection(AccessLevel.Controller):
            self._ensure_running(machine)
            machine.abort_current_run()

    def get_status(self, machine: MachineReference | None = None) -> RunStatus:
        """
        Return the status of the experiment, if currently running.

        Requires Observer access on the machine.

        Raises
        ------
        NotRunningError
            the experiment is not currently running
        """
        machine = self._ensure_machine(machine)
        with machine.ensured_connection(AccessLevel.Observer):
            return self._ensure_running(machine)

    @classmethod
    def _from_running_via_sync(
        cls, machine: MachineReference, include_tiffs: bool = False
    ) -> "Experiment":
        exp = cls(_create_xml=False)

        exp.root_dir.mkdir(parents=True)

        machine = exp._ensure_machine(machine)

        with machine.ensured_connection():
            crt: str | None = machine.current_run_name

            if not crt:
                raise ValueError("Nothing is currently running.")

            exp.name = crt

            exp.sync_from_machine(
                machine, log_method="copy", include_tiffs=include_tiffs
            )

            exp._update_from_files()

        return exp

    def sync_from_machine(
        self,
        machine: MachineReference | None = None,
        log_method: Literal["copy", "eval"] = "eval",
        include_tiffs: bool = False,
    ) -> None:
        """
        Try to synchronize the data in the experiment to the current state of the run on a
        machine, more efficiently than reloading everything.
        """
        machine = self._ensure_machine(machine)
        self._clear_cache()

        with machine.ensured_connection(AccessLevel.Observer):
            # Get a list of all the files in the experiment folder
            machine_files = machine.list_files(
                f"experiments:{self.runtitle_safe}/", verbose=True, recursive=True
            )

            # Transfer anything we don't have
            for f in machine_files:
                name: str = os.path.basename(f["path"])

                if name.lower().endswith("tiff") and not include_tiffs:
                    continue

                sdspath = self._sdspath(
                    re.sub(".*/apldbio/sds/(.*)$", r"\1", f["path"])
                )
                log.debug(f"checking {f['path']} mtime {f['mtime']} to {sdspath}")
                if os.path.exists(sdspath) and os.path.getmtime(sdspath) >= float(
                    f["mtime"].timestamp()
                ):
                    log.debug(f"{sdspath} has {os.path.getmtime(sdspath)}")
                    continue
                from pathlib import Path  # FIXME

                log.info(f"Updating {sdspath}")

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
                                    f" falling back to copy. ('{checklocal.decode()}' ≠ '{rdat[0:20].decode()}')"
                                )
                                b.seek(0)
                                b.write(machine.read_file(f["path"]))
                else:
                    raise NotImplementedError
                os.utime(sdspath, (f["atime"].timestamp(), f["mtime"].timestamp()))

            # The message log is tricky. Ideally we'd use rsync or wc+tail. TODO
            self._update_from_files()

    def change_protocol_from_now(
        self, new_stages: Sequence[Stage], machine: MachineReference | None = None
    ) -> None:
        """
        For a running experiment, change the remaining stages to be the provided :param:`new_stages` list.
        This is a convenience function that:

        1. Gets the currently-running stage and cycle.
        2. Sets the repeat number of the current stage to its current cycle, thus ending it after the end of
           the current cycle.
        3. Changes the remainder of the stages to be those in the :param:`new_stages` list.

        Because this does not impact any current or past stages, there is less of a need to ensure that the
        stages provided are compatible with the old protocol.  The only check done is to ensure that, if
        the provided stages have any collection commands using default filters, the old protocol has specified
        default filters.  This function does not allow the default filters to be changed: if you want to use
        filters other than the defaults, or defaults were not provided in the old protocol, then either specify
        filters explicitly (recommended) for new stages you'd like to be different, or use
        :any:`Experiment.change_protocol` directly.
        """
        self._clear_cache()
        machine = self._ensure_machine(machine, needed_level=AccessLevel.Controller)
        with machine.ensured_connection(AccessLevel.Controller):
            self._ensure_running(machine)

            proto = machine.get_running_protocol()

            runstatus = machine.run_status()

            proto.stages[-1].repeat = runstatus.cycle
            proto.stages += new_stages

            machine.define_protocol(proto)
            self.protocol = proto

            self._update_tcprotocol_xml()

            # Push new tcprotocol.xml
            machine.write_file(
                "${LogFolder}/tcprotocol.xml",
                open(self._sdspath("tcprotocol.xml"), "rb").read(),
            )
            machine.write_file(
                "${LogFolder}/qsl-tcprotocol.xml",
                open(self._sdspath("qsl-tcprotocol.xml"), "rb").read(),
            )

    def change_protocol(
        self,
        new_protocol: Protocol,
        machine: MachineReference | None = None,
        force: bool = False,
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
        self._clear_cache()
        machine = self._ensure_machine(machine, needed_level=AccessLevel.Controller)
        with machine.ensured_connection(AccessLevel.Controller):
            if not force:
                self._ensure_running(machine)

            runstatus = machine.run_status()

            # Get running protocol and status.
            machine_proto = machine.get_running_protocol()

            # FIXME: can't just do this, may have extra default things added
            # if machine_proto != self.protocol:
            #    raise ValueError("Machine and experiment protocols differ.")

            if not force:
                # Check compatibility of changes.
                machine_proto.check_compatible(new_protocol, runstatus)

            new_protocol.name = machine_proto.name
            new_protocol.volume = machine_proto.volume
            new_protocol.runmode = machine_proto.runmode

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
            machine.write_file(
                "${LogFolder}/qsl-tcprotocol.xml",
                open(self._sdspath("qsl-tcprotocol.xml"), "rb").read(),
            )

    #     def change_plate_setup(self, new_plate_setup: Optional[PlateSetup], machine: MachineReference | None = None,
    # ):
    #         """
    #         Change the plate setup for the experiment, including on the machine where it is running.

    #         """

    def save_file(
        self,
        path_or_stream: str | os.PathLike[str] | io.BytesIO | IO[bytes] = ".",
        overwrite: bool = False,
        update_files: bool = True,
    ) -> None:
        """
        Save an EDS file of the experiment. This *should* be readable by AB's software,
        but makes no attempt to hide that it was written by QSLib, and contains some other
        information. By default, this will refuse to overwrite an existing file.

        Parameters
        ----------
        path_or_stream : str or os.PathLike[str] or io.BytesIO
            A filename, open binary IO, or directory.  If a directory, the file will
            be saved with the name from `Experiment.runtitle_safe`.
        overwrite : bool, optional
            If True, overwrite any existing file without warning. Defaults to False.
        update_files: bool, optional
            If True (default), update files before saving. Use False if you don't want
            qslib to touch the files, for example, if you have just loaded a run from
            the machine and don't want qslib to change anything based on its interpretation.
        """
        mode: Literal["w", "x"]
        if overwrite:
            mode = "w"
        else:
            mode = "x"

        if update_files:
            self._update_files()

        if isinstance(path_or_stream, (IO, io.BytesIO)):
            path: Union[Path, IO] = path_or_stream
        else:
            path = Path(path_or_stream)
            if path.is_dir():
                path = (path / (self.runtitle_safe + ".eds"))

        with zipfile.ZipFile(path, mode) as z:
            for root, _, files in os.walk(self._dir_base):
                for file in files:
                    fpath = os.path.join(root, file)
                    z.write(fpath, os.path.relpath(fpath, self._dir_base))

    @property
    def sample_wells(self) -> _SampleWellsView:
        """A dictionary of sample names to sample wells (convenience read/write access to the :class:`PlateSetup` ."""
        return self.plate_setup.sample_wells

    @sample_wells.setter
    def sample_wells(self, new_sample_wells: dict[str, list[str]]) -> None:
        raise NotImplementedError
        # self.plate_setup.sample_wells = new_sample_wells

    @property
    @cached_method
    def temperature_ramps_polars(self) -> pl.DataFrame:
        m = self._msglog_path().read_bytes()
        r = re.compile(rb"Info (?P<timestamp>[\d.]+) TBC:SETTing ramping \"Zone(?P<zone>\d+)\" from (?P<from>[\d.]+) to (?P<to>[\d.]+) \(rate=(?P<rate>[\d.]+), timeout=(?P<timeout>[\d.]+), msgid=0x(?P<msgid>\w+)\)")

        tbc_events = pl.from_records(
            [
                (float(t), int(z), float(f), float(tt), float(r), float(to), int(mid, 16))
                for t, z, f, tt, r, to, mid in r.findall(m)
            ],
            schema=["timestamp", "zone", "from", "to", "rate", "timeout", "msgid"],
            orient="row"
        )
        tbc_events = tbc_events.with_columns(
            timestamp = (pl.col("timestamp")*1000).cast(pl.Datetime(time_unit="ms", time_zone="UTC"))
        )
        return tbc_events

    @property
    @cached_method
    def temperature_setpoints_polars(self) -> pl.DataFrame:
        tbc_events = self.temperature_ramps_polars.lazy()
        sandeevent = tbc_events.select(
            pl.col("timestamp").alias("start_timestamp"),
            (pl.col("timestamp") + (pl.col("to") - pl.col("from")).abs() / pl.col("rate")).alias("end_timestamp"),
            pl.col("zone"),
            pl.col("from"),
            pl.col("to"),
        )

        sevent = sandeevent.select(timestamp=pl.col("start_timestamp"), zone=pl.col("zone"), temperature=pl.col("from"))
        eevent = sandeevent.select(timestamp=pl.col("end_timestamp"), zone=pl.col("zone"), temperature=pl.col("to"))

        sett = pl.concat([sevent, eevent], how="diagonal").sort("timestamp")
        
        sett = sett.with_columns((pl.col("timestamp")*1000).cast(pl.Datetime(time_unit="ms", time_zone="UTC")))
        return sett.collect()

    @property
    def root_dir(self) -> Path:
        if self.spec_major_version == 1:
            return Path(self._dir_eds)
        else:
            return Path(self._dir_base)

    def __init__(
        self,
        name: str | None = None,
        protocol: Protocol | None = None,
        plate_setup: PlateSetup | None = None,
        _create_xml: bool = True,
    ):
        self._tmp_dir_obj = tempfile.TemporaryDirectory()
        self._dir_base = Path(self._tmp_dir_obj.name)
        self._dir_eds = self._dir_base / "apldbio" / "sds"

        self._protocol_from_qslib: Protocol | None = None
        self._protocol_from_log: Protocol | None = None
        self._protocol_from_xml: Protocol | None = None
        self.runstarttime: datetime | None = None
        self.runendtime: datetime | None = None
        self.activestarttime: datetime | None = None
        self.activeendtime: datetime | None = None

        self.machine: Machine | None = None

        if name is not None:
            # Check if name looks like a file path and warn if it's an existing file
            if isinstance(name, str) and ('/' in name or '\\' in name or name.endswith('.eds')):
                try:
                    if Path(name).is_file():
                        warn(
                            f"The name '{name}' appears to be a file path to an existing file. "
                            f"Did you mean to use Experiment.from_file('{name}') instead of Experiment('{name}')?",
                            UserWarning,
                            stacklevel=2
                        )
                except (OSError, ValueError):
                    # Path validation failed, continue with normal behavior
                    pass
            self.name = name
        else:
            self.name = _nowuuid()

        # Ensure that name is safe for use as a filename:
        # this will raise a ValueError for us if it isn't.
        self.runtitle_safe

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
            self.user: str | None = None
            self.writesoftware = f"QSLib {__version__}"

            self._new_xml_files()
            self._update_files()

    def _new_xml_files(self) -> None:
        os.mkdir(self._dir_base / "apldbio")
        os.mkdir(self._dir_base / "apldbio" / "sds")

        e = ET.ElementTree(ET.Element("Experiment"))

        ET.SubElement(e.getroot(), "Label").text = "ruo"
        tp = ET.SubElement(e.getroot(), "Type")
        ET.SubElement(tp, "Id").text = "Custom"
        ET.SubElement(tp, "Name").text = "Custom"
        ET.SubElement(tp, "Description").text = "Custom QSLib experiment"
        ET.SubElement(tp, "ResultPersisterName").text = "scAnalysisResultPersister"
        ET.SubElement(tp, "ContributedResultPersisterName").text = (
            "mcAnalysisResultPersister"
        )
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
            open(self._dir_eds / "tcprotocol.xml", "wb")
        )

        ET.ElementTree(ET.Element("QSLTCProtocol")).write(
            open(self._dir_eds / "qsl-tcprotocol.xml", "wb")
        )

        with open(self._dir_eds / "Manifest.mf", "w") as f:
            f.write(_MANIFEST_CONTENTS)

        # File will not load in AB without this, even though it is
        # useless for our purposes.
        with open(self._dir_eds / "analysis_protocol.xml", "w") as f:
            f.write(_ANALYSIS_PROTOCOL_TEXT)

    def _sdspath(self, path: str | os.PathLike[str]) -> Path:
        return self._dir_eds / path

    def _update_files(self) -> None:
        self._update_experiment_xml()
        self._update_tcprotocol_xml()
        self._update_platesetup_xml()

    def _update_from_files(self) -> None:
        p = self.root_dir
        if self.spec_major_version == 1:
            if (p / "experiment.xml").is_file():
                self._update_from_experiment_xml()
            if (p / "tcprotocol.xml").is_file():
                self._update_from_tcprotocol_xml()
            if (p / "plate_setup.xml").is_file():
                self._update_from_platesetup_xml()
            if (p / "messages.log").is_file():
                self._update_from_log()
        elif self.spec_major_version == 2:
            manifest = _get_manifest_info(self.root_dir, checkinfo=False)
            self.spec_version = manifest["Specification-Version"]
            self.writesoftware = (
                manifest["Implementation-Title"]
                + " "
                + manifest["Implementation-Version"]
            )
            self._update_from_expdata_v2()
            if (p / "run" / "messages.log").is_file():
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

        z = zipfile.ZipFile(file)

        try:
            manifest_info = _get_manifest_info(z, checkinfo=True)
            exp.spec_major_version = int(manifest_info["Specification-Version"][0])
        except ValueError:
            exp.spec_major_version = 1

        z.extractall(exp._dir_base)

        exp._update_from_files()

        return exp

    @classmethod
    def from_running(cls, machine: MachineReference) -> "Experiment":
        """Create an experiment from the one currently running on a machine.

        Parameters
        ----------
        machine : Machine
            the machine to connect to

        Returns
        -------
        Experiment
            a copy of the running experiment
        """
        exp = cls(_create_xml=False)

        machine = exp._ensure_machine(machine)

        with machine.ensured_connection():
            crt = machine.current_run_name

            if not crt:
                raise ValueError("Nothing is currently running.")

            z = machine.read_dir_as_zip(crt, leaf="EXP")

            z.extractall(exp._dir_base)

            exp._update_from_files()

        return exp

    @classmethod
    def from_uncollected(
        cls, machine: MachineReference, name: str, move: bool = False
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
            a copy of the experiment
        """
        exp = cls(_create_xml=False)

        machine = exp._ensure_machine(machine)

        with machine.ensured_connection():
            if move:
                raise NotImplementedError

            try:
                z = machine.read_dir_as_zip(_safe_exp_name(name), leaf="EXP")
            except IOError: # FIXME
                try:
                    z = machine.read_dir_as_zip(name, leaf="EXP")
                except IOError:
                    raise ValueError(
                        f"Could not find experiment {name} in uncollect runs on {machine}."
                    )

            z.extractall(exp._dir_base)

            exp._update_from_files()

        return exp

    @classmethod
    def latest_from_machine(cls, machine: MachineReference) -> Experiment:
        machine = cls._ensure_machine(machine)
        try:
            return Experiment.from_running(machine)
        except:
            m = machine.list_runs_in_storage(verbose=True)
            m.sort(key = lambda k: k['mtime'])
            return Experiment.from_machine_storage(machine, m[-1]['path'])

    @classmethod
    def from_machine_storage(cls, machine: MachineReference, name: str) -> Experiment:
        """Create an experiment from the machine's storage.

        Parameters
        ----------
        machine : Machine
            the machine to connect to.

        name: str
            the name of the run to collect.

        Returns
        -------
        Experiment
            a copy of the experiment
        """
        exp = cls(_create_xml=False)

        machine = exp._ensure_machine(machine)

        with machine.ensured_connection():
            o = None
            for possible_name in [
                _safe_exp_name(name) + ".eds",
                _safe_exp_name(name),
                name + ".eds",
                name,
            ]:
                try:
                    o = machine.read_file(possible_name, context="public_run_complete")
                    break
                except IOError:
                    continue
            if o is None:
                raise FileNotFoundError(f"Could not find {name} on {machine.host}.")

            z = zipfile.ZipFile(io.BytesIO(o))

            z.extractall(exp._dir_base)

            exp._update_from_files()

        return exp

    @classmethod
    def from_machine(cls, machine: MachineReference, name: str) -> Experiment:
        """Create an experiment from data on a machine, checking the running
        experiment if any, the machine's public_run_complete storage, and the
        machine's uncollected storage.

        Parameters
        ----------
        machine : Machine | str
            the machine to connect to, either as a Machine or a host name

        name: str
            the name of the run

        Returns
        -------
        Experiment
            a copy of the experiment
        """
        if isinstance(machine, str):
            machine = Machine(machine)

        safename = _safe_exp_name(name)

        with machine.ensured_connection():
            if machine.current_run_name in [safename, name]:
                exp = cls.from_running(machine)
                return exp

            storage_runs = machine.list_runs_in_storage()
            if (name in storage_runs) or (safename in storage_runs):
                exp = cls.from_machine_storage(machine, name)
                return exp

            exp_runs = machine.list_files("", verbose=False, leaf="EXP")
            if ((name + "/") in exp_runs) or ((safename + "/") in exp_runs):
                exp = cls.from_uncollected(machine, name)

            else:
                raise FileNotFoundError(f"Could not find run {name} on {machine.host}.")
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
        self.user = exml.findtext("Operator") or None
        self.createdtime = datetime.fromtimestamp(
            float(_find_or_raise(exml, "CreatedTime").text) / 1000.0,  # type: ignore
            tz=timezone.utc
        )
        self.runstate = exml.findtext("RunState") or "UNKNOWN"  # type: ignore

        self._plate_type_id = exml.findtext("PlateTypeID") or None
        if self._plate_type_id == "TYPE_8X12":
            self.plate_type: Literal[96, 384, None] = 96
        elif self._plate_type_id == "TYPE_16X24":
            self.plate_type = 384
        else:
            self.plate_type = None

        self.writesoftware = (
            exml.findtext(
                "ExperimentProperty[@type='RunInfo']/PropertyValue[@key='softwareVersion']/String"
            )
            or "UNKNOWN"
        )
        if x := exml.findtext("RunStartTime"):
            self.runstarttime = datetime.fromtimestamp(float(x) / 1000.0, tz=timezone.utc)
        if x := exml.findtext("RunEndTime"):
            self.runendtime = datetime.fromtimestamp(float(x) / 1000.0, tz=timezone.utc)

    def _update_from_expdata_v2(self) -> None:
        summary = json.load(open(os.path.join(self._dir_base, "summary.json")))
        # fixme: this should go elsewhere, we have it here now because we need it for filterdata

        self.name = summary.get("name", "unknown")

        self.runstate = summary.get("runStatus", "UNKNOWN")
        self.createdtime = datetime.fromtimestamp(
            summary.get("createdTime", 0) / 1000.0, tz=timezone.utc
        )

        self._plate_type_id = summary["blockType"]
        if self._plate_type_id == "BLOCK_384W":
            self.plate_type = 384
        elif self._plate_type_id == "BLOCK_96W":
            self.plate_type = 96
        else:
            raise ValueError(f"Unknown block type {self._plate_type_id}")

    def _update_tcprotocol_xml(self) -> None:
        if self.protocol:
            # exml = ET.parse(os.path.join(self._dir_eds, "tcprotocol.xml"))
            tcxml, qstcxml = self.protocol.to_xml()
            ET.indent(tcxml)
            ET.indent(qstcxml)
            tcxml.write(self.root_dir / "tcprotocol.xml")

            # Make new machine with stripped password:
            if self.machine:
                m2d = self.machine.asdict(password=False)
                ET.SubElement(qstcxml.getroot(), "MachineConnection").text = toml.dumps(
                    m2d
                )

            qstcxml.write(self.root_dir / "qsl-tcprotocol.xml")

    def _update_from_tcprotocol_xml(self) -> None:
        exml = ET.parse(self.root_dir / "tcprotocol.xml")
        if (self.root_dir / "qsl-tcprotocol.xml").is_file():
            qstcxml = ET.parse(self.root_dir / "qsl-tcprotocol.xml")

            if ((x := qstcxml.find("QSLibProtocolCommand")) is not None) and (
                x.text is not None
            ):
                try:
                    self._protocol_from_qslib = Protocol.from_scpicommand(
                        SCPICommand.from_string(x.text)
                    )
                except ValueError:
                    self._protocol_from_qslib = None
            else:
                self._protocol_from_qslib = None

            if (mc := qstcxml.findtext("MachineConnection")) and not self.machine:
                try:
                    self.machine = Machine(**toml.loads(mc))
                except ValueError:
                    pass
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

    @cached_method
    def _get_filterdatareadings(self) -> list[FilterDataReading]:
        if self.spec_major_version != 1:
            raise ValueError("Filterdata is only supported for spec version 1")
        fdp = os.path.join(self._dir_eds, "filterdata.xml")
        all_quant_files = [tuple(int(y[1:]) for y in x.stem.split("_")) for x in (Path(self._dir_eds) / "quant").glob("*_E*.quant")]
        all_quant_files.sort(key = lambda x: x[-1]) # use longest exposure (it is last, which will mean it remains in the dict)
        all_quant_dict = {}
        for x in all_quant_files:
            qstring = open(Path(self._dir_eds) / "quant" / f"S{x[0]:02}_C{x[1]:03}_T{x[2]:02}_P{x[3]:04}_M{x[4]:01}_X{x[5]:01}_E{x[6]:01}.quant", "r").read()
            qss = qstring.split("\n\n")[3].split("\n")
            assert len(qss) == 3
            assert qss[0] == "[conditions]"
            qd = {k: v for k, v in zip(qss[1].split("\t"), qss[2].split("\t"))}
            all_quant_dict[(x[0], x[1], x[2], x[3], x[4], x[5])] = float(qd["Time"])
        if os.path.isfile(fdp):
            fdx = ET.parse(fdp)
            fdrs = [ 
                FilterDataReading(x, timestamp_dict=all_quant_dict)
                for x in fdx.findall(".//PlateData")
            ]
            return fdrs
        elif fdfs := glob(
                os.path.join(self._dir_eds, "filter", "*_filterdata.xml")
            ):
            fdrs = [
                FilterDataReading.from_file(fdf, timestamp_dict=all_quant_dict)
                for fdf in fdfs
            ]
            return fdrs
        else:
            raise ValueError("No filterdata found")


    def filter_data_polars_lazy(self) -> 'pl.LazyFrame':
        import polars as pl
        from .polars_data import polars_from_filterdata
        start_time = self.activestarttime.timestamp() if self.activestarttime else None
        d = pl.concat(polars_from_filterdata(x, start_time=start_time) for x in self._get_filterdatareadings()).sort("timestamp")
        duration = (1000 * (pl.col("from") - pl.col("to")).abs() / pl.col("rate")).alias("duration").cast(pl.Duration(time_unit="ms"))
        time_since_ramp = (pl.col("timestamp") - pl.col("timestamp_ramp")).alias("time_since_ramp")

        d = d.join_asof(self.temperature_ramps_polars.lazy().select("timestamp", "zone", "from", "to", "rate"), on="timestamp", by="zone", suffix="_ramp", coalesce=False).with_columns(
            pl.when(time_since_ramp > duration).then(pl.col("to")).otherwise(pl.col("from") + (time_since_ramp / duration) * (pl.col("to") - pl.col("from"))).alias("set_temperature")
             ).drop("timestamp_ramp", "to", "from")
        if self.plate_setup:
            d = d.join(
                pl.from_pandas(self.plate_setup.well_sample, include_index=True).lazy().rename({"0": "sample", "index": "well"}), 
                on="well", how="left")
        return d
    
    @property
    @cached_method
    def filter_data_polars(self) -> pl.DataFrame:
        return self.filter_data_polars_lazy().collect()
    
    @property
    @cached_method
    def filter_data(self) -> pd.DataFrame:
        if self.spec_major_version == 1:
            try:
                return df_from_readings(
                    self._get_filterdatareadings(),
                    self.activestarttime.timestamp() if self.activestarttime else None,
                )
            except ValueError:
                raise ValueError("Could not load filter data")
        else:
            fdp = os.path.join(self._dir_base, "run/filter_data.json")
            if self.plate_type is None:
                raise ValueError("Plate type must be set before loading analysis.")
            if os.path.isfile(fdp):
                with open(fdp, "r") as f:
                    return _filterdata_df_v2(
                        json.load(f),
                        self.plate_type,
                        quant_files_path=(Path(self.root_dir) / "run/quant"),
                        start_time=(
                            self.activestarttime.timestamp()
                            if self.activestarttime
                            else None
                        ),
                    )

    @property
    def multicomponent_data(self) -> pd.DataFrame:
        try:
            if self.spec_major_version == 1:
                mdp = os.path.join(self._dir_eds, "multicomponentdata.xml")
                fdx = ET.parse(mdp)
                return _parse_multicomponent_data_v1(fdx)
            else:
                mdp = os.path.join(self._dir_base, "primary/multicomponent_data.json")
                with open(mdp, "r") as f:
                    return _parse_multicomponent_data_v2(json.load(f), self.plate_type)
        except Exception as e:
            if self.runstate == "INIT":
                raise ValueError("Run hasn't started yet: no data available.")
            else:
                raise ValueError("Multicomponent data is not available")

    @property
    @cached_method
    def analysis_result(self) -> pd.DataFrame:
        if self.spec_major_version == 1:
            adp = os.path.join(self._dir_eds, "analysis_result.txt")
            if not os.path.isfile(adp):
                if self.runstate == "INIT":
                    raise ValueError("Run hasn't started yet: no data available.")
                else:
                    raise ValueError("Analysis result is not available")
            else:
                with open(adp, "r") as f:
                    return _parse_analysis_result(f.read(), plate_type=self.plate_type)[0]
        else:
            raise NotImplementedError("Analysis result is not available for spec version 2. You might find data in _analysis_dict_v2")

    @property
    @cached_method
    def amplification_data(self) -> pd.DataFrame:
        if self.spec_major_version == 1:
            adp = os.path.join(self._dir_eds, "analysis_result.txt")
            if not os.path.isfile(adp):
                if self.runstate == "INIT":
                    raise ValueError("Run hasn't started yet: no data available.")
                raise ValueError("Amplification data is not available")
            with open(adp, "r") as f:
                return _parse_analysis_result(f.read(), plate_type=self.plate_type)[1]
        else:
            raise NotImplementedError("Amplification data is not available for spec version 2. You might find data in _analysis_dict_v2")

    @property
    @cached_method
    def _analysis_dict_v2(self) -> dict:
        if self.spec_major_version != 2:
            raise ValueError("Analysis dict is only available for spec version 2")
        adp = os.path.join(self.root_dir, "primary/analysis_result.json")
        if not os.path.isfile(adp):
            raise ValueError("Analysis result is not available")
        else:
            return json.load(open(adp, "r"))

    def _update_from_data(self) -> None:
        if self.spec_major_version == 1:

            adp = os.path.join(self._dir_eds, "analysis_result.txt")
            if self.plate_type is None:
                raise ValueError("Plate type must be set before loading analysis.")
            if os.path.isfile(adp):
                with open(adp, "r") as f:
                    (
                        self._analysis_result,
                        self._amplification_data,
                    ) = _parse_analysis_result(
                        f.read(), plate_type=self.plate_type
                    )  # FIXME: plate type
        else:  # spec version 2

            ap = Path(self.root_dir) / "primary" / "analysis_result.json"
            if ap.is_file():
                self._analysis_dict = json.load(ap.open())

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
    
    def _msglog_path(self) -> Path:
        if self.spec_major_version == 1:
            return Path(self._dir_eds) / "messages.log"
        else:
            return Path(self._dir_base) / "run" / "messages.log"

    def _update_from_log(self) -> None:
        logpath = self._msglog_path()
        if not logpath.is_file():
            return
        try:
            msglog = logpath.read_text()
        except UnicodeDecodeError as error:
            log.debug(
                "Decoding log failed. If <binary.reply> is present in log this may be the cause:"
                "if so contact Constantine (<const@costi.net>).  Continuing with failed characters"
                " replaced with backslash notation"
                "Failure was in this area:\n"
                "{!r}".format(error.object[error.start - 500 : error.end + 500])
            )
            msglog = open(
                logpath,
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

        m: Optional[re.Match[str]]
        stages: list[dict[str, int | str | datetime]] = []

        for m in ms:
            ts = datetime.fromtimestamp(float(m["ts"]), tz=timezone.utc)
            if m["msg"] == "Starting":
                self.runstarttime = ts
            elif m["msg"] == "Stage":
                if m["ext"] == "PRERUN":
                    self.prerunstart = ts
                    self.runstate = "RUNNING"
                elif (self.activestarttime is None) and (self.prerunstart is not None):
                    self.activestarttime = ts
                elif m["ext"] == "POSTRun":
                    self.activeendtime = ts
                try:
                    stages.append({"stage": int(m["ext"]), "start_time": ts})
                except ValueError:
                    stages.append({"stage": m["ext"], "start_time": ts})
                if len(stages) > 1:
                    stages[-2]["end_time"] = ts
            elif m["msg"] == "Ended":
                self.runendtime = ts
                self.runstate = "COMPLETE"
                if len(stages) > 1:
                    stages[-1].setdefault("end_time", ts)
            elif m["msg"] == "Aborted":
                self.runstate = "ABORTED"
                self.activeendtime = ts
                self.runendtime = ts
                if len(stages) > 1:
                    stages[-1].setdefault("end_time", ts)
            elif m["msg"] == "Stopped":
                self.runstate = "STOPPED"
                self.activeendtime = ts
                self.runendtime = ts
                if len(stages) > 1:
                    stages[-1].setdefault("end_time", ts)

        self.stages = pd.DataFrame(stages, columns=["stage", "start_time", "end_time"])

        if self.activestarttime:
            self.stages["start_seconds"] = (
                self.stages["start_time"] - self.activestarttime
            ).astype("timedelta64[ms]").astype("int64") / 1000
            self.stages["end_seconds"] = (
                self.stages["end_time"] - self.activestarttime
            ).astype("timedelta64[ms]").astype("int64") / 1000

        try:
            # In normal runs, the protocol is there without the PROT command at the
            # beginning of the log as an info command, with quote.message.  Let's
            # try to grab it!
            if m := re.match(
                r"^Info (?:[\d.]+) (<quote.message>.*?</quote.message>)",
                msglog,
                re.DOTALL | re.MULTILINE,
            ):
                # We can get the prot name too, and sample volume! FIXME: not from qslib runs!
                rp = re.search(
                    r"NEXT RP (?:-CoverTemperature=(?P<ct>[\d.]+) )?"
                    r"(?:-SampleVolume=(?P<sv>[\d.]+) )?([\w-]+) (?P<protoname>[\w-]+)",
                    msglog,
                    re.IGNORECASE,
                )
                if rp:
                    prot = Protocol.from_scpicommand(
                        SCPICommand.from_string(f"PROT {rp['protoname']} {m[1]}")
                    )
                    if rp[1]:
                        prot.volume = float(rp["sv"])
                else:
                    prot = Protocol.from_scpicommand(
                        SCPICommand.from_string(f"PROT unknown_name {m[1]}")
                    )
                self._protocol_from_log = prot

                # Now that we know the protocol name, we can search for whether the protocol was changed later:
                for mm in re.finditer(
                    r"^Debug.*<quote.message>C:.* (PROT[ O].*\n(c:.*\n)+.*)</quote.message>",
                    msglog,
                    re.MULTILINE,
                ):
                    newprot = Protocol.from_scpicommand(
                        SCPICommand.from_string(mm[1].replace("\nc:", "\n"))
                    )
                    # if newprot.name == prot.name:
                    self._protocol_from_log = newprot

            else:
                self._protocol_from_log = None
        except ValueError:
            self._protocol_from_log = None

        events = []
        for m in re.finditer(
            r"^Debug ([\d.]+) (Drawer|Cover) (.+)$", msglog, re.MULTILINE
        ):
            events.append((float(m[1]), m[2], m[3]))

        self.events = pd.DataFrame(
            events, columns=["timestamp", "type", "message"], dtype="object"
        )

        if self.activestarttime:
            self.events["seconds"] = (
                self.events["timestamp"] - self.activestarttime.timestamp()
            )
            self.events["hours"] = self.events["seconds"] / 3600.0


    @property
    @cached_method
    def temperatures(self) -> pd.DataFrame:
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

        temps, _ = self._temperatures_from_log()
        if temps is None:
            raise ValueError("Could not load temperatures")
        return temps
    
    @property
    @cached_method
    def num_zones(self) -> int:
        temps, num_zones = self._temperatures_from_log()
        if temps is None:
            raise ValueError("Could not load temperatures")
        return num_zones
    

    @property
    @cached_method
    def _temperatures_polars(self) -> tuple[pl.DataFrame, int]:
        from ._qslib import TemperatureLog, get_n_zones
        try:
            b = self._msglog_path().read_bytes()
        except FileNotFoundError as e:
            raise ValueError("no temperature data")
        try:
            n_zones = get_n_zones(b)
        except Exception as e:
            raise ValueError("no temperature data")
        tl = TemperatureLog.parse_to_polars(b)
        return tl, n_zones


    @property
    @cached_method
    def temperatures_polars(self) -> pl.DataFrame:
        tl, n_zones = self._temperatures_polars
        tl = tl.with_columns((pl.col("timestamp")*1000).cast(pl.Datetime(time_unit="ms", time_zone="UTC")))
        return tl

    def _temperatures_from_log(self) -> tuple[pd.DataFrame, int]:
        temperatures, num_zones = self._temperatures_polars


        temperatures = temperatures.to_pandas()
        ix = pd.MultiIndex.from_tuples(
            [(cast(str, "time"), cast(Union[str, int], "timestamp"))]
            + [("sample", n) for n in range(1, num_zones + 1)]
            + [("other", "heatsink"), ("other", "cover")]
            + [("block", n) for n in range(1, num_zones + 1)]
        )
        temperatures.columns = ix

        if self.activestarttime:
            temperatures[("time", "seconds")] = (
                temperatures[("time", "timestamp")]
                - self.activestarttime.timestamp()
            )
            temperatures[("time", "hours")] = (
                temperatures[("time", "seconds")] / 3600.0
            )
        temperatures[("sample", "avg")] = temperatures["sample"].mean(
            axis=1
        )
        temperatures[("block", "avg")] = temperatures["block"].mean(
            axis=1
        )

        return temperatures, num_zones

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
        process: Sequence[Processor] | Processor | None = None,
        normalization: Processor | None = None,
        ax: "Axes | None" = None,
        marker: str | None = None,
        legend: bool | Literal["inset", "right"] = True,
        figure_kw: dict[str, Any] | None = None,
        line_kw: dict[str, Any] | None = None,
    ) -> "Axes":
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

        Parameters
        ----------

        samples
            A reference to a single sample (a string), a list of sample names, or a Python
            regular expression as a string, matching sample names (full start-to-end matches
            only). Well names may also be included, in which case each well will be treated
            without regard to the sample name that may refer to it.  Note this means you cannot
            give your samples names that correspond with well references.  If not provided,
            all (named) samples will be included.

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

        ax
            Optional.  An axes to put the plot on.  If not provided, the function will
            create a new figure, by default with constrained_layout=True, though this
            can be modified with figure_kw.

        marker
            The marker format for data points, or None for no markers (default).

        legend
            Whether to add a legend.  True (default) decides whether to have the legend
            as an inset or to the right of the axes based on the number of lines.  "inset"
            and "right" specify the positioning.  Note that for "right", you must use
            some method to adjust the axes positioning: constrained_layout, tight_layout,
            or manually reducing the axes width are all options.

        figure_kw
            Optional.  A dictionary of options passed through as keyword options to
            the figure creation.  Only applies if ax is None.

        line_kw
            Optional.  A dictionary of keywords passed to all three plotting commands.

        Returns
        -------

        Axes
            The axes object of the plot.

        """

        import matplotlib.pyplot as plt

        if process is None:
            process = [normalization or NormRaw()]
        elif normalization:
            raise ValueError(
                "Can't specify both process and normalization (include normalization in process list)."
            )
        if isinstance(process, Processor):
            process = [process]

        if filters is None:
            filters = self.all_filters

        samples = self._get_samples(samples)

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
            ax = plt.figure(
                **(
                    {"constrained_layout": True}
                    | (({} if figure_kw is None else figure_kw))
                )
            ).add_subplot()

        data = self.welldata

        for processor in process:
            data = processor.process_scoped(data, "all")

        all_wells = self.plate_setup.get_wells(samples)

        reduceddata = data.loc[[f.lowerform for f in filters], all_wells]

        anneallines: "list[list[Line2D]]" = []
        meltlines: "list[list[Line2D]]" = []
        betweenlines: "list[list[Line2D]]" = []

        for processor in process:
            reduceddata = processor.process_scoped(reduceddata, "limited")

        for filter in filters:
            filterdat: pd.DataFrame = reduceddata.loc[filter.lowerform, :]  # type: ignore

            annealdat: pd.DataFrame = filterdat.loc[anneal_stages, :]  # type: ignore
            meltdat: pd.DataFrame = filterdat.loc[melt_stages, :]  # type: ignore

            if len(between_stages) > 0:
                betweendat: pd.DataFrame | None = filterdat.loc[between_stages, :]  # type: ignore
            else:
                betweendat = None

            for sample in samples:
                wells = self.plate_setup.get_wells(sample)

                for well in wells:
                    label = _gen_label(
                        self.plate_setup.get_descriptive_string(sample),
                        well,
                        filter,
                        samples,
                        wells,
                        filters,
                    )

                    anneallines.append(
                        ax.plot(
                            annealdat.loc[:, (well, "st")],
                            annealdat.loc[:, (well, "fl")],
                            label=label,
                            marker=marker,
                            **(line_kw if line_kw is not None else {}),
                        )
                    )

                    color = anneallines[-1][-1].get_color()

                    meltlines.append(
                        ax.plot(
                            meltdat.loc[:, (well, "st")],
                            meltdat.loc[:, (well, "fl")],
                            color=color,
                            linestyle="dashed",
                            marker=marker,
                            **(line_kw if line_kw is not None else {}),
                        )
                    )

                    if betweendat is not None:
                        betweenlines.append(
                            ax.plot(
                                betweendat.loc[:, (well, "st")],
                                betweendat.loc[:, (well, "fl")],
                                color=color,
                                linestyle="dotted",
                                marker=marker,
                                **(line_kw if line_kw is not None else {}),
                            )
                        )

        ax.set_xlabel("temperature (°C)")

        ylabel: str | None = None
        for processor in process:
            ylabel = processor.ylabel(ylabel)
        ax.set_ylabel(ylabel or "fluorescence")

        if legend is True:
            if len(anneallines) < 6:
                legend = "inset"
            else:
                legend = "right"

        if legend == "inset":
            ax.legend()
        elif legend == "right":
            ax.legend(bbox_to_anchor=(1.04, 1), loc="upper left")

        ax.set_title(
            _gen_axtitle(
                self.name,
                anneal_stages + between_stages + melt_stages,
                samples,
                all_wells,
                filters,
            )
        )

        return ax

    def _get_samples(self, samples: str | Sequence[str] | None) -> Sequence[str]:
        if isinstance(samples, str):
            if samples in self.plate_setup.sample_wells:
                samples = [samples]
            else:
                samples = [
                    k
                    for k in self.plate_setup.sample_wells
                    if re.match(samples + "$", k)
                ]
                if not samples:
                    raise ValueError("Samples not found")
        elif samples is None:
            samples = list(self.plate_setup.sample_wells.keys())
        return samples

    def plot_over_time(
        self,
        samples: str | Sequence[str] | None = None,
        filters: str | FilterSet | Collection[str | FilterSet] | None = None,
        stages: slice | int | Sequence[int] = slice(None),
        process: Sequence[Processor] | Processor | None = None,
        normalization: Processor | None = None,
        ax: "Axes" | "Sequence[Axes]" | None = None,
        legend: bool | Literal["inset", "right"] = True,
        temperatures: Literal[False, "axes", "inset", "twin"] = "axes",
        marker: str | None = None,
        stage_lines: bool | Literal["fluorescence", "temperature"] = True,
        annotate_stage_lines: (
            bool
            | float
            | Literal["fluorescence", "temperature"]
            | Tuple[Literal["fluorescence", "temperature"], float]
        ) = True,
        annotate_events: bool = True,
        figure_kw: dict[str, Any] | None = None,
        line_kw: dict[str, Any] | None = None,
        start_time=None,
        time_units: Literal["hours", "seconds"] = "hours",
    ) -> "Sequence[Axes]":
        """
        Plots fluorescence over time, optionally with temperatures over time.

        Line labels are intended to provide full information when read in combination with
        the axes title.  They will only include information that does not apply to all
        lines.  For example, if every line is from the same filter set, but different
        samples, then only the sample will be shown.  If every line is from the same sample,
        but different filter sets, then only the filter set will be shown. Wells are shown
        if a sample has multiple wells.

        Parameters
        ----------

        samples
            A reference to a single sample (a string), a list of sample names, or a Python
            regular expression as a string, matching sample names (full start-to-end matches
            only). Well names may also be included, in which case each well will be treated
            without regard to the sample name that may refer to it.  Note this means you cannot
            give your samples names that correspond with well references.  If not provided,
            all (named) samples will be included.

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
            Optional (default "axes").  Several alternatives for displaying temperatures.
            "axes" uses a separate axes (created if ax is not provided, otherwise ax must
            be a list of two axes).

            Temperatures are from Experiment.temperature, and are thus the temperatures
            as recorded during the run, not the set temperatures.  Note that this has a
            *very* large number of data points, something that should be dealt with at
            some point.

        ax
            Optional.  An axes to put the plot on.  If not provided, the function will
            create a new figure, by default with constrained_layout=True, though this
            can be modified with figure_kw.  If `temperatures="axes"`, you must provide
            a list or tuple of *two* axes, the first for fluorescence, the second
            for temperature.

        marker
            The marker format for data points, or None for no markers (default).

        legend
            Whether to add a legend.  True (default) decides whether to have the legend
            as an inset or to the right of the axes based on the number of lines.  "inset"
            and "right" specify the positioning.  Note that for "right", you must use
            some method to adjust the axes positioning: constrained_layout, tight_layout,
            or manually reducing the axes width are all options.

        stage_lines
            Whether to include dotted vertical lines on transitions between stages.  If
            "fluorescence" or "temperature", include only on one of the two axes.

        annotate_stage_lines
            Whether to include text annotations for stage lines.  Float parameter allows
            setting the minimum duration of stage, as a fraction of total plotted time, to
            annotate, in order to avoid overlapping annotations (default threshold is 0.05).

        annotate_events
            Whether to include annotations for events (drawer open/close, cover open/close).

        figure_kw
            Optional.  A dictionary of options passed through as keyword options to
            the figure creation.  Only applies if ax is None.

        line_kw
            Optional.  A dictionary of keywords passed to fluorescence plot commands.

        """

        import matplotlib.pyplot as plt

        if process is None:
            process = [normalization or NormRaw()]
        elif normalization:
            raise ValueError(
                "Can't specify both process and normalization (include normalization in process list)."
            )
        if isinstance(process, Processor):
            process = [process]

        if filters is None:
            filters = self.all_filters

        filters = _normalize_filters(filters)

        samples = self._get_samples(samples)

        if isinstance(stages, int):
            stages = [stages]

        fig = None

        if ax is None:
            if temperatures == "axes":
                fig, ax_arr = plt.subplots(
                    2,
                    1,
                    sharex="all",
                    gridspec_kw={"height_ratios": [3, 1]},
                    **(
                        cast(dict[str, Any], {"constrained_layout": True})
                        | (({} if figure_kw is None else figure_kw))
                    ),
                )
                ax = list(ax_arr)
            else:
                fig, ax = plt.subplots(1, 1, **({} if figure_kw is None else figure_kw))
                ax = [cast("Axes", ax)]

        elif (not isinstance(ax, (Sequence, np.ndarray))) or isinstance(ax, Axes):
            ax = [ax]

        ax = cast("Sequence[Axes]", ax)

        data = self.welldata

        for processor in process:
            data = processor.process_scoped(data, "all")

        all_wells = self.plate_setup.get_wells(samples) + ["time"]

        reduceddata = data.loc[[f.lowerform for f in filters], all_wells]

        for processor in process:
            reduceddata = processor.process_scoped(reduceddata, "limited")

        if start_time is None:
            start_time_value = 0.0
        elif isinstance(start_time, (int, float)):
            start_time_value = start_time
        else:
            raise TypeError("start_time invalid type")

        lines = []
        for filter in filters:
            filterdat: pd.DataFrame = reduceddata.loc[filter.lowerform, :]  # type: ignore

            for sample in samples:
                wells = self.plate_setup.get_wells(sample)

                for well in wells:
                    label = _gen_label(
                        self.plate_setup.get_descriptive_string(sample),
                        well,
                        filter,
                        samples,
                        wells,
                        filters,
                    )

                    lines.append(
                        ax[0].plot(
                            filterdat.loc[stages, ("time", time_units)]
                            - start_time_value,
                            filterdat.loc[stages, (well, "fl")],
                            label=label,
                            marker=marker,
                            **(line_kw if line_kw is not None else {}),
                        )
                    )

        ax[-1].set_xlabel("time (hours)")

        ylabel: str | None = None
        for processor in process:
            ylabel = processor.ylabel(ylabel)
        ax[0].set_ylabel(ylabel or "fluorescence")

        if legend is True:
            if len(lines) < 6:
                legend = "inset"
            else:
                legend = "right"

        if legend == "inset":
            ax[0].legend()
        elif legend == "right":
            ax[0].legend(bbox_to_anchor=(1.04, 1), loc="upper left")

        xlims = ax[0].get_xlim()

        if isinstance(annotate_stage_lines, (tuple, list)):
            if annotate_stage_lines[0] == "fluorescence":
                fl_asl: bool | float = annotate_stage_lines[1]
                t_asl: bool | float = False
            elif annotate_stage_lines[0] == "temperature":
                fl_asl = False
                t_asl = annotate_stage_lines[1]
            else:
                raise ValueError
        elif annotate_stage_lines == "temperature":
            t_asl = True
            fl_asl = False
        elif annotate_stage_lines == "fluorescence":
            t_asl = False
            fl_asl = True
        else:
            t_asl = annotate_stage_lines
            fl_asl = annotate_stage_lines

        if stage_lines == "temperature":
            t_sl = True
            fl_sl = False
        elif stage_lines == "fluorescence":
            t_sl = False
            fl_sl = True
        else:
            t_sl = stage_lines
            fl_sl = stage_lines

        self._annotate_stages(
            ax[0], fl_sl, fl_asl, (xlims[1] - xlims[0]) * 3600.0, stages=stages
        )

        if annotate_events:
            self._annotate_events(ax[0], stages=stages)

        if temperatures == "axes":
            if len(ax) < 2:
                raise ValueError("Temperature axes requires at least two axes in ax")

            xlims = ax[0].get_xlim()
            tmin, tmax = np.inf, 0.0

            for i, fr in reduceddata.groupby("filter_set", as_index=False):
                d = fr.loc[i, ("time", time_units)].loc[stages]
                tmin = min(tmin, d.min())
                tmax = max(tmax, d.max())

            self.plot_temperatures(
                hours=(tmin, tmax),
                ax=ax[1],
                stage_lines=t_sl,
                annotate_stage_lines=t_asl,
                annotate_events=annotate_events,
            )

            ax[0].set_xlim(xlims)
            ax[1].set_xlim(xlims)
        elif temperatures is False:
            pass
        else:
            raise NotImplementedError

        ax[0].set_title(_gen_axtitle(self.name, stages, samples, all_wells, filters))

        return ax

    def plot_protocol(
        self, ax: Optional[Axes] = None
    ) -> "Tuple[Axes, Tuple[List[Line2D], List[Line2D]]]":
        """A plot of the temperature and data collection points in the experiment's protocol."""

        return self.protocol.plot_protocol(ax)

    def plot_temperatures(
        self,
        *,
        sel: slice | Callable[[pd.DataFrame], bool] = slice(None),
        hours: tuple[float, float] | None = None,
        ax: Optional[Axes] = None,
        stage_lines: bool = True,
        annotate_stage_lines: bool | float = True,
        annotate_events: bool = True,
        legend: bool = False,
        figure_kw: Mapping[str, Any] | None = None,
        line_kw: Mapping[str, Any] | None = None,
    ) -> "Axes":
        """Plot sample temperature readings.

        Parameters
        ----------

        sel
            A selector for the temperature DataFrame.  This is not necessarily
            easy to use; `hours` is an easier alternative.

        hours
            Constructs a selector to show temperatures for a time range.
            :param:`sel` should not be set.

        ax
            Optional.  An axes to put the plot on.  If not provided, the function will
            create a new figure, by default with constrained_layout=True, though this
            can be modified with figure_kw.

        stage_lines
            Whether to include dotted vertical lines on transitions between stages.

        annotate_stage_lines
            Whether to include text annotations for stage lines.  Float parameter allows
            setting the minimum duration of stage, as a fraction of total plotted time, to
            annotate, in order to avoid overlapping annotations (default threshold is 0.05).

        annotate_events
            Whether to include annotations for events (cover opening/closing, drawer opening/closing).

        legend
            Whether to add a legend.

        figure_kw
            Optional.  A dictionary of options passed through as keyword options to
            the figure creation.  Only applies if ax is None.

        line_kw
            Optional.  A dictionary of keywords passed to plot commands.
        """

        import matplotlib.pyplot as plt
        from matplotlib.axes import Axes

        if not hasattr(self, "temperatures") or self.temperatures is None:
            raise ValueError("Experiment has no temperature data.")

        if hours is not None:
            if sel != slice(None):
                raise ValueError("sel and hours cannot both be set.")
            tmin, tmax = hours

            sel = lambda x: (tmin <= x["time", "hours"]) & (x["time", "hours"] <= tmax)

        if ax is None:
            _, ax = plt.subplots(**(figure_kw or {}))

        ax = cast(Axes, ax)

        reltemps = self.temperatures.loc[sel, :]

        for x in range(1, self.num_zones + 1):
            ax.plot(
                reltemps.loc[:, ("time", "hours")],
                reltemps.loc[:, ("sample", x)],
                label=f"zone {x}",
                **(line_kw or {}),
            )

        v = reltemps.loc[:, ("time", "hours")]
        totseconds = 3600.0 * (v.iloc[-1] - v.iloc[0])

        self._annotate_stages(
            ax, stage_lines, annotate_stage_lines, totseconds, stages=False
        )

        if annotate_events:
            self._annotate_events(ax, stages=False)

        ax.set_ylabel("temperature (°C)")
        ax.set_xlabel("time (hours)")

        if legend:
            ax.legend()

        return ax

    def _annotate_stages(
        self,
        ax: "Axes",
        stage_lines: bool,
        annotate_stage_lines: bool | float,
        totseconds,
        stages: slice | Sequence[int] | bool = slice(None),
    ):
        if stage_lines:
            if isinstance(annotate_stage_lines, float):
                annotate_frac = annotate_stage_lines
                annotate_stage_lines = True
            else:
                annotate_frac = 0.05

            for _, s in self.stages.iterrows():
                if s.stage == "PRERUN" or s.stage == "POSTRUN" or s.stage == "POSTRun":
                    continue  # FIXME: maybe we should include these
                    #

                xlim = ax.get_xlim()
                if (stages != slice(None)) and (
                    not (xlim[0] <= s.start_seconds / 3600.0 <= xlim[1])
                ):
                    continue

                xtrans = ax.get_xaxis_transform()
                vline = ax.axvline(
                    s.start_seconds / 3600.0,
                    linestyle="dotted",
                    color="black",
                    linewidth=0.5,
                )
                durfrac = (s.end_seconds - s.start_seconds) / totseconds
                if annotate_stage_lines and (durfrac > annotate_frac):
                    ax.annotate(
                        f"stage {s.stage}",
                        xy=(1, 0.9),
                        xycoords=vline,
                        xytext=(5, 0),
                        textcoords="offset points",
                        rotation=90,
                        verticalalignment="top",
                        horizontalalignment="left",
                    )

    def _annotate_events(self, ax, stages: slice | Sequence[int] | bool = slice(None)):
        first_time = self.stages.iloc[0, :]["start_seconds"] / 3600.0
        last_time = self.stages.iloc[-1, :]["end_seconds"] / 3600.0

        opi = self.events.index[
            (self.events["type"] == "Cover") & (self.events["message"] == "Raising")
        ]
        cl = self.events.index[
            (self.events["type"] == "Cover") & (self.events["message"] == "Lowered")
        ]
        cli = [cl[cl > x][0] if len(cl[cl > x]) > 0 else None for x in opi]
        for x1, x2 in zip(opi, cli):
            xlim = ax.get_xlim()
            if not (
                xlim[0] <= self.events.loc[x1, "hours"] <= xlim[1]
                or (
                    x2 is not None
                    and xlim[0] <= self.events.loc[x2, "hours"] <= xlim[1]
                )
            ):
                if stages != slice(None):
                    continue
            ax.axvspan(
                self.events.loc[x1, "hours"],
                self.events.loc[x2, "hours"] if x2 is not None else last_time,
                alpha=0.5,
                color="yellow",
            )

        opi = self.events.index[
            (self.events["type"] == "Drawer") & (self.events["message"] == "Opening")
        ]
        cl = self.events.index[
            (self.events["type"] == "Drawer") & (self.events["message"] == "Closed")
        ]
        cli = [cl[cl > x][0] if len(cl[cl > x]) > 0 else None for x in opi]
        for x1, x2 in zip(opi, cli):
            xlim = ax.get_xlim()
            if not (
                xlim[0] <= self.events.loc[x1, "hours"] <= xlim[1]
                or (
                    x2 is not None
                    and xlim[0] <= self.events.loc[x2, "hours"] <= xlim[1]
                )
            ):
                if stages != slice(None):
                    continue
            ax.axvspan(
                self.events.loc[x1, "hours"],
                self.events.loc[x2, "hours"] if x2 is not None else last_time,
                alpha=0.5,
                color="red",
            )

    def plot_over_time_altair(
            self,
            samples: str | Sequence[str] | None = None,
            filters: str | FilterSet | Collection[str | FilterSet] | None = None,
            stages: slice | int | Sequence[int] | None = None,
            process = None,
            start_time: Literal['experiment', 'stage'] = 'experiment',
            duration_units: Literal["hours", "minutes", "seconds"] = "hours",
            show_legend: bool = True,
    ):
        import altair as alt
        alt.data_transformers.enable("vegafusion")

        d = self.filter_data_polars.lazy()
        match process:
            case tuple():
                pass
            case pl.Expr():
                d = d.with_columns(process.alias("processed_fluorescence"))
            case None:
                d = d.with_columns(pl.col("fluorescence").alias("processed_fluorescence"))

        match samples:
            case None:
                pass
            case str():
                d = d.filter(pl.col("sample").str.count_matches(samples) > 0)
            case x if isinstance(x, Sequence):
                d = d.filter(pl.col("sample").is_in(x))
            case _:
                raise ValueError(f"Invalid samples: {samples}")

        match filters:
            case None:
                pass
            case str():
                d = d.filter(pl.col("filter_set").str.count_matches(filters) > 0)
            case x if isinstance(x, FilterSet):
                d = d.filter(pl.col("filter_set").is_in(x))
            case x if isinstance(x, Sequence):
                d = d.filter(pl.col("filter_set").is_in(x))
            case _:
                raise ValueError(f"Invalid filters: {filters}")
            
        match stages:
            case slice():
                start = 1 if stages.start is None else stages.start
                stop = self.stages.iloc[-2]["stage"] + 1 if stages.stop is None else stages.stop
                step = 1 if stages.step is None else stages.step
                stages = slice(start, stop, step)
                d = d.filter(pl.col("stage").is_in(range(stages.start, stages.stop, stages.step)))
                first_stage = stages.start  
            case int():
                d = d.filter(pl.col("stage") == stages)
                first_stage = stages
            case x if isinstance(x, Sequence):
                d = d.filter(pl.col("stage").is_in(stages))
                first_stage = min(stages)
            case None:
                pass
            case _:
                raise ValueError(f"Invalid stages: {stages}")

        match start_time:
            case 'experiment':
                d = d.with_columns((pl.col("timestamp") - pl.lit(self.activestarttime)).alias("time_since_mark"))
                start_time_descr = "experiment start"
            case 'stage':
                first_stage_start = self.stages.loc[first_stage, 'start_time']
                print(first_stage_start, first_stage)
                d = d.with_columns((pl.col("timestamp") - pl.lit(first_stage_start)).alias("time_since_mark"))
                start_time_descr = f"stage {first_stage} start"
            case _:
                raise ValueError(f"Invalid start_time: {start_time}")
        

        d = d.with_columns(time_since_mark_float=pl.col("time_since_mark").cast(pl.Float64))

        match duration_units:
            case "hours":
                d = d.with_columns(time_since_mark_float=pl.col("time_since_mark_float") / 3600000)
            case "minutes":
                d = d.with_columns(time_since_mark_float=pl.col("time_since_mark_float") / 60000)
            case "seconds":
                d = d.with_columns(time_since_mark_float=pl.col("time_since_mark_float") / 1000)
            case _:
                raise ValueError(f"Invalid duration_units: {duration_units}")

        d = d.collect()
        
        return alt.Chart(d).mark_line().encode(
            alt.X("time_since_mark_float:Q", axis=alt.Axis(title=f"Time since {start_time_descr} ({duration_units})")),
            alt.Y("processed_fluorescence:Q", axis=alt.Axis(title="Fluorescence")).scale(zero=False),
            color=alt.Color("sample:N", legend=alt.Legend(title="Sample") if show_legend else None),
            tooltip=["time_since_mark_float", "fluorescence", "processed_fluorescence", "sample", "filter_set", "sample_temperature", "set_temperature", "stage", "step", "cycle"]
        )


def _normalize_filters(
    filters: str | FilterSet | Collection[str | FilterSet],
) -> list[FilterSet]:
    if isinstance(filters, str) or isinstance(filters, FilterSet):
        filters = [filters]
    return [FilterSet.fromstring(filter) for filter in filters]


def _gen_label(
    sample: str,
    well: str,
    filter: FilterSet,
    samples: Sequence[str],
    wells: Sequence[str],
    filters: Sequence[FilterSet],
) -> str:
    label = ""
    if len(samples) > 1:
        label = sample
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


def _gen_axtitle(
    expname: str,
    stages: Sequence[int] | slice | int,
    samples: Sequence[str],
    wells: Sequence[str],
    filters: Sequence[FilterSet],
) -> str:
    elems: list[str] = []
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


def _get_manifest_info(f: zipfile.ZipFile | os.PathLike[str] | str, checkinfo=True):
    try:
        if isinstance(f, zipfile.ZipFile):
            m = f.open("apldbio/sds/Manifest.mf")
        else:
            m = (Path(f) / "apldbio/sds/Manifest.mf").open("rb")
    except (KeyError, FileNotFoundError):
        try:
            if isinstance(f, zipfile.ZipFile):
                m = f.open("Manifest.mf")
            else:
                m = (Path(f) / "Manifest.mf").open("rb")
        except (KeyError, FileNotFoundError):
            raise ValueError("No EDS manifest file found. Is this a valid EDS?")

    # Manifest files for EDS archives should just be splittable by : into key/value pairs
    manifest_properties = dict(
        line.decode("utf-8").rstrip().split(": ", 1)
        for line in m.readlines()
        if len(line) > 2
    )
    m.close()

    if not checkinfo:
        return manifest_properties

    if (
        v := manifest_properties.get("Specification-Title")
    ) != "Experiment Document Specification":
        raise ValueError(
            f"Manifest file does not appear to be for an EDS (Specification-Title is {v})"
        )

    sv = manifest_properties["Specification-Version"]
    if sv[0] not in ("1", "2"):
        raise ValueError(
            f"QSLib does not support EDS files of specification version {sv}"
        )
    elif sv[0] == "2":
        warn(
            f"QSLib support for EDS specification version 2 is preliminary.  This file is version {sv}"
        )
    elif sv not in ("1.3.0", "1.3.1", "1.3.2"):
        warn(
            f"{sv} is an EDS specification version QSLib hasn't been specifically tested with."
        )

    return manifest_properties
