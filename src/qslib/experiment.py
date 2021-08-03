from __future__ import annotations
from dataclasses import InitVar, dataclass, field
from datetime import datetime
from glob import glob
import io
import os
from posixpath import join
from typing import Any, IO, Iterable, Union

from qslib.plate_setup import PlateSetup
from .machine import Machine, RunStatus
from .tcprotocol import Protocol, Stage, Step
import tempfile
import zipfile
import base64
import logging
import xml.etree.ElementTree as ET
import re
import pandas as pd
from .data import df_from_readings, FilterSet, FilterDataReading
from .version import __version__

TEMPLATE_NAME = "ruo"


def _find_or_create(element: ET.Element | ET.ElementTree, path: str) -> ET.Element:
    if isinstance(element, ET.ElementTree):
        element = element.getroot()
    if (m := element.find(path)) is not None:
        return m
    else:
        e = element
        for elemname in path.split("/"):
            if te := e.find(elemname):
                e = te
            else:
                e = ET.SubElement(e, elemname)
    return e


def _set_or_create(
    element: ET.Element | ET.ElementTree, path: str, text=None, **kwargs
) -> ET.Element:
    e = _find_or_create(element, path)
    for k, v in kwargs.items():
        e.attrib[k] = v
    if text is not None:
        e.text = text
    return e


def _text_or_none(element: ET.Element | ET.ElementTree, path: str) -> Union[str, None]:
    if (e := element.find(path)) is not None:
        return e.text
    else:
        return None


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


log = logging.getLogger("experiment")

_TABLE_FORMAT = {"markdown": "pipe", "org": "orgtbl"}


class Experiment:
    """A QuantStudio experiment / EDS file

    This class can create, modify, load and save experiments in several ways, run them,
    and control and modify them while running.

    Experiments can be loaded from a file:

    >>> exp = Experiment.from_file("experiment.eds")

    They can also be loaded from a running experiment:

    >>> machine = Machine("localhost", 7000, password="password")
    >>> exp = Experiment.from_running(machine)

    Or from the machine's storage:

    >>> exp = Experiment.from_machine_storage(machine, "experiment.eds")

    They can also be created from scratch:

    >>> exp = Experiment("an-experiment-name")
    >>> exp.protocol = tc.Protocol([tc.Stage([tc.Step(time=60, temperature=60)])])
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
      AB D&A, but may still cause problems.
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
    the start of the protocol if the lamp needs to warm up.

    None if the file has not been updated since the start of the run
    """
    runendtime: datetime | None
    """
    The run end time as a datetime, taken from experiment.xml.
    """
    createdtime: datetime
    """
    The run creation time.
    """
    modifiedtime: datetime
    """
    The last modification time. QSLib sets this on write. AB D&A may not.
    """
    runstate: str
    """
    Run state, possible values INIT, COMPLETED, ABORTED.
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
    welldata: pd.DataFrame
    """
    A DataFrame with fluorescence reading information.

    Indices (multi-index) are (filter_set, stage, cycle, step, point), where filter_set
    is a string in familiar form (eg, "x1-m4") and the rest are int.

    Columns (as multi-index)
    ------------------------

    ("time", ...) : float
        Time of the data collection, taken from the .quant file.  May differ for different
        filter sets.  Options are "timestamp" (unix timestamp in seconds), "seconds", and
        "hours" (the latter two from the start of the run, which may be prior to the start
        of the experiment protocol if the lamp needed a 3 minute warmup).

    (well, option) : float
        Data for a well, with well formatted like "A05".  Options are "rt" (read temperature
        from .quant file), "st" (more stable temperature), and "fl" (fluorescence).

    ("exposure", "exposure") : float
        Exposure time from filterdata.xml.  Misleading, because it only refers to the
        longest exposure of multiple exposures.

    """
    filterdata: pd.DataFrame
    temperatures: pd.DataFrame
    """
    A DataFrame of temperature readings, at one second resolution, during the experiment
    (and potentially slightly before and after, if included in the message log).

    Columns (as multi-index)
    ------------------------

    ("time", ...) : float
        Time of temperature reading, for choices of "timestamp" (Unix timestamp in seconds), 
        "seconds" (seconds since start of run, possibly not start of protocol if lamp needed
        to warm up), or "hours". The latter two may be negative.

    ("sample", ...) : float
        Sample temperature for blocks 1, 2, ..., 6, and average in "avg".

    ("block", ...) : float
        Block temperature for blocks 1, 2, ..., 6, and average in "avg".

    ("other", "cover") : float
        Cover temperature

    ("other", "heatsink") : float
        Heatsink temperature

    """

    def summary(self, format="markdown", plate="list") -> str:
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
        s = f"QS Experiment {self.name} ({self.runstate})\n\n"
        s += f"{self.protocol}\n\n"
        if plate == "list":
            s += f"{self.plate_setup}\n"
        elif plate == "table":
            s += f"{self.plate_setup.to_table(tablefmt=_TABLE_FORMAT.get(format, format))}\n"
        s += f"Created: {self.createdtime}\n"
        if self.runstarttime:
            s += f"Run Started: {self.runstarttime}\n"
        if self.runendtime:
            s += f"Run Ended: {self.runendtime}\n"
        s += f"Written by: {self.writesoftware}\n"
        s += f"Read by: QSLib {__version__}\n"
        return s

    def __str__(self) -> str:
        return self.summary()

    @property
    def runtitle_safe(self) -> str:
        """Run name with " " replaced by "-"."""
        return self.name.replace(" ", "-")

    def _ensure_machine(self, machine: Machine | str | None, password=None) -> Machine:
        if isinstance(machine, Machine):
            self.machine = machine
            return machine
        elif isinstance(machine, str):
            self.machine = Machine(machine, password=password)
            return self.machine
        elif c := getattr(self, "machine", None):
            return c
        else:
            log.warn("creating new machine")
            self.machine = Machine(
                self.info["host"], self.info["port"], self.info["password"]
            )
            return self.machine

    def _ensure_running(self, machine: Machine):
        crt = machine.run_status()
        if crt.name != self.runtitle_safe:
            raise NotRunningError(machine, self.runtitle_safe, crt)
        return crt

    def _populate_folder(self):
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

    def run(self, machine: Machine | str | None = None, password=None):
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
        log.info(f"Attempting to sat {self.runtitle_safe} on {machine.host}.")

        # Ensure machine isn't running:
        if (x := machine.run_status()).state != "IDLE":
            raise MachineBusyError(machine, x)

        if self.runstate != "INIT":
            raise ValueError

        with machine.at_access("Controller", exclusive=True):
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
            machine.run_command_to_ack(f"RP {self.runtitle_safe} {self.protocol.name}")
            log.info(f"Run {self.runtitle_safe} started on {machine.host}.")

    def create_new_copy(self) -> Experiment:
        """Create a copy of the experiment, with data and run information removed, suitable
        for rerunning.

        Returns
        -------
        Experiment
        """
        raise NotImplementedError

    def collect_finished(self, machine: Machine | None = None):
        """NOT YET IMPLEMTED

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

    def pause_now(self, machine: Machine | None = None):
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

    def resume(self, machine: Machine | None = None):
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

    def stop(self, machine: Machine | None = None):
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

    def abort(self, machine: Machine | None = None):
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

    def sync_from_machine(self, machine: Machine | None = None):
        """NOT YET IMPLEMENTED
        Try to synchronize the data in the experiment to the current state of the run on a
        machine, more efficiently than reloading everything.
        """
        machine = self._ensure_machine(machine)
        self._ensure_running(machine)

        raise NotImplementedError("Use Experiment.from_running for now.")

    def change_protocol(self, protocol: Protocol, machine: Machine | None = None):
        """NOT YET IMPLEMENTED
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
        protocol : Protocol
            [description]
        machine : Machine, optional
            [description], by default None

        """
        machine = self._ensure_machine(machine)
        self._ensure_running(machine)
        raise NotImplementedError

        # Get running protocol and status.

        # Check compatibility of changes.

        # Make changes.

        # Save changes in tcprotocol.xml

        # Push new tcprotocol.xml

    def save_file(self, file: str | IO[bytes], overwrite=False):
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

    def save_file_without_changes(self, file: str | IO[bytes], overwrite=False):
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

    def __init__(self, name=None, protocol=None, plate_setup=None, _create_xml=True):
        self._tmp_dir_obj = tempfile.TemporaryDirectory()
        self._dir_base = self._tmp_dir_obj.name
        self._dir_eds = os.path.join(self._dir_base, "apldbio", "sds")

        if name is not None:
            self.name = name
        else:
            self.name = datetime.now().strftime("%Y-%m-%dT%H%M")

        if protocol is None:
            self.protocol = Protocol([Stage([Step(60, 25)])])

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
            self.writesoftware = f"QSLib {__version__}"

            self._new_xml_files()
            self._update_files()

    def _new_xml_files(self) -> None:
        ET.ElementTree(ET.Element("Experiment")).write(
            os.path.join(self._dir_eds, "experiment.xml")
        )
        ET.ElementTree(ET.Element("Plate")).write(
            os.path.join(self._dir_eds, "plate_setup.xml")
        )
        ET.ElementTree(ET.Element("TCProtocol")).write(
            os.path.join(self._dir_eds, "tcprotocol.xml")
        )

    def _update_files(self) -> None:
        self._update_experiment_xml()
        self._update_tcprotocol_xml()
        self._update_platesetup_xml()

    def _update_from_files(self) -> None:
        self._update_from_experiment_xml()
        self._update_from_tcprotocol_xml()
        self._update_from_platesetup_xml()
        self._update_from_log()
        self._update_data()

        if self._protocol_from_xml:
            self.protocol = self._protocol_from_xml
        if self._protocol_from_log:
            self.protocol = self._protocol_from_log
        if self._protocol_from_qslib:
            self.protocol = self._protocol_from_qslib

    @classmethod
    def from_file(cls, file: str | os.PathLike[str] | IO[bytes]) -> "Experiment":
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

        exml.write(os.path.join(self._dir_eds, "experiment.xml"))

    def _update_from_experiment_xml(self):
        exml = ET.parse(os.path.join(self._dir_eds, "experiment.xml"))

        self.name = exml.findtext("Name")
        self.user = _text_or_none(exml, "Operator")
        self.createdtime = datetime.fromtimestamp(float(_find_or_raise(exml, "CreatedTime").text) / 1000.0)  # type: ignore
        self.runstate = exml.findtext("RunState")
        self.writesoftware = exml.findtext(
            "ExperimentProperty[@type='RunInfo']/PropertyValue[@key='softwareVersion']/String"
        )
        if x := exml.findtext("RunStartTime"):
            self.runstarttime = datetime.fromtimestamp(float(x) / 1000.0)
        if x := exml.findtext("RunEndTime"):
            self.runendtime = datetime.fromtimestamp(float(x) / 1000.0)

    def _update_tcprotocol_xml(self):
        if self.protocol:
            exml = ET.ElementTree(self.protocol.to_xml())
            exml.write(os.path.join(self._dir_eds, "tcprotocol.xml"))

    def _update_from_tcprotocol_xml(self):
        exml = ET.parse(os.path.join(self._dir_eds, "tcprotocol.xml"))

        if (x := exml.find("QSLibProtocol")) is not None:
            self._protocol_from_qslib = Protocol.from_repr(x.text)
            self._protocol_from_xml = None
        else:
            self._protocol_from_qslib = None
            try:
                self._protocol_from_xml = Protocol.from_xml(exml.getroot())
            except:
                self._protocol_from_xml = None

    def _update_platesetup_xml(self):
        x = ET.parse(os.path.join(self._dir_eds, "plate_setup.xml"))
        self.plate_setup.update_xml(x.getroot())
        x.write(os.path.join(self._dir_eds, "plate_setup.xml"))

    def _update_from_platesetup_xml(self):
        x = ET.parse(os.path.join(self._dir_eds, "plate_setup.xml")).getroot()
        self.plate_setup = PlateSetup.from_platesetup_xml(x)

    def _update_data(self):
        fdp = os.path.join(self._dir_eds, "filterdata.xml")
        if os.path.isfile(fdp):
            fdx = ET.parse(fdp)
            fdrs = [
                FilterDataReading(x, sds_dir=self._dir_eds)
                for x in fdx.findall(".//PlateData")
            ]
            self.welldata = df_from_readings(fdrs, self.runstarttime.timestamp())
        elif fdfs := glob(os.path.join(self._dir_eds, "filter", "*_filterdata.xml")):
            fdrs = [
                FilterDataReading(
                    ET.parse(fdf).find(".//PlateData"), sds_dir=self._dir_eds
                )
                for fdf in fdfs
            ]
            self.welldata = df_from_readings(fdrs, self.runstarttime.timestamp())
        else:
            self.welldata = None

        if self.welldata is not None:
            self.rawdata = _fdc_to_rawdata(self.welldata, self.runstarttime.timestamp())  # type: ignore
            self.filterdata = self.rawdata

    def data_for_sample(self, sample: str):
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
        wells = self.plate_setup.sample_wells[sample]
        x = self.welldata.loc[:, wells]
        return x

    def _update_from_log(self):
        if not os.path.isfile(os.path.join(self._dir_eds, "messages.log")):
            return
        try:
            msglog = open(os.path.join(self._dir_eds, "messages.log"), "r").read()
        except UnicodeDecodeError as e:
            log.warn(
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

        if m := re.search(r'^Run ([\d.]+) Starting "([^"]+)"$', msglog, re.MULTILINE):
            self.runstarttime = datetime.fromtimestamp(float(m[1]))
        else:
            self.runstarttime = None
            return

        tt = []

        # In normal runs, the protocol is there without the PROT command at the
        # beginning of the log as an info command, with quote.message.  Let's
        # try to grab it!
        if m := re.match(
            r"^Info (?:[\d.]+) (<quote.message>.*?</quote.message>)", msglog, re.DOTALL
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

        self.num_zones = int((len(tt[0]) - 3) / 2)

        self.temperatures = pd.DataFrame(
            tt,
            columns=pd.MultiIndex.from_tuples(
                [("time", "timestamp")]
                + [("sample", n) for n in range(1, self.num_zones + 1)]
                + [("other", "heatsink"), ("other", "cover")]
                + [("block", n) for n in range(1, self.num_zones + 1)]
            ),
        )

        self.temperatures[("time", "seconds")] = (
            self.temperatures[("time", "timestamp")] - self.runstarttime.timestamp()
        )
        self.temperatures[("time", "hours")] = (
            self.temperatures[("time", "seconds")] / 3600.0
        )
        self.temperatures[("sample", "avg")] = self.temperatures["sample"].mean(axis=1)
        self.temperatures[("block", "avg")] = self.temperatures["block"].mean(axis=1)


def _fdc_to_rawdata(fdc: pd.DataFrame, start_time: float) -> pd.DataFrame:
    ret: pd.DataFrame = fdc.loc[:, (slice(None), "fl")].copy()
    ret.columns = [f"{r}{c}" for r in "ABCDEFGH" for c in range(1, 13)]
    for i in range(0, 6):
        ret[f"temperature_{i+1}"] = fdc[f"A{(i+1)*2:02}", "rt"]
    ret["temperature_avg"] = ret.loc[:, slice("temperature_1", "temperature_6")].mean(
        axis=1
    )
    ret["timestamp"] = fdc["time", "timestamp"]
    ret["exptime"] = ret["timestamp"] - start_time
    ret["exphrs"] = (ret["timestamp"] - start_time) / 60 / 60
    # ret['time'] = ret['time'].astype('datetime64[s]') # fixme: should we?

    return ret
